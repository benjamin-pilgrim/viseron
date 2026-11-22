"""Tier handler."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import timedelta
from queue import Queue
from threading import Timer
from typing import TYPE_CHECKING, Literal, cast

import numpy as np
from apscheduler.jobstores.base import ConflictingIdError
from sqlalchemy import Delete, delete, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import NoResultFound
from watchdog.events import (
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserverVFS

from viseron.components.storage.const import (
    COMPONENT,
    EVENT_CHECK_TIER,
    EVENT_FILE_CREATED,
    EVENT_FILE_DELETED,
    LATEST_SNAPSHOT_FILENAME,
    TIER_CATEGORY_RECORDER,
    TIER_CATEGORY_SNAPSHOTS,
    TIER_SUBCATEGORY_EVENT_CLIPS,
    TIER_SUBCATEGORY_FACE_RECOGNITION,
    TIER_SUBCATEGORY_IMAGE_CLASSIFICATION,
    TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION,
    TIER_SUBCATEGORY_MOTION_DETECTOR,
    TIER_SUBCATEGORY_OBJECT_DETECTOR,
    TIER_SUBCATEGORY_SEGMENTS,
    TIER_SUBCATEGORY_THUMBNAILS,
    CleanupJobNames,
)
from viseron.components.storage.files import (
    delete_file_location_by_path,
    is_new_file_location,
    upsert_file,
    upsert_file_at_location,
    upsert_file_location,
)
from viseron.components.storage.models import (
    DeferredArtifacts,
    FileLocations,
    FileLocationState,
    Files,
    FilesMeta,
    Motion,
    MotionContours,
    Objects,
    PostProcessorResults,
    Recordings,
)
from viseron.components.storage.storage_subprocess import (
    DataItem,
    DataItemCopyFile,
    DataItemDeleteFile,
    DataItemMoveFile,
)
from viseron.components.storage.thumbnails import upsert_thumbnail_file
from viseron.components.storage.util import (
    EventCheckTier,
    EventFileCreated,
    EventFileDeleted,
    get_event_clips_path,
    get_segments_path,
    get_thumbnails_path,
    get_timelapse_path,
    is_storage_temp_file,
)
from viseron.components.webserver.const import COMPONENT as WEBSERVER_COMPONENT
from viseron.const import VISERON_SIGNAL_LAST_WRITE, VISERON_SIGNAL_STOPPING
from viseron.domains.camera import FailedCamera
from viseron.domains.camera.const import (
    CONFIG_CONTINUOUS_RECORDING,
    CONFIG_RECORDER,
    CONFIG_RETAIN,
    CONFIG_SCHEDULE,
    CONFIG_SCHEDULE_CONTINUOUS,
)
from viseron.domains.camera.schedule import resolve_timezone
from viseron.events import Event, EventEmptyData
from viseron.helpers import utcnow
from viseron.helpers.named_timer import NamedTimer
from viseron.helpers.validators import UNDEFINED
from viseron.watchdog.thread_watchdog import RestartableThread

if TYPE_CHECKING:
    from collections.abc import Callable

    import numpy as np
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.dml import ReturningDelete
    from watchdog.observers.api import BaseObserver

    from viseron import Viseron
    from viseron.components.storage import Storage
    from viseron.components.storage.config import (
        RecorderTier,
        SnapshotsTier,
        Tier,
        TimelapseTier,
    )
    from viseron.components.webserver import Webserver
    from viseron.domains.camera import AbstractCamera


def _src_path(event: FileSystemEvent) -> str:
    """Return the source path of a watchdog event as a str.

    watchdog types ``src_path`` as ``bytes | str`` because a watched path may be
    given as bytes. Viseron always schedules str paths, so it is always a str.
    """
    return cast("str", event.src_path)


LOGGER = logging.getLogger(__name__)


class TierHandler(FileSystemEventHandler):
    """Moves files up configured tiers."""

    def __init__(
        self,
        vis: Viseron,
        camera: AbstractCamera,
        tier_id: int,
        category: str,
        subcategory: str,
        tier: Tier,
        next_tier: Tier | None,
    ) -> None:
        self._logger = logging.getLogger(
            f"{__name__}.{camera.identifier}.tier_{tier_id}.{category}.{subcategory}"
        )
        super().__init__()

        self._vis = vis
        self._storage = vis.data[COMPONENT]
        self._webserver = vis.data[WEBSERVER_COMPONENT]
        self._camera = camera
        self._tier_id = tier_id
        self._category = category
        self._subcategory = subcategory
        self._tier = tier
        self._next_tier = next_tier

        self.initialize()
        vis.register_signal_handler(VISERON_SIGNAL_LAST_WRITE, self._shutdown)
        vis.register_signal_handler(VISERON_SIGNAL_STOPPING, self._stop_observer)
        vis.listen_event(
            EVENT_CHECK_TIER.format(
                camera_identifier=camera.identifier,
                tier_id=tier_id,
                category=category,
                subcategory=subcategory,
            ),
            self._check_tier_event_handler,
        )
        self._check_tier_lock = threading.Lock()
        self._tier_check_in_progress = False

        self._pending_updates: dict[str, Timer] = {}
        self._event_queue: Queue[FileSystemEvent | None] = Queue()
        self._event_thread = RestartableThread(
            target=self._process_events,
            daemon=True,
            name=f"tier_handler_{camera.identifier}",
            stage=VISERON_SIGNAL_LAST_WRITE,
        )
        self._event_thread.start()

        self._throttle_period = tier.check_interval.to_timedelta()
        self._time_of_last_call = utcnow() - self._throttle_period

        self._logger.debug("Tier %s monitoring path: %s", tier_id, self._path)
        os.makedirs(self._path, exist_ok=True)
        self._observer: BaseObserver | None = None
        # With one writer per storage root, completed transfers commit archive
        # locations directly. Startup reference repair probes known DB paths.
        # Watching the archive would repeatedly enumerate all historical footage.
        if not (
            tier_id > 0
            and category == TIER_CATEGORY_RECORDER
            and subcategory == TIER_SUBCATEGORY_SEGMENTS
        ):
            self._observer = (
                PollingObserverVFS(
                    stat=os.stat,
                    listdir=os.scandir,
                    polling_interval=tier.poll_interval,
                )
                if tier.poll
                else Observer()
            )
            self._observer.schedule(
                self,
                self._path,
                recursive=True,
            )
            self._observer.start()

    @property
    def tier(self) -> Tier:
        """Tier configuration."""
        return self._tier

    @property
    def first_tier(self) -> bool:
        """Return if first tier."""
        return self._tier_id == 0

    @property
    def tier_id(self) -> int:
        """Return tier id."""
        return self._tier_id

    def add_file_handler(self, path: str, pattern: str) -> None:
        """Add file handler to webserver."""
        self._logger.debug(f"Adding handler for /files{pattern}")
        add_file_handler(
            self._vis,
            self._webserver,
            path,
            pattern,
            self._camera,
            self._category,
            self._subcategory,
        )

    @property
    def tier_base_path(self) -> str:
        """Return tier base path."""
        return self._tier.path

    def initialize(self) -> None:
        """Tier handler specific initialization."""
        self._path = os.path.join(
            self._tier.path,
            self._category,
            self._subcategory,
            self._camera.identifier,
        )

    def _create_dataitem(
        self,
        *,
        force: bool = False,
    ) -> DataItem:
        """Create a DataItem for the check tier command."""
        raise NotImplementedError

    def _check_tier_event_handler(self, event: Event[EventCheckTier]) -> None:
        """Handle check tier event."""
        self._storage.cleanup_manager.run_job(CleanupJobNames.ORPHANED_FILES)
        self.check_tier(force=getattr(event.data, "force", False))

    def check_tier(self, *, force: bool = False) -> None:
        """Check if file should be moved to next tier."""
        with self._check_tier_lock:
            if self._tier_check_in_progress:
                if force:
                    self._schedule_capacity_retry()
                return
            # Throttling is also done in the worker process, but we do the same here
            # in order to not spam the workers with unneeded requests.
            now = utcnow()
            time_since_last_call = now - self._time_of_last_call
            if not force and time_since_last_call < self._throttle_period:
                return
            # Claim the in-flight slot before sending. The reply can take tens of
            # seconds, and every file event in that window would otherwise pass
            # the guard above and send a duplicate request, each one registering
            # a callback in the subprocess worker. The subprocess dedupes those
            # duplicates, so only one reply comes back and the rest of the
            # callbacks would leak. Marking in-flight here collapses the flood at
            # the source. Cleared in on_check_tier_result.
            self._tier_check_in_progress = True

        try:
            self._storage.tier_check_worker_send_command(
                self._create_dataitem(force=force),
                self.on_check_tier_result,
            )
        except Exception:
            # Release the slot if the command never made it out, otherwise this
            # tier would never be checked again.
            with self._check_tier_lock:
                self._tier_check_in_progress = False
            raise

    def _check_tier(self, get_session: Callable[[], Session], data: np.ndarray) -> None:
        files_processed = 0
        for file in data:
            if files_processed >= self._storage.file_batch_size:
                time.sleep(self._storage.sleep_between_batches)
                files_processed = 0

            handle_file(
                self._vis,
                get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                self._next_tier,
                file["path"],
                file["tier_path"],
                self._logger,
            )
            files_processed += 1

    def _release_check_tier(self) -> None:
        """Advance the throttle and release the in-flight slot.

        The throttle is advanced on every reply, including no-op replies where
        data is None (subprocess-side throttling, a check already in progress for
        the camera, or a job superseded in the dedup queue). Advancing only on
        replies that carried data would leave _time_of_last_call permanently
        stale, so a stream of no-op replies would keep the throttle open and let
        every file event send a fresh request.
        """
        with self._check_tier_lock:
            self._time_of_last_call = utcnow()
            self._tier_check_in_progress = False

    def _schedule_capacity_retry(self) -> None:
        """Retry a deferred tier after the worker has had time to make progress."""
        job_id = (
            f"storage-retry-{self._camera.identifier}-{self._tier_id}-"
            f"{self._category}-{self._subcategory}"
        )
        try:
            self._vis.background_scheduler.add_job(
                self.check_tier,
                "date",
                run_date=utcnow() + timedelta(seconds=5),
                kwargs={"force": True},
                id=job_id,
                misfire_grace_time=None,
            )
        except ConflictingIdError:
            pass

    def on_check_tier_result(self, item: DataItem) -> None:
        """Handle the result of the check tier command."""
        if item.deferred:
            self._release_check_tier()
            self._schedule_capacity_retry()
            return
        if item.error:
            self._logger.error("Error in tier check process: %s", item.error)

        # No payload to process: the subprocess throttled the job, a check was
        # already running for this camera, or the job was superseded in the dedup
        # queue. Release the slot immediately so the next event can send again.
        if item.data is None or len(item.data) == 0:
            self._release_check_tier()
            return

        # Bind to a local so the closure below keeps the non-None narrowing.
        data = item.data

        def run() -> None:
            """Run in a thread to not block the output queue handler that calls this."""
            try:
                self._check_tier(
                    self._storage.get_session,
                    data,
                )
            finally:
                self._release_check_tier()

        try:
            RestartableThread(
                target=run,
                name=(
                    "storage.tier_handler.check_tier."
                    f"{item.camera_identifier}.{item.tier_id}"
                ),
                register=False,
                daemon=True,
            ).start()
        except Exception:
            # The thread never started, so run()'s finally will not fire.
            self._release_check_tier()
            raise

    def _process_events(self) -> None:
        while True:
            event = self._event_queue.get()
            if event is None:
                self._logger.debug("Stopping event handler")
                break
            try:
                if isinstance(event, FileDeletedEvent):
                    self._on_deleted(event)
                elif isinstance(event, FileCreatedEvent):
                    self._on_created(event)
                elif isinstance(event, FileMovedEvent):
                    if self._path_contains(event.dest_path):
                        self._on_created(event)
                    else:
                        self._on_deleted(FileDeletedEvent(event.src_path))
                elif isinstance(event, FileModifiedEvent):
                    self._on_modified(event)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Error processing filesystem event: %s", event)

    def _path_contains(self, path: str) -> bool:
        """Return if a path belongs to this tier handler."""
        return path == self._path or path.startswith(f"{self._path}{os.sep}")

    @staticmethod
    def _event_path(event: FileSystemEvent) -> str:
        """Return the storage path affected by a filesystem event."""
        if isinstance(event, FileMovedEvent):
            return event.dest_path
        return event.src_path

    def on_any_event(self, event: FileSystemEvent) -> None:
        """Handle file system events."""
        if isinstance(event, FileMovedEvent) and not self._path_contains(
            event.dest_path
        ):
            event_path = event.src_path
        else:
            event_path = self._event_path(event)
        if self._storage.is_ignored_file(os.path.basename(event_path)):
            return
        if is_storage_temp_file(event_path):
            return
        self._event_queue.put(event)

    def _on_created(self, event: FileCreatedEvent | FileMovedEvent) -> None:
        """Insert into database when file is created."""
        event_path = self._event_path(event)
        self._logger.debug("File created: %s", event_path)
        try:
            os.stat(event_path)
        except FileNotFoundError:
            self._logger.debug("File not found after create event: %s", event_path)
            return
        except OSError:
            self._logger.warning(
                "Failed to stat created file: %s", event_path, exc_info=True
            )
            return

        file_meta = self._storage.temporary_files_meta.pop(event_path, None)
        if (
            file_meta is None
            and self._category == TIER_CATEGORY_RECORDER
            and self._subcategory == TIER_SUBCATEGORY_SEGMENTS
        ):
            # Segment create events can beat the fragmenter/move metadata handoff,
            # especially with polling observers. Wait briefly so timeline metadata
            # is not replaced by utcnow()/None just because the event arrived first.
            for _ in range(10):
                time.sleep(0.1)
                file_meta = self._storage.temporary_files_meta.pop(event_path, None)
                if file_meta is not None:
                    break
        upserted_file = upsert_file_at_location(
            self._storage.get_session,
            self._camera.identifier,
            self._category,
            self._subcategory,
            self._tier_id,
            self._tier.path,
            event_path,
            orig_ctime=file_meta.orig_ctime if file_meta else utcnow(),
            duration=file_meta.duration if file_meta else None,
            hls_init_hash=file_meta.hls_init_hash if file_meta else None,
        )
        if upserted_file and upserted_file.created:
            self._vis.dispatch_event(
                EVENT_FILE_CREATED.format(
                    camera_identifier=self._camera.identifier,
                    category=self._category,
                    subcategory=self._subcategory,
                ),
                EventFileCreated(
                    camera_identifier=self._camera.identifier,
                    category=self._category,
                    subcategory=self._subcategory,
                    file_name=os.path.basename(event_path),
                    path=event_path,
                ),
                store=False,
            )
        else:
            self._logger.debug(
                "File %s already exists in database; create event likely raced "
                "with move callback or duplicate watcher event",
                event_path,
            )

        self.check_tier()

    def _on_modified(self, event: FileModifiedEvent) -> None:
        """Update database when file is moved."""
        src_path = _src_path(event)

        def _update_size() -> None:
            """Update the size of a file in the database.

            Runs in a Timer to avoid spamming updates on duplicate events.
            """
            self._logger.debug("File modified (delayed event): %s", src_path)
            self._pending_updates.pop(src_path, None)
            try:
                size = os.path.getsize(src_path)
            except FileNotFoundError:
                self._logger.debug("File not found: %s", src_path)
                return
            except OSError:
                self._logger.warning(
                    "Failed to stat modified file: %s", event.src_path, exc_info=True
                )
                return

            with self._storage.get_session() as session:
                session.execute(
                    update(FileLocations)
                    .where(FileLocations.path == event.src_path)
                    .values(size=size)
                )
                session.execute(
                    update(Files).where(Files.path == event.src_path).values(size=size)
                )
                session.commit()

            self.check_tier()

        if src_path in self._pending_updates:
            self._pending_updates[src_path].cancel()
        self._pending_updates[src_path] = NamedTimer(
            1,
            _update_size,
            name=f"update_size for {src_path}",
            daemon=False,  # We want to wait for the update to finish on shutdown
        )
        self._pending_updates[src_path].start()

    def _on_deleted(self, event: FileDeletedEvent) -> None:
        """Remove file from database when it is deleted."""
        src_path = _src_path(event)
        self._logger.debug("File deleted: %s", event.src_path)
        moving_files = getattr(self._storage, "temporary_moving_files", set())
        with self._storage.get_session() as session:
            delete_file_location_by_path(
                session,
                event.src_path,
                delete_logical=event.src_path not in moving_files,
            )
            session.commit()

        self._vis.dispatch_event(
            EVENT_FILE_DELETED.format(
                camera_identifier=self._camera.identifier,
                category=self._category,
                subcategory=self._subcategory,
            ),
            EventFileDeleted(
                camera_identifier=self._camera.identifier,
                category=self._category,
                subcategory=self._subcategory,
                file_name=os.path.basename(src_path),
                path=src_path,
            ),
            store=False,
        )

    def _shutdown(self) -> None:
        """Shutdown the observer and event handler."""
        self._logger.debug("Initiating observer shutdown")
        if self._tier.move_on_shutdown:
            self._logger.debug("Forcing move of files")
            force_move_files(
                self._vis,
                self._storage,
                self._storage.get_session,
                self._category,
                self._subcategory,
                self._tier_id,
                self._camera.identifier,
                self._tier,
                self._next_tier,
                self._logger,
            )

    def _stop_observer(self) -> None:
        """Stop the observer."""
        self._logger.debug("Stopping observer")
        for pending_update in self._pending_updates.copy().values():
            pending_update.join()
        self._event_queue.put(None)
        self._event_thread.join()
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()


class RetentionTierHandler(TierHandler):
    """Handle a tier whose max/min age and size decide when files move."""

    _tier: SnapshotsTier

    def initialize(self) -> None:
        """Initialize the retention rules of the tier."""
        super().initialize()
        self._max_bytes = self._tier.max_size.to_bytes()
        self._min_bytes = self._tier.min_size.to_bytes()
        self._max_age = self._tier.max_age.to_timedelta()
        self._min_age = self._tier.min_age.to_timedelta()

        if self._next_tier is None and not self._max_age and not self._max_bytes:
            self._logger.warning(
                "Last tier '%s' has no max_age or max_size configured; "
                "files on this tier will accumulate indefinitely.",
                self._tier.path,
            )

    def _create_dataitem(self, *, force: bool = False) -> DataItem:
        """Create a DataItem for the check tier command."""
        return DataItem(
            cmd="check_tier",
            camera_identifier=self._camera.identifier,
            tier_id=self._tier_id,
            category=self._category,
            subcategories=[self._subcategory],
            throttle_period=self._throttle_period,
            max_bytes=self._max_bytes,
            min_age=self._min_age,
            max_age=self._max_age,
            min_bytes=self._min_bytes,
            drain=self._tier.drain,
            force=force,
        )


class SegmentsTierHandler(TierHandler):
    """Handle the recorder tiers."""

    _tier: RecorderTier

    def initialize(self) -> None:
        """Initialize recorder tier."""
        self._path = get_segments_path(self._tier, self._camera)

        continuous = self._tier.continuous
        self._continuous_max_bytes = continuous.max_size.to_bytes()
        self._continuous_min_bytes = continuous.min_size.to_bytes()
        self._continuous_max_age = continuous.max_age.to_timedelta()
        self._continuous_min_age = continuous.min_age.to_timedelta()
        self._continuous_params = [
            self._continuous_max_bytes,
            self._continuous_min_age,
            self._continuous_min_bytes,
            self._continuous_max_age,
        ]

        events = self._tier.events
        self._events_max_bytes = events.max_size.to_bytes()
        self._events_min_bytes = events.min_size.to_bytes()
        self._events_min_age = events.min_age.to_timedelta()

        if self.first_tier and self._camera.config.get(CONFIG_RECORDER, {}).get(
            CONFIG_RETAIN, None
        ):
            self._logger.warning(
                f"Camera {self._camera.identifier} is using 'retain' for 'recorder' "
                "which has been deprecated and will be removed in a future release. "
                "Please use the new 'storage' component with the 'max_age' config "
                "option instead. For now, the value of 'retain' will be used as "
                "'max_age' for the first tier, but this WILL change and might cause "
                "you to lose data."
            )
            self._events_max_age = timedelta(
                days=self._camera.config[CONFIG_RECORDER][CONFIG_RETAIN]
            )
        else:
            self._events_max_age = events.max_age.to_timedelta()
        self._events_params = [
            self._events_max_bytes,
            self._events_max_age,
            self._events_min_bytes,
            self._events_min_age,
        ]

        self._events_enabled = any(self._events_params)
        self._continuous_configured = (
            any(self._continuous_params)
            and self._camera.config[CONFIG_RECORDER][CONFIG_CONTINUOUS_RECORDING]
        )

        if (
            self._next_tier is None
            and not any(self._events_params)
            and not any(self._continuous_params)
        ):
            self._logger.warning(
                "Last recorder tier '%s' has no retention (max_age/max_size) "
                "configured for events or continuous; files on this tier will "
                "accumulate indefinitely.",
                self._path,
            )

        self.add_file_handler(self._path, rf"{self._path}/(.*.m4s$)")
        self.add_file_handler(self._path, rf"{self._path}/(.*.mp4$)")

    def _continuous_schedule_entries(self) -> list[dict[str, str]] | None:
        """Return the configured continuous-recording schedule entries, if any.

        None means continuous recording is unrestricted by a schedule (either
        no schedule is configured, or the schedule doesn't restrict
        continuous).
        """
        schedule = self._camera.config[CONFIG_RECORDER][CONFIG_SCHEDULE]
        if not schedule or schedule == UNDEFINED:
            return None
        entries = schedule[CONFIG_SCHEDULE_CONTINUOUS]
        if not entries or entries == UNDEFINED:
            return None
        return entries

    def _continuous_schedule_timezone(self) -> str | None:
        """Return the timezone the continuous-recording schedule is evaluated in.

        None when there is no continuous schedule to evaluate.
        """
        if self._continuous_schedule_entries() is None:
            return None
        schedule = self._camera.config[CONFIG_RECORDER][CONFIG_SCHEDULE]
        return resolve_timezone(schedule)

    def _create_dataitem(self, *, force: bool = False) -> DataItem:
        """Create a DataItem for the check tier command."""
        return DataItem(
            cmd="check_tier",
            camera_identifier=self._camera.identifier,
            tier_id=self._tier_id,
            category=self._category,
            subcategories=[
                self._subcategory,
                TIER_SUBCATEGORY_THUMBNAILS,
                TIER_SUBCATEGORY_EVENT_CLIPS,
            ],
            throttle_period=self._throttle_period,
            files_enabled=self._continuous_configured,
            max_bytes=self._continuous_max_bytes,
            min_age=max(
                self._continuous_min_age,
                timedelta(seconds=self._camera.recorder.lookback),
            ),
            max_age=self._continuous_max_age,
            min_bytes=self._continuous_min_bytes,
            drain=self._tier.drain,
            events_enabled=self._events_enabled,
            events_max_bytes=self._events_max_bytes,
            events_min_age=self._events_min_age,
            events_max_age=self._events_max_age,
            events_min_bytes=self._events_min_bytes,
            continuous_schedule=self._continuous_schedule_entries(),
            continuous_schedule_timezone=self._continuous_schedule_timezone(),
            continuous_lookback_seconds=self._camera.recorder.lookback,
            force=force,
        )

    @property
    def events_enabled(self) -> bool:
        """Return if events are enabled."""
        return self._events_enabled

    @property
    def continuous_enabled(self) -> bool:
        """Return if continuous is enabled."""
        return self._continuous_configured

    def _handle_events(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        events_next_tier: SegmentsTierHandler | None,
    ) -> list[int]:
        # A file can be in multiple recordings, so we need to keep track of which
        # files we have already processed using processed_paths
        files_processed = 0
        processed_paths: list[str] = []
        recording_ids: list[int] = []
        for file in data:
            if file["path"] in processed_paths:
                continue

            if files_processed >= self._storage.file_batch_size:
                time.sleep(self._storage.sleep_between_batches)
                files_processed = 0

            if file["recording_id"] >= 0 and file["recording_id"] not in recording_ids:
                recording_ids.append(int(file["recording_id"]))

            force_delete = bool(file["recording_id"] == -1)
            handle_file(
                self._vis,
                get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                events_next_tier.tier if events_next_tier else None,
                file["path"],
                file["tier_path"],
                self._logger,
                force_delete=force_delete,
                next_tier_id=events_next_tier.tier_id if events_next_tier else None,
            )
            processed_paths.append(file["path"])
            files_processed += 1
        return recording_ids

    def _handle_continuous(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        continuous_next_tier: SegmentsTierHandler | None,
    ) -> None:
        files_processed = 0
        for file in data:
            if files_processed >= self._storage.file_batch_size:
                time.sleep(self._storage.sleep_between_batches)
                files_processed = 0

            handle_file(
                self._vis,
                get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                continuous_next_tier.tier if continuous_next_tier else None,
                file["path"],
                file["tier_path"],
                self._logger,
                next_tier_id=(
                    continuous_next_tier.tier_id if continuous_next_tier else None
                ),
            )
            files_processed += 1

    def _handle_events_and_continuous(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        events_next_tier: SegmentsTierHandler | None,
        continuous_next_tier: SegmentsTierHandler | None,
    ) -> list[int]:
        # A file can be in multiple recordings, so we need to keep track of which
        # files we have already processed using processed_paths
        files_processed = 0
        processed_paths: list[str] = []
        recording_ids: list[int] = []
        for file in data:
            if file["path"] in processed_paths:
                continue

            if files_processed >= self._storage.file_batch_size:
                time.sleep(self._storage.sleep_between_batches)
                files_processed = 0

            if file["recording_id"] >= 0 and file["recording_id"] not in recording_ids:
                recording_ids.append(int(file["recording_id"]))

            force_delete = False
            next_tier = None
            # If the file is not part of a recording, and no succeeding tiers
            # store continuous recordings we can delete the file
            if file["recording_id"] == -1 and continuous_next_tier is None:
                force_delete = True
            # If no succeeding tier stores either events or continuous
            # recordings, we can delete the file
            elif events_next_tier is None and continuous_next_tier is None:
                force_delete = True
            elif events_next_tier and continuous_next_tier is None:
                next_tier = events_next_tier
            elif continuous_next_tier and events_next_tier is None:
                next_tier = continuous_next_tier
            elif events_next_tier and continuous_next_tier:
                # Find the lowest tier_id for the two next tiers
                next_tier = (
                    events_next_tier
                    if events_next_tier.tier_id < continuous_next_tier.tier_id
                    else continuous_next_tier
                )

            handle_file(
                self._vis,
                get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                next_tier.tier if next_tier else None,
                file["path"],
                file["tier_path"],
                self._logger,
                force_delete=force_delete,
                next_tier_id=next_tier.tier_id if next_tier else None,
            )
            processed_paths.append(file["path"])
            files_processed += 1
        return recording_ids

    def _check_tier(self, get_session: Callable[[], Session], data: np.ndarray) -> None:
        events_next_tier = None
        recording_ids: list[int] = []
        if self._events_enabled and not self.continuous_enabled:
            events_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "events"
            )
            recording_ids = self._handle_events(get_session, data, events_next_tier)
        elif self.continuous_enabled and not self._events_enabled:
            continuous_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "continuous"
            )
            self._handle_continuous(get_session, data, continuous_next_tier)
        else:
            events_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "events"
            )
            continuous_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "continuous"
            )
            recording_ids = self._handle_events_and_continuous(
                get_session, data, events_next_tier, continuous_next_tier
            )

        # Signal to the thumbnail tier that the recording has been moved
        if recording_ids:
            self._logger.debug("Handle thumbnails for recordings: %s", recording_ids)
            for recording_id in recording_ids:
                thumbnail_tier_handler: ThumbnailTierHandler = (
                    self._storage.camera_tier_handlers[self._camera.identifier][
                        self._category
                    ][self._tier_id][TIER_SUBCATEGORY_THUMBNAILS]
                )
                thumbnail_tier_handler.move_thumbnail(
                    recording_id,
                    events_next_tier.tier if events_next_tier else None,
                )

        # Signal to the event clips tier that the recording has been moved
        if recording_ids:
            self._logger.debug("Handle event clip for recordings: %s", recording_ids)
            for recording_id in recording_ids:
                recordings_tier_handler: EventClipTierHandler = (
                    self._storage.camera_tier_handlers[self._camera.identifier][
                        self._category
                    ][self._tier_id][TIER_SUBCATEGORY_EVENT_CLIPS]
                )
                recordings_tier_handler.move_event_clip(
                    recording_id,
                    events_next_tier.tier if events_next_tier else None,
                )

        # Delete recordings from Recordings table if this is the last tier
        if recording_ids and events_next_tier is None:
            self._logger.debug("Deleting recordings: %s", recording_ids)
            with get_session() as _session:
                stmt = delete(Recordings).where(Recordings.id.in_(recording_ids))
                _session.execute(stmt)
                _session.commit()


SNAPSHOT_REFERENCE_MODELS: dict[str, type[Motion | Objects | PostProcessorResults]] = {
    TIER_SUBCATEGORY_MOTION_DETECTOR: Motion,
    TIER_SUBCATEGORY_OBJECT_DETECTOR: Objects,
    TIER_SUBCATEGORY_FACE_RECOGNITION: PostProcessorResults,
    TIER_SUBCATEGORY_IMAGE_CLASSIFICATION: PostProcessorResults,
    TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION: PostProcessorResults,
}


class SnapshotTierHandler(RetentionTierHandler):
    """Handle the snapshot tiers."""

    def initialize(self) -> None:
        """Initialize snapshot tier."""
        super().initialize()
        self.add_file_handler(self._path, rf"{self._path}/(.*.jpg$)")
        self._storage.ignore_file(LATEST_SNAPSHOT_FILENAME)

    def _on_deleted(self, event: FileDeletedEvent) -> None:
        src_path = _src_path(event)

        if src_path in getattr(self._storage, "temporary_moving_files", set()):
            super()._on_deleted(event)
            return

        with self._storage.get_session() as session:
            deleted_file_id = (
                select(FileLocations.file_id)
                .where(FileLocations.path == src_path)
                .scalar_subquery()
            )
            another_location = session.execute(
                select(FileLocations.id)
                .where(FileLocations.file_id == deleted_file_id)
                .where(FileLocations.path != src_path)
                .limit(1)
            ).scalar_one_or_none()
        if another_location is not None:
            super()._on_deleted(event)
            return

        stmt: Delete | ReturningDelete[tuple[int]]
        if self._subcategory == TIER_SUBCATEGORY_MOTION_DETECTOR:
            with self._storage.get_session() as session:
                file_id = session.execute(
                    select(FileLocations.file_id).where(
                        FileLocations.path == event.src_path
                    )
                ).scalar_one_or_none()
                delete_condition = Motion.snapshot_path == event.src_path
                if file_id is not None:
                    delete_condition = or_(
                        delete_condition, Motion.snapshot_file_id == file_id
                    )
                stmt = delete(Motion).where(delete_condition).returning(Motion.id)
                result = session.execute(stmt)
                motion_ids = [row[0] for row in result]

                if motion_ids:
                    stmt2 = delete(MotionContours).where(
                        MotionContours.motion_id.in_(motion_ids)
                    )
                    session.execute(stmt2)

                session.commit()

        elif self._subcategory == TIER_SUBCATEGORY_OBJECT_DETECTOR:
            with self._storage.get_session() as session:
                file_id = session.execute(
                    select(FileLocations.file_id).where(
                        FileLocations.path == event.src_path
                    )
                ).scalar_one_or_none()
                delete_condition = Objects.snapshot_path == event.src_path
                if file_id is not None:
                    delete_condition = or_(
                        delete_condition, Objects.snapshot_file_id == file_id
                    )
                stmt = delete(Objects).where(delete_condition)
                session.execute(stmt)
                session.commit()

        elif self._subcategory in [
            TIER_SUBCATEGORY_FACE_RECOGNITION,
            TIER_SUBCATEGORY_IMAGE_CLASSIFICATION,
            TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION,
        ]:
            with self._storage.get_session() as session:
                file_id = session.execute(
                    select(FileLocations.file_id).where(
                        FileLocations.path == event.src_path
                    )
                ).scalar_one_or_none()
                delete_condition = PostProcessorResults.snapshot_path == event.src_path
                if file_id is not None:
                    delete_condition = or_(
                        delete_condition,
                        PostProcessorResults.snapshot_file_id == file_id,
                    )
                stmt = delete(PostProcessorResults).where(delete_condition)
                session.execute(stmt)
                session.commit()

        super()._on_deleted(event)


class ThumbnailTierHandler(TierHandler):
    """Handle thumbnails."""

    _tier: RecorderTier

    def initialize(self) -> None:
        """Initialize thumbnail tier."""
        self._path = get_thumbnails_path(self._tier, self._camera)
        self._storage.ignore_file("latest_thumbnail.jpg")

    def check_tier(self, *, force: bool = False) -> None:
        """Do nothing, as we don't want to move thumbnails."""

    def _on_created(self, event: FileCreatedEvent) -> None:
        src_path = _src_path(event)

        try:
            thumbnail_file_id = upsert_thumbnail_file(
                self._storage.get_session,
                self._storage,
                self._camera.identifier,
                event.src_path,
            )
            with self._storage.get_session() as session:
                values: dict[str, str | int] = {"thumbnail_path": event.src_path}
                if thumbnail_file_id is not None:
                    values["thumbnail_file_id"] = thumbnail_file_id

                stmt = (
                    update(Recordings)
                    .where(
                        Recordings.id == os.path.basename(event.src_path).split(".")[0]
                    )
                    .values(**values)
                )
                session.execute(stmt)
                session.commit()
        except Exception as error:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.error(
                "Failed to update thumbnail path for recording with path: "
                f"{src_path}: {error}"
            )
        super()._on_created(event)

    def move_thumbnail(self, recording_id: int, next_tier: Tier | None) -> None:
        """Move thumbnail to next tier."""
        with self._storage.get_session() as session:
            sel = select(Recordings).where(Recordings.id == recording_id)
            try:
                recording = session.execute(sel).scalar_one()
            except NoResultFound as err:
                self._logger.error(
                    "Failed to move thumbnail for recording with id %s: %s",
                    recording_id,
                    err,
                )
                return

            if recording.thumbnail_path is None:
                return

            handle_file(
                self._vis,
                self._storage.get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                next_tier,
                recording.thumbnail_path,
                self._tier.path,
                self._logger,
            )
            session.commit()


class EventClipTierHandler(TierHandler):
    """Handle event clips created by create_event_clip."""

    _tier: RecorderTier

    def initialize(self) -> None:
        """Initialize event clips tier."""
        self._path = get_event_clips_path(self._tier, self._camera)

    def check_tier(self, *, force: bool = False) -> None:
        """Do nothing, as we move event clips manually."""

    def _update_clip_path(self, event: FileCreatedEvent) -> None:
        src_path = _src_path(event)

        try:
            clip_file_id = upsert_file(
                self._storage.get_session,
                self._storage,
                self._camera.identifier,
                TIER_CATEGORY_RECORDER,
                TIER_SUBCATEGORY_EVENT_CLIPS,
                event.src_path,
            )
            with self._storage.get_session() as session:
                values: dict[str, str | int] = {"clip_path": event.src_path}
                if clip_file_id is not None:
                    values["clip_file_id"] = clip_file_id

                stmt = (
                    update(Recordings)
                    .where(Recordings.camera_identifier == self._camera.identifier)
                    .where(
                        Recordings.clip_path.like(
                            f"%{src_path.split('/')[-2]}/{os.path.basename(src_path)}"
                        )
                    )
                    .values(**values)
                )
                session.execute(stmt)
                session.commit()
        except Exception as error:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.error(
                "Failed to update clip path for recording with path: "
                f"{src_path}: {error}"
            )

    def _on_created(self, event: FileCreatedEvent) -> None:
        if not self.first_tier:
            self._update_clip_path(event)
        super()._on_created(event)

    def move_event_clip(self, recording_id: int, next_tier: Tier | None) -> None:
        """Move event clip to next tier."""
        with self._storage.get_session() as session:
            sel = (
                select(Recordings)
                .where(Recordings.id == recording_id)
                .where(Recordings.clip_path.is_not(None))
            )
            recording = session.execute(sel).scalar()
            if recording is None:
                return

            handle_file(
                self._vis,
                self._storage.get_session,
                self._storage,
                self._camera.identifier,
                self._tier_id,
                self._category,
                self._subcategory,
                self._tier,
                next_tier,
                recording.clip_path,
                self._tier.path,
                self._logger,
            )
            session.commit()


def find_next_tier_segments(
    storage: Storage,
    tier_id: int,
    camera: AbstractCamera,
    file_type: Literal["events", "continuous"],
) -> SegmentsTierHandler | None:
    """Find the next tier for segments."""
    next_tier = None
    for tier in storage.camera_tier_handlers[camera.identifier][TIER_CATEGORY_RECORDER][
        tier_id + 1 :
    ]:
        segments_tier_handler: SegmentsTierHandler = tier[TIER_SUBCATEGORY_SEGMENTS]
        if segments_tier_handler.events_enabled and file_type == "events":
            next_tier = segments_tier_handler
            break
        if segments_tier_handler.continuous_enabled and file_type == "continuous":
            next_tier = segments_tier_handler
            break
    return next_tier


def _retry_tier(
    storage: Storage, camera: str, tier_id: int, category: str, subcategory: str
) -> None:
    """Reschedule checks for media that was deferred at admission."""
    try:
        handler = storage.camera_tier_handlers[camera][category][tier_id][subcategory]
    except (IndexError, KeyError):
        return
    handler._schedule_capacity_retry()


def _defer_artifact(
    get_session: Callable[..., Session],
    camera_identifier: str,
    subcategory: str,
    src: str,
    *,
    dst: str | None = None,
    dst_tier_id: int | None = None,
    dst_tier_path: str | None = None,
) -> None:
    """Persist the exact move or delete without allocating a scheduler job."""
    key = hashlib.sha256(
        json.dumps((camera_identifier, subcategory, src, dst)).encode()
    ).hexdigest()
    with get_session() as session:
        session.execute(
            insert(DeferredArtifacts)
            .values(
                operation_key=key,
                camera_identifier=camera_identifier,
                subcategory=subcategory,
                src=src,
                dst=dst,
                dst_tier_id=dst_tier_id,
                dst_tier_path=dst_tier_path,
                retry_at=utcnow() + timedelta(seconds=5),
            )
            .on_conflict_do_nothing(index_elements=[DeferredArtifacts.operation_key])
        )
        session.commit()


class ArtifactRetryQueue:
    """Drain durable artifact retries with a bounded number of requests in flight."""

    def __init__(self, vis: Viseron, storage: Storage) -> None:
        self._vis = vis
        self._storage = storage
        self._in_flight: dict[int, str] = {}
        self._lock = threading.Lock()

    def drain(self) -> None:
        """Submit a batch of due artifacts to the storage worker."""
        with self._lock:
            busy_paths = set(self._in_flight.values())
        limit = 64 - len(busy_paths)
        if limit <= 0:
            return

        with self._storage.get_session() as session:
            now = utcnow()
            pending = list(
                session.scalars(
                    select(DeferredArtifacts)
                    .where(DeferredArtifacts.retry_at <= now)
                    .where(DeferredArtifacts.src.not_in(busy_paths))
                    .order_by(DeferredArtifacts.retry_at, DeferredArtifacts.id)
                    .limit(limit)
                )
            )
            for operation in pending:
                operation.retry_at = now + timedelta(seconds=5)
            # The worker callbacks need these values after the session closes.
            session.flush()
            for operation in pending:
                session.expunge(operation)
            session.commit()

        for operation in pending:
            if operation.src in busy_paths:
                continue
            requested = self._storage.camera_requested_files_count.get(
                operation.camera_identifier
            )
            if requested is not None and operation.src in requested.filenames:
                continue
            busy_paths.add(operation.src)
            with self._lock:
                self._in_flight[operation.id] = operation.src
            try:
                self._submit(operation)
            except Exception:  # pylint: disable=broad-except
                with self._lock:
                    self._in_flight.pop(operation.id, None)
                LOGGER.exception(
                    "Could not submit artifact retry for %s", operation.src
                )

    def _submit(self, operation: DeferredArtifacts) -> None:
        def complete(success: bool) -> None:
            try:
                if success:
                    with self._storage.get_session() as session:
                        session.execute(
                            delete(DeferredArtifacts).where(
                                DeferredArtifacts.id == operation.id
                            )
                        )
                        session.commit()
            finally:
                with self._lock:
                    self._in_flight.pop(operation.id, None)

        if operation.dst is None:
            delete_file(
                self._storage,
                operation.src,
                callback=lambda item: complete(not item.deferred and not item.error),
            )
        else:
            if operation.dst_tier_id is None or operation.dst_tier_path is None:
                raise ValueError(
                    f"Missing destination tier for artifact {operation.id}"
                )
            move_file(
                self._vis,
                self._storage,
                self._storage.get_session,
                operation.camera_identifier,
                operation.dst_tier_id - 1,
                TIER_CATEGORY_RECORDER,
                operation.subcategory,
                operation.src,
                operation.dst,
                operation.dst_tier_path,
                LOGGER,
                dst_tier_id=operation.dst_tier_id,
                on_complete=complete,
            )


def _is_recording_artifact(category: str, subcategory: str) -> bool:
    return category == TIER_CATEGORY_RECORDER and subcategory in (
        TIER_SUBCATEGORY_EVENT_CLIPS,
        TIER_SUBCATEGORY_THUMBNAILS,
    )


def handle_file(
    vis: Viseron,
    get_session: Callable[..., Session],
    storage: Storage,
    camera_identifier: str,
    curr_tier_id: int,
    curr_tier_category: str,
    curr_tier_subcategory: str,
    curr_tier: Tier,
    next_tier: Tier | None,
    path: str,
    tier_path: str,
    logger: logging.Logger,
    *,
    force_delete: bool = False,
    next_tier_id: int | None = None,
) -> None:
    """Move file if there is a succeeding tier, else delete the file."""
    if path in storage.camera_requested_files_count[camera_identifier].filenames:
        logger.debug("File %s is recently requested, skipping", path)
        return

    if force_delete or next_tier is None:

        def on_delete(item: DataItemDeleteFile) -> None:
            if not item.deferred:
                return
            if _is_recording_artifact(curr_tier_category, curr_tier_subcategory):
                _defer_artifact(
                    get_session, camera_identifier, curr_tier_subcategory, path
                )
            else:
                _retry_tier(
                    storage,
                    camera_identifier,
                    curr_tier_id,
                    curr_tier_category,
                    curr_tier_subcategory,
                )

        delete_file(storage, path, callback=on_delete)
        return

    new_path = os.path.normpath(path.replace(tier_path, next_tier.path, 1))
    if new_path == os.path.normpath(path):
        logger.warning(
            "Failed to move file %s to next tier, new path is the same as old. "
            "Current tier path: %s. Recorded tier path: %s.",
            path,
            curr_tier.path,
            tier_path,
        )
        # No move can be submitted for this row. OrphanedFilesCleanup can
        # remove the on-disk file if it still exists.
        with get_session() as session:
            delete_file_location_by_path(session, path)
            session.commit()
        return

    # The worker and the move callback update locations once the filesystem
    # operation succeeds. Until then the recorded path is needed for retries,
    # even when it belongs to a previous tier configuration.
    move_file(
        vis,
        storage,
        get_session,
        camera_identifier,
        curr_tier_id,
        curr_tier_category,
        curr_tier_subcategory,
        path,
        new_path,
        next_tier.path,
        logger,
        **({"dst_tier_id": next_tier_id} if next_tier_id is not None else {}),
    )


def delete_file(
    storage: Storage,
    path: str,
    callback: Callable[[DataItemDeleteFile], None] | None = None,
) -> None:
    """Delete file from storage."""
    storage.tier_check_worker_send_command(
        DataItemDeleteFile(
            cmd="delete_file",
            src=path,
        ),
        callback=callback,
    )


def move_file(
    vis: Viseron,
    storage: Storage,
    get_session: Callable[..., Session],
    camera_identifier: str,
    curr_tier_id: int,
    _curr_tier_category: str,
    _curr_tier_subcategory: str,
    src: str,
    dst: str,
    dst_tier_path: str,
    logger: logging.Logger,
    *,
    dst_tier_id: int | None = None,
    on_complete: Callable[[bool], None] | None = None,
) -> None:
    """Move file from src to dst.

    Publish and commit the new location before requesting removal of the source.
    """
    dst_tier_id = curr_tier_id + 1 if dst_tier_id is None else dst_tier_id
    logger.debug("Moving file from %s to %s", src, dst)
    file_meta: FilesMeta
    file_id: int
    try:
        with get_session() as session:
            sel = (
                select(Files)
                .join(FileLocations, FileLocations.file_id == Files.id)
                .where(FileLocations.path == src)
                .where(FileLocations.state == FileLocationState.AVAILABLE.value)
            )
            res = session.execute(sel).scalar_one()
            file_id = res.id
            file_meta = FilesMeta(
                orig_ctime=res.orig_ctime,
                duration=res.duration,
                hls_init_hash=res.hls_init_hash,
            )
    except NoResultFound as error:
        logger.debug(f"Failed to find metadata for {src}: {error}")
        if on_complete is not None:
            on_complete(True)
            return
        delete_file(
            storage,
            src,
        )
        return

    def _commit_published_move(size: int) -> None:
        with get_session() as session:
            location_created = is_new_file_location(session, dst)
            upsert_file_location(
                session,
                file_id,
                dst_tier_id,
                dst_tier_path,
                dst,
                size,
                state=FileLocationState.AVAILABLE.value,
            )
            delete_file_location_by_path(
                session,
                src,
                missing_state=FileLocationState.PENDING_DELETE.value,
            )
            session.execute(
                update(Files)
                .where(Files.id == file_id)
                .values(
                    tier_id=dst_tier_id,
                    tier_path=dst_tier_path,
                    path=dst,
                    directory=os.path.dirname(dst),
                    filename=os.path.basename(dst),
                    size=size,
                    orig_ctime=file_meta.orig_ctime,
                    duration=file_meta.duration,
                    hls_init_hash=file_meta.hls_init_hash,
                )
            )
            if _curr_tier_category == TIER_CATEGORY_SNAPSHOTS:
                snapshot = SNAPSHOT_REFERENCE_MODELS.get(_curr_tier_subcategory)
                if snapshot is not None:
                    session.execute(
                        update(snapshot)
                        .where(
                            or_(
                                snapshot.snapshot_path == src,
                                snapshot.snapshot_file_id == file_id,
                            )
                        )
                        .values(snapshot_path=dst, snapshot_file_id=file_id)
                    )
            session.commit()

        storage.temporary_files_meta.pop(dst, None)
        moving_files = getattr(storage, "temporary_moving_files", None)
        if moving_files is not None:
            moving_files.discard(src)
        if location_created:
            vis.dispatch_event(
                EVENT_FILE_CREATED.format(
                    camera_identifier=camera_identifier,
                    category=_curr_tier_category,
                    subcategory=_curr_tier_subcategory,
                ),
                EventFileCreated(
                    camera_identifier=camera_identifier,
                    category=_curr_tier_category,
                    subcategory=_curr_tier_subcategory,
                    file_name=os.path.basename(dst),
                    path=dst,
                ),
                store=False,
            )
        # Wake the destination tier after the authoritative DB commit. This is a
        # liveness nudge for missed/delayed watcher create events on remote
        # filesystems; correctness does not depend on the event being delivered.
        vis.dispatch_event(
            EVENT_CHECK_TIER.format(
                camera_identifier=camera_identifier,
                tier_id=dst_tier_id,
                category=_curr_tier_category,
                subcategory=_curr_tier_subcategory,
            ),
            EventEmptyData(),
            store=False,
        )
        # The pending location also lets periodic cleanup finish the move if
        # this delete is deferred, interrupted or fails on an unavailable mount.
        delete_file(storage, src)

    def _move_file_callback(item: DataItemMoveFile) -> None:
        if item.deferred or item.retryable_error:
            storage.temporary_files_meta.pop(dst, None)
            storage.temporary_moving_files.discard(src)
            if item.retryable_error:
                logger.warning("Retrying move from %s to %s: %s", src, dst, item.error)
            if on_complete is not None:
                on_complete(False)
            elif _is_recording_artifact(_curr_tier_category, _curr_tier_subcategory):
                _defer_artifact(
                    get_session,
                    camera_identifier,
                    _curr_tier_subcategory,
                    src,
                    dst=dst,
                    dst_tier_id=dst_tier_id,
                    dst_tier_path=dst_tier_path,
                )
            else:
                _retry_tier(
                    storage,
                    camera_identifier,
                    curr_tier_id,
                    _curr_tier_category,
                    _curr_tier_subcategory,
                )
            return
        if item.source_missing:
            storage.temporary_files_meta.pop(dst, None)
            moving_files = getattr(storage, "temporary_moving_files", None)
            if moving_files is not None:
                moving_files.discard(src)
            logger.debug("Source file %s was missing when moving to %s", src, dst)
            success = False
            try:
                with get_session() as session:
                    delete_file_location_by_path(session, src)
                    session.commit()
                success = True
            finally:
                if on_complete is not None:
                    on_complete(success)
            return
        if item.published and item.size is not None:
            success = False
            try:
                _commit_published_move(item.size)
                success = True
            finally:
                if on_complete is not None:
                    on_complete(success)
            return
        storage.temporary_files_meta.pop(dst, None)
        storage.temporary_moving_files.discard(src)
        logger.error("Error moving file %s to %s: %s", src, dst, item.error)
        if on_complete is not None:
            on_complete(False)

    def _send_segment_move() -> None:
        storage.temporary_files_meta[dst] = file_meta
        moving_files = getattr(storage, "temporary_moving_files", None)
        if moving_files is not None:
            moving_files.add(src)
        destination_root = None
        if (
            _curr_tier_category == TIER_CATEGORY_RECORDER
            and _curr_tier_subcategory == TIER_SUBCATEGORY_EVENT_CLIPS
        ):
            destination_root = os.path.normpath(
                os.path.join(
                    dst_tier_path, TIER_SUBCATEGORY_EVENT_CLIPS, camera_identifier
                )
            )
        storage.tier_check_worker_send_command(
            DataItemMoveFile(
                cmd="move_file",
                src=src,
                dst=dst,
                destination_root=destination_root,
            ),
            callback=_move_file_callback,
        )

    def _copy_init_callback(item: DataItemCopyFile) -> None:
        if item.deferred or item.retryable_error:
            _retry_tier(
                storage,
                camera_identifier,
                curr_tier_id,
                _curr_tier_category,
                _curr_tier_subcategory,
            )
            return
        if item.published:
            if item.source_missing:
                logger.debug(
                    "Using existing destination HLS init while moving %s to %s; "
                    "source sidecar %s was missing",
                    src,
                    dst,
                    item.src,
                )
            _send_segment_move()
            return
        if item.source_missing:
            logger.warning(
                "Skipping move from %s to %s because recorder segment sidecar %s "
                "is missing and %s is not available yet",
                src,
                dst,
                item.src,
                item.dst,
            )
            return
        logger.error(
            "Skipping move from %s to %s because recorder segment sidecar %s "
            "could not be copied to %s: %s",
            src,
            dst,
            item.src,
            item.dst,
            item.error,
        )

    if (
        _curr_tier_category == TIER_CATEGORY_RECORDER
        and _curr_tier_subcategory == TIER_SUBCATEGORY_SEGMENTS
        and os.path.basename(src) != "init.mp4"
    ):
        init_filename = (
            f"init-{file_meta.hls_init_hash}.mp4"
            if file_meta.hls_init_hash
            else "init.mp4"
        )
        src_init = os.path.join(os.path.dirname(src), init_filename)
        dst_init = os.path.join(os.path.dirname(dst), init_filename)
        if src_init != dst_init:
            # HLS init files are intentionally ignored by storage watchers and
            # have no Files rows, but fMP4 HLS needs the matching init beside
            # any segment directory the playlist references. Copy it before
            # committing the segment move so a later-tier segment never becomes
            # visible without its sidecar.
            storage.tier_check_worker_send_command(
                DataItemCopyFile(
                    cmd="copy_file",
                    src=src_init,
                    dst=dst_init,
                ),
                callback=_copy_init_callback,
            )
            return

    _send_segment_move()


def force_move_files(
    vis: Viseron,
    storage: Storage,
    get_session: Callable[..., Session],
    category: str,
    subcategory: str,
    tier_id: int,
    camera_identifier: str,
    curr_tier: Tier,
    next_tier: Tier | None,
    logger: logging.Logger,
) -> None:
    """Get and move/delete all files in tier."""
    with get_session(expire_on_commit=False) as session:
        stmt = (
            select(FileLocations.path, FileLocations.tier_path)
            .join(Files, Files.id == FileLocations.file_id)
            .where(Files.camera_identifier == camera_identifier)
            .where(FileLocations.tier_id == tier_id)
            .where(FileLocations.state == FileLocationState.AVAILABLE.value)
            .where(Files.category == category)
            .where(Files.subcategory == subcategory)
        )
        result = session.execute(stmt).all()
        for file in result:
            handle_file(
                vis,
                get_session,
                storage,
                camera_identifier,
                tier_id,
                category,
                subcategory,
                curr_tier,
                next_tier,
                file[0],
                file[1],
                logger,
            )
        session.commit()


def add_file_handler(
    vis: Viseron,
    webserver: Webserver,
    path: str,
    pattern: str,
    camera: AbstractCamera | FailedCamera,
    category: str,
    subcategory: str,
) -> None:
    """Add file handler to webserver."""
    # We have to import this here to avoid circular imports
    # pylint: disable-next=import-outside-toplevel
    from viseron.components.webserver.tiered_file_handler import (  # noqa: PLC0415
        TieredFileHandler,
    )

    webserver.application.add_handlers(
        r".*",
        [
            (
                (rf"/files{pattern}"),
                TieredFileHandler,
                {
                    "path": path,
                    "vis": vis,
                    "camera_identifier": camera.identifier,
                    "failed": bool(isinstance(camera, FailedCamera)),
                    "category": category,
                    "subcategory": subcategory,
                },
            )
        ],
    )


class TimelapseTierHandler(RetentionTierHandler):
    """Handle timelapse files."""

    _tier: TimelapseTier

    def initialize(self) -> None:
        """Initialize timelapse tier."""
        super().initialize()

        self._path = get_timelapse_path(self._tier, self._camera)
        self._interval = self._tier.interval.to_timedelta()
        self.add_file_handler(self._path, rf"{self._path}/(.*.jpg$)")

    def _on_created(self, event: FileCreatedEvent) -> None:
        """Handle file creation with interval-based cleanup."""
        src_path = _src_path(event)

        super()._on_created(event)

        # If no interval is set, keep all files
        if not self._interval:
            return

        # Check if there's already a file within the interval
        try:
            with self._storage.get_session() as session:
                current_file_stmt = select(Files.orig_ctime).where(
                    Files.id
                    == select(FileLocations.file_id)
                    .where(FileLocations.path == event.src_path)
                    .scalar_subquery()
                )
                current_file_result = session.execute(current_file_stmt).scalar_one()
                current_file_datetime = current_file_result

                interval_start = current_file_datetime - self._interval
                interval_end = current_file_datetime

                stmt = (
                    select(Files)
                    .join(FileLocations, FileLocations.file_id == Files.id)
                    .where(
                        FileLocations.tier_id == self._tier_id,
                        FileLocations.state == FileLocationState.AVAILABLE.value,
                        Files.camera_identifier == self._camera.identifier,
                        Files.category == self._category,
                        Files.subcategory == self._subcategory,
                        Files.id
                        != select(FileLocations.file_id)
                        .where(FileLocations.path == event.src_path)
                        .scalar_subquery(),
                        Files.orig_ctime >= interval_start,
                        Files.orig_ctime <= interval_end,
                    )
                )

                result = session.execute(stmt).scalars().all()

                if result:
                    self._logger.debug(
                        f"File within interval already exists, removing current file: "
                        f"{src_path}"
                    )
                    delete_file(self._storage, src_path)

                    delete_file_location_by_path(session, event.src_path)
                    session.commit()

        except Exception as e:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.error(f"Error during timelapse interval cleanup: {e}")
