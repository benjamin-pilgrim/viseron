"""Tier handler."""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import os
import re
import stat
import threading
import time
from datetime import timedelta
from functools import partial
from queue import Queue
from threading import Timer
from typing import TYPE_CHECKING, Any, Literal, cast

from apscheduler.jobstores.base import JobLookupError
from sqlalchemy import Delete, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
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

from viseron.components.storage.asset_moves import (
    asset_locks,
    asset_move_pending,
    destination_pending,
    is_recording_asset,
    reserve as reserve_asset_move,
)
from viseron.components.storage.const import (
    COMPONENT,
    CONFIG_CHECK_INTERVAL,
    CONFIG_CONTINUOUS,
    CONFIG_DAYS,
    CONFIG_DRAIN,
    CONFIG_EVENTS,
    CONFIG_HOURS,
    CONFIG_INTERVAL,
    CONFIG_MAX_AGE,
    CONFIG_MAX_SIZE,
    CONFIG_MIN_AGE,
    CONFIG_MIN_SIZE,
    CONFIG_MINUTES,
    CONFIG_MOVE_ON_SHUTDOWN,
    CONFIG_PATH,
    CONFIG_POLL,
    CONFIG_POLL_INTERVAL,
    CONFIG_SECONDS,
    CONFIG_TIER_MARKERS,
    DEFAULT_POLL_INTERVAL,
    EVENT_CHECK_TIER,
    EVENT_FILE_CREATED,
    EVENT_FILE_DELETED,
    LATEST_SNAPSHOT_FILENAME,
    TIER_CATEGORY_RECORDER,
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
from viseron.components.storage.dispatch_fence import create_token
from viseron.components.storage.io_retry import (
    FilesystemRetry,
    PublicationRetry,
    StorageRetryLater,
)
from viseron.components.storage.lost_source_recovery import (
    LostSourceRecovery,
    reconcile_asset as reconcile_lost_asset,
    reconcile_segment as reconcile_lost_segment,
)
from viseron.components.storage.models import (
    FILE_KEY_SEQUENCE,
    Files,
    FilesMeta,
    Motion,
    MotionContours,
    Objects,
    PendingMove,
    PendingSegmentMove,
    PostProcessorResults,
    Recordings,
)
from viseron.components.storage.pending_journal import (
    PREFIX,
    assign_destination_id,
    assign_file_key,
    marker_for_path,
    protected_by_pending,
    publish_or_verify,
    release_indexed_source,
    source_run_available,
    sync_dir,
)
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    empty_recording_run,
    is_run_directory,
    recording_camera_paths,
)
from viseron.components.storage.segment_moves import (
    acknowledge as acknowledge_segment_move,
    finish_completed as finish_completed_segment_move,
    protects as segment_move_pending,
)
from viseron.components.storage.source_move_intent import (
    is_source_move_marker,
    marker_for_source,
    source_move_pending,
)
from viseron.components.storage.startup_reconciliation import missing_volatile_path
from viseron.components.storage.storage_subprocess import (
    DataItem,
    DataItemDeleteFile,
    DataItemMoveFile,
    StorageCallbackCapacityError,
)
from viseron.components.storage.util import (
    EventFileCreated,
    EventFileDeleted,
    calculate_age,
    calculate_bytes,
    get_event_clips_path,
    get_segments_path,
    get_thumbnails_path,
    get_timelapse_path,
    is_tier_marker,
    is_tier_move_temp_file,
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
    from collections.abc import Callable, Sequence

    import numpy as np
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.dml import ReturningDelete

    from viseron import Viseron
    from viseron.components.storage import Storage
    from viseron.components.webserver import Webserver
    from viseron.domains.camera import AbstractCamera


JOURNAL_BATCH_LIMIT = 1024
JOURNAL_STALE_SECONDS = 300
JOURNAL_TIMER_SLOP = 0.05


class SourceJournalMismatch(ValueError):
    """The source database identity no longer matches the pending journal."""


def _src_path(event: FileSystemEvent) -> str:
    """Return the source path of a watchdog event as a str.

    watchdog types ``src_path`` as ``bytes | str`` because a watched path may be
    given as bytes. Viseron always schedules str paths, so it is always a str.
    """
    return cast("str", event.src_path)


class _EventRetentionBatch:
    """Release one event batch only after all source changes have committed."""

    def __init__(self, paths: set[str], on_complete: Callable[[], None]) -> None:
        self._paths = paths.copy()
        self._on_complete = on_complete
        self._lock = threading.Lock()

    def completed(self, path: str) -> None:
        """Coalesce repeated and concurrently delivered commit acknowledgements."""
        with self._lock:
            if path not in self._paths:
                return
            self._paths.remove(path)
            finished = not self._paths
        if finished:
            self._on_complete()


class TierHandler(FileSystemEventHandler):
    """Moves files up configured tiers."""

    def __init__(
        self,
        vis: Viseron,
        camera: AbstractCamera,
        tier_id: int,
        category: str,
        subcategory: str,
        tier: dict[str, Any],
        next_tier: dict[str, Any] | None,
    ) -> None:
        self._logger = logging.getLogger(
            f"{__name__}.{camera.identifier}.tier_{tier_id}.{category}.{subcategory}"
        )
        super().__init__()

        self._vis = vis
        self._storage = vis.data[COMPONENT]
        self._webserver = self._vis.data[WEBSERVER_COMPONENT]
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
        self._tier_check_force_pending = False
        self._tier_check_retry_timer: threading.Timer | None = None
        self._tier_check_stopping = False
        self._deletion_retry_stop = threading.Event()

        self._publication_io_retry = FilesystemRetry(self._logger)
        self._journal_retry_lock = threading.Lock()
        self._journal_retry_timer: threading.Timer | None = None
        self._journal_retry_paths: set[str] = set()
        self._journal_retry_rescan = False
        self._journal_retry_stopping = False
        self._journal_replay_after: str | None = None
        self._journal_reported_issues: set[tuple[str, str]] = set()
        self._pending_updates: dict[str, Timer] = {}
        self._event_queue: Queue[FileSystemEvent | None] = Queue()
        self._event_thread = RestartableThread(
            target=self._process_events,
            daemon=True,
            name=f"tier_handler_{camera.identifier}",
            stage=VISERON_SIGNAL_LAST_WRITE,
        )
        self._event_thread.start()

        self._throttle_period = timedelta(
            days=tier[CONFIG_CHECK_INTERVAL].get(CONFIG_DAYS, 0),
            hours=tier[CONFIG_CHECK_INTERVAL].get(CONFIG_HOURS, 0),
            minutes=tier[CONFIG_CHECK_INTERVAL].get(CONFIG_MINUTES, 0),
            seconds=tier[CONFIG_CHECK_INTERVAL].get(CONFIG_SECONDS, 0),
        )
        self._time_of_last_call = utcnow() - self._throttle_period

        self._logger.debug("Tier %s monitoring path: %s", tier_id, self._path)
        self._observer_lock = threading.Lock()
        self._observer_stopping = False
        self._observer = (
            self._start_observer() if self._storage.tier_available(self._path) else None
        )
        self._replay_pending_publications()
        self._source_intent_job = None
        self._source_intent_cursor = {}
        self._source_scan_lock = threading.Lock()
        self._hold_retry_lock = threading.Lock()
        self._hold_retry_token = create_token()
        self._hold_retry_active = set()
        self._hold_retry_stopping = False
        self._hold_retry_since = 0.0
        self._hold_retry_warned = False
        self._asset_move_cursor = 0
        if self._category == TIER_CATEGORY_RECORDER and self._subcategory in (
            TIER_SUBCATEGORY_THUMBNAILS,
            TIER_SUBCATEGORY_EVENT_CLIPS,
        ):
            self._source_intent_job = self._vis.background_scheduler.add_job(
                self._reconcile_asset_moves,
                "interval",
                seconds=30,
                next_run_time=utcnow(),
                max_instances=1,
                coalesce=True,
            )
        elif self._subcategory == TIER_SUBCATEGORY_SEGMENTS:
            self._source_intent_job = self._vis.background_scheduler.add_job(
                self._reconcile_source_intents,
                "interval",
                seconds=30,
                next_run_time=utcnow(),
                max_instances=1,
                coalesce=True,
            )

    def _reconcile_asset_moves(self) -> None:
        """Bound retries and keep unavailable asset storage off scheduler threads."""
        if self._tier_check_stopping or not self._source_scan_lock.acquire(False):
            return

        def replay() -> None:
            try:
                self._replay_asset_moves()
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Could not replay recording asset moves")
            finally:
                self._source_scan_lock.release()

        try:
            threading.Thread(
                target=replay, daemon=True, name="asset-move-replay"
            ).start()
        except Exception:
            self._source_scan_lock.release()
            raise

    def _replay_asset_moves(self) -> None:
        """Resume a rotating batch without retaining ORM state across dispatches."""
        with self._storage.get_session() as session:
            rows = session.execute(
                select(
                    Files.id,
                    Files.path,
                    Files.move_target["path"].astext,
                    Files.move_target["tier_path"].astext,
                )
                .where(
                    Files.camera_identifier == self._camera.identifier,
                    Files.category == self._category,
                    Files.subcategory == self._subcategory,
                    Files.tier_id == self._tier_id,
                    Files.tier_path == self._tier[CONFIG_PATH],
                    Files.move_target.is_not(None),
                    Files.id > self._asset_move_cursor,
                )
                .order_by(Files.id)
                .limit(20)
            ).all()

        # move_file commits and closes a thread-scoped session. Materialize only
        # scalar work items and release the query session before dispatching; an
        # ORM batch would be expired/detached by the first move's transaction.
        if not rows:
            self._asset_move_cursor = 0
            return
        for file_id, source, destination, tier_path in rows:
            self._asset_move_cursor = file_id
            if self._tier_check_stopping:
                return
            if not (
                self._storage.tier_available(source)
                and self._storage.tier_available(destination)
            ):
                continue
            if self._recover_lost_source(
                source,
                destination,
                partial(
                    self._recover_asset_source, file_id, source, destination, tier_path
                ),
                self._tier[CONFIG_PATH],
            ):
                continue
            try:
                move_file(
                    self._vis,
                    self._storage,
                    self._storage.get_session,
                    self._camera.identifier,
                    self._tier_id,
                    self._category,
                    self._subcategory,
                    source,
                    destination,
                    tier_path,
                    self._logger,
                )
            except (OSError, SQLAlchemyError, StorageCallbackCapacityError):
                self._logger.warning("Deferred asset move for %s", source)

    def _recover_asset_source(self, file_id, source, destination, tier_path):
        """Inspect lost assets only after ordinary replay's local source check."""
        if not missing_volatile_path(source, self._tier[CONFIG_PATH]):
            return None
        tiers = self._storage.camera_tier_handlers[self._camera.identifier][
            self._category
        ]
        target = next(
            (
                group[self._subcategory]
                for group in tiers[self._tier_id + 1 :]
                if self._subcategory in group
                and group[self._subcategory].tier[CONFIG_PATH] == tier_path
                and os.path.dirname(destination)
                == os.path.abspath(group[self._subcategory]._path)
            ),
            None,
        )
        if target is None:
            return "deferred"
        return reconcile_lost_asset(
            self._storage.get_session,
            file_id,
            source_tier=self._tier[CONFIG_PATH],
            source_root=self._path,
            destination_tier=target.tier[CONFIG_PATH],
            destination_root=target._path,
            destination_id=target._tier_id,
            available=self._storage.tier_available,
            markers=self._storage.config.get(CONFIG_TIER_MARKERS, {}),
        )

    def _recover_segment_source(self, move, destination):
        """Keep mount probes and uncertain evidence inside shared retry gates."""
        if move.metadata_record["source_db"]["tier_id"] != self._tier_id:
            return "deferred"
        if not missing_volatile_path(
            move.source, move.metadata_record["source_db"]["tier_path"]
        ):
            return None
        return reconcile_lost_segment(
            self._storage.get_session,
            move,
            source_tier=self._tier[CONFIG_PATH],
            source_root=self._path,
            destination_tier=destination.tier[CONFIG_PATH],
            destination_root=destination._path,
            destination_id=destination._tier_id,
            available=self._storage.tier_available,
            markers=self._storage.config.get(CONFIG_TIER_MARKERS, {}),
        )

    def _recover_lost_source(self, source, destination, operation, volatile_tier):
        """Bound unavailable-filesystem probes and unchanged recovery failures."""
        if not hasattr(self, "_lost_source_recovery"):
            self._lost_source_recovery = LostSourceRecovery(self._logger)
        return self._lost_source_recovery.attempt(
            source,
            destination,
            operation,
            probe=partial(missing_volatile_path, source, volatile_tier),
        )

    def _asset_destination_pending(self, path: str) -> bool:
        with self._storage.get_session() as session:
            return destination_pending(session, path)

    def _reconcile_source_intents(self):
        """Run at most one isolated scan without occupying the scheduler."""
        with self._hold_retry_lock:
            if self._hold_retry_stopping:
                return
            if (
                self._hold_retry_active
                and not self._hold_retry_warned
                and time.monotonic() - self._hold_retry_since > JOURNAL_STALE_SECONDS
            ):
                self._hold_retry_warned = True
                self._logger.warning(
                    "Held move retry is still active; a storage worker "
                    "or callback may be stalled"
                )
            if not self._source_scan_lock.acquire(blocking=False):
                return

        def scan():
            try:
                self._replay_segment_moves()
            finally:
                self._source_scan_lock.release()

        try:
            threading.Thread(
                target=scan, daemon=True, name="segment-move-replay"
            ).start()
        except Exception:
            self._source_scan_lock.release()
            raise

    def _replay_segment_moves(self):
        """Retry a rotating bounded batch of indexed moves, never walk archives."""
        try:
            with self._hold_retry_lock:
                active = set(self._hold_retry_active)
            if active:
                with self._storage.get_session() as session:
                    unfinished = set(
                        session.execute(
                            select(PendingSegmentMove.source).where(
                                PendingSegmentMove.source.in_(active)
                            )
                        ).scalars()
                    )
                with self._hold_retry_lock:
                    self._hold_retry_active.difference_update(active - unfinished)
            after = self._source_intent_cursor.get("after_id", 0)
            with self._storage.get_session() as session:
                moves = (
                    session.execute(
                        select(PendingSegmentMove)
                        .where(
                            PendingSegmentMove.camera_identifier
                            == self._camera.identifier,
                            PendingSegmentMove.source_root
                            == os.path.abspath(self._path),
                            PendingSegmentMove.id > after,
                        )
                        .order_by(PendingSegmentMove.id)
                        .limit(20)
                    )
                    .scalars()
                    .all()
                )
            if not moves:
                self._source_intent_cursor["after_id"] = 0
                return
            for move in moves:
                self._source_intent_cursor["after_id"] = move.id
                if self._hold_retry_stopping:
                    return
                destination = self._pending_segment_destination(move)
                if destination is None:
                    self._logger.warning(
                        "Pending segment destination is not configured: %s",
                        move.destination,
                    )
                    continue
                if self._recover_lost_source(
                    move.source,
                    move.destination,
                    partial(self._recover_segment_source, move, destination),
                    move.metadata_record["source_db"]["tier_path"],
                ):
                    continue
                try:
                    with camera_lock("/tmp/viseron-run-locks", move.destination_root):
                        if self._storage.tier_available(
                            move.destination
                        ) and os.path.lexists(marker_for_path(move.destination)):
                            # Replay publication even when its worker callback was lost.
                            destination._schedule_journal_retry(move.destination)
                            continue
                except BlockingIOError:
                    continue
                with self._storage.get_session() as session:
                    try:
                        if finish_completed_segment_move(
                            session, move, self._storage.tier_available
                        ):
                            with self._storage.pending_moves_lock:
                                pending = self._storage.pending_moves.get(move.source)
                            destination._complete_pending_move(
                                pending,
                                self._storage.pending_moves,
                                {"source_snapshot": {"source": move.source}},
                                move.destination,
                            )
                            continue
                    except BlockingIOError:
                        continue
                self._retry_held_move(
                    str(marker_for_source(move.source)),
                    {
                        **move.metadata_record,
                        "source": move.source,
                        "destination": move.destination,
                        "size": move.size,
                        "pending_move_id": move.id,
                        "destination_tier_id": move.destination_tier_id,
                        "destination_tier_path": move.destination_tier_path,
                    },
                )
        except Exception:
            self._logger.exception("Pending segment move replay failed; retrying")

    def _pending_segment_destination(self, move):
        """Resolve a queued path only against configured later camera tiers."""
        tiers = self._storage.camera_tier_handlers[self._camera.identifier][
            self._category
        ]
        return next(
            (
                tier[self._subcategory]
                for tier in tiers[self._tier_id + 1 :]
                if self._subcategory in tier
                and os.path.abspath(tier[self._subcategory]._path)
                == move.destination_root
            ),
            None,
        )

    def _retry_held_move(self, marker, record):
        """Isolate potentially stalled tier probes to one daemon per camera."""
        source = record.get("source")
        if not isinstance(source, str):
            return
        with self._hold_retry_lock:
            if self._hold_retry_stopping:
                return
            if self._hold_retry_active:
                if (
                    not self._hold_retry_warned
                    and time.monotonic() - self._hold_retry_since
                    > JOURNAL_STALE_SECONDS
                ):
                    self._hold_retry_warned = True
                    self._logger.warning(
                        "Held move retry is still active; a storage worker "
                        "or callback may be stalled"
                    )
                return
            self._hold_retry_active.add(source)
            self._hold_retry_since = time.monotonic()
            self._hold_retry_warned = False

        def run():
            queued = False
            try:
                queued = self._retry_held_move_worker(marker, record)
            except Exception:
                self._logger.exception("Could not retry held move %s", marker)
            finally:
                if not queued:
                    with self._hold_retry_lock:
                        self._hold_retry_active.discard(source)

        try:
            threading.Thread(target=run, daemon=True, name="held-move-retry").start()
        except Exception:
            with self._hold_retry_lock:
                self._hold_retry_active.discard(source)
            raise

    def _hold_retry_stopped(self):
        with self._hold_retry_lock:
            return self._hold_retry_stopping

    def _retry_held_move_worker(self, marker, record):
        """Inspect and replay an incomplete source hold in an isolated worker."""
        source = record.get("source")
        destination = record.get("destination")
        source_db = record.get("source_db")
        if not isinstance(source, str) or not isinstance(destination, str):
            return
        if not isinstance(source_db, dict) or not os.path.isabs(source):
            return
        source_dir = os.path.dirname(source)
        if (
            os.path.dirname(source_dir) != os.path.abspath(self._path)
            or not is_run_directory(os.path.basename(source_dir))
            or marker != str(marker_for_source(source))
            or not source.endswith(".m4s")
        ):
            return
        tiers = self._storage.camera_tier_handlers[self._camera.identifier][
            self._category
        ]
        next_tier = next(
            (
                tier[self._subcategory]
                for tier in tiers[self._tier_id + 1 :]
                if self._subcategory in tier
                and os.path.abspath(tier[self._subcategory]._path)
                == os.path.dirname(os.path.dirname(destination))
            ),
            None,
        )
        if (
            next_tier is None
            or os.path.basename(os.path.dirname(destination))
            != os.path.basename(source_dir)
            or os.path.basename(destination) != os.path.basename(source)
        ):
            return
        try:
            with camera_lock(
                "/tmp/viseron-run-locks", os.path.abspath(next_tier._path)
            ):
                if (
                    not self._storage.tier_available(destination)
                    or self._hold_retry_stopped()
                ):
                    return False
                journal_exists = os.path.lexists(marker_for_path(destination))
                if self._hold_retry_stopped():
                    return False
                if journal_exists:
                    # Destination owns this replay and checks its own stop
                    # state. Never hold our stop lock through timer creation.
                    if not self._hold_retry_stopped():
                        next_tier._schedule_journal_retry(destination)
                    return False
                destination_exists = os.path.lexists(destination)
                if self._hold_retry_stopped():
                    return False
                if destination_exists:
                    self._logger.warning(
                        "Held move destination exists without a journal: %s",
                        destination,
                    )
                    return False
        except BlockingIOError:
            return False
        try:
            with self._storage.get_session() as session:
                row = session.execute(
                    select(Files).where(Files.path == source)
                ).scalar_one_or_none()
                if self._hold_retry_stopped():
                    return False
                if (
                    row is None
                    or row.id != source_db.get("id")
                    or row.tier_id != source_db.get("tier_id")
                    or row.tier_path != source_db.get("tier_path")
                    or row.camera_identifier != self._camera.identifier
                    or row.category != self._category
                    or row.subcategory != self._subcategory
                    or row.file_key != record.get("file_key")
                    or row.size != record.get("size")
                ):
                    raise ValueError("source row changed before held move retry")  # noqa: TRY301
                metadata = {
                    "orig_ctime": row.orig_ctime.isoformat(),
                    "duration": row.duration,
                    "file_key": row.file_key,
                    "source_db": source_db,
                    **(
                        {"pending_move_id": record["pending_move_id"]}
                        if "pending_move_id" in record
                        else {}
                    ),
                    **{
                        key: record[key]
                        for key in ("destination_tier_id", "destination_tier_path")
                        if key in record
                    },
                }

            def done(item):
                with self._hold_retry_lock:
                    self._hold_retry_active.discard(source)
                if item.error:
                    self._logger.warning(
                        "Held move retry failed for %s: %s", source, item.error
                    )
                elif (item.publication_meta or {}).get("publication_ready"):
                    # Recovery uses a different callback from initial promotion.
                    # A durable copy still needs prompt destination indexing,
                    # including when final source work was deferred.
                    if not self._hold_retry_stopped():
                        next_tier._schedule_journal_retry(destination)

            with self._hold_retry_lock:
                if self._hold_retry_stopping:
                    return False
                token = self._hold_retry_token
            # Manager IPC can block. It runs in the bounded daemon, never under
            # the stop lock; a late item is rejected by the child token check.
            self._storage.tier_check_worker_send_command(
                DataItemMoveFile(
                    cmd="move_file",
                    src=source,
                    dst=destination,
                    camera_identifier=self._camera.identifier,
                    publication_meta=metadata,
                    dispatch_token=token,
                ),
                callback=done,
            )
            return True
        except Exception:
            self._logger.exception("Could not retry held move %s", marker)
            return False

    def _schedule_journal_retry(
        self, path: str | None = None, *, delay: float = 1.0
    ) -> None:
        """Keep one timer and a bounded set of pending paths per handler."""
        with self._journal_retry_lock:
            if self._journal_retry_stopping:
                return
            if path is None or len(self._journal_retry_paths) >= JOURNAL_BATCH_LIMIT:
                self._journal_retry_rescan = True
            else:
                self._journal_retry_paths.add(path)
            self._arm_journal_retry_locked(delay)

    def _arm_journal_retry_locked(self, delay: float) -> None:
        """Wake fresh work promptly; canceled callbacks cannot steal its timer."""
        due = time.monotonic() + delay
        if self._journal_retry_timer is not None:
            # Shared cooldown calculations can differ by microseconds.
            if getattr(self, "_journal_retry_due", due) <= due + JOURNAL_TIMER_SLOP:
                return
            self._journal_retry_timer.cancel()
        generation = getattr(self, "_journal_retry_generation", 0) + 1
        self._journal_retry_generation = generation
        self._journal_retry_due = due
        timer = threading.Timer(
            delay, lambda: self._retry_journal_publications(generation)
        )
        timer.daemon = True
        self._journal_retry_timer = timer
        timer.start()

    def _retry_journal_publications(self, generation=None) -> None:
        with self._journal_retry_lock:
            if generation is not None and generation != self._journal_retry_generation:
                return
            self._journal_retry_timer = None
            if self._journal_retry_stopping:
                return
            paths = self._journal_retry_paths
            self._journal_retry_paths = set()
            rescan = self._journal_retry_rescan
            self._journal_retry_rescan = False
            for path in paths:
                if self._event_queue.qsize() >= JOURNAL_BATCH_LIMIT:
                    rescan = True
                    break
                self._event_queue.put(FileCreatedEvent(path))
        if rescan:
            self._replay_pending_publications()

    def _replay_pending_publications(self) -> None:
        """Queue a bounded, rotating batch after startup or tier recovery."""
        if not self._storage.tier_available(self._path):
            self._schedule_journal_retry()
            return
        if getattr(self, "_tier_id", 0) > 0:
            # Later tiers contain the archive. Only query known outstanding moves.
            with self._storage.get_session() as session:
                moves = (
                    session.execute(
                        select(PendingSegmentMove)
                        .where(
                            PendingSegmentMove.camera_identifier
                            == self._camera.identifier,
                            PendingSegmentMove.destination_root
                            == os.path.abspath(self._path),
                            PendingSegmentMove.id
                            > (getattr(self, "_journal_replay_after", None) or 0),
                        )
                        .order_by(PendingSegmentMove.id)
                        .limit(JOURNAL_BATCH_LIMIT)
                    )
                    .scalars()
                    .all()
                )
            for move in moves:
                journal_exists = os.path.lexists(marker_for_path(move.destination))
                with self._journal_retry_lock:
                    if self._journal_retry_stopping:
                        return
                    if self._event_queue.qsize() >= JOURNAL_BATCH_LIMIT:
                        self._schedule_journal_retry_unlocked()
                        return
                    if journal_exists:
                        self._event_queue.put(FileCreatedEvent(move.destination))
                    self._journal_replay_after = move.id
            if len(moves) < JOURNAL_BATCH_LIMIT:
                self._journal_replay_after = None
            else:
                self._schedule_journal_retry()
            return
        queued = 0
        after = getattr(self, "_journal_replay_after", None)
        skipped = False
        for root, dirs, files in os.walk(self._path):
            dirs.sort()
            if not is_run_directory(os.path.basename(root)):
                continue
            for name in sorted(files):
                if not name.startswith(PREFIX) or not name.endswith(".m4s.json"):
                    continue
                target = os.path.join(root, name[len(PREFIX) : -len(".json")])
                marker = os.path.join(root, name)
                try:
                    with camera_lock(
                        "/tmp/viseron-run-locks", os.path.abspath(os.path.dirname(root))
                    ):
                        with open(marker, encoding="utf-8") as source:
                            record = json.load(source)
                        temp_name = record.get("temp_name", "")
                        valid_temp = (
                            isinstance(temp_name, str)
                            and temp_name.startswith(".viseron-tier-move-")
                            and os.path.basename(temp_name) == temp_name
                        )
                        if not os.path.lexists(target) and not (
                            valid_temp
                            and os.path.lexists(os.path.join(root, temp_name))
                        ):
                            issue = "media is missing"
                        elif (
                            time.time() - os.stat(marker).st_mtime
                            > JOURNAL_STALE_SECONDS
                        ):
                            issue = "pending for over five minutes"
                        else:
                            issue = None
                        if issue:
                            key = (marker, issue)
                            with self._journal_retry_lock:
                                if key not in self._journal_reported_issues:
                                    if (
                                        len(self._journal_reported_issues)
                                        < JOURNAL_BATCH_LIMIT
                                    ):
                                        self._journal_reported_issues.add(key)
                                        self._logger.warning(
                                            "Journal %s: %s", marker, issue
                                        )
                except (OSError, ValueError, TypeError):
                    # Replay remains responsible for retaining/reporting bad markers.
                    pass
                if after is not None and target <= after:
                    skipped = True
                    continue
                with self._journal_retry_lock:
                    if self._journal_retry_stopping:
                        return
                    if self._event_queue.qsize() >= JOURNAL_BATCH_LIMIT:
                        self._schedule_journal_retry_unlocked()
                        return
                    self._event_queue.put(FileCreatedEvent(target))
                    self._journal_replay_after = target
                queued += 1
                if queued >= JOURNAL_BATCH_LIMIT:
                    self._schedule_journal_retry()
                    return
        self._journal_replay_after = None
        if skipped:
            self._schedule_journal_retry()

    def _schedule_journal_retry_unlocked(self) -> None:
        """Request a rescan while the journal retry lock is already held."""
        self._journal_retry_rescan = True
        self._arm_journal_retry_locked(1.0)

    def _start_observer(self):
        """Create a fresh observer for the current directory mount."""
        os.makedirs(self._path, exist_ok=True)
        observer = (
            PollingObserverVFS(
                stat=os.stat,
                listdir=os.scandir,
                polling_interval=self._tier.get(
                    CONFIG_POLL_INTERVAL, DEFAULT_POLL_INTERVAL
                ),
            )
            if self._tier[CONFIG_POLL]
            else Observer()
        )
        observer.schedule(
            self,
            self._path,
            recursive=True,
        )
        observer.start()
        return observer

    @staticmethod
    def _dispose_observer(observer) -> None:
        """Stop an old observer without blocking recovery on a stalled mount."""
        observer.stop()
        observer.join()

    def restart_for_tier(self, root: str) -> bool:
        """Reattach this watcher if its directory is under a recovered tier."""
        if os.path.commonpath((root, os.path.abspath(self._path))) != root:
            return True
        with self._observer_lock:
            if self._observer_stopping:
                return True
            if not self._storage.tier_available(self._path):
                return False
            try:
                observer = self._start_observer()
            except OSError:
                self._logger.exception("Could not restart watcher for %s", self._path)
                return False
            previous_observer = self._observer
            self._observer = observer
        self._replay_pending_publications()
        if previous_observer is not None:
            threading.Thread(
                target=self._dispose_observer,
                args=(previous_observer,),
                name="tier-observer-dispose",
                daemon=True,
            ).start()
        return True

    @property
    def tier(self) -> dict[str, Any]:
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

    def initialize(self) -> None:
        """Tier handler specific initialization."""
        self._path = os.path.join(
            self._tier[CONFIG_PATH],
            self._category,
            self._subcategory,
            self._camera.identifier,
        )

        self._max_bytes = calculate_bytes(self._tier[CONFIG_MAX_SIZE])
        self._min_bytes = calculate_bytes(self._tier[CONFIG_MIN_SIZE])
        self._max_age = calculate_age(self._tier[CONFIG_MAX_AGE])
        self._min_age = calculate_age(self._tier[CONFIG_MIN_AGE])

        if self._next_tier is None and not self._max_age and not self._max_bytes:
            self._logger.warning(
                "Last tier '%s' has no max_age or max_size configured; "
                "files on this tier will accumulate indefinitely.",
                self._tier[CONFIG_PATH],
            )

    def _create_dataitem(
        self,
    ) -> DataItem:
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
            drain=self._tier[CONFIG_DRAIN],
        )

    def _check_tier_event_handler(self, _event: Event) -> None:
        """Handle check tier event."""
        self._storage.cleanup_manager.run_job(CleanupJobNames.ORPHANED_FILES)
        self.check_tier(force=True)

    def check_tier(self, *, force: bool = False) -> bool:
        """Check if file should be moved to next tier."""
        with self._check_tier_lock:
            if self._tier_check_stopping:
                return False
            if self._tier_check_in_progress:
                if force:
                    self._tier_check_force_pending = True
                    return True
                return False
            if force:
                self._tier_check_force_pending = False
                if self._tier_check_retry_timer is not None:
                    self._tier_check_retry_timer.cancel()
                    self._tier_check_retry_timer = None
            # Throttling is also done in the worker process, but we do the same here
            # in order to not spam the workers with unneeded requests.
            now = utcnow()
            time_since_last_call = now - self._time_of_last_call
            if not force and time_since_last_call < self._throttle_period:
                return False
            # Claim the in-flight slot before sending. The reply can take tens of
            # seconds, and every file event in that window would otherwise pass
            # the guard above and send a duplicate request, each one registering
            # a callback in the subprocess worker. The subprocess dedupes those
            # duplicates, so only one reply comes back and the rest of the
            # callbacks would leak. Marking in-flight here collapses the flood at
            # the source. Cleared in on_check_tier_result.
            self._tier_check_in_progress = True

        try:
            item = self._create_dataitem()
            if force:
                item.throttle_period = timedelta(0)
            self._storage.tier_check_worker_send_command(
                item, self.on_check_tier_result
            )
            return True
        except StorageCallbackCapacityError:
            with self._check_tier_lock:
                self._tier_check_in_progress = False
                if force or self._tier_check_force_pending:
                    self._schedule_forced_tier_retry_locked()
            self._logger.warning("Storage worker busy; tier check deferred")
            return False
        except Exception:
            # Release the slot if the command never made it out, otherwise this
            # tier would never be checked again.
            with self._check_tier_lock:
                self._tier_check_in_progress = False
                if force or self._tier_check_force_pending:
                    self._schedule_forced_tier_retry_locked()
            raise

    def _schedule_forced_tier_retry_locked(self, delay: float = 1.0) -> None:
        """Retry failed submissions without flooding the worker."""
        self._tier_check_force_pending = True
        if self._tier_check_stopping or self._tier_check_retry_timer is not None:
            return
        timer = threading.Timer(delay, self._retry_forced_tier_check)
        timer.daemon = True
        self._tier_check_retry_timer = timer
        timer.start()

    def _retry_forced_tier_check(self) -> None:
        with self._check_tier_lock:
            self._tier_check_retry_timer = None
            if self._tier_check_stopping or not self._tier_check_force_pending:
                return
            self._tier_check_force_pending = False
        try:
            self.check_tier(force=True)
        except Exception:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.exception("Failed to retry forced tier check")

    def _check_tier(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        retention_recording_ids: tuple[int, ...] | None = None,
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
            if self._tier_check_force_pending:
                self._schedule_forced_tier_retry_locked()

    def on_check_tier_result(self, item: DataItem) -> None:
        """Handle the result of the check tier command."""
        if item.error:
            self._logger.error("Error in tier check process: %s", item.error)

        # No payload to process: the subprocess throttled the job, a check was
        # already running for this camera, or the job was superseded in the dedup
        # queue. Release the slot immediately so the next event can send again.
        if item.data is None or len(item.data) == 0:
            if item.deferred and item.throttle_period == timedelta(0):
                with self._check_tier_lock:
                    self._tier_check_force_pending = True
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
                    retention_recording_ids=item.retention_recording_ids,
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
            if not self._storage.tier_available(_src_path(event)):
                self._logger.debug("Ignoring event on unavailable tier: %s", event)
                continue
            if isinstance(event, FileDeletedEvent):
                try:
                    os.stat(_src_path(event))
                except FileNotFoundError:
                    pass
                except OSError:
                    self._logger.debug("Cannot confirm deletion: %s", event)
                    continue
                else:
                    # Events queued before a remount may describe the old mount.
                    self._logger.debug("Ignoring stale deletion event: %s", event)
                    continue
                self._on_deleted(event)
            elif isinstance(event, FileCreatedEvent):
                self._on_created(event)
            elif isinstance(event, FileModifiedEvent):
                self._on_modified(event)

    def on_any_event(self, event: FileSystemEvent) -> None:
        """Handle file system events."""
        src_path = _src_path(event)

        if is_tier_marker(src_path, self._storage.config.get(CONFIG_TIER_MARKERS, {})):
            return
        if is_source_move_marker(src_path):
            return

        if is_tier_move_temp_file(src_path):
            if isinstance(event, FileMovedEvent):
                destination = cast("str", event.dest_path)
                if (
                    not is_tier_move_temp_file(destination)
                    and not is_source_move_marker(destination)
                    and os.path.basename(destination) not in self._storage.ignored_files
                    and os.path.commonpath((self._path, destination))
                    == os.path.normpath(self._path)
                ):
                    self._event_queue.put(FileCreatedEvent(destination))
            return

        if os.path.basename(src_path) in self._storage.ignored_files:
            return
        self._event_queue.put(event)

    def _on_created(self, event: FileCreatedEvent) -> None:
        """Bound held-path retries as well as failures of a whole filesystem."""
        src_path = _src_path(event)
        if is_source_move_marker(src_path):
            return
        if not hasattr(self, "_publication_io_retry"):
            self._publication_io_retry = FilesystemRetry(self._logger)
        if not hasattr(self, "_publication_retry"):
            self._publication_retry = PublicationRetry(self._logger)
        delay = self._publication_retry.remaining(src_path)
        if delay:
            self._schedule_journal_retry(src_path, delay=delay)
            return
        try:
            with self._publication_io_retry.attempt("publication", src_path) as attempt:
                if self._on_created_publication(event):
                    attempt.completed = True
                    self._publication_retry.complete(src_path)
                    return
        except BlockingIOError:
            # A busy camera lock is short-lived contention, not an unchanged
            # recovery failure. Keep fresh acknowledgements moving promptly.
            self._schedule_journal_retry(src_path, delay=1.0)
            return
        except StorageRetryLater as error:
            self._schedule_journal_retry(src_path, delay=error.delay)
            return
        except OSError:
            self._logger.exception("Pending publication failed: %s", src_path)
        delay = self._publication_retry.defer(src_path, notify=not attempt.recovering)
        self._schedule_journal_retry(src_path, delay=delay)

    def _on_created_publication(self, event: FileCreatedEvent) -> bool:
        """Index a publication, acknowledging its journal only after commit."""
        src_path = _src_path(event)
        if is_source_move_marker(src_path):
            return False
        directory = os.path.dirname(src_path)
        if not is_run_directory(os.path.basename(directory)):
            self._on_created_impl(event)
            return True
        try:
            with camera_lock(
                "/tmp/viseron-run-locks", os.path.abspath(os.path.dirname(directory))
            ):
                marker = marker_for_path(src_path)
                file_meta = None
                if os.path.exists(marker):
                    with open(marker, encoding="utf-8") as stream:
                        record = json.load(stream)
                    if not source_run_available(record):
                        return False
                    if record.get("version") == 2:  # noqa: PLR2004
                        record = publish_or_verify(marker)
                    if (
                        record.get("version") != 2  # noqa: PLR2004
                        or record.get("filename") != os.path.basename(src_path)
                        or not os.path.isfile(os.path.join(directory, "init.mp4"))
                    ):
                        raise ValueError("Invalid pending publication")  # noqa: TRY301
                    data = record["meta"]
                    if data.get("file_key") is None:
                        # Sequence allocation can leave gaps after a crash; it
                        # must be persisted before the row is acknowledged.
                        with self._storage.get_session() as session:
                            key = session.execute(
                                select(FILE_KEY_SEQUENCE.next_value())
                            ).scalar_one()
                        record = assign_file_key(marker, key)
                        data = record["meta"]
                    file_meta = FilesMeta(
                        orig_ctime=datetime.datetime.fromisoformat(data["orig_ctime"]),
                        duration=data["duration"],
                        file_key=data["file_key"],
                    )
                if (
                    file_meta is not None
                    and record.get("source_intent")
                    and record["meta"].get("source_db") is not None
                ):
                    if record.get("destination_id") is None:
                        with self._storage.get_session() as session:
                            existing = session.execute(
                                select(Files.id).where(Files.path == src_path)
                            ).scalar_one_or_none()
                            if existing is not None:
                                issue = (marker, "unbound destination row")
                                with self._journal_retry_lock:
                                    reported = getattr(
                                        self, "_journal_reported_issues", set()
                                    )
                                    if (
                                        issue not in reported
                                        and len(reported) < JOURNAL_BATCH_LIMIT
                                    ):
                                        reported.add(issue)
                                        self._journal_reported_issues = reported
                                        self._logger.warning(
                                            "Journal %s has an unbound destination "
                                            "database row. Files are retained; "
                                            "stop Viseron and reconcile the row "
                                            "before retrying. See %s.",
                                            marker,
                                            "docs/testing/tier-journal-recovery.md",
                                        )

                                return False
                            destination_id = session.execute(
                                select(
                                    func.nextval(
                                        func.pg_get_serial_sequence("files", "id")
                                    )
                                )
                            ).scalar_one()
                        record = assign_destination_id(marker, destination_id)
                    try:
                        self._on_created_impl(event, file_meta, journal_source=record)
                    except SourceJournalMismatch:
                        self._withdraw_journal_destination(src_path, record, file_meta)
                        raise
                else:
                    self._on_created_impl(event, file_meta)
                if file_meta is not None and not record.get("source_intent"):
                    os.unlink(marker)
                    sync_dir(directory)
                elif file_meta is not None and record.get("source_intent"):
                    camera_root = os.path.abspath(os.path.dirname(directory))
                    parent, camera_name = os.path.split(camera_root)
                    spool_root = os.path.join(parent, ".viseron-spool", camera_name)

                    def committed(saved):
                        with self._storage.get_session() as session:
                            row = session.execute(
                                select(Files).where(Files.path == src_path)
                            ).scalar_one_or_none()
                            return bool(
                                row is not None
                                and (
                                    saved.get("destination_id") is None
                                    or row.id == saved["destination_id"]
                                )
                                and row.file_key == saved["meta"]["file_key"]
                                and row.size == saved["identity"][2]
                                and row.camera_identifier == self._camera.identifier
                                and row.tier_id == self._tier_id
                                and row.tier_path == self._tier[CONFIG_PATH]
                                and row.category == self._category
                                and row.subcategory == self._subcategory
                                and row.orig_ctime == file_meta.orig_ctime
                                and row.duration == file_meta.duration
                            )

                    source_root = os.path.abspath(
                        os.path.dirname(
                            os.path.dirname(record["source_snapshot"]["source"])
                        )
                    )
                    roots = [spool_root]
                    if source_root != spool_root:
                        tiers = self._storage.camera_tier_handlers[
                            self._camera.identifier
                        ][self._category]
                        # Only prior configured tiers can be sources of a promotion.
                        roots = [
                            os.path.abspath(tier[self._subcategory]._path)
                            for tier in tiers[: self._tier_id]
                            if self._subcategory in tier
                        ]

                    if source_root != spool_root and not isinstance(
                        record["meta"].get("source_db"), dict
                    ):
                        issue = (marker, "missing source database identity")
                        with self._journal_retry_lock:
                            reported = getattr(self, "_journal_reported_issues", set())
                            if (
                                issue not in reported
                                and len(reported) < JOURNAL_BATCH_LIMIT
                            ):
                                reported.add(issue)
                                self._journal_reported_issues = reported
                                self._logger.warning(
                                    "Journal %s lacks source database identity "
                                    "from an older "
                                    "version; source retained. Inspect the source and "
                                    "destination database rows before manual recovery; "
                                    "do not remove the journal or source.",
                                    marker,
                                )

                    @contextlib.contextmanager
                    def source_index_guard(saved, source_present):
                        snapshot = saved["source_snapshot"]
                        expected = saved["meta"].get("source_db")
                        if not isinstance(expected, dict):
                            yield None
                            return
                        with self._storage.get_session() as session:
                            _lock_path(session, snapshot["source"])
                            row = session.execute(
                                select(Files)
                                .where(Files.path == snapshot["source"])
                                .with_for_update()
                            ).scalar_one_or_none()
                            if row is None:
                                yield (None if source_present else lambda: None)
                                return
                            if (
                                row.id != expected.get("id")
                                or row.tier_id != expected.get("tier_id")
                                or row.tier_path != expected.get("tier_path")
                                or row.file_key != saved["meta"]["file_key"]
                                or row.size != snapshot["size"]
                                or row.camera_identifier != self._camera.identifier
                                or row.category != self._category
                                or row.subcategory != self._subcategory
                                or row.orig_ctime != file_meta.orig_ctime
                                or row.duration != file_meta.duration
                            ):
                                yield None
                                return

                            def finalize():
                                session.delete(row)
                                session.commit()

                            yield finalize

                    pending_moves = getattr(self._storage, "pending_moves", None)
                    pending = None
                    if pending_moves is not None and source_root != spool_root:
                        with self._storage.pending_moves_lock:
                            pending = pending_moves.get(
                                record["source_snapshot"]["source"]
                            )
                    result = release_indexed_source(
                        marker,
                        roots,
                        self._storage.tier_available,
                        committed,
                        "/tmp/viseron-run-locks",
                        source_index_guard=(
                            None if source_root == spool_root else source_index_guard
                        ),
                    )
                    if result != "released":
                        if result in (
                            "source-changed",
                            "source-index-conflict",
                            "source-intent-mismatch",
                        ):
                            self._withdraw_journal_destination(
                                src_path, record, file_meta
                            )
                        return False
                    with self._storage.get_session() as session:
                        acknowledge_segment_move(
                            session,
                            record["source_snapshot"]["source"],
                            src_path,
                            record["meta"],
                        )
                    self._complete_pending_move(
                        pending, pending_moves, record, src_path
                    )
            return True
        except BlockingIOError:
            raise
        except OSError:
            raise
        except Exception:
            self._logger.exception("Pending publication failed: %s", src_path)
        return False

    def _complete_pending_move(self, pending, pending_moves, record, src_path):
        """Deliver an in-memory completion only after the durable handoff."""
        if pending is not None:
            completed_pending = None
            with self._storage.pending_moves_lock:
                source_path = record["source_snapshot"]["source"]
                if (
                    pending.dst == src_path
                    and pending.tier_id == self._tier_id
                    and pending.tier_path == self._tier[CONFIG_PATH]
                    and pending_moves.get(source_path) is pending
                ):
                    pending_moves.pop(source_path, None)
                    completed_pending = pending
            if (
                completed_pending is not None
                and completed_pending.on_complete is not None
            ):
                completed_pending.on_complete()

    def _withdraw_journal_destination(self, path, record, file_meta):
        """Withdraw only this journal's matching row; retain all media and holds."""
        with self._storage.get_session() as session:
            _lock_path(session, path)
            row = session.execute(
                select(Files).where(Files.path == path).with_for_update()
            ).scalar_one_or_none()
            if (
                row is not None
                and row.id == record.get("destination_id")
                and row.file_key == record["meta"]["file_key"]
                and row.size == record["identity"][2]
                and row.camera_identifier == self._camera.identifier
                and row.tier_id == self._tier_id
                and row.tier_path == self._tier[CONFIG_PATH]
                and row.category == self._category
                and row.subcategory == self._subcategory
                and row.orig_ctime == file_meta.orig_ctime
                and row.duration == file_meta.duration
            ):
                session.delete(row)
                session.commit()

    def _on_created_impl(
        self, event: FileCreatedEvent, journal_meta=None, *, journal_source=None
    ) -> None:
        """Insert a file, using journal metadata when available."""
        src_path = _src_path(event)
        self._logger.debug("File created: %s", src_path)
        with self._storage.pending_moves_lock:
            queued_meta = self._storage.temporary_files_meta.pop(src_path, None)
        file_meta = journal_meta or queued_meta
        size = os.path.getsize(src_path)
        with self._storage.get_session() as session:
            _lock_path(session, src_path)
            if journal_source is not None:
                snapshot = journal_source["source_snapshot"]
                expected = journal_source["meta"]["source_db"]
                if snapshot.get("source_db") != expected:
                    raise SourceJournalMismatch("source database snapshot mismatch")
                _lock_path(session, snapshot["source"])
                old_row = session.execute(
                    select(Files)
                    .where(Files.path == snapshot["source"])
                    .with_for_update()
                ).scalar_one_or_none()
                if old_row is None:
                    # Replay after an already committed release is allowed only
                    # when the destination row was previously indexed.
                    existing = session.execute(
                        select(Files).where(Files.path == src_path)
                    ).scalar_one_or_none()
                    if existing is None:
                        raise SourceJournalMismatch(
                            "source row missing before destination index"
                        )
                elif (
                    old_row.id != expected.get("id")
                    or old_row.tier_id != expected.get("tier_id")
                    or old_row.tier_path != expected.get("tier_path")
                    or old_row.file_key != journal_meta.file_key
                    or old_row.size != snapshot["size"]
                    or old_row.camera_identifier != self._camera.identifier
                    or old_row.category != self._category
                    or old_row.subcategory != self._subcategory
                    or old_row.orig_ctime != journal_meta.orig_ctime
                    or old_row.duration != journal_meta.duration
                ):
                    raise SourceJournalMismatch(
                        "source row changed before destination index"
                    )
            stmt = insert(Files).values(
                tier_id=self._tier_id,
                tier_path=self._tier[CONFIG_PATH],
                camera_identifier=self._camera.identifier,
                category=self._category,
                subcategory=self._subcategory,
                path=src_path,
                directory=os.path.dirname(src_path),
                filename=os.path.basename(src_path),
                size=size,
                orig_ctime=file_meta.orig_ctime if file_meta else utcnow(),
                duration=file_meta.duration if file_meta else None,
                file_key=(
                    file_meta.file_key
                    if file_meta and file_meta.file_key is not None
                    else FILE_KEY_SEQUENCE.next_value()
                ),
            )
            if journal_source is not None:
                stmt = stmt.values(id=journal_source["destination_id"])
            if journal_meta is not None:
                # Do not update an unrelated concurrent insert.
                session.execute(
                    stmt.on_conflict_do_nothing(index_elements=[Files.path])
                )
                # Journal replay may repeat after commit, but an unrelated row
                # at the same path must never count as acknowledgement.
                existing = session.execute(
                    select(Files).where(Files.path == src_path)
                ).scalar_one_or_none()
                if existing is not None:
                    if (
                        (
                            journal_source is not None
                            and existing.id != journal_source["destination_id"]
                        )
                        or existing.file_key != file_meta.file_key
                        or existing.size != size
                        or existing.camera_identifier != self._camera.identifier
                        or existing.tier_id != self._tier_id
                        or existing.tier_path != self._tier[CONFIG_PATH]
                        or existing.category != self._category
                        or existing.subcategory != self._subcategory
                        or existing.orig_ctime != file_meta.orig_ctime
                        or existing.duration != file_meta.duration
                    ):
                        raise ValueError(
                            "Conflicting database row for pending publication"
                        )
            else:
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[Files.path], set_={"size": size}
                    )
                )
            session.commit()

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
                file_name=os.path.basename(src_path),
                path=src_path,
            ),
            store=False,
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

            with self._storage.get_session() as session:
                stmt = update(Files).where(Files.path == src_path).values(size=size)
                session.execute(stmt)
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
        """Fence stale deletion events and pending run publications."""
        src_path = _src_path(event)
        directory = os.path.dirname(src_path)
        if not is_run_directory(os.path.basename(directory)):
            if self._category == TIER_CATEGORY_RECORDER and self._subcategory in (
                TIER_SUBCATEGORY_THUMBNAILS,
                TIER_SUBCATEGORY_EVENT_CLIPS,
            ):
                with self._storage.get_session() as session:
                    if asset_move_pending(session, src_path):
                        return
            return self._on_deleted_impl(event)
        try:
            with camera_lock(
                "/tmp/viseron-run-locks", os.path.abspath(os.path.dirname(directory))
            ):
                if (
                    protected_by_pending(src_path)
                    or source_move_pending(src_path)
                    or os.path.lexists(src_path)
                ):
                    return
                with self._storage.get_session() as session:
                    if segment_move_pending(session, src_path):
                        return
                self._on_deleted_impl(event)
        except CameraLockBusy:
            # Return one event to the existing queue, without creating timers or
            # multiplying events. Limit retries and let shutdown interrupt the wait.
            if not self._deletion_retry_stop.wait(0.1):
                with self._check_tier_lock:
                    if not self._tier_check_stopping:
                        self._event_queue.put(event)
        except OSError:
            self._logger.warning(
                "Deferring deletion event for %s", src_path, exc_info=True
            )

    def _on_deleted_impl(self, event: FileDeletedEvent) -> None:
        """Remove file from database when it is deleted."""
        src_path = _src_path(event)

        self._logger.debug("File deleted: %s", src_path)
        with self._storage.pending_moves_lock:
            pending_move = self._storage.pending_moves.pop(src_path, None)
        with self._storage.get_session() as session:
            if pending_move:
                _hand_over_row(session, src_path, pending_move)
            stmt = delete(Files).where(Files.path == src_path)
            session.execute(stmt)
            session.commit()

        if pending_move is not None and pending_move.on_complete is not None:
            pending_move.on_complete()

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
        if self._tier[CONFIG_MOVE_ON_SHUTDOWN]:
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
        with self._hold_retry_lock:
            self._hold_retry_stopping = True
            try:
                os.unlink(self._hold_retry_token)
            except FileNotFoundError:
                pass
        if self._source_intent_job is not None:
            try:
                self._source_intent_job.remove()
            except JobLookupError:
                # Scheduler shutdown may already have removed the job.
                pass
            self._source_intent_job = None
        with self._journal_retry_lock:
            self._journal_retry_stopping = True
            self._journal_retry_paths.clear()
            if self._journal_retry_timer is not None:
                self._journal_retry_timer.cancel()
                self._journal_retry_timer = None
        with self._check_tier_lock:
            self._tier_check_stopping = True
            self._deletion_retry_stop.set()
            self._tier_check_force_pending = False
            if self._tier_check_retry_timer is not None:
                self._tier_check_retry_timer.cancel()
                self._tier_check_retry_timer = None
        with self._observer_lock:
            self._observer_stopping = True
            observer = self._observer
        for pending_update in self._pending_updates.copy().values():
            pending_update.join()
        self._event_queue.put(None)
        self._event_thread.join()
        if observer is not None:
            observer.stop()
            observer.join()


class SegmentsTierHandler(TierHandler):
    """Handle the recorder tiers."""

    def initialize(self) -> None:
        """Initialize recorder tier."""
        self._path = get_segments_path(self._tier, self._camera)

        self._continuous_max_bytes = calculate_bytes(
            self._tier[CONFIG_CONTINUOUS][CONFIG_MAX_SIZE]
        )
        self._continuous_min_bytes = calculate_bytes(
            self._tier[CONFIG_CONTINUOUS][CONFIG_MIN_SIZE]
        )
        self._continuous_max_age = calculate_age(
            self._tier[CONFIG_CONTINUOUS][CONFIG_MAX_AGE]
        )
        self._continuous_min_age = calculate_age(
            self._tier[CONFIG_CONTINUOUS][CONFIG_MIN_AGE]
        )
        self._continuous_params = [
            self._continuous_max_bytes,
            self._continuous_min_age,
            self._continuous_min_bytes,
            self._continuous_max_age,
        ]

        self._events_max_bytes = calculate_bytes(
            self._tier[CONFIG_EVENTS][CONFIG_MAX_SIZE]
        )
        self._events_min_bytes = calculate_bytes(
            self._tier[CONFIG_EVENTS][CONFIG_MIN_SIZE]
        )
        self._events_min_age = calculate_age(self._tier[CONFIG_EVENTS][CONFIG_MIN_AGE])

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
            self._events_max_age = calculate_age(
                self._tier[CONFIG_EVENTS][CONFIG_MAX_AGE]
            )
        self._events_params = [
            self._events_max_bytes,
            self._events_max_age,
            self._events_min_bytes,
            self._events_min_age,
        ]

        self._size_percentages = {
            name: rules[limit]["percent"]
            for rules, limit, name in (
                (self._tier[CONFIG_CONTINUOUS], CONFIG_MAX_SIZE, "max_bytes"),
                (self._tier[CONFIG_CONTINUOUS], CONFIG_MIN_SIZE, "min_bytes"),
                (self._tier[CONFIG_EVENTS], CONFIG_MAX_SIZE, "events_max_bytes"),
                (self._tier[CONFIG_EVENTS], CONFIG_MIN_SIZE, "events_min_bytes"),
            )
            if "percent" in rules[limit]
        }
        self._events_enabled = (
            "retention" in self._tier
            or any(self._events_params)
            or any(name.startswith("events_") for name in self._size_percentages)
        )
        self._continuous_configured = (
            "retention" in self._tier
            or any(self._continuous_params)
            or any(
                name in self._size_percentages for name in ("max_bytes", "min_bytes")
            )
        ) and self._camera.config[CONFIG_RECORDER][CONFIG_CONTINUOUS_RECORDING]

        if (
            self._next_tier is None
            and not any(self._events_params)
            and not any(self._continuous_params)
            and not self._size_percentages
            and not self._tier.get("retention", {}).get("max_size")
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

    def _create_dataitem(self) -> DataItem:
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
            drain=self._tier[CONFIG_DRAIN],
            events_enabled=self._events_enabled,
            events_max_bytes=self._events_max_bytes,
            events_min_age=self._events_min_age,
            events_max_age=self._events_max_age,
            events_min_bytes=self._events_min_bytes,
            continuous_schedule=self._continuous_schedule_entries(),
            continuous_schedule_timezone=self._continuous_schedule_timezone(),
            continuous_lookback_seconds=self._camera.recorder.lookback,
            size_percentages=dict(self._size_percentages),
            size_path=self._path,
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
        on_complete: Callable[[str], None] | None = None,
        retention_recording_ids: tuple[int, ...] | None = None,
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
                retention_recording_ids=retention_recording_ids,
                force_delete=force_delete,
                on_complete=(
                    partial(on_complete, str(file["path"]))
                    if on_complete is not None
                    else None
                ),
            )
            processed_paths.append(file["path"])
            files_processed += 1
        return recording_ids

    def _handle_continuous(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        continuous_next_tier: SegmentsTierHandler | None,
        retention_recording_ids: tuple[int, ...] | None = None,
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
                retention_recording_ids=retention_recording_ids,
            )
            files_processed += 1

    def _handle_events_and_continuous(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        events_next_tier: SegmentsTierHandler | None,
        continuous_next_tier: SegmentsTierHandler | None,
        on_complete: Callable[[str], None] | None = None,
        retention_recording_ids: tuple[int, ...] | None = None,
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
                retention_recording_ids=retention_recording_ids,
                force_delete=force_delete,
                on_complete=(
                    partial(on_complete, str(file["path"]))
                    if on_complete is not None
                    else None
                ),
            )
            processed_paths.append(file["path"])
            files_processed += 1
        return recording_ids

    def _check_tier(
        self,
        get_session: Callable[[], Session],
        data: np.ndarray,
        retention_recording_ids: tuple[int, ...] | None = None,
    ) -> None:
        if retention_recording_ids is None:
            retention_recording_ids = tuple(
                sorted(
                    {
                        int(row["recording_id"])
                        for row in data
                        if "recording_id" in data.dtype.names
                        and row["recording_id"] >= 0
                    }
                )
            )
        events_next_tier = None
        recording_ids = (
            sorted(
                {int(row["recording_id"]) for row in data if row["recording_id"] >= 0}
            )
            if self._events_enabled
            else []
        )
        paths = {
            str(row["path"])
            for row in data
            if self._events_enabled and row["recording_id"] >= 0
        }

        def schedule_completion() -> None:
            # Commit callbacks can run under a publication lock. Queue the
            # finalization so asset checks never perform remote I/O under it.
            if not getattr(self, "_tier_check_stopping", False):
                try:
                    self._vis.background_scheduler.add_job(
                        self._complete_event_retention,
                        args=[get_session, recording_ids, paths, events_next_tier],
                    )
                except RuntimeError:
                    self._logger.warning(
                        "Could not schedule event finalization; keeping metadata",
                        exc_info=True,
                    )

        batch = (
            _EventRetentionBatch(paths, schedule_completion) if recording_ids else None
        )
        on_complete = batch.completed if batch is not None else None
        if self._events_enabled and not self.continuous_enabled:
            events_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "events"
            )
            self._handle_events(
                get_session,
                data,
                events_next_tier,
                on_complete,
                retention_recording_ids,
            )
        elif self.continuous_enabled and not self._events_enabled:
            continuous_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "continuous"
            )
            self._handle_continuous(
                get_session, data, continuous_next_tier, retention_recording_ids
            )
        else:
            events_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "events"
            )
            continuous_next_tier = find_next_tier_segments(
                self._storage, self._tier_id, self._camera, "continuous"
            )
            self._handle_events_and_continuous(
                get_session,
                data,
                events_next_tier,
                continuous_next_tier,
                on_complete,
                retention_recording_ids,
            )

    def _complete_event_retention(
        self,
        get_session: Callable[[], Session],
        recording_ids: list[int],
        paths: set[str],
        events_next_tier: SegmentsTierHandler | None,
    ) -> None:
        """Finalize only committed segment batches belonging to ended events."""
        if getattr(self, "_tier_check_stopping", False):
            return
        with get_session() as session:
            # A stale or early callback cannot retire an event whose source
            # rows still exist, including a new publication at the same path.
            if session.execute(
                select(Files.id).where(Files.path.in_(paths)).limit(1)
            ).first():
                return
            remaining_source = (
                select(Files.id)
                .where(
                    Files.camera_identifier == Recordings.camera_identifier,
                    Files.tier_id == self._tier_id,
                    Files.category == TIER_CATEGORY_RECORDER,
                    Files.subcategory == TIER_SUBCATEGORY_SEGMENTS,
                    Files.orig_ctime < Recordings.end_time,
                    or_(
                        Files.duration.is_(None),
                        Files.orig_ctime
                        + func.make_interval(0, 0, 0, 0, 0, 0, Files.duration)
                        >= Recordings.adjusted_start_time,
                    ),
                    or_(
                        Recordings.recorder_run_id.is_(None),
                        Files.directory.endswith("/" + Recordings.recorder_run_id),
                    ),
                )
                .exists()
            )
            eligible = (
                Recordings.id.in_(recording_ids),
                Recordings.camera_identifier == self._camera.identifier,
                Recordings.end_time.is_not(None),
                ~remaining_source,
            )
            if events_next_tier is None:
                # Index absence is insufficient: staging or an in-flight
                # publication can still own this event's historical footage.
                retired = []
                recordings = (
                    session.execute(
                        select(Recordings).where(
                            *eligible, Recordings.recorder_run_id.is_not(None)
                        )
                    )
                    .scalars()
                    .all()
                )
                if not recordings:
                    return
                try:
                    roots = recording_camera_paths(self._storage, self._camera)
                except OSError:
                    return
                for recording in recordings:
                    with empty_recording_run(
                        roots, recording.recorder_run_id, self._storage.tier_markers
                    ) as empty:
                        if not empty:
                            continue
                        # Keep both the disk locks and the final SQL eligibility
                        # recheck through commit, before any asset cleanup.
                        retired.extend(
                            session.execute(
                                delete(Recordings)
                                .where(
                                    *eligible,
                                    Recordings.id == recording.id,
                                    Recordings.recorder_run_id
                                    == recording.recorder_run_id,
                                )
                                .returning(
                                    Recordings.id,
                                    Recordings.thumbnail_path,
                                    Recordings.clip_path,
                                )
                            ).all()
                        )
                        session.commit()
            else:
                recording_ids = list(
                    session.execute(select(Recordings.id).where(*eligible)).scalars()
                )

        if events_next_tier is None:
            self._delete_retired_event_assets(get_session, retired)
            return

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

    def _delete_retired_event_assets(
        self, get_session: Callable[[], Session], retired: Sequence[Any]
    ) -> None:
        """Queue cleanup only after retirement commits, using exact owned paths."""
        for recording_id, thumbnail_path, clip_path in retired:
            for subcategory, path in (
                (TIER_SUBCATEGORY_THUMBNAILS, thumbnail_path),
                (TIER_SUBCATEGORY_EVENT_CLIPS, clip_path),
            ):
                if not path or getattr(self, "_tier_check_stopping", False):
                    continue
                with get_session() as session:
                    owned = session.execute(
                        select(Files.id).where(
                            Files.path == path,
                            Files.camera_identifier == self._camera.identifier,
                            Files.category == TIER_CATEGORY_RECORDER,
                            Files.subcategory == subcategory,
                            Files.tier_id == self._tier_id,
                            Files.tier_path == self._tier[CONFIG_PATH],
                            Files.move_target.is_(None),
                        )
                    ).first()
                if owned is None:
                    continue
                try:
                    delete_file(self._storage, path)
                except StorageCallbackCapacityError:
                    # Metadata was retired, so existing orphan-asset cleanup
                    # can retry this file without an in-memory callback.
                    self._logger.warning(
                        "Deferred retired asset cleanup for event %s: %s",
                        recording_id,
                        path,
                    )


SNAPSHOT_REFERENCE_MODELS: dict[str, type[Motion | Objects | PostProcessorResults]] = {
    TIER_SUBCATEGORY_MOTION_DETECTOR: Motion,
    TIER_SUBCATEGORY_OBJECT_DETECTOR: Objects,
    TIER_SUBCATEGORY_FACE_RECOGNITION: PostProcessorResults,
    TIER_SUBCATEGORY_IMAGE_CLASSIFICATION: PostProcessorResults,
    TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION: PostProcessorResults,
}


class SnapshotTierHandler(TierHandler):
    """Handle the snapshot tiers."""

    def initialize(self) -> None:
        """Initialize snapshot tier."""
        super().initialize()
        self.add_file_handler(self._path, rf"{self._path}/(.*.jpg$)")
        self._storage.ignore_file(LATEST_SNAPSHOT_FILENAME)

    def _on_deleted(self, event: FileDeletedEvent) -> None:
        src_path = _src_path(event)

        with self._storage.pending_moves_lock:
            pending_move = self._storage.pending_moves.get(src_path)
        if pending_move:
            self._follow_move(src_path, pending_move)
            super()._on_deleted(event)
            return

        stmt: Delete | ReturningDelete[tuple[int]]
        if self._subcategory == TIER_SUBCATEGORY_MOTION_DETECTOR:
            with self._storage.get_session() as session:
                stmt = (
                    delete(Motion)
                    .where(Motion.snapshot_path == src_path)
                    .returning(Motion.id)
                )
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
                stmt = delete(Objects).where(Objects.snapshot_path == src_path)
                session.execute(stmt)
                session.commit()

        elif self._subcategory in [
            TIER_SUBCATEGORY_FACE_RECOGNITION,
            TIER_SUBCATEGORY_IMAGE_CLASSIFICATION,
            TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION,
        ]:
            with self._storage.get_session() as session:
                stmt = delete(PostProcessorResults).where(
                    PostProcessorResults.snapshot_path == src_path
                )
                session.execute(stmt)
                session.commit()

        super()._on_deleted(event)

    def _follow_move(self, src_path: str, pending_move: PendingMove) -> None:
        """Point detector rows at the new location of a snapshot that moved tier."""
        model = SNAPSHOT_REFERENCE_MODELS[self._subcategory]
        with self._storage.get_session() as session:
            session.execute(
                update(model)
                .where(model.snapshot_path == src_path)
                .values(snapshot_path=pending_move.dst)
            )
            session.commit()


class ThumbnailTierHandler(TierHandler):
    """Handle thumbnails."""

    def initialize(self) -> None:
        """Initialize thumbnail tier."""
        self._path = get_thumbnails_path(self._tier, self._camera)
        self.add_file_handler(self._path, rf"{self._path}/(.*.jpg$)")
        self._storage.ignore_file("latest_thumbnail.jpg")

    def check_tier(self, *, force: bool = False) -> bool:  # noqa: ARG002
        """Do nothing, as we don't want to move thumbnails."""
        return True

    def _on_created(self, event: FileCreatedEvent) -> None:
        src_path = _src_path(event)
        if self._asset_destination_pending(src_path):
            return

        try:
            with self._storage.get_session() as session:
                stmt = (
                    update(Recordings)
                    .where(Recordings.id == os.path.basename(src_path).split(".")[0])
                    .where(Recordings.camera_identifier == self._camera.identifier)
                    .where(
                        or_(
                            Recordings.thumbnail_path.is_(None),
                            Recordings.thumbnail_path == src_path,
                        )
                    )
                    .values(thumbnail_path=src_path)
                )
                session.execute(stmt)
                session.commit()
        except Exception as error:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.error(
                "Failed to update thumbnail path for recording with path: "
                f"{src_path}: {error}"
            )
        super()._on_created(event)

    def move_thumbnail(
        self, recording_id: int, next_tier: dict[str, Any] | None
    ) -> None:
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

            if not recording.thumbnail_path:
                self._logger.debug(
                    "Recording %s has no thumbnail to move", recording_id
                )
                return

            indexed = session.execute(
                select(Files.id).where(
                    Files.path == recording.thumbnail_path,
                    Files.camera_identifier == self._camera.identifier,
                    Files.tier_id == self._tier_id,
                    Files.tier_path == self._tier[CONFIG_PATH],
                    Files.category == self._category,
                    Files.subcategory == self._subcategory,
                )
            ).first()
            if indexed is None:
                return  # A duplicate or stale event batch no longer owns this asset.

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
                self._tier[CONFIG_PATH],
                self._logger,
            )
            session.commit()


class EventClipTierHandler(TierHandler):
    """Handle event clips created by create_event_clip."""

    def initialize(self) -> None:
        """Initialize event clips tier."""
        self._path = get_event_clips_path(self._tier, self._camera)
        self.add_file_handler(
            self._path,
            rf"{re.escape(self._path)}/(.*\.{re.escape(self._camera.extension)}$)",
        )

    def check_tier(self, *, force: bool = False) -> bool:  # noqa: ARG002
        """Do nothing, as we move event clips manually."""
        return True

    def _on_created(self, event: FileCreatedEvent) -> None:
        if self._asset_destination_pending(_src_path(event)):
            return
        super()._on_created(event)

    def move_event_clip(
        self, recording_id: int, next_tier: dict[str, Any] | None
    ) -> None:
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

            indexed = session.execute(
                select(Files.id).where(
                    Files.path == recording.clip_path,
                    Files.camera_identifier == self._camera.identifier,
                    Files.tier_id == self._tier_id,
                    Files.tier_path == self._tier[CONFIG_PATH],
                    Files.category == self._category,
                    Files.subcategory == self._subcategory,
                )
            ).first()
            if indexed is None:
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
                self._tier[CONFIG_PATH],
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


def handle_file(
    vis: Viseron,
    get_session: Callable[..., Session],
    storage: Storage,
    camera_identifier: str,
    curr_tier_id: int,
    curr_tier_category: str,
    curr_tier_subcategory: str,
    curr_tier: dict[str, Any],
    next_tier: dict[str, Any] | None,
    path: str,
    tier_path: str,
    logger: logging.Logger,
    *,
    force_delete: bool = False,
    on_complete: Callable[[], None] | None = None,
    retention_recording_ids: tuple[int, ...] | None = None,
) -> None:
    """Move file if there is a succeeding tier, else delete the file."""
    if not storage.tier_available(path):
        logger.debug("Skipping operation on unavailable tier for %s", path)
        return
    if (
        not force_delete
        and next_tier is not None
        and not storage.tier_available(next_tier[CONFIG_PATH])
    ):
        logger.debug(
            "Skipping operation to unavailable tier %s", next_tier[CONFIG_PATH]
        )
        return
    if (
        os.path.basename(path)
        in storage.camera_requested_files_count[camera_identifier].filenames
    ):
        logger.debug("File %s is recently requested, skipping", path)
        return

    stuck_row = False
    if force_delete or next_tier is None:

        def on_delete(item: DataItemDeleteFile) -> None:
            if item.deferred:
                _schedule_source_retry(
                    storage,
                    camera_identifier,
                    curr_tier_category,
                    curr_tier_id,
                    curr_tier_subcategory,
                    path,
                    logger,
                    retry_after=item.retry_after,
                )
            elif item.error is None and on_complete is not None:
                on_complete()

        try:
            delete_file(
                storage,
                path,
                callback=on_delete,
                retention_recording_ids=(
                    retention_recording_ids
                    if curr_tier_category == "recorder" and path.endswith(".m4s")
                    else None
                ),
            )
        except StorageCallbackCapacityError:
            _schedule_source_retry(
                storage,
                camera_identifier,
                curr_tier_category,
                curr_tier_id,
                curr_tier_subcategory,
                path,
                logger,
            )
            return
    else:
        new_path = path.replace(tier_path, next_tier[CONFIG_PATH], 1)
        if new_path == path:
            logger.warning(
                "Failed to move file %s to next tier, new path is the same as old. "
                "Viseron tries to mitigate this, but it can happen if you recently "
                "changed the tier paths or a previous move failed.",
                path,
            )
            stuck_row = True
        else:
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
                next_tier[CONFIG_PATH],
                logger,
                on_complete=on_complete,
            )

    # Delete the file from the database if:
    # - tier_path is not the same as curr_tier[CONFIG_PATH]. This is an
    #   indication that the tier configuration has changed and since the old
    #   path is not monitored, the delete signal will not be received by
    #   Viseron.
    # - the move target equals the source (stuck_row). The row points at a
    #   path this handler can never move or delete, so each tier-check pass
    #   would re-process it and slowly leak SQLAlchemy connection objects in
    #   the storage subprocess. Drop the row; OrphanedFilesCleanup will sweep
    #   the on-disk file if it still exists.
    if stuck_row or tier_path != curr_tier[CONFIG_PATH]:
        logger.debug(
            "Deleting file %s from database since tier paths are different. "
            "file tier_path: %s, current tier_path: %s, stuck_row: %s",
            path,
            tier_path,
            curr_tier[CONFIG_PATH],
            stuck_row,
        )
        run = os.path.dirname(path)
        if is_run_directory(os.path.basename(run)):
            try:
                with camera_lock(
                    "/tmp/viseron-run-locks", os.path.abspath(os.path.dirname(run))
                ):
                    if protected_by_pending(path) or source_move_pending(path):
                        return
                    with get_session() as session:
                        stmt = delete(Files).where(
                            Files.path == path,
                            Files.tier_id == curr_tier_id,
                            Files.tier_path == tier_path,
                            Files.move_target.is_(None),
                        )
                        session.execute(stmt)
                        session.commit()
            except OSError:
                logger.warning(
                    "Deferring database row cleanup for %s", path, exc_info=True
                )
            return
        with get_session() as session:
            stmt = delete(Files).where(
                Files.path == path,
                Files.tier_id == curr_tier_id,
                Files.tier_path == tier_path,
                Files.move_target.is_(None),
            )
            session.execute(stmt)
            session.commit()


def delete_file(
    storage: Storage,
    path: str,
    *,
    callback: Callable[[DataItemDeleteFile], None] | None = None,
    retention_recording_ids: tuple[int, ...] | None = None,
) -> None:
    """Delete file from storage."""
    storage.tier_check_worker_send_command(
        DataItemDeleteFile(
            cmd="delete_file",
            src=path,
            retention_recording_ids=retention_recording_ids,
        ),
        callback=callback,
    )


def _schedule_source_retry(
    storage: Storage,
    camera_identifier: str,
    category: str,
    tier_id: int,
    subcategory: str,
    path: str,
    logger: logging.Logger,
    *,
    retry_after: float = 0.0,
) -> None:
    """Coalesce contention retries through the original tier check."""
    try:
        handler = storage.camera_tier_handlers[camera_identifier][category][tier_id][
            subcategory
        ]
        with handler._check_tier_lock:  # pylint: disable=protected-access
            if retry_after > 0:
                handler._schedule_forced_tier_retry_locked(retry_after)  # pylint: disable=protected-access
            else:
                handler._schedule_forced_tier_retry_locked()  # pylint: disable=protected-access
    except (KeyError, IndexError):
        logger.warning("Cannot schedule deferred operation retry for %s", path)


def _mark_promotion_failed(storage: Storage, source_db: dict[str, Any] | None) -> None:
    """Keep capacity policy informed when an unavailable tier defers promotion."""
    manager = getattr(storage, "pressure_manager", None)
    if manager is not None and source_db is not None:
        manager.promotion_failed.add(source_db["tier_path"])


def move_file(
    vis: Viseron,
    storage: Storage,
    get_session: Callable[..., Session],
    camera_identifier: str,
    curr_tier_id: int,
    curr_tier_category: str,
    curr_tier_subcategory: str,
    src: str,
    dst: str,
    dst_tier_path: str,
    logger: logging.Logger,
    *,
    on_complete: Callable[[], None] | None = None,
) -> None:
    """Move file from src to dst.

    To avoid race conditions where a file is referenced at the same time as it is being
    moved, causing a 404 in the browser, we copy the file to the new location and then
    delete the old one.
    """
    manager = getattr(storage, "pressure_manager", None)
    if (
        curr_tier_category == TIER_CATEGORY_RECORDER
        and manager is not None
        and manager.accepts_promotions(dst_tier_path) is False
    ):
        logger.debug("Deferring promotion to pressured tier %s", dst_tier_path)
        return
    logger.debug("Moving file from %s to %s", src, dst)
    file_meta = None
    source_db = None
    pending_move = None
    asset_target = None
    try:
        reservation_lock = (
            asset_locks(src)
            if curr_tier_category == TIER_CATEGORY_RECORDER
            and curr_tier_subcategory
            in (TIER_SUBCATEGORY_THUMBNAILS, TIER_SUBCATEGORY_EVENT_CLIPS)
            else contextlib.nullcontext()
        )
        with reservation_lock, get_session() as session:
            _lock_path(session, src)
            sel = select(Files).where(Files.path == src).with_for_update()
            res = session.execute(sel).scalar_one()
            source_db = {
                "id": res.id,
                "tier_id": res.tier_id,
                "tier_path": res.tier_path,
            }
            file_meta = FilesMeta(
                orig_ctime=res.orig_ctime,
                duration=res.duration,
                file_key=res.file_key,
            )
            segment_tier_id = _segment_destination_tier(
                storage, res, curr_tier_id, dst_tier_path
            )
            if is_recording_asset(res):
                destination_tier_id = None
                handlers = storage.camera_tier_handlers[camera_identifier][
                    curr_tier_category
                ]
                waiting_for_handlers = not handlers
                for index, group in enumerate(handlers):
                    handler = group.get(curr_tier_subcategory)
                    if handler is None:
                        waiting_for_handlers = True
                        continue
                    if handler.tier[CONFIG_PATH] == dst_tier_path:
                        destination_tier_id = index
                        break
                if destination_tier_id is None:
                    if waiting_for_handlers:
                        # Recovery is scheduled during handler construction,
                        # before later asset handlers have entered the registry.
                        # Keep the reservation for the next bounded replay pass.
                        logger.debug(
                            "Deferring asset move until tier handlers are ready: %s",
                            src,
                        )
                        return
                    raise OSError("Asset destination tier is no longer configured")
                asset_target = reserve_asset_move(
                    res, dst, destination_tier_id, dst_tier_path
                )
                session.commit()
            pending_move = PendingMove(
                dst=dst,
                tier_id=(asset_target["tier_id"] if asset_target else segment_tier_id),
                tier_path=dst_tier_path,
                on_complete=on_complete,
            )
            with storage.pending_moves_lock:
                if src in storage.pending_moves:
                    logger.debug("Move already pending for %s", src)
                    return
                storage.temporary_files_meta[dst] = file_meta
                storage.pending_moves[src] = pending_move
    except NoResultFound:
        # A previously selected batch can outlive another completed move. Only
        # downgrade the warning when the old file is absent and the expected
        # destination is both indexed for this camera/tier and present on disk.
        try:
            try:
                os.lstat(src)
            except FileNotFoundError:
                with get_session() as session:
                    destination = session.execute(
                        select(Files).where(
                            Files.path == dst,
                            Files.camera_identifier == camera_identifier,
                            Files.category == curr_tier_category,
                            Files.subcategory == curr_tier_subcategory,
                            Files.tier_id == curr_tier_id + 1,
                            Files.tier_path == dst_tier_path,
                        )
                    ).scalar_one_or_none()
                    if destination is not None and stat.S_ISREG(os.lstat(dst).st_mode):
                        logger.debug("Move already completed for %s", src)
                        return
        except (OSError, SQLAlchemyError):
            # An unavailable destination is not evidence of a completed move.
            pass
        logger.warning(
            "Cannot move %s: source metadata is missing; leaving the file in place",
            src,
        )
        return

    def _move_file_callback(
        item: DataItemMoveFile,
    ) -> None:
        metadata = item.publication_meta or {}
        if (
            not item.error
            and metadata.get("pending_move_id") is not None
            and (not item.deferred or metadata.get("publication_ready", False))
        ):
            # A completed copy needs indexing even if final source work was
            # deferred. Preserve completion state until verified source release.
            destination = storage.camera_tier_handlers[camera_identifier][
                curr_tier_category
            ][segment_tier_id][curr_tier_subcategory]
            destination._schedule_journal_retry(dst)
            if item.deferred:
                return
        if item.deferred:
            if item.retry_after > 0 and not item.deferred_by_hold:
                _mark_promotion_failed(storage, source_db)
            with storage.pending_moves_lock:
                if storage.pending_moves.get(src) is pending_move:
                    storage.pending_moves.pop(src, None)
                if storage.temporary_files_meta.get(dst) is file_meta:
                    storage.temporary_files_meta.pop(dst, None)
            if not item.deferred_by_hold:
                _schedule_source_retry(
                    storage,
                    camera_identifier,
                    curr_tier_category,
                    curr_tier_id,
                    curr_tier_subcategory,
                    src,
                    logger,
                    retry_after=item.retry_after,
                )
            return
        if asset_target is not None and not item.error:
            with storage.pending_moves_lock:
                if storage.pending_moves.get(src) is pending_move:
                    storage.pending_moves.pop(src, None)
                if storage.temporary_files_meta.get(dst) is file_meta:
                    storage.temporary_files_meta.pop(dst, None)
            if on_complete is not None:
                on_complete()
            return
        if item.error:
            _mark_promotion_failed(storage, source_db)
            logger.error(f"Error moving file {src} to {dst}: {item.error}")
            with storage.pending_moves_lock:
                if storage.pending_moves.get(src) is pending_move:
                    storage.pending_moves.pop(src, None)
            vis.dispatch_event(
                EVENT_CHECK_TIER.format(
                    camera_identifier=camera_identifier,
                    # It is fine if the next tier does not exist since there will be no
                    # listeners for this event in that case
                    tier_id=curr_tier_id + 1,
                    category=curr_tier_category,
                    subcategory=curr_tier_subcategory,
                ),
                EventEmptyData(),
                store=False,
            )

    try:
        storage.tier_check_worker_send_command(
            DataItemMoveFile(
                cmd="move_file",
                src=src,
                dst=dst,
                camera_identifier=camera_identifier,
                publication_meta={
                    "orig_ctime": file_meta.orig_ctime.isoformat(),
                    "duration": file_meta.duration,
                    "file_key": file_meta.file_key,
                    "source_db": source_db,
                    "destination_tier_id": segment_tier_id,
                    "destination_tier_path": dst_tier_path,
                    **({"asset_target": asset_target} if asset_target else {}),
                },
            ),
            callback=_move_file_callback,
        )
    except Exception:
        # No worker can complete a command that was not accepted. Avoid
        # removing state replaced by another operation in the meantime.
        with storage.pending_moves_lock:
            if (
                file_meta is not None
                and storage.temporary_files_meta.get(dst) is file_meta
            ):
                storage.temporary_files_meta.pop(dst, None)
            if (
                pending_move is not None
                and storage.pending_moves.get(src) is pending_move
            ):
                storage.pending_moves.pop(src, None)
        raise


def _lock_path(session: Session, path: str) -> None:
    """Serialize writers of the row for path until the transaction ends.

    Without it, the NOT EXISTS guard in _hand_over_row cannot see a destination
    row that is inserted but not yet committed, so the UPDATE waits for it and then
    fails on files_path_key, which Postgres logs as an error.
    """
    session.execute(select(func.pg_advisory_xact_lock(func.hashtext(path))))


def _hand_over_row(session: Session, src: str, pending_move: PendingMove) -> None:
    """Point the row of a moved file at its destination.

    The destination observer can see the new file long after the source observer
    sees the delete, for instance when it polls, and the file key has to resolve
    in between. The copy has finished once the source is deleted.
    """
    _lock_path(session, pending_move.dst)
    dst_row_exists = select(Files.id).where(Files.path == pending_move.dst).exists()
    stmt = (
        update(Files)
        .where(Files.path == src, ~dst_row_exists)
        .values(
            path=pending_move.dst,
            directory=os.path.dirname(pending_move.dst),
            tier_id=pending_move.tier_id,
            tier_path=pending_move.tier_path,
        )
    )
    session.execute(stmt)


def force_move_files(
    vis: Viseron,
    storage: Storage,
    get_session: Callable[..., Session],
    category: str,
    subcategory: str,
    tier_id: int,
    camera_identifier: str,
    curr_tier: dict[str, Any],
    next_tier: dict[str, Any] | None,
    logger: logging.Logger,
) -> None:
    """Get and move/delete all files in tier."""
    with get_session(expire_on_commit=False) as session:
        stmt = (
            select(Files.path, Files.tier_path)
            .where(Files.camera_identifier == camera_identifier)
            .where(Files.tier_id == tier_id)
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


class TimelapseTierHandler(TierHandler):
    """Handle timelapse files."""

    def initialize(self) -> None:
        """Initialize timelapse tier."""
        super().initialize()

        self._path = get_timelapse_path(self._tier, self._camera)
        self._interval = calculate_age(self._tier.get(CONFIG_INTERVAL, {}))
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
                    Files.path == src_path
                )
                current_file_result = session.execute(current_file_stmt).scalar_one()
                current_file_datetime = current_file_result

                interval_start = current_file_datetime - self._interval
                interval_end = current_file_datetime

                stmt = select(Files).where(
                    Files.tier_id == self._tier_id,
                    Files.camera_identifier == self._camera.identifier,
                    Files.category == self._category,
                    Files.subcategory == self._subcategory,
                    Files.path != src_path,
                    Files.orig_ctime >= interval_start,
                    Files.orig_ctime <= interval_end,
                )

                result = session.execute(stmt).scalars().all()

                if result:
                    self._logger.debug(
                        f"File within interval already exists, removing current file: "
                        f"{src_path}"
                    )
                    delete_file(self._storage, src_path)

                    delete_stmt = delete(Files).where(Files.path == src_path)
                    session.execute(delete_stmt)
                    session.commit()

        except Exception as e:  # pylint: disable=broad-except # noqa: BLE001
            self._logger.error(f"Error during timelapse interval cleanup: {e}")


def _segment_destination_tier(storage, row, tier_id, tier_path):
    """Resolve the configured target; only active workers reserve copies."""
    if (
        row.category != TIER_CATEGORY_RECORDER
        or row.subcategory != TIER_SUBCATEGORY_SEGMENTS
        or not is_run_directory(os.path.basename(row.directory))
    ):
        return tier_id + 1
    destination_id = None
    for index, handlers in enumerate(
        storage.camera_tier_handlers[row.camera_identifier][row.category]
    ):
        handler = handlers.get(row.subcategory)
        if handler is not None and handler.tier[CONFIG_PATH] == tier_path:
            destination_id = index
            break
    if destination_id is None or destination_id <= row.tier_id:
        raise ValueError("Segment destination is not a configured later tier")
    return destination_id
