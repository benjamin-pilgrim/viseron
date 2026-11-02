"""Cleanup jobs for removing orphaned files and database records."""

from __future__ import annotations

import contextlib
import datetime
import logging
import os
import stat
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import setproctitle
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import and_, delete, exists, func, or_, select
from sqlalchemy.orm import sessionmaker

from viseron.components.storage.const import (
    ENGINE,
    CONFIG_TIER_MARKERS,
    TIER_CATEGORY_RECORDER,
    TIER_SUBCATEGORY_SEGMENTS,
    CleanupJobNames,
)
from viseron.components.storage.pending_journal import protected_by_pending
from viseron.components.storage.source_move_intent import (
    is_source_move_marker,
    source_move_pending,
)
from viseron.components.storage.run_lifecycle import (
    camera_lock,
    is_run_directory,
    reclaim_empty_run,
    remove_stale_temporary,
)
from viseron.components.storage.models import (
    Events,
    Files,
    Motion,
    Objects,
    PostProcessorResults,
    Recordings,
)
from viseron.components.storage.util import (
    is_tier_marker,
    is_tier_move_temp_file,
    tier_markers_available,
    require_tier_markers,
)
from viseron.const import TEMP_DIR, VISERON_SIGNAL_SHUTDOWN
from viseron.domains.camera.const import DOMAIN as CAMERA_DOMAIN
from viseron.exceptions import DomainNotRegisteredError
from viseron.helpers import utcnow
from viseron.helpers.child_process_context import (
    CHILD_PROCESS_START_METHOD,
    get_child_process_context,
)
from viseron.helpers.logs import SensitiveInformationFilter, enable_child_logging
from viseron.viseron_types import SnapshotDomain
from viseron.watchdog.process_watchdog import RestartableProcess
from viseron.watchdog.thread_watchdog import RestartableThread

if TYPE_CHECKING:
    from collections.abc import Iterator
    from multiprocessing.synchronize import Event

    from apscheduler.triggers.interval import IntervalTrigger
    from sqlalchemy import CursorResult
    from sqlalchemy.orm import Session

    from viseron import Viseron
    from viseron.components.storage import Storage
    from viseron.domains.camera import AbstractCamera

LOGGER = logging.getLogger(__name__)

LOG_THROTTLE_SECONDS = 5

BATCH_SIZE = 100


def file_is_missing(path: str) -> bool:
    """Return true only when stat reports that a file does not exist.

    Other filesystem errors can indicate an unavailable remote tier and must not
    be used as evidence that its database records are orphaned.
    """
    try:
        os.stat(path)
    except FileNotFoundError:
        return True
    except OSError as error:
        LOGGER.debug("Skipping inaccessible file %s: %s", path, error)
    return False


@dataclass(frozen=True)
class CleanupJobConfig:
    """Everything a cleanup job child process needs, as plain picklable data."""

    name: str
    paths: list[str]
    ignored_files: frozenset[str]
    sensitive_strings: tuple[str, ...]
    log_level: int
    tier_markers: dict[str, str] | None = None
    recording_paths: dict[str, tuple[str, ...]] | None = None


class CleanupJobRunner:
    """State of a cleanup job running inside the child process."""

    def __init__(self, config: CleanupJobConfig, kill_event: Event) -> None:
        self.name = config.name
        self.paths = config.paths
        self.ignored_files = config.ignored_files
        self.kill_event = kill_event
        self.tier_markers = config.tier_markers or {}
        self.recording_paths = config.recording_paths or {}
        self._session_maker = sessionmaker(bind=ENGINE)
        self._last_log_time = 0.0

    def get_session(self) -> Session:
        """Get a new sqlalchemy session."""
        return self._session_maker()

    def log_progress(self, message: str) -> None:
        """Log progress of the cleanup job.

        Throttled to only run every 5s so that it doesn't spam the logs.
        """
        now = time.time()
        if now - self._last_log_time > LOG_THROTTLE_SECONDS:
            LOGGER.debug(message)
            self._last_log_time = now


def run_cleanup_job(
    job: type[BaseCleanupJob], config: CleanupJobConfig, kill_event: Event
) -> None:
    """Entrypoint of the cleanup job child process."""
    setproctitle.setproctitle(f"viseron_{config.name}")
    enable_child_logging(config.sensitive_strings, config.log_level)
    job.work(CleanupJobRunner(config, kill_event))


class BaseCleanupJob(ABC):
    """Base class for cleanup jobs.

    The work of a job runs in a child process created with the forkserver start method
    to reduce memory usage. Anything the work needs from the running Viseron instance
    is resolved into a CleanupJobConfig before the child process is started.
    """

    def __init__(
        self, vis: Viseron, storage: Storage, interval_trigger: IntervalTrigger
    ) -> None:
        self._vis = vis
        self._storage = storage
        self._interval_trigger = interval_trigger

        self.kill_event = get_child_process_context().Event()
        self.run_lock = threading.Lock()
        self.running = False

    def _get_cameras(self) -> dict[str, AbstractCamera] | None:
        """Get list of registered camera identifiers."""
        try:
            return self._vis.get_registered_identifiers(CAMERA_DOMAIN)
        except DomainNotRegisteredError:
            return None

    def _get_all_paths(self) -> list[str]:
        """Return the storage paths of all cameras, across all tiers."""
        cameras = self._get_cameras() or {}
        paths = []
        for camera in cameras.values():
            paths += self._storage.get_event_clips_path(camera, all_tiers=True)
            paths += self._storage.get_segments_path(camera, all_tiers=True)
            paths += self._storage.get_thumbnails_path(camera, all_tiers=True)
            if timelapse_path := self._storage.get_timelapse_path(
                camera, all_tiers=True
            ):
                paths += timelapse_path

            for domain in SnapshotDomain:
                paths += self._storage.get_snapshots_path(
                    camera, domain, all_tiers=True
                )
        return paths

    def _get_paths(self) -> list[str]:
        """Return the storage paths the job processes."""
        return []

    def _get_recording_paths(self) -> dict[str, tuple[str, ...]]:
        """Return recording roots needed by event recovery, resolved in the parent."""
        return {}

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the cleanup job."""

    @property
    def interval_trigger(self) -> IntervalTrigger:
        """Return the trigger interval for the cleanup job."""
        return self._interval_trigger

    @staticmethod
    @abstractmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the cleanup job inside the child process."""

    def run(self) -> None:
        """Run the cleanup job in a child process."""
        with self.run_lock:
            if self.running:
                return
            self.running = True

        process_config = CleanupJobConfig(
            name=self.name,
            paths=self._get_paths(),
            recording_paths=self._get_recording_paths(),
            ignored_files=frozenset(self._storage.ignored_files),
            sensitive_strings=tuple(SensitiveInformationFilter.sensitive_strings),
            log_level=logging.getLogger().level,
            tier_markers=dict(self._storage.config.get(CONFIG_TIER_MARKERS, {})),
        )
        process = RestartableProcess(
            name=self.name,
            target=run_cleanup_job,
            args=(type(self), process_config, self.kill_event),
            daemon=True,
            register=False,
            context=CHILD_PROCESS_START_METHOD,
            child_logger_names=(LOGGER.name,),
        )
        process.start()
        process.join()
        process.stop()

        with self.run_lock:
            self.running = False


class BaseTableCleanupJob(BaseCleanupJob):
    """Base class for database table cleanup jobs that use batch processing."""

    @staticmethod
    def batch_delete_orphaned(
        runner: CleanupJobRunner,
        session: Session,
        table: type[PostProcessorResults | Motion | Objects],
        path_column: Any,
    ) -> None:
        """Delete orphaned records in batches using cursor-based pagination.

        Args:
            session: Database session
            table: SQLAlchemy table to clean up
            path_column: Column containing the file path to check against Files table
        """
        total_deleted = 0
        last_id = 0
        now = time.time()

        # Calculate cutoff time for 5 minutes ago
        cutoff_time = utcnow() - datetime.timedelta(minutes=5)

        while True:
            if runner.kill_event.is_set():
                break

            # Get next batch of records that need checking
            batch = session.execute(
                select(table.id, path_column)
                .where(
                    and_(
                        table.id > last_id,
                        ~exists().where(Files.path == path_column),
                        table.created_at
                        < cutoff_time,  # Only consider records older than 5 minutes
                    )
                )
                .order_by(table.id)
                .limit(BATCH_SIZE)
            ).all()

            if not batch:
                break

            # Update cursor
            last_id = batch[-1][0]

            # Get IDs to delete
            batch_ids = [row[0] for row in batch]

            # Delete the batch
            result = cast(  # https://github.com/sqlalchemy/sqlalchemy/issues/12813
                "CursorResult[Any]",
                session.execute(delete(table).where(table.id.in_(batch_ids))),
            )
            session.commit()
            total_deleted += result.rowcount
            runner.log_progress(
                f"{runner.name} deleted {result.rowcount} records in batch"
            )
            time.sleep(1)

        LOGGER.debug(
            "%s deleted %d total records, took %s",
            runner.name,
            total_deleted,
            time.time() - now,
        )


class OrphanedFilesCleanup(BaseCleanupJob):
    """Cleanup job that removes files with no corresponding database records.

    Walks through recordings, segments and snapshots directories to find and delete
    any files that don't have a matching record in the Files table.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_FILES.value

    def _get_paths(self) -> list[str]:
        """Return the storage paths the job processes."""
        return self._get_all_paths()

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        deleted_count = 0
        if not runner.paths:
            return

        total_files_processed = 0
        with runner.get_session() as session:
            for path in runner.paths:
                if runner.kill_event.is_set():
                    break
                if not tier_markers_available(path, runner.tier_markers):
                    LOGGER.warning("Skipping cleanup of unavailable tier %s", path)
                    continue
                LOGGER.debug("%s checking %s", runner.name, path)
                for root, _, files in os.walk(path):
                    if runner.kill_event.is_set():
                        break

                    files_processed = 0
                    for file in files:
                        if runner.kill_event.is_set():
                            break

                        total_files_processed += 1
                        files_processed += 1
                        if file in runner.ignored_files:
                            continue
                        file_path = os.path.join(root, file)
                        # A source intent is durable evidence, including malformed
                        # records; only move recovery may remove it.
                        if is_source_move_marker(file):
                            continue
                        if is_tier_move_temp_file(file):
                            # Run-init identity and its prepared temporary are
                            # persistent recovery state, not stale scratch files.
                            if file == ".viseron-tier-move-run-init.json":
                                continue
                            if is_run_directory(os.path.basename(root)):
                                camera = os.path.abspath(os.path.dirname(root))
                                try:
                                    with camera_lock("/tmp/viseron-run-locks", camera):
                                        if remove_stale_temporary(
                                            file_path,
                                            lambda file_path=file_path: (
                                                require_tier_markers(
                                                    file_path, runner.tier_markers
                                                )
                                            ),
                                        ):
                                            deleted_count += 1
                                except OSError:
                                    LOGGER.debug(
                                        "Cannot clean temporary %s",
                                        file_path,
                                        exc_info=True,
                                    )
                            continue
                        if is_tier_marker(file_path, runner.tier_markers):
                            continue
                        if not tier_markers_available(file_path, runner.tier_markers):
                            continue
                        try:
                            if is_run_directory(os.path.basename(root)):
                                camera = os.path.abspath(os.path.dirname(root))
                                # All publishers and acknowledgers must take this lock.
                                with camera_lock("/tmp/viseron-run-locks", camera):
                                    file_exists = session.execute(
                                        select(Files).where(Files.path == file_path)
                                    ).first()
                                    if (
                                        not file_exists
                                        and not protected_by_pending(file_path)
                                        and not source_move_pending(file_path)
                                    ):
                                        if os.path.exists(file_path):
                                            os.remove(file_path)
                                            deleted_count += 1
                            else:
                                file_exists = session.execute(
                                    select(Files).where(Files.path == file_path)
                                ).first()
                                if not file_exists and os.path.exists(file_path):
                                    os.remove(file_path)
                                    deleted_count += 1
                        except OSError:
                            LOGGER.warning(
                                "Unable to check cleanup candidate %s",
                                file_path,
                                exc_info=True,
                            )
                        runner.log_progress(
                            f"{runner.name} processed {files_processed}/{len(files)} "
                            f"files in {root}",
                        )
                        if total_files_processed % 100 == 0:
                            time.sleep(1)
                    LOGGER.debug(
                        f"{runner.name} processed {files_processed}/{len(files)} "
                        f"files in {root}",
                    )

        LOGGER.debug(
            "%s deleted %d/%d processed files, took %s",
            runner.name,
            deleted_count,
            total_files_processed,
            time.time() - now,
        )


class OrphanedDatabaseFilesCleanup(BaseCleanupJob):
    """Cleanup job that removes rows from Files with no corresponding files on disk."""

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_DB_FILES.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        total_deleted = 0
        last_id = 0
        total_files_processed = 0

        with runner.get_session() as session:
            count = session.execute(
                select(
                    func.count(),
                ).select_from(Files)
            ).scalar()

            while True:
                if runner.kill_event.is_set():
                    break

                # Get next batch of files to check
                time.sleep(1)
                files = session.execute(
                    select(Files.id, Files.path)
                    .where(Files.id > last_id)
                    .order_by(Files.id)
                    .limit(BATCH_SIZE)
                ).all()

                if not files:
                    break

                # Update cursor
                last_id = files[-1][0]

                # Find records where files don't exist
                to_delete = []
                for file_id, file_path in files:
                    run_dir = os.path.dirname(file_path)
                    if not is_run_directory(os.path.basename(run_dir)):
                        if tier_markers_available(
                            file_path, runner.tier_markers
                        ) and file_is_missing(file_path):
                            to_delete.append(file_id)
                        continue
                    try:
                        with camera_lock(
                            "/tmp/viseron-run-locks",
                            os.path.abspath(os.path.dirname(run_dir)),
                        ):
                            if (
                                tier_markers_available(file_path, runner.tier_markers)
                                and not source_move_pending(file_path)
                                and not protected_by_pending(file_path)
                                and file_is_missing(file_path)
                            ):
                                result = session.execute(
                                    delete(Files).where(
                                        Files.id == file_id, Files.path == file_path
                                    )
                                )
                                session.commit()
                                total_deleted += result.rowcount
                    except OSError:
                        LOGGER.debug(
                            "Deferred missing run-file cleanup: %s",
                            file_path,
                            exc_info=True,
                        )

                if to_delete:
                    result = cast(
                        "CursorResult[Any]",
                        session.execute(delete(Files).where(Files.id.in_(to_delete))),
                    )
                    session.commit()
                    total_deleted += result.rowcount
                    LOGGER.debug(
                        "%s deleted %d rows in batch", runner.name, result.rowcount
                    )
                total_files_processed += len(files)
                runner.log_progress(
                    f"{runner.name} processed {total_files_processed}/{count} files"
                )

        LOGGER.debug(
            "%s deleted %d total database records for non-existent files, took %s",
            runner.name,
            total_deleted,
            time.time() - now,
        )


class ZeroSizeFilesCleanup(BaseCleanupJob):
    """Cleanup job that handles zero-size files in the database.

    For any Files in the database with zero size, the job will attempt to update the
    size. If the size on disk is zero the file is removed.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ZERO_SIZE_FILES.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        processed = 0
        updated = 0
        deleted = 0
        last_id = 0

        # Only consider files older than 5 minutes
        cutoff_time = utcnow() - datetime.timedelta(minutes=5)

        with runner.get_session() as session:
            while True:
                if runner.kill_event.is_set():
                    break

                batch = (
                    session.execute(
                        select(Files)
                        .where(
                            and_(
                                Files.id > last_id,
                                Files.size == 0,
                                Files.created_at < cutoff_time,
                            )
                        )
                        .order_by(Files.id)
                        .limit(BATCH_SIZE)
                    )
                    .scalars()
                    .all()
                )

                if not batch:
                    break

                last_id = batch[-1].id

                to_delete_ids: list[int] = []
                for file_row in batch:
                    if runner.kill_event.is_set():
                        break
                    processed += 1
                    path = file_row.path
                    if not tier_markers_available(path, runner.tier_markers):
                        continue
                    run_dir = os.path.dirname(path)
                    lock = (
                        camera_lock(
                            "/tmp/viseron-run-locks",
                            os.path.abspath(os.path.dirname(run_dir)),
                        )
                        if is_run_directory(os.path.basename(run_dir))
                        else contextlib.nullcontext()
                    )
                    try:
                        with lock:
                            if source_move_pending(path) or protected_by_pending(path):
                                continue
                            stat_size = os.path.getsize(path)
                            if stat_size > 0:
                                file_row.size = stat_size
                                updated += 1
                            else:
                                os.remove(path)
                                if is_run_directory(os.path.basename(run_dir)):
                                    session.execute(
                                        delete(Files).where(
                                            Files.id == file_row.id, Files.path == path
                                        )
                                    )
                                    session.commit()
                                else:
                                    to_delete_ids.append(file_row.id)
                                deleted += 1
                    except OSError as error:
                        LOGGER.debug(
                            "Skipping inaccessible or busy file %s: %s", path, error
                        )
                        continue

                if to_delete_ids:
                    session.execute(delete(Files).where(Files.id.in_(to_delete_ids)))
                session.commit()
                time.sleep(0.5)

        LOGGER.debug(
            "%s processed %d files, updated %d sizes, deleted %d records, took %s",
            runner.name,
            processed,
            updated,
            deleted,
            time.time() - now,
        )


class EmptyFoldersCleanup(BaseCleanupJob):
    """Cleanup job that removes empty directories from the storage locations.

    Walks through all storage paths (recordings, segments, thumbnails, snapshots)
    and removes any empty directories encountered. Uses a bottom-up traversal to
    ensure nested empty directories are handled properly.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.EMPTY_FOLDERS.value

    def _get_paths(self) -> list[str]:
        """Return the storage paths the job processes."""
        return self._get_all_paths()

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        deleted_count = 0
        processed_count = 0
        if not runner.paths:
            return

        for path in runner.paths:
            if not tier_markers_available(path, runner.tier_markers):
                LOGGER.warning("Skipping cleanup of unavailable tier %s", path)
                continue
            time.sleep(1)
            for root, dirs, files in os.walk(path, topdown=False):
                if not tier_markers_available(root, runner.tier_markers):
                    continue
                processed_count += 1
                runner.log_progress(
                    f"{runner.name} processed {processed_count} folders"
                )
                if root == path:
                    continue
                if is_run_directory(os.path.basename(root)):
                    camera = os.path.abspath(os.path.dirname(root))

                    def has_refs(directory: str) -> bool:
                        with runner.get_session() as session:
                            return (
                                session.execute(
                                    select(Files.id)
                                    .where(Files.directory == directory)
                                    .limit(1)
                                ).first()
                                is not None
                            )

                    if reclaim_empty_run(
                        "/tmp/viseron-run-locks",
                        camera,
                        root,
                        lambda root=root: require_tier_markers(
                            root, runner.tier_markers
                        ),
                        has_refs,
                    ):
                        deleted_count += 1
                    continue
                if not dirs and not files:
                    LOGGER.debug("Deleting folder %s", root)
                    os.rmdir(root)
                    deleted_count += 1

        LOGGER.debug(
            "%s deleted %d empty folders, took %s",
            runner.name,
            deleted_count,
            time.time() - now,
        )


class OrphanedThumbnailsCleanup(BaseCleanupJob):
    """Cleanup job that removes thumbnail files with no database records."""

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_THUMBNAILS.value

    def _get_paths(self) -> list[str]:
        """Return the storage paths the job processes."""
        cameras = self._get_cameras() or {}
        paths = []
        for camera in cameras.values():
            paths += self._storage.get_thumbnails_path(camera, all_tiers=True)
        return paths

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        deleted_count = 0
        total_files_processed = 0

        if not runner.paths:
            return

        with runner.get_session() as session:
            for thumbnails_path in runner.paths:
                files_processed = 0
                if not tier_markers_available(thumbnails_path, runner.tier_markers):
                    LOGGER.warning("Skipping unavailable tier %s", thumbnails_path)
                    continue
                if not os.path.exists(thumbnails_path):
                    continue

                files_to_check: list[str] = []
                for root, _, files in os.walk(thumbnails_path):
                    files_to_check.extend(
                        os.path.join(root, f)
                        for f in files
                        if f not in runner.ignored_files
                        and not is_tier_move_temp_file(f)
                    )

                # Process files in batches
                for i in range(0, len(files_to_check), BATCH_SIZE):
                    if runner.kill_event.is_set():
                        break

                    batch = files_to_check[i : i + BATCH_SIZE]
                    existing_thumbnails = {
                        row[0]
                        for row in session.execute(
                            select(Recordings.thumbnail_path).where(
                                Recordings.thumbnail_path.in_(batch)
                            )
                        ).all()
                    }

                    # Delete files that don't exist in database
                    for file_path in batch:
                        if is_tier_marker(file_path, runner.tier_markers):
                            continue
                        if not tier_markers_available(file_path, runner.tier_markers):
                            continue
                        if file_path not in existing_thumbnails and os.path.exists(
                            file_path
                        ):
                            os.remove(file_path)
                            deleted_count += 1
                        total_files_processed += 1
                        files_processed += 1

                    runner.log_progress(
                        f"{runner.name} processed "
                        f"{files_processed}/{len(files_to_check)} "
                        f"files in {thumbnails_path}"
                    )
                    time.sleep(1)
                LOGGER.debug(
                    f"{runner.name} processed "
                    f"{files_processed}/{len(files_to_check)} "
                    f"files in {thumbnails_path}"
                )

        LOGGER.debug(
            "%s deleted %d/%d orphaned thumbnails, took %s",
            runner.name,
            deleted_count,
            total_files_processed,
            time.time() - now,
        )


class OrphanedEventClipsCleanup(BaseCleanupJob):
    """Cleanup job that removes clip files with no corresponding database records."""

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_EVENT_CLIPS.value

    def _get_paths(self) -> list[str]:
        """Return the storage paths the job processes."""
        cameras = self._get_cameras() or {}
        paths = []
        for camera in cameras.values():
            paths += self._storage.get_event_clips_path(camera, all_tiers=True)
        return paths

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        deleted_count = 0
        total_files_processed = 0

        if not runner.paths:
            return

        with runner.get_session() as session:
            for event_clips_path in runner.paths:
                files_processed = 0
                if not tier_markers_available(event_clips_path, runner.tier_markers):
                    LOGGER.warning("Skipping unavailable tier %s", event_clips_path)
                    continue
                if not os.path.exists(event_clips_path):
                    continue

                # Collect all files first
                files_to_check: list[str] = []
                for root, _, files in os.walk(event_clips_path):
                    files_to_check.extend(
                        os.path.join(root, f)
                        for f in files
                        if f not in runner.ignored_files
                        and not is_tier_move_temp_file(f)
                    )

                # Process files in batches
                for i in range(0, len(files_to_check), BATCH_SIZE):
                    if runner.kill_event.is_set():
                        break

                    batch = files_to_check[i : i + BATCH_SIZE]
                    existing_clips = {
                        row[0]
                        for row in session.execute(
                            select(Recordings.clip_path).where(
                                Recordings.clip_path.in_(batch)
                            )
                        ).all()
                    }

                    # Delete files that don't exist in database
                    for file_path in batch:
                        if is_tier_marker(file_path, runner.tier_markers):
                            continue
                        if not tier_markers_available(file_path, runner.tier_markers):
                            continue
                        if file_path not in existing_clips and os.path.exists(
                            file_path
                        ):
                            os.remove(file_path)
                            deleted_count += 1
                        total_files_processed += 1
                        files_processed += 1

                    runner.log_progress(
                        f"{runner.name} processed "
                        f"{files_processed}/{len(files_to_check)} "
                        f"files in {event_clips_path}"
                    )
                    time.sleep(1)
                LOGGER.debug(
                    f"{runner.name} processed "
                    f"{files_processed}/{len(files_to_check)} "
                    f"files in {event_clips_path}"
                )

        LOGGER.debug(
            "%s deleted %d/%d orphaned clips, took %s",
            runner.name,
            deleted_count,
            total_files_processed,
            time.time() - now,
        )


@contextlib.contextmanager
def _empty_recording_run(
    runner: CleanupJobRunner, recording: Recordings
) -> Iterator[bool]:
    """Prove a whole run is drained; uncertainty keeps the event for a later pass.

    This is deliberately coarser than normal callback-driven retention. It only
    recovers lost callbacks once the run has no media, staging or recovery state.
    Unscoped legacy events require manual reconciliation.
    """
    roots = getattr(runner, "recording_paths", {}).get(recording.camera_identifier)
    run_id = recording.recorder_run_id
    if not roots or not run_id or not is_run_directory(run_id):
        yield False
        return
    with contextlib.ExitStack() as locks:
        empty = False
        try:
            for root in sorted(set(roots)):
                locks.enter_context(camera_lock("/tmp/viseron-run-locks", root))
                require_tier_markers(root, runner.tier_markers)
                if not stat.S_ISDIR(os.stat(root, follow_symlinks=False).st_mode):
                    break
                run = os.path.join(root, run_id)
                try:
                    info = os.stat(run, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISDIR(info.st_mode):
                    break
                with os.scandir(run) as entries:
                    if any(
                        entry.name
                        not in {"init.mp4", ".viseron-tier-move-run-init.json"}
                        or not entry.is_file(follow_symlinks=False)
                        for entry in entries
                    ):
                        break
            else:
                for root in roots:
                    require_tier_markers(root, runner.tier_markers)
                empty = True
        except OSError:
            LOGGER.debug("Deferring event recovery for %s", recording.id, exc_info=True)
        yield empty


class OrphanedRecordingsCleanup(BaseCleanupJob):
    """Cleanup job that removes orphaned recording entries from the database."""

    def _get_recording_paths(self) -> dict[str, tuple[str, ...]]:
        """Include all tiers, current spool and retained legacy staging."""
        paths = {}
        for identifier, camera in (self._get_cameras() or {}).items():
            roots = list(self._storage.get_segments_path(camera, all_tiers=True))
            roots.append(camera.temp_segments_folder)
            legacy = TEMP_DIR + camera.segments_folder
            try:
                os.lstat(legacy)
            except FileNotFoundError:
                pass
            else:
                roots.append(legacy)
            paths[identifier] = tuple(sorted({os.path.abspath(root) for root in roots}))
        return paths

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_RECORDINGS.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        total_deleted = 0
        total_processed = 0

        with runner.get_session() as session:
            count = session.execute(
                select(func.count())
                .select_from(Recordings)
                .where(Recordings.end_time.is_not(None))
            ).scalar()

            if not count:
                return

            last_id = 0
            while True:
                if runner.kill_event.is_set():
                    break

                # Get next batch of recordings
                batch = session.execute(
                    select(Recordings)
                    .where(
                        and_(Recordings.id > last_id, Recordings.end_time.is_not(None))
                    )
                    .order_by(Recordings.id)
                    .limit(BATCH_SIZE)
                ).all()

                if not batch:
                    break

                # Update cursor
                last_id = batch[-1][0].id

                for (recording,) in batch:
                    if runner.kill_event.is_set():
                        break
                    # Missing rows alone do not prove that footage was retired.
                    # Hold every publication lock until the deletion commits.
                    with _empty_recording_run(runner, recording) as empty:
                        if (
                            not empty
                            or runner.kill_event.is_set()
                            or recording.recorder_run_id is None
                        ):
                            continue
                        has_segments = session.execute(
                            select(Files.id)
                            .where(
                                Files.category == TIER_CATEGORY_RECORDER,
                                Files.subcategory == TIER_SUBCATEGORY_SEGMENTS,
                                Files.camera_identifier == recording.camera_identifier,
                                Files.directory.endswith(
                                    "/" + recording.recorder_run_id
                                ),
                                Files.orig_ctime < recording.end_time,
                                or_(
                                    Files.duration.is_(None),
                                    Files.orig_ctime
                                    + func.make_interval(
                                        0, 0, 0, 0, 0, 0, Files.duration
                                    )
                                    >= recording.adjusted_start_time,
                                ),
                            )
                            .limit(1)
                        ).first()
                        if has_segments:
                            continue
                        result = cast(
                            "CursorResult[Any]",
                            session.execute(
                                delete(Recordings).where(
                                    Recordings.id == recording.id,
                                    Recordings.end_time.is_not(None),
                                )
                            ),
                        )
                        session.commit()
                        total_deleted += result.rowcount
                    time.sleep(0.1)

                total_processed += len(batch)
                runner.log_progress(
                    f"{runner.name} processed {total_processed}/{count} recordings"
                )
                time.sleep(1)

        LOGGER.debug(
            "%s deleted %d/%d orphaned recordings, took %s",
            runner.name,
            total_deleted,
            total_processed,
            time.time() - now,
        )


class OrphanedPostProcessorResultsCleanup(BaseTableCleanupJob):
    """Cleanup job that removes orphaned post-processor results from the database.

    Deletes records from the PostProcessorResults table where the referenced
    snapshot file no longer exists in the Files table, ensuring that results
    without associated files are removed.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_POSTPROCESSOR_RESULTS.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        LOGGER.debug("Running %s", runner.name)
        with runner.get_session() as session:
            BaseTableCleanupJob.batch_delete_orphaned(
                runner,
                session,
                PostProcessorResults,
                PostProcessorResults.snapshot_path,
            )


class OrphanedObjectsCleanup(BaseTableCleanupJob):
    """Cleanup job that removes orphaned object detection records from the database.

    Deletes records from the Objects table where the referenced snapshot file
    no longer exists in the Files table, ensuring that object detections
    without associated files are removed.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_OBJECTS.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        LOGGER.debug("Running %s", runner.name)
        with runner.get_session() as session:
            BaseTableCleanupJob.batch_delete_orphaned(
                runner, session, Objects, Objects.snapshot_path
            )


class OrphanedMotionCleanup(BaseTableCleanupJob):
    """Cleanup job that removes orphaned motion detection records from the database.

    Deletes records from the Motion table where the referenced snapshot file
    no longer exists in the Files table, ensuring that motion detections
    without associated files are removed.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.ORPHANED_MOTION.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        LOGGER.debug("Running %s", runner.name)
        with runner.get_session() as session:
            BaseTableCleanupJob.batch_delete_orphaned(
                runner, session, Motion, Motion.snapshot_path
            )


class OldEventsCleanup(BaseCleanupJob):
    """Cleanup job that removes old events from the database.

    Deletes records from the Events table that are older than a specified
    number of days.
    """

    @property
    def name(self) -> str:
        """Return job name."""
        return CleanupJobNames.OLD_EVENTS.value

    @staticmethod
    def work(runner: CleanupJobRunner) -> None:
        """Run the job."""
        now = time.time()
        LOGGER.debug("Running %s", runner.name)
        with runner.get_session() as session:
            stmt = delete(Events).where(
                Events.created_at < utcnow() - datetime.timedelta(days=7)
            )
            result = cast("CursorResult[Any]", session.execute(stmt))
            session.commit()
            LOGGER.debug(
                "%s deleted %d old events, took %s",
                runner.name,
                result.rowcount,
                time.time() - now,
            )


class CleanupManager:
    """Manager class that handles scheduling and running of cleanup jobs.

    Initializes all cleanup jobs and manages their execution through a background
    scheduler. Provides functionality to start and stop the cleanup process for
    both filesystem and database maintenance tasks.
    """

    def __init__(self, vis: Viseron, storage: Storage) -> None:
        self._vis = vis
        self.jobs: list[BaseCleanupJob] = [
            OrphanedFilesCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedDatabaseFilesCleanup(
                vis, storage, CronTrigger(hour=0, jitter=3600)
            ),
            ZeroSizeFilesCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            EmptyFoldersCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedThumbnailsCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedEventClipsCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedRecordingsCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedPostProcessorResultsCleanup(
                vis, storage, CronTrigger(hour=0, jitter=3600)
            ),
            OrphanedObjectsCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OrphanedMotionCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
            OldEventsCleanup(vis, storage, CronTrigger(hour=0, jitter=3600)),
        ]
        vis.register_signal_handler(VISERON_SIGNAL_SHUTDOWN, self.stop)

    def run_job(self, job_name: CleanupJobNames) -> None:
        """Run a specific cleanup job."""
        for job in self.jobs:
            if job.name == job_name.value:
                with job.run_lock:
                    if job.running:
                        return
                LOGGER.debug("Running cleanup job %s", job.name)
                RestartableThread(
                    name=f"run_job_{job.name}",
                    target=job.run,
                    register=False,
                    daemon=True,
                ).start()
                return

    def start(self) -> None:
        """Start the cleanup scheduler."""
        for job in self.jobs:
            self._vis.background_scheduler.add_job(
                job.run,
                trigger=job.interval_trigger,
                name=job.name,
                id=job.name,
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )

    def stop(self) -> None:
        """Stop the cleanup scheduler."""
        LOGGER.debug("Stopping cleanup jobs")
        for job in self.jobs:
            LOGGER.debug("Sending kill event to %s", job.name)
            job.kill_event.set()
