"""Tier size/age checker for the storage component."""

from __future__ import annotations

import contextlib
import hashlib
import datetime
import logging
import os
import re
import shutil
import stat
import tempfile
import threading
from typing import TYPE_CHECKING, cast

import numpy as np
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import scoped_session, sessionmaker

from viseron.components.storage.segment_moves import begin_copy
from viseron.components.storage.segment_moves import protects as segment_move_pending
from viseron.components.storage.asset_moves import (
    asset_locks,
    asset_move_pending,
    move_asset,
)
from viseron.components.storage.dispatch_fence import token_permits_start
from viseron.components.storage.pending_journal import (
    prepare,
    sync_dir,
    protected_by_pending,
    publish_or_verify,
)
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
)
from viseron.components.storage.source_move_intent import (
    prepare_source_intent,
    set_source_digest,
    source_move_pending,
)
from viseron.components.storage.const import ENGINE
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.pressure import event_fence
from viseron.components.storage.util import (
    TIER_MOVE_TEMP_PREFIX,
    is_tier_marker,
    require_tier_markers,
)
from viseron.const import CAMERA_SEGMENT_DURATION
from viseron.domains.camera.schedule import schedule_state_changes
from viseron.helpers import utcnow

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import ColumnElement
    from sqlalchemy.orm import Session

    from viseron.components.storage.storage_subprocess import (
        DataItem,
        DataItemDeleteFile,
        DataItemMoveFile,
    )

LOGGER = logging.getLogger(__name__)

# Upper bound on the schedule windows the fast-path gate resolves before giving
# up and letting the full check decide.
MAX_SCHEDULE_CHANGES = 100

FILES_DTYPE = np.dtype(
    [
        ("id", np.int64),
        ("size", np.int64),
        ("orig_ctime", np.int64),
        ("path", "U512"),
        ("tier_path", "U512"),
    ]
)

RECORDINGS_DTYPE = np.dtype(
    [
        ("id", np.int64),
        ("start_time", np.int64),
        ("adjusted_start_time", np.int64),
        ("end_time", np.int64),
        ("created_at", np.int64),
        ("recorder_run_id", "U64"),
        ("active", np.bool_),
    ]
)

RECORDINGS_FILES_DTYPE = np.dtype(
    [
        ("recording_id", np.int64),
        ("id", np.int64),
        ("size", np.int64),
        ("orig_ctime", np.int64),
        ("path", "U512"),
        ("tier_path", "U512"),
    ]
)


class Worker:
    """Worker process for checking storage tiers in a separate shell."""

    def __init__(
        self,
    ) -> None:
        self._get_session: Callable[[], Session] = scoped_session(
            sessionmaker(bind=ENGINE)
        )
        self._last_call: dict[str, float] = {}
        self._check_locks: dict[str, threading.Lock] = {}
        self._checks_in_progress: dict[str, bool] = {}

    @staticmethod
    def _tier_files_filter(item: DataItem) -> tuple[ColumnElement[bool], ...]:
        """Return the filter selecting the files a DataItem covers."""
        return (
            Files.camera_identifier == item.camera_identifier,
            Files.tier_id == item.tier_id,
            Files.category == item.category,
            Files.subcategory.in_(item.subcategories),
        )

    def _inactive_files_exist(
        self,
        session: Session,
        item: DataItem,
        oldest_ctime: datetime.datetime,
        now: datetime.datetime,
    ) -> bool:
        """Return if the continuous schedule has anything left to evict.

        Files recorded while the schedule was inactive are kept only as event
        pre-roll, so they can go once they are older than the lookback buffer.

        Age alone is not enough to ask about: a tier nearly always holds files
        older than the lookback buffer, which would make the answer yes for
        every camera with a schedule. The inactive windows are worked out first
        instead, and only files falling inside one of them count.
        """
        cutoff = now - datetime.timedelta(
            seconds=item.continuous_lookback_seconds + CAMERA_SEGMENT_DURATION
        )
        if oldest_ctime >= cutoff:
            return False

        changes = schedule_state_changes(
            cast("list[dict[str, str]]", item.continuous_schedule),
            item.continuous_schedule_timezone or "UTC",
            oldest_ctime,
            cutoff,
            max_changes=MAX_SCHEDULE_CHANGES,
        )
        if len(changes) > MAX_SCHEDULE_CHANGES:
            # Too many windows to express as one query, let the full check decide
            return True

        inactive_windows: list[tuple[datetime.datetime, datetime.datetime]] = []
        for index, (timestamp, active) in enumerate(changes):
            if active:
                continue
            inactive_windows.append(
                (
                    # The first change is the range start, which is oldest_ctime
                    # itself. Use it verbatim to keep sub-second precision.
                    oldest_ctime
                    if index == 0
                    else datetime.datetime.fromtimestamp(
                        timestamp, tz=datetime.timezone.utc
                    ),
                    datetime.datetime.fromtimestamp(
                        changes[index + 1][0], tz=datetime.timezone.utc
                    )
                    if index + 1 < len(changes)
                    else cutoff,
                )
            )

        if not inactive_windows:
            return False

        return (
            session.execute(
                select(Files.id)
                .where(
                    *self._tier_files_filter(item),
                    or_(
                        *[
                            and_(
                                Files.orig_ctime >= window_start,
                                Files.orig_ctime < window_end,
                            )
                            for window_start, window_end in inactive_windows
                        ]
                    ),
                )
                .limit(1)
            ).first()
            is not None
        )

    def _should_check_tier_files(self, item: DataItem) -> bool:
        """Quick aggregate check if tier actually needs processing.

        For files-only checks, does a SUM(size)/MIN(orig_ctime) query
        to determine if any files exceed limits.
        """
        if not item.files_enabled:
            return False

        with self._get_session() as session:
            result = session.execute(
                select(func.sum(Files.size), func.min(Files.orig_ctime)).where(
                    *self._tier_files_filter(item)
                )
            ).first()

            if result is None or result[0] is None:
                return False

            total_size = result[0]
            oldest_ctime = result[1]
            now = utcnow()

            if 0 < item.max_bytes <= total_size:
                return True
            if (
                item.max_age.total_seconds() > 0
                and total_size >= item.min_bytes
                and oldest_ctime < now - item.max_age
            ):
                return True
            if (  # noqa: SIM103
                item.continuous_schedule is not None
                and self._inactive_files_exist(session, item, oldest_ctime, now)
            ):
                return True

            return False

    def _check_tier(self, item: DataItem) -> None:
        if (
            item.category == "recorder"
            and "segments" in item.subcategories
            and not item.events_enabled
        ):
            # Continuous-only retention governs closed events too. Capture them
            # before selecting files; a later event must not inherit this decision.
            with self._get_session() as session:
                item.retention_recording_ids = tuple(
                    session.execute(
                        select(Recordings.id).where(
                            Recordings.camera_identifier == item.camera_identifier,
                            Recordings.end_time.is_not(None),
                        )
                    ).scalars()
                )
        files = np.empty(0, dtype=FILES_DTYPE)
        if item.cmd == "check_tier" and item.files_enabled:
            LOGGER.debug(
                "Received check_tier command for files "
                "for %s tier %s category %s subcategory %s",
                item.camera_identifier,
                item.tier_id,
                item.category,
                item.subcategories[0],
            )
            files = self.check_tier_files(item)

        recordings = np.empty(0, dtype=RECORDINGS_DTYPE)
        if item.cmd == "check_tier" and item.events_enabled:
            LOGGER.debug(
                "Received check_tier command for recordings "
                "for %s tier %s category %s subcategory %s",
                item.camera_identifier,
                item.tier_id,
                item.category,
                item.subcategories[0],
            )
            recordings = self.check_tier_recordings(item)

        if item.events_enabled and not item.files_enabled:
            item.data = recordings
        elif item.files_enabled and not item.events_enabled:
            item.data = files
        elif item.events_enabled and item.files_enabled:
            LOGGER.debug(
                "Finding overlapping files and recordings "
                "for %s tier %s category %s subcategory %s",
                item.camera_identifier,
                item.tier_id,
                item.category,
                item.subcategories[0],
            )
            # Find overlapping IDs
            if files.size > 0 and recordings.size > 0:
                overlapping_ids = np.intersect1d(files["id"], recordings["id"])
                if overlapping_ids.size > 0:
                    # Return full recordings array for overlapping IDs
                    item.data = recordings[np.isin(recordings["id"], overlapping_ids)]
                else:
                    item.data = np.empty(0, dtype=recordings.dtype)
            else:
                item.data = np.empty(
                    0,
                    dtype=recordings.dtype if recordings.size > 0 else files.dtype,
                )
        else:
            item.data = np.empty(0, dtype=FILES_DTYPE)

        LOGGER.debug(
            "Found %d files to move for %s tier %s category %s subcategory %s",
            len(item.data) if item.data is not None else 0,
            item.camera_identifier,
            item.tier_id,
            item.category,
            item.subcategories[0],
        )

    def check_tier(self, item: DataItem) -> None:
        """Handle input commands in the child process.

        Calls are throttled based on the throttle_period defined in the tier.
        """
        if item.camera_identifier not in self._check_locks:
            self._check_locks[item.camera_identifier] = threading.Lock()
        if item.throttle_key not in self._last_call:
            self._last_call[item.throttle_key] = 0
        if item.camera_identifier not in self._checks_in_progress:
            self._checks_in_progress[item.camera_identifier] = False

        with self._check_locks[item.camera_identifier]:
            if self._checks_in_progress[item.camera_identifier]:
                item.deferred = True
                return
            now = utcnow().timestamp()
            throttle_period = item.throttle_period.total_seconds()
            last_call = self._last_call[item.throttle_key]
            if throttle_period > 0 and (now - last_call) < throttle_period:
                item.data = None
                return
            self._checks_in_progress[item.camera_identifier] = True

        try:
            self._check_tier(item)
        finally:
            LOGGER.debug(
                "Execution took %.2f seconds for %s tier %s category %s subcategory %s",
                utcnow().timestamp() - now,
                item.camera_identifier,
                item.tier_id,
                item.category,
                item.subcategories[0],
            )
            with self._check_locks[item.camera_identifier]:
                self._last_call[item.throttle_key] = utcnow().timestamp()
                self._checks_in_progress[item.camera_identifier] = False

    def move_file(self, item: DataItemMoveFile) -> None:
        """Move file from source to destination."""
        move_file(
            self._get_session,
            item.src,
            item.dst,
            LOGGER,
            tier_markers=item.tier_markers,
            camera_identifier=item.camera_identifier,
            publication_meta=item.publication_meta,
        )

    def delete_file(self, item: DataItemDeleteFile) -> None:
        """Delete file."""
        item.deferred = delete_file(
            self._get_session,
            item.src,
            LOGGER,
            tier_markers=item.tier_markers,
            retention_recording_ids=item.retention_recording_ids,
        ) is False

    def work_input(self, item: DataItem | DataItemMoveFile | DataItemDeleteFile):
        """Perform work on input item from child process."""
        try:
            if item.cmd == "move_file" and item.dispatch_token is not None:
                if not token_permits_start(item.dispatch_token):
                    item.error = "retry cancelled after handler shutdown"
                    return
            if item.cmd == "check_tier":
                self.check_tier(item)
            if item.cmd == "move_file":
                # Durable holds are resumed by indexed move replay. A
                # normal tier scan must not independently copy the same file.
                if item.dispatch_token is None and source_move_pending(item.src):
                    item.deferred = True
                    item.deferred_by_hold = True
                    return
                self.move_file(item)
            if item.cmd == "delete_file":
                self.delete_file(item)
        except CameraLockBusy as e:
            if item.cmd in ("move_file", "delete_file"):
                item.deferred = True
                LOGGER.debug(
                    "Storage operation deferred due to camera lock contention: %s", e
                )
            else:
                LOGGER.error("Error processing command: %s, error: %s", item, e)
                item.error = str(e)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error(
                "Error processing command: %s, error: %s",
                item,
                e,
            )
            item.error = str(e)

    def load_tier(self, item: DataItem):
        """Load the tier data for the camera."""
        data = load_tier(
            self._get_session,
            item.category,
            item.subcategories,
            item.tier_id,
            item.camera_identifier,
        )
        LOGGER.debug(
            "Loaded %d files into numpy array",
            len(data),
        )
        return data

    def load_recordings(self, item: DataItem):
        """Load the recordings data for the camera."""
        data = load_recordings(
            self._get_session,
            item.camera_identifier,
        )
        LOGGER.debug(
            "Loaded %d recordings into numpy array",
            len(data),
        )
        return data

    def check_tier_files(self, item: DataItem):
        """Check the tier using the loaded numpy array."""
        if not self._should_check_tier_files(item):
            LOGGER.debug(
                "Fast-path: skipping tier check for %s tier %s "
                "category %s subcategory %s (nothing to move)",
                item.camera_identifier,
                item.tier_id,
                item.category,
                item.subcategories[0],
            )
            return np.empty(0, dtype=FILES_DTYPE)

        data = self.load_tier(item)
        now = utcnow()

        # If min_age is not set, we want to ignore recent files.
        # This is to avoid moving files that are still being written to
        if item.min_age:
            min_age_timestamp = (now - item.min_age).timestamp()
        else:
            min_age_timestamp = (
                now - datetime.timedelta(seconds=CAMERA_SEGMENT_DURATION * 2)
            ).timestamp()

        if item.max_age:
            max_age_timestamp = (now - item.max_age).timestamp()
        else:
            max_age_timestamp = 0

        # We want to ignore files that are less than 5 times the
        # segment duration old. This is to improve HLS streaming
        file_min_age = (
            now - datetime.timedelta(seconds=CAMERA_SEGMENT_DURATION * 5)
        ).timestamp()

        LOGGER.debug(
            "Files to move parameters: "
            "camera_identifier(%s), tier_id(%s), category(%s), subcategories(%s), "
            "max_bytes(%s), min_age_timestamp(%s), min_bytes(%s), "
            "max_age_timestamp(%s)",
            item.camera_identifier,
            item.tier_id,
            item.category,
            item.subcategories,
            item.max_bytes,
            min_age_timestamp,
            item.min_bytes,
            max_age_timestamp,
        )

        return get_continuous_files_to_move(
            data=data,
            max_bytes=item.max_bytes,
            min_age_timestamp=min_age_timestamp,
            min_bytes=item.min_bytes,
            max_age_timestamp=max_age_timestamp,
            file_min_age_timestamp=file_min_age,
            drain=item.drain,
            now=now,
            schedule_entries=item.continuous_schedule,
            timezone=item.continuous_schedule_timezone,
            lookback_seconds=item.continuous_lookback_seconds,
        )

    def check_tier_recordings(
        self,
        item: DataItem,
        now: datetime.datetime | None = None,
    ):
        """Check the recordings tier using the loaded numpy array."""
        if (
            item.events_max_bytes is None
            or item.events_min_bytes is None
            or item.events_min_age is None
            or item.events_max_age is None
        ):
            raise ValueError(
                "events_max_bytes, events_min_bytes, events_min_age, and events_max_age"
                " must be set for check_tier_recordings"
            )

        if now is None:
            now = utcnow()

        files_data = self.load_tier(item)
        recordings_data = self.load_recordings(item)
        now = utcnow()

        # If min_age is not set, we want to ignore files that are less than 5 secs old
        # This is to avoid moving files that are still being written to
        if item.events_min_age:
            min_age_timestamp = (now - item.events_min_age).timestamp()
        else:
            min_age_timestamp = (
                now - datetime.timedelta(seconds=CAMERA_SEGMENT_DURATION * 2)
            ).timestamp()

        if item.events_max_age:
            max_age_timestamp = (now - item.events_max_age).timestamp()
        else:
            max_age_timestamp = 0

        # We want to ignore files that are less than 5 times the
        # segment duration old. This is to improve HLS streaming
        file_min_age = (
            now - datetime.timedelta(seconds=CAMERA_SEGMENT_DURATION * 5)
        ).timestamp()

        LOGGER.debug(
            "Recordings to move parameters: "
            "camera_identifier(%s), tier_id(%s), category(%s), subcategories(%s), "
            "events_max_bytes(%s), min_age_timestamp(%s), events_min_bytes(%s), "
            "max_age_timestamp(%s), file_min_age(%s)",
            item.camera_identifier,
            item.tier_id,
            item.category,
            item.subcategories,
            item.events_max_bytes,
            min_age_timestamp,
            item.events_min_bytes,
            max_age_timestamp,
            file_min_age,
        )
        authorized_recordings: set[int] = set()
        rows_to_move = get_recordings_to_move(
            recordings_data,
            files_data,
            CAMERA_SEGMENT_DURATION,
            item.events_max_bytes,
            min_age_timestamp,
            item.events_min_bytes,
            max_age_timestamp,
            file_min_age,
            drain=item.drain,
            retention_recording_ids=authorized_recordings,
        )
        item.retention_recording_ids = tuple(sorted(authorized_recordings))
        return rows_to_move


def load_tier(
    get_session: Callable[..., Session],
    category: str,
    subcategories: list[str],
    tier_id: int,
    camera_identifier: str,
):
    """Load the tier files data for the camera."""
    with get_session() as session:
        stmt = select(
            Files.id, Files.size, Files.orig_ctime, Files.path, Files.tier_path
        ).where(
            Files.camera_identifier == camera_identifier,
            Files.tier_id == tier_id,
            Files.category == category,
            Files.subcategory.in_(subcategories),
        )
        result = session.execute(stmt).yield_per(1000)
        data = [
            (
                row.id,
                row.size,
                int(row.orig_ctime.timestamp()),
                row.path,
                row.tier_path,
            )
            for row in result
        ]
        return np.array(
            data,
            dtype=FILES_DTYPE,
        )


def load_recordings(
    get_session: Callable[..., Session],
    camera_identifier: str,
):
    """Load the tier recordings data for the camera."""
    with get_session() as session:
        stmt = select(
            Recordings.id,
            Recordings.start_time,
            Recordings.end_time,
            Recordings.adjusted_start_time,
            Recordings.created_at,
            Recordings.recorder_run_id,
        ).where(
            Recordings.camera_identifier == camera_identifier,
        )
        result = session.execute(stmt).yield_per(1000)
        data = [
            (
                row.id,
                int(row.start_time.timestamp()),
                int(row.adjusted_start_time.timestamp()),
                int(row.end_time.timestamp() if row.end_time else utcnow().timestamp()),
                int(row.created_at.timestamp()),
                row.recorder_run_id or "",
                row.end_time is None,
            )
            for row in result
        ]

    return np.array(
        data,
        dtype=RECORDINGS_DTYPE,
    )


def get_files_to_move(
    data: np.ndarray,
    max_bytes: int,
    min_age_timestamp: float,
    min_bytes: int,
    max_age_timestamp: float,
    file_min_age_timestamp: float,
    drain: bool,
):
    """Get id of files to move.

    The processing is done in the following steps:
    1. First the array is sorted by the timestamp.
    2. np.cumsum is used to calculate the cumulative sum of the sizes.
    3. Any rows where the cumulative size exceeds the tier size are marked
        for moving to the next tier.
    4. file_min_age_timestamp is applied as a hard floor on the selected files,
        the same way get_recordings_to_move does it. min_age_timestamp only
        gates the bytes branch, so this is what keeps files that are still
        being written to from being moved by the age branch. Like
        get_recordings_to_move, drain bypasses it.
    """
    # Sort by timestamp
    sorted_indices = np.argsort(data["orig_ctime"])
    data = data[sorted_indices][::-1]

    # Calculate cumulative size
    cumulative_size = np.cumsum(data["size"])

    # Find indices where cumulative size exceeds the tier size
    bytes_indices_to_move = np.empty(0, dtype=np.int64)
    if max_bytes > 0:
        bytes_indices_to_move = np.where(
            (cumulative_size >= max_bytes) & (data["orig_ctime"] <= min_age_timestamp)
        )[0]

    age_indices_to_move = np.empty(0, dtype=np.int64)
    if max_age_timestamp > 0:
        age_indices_to_move = np.where(
            (data["orig_ctime"] < max_age_timestamp) & (cumulative_size >= min_bytes)
        )[0]

    rows_to_move = np.empty(0, dtype=data.dtype)
    if drain and (bytes_indices_to_move.size > 0 or age_indices_to_move.size > 0):
        # If drain is set, move all files
        rows_to_move = data
    else:
        # Combine the indices to move based on size and age
        indices_to_move = np.unique(
            np.concatenate((bytes_indices_to_move, age_indices_to_move))
        )

        if indices_to_move.size > 0:
            rows_to_move = data[indices_to_move]

        rows_to_move = rows_to_move[
            rows_to_move["orig_ctime"] <= file_min_age_timestamp
        ]

    stripped_rows = rows_to_move[["id", "path", "tier_path"]]
    return stripped_rows[::-1]


def get_continuous_files_to_move(
    *,
    data: np.ndarray,
    max_bytes: int,
    min_age_timestamp: float,
    min_bytes: int,
    max_age_timestamp: float,
    file_min_age_timestamp: float,
    drain: bool,
    now: datetime.datetime,
    schedule_entries: list[dict[str, str]] | None,
    timezone: str | None,
    lookback_seconds: int,
) -> np.ndarray:
    """Get id of continuous files to move, aware of an optional recording schedule.

    Without a schedule this is identical to get_files_to_move. With a schedule,
    each file's own orig_ctime decides whether continuous recording was active
    when it was written:
    - Files written while active keep the full configured retention
      (max_bytes/max_age/etc), exactly as if there were no schedule.
    - Files written while inactive are capped to the lookback buffer, so they
      age out once no longer needed as event pre-roll instead of accumulating
      for the full retention window.
    """
    if schedule_entries is None or data.size == 0:
        return get_files_to_move(
            data=data,
            max_bytes=max_bytes,
            min_age_timestamp=min_age_timestamp,
            min_bytes=min_bytes,
            max_age_timestamp=max_age_timestamp,
            file_min_age_timestamp=file_min_age_timestamp,
            drain=drain,
        )

    # Evaluating the schedule per file parses two cron expressions per entry,
    # which does not scale to the thousands of rows a tier holds. The state only
    # flips where a cron fires, so the range the files span is resolved once and
    # each file is bisected into it.
    ctimes = data["orig_ctime"]
    changes = schedule_state_changes(
        schedule_entries,
        cast("str", timezone),
        datetime.datetime.fromtimestamp(int(ctimes.min()), tz=datetime.timezone.utc),
        datetime.datetime.fromtimestamp(int(ctimes.max()), tz=datetime.timezone.utc),
    )
    boundaries = np.array([timestamp for timestamp, _ in changes])
    states = np.array([active for _, active in changes], dtype=bool)
    active_mask = states[np.searchsorted(boundaries, ctimes, side="right") - 1]

    active_rows = get_files_to_move(
        data=data[active_mask],
        max_bytes=max_bytes,
        min_age_timestamp=min_age_timestamp,
        min_bytes=min_bytes,
        max_age_timestamp=max_age_timestamp,
        file_min_age_timestamp=file_min_age_timestamp,
        drain=drain,
    )

    inactive_max_age_timestamp = (
        now - datetime.timedelta(seconds=lookback_seconds + CAMERA_SEGMENT_DURATION)
    ).timestamp()
    inactive_rows = get_files_to_move(
        data=data[~active_mask],
        max_bytes=0,
        min_age_timestamp=0,
        min_bytes=0,
        max_age_timestamp=inactive_max_age_timestamp,
        file_min_age_timestamp=file_min_age_timestamp,
        drain=False,
    )

    return np.concatenate((active_rows, inactive_rows))


def get_recordings_to_move(
    recordings_data: np.ndarray,
    files_data: np.ndarray,
    segment_length: int,
    max_bytes: int,
    min_age_timestamp: float,
    min_bytes: int,
    max_age_timestamp: float,
    file_min_age_timestamp: float,
    drain: bool,
    retention_recording_ids: set[int] | None = None,
):
    """Get files to move based on recording grouping."""
    # Sort recordings by adjusted start time (descending)
    sorted_indices_recordings = np.argsort(recordings_data["adjusted_start_time"])[::-1]
    recordings_data = recordings_data[sorted_indices_recordings]

    # Sort files_data by orig_ctime (ascending) for efficient searching
    files_data.sort(order="orig_ctime")

    recordings_size = np.zeros(len(recordings_data), dtype=np.int64)
    associated_files_data_list = []

    for i, recording in enumerate(recordings_data):
        if files_data.size > 0:
            start_time_search = recording["adjusted_start_time"]
            end_time_search = recording["end_time"] + segment_length

            # Find relevant files using searchsorted
            start_idx = np.searchsorted(
                files_data["orig_ctime"], start_time_search, side="left"
            )
            end_idx = np.searchsorted(
                files_data["orig_ctime"], end_time_search, side="right"
            )
            relevant_files_for_recording = files_data[start_idx:end_idx]
            run_id = recording["recorder_run_id"]
            if run_id:
                relevant_files_for_recording = relevant_files_for_recording[
                    np.array(
                        [
                            os.path.basename(os.path.dirname(path)) == run_id
                            for path in relevant_files_for_recording["path"]
                        ],
                        dtype=bool,
                    )
                ]
        else:
            relevant_files_for_recording = np.empty(0, dtype=files_data.dtype)

        current_recording_total_size = 0
        if relevant_files_for_recording.size > 0:
            for file_row in relevant_files_for_recording:
                associated_files_data_list.append(
                    (
                        recording["id"],
                        file_row["id"],
                        file_row["size"],
                        file_row["orig_ctime"],
                        file_row["path"],
                        file_row["tier_path"],
                    )
                )
                current_recording_total_size += file_row["size"]
        recordings_size[i] = current_recording_total_size

    associated_file_ids_set = {tup[1] for tup in associated_files_data_list}

    other_files_data_list = []
    if files_data.size > 0:
        for file_row in files_data:
            if file_row["id"] not in associated_file_ids_set:
                other_files_data_list.append(
                    (
                        -1,  # recording_id for unassociated files
                        file_row["id"],
                        file_row["size"],
                        file_row["orig_ctime"],
                        file_row["path"],
                        file_row["tier_path"],
                    )
                )

    combined_files_list = associated_files_data_list + other_files_data_list

    if not combined_files_list:
        return np.empty(0, dtype=RECORDINGS_FILES_DTYPE)

    recordings_files = np.array(combined_files_list, dtype=RECORDINGS_FILES_DTYPE)

    # Sort by id, then orig_ctime to ensure consistent unique selection if needed
    # Although file IDs should be unique.
    recordings_files.sort(order=["id", "orig_ctime"])

    recording_cumulative_sizes = np.cumsum(recordings_size)

    bytes_indices_to_move_recordings = np.empty(0, dtype=np.int64)
    if max_bytes > 0:
        bytes_indices_to_move_recordings = np.where(
            (recording_cumulative_sizes >= max_bytes)
            & (recordings_data["created_at"] <= min_age_timestamp)
        )[0]

    age_indices_to_move_recordings = np.empty(0, dtype=np.int64)
    if max_age_timestamp > 0:
        age_indices_to_move_recordings = np.where(
            (recordings_data["created_at"] < max_age_timestamp)
            & (recording_cumulative_sizes >= min_bytes)
        )[0]

    files_to_move_np = np.empty(0, dtype=RECORDINGS_FILES_DTYPE)
    if drain and (
        bytes_indices_to_move_recordings.size > 0
        or age_indices_to_move_recordings.size > 0
    ):
        # Active events remain protected even when a tier is draining.
        recording_indices_to_move = np.flatnonzero(~recordings_data["active"])
        files_to_move_np = recordings_files
    else:
        # Indices of recordings_data that need to be moved
        recording_indices_to_move = np.unique(
            np.concatenate(
                (bytes_indices_to_move_recordings, age_indices_to_move_recordings)
            )
        )
        recording_indices_to_move = recording_indices_to_move[
            ~recordings_data["active"][recording_indices_to_move]
        ]

        files_to_move_list = []

        if recording_indices_to_move.size > 0:
            moved_recording_ids = recordings_data[recording_indices_to_move]["id"]
            # Select files associated with these moved recordings
            for r_file in recordings_files:
                if r_file["recording_id"] in moved_recording_ids:
                    if r_file["orig_ctime"] <= file_min_age_timestamp:
                        files_to_move_list.append(r_file)
                elif r_file["recording_id"] == -1:  # Always consider "other" files
                    if r_file["orig_ctime"] <= file_min_age_timestamp:
                        files_to_move_list.append(r_file)
        elif files_data.size > 0:  # No recordings to move, but check "other" files
            for r_file in recordings_files:
                if (
                    r_file["recording_id"] == -1
                    and r_file["orig_ctime"] <= file_min_age_timestamp
                ):
                    files_to_move_list.append(r_file)

        if not files_to_move_list:
            return np.empty(0, dtype=RECORDINGS_FILES_DTYPE)

        # Convert list of structured array rows to a new array
        files_to_move_np = np.array(files_to_move_list, dtype=RECORDINGS_FILES_DTYPE)

    if retention_recording_ids is not None:
        retention_recording_ids.update(
            int(value) for value in recordings_data[recording_indices_to_move]["id"]
        )

    # A fragment shared with any retained event cannot be moved on behalf of
    # an eligible event. Check the associations before deduplicating by file.
    if files_to_move_np.size:
        selected_ids = recordings_data[recording_indices_to_move]["id"]
        protected = recordings_files[
            (recordings_files["recording_id"] >= 0)
            & ~np.isin(recordings_files["recording_id"], selected_ids)
        ]["id"]
        files_to_move_np = files_to_move_np[~np.isin(files_to_move_np["id"], protected)]

    # Ensure unique files by ID, taking the first occurrence after sorting
    if files_to_move_np.size > 0:
        _, unique_indices = np.unique(files_to_move_np["id"], return_index=True)
        files_to_move_np = files_to_move_np[unique_indices]
    else:
        return np.empty(0, dtype=RECORDINGS_FILES_DTYPE)

    # Strip to required columns
    stripped_files_to_move = files_to_move_np[
        ["recording_id", "id", "path", "tier_path"]
    ]

    # Remove any files that are not m4s
    stripped_files_to_move = stripped_files_to_move[
        np.char.endswith(stripped_files_to_move["path"], ".m4s")
    ]
    return stripped_files_to_move


def delete_file(
    get_session: Callable[..., Session],
    path: str,
    logger: logging.Logger,
    *,
    tier_markers: dict[str, str] | None = None,
    retention_recording_ids: tuple[int, ...] | None = None,
) -> bool:
    """Delete a file, deferring stale retention authorization."""
    logger.debug("Deleting file %s", path)
    if is_tier_marker(path, tier_markers or {}):
        raise OSError(f"Refusing to delete storage marker {path}")
    require_tier_markers(path, tier_markers or {})
    run_dir = os.path.dirname(path)
    lock = (
        camera_lock("/tmp/viseron-run-locks", os.path.abspath(os.path.dirname(run_dir)))
        if is_run_directory(os.path.basename(run_dir))
        else asset_locks(path)
    )
    with lock, get_session() as session:
        session.execute(select(func.pg_advisory_xact_lock(func.hashtext(path))))
        if (
            source_move_pending(path)
            or protected_by_pending(path)
            or asset_move_pending(session, path)
            or segment_move_pending(session, path)
        ):
            raise BlockingIOError("File has an active publication hold")
        if retention_recording_ids is not None:
            row = session.execute(
                select(Files).where(Files.path == path)
            ).scalar_one_or_none()
            if (
                row is None
                or row.category != "recorder"
                or row.subcategory != "segments"
                or row.duration is None
            ):
                return False
            event_fence(session, row.camera_identifier)
            claimed = session.execute(
                select(Recordings.id)
                .where(
                    Recordings.camera_identifier == row.camera_identifier,
                    Recordings.adjusted_start_time
                    <= row.orig_ctime + datetime.timedelta(seconds=row.duration),
                    or_(
                        Recordings.end_time.is_(None),
                        Recordings.end_time > row.orig_ctime,
                    ),
                    or_(
                        Recordings.recorder_run_id.is_(None),
                        Recordings.recorder_run_id == os.path.basename(row.directory),
                    ),
                    or_(
                        Recordings.end_time.is_(None),
                        Recordings.id.not_in(retention_recording_ids),
                    ),
                )
                .limit(1)
            ).first()
            if claimed:
                logger.debug("Deferring stale retention deletion for %s", path)
                return False
        require_tier_markers(path, tier_markers or {})
        os.remove(path)
        require_tier_markers(path, tier_markers or {})
        stmt = delete(Files).where(Files.path == path)
        session.execute(stmt)
        session.commit()
        return True


RUN_DIRECTORY_RE = re.compile(r"^\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}$")


def publish_run_init(src: str, dst: str, tier_markers: dict[str, str]) -> None:
    """Atomically publish an init without replacing a competing publisher.

    Linux renameat2(RENAME_NOREPLACE) is required; fail closed if unavailable.
    """
    run_id = os.path.basename(os.path.dirname(src))
    if not src.endswith(".m4s") or not RUN_DIRECTORY_RE.fullmatch(run_id):
        return
    if os.path.basename(os.path.dirname(dst)) != run_id:
        raise OSError("Destination does not preserve run identity")
    source_init = os.path.join(os.path.dirname(src), "init.mp4")
    destination_dir = os.path.dirname(dst)
    destination_init = os.path.join(destination_dir, "init.mp4")
    require_tier_markers(source_init, tier_markers)
    require_tier_markers(destination_init, tier_markers)
    # The marker ties the published init to this source and allows later
    # fragments to reuse it without a routine network readback. Existing
    # unmarked inits are adopted only after an exceptional content check.
    from viseron.components.storage.run_init_prototype import ensure_init

    ensure_init(source_init, destination_dir, adopt_existing=True)
    require_tier_markers(source_init, tier_markers)
    require_tier_markers(destination_init, tier_markers)


def move_file(
    _get_session: Callable[..., Session],
    src: str,
    dst: str,
    logger: logging.Logger,
    *,
    tier_markers: dict[str, str] | None = None,
    camera_identifier: str | None = None,
    publication_meta: dict | None = None,
    verify_readback: bool = False,
) -> None:
    """Move a file while serializing generation publications with cleanup."""
    if publication_meta is not None and "asset_target" in publication_meta:
        move_asset(_get_session, src, dst, publication_meta, tier_markers or {})
        return
    is_run = is_run_directory(os.path.basename(os.path.dirname(src)))
    if is_run and (camera_identifier is None or publication_meta is None):
        raise ValueError("Run segment moves require camera and publication metadata")
    destination_camera = os.path.abspath(os.path.dirname(os.path.dirname(dst)))
    lock = (
        camera_lock("/tmp/viseron-run-locks", destination_camera)
        if is_run
        else contextlib.nullcontext()
    )
    with lock:
        _move_file_locked(
            _get_session,
            src,
            dst,
            logger,
            tier_markers=tier_markers,
            publication_meta=publication_meta,
            verify_readback=verify_readback,
        )


def _move_file_locked(
    _get_session: Callable[..., Session],
    src: str,
    dst: str,
    logger: logging.Logger,
    *,
    tier_markers: dict[str, str] | None = None,
    publication_meta: dict | None = None,
    verify_readback: bool = False,
) -> None:
    """Move file from src to dst.

    To avoid race conditions where a file is referenced at the same time as it is being
    moved, causing a 404 in the browser, we copy the file to the new location and then
    delete the old one.
    """
    logger.debug("Moving file from %s to %s", src, dst)
    if is_tier_marker(src, tier_markers or {}) or is_tier_marker(
        dst, tier_markers or {}
    ):
        raise OSError("Refusing to move a storage marker")
    require_tier_markers(src, tier_markers or {})
    require_tier_markers(dst, tier_markers or {})
    temp_path: str | None = None
    protected_temp = False
    source_intent = None
    is_run_source = is_run_directory(os.path.basename(os.path.dirname(src)))
    if is_run_source:
        source_camera = os.path.abspath(os.path.dirname(os.path.dirname(src)))
        with camera_lock("/tmp/viseron-run-locks", source_camera):
            require_tier_markers(src, tier_markers or {})
            if "destination_tier_id" in publication_meta:
                with _get_session() as session:
                    publication_meta["pending_move_id"] = begin_copy(
                        session, src, dst, publication_meta
                    )
            source_intent = prepare_source_intent(
                src,
                dst,
                os.stat(src, follow_symlinks=False),
                publication_meta.get("file_key"),
                source_db=publication_meta.get("source_db"),
            )
    try:
        publish_run_init(src, dst, tier_markers or {})
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, "rb") as source:
            source_stat = os.fstat(source.fileno())
            source_size = source_stat.st_size
            fd, temp_path = tempfile.mkstemp(
                prefix=TIER_MOVE_TEMP_PREFIX, dir=os.path.dirname(dst)
            )
            digest = hashlib.sha256() if source_intent is not None else None
            with os.fdopen(fd, "wb") as target:
                os.fchmod(target.fileno(), stat.S_IMODE(source_stat.st_mode))
                if digest is None:
                    shutil.copyfileobj(source, target)
                else:
                    while chunk := source.read(1024 * 1024):
                        target.write(chunk)
                        digest.update(chunk)
                target.flush()
                os.fsync(target.fileno())
                if (
                    os.fstat(target.fileno()).st_size != source_size
                    or os.fstat(source.fileno()).st_size != source_size
                ):
                    raise OSError(f"Incomplete copy of {src}")  # noqa: TRY301
        if source_intent is not None:
            with open(
                os.path.join(os.path.dirname(src), "init.mp4"), "rb"
            ) as init_source:
                init_digest = hashlib.sha256(init_source.read()).hexdigest()
            if verify_readback:
                copied_digest = hashlib.sha256()
                with open(temp_path, "rb") as copied:
                    while chunk := copied.read(1024 * 1024):
                        copied_digest.update(chunk)
                if copied_digest.digest() != digest.digest():
                    raise OSError("Destination readback checksum mismatch")  # noqa: TRY301
        elif verify_readback:
            raise ValueError("Readback verification requires run-segment metadata")
        if source_intent is not None:
            marker = prepare(
                os.path.dirname(dst),
                os.path.basename(dst),
                publication_meta,
                temp_path=temp_path,
                source_intent=source_intent,
                copied_digest=digest.hexdigest(),
            )
            protected_temp = True
            publish_or_verify(marker)
            temp_path = None
        else:
            os.replace(temp_path, dst)
            sync_dir(os.path.dirname(dst))
            temp_path = None
        require_tier_markers(dst, tier_markers or {})
        is_run = is_run_directory(os.path.basename(os.path.dirname(src)))
        source_camera = os.path.abspath(os.path.dirname(os.path.dirname(src)))
        source_lock = (
            camera_lock("/tmp/viseron-run-locks", source_camera)
            if is_run
            else contextlib.nullcontext()
        )
        # No destination I/O while holding the local source lock.
        with source_lock:
            require_tier_markers(src, tier_markers or {})
            current = os.stat(src)
            if (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
            ) != (
                source_stat.st_dev,
                source_stat.st_ino,
                source_stat.st_size,
                source_stat.st_mtime_ns,
            ):
                raise OSError("Source changed while being copied")  # noqa: TRY301
            if source_intent is not None:
                # Experiment: protect source until a future reconciler proves the
                # destination was indexed. Do not enable deletion without proof.
                set_source_digest(source_intent, digest.hexdigest(), init_digest)
            else:
                os.remove(src)
    except OSError as error:
        logger.debug("Failed to move file %s to %s: %s", src, dst, error)
        # A destination or network failure says nothing about whether the source
        # is safe to discard. Leave both the source and its record for retry.
        raise
    finally:
        if temp_path is not None and not protected_temp:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning("Could not remove temporary file %s", temp_path)
