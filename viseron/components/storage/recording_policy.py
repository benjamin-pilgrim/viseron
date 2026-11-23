"""Bounded plans for recorder forwarding and physical filesystem pressure."""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from sqlalchemy import and_, case, func, or_, select

from viseron.components.storage.models import (
    FileLocations,
    FileLocationState,
    Files,
    Recordings,
)
from viseron.components.storage.util import (
    raise_if_directory_unavailable,
    tier_destination_root,
)
from viseron.const import CAMERA_SEGMENT_DURATION
from viseron.helpers import utcnow

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

    from viseron.components.storage.storage_subprocess import DataItem

ACTION_LIMIT = 128
MIN_FREE_BYTES = 16 * 1024**2


@dataclass(frozen=True)
class RecordingAction:
    """A proposed operation, revalidated before destructive execution."""

    operation: Literal["move", "delete"]
    src: str
    tier_path: str
    destination_tier: int | None = None
    destination_path: str | None = None
    subcategory: str = "segments"


def event_fence(session: Session, camera: str) -> None:
    """Serialize event creation against emergency event classification."""
    session.execute(
        select(func.pg_advisory_xact_lock(func.hashtext("recording-events:" + camera)))
    )


def event_coverage(*, active_only: bool = False):
    """Find events overlapping the actual segment interval, counting each once."""
    predicates = [
        Recordings.camera_identifier == Files.camera_identifier,
        or_(
            and_(
                Files.subcategory == "segments",
                Recordings.adjusted_start_time
                <= Files.orig_ctime
                + func.make_interval(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    func.coalesce(Files.duration, CAMERA_SEGMENT_DURATION),
                ),
                or_(
                    Recordings.end_time.is_(None),
                    Recordings.end_time > Files.orig_ctime,
                ),
            ),
            and_(
                Files.subcategory == "thumbnails",
                or_(
                    Recordings.thumbnail_file_id == Files.id,
                    Recordings.thumbnail_path == Files.path,
                ),
            ),
            and_(
                Files.subcategory == "event_clips",
                or_(
                    Recordings.clip_file_id == Files.id,
                    Recordings.clip_path == Files.path,
                ),
            ),
        ),
    ]
    if active_only:
        predicates.append(Recordings.end_time.is_(None))
    return select(Recordings.id).where(*predicates).correlate(Files).exists()


def filesystem_pressure(path: str, reserve: int = MIN_FREE_BYTES) -> bool:
    """Check physical space and inodes without walking recording directories."""
    filesystem = os.statvfs(path)
    return filesystem.f_bavail * filesystem.f_frsize < reserve or (
        filesystem.f_files > 0 and filesystem.f_favail == 0
    )


def plan_recording_actions(get_session: Callable[[], Session], item: DataItem) -> None:
    """Select at most one bounded batch from known available segment locations."""
    if item.size_path is None:
        raise ValueError("Recording policy requires a tier filesystem path")
    now = utcnow()
    if item.promote_after is None and not filesystem_pressure(item.size_path):
        return
    with get_session() as session:
        owned = (
            Files.camera_identifier == item.camera_identifier,
            Files.category == "recorder",
            Files.subcategory.in_(("segments", "thumbnails", "event_clips")),
            or_(Files.subcategory == "segments", event_coverage()),
            FileLocations.tier_id == item.tier_id,
            FileLocations.state == FileLocationState.AVAILABLE.value,
        )
        size, largest, event_size = session.execute(
            select(
                func.coalesce(func.sum(FileLocations.size), 0),
                func.coalesce(func.max(FileLocations.size), 0),
                func.coalesce(
                    func.sum(case((event_coverage(), FileLocations.size), else_=0)), 0
                ),
            )
            .select_from(Files)
            .join(FileLocations, FileLocations.file_id == Files.id)
            .where(*owned)
        ).one()
        physical = filesystem_pressure(
            item.size_path, max(MIN_FREE_BYTES, int(largest) * 2)
        )
        budget = (item.max_bytes > 0 and size >= item.max_bytes) or (
            (item.events_max_bytes or 0) > 0 and event_size >= item.events_max_bytes
        )
        item.physical_pressure = physical
        if not (physical or budget or item.promotion_cutoff is not None):
            return
        safe_age = max(
            CAMERA_SEGMENT_DURATION * 5,
            item.continuous_lookback_seconds + CAMERA_SEGMENT_DURATION * 2,
        )
        # An open batch has a fixed cutoff. Incoming footage cannot keep it open.
        cache_delay = 0 if physical or budget else (item.promote_after or 0)
        cutoff = (
            now if physical or budget else item.promotion_cutoff
        ) - datetime.timedelta(seconds=max(safe_age, cache_delay))
        ordinary_first = item.on_full in ("ordinary_first", "preserve_events")
        rows = session.execute(
            select(
                FileLocations.path,
                FileLocations.tier_path,
                Files.subcategory,
                event_coverage().label("event"),
                event_coverage(active_only=True).label("active"),
            )
            .select_from(Files)
            .join(FileLocations, FileLocations.file_id == Files.id)
            .where(
                *owned,
                Files.orig_ctime < cutoff,
                FileLocations.path.not_in(item.requested_paths),
            )
            .order_by(
                *([event_coverage()] if ordinary_first and physical else []),
                Files.orig_ctime,
                Files.id,
            )
            .limit(ACTION_LIMIT)
        ).all()
        destination_health: dict[str, bool] = {}
        for row in rows:
            target = item.promotion_targets.get("events" if row.event else "continuous")
            if row.event and "continuous" in item.promotion_targets:
                continuous_target = item.promotion_targets["continuous"]
                if target is None or continuous_target[0] < target[0]:
                    target = continuous_target
            healthy = False
            if target is not None:
                probe = os.path.join(target[1], row.subcategory, item.camera_identifier)
                if probe not in destination_health:
                    try:
                        raise_if_directory_unavailable(
                            tier_destination_root(target[1], row.subcategory)
                        )
                        os.makedirs(probe, exist_ok=True)
                        destination_health[probe] = not filesystem_pressure(
                            probe, max(MIN_FREE_BYTES, int(largest) * 2)
                        )
                        filesystem = os.statvfs(probe)
                        destination_health[probe] &= (
                            not bool(filesystem.f_flag & getattr(os, "ST_RDONLY", 1))
                            if hasattr(filesystem, "f_flag")
                            else True
                        )
                    except OSError:
                        destination_health[probe] = False
                healthy = destination_health[probe] and not item.destination_failed
            if target is not None and healthy:
                item.recording_actions.append(
                    RecordingAction(
                        "move",
                        row.path,
                        row.tier_path,
                        *target,
                        subcategory=row.subcategory,
                    )
                )
            elif (
                physical
                and row.subcategory == "segments"
                and item.on_full
                and not row.active
                and (item.on_full != "preserve_events" or not row.event)
            ):
                item.recording_actions.append(
                    RecordingAction("delete", row.path, row.tier_path)
                )
        item.recording_batch_complete = len(rows) < ACTION_LIMIT
