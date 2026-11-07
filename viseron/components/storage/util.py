"""Storage component utility functions."""

from __future__ import annotations

import os
import stat
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from viseron.components.storage.const import (
    TIER_CATEGORY_SNAPSHOTS,
    TIER_SUBCATEGORY_EVENT_CLIPS,
    TIER_SUBCATEGORY_SEGMENTS,
    TIER_SUBCATEGORY_THUMBNAILS,
    TIER_SUBCATEGORY_TIMELAPSE,
)
from viseron.events import EventData

# Files with this prefix are private, incomplete tier-move copies.
TIER_MOVE_TEMP_PREFIX = ".viseron-tier-move-"


def is_tier_move_temp_file(path: str) -> bool:
    """Return whether a path is an unpublished tier-move temporary file."""
    return os.path.basename(path).startswith(TIER_MOVE_TEMP_PREFIX)


def tier_markers_available(path: str, markers: dict[str, str]) -> bool:
    """Check every configured marker protecting an absolute path.

    A missing or inaccessible marker is a failed check, even if the directory
    still exists underneath an unmounted share.
    """
    path = os.path.abspath(path)
    for root, marker in markers.items():
        if os.path.commonpath((root, path)) != root:
            continue
        try:
            if not stat.S_ISREG(os.lstat(os.path.join(root, marker)).st_mode):
                return False
        except OSError:
            return False
    return True


def is_tier_marker(path: str, markers: dict[str, str]) -> bool:
    """Return whether path is a configured marker that must never be cleaned up."""
    return os.path.abspath(path) in {
        os.path.join(root, marker) for root, marker in markers.items()
    }


def require_tier_markers(path: str, markers: dict[str, str]) -> None:
    """Fail closed if a configured marker protecting path is unavailable."""
    if not tier_markers_available(path, markers):
        raise OSError(f"Required storage marker unavailable for {path}")


if TYPE_CHECKING:
    from types import TracebackType

    from viseron.components.storage.config import Tier
    from viseron.domains.camera import AbstractCamera, FailedCamera
    from viseron.viseron_types import SnapshotDomain


def calculate_age(age: Any) -> timedelta:
    """Read an age from a legacy camera schema or a typed storage model."""
    if hasattr(age, "to_timedelta"):
        return age.to_timedelta()
    age = age or {}
    return timedelta(
        days=age.get("days") or 0,
        hours=age.get("hours") or 0,
        minutes=age.get("minutes") or 0,
        seconds=age.get("seconds") or 0,
    )


def calculate_bytes(size: Any) -> int:
    """Read a size from a legacy camera schema or a typed storage model."""
    if hasattr(size, "to_bytes"):
        return size.to_bytes()
    size = size or {}
    return int((size.get("gb") or 0) * 1024**3 + (size.get("mb") or 0) * 1024**2)


def convert_mb_to_bytes(mb: int) -> int:
    """Convert megabytes to bytes for legacy callers."""
    return mb * 1024**2


def convert_gb_to_bytes(gb: int) -> int:
    """Convert gigabytes to bytes for legacy callers."""
    return gb * 1024**3


def get_segments_path(tier: Tier, camera: AbstractCamera | FailedCamera) -> str:
    """Get segments path for camera."""
    return os.path.join(tier.path, TIER_SUBCATEGORY_SEGMENTS, camera.identifier)


def get_event_clips_path(tier: Tier, camera: AbstractCamera | FailedCamera) -> str:
    """Get event clips path for camera."""
    return os.path.join(tier.path, TIER_SUBCATEGORY_EVENT_CLIPS, camera.identifier)


def get_thumbnails_path(tier: Tier, camera: AbstractCamera | FailedCamera) -> str:
    """Get thumbnails path for camera."""
    return os.path.join(tier.path, TIER_SUBCATEGORY_THUMBNAILS, camera.identifier)


def get_snapshots_path(
    tier: Tier,
    camera: AbstractCamera | FailedCamera,
    domain: SnapshotDomain,
) -> str:
    """Get snapshots path for camera."""
    return os.path.join(
        tier.path, TIER_CATEGORY_SNAPSHOTS, domain.value, camera.identifier
    )


def get_timelapse_path(tier: Tier, camera: AbstractCamera | FailedCamera) -> str:
    """Get timelapse path for camera."""
    return os.path.join(tier.path, TIER_SUBCATEGORY_TIMELAPSE, camera.identifier)


@dataclass
class EventFile(EventData):
    """Event data for file events."""

    camera_identifier: str
    category: str
    subcategory: str
    file_name: str
    path: str


class EventFileCreated(EventFile):
    """Event data for file created events."""


class EventFileDeleted(EventFile):
    """Event data for file deleted events."""


class RequestedFilesCount:
    """Context manager for keeping track of recently requested files."""

    def __init__(self) -> None:
        self.count = 0
        self.filenames: list[str] = []

    def remove_filename(self, filename: str) -> None:
        """Remove a filename from the list of active filenames."""
        self.filenames.remove(filename)

    def __call__(self, filename: str) -> RequestedFilesCount:
        """Add a filename to the list of active filenames."""
        self.filenames.append(filename)
        timer = threading.Timer(2, self.remove_filename, args=(filename,))
        timer.start()
        return self

    def __enter__(self) -> int:
        """Increment the counter when entering the context."""
        self.count += 1
        return self.count

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Decrement the counter when exiting the context."""
        self.count -= 1
