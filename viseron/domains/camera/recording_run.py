"""Recorder run names shared by process launch, storage and event selection."""

from datetime import UTC, datetime
from pathlib import Path, PurePath
import re
from uuid import uuid4

RUN_TOKEN = "__VISERON_RUN__"
RUN_PATTERN = re.compile(r"^\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}$")


def new_run_id() -> str:
    """Name an actual encoder launch in UTC start order."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid4().hex[:12]


def is_run_directory(name: str) -> bool:
    """Recognize the recording directory format without accepting path traversal."""
    return RUN_PATTERN.fullmatch(name) is not None


def recording_run_id(path: str) -> str | None:
    """Get a segment's run, retaining support for legacy flat recordings."""
    name = PurePath(path).parent.name
    return name if is_run_directory(name) else None


def segment_init_files(path: str, init_hash: str | None) -> list[str]:
    """List the init files that must accompany a segment during promotion."""
    names = [f"init-{init_hash}.mp4" if init_hash else "init.mp4"]
    if recording_run_id(path) and init_hash:
        names.append("init.mp4")
    return names


def remove_finished_run_metadata(directory: str) -> bool:
    """Remove metadata-only output after the caller has proved its encoder stopped.

    Media, partial output, symlinks and unknown files are retained for recovery.
    """
    path = Path(directory)
    if path.is_symlink() or not is_run_directory(path.name):
        return False
    try:
        entries = list(path.iterdir())
        if any(
            entry.is_symlink()
            or not entry.is_file()
            or (
                entry.name not in {"init.mp4", "index.m3u8"}
                and re.fullmatch(r"init-[0-9a-f]{64}\.mp4", entry.name) is None
            )
            for entry in entries
        ):
            return False
        for entry in entries:
            entry.unlink()
        path.rmdir()
        return True
    except FileNotFoundError:
        return True
    except OSError:
        return False


def prune_stopped_spool_metadata(root: str) -> None:
    """Reclaim metadata before starting this camera's first writer after startup."""
    try:
        for directory in Path(root).iterdir():
            if directory.is_dir() and is_run_directory(directory.name):
                remove_finished_run_metadata(str(directory))
    except OSError:
        pass
