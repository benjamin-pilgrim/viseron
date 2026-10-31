"""Persist observed HLS playlist timing alongside staged fragments.

A record contains only timing observed in FFmpeg's playlist, not an estimate.
The staged file identity is checked again when the fragment is published.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
import os
import re
import stat
import tempfile
from pathlib import Path

from viseron.components.storage.source_move_intent import source_move_pending
from viseron.helpers.recording_timing_contract import (
    TIMING_CONTRACT_CONTENT,
    TIMING_CONTRACT_FILE,
)
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
)

_LOGGER = logging.getLogger(__name__)
_SEGMENT = re.compile(r"^\d{9,}-\d+-\d{13,}\.m4s$")
_PREFIX = ".viseron-timing-"
_TIMING_RECORD = re.compile(r"^\.viseron-timing-(\d{9,}-\d+-\d{13,}\.m4s)\.json$")
_MAX_SEGMENT_DURATION = 86400


def _identity(path: Path) -> list[int]:
    info = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("fragment is not a regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def _records(contents: str):
    duration = None
    timestamp = None
    for line in contents.splitlines():
        if line.startswith("#EXT-X-DISCONTINUITY"):
            duration = timestamp = None
        elif line.startswith("#EXTINF:"):
            timestamp = None
            try:
                duration = float(line.split(":", 1)[1].split(",", 1)[0])
            except ValueError:
                duration = None
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            if duration is None or timestamp is not None:
                duration = timestamp = None
                continue
            try:
                timestamp = datetime.datetime.fromisoformat(line.split(":", 1)[1])
                if timestamp.utcoffset() is None:
                    timestamp = None
            except ValueError:
                timestamp = None
        elif line and not line.startswith("#"):
            if (
                _SEGMENT.fullmatch(line)
                and duration is not None
                and math.isfinite(duration)
                and 0 < duration < _MAX_SEGMENT_DURATION
                and timestamp
            ):
                yield line, duration, timestamp.astimezone(datetime.timezone.utc)
            duration = timestamp = None


def capture_playlist_times(stage: Path) -> None:
    """Capture available playlist entries; missing or changed files are skipped."""
    try:
        runs = list(stage.iterdir())
    except OSError:
        _LOGGER.exception("Cannot inspect recording spool %s", stage)
        return
    for run in runs:
        if not is_run_directory(run.name) or not run.is_dir() or run.is_symlink():
            continue
        try:
            with camera_lock("/tmp/viseron-run-locks", os.path.abspath(stage)):
                if (run / TIMING_CONTRACT_FILE).read_text(
                    encoding="utf-8"
                ) != TIMING_CONTRACT_CONTENT:
                    continue
                contents = (run / "index.m3u8").read_text(encoding="utf-8")
                _capture_run(run, contents)
                _prune_released_records(run)
        except (CameraLockBusy, FileNotFoundError):
            continue
        except (OSError, UnicodeError):
            _LOGGER.warning("Cannot capture timing playlist %s", run, exc_info=True)


def _capture_run(run: Path, contents: str) -> None:
    for name, duration, timestamp in _records(contents):
        fragment = run / name
        target = run / f"{_PREFIX}{name}.json"
        if target.exists():
            continue
        temp = None
        try:
            identity = _identity(fragment)
            record = {
                "version": 1,
                "name": name,
                "identity": identity,
                "duration": duration,
                "program_date_time": timestamp.isoformat(),
            }
            fd, temp = tempfile.mkstemp(prefix=".viseron-timing-tmp-", dir=run)
            with os.fdopen(fd, "w", encoding="utf-8") as output:
                json.dump(record, output, sort_keys=True)
                output.flush()
                os.fsync(output.fileno())
            if _identity(fragment) != identity:
                raise ValueError("fragment changed while recording timing")  # noqa: TRY301
            try:
                os.link(temp, target)
            except FileExistsError:
                pass
            directory = os.open(run, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except FileNotFoundError:
            # A concurrently acknowledged fragment may already be gone.
            continue
        except (OSError, ValueError):
            _LOGGER.warning("Cannot preserve timing for %s", fragment, exc_info=True)
        finally:
            if temp:
                try:
                    os.unlink(temp)
                except FileNotFoundError:
                    pass


def _prune_released_records(run: Path) -> None:
    """Discard timing only after the staged source and its hold are absent."""
    for record in run.iterdir():
        match = _TIMING_RECORD.fullmatch(record.name)
        if not match or record.is_symlink() or not record.is_file():
            continue
        source = run / match.group(1)
        if not os.path.lexists(source) and not source_move_pending(source):
            try:
                record.unlink()
            except FileNotFoundError:
                pass


def read_playlist_time(
    fragment: Path,
) -> tuple[datetime.datetime, float, list[int]] | None:
    """Return only a record that still identifies the original staged file."""
    if not _SEGMENT.fullmatch(fragment.name):
        return None
    try:
        if (fragment.parent / TIMING_CONTRACT_FILE).read_text(
            encoding="utf-8"
        ) != TIMING_CONTRACT_CONTENT:
            return None
        record = json.loads(
            (fragment.parent / f"{_PREFIX}{fragment.name}.json").read_text()
        )
        timestamp = datetime.datetime.fromisoformat(record["program_date_time"])
        duration = float(record["duration"])
        if (
            record["version"] != 1
            or record["name"] != fragment.name
            or record["identity"] != _identity(fragment)
            or timestamp.utcoffset() is None
            or not 0 < duration < _MAX_SEGMENT_DURATION
        ):
            return None
        return timestamp.astimezone(datetime.timezone.utc), duration, record["identity"]
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
