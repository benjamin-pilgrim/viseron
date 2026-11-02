"""Persist observed HLS playlist timing alongside staged fragments.

A record contains only timing observed in FFmpeg's playlist, not an estimate.
The staged file identity is checked again when the fragment is published.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import math
import os
import re
import stat
import tempfile
from pathlib import Path

from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
)
from viseron.components.storage.source_move_intent import source_move_pending
from viseron.helpers.recording_timing_contract import (
    TIMING_CONTRACT_CONTENT,
    TIMING_CONTRACT_FILE,
)

_LOGGER = logging.getLogger(__name__)
_SEGMENT = re.compile(r"^\d{9,}-\d+-\d{13,}\.m4s$")
_PREFIX = ".viseron-timing-"
_TIMING_RECORD = re.compile(r"^\.viseron-timing-(\d{9,}-\d+-\d{13,}\.m4s)\.json$")
_MAX_SEGMENT_DURATION = 86400
_TIMING_RECORD_VERSION = 2


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
                _prune_released_records(run)
            _capture_run(stage, run, contents)
        except (CameraLockBusy, FileNotFoundError):
            continue
        except (OSError, UnicodeError):
            _LOGGER.warning("Cannot capture timing playlist %s", run, exc_info=True)


def _source_digest(fragment: Path) -> tuple[list[int], str]:
    """Read only the local source and reject a source that changes mid-read."""
    fd = os.open(fragment, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("fragment is not a regular file")
        identity = [before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns]
        digest = hashlib.sha256()
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(source.fileno())
        if identity != [after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns]:
            raise ValueError("fragment changed while calculating timing digest")
    return identity, digest.hexdigest()


def _capture_run(stage: Path, run: Path, contents: str) -> None:
    for name, duration, timestamp in _records(contents):
        fragment = run / name
        target = run / f"{_PREFIX}{name}.json"
        if target.exists():
            continue
        temp = None
        try:
            # Hash outside the camera lock: a large fragment must not stall
            # publication, event handling or retention for this camera.
            identity, digest = _source_digest(fragment)
            record = {
                "version": _TIMING_RECORD_VERSION,
                "name": name,
                "identity": identity,
                "sha256": digest,
                "duration": duration,
                "program_date_time": timestamp.isoformat(),
            }
            with camera_lock("/tmp/viseron-run-locks", os.path.abspath(stage)):
                if _identity(fragment) != identity:
                    raise ValueError("fragment changed while recording timing")  # noqa: TRY301
                fd, temp = tempfile.mkstemp(prefix=".viseron-timing-tmp-", dir=run)
                with os.fdopen(fd, "w", encoding="utf-8") as output:
                    json.dump(record, output, sort_keys=True)
                    output.flush()
                    os.fsync(output.fileno())
                try:
                    os.link(temp, target)
                except FileExistsError:
                    pass
                directory = os.open(run, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        except (FileNotFoundError, CameraLockBusy):
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
) -> tuple[datetime.datetime, float, list[int], str | None] | None:
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
            record["version"] not in (1, _TIMING_RECORD_VERSION)
            or record["name"] != fragment.name
            or (
                record["version"] == _TIMING_RECORD_VERSION
                and not re.fullmatch(r"[0-9a-f]{64}", record["sha256"])
            )
            or timestamp.utcoffset() is None
            or not 0 < duration < _MAX_SEGMENT_DURATION
        ):
            return None
        identity = _identity(fragment)
        expected_digest = (
            record["sha256"] if record["version"] == _TIMING_RECORD_VERSION else None
        )
        if record["identity"] != identity:
            # Older records lack a content hash and cannot safely survive an
            # identity change. Preserve those sources for manual recovery.
            if record["version"] == 1:
                return None
            identity, actual_digest = _source_digest(fragment)
            if actual_digest != expected_digest:
                return None
        return (
            timestamp.astimezone(datetime.timezone.utc),
            duration,
            identity,
            expected_digest,
        )
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def read_live_playlist_time(fragment: Path):
    """Read a completed entry when a full spool cannot persist a timing sidecar.

    The run must advertise the timing contract. Callers must recheck the returned
    immutable file identity under the publication lock before removing any bytes.
    """
    if not _SEGMENT.fullmatch(fragment.name):
        return None
    try:
        if (fragment.parent / TIMING_CONTRACT_FILE).read_text(
            encoding="utf-8"
        ) != TIMING_CONTRACT_CONTENT:
            return None
        contents = (fragment.parent / "index.m3u8").read_text(encoding="utf-8")
        for name, duration, timestamp in _records(contents):
            if name == fragment.name:
                return timestamp, duration, _identity(fragment), None
    except (OSError, UnicodeError, ValueError):
        return None
    return None
