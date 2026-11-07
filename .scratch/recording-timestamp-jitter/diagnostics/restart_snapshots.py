"""Preserve bounded local diagnostics when a camera changes recording run."""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import threading
import time
import uuid
from pathlib import Path

_RUN = re.compile(r"\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}")
_CAMERA = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,127}")
_INCIDENT = re.compile(r"\d{20}-[0-9a-f]{8}")
_READ_LIMIT = 1024 * 1024


class RecordingRunFollower:
    """Tail the local timing log, keeping camera state across log rotation."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._identity: tuple[int, int] | None = None
        self._offset = 0
        self._primed = False
        self._runs: dict[str, tuple[str, float]] = {}

    def poll(self) -> list[dict]:
        """Return new run transitions; existing log contents establish a baseline."""
        changes = []
        try:
            with self.path.open("rb") as stream:
                info = os.fstat(stream.fileno())
                identity = (info.st_dev, info.st_ino)
                if identity != self._identity or info.st_size < self._offset:
                    self._identity, self._offset = identity, 0
                stream.seek(self._offset)
                block = stream.read(_READ_LIMIT)
                # A writer may still be completing the final JSON record.
                complete = block.rfind(b"\n") + 1
                self._offset += complete
                for line in block[:complete].splitlines():
                    try:
                        row = json.loads(line)
                        if (
                            not isinstance(row, dict)
                            or row.get("event") != "media_sample"
                        ):
                            continue
                        camera, run = row.get("camera"), row.get("run")
                        when = row.get("observed_at")
                        if (
                            not isinstance(camera, str)
                            or not _CAMERA.fullmatch(camera)
                            or not isinstance(run, str)
                            or not _RUN.fullmatch(run)
                            or not isinstance(when, (float, int))
                            or not math.isfinite(when)
                        ):
                            continue
                        previous = self._runs.get(camera)
                        if previous and when <= previous[1]:
                            continue
                        self._runs[camera] = (run, when)
                        if self._primed and previous and previous[0] != run:
                            changes.append(
                                {
                                    "event": "recorder_run_changed",
                                    "camera": camera,
                                    "previous_run": previous[0],
                                    "run": run,
                                    "observed_at": when,
                                }
                            )
                    except (ValueError, TypeError):
                        continue
                self._primed = True
        except OSError:
            pass  # Missing or rotating diagnostics must not stop network capture.
        return changes


class RestartSnapshots:
    """Keep three before/after snapshots, with a cooldown for each camera."""

    def __init__(
        self,
        root,
        snapshot,
        emit,
        *,
        limit=3,
        cooldown=600,
        followup=60,
        clock=time.time,
        timer=threading.Timer,
    ):
        if limit < 1 or cooldown < 0 or followup < 0:
            raise ValueError("invalid snapshot bounds")
        self.root, self.snapshot, self.emit = Path(root), snapshot, emit
        self.limit, self.cooldown, self.followup = limit, cooldown, followup
        self.clock, self.timer = clock, timer
        self._lock = threading.RLock()
        self._timers = []
        self._last = {}
        self._closed = False
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        for directory in self._directories():
            try:
                row = json.loads((directory / "incident.json").read_text())
                camera, when = row["camera"], row["captured_at"]
                self._last[camera] = max(self._last.get(camera, float("-inf")), when)
            except (OSError, ValueError, KeyError, TypeError):
                continue

    def _directories(self):
        return sorted(
            p
            for p in self.root.iterdir()
            if _INCIDENT.fullmatch(p.name) and p.is_dir() and not p.is_symlink()
        )

    def trigger(self, event):
        """Save a transition without replacing the preserved first network stall."""
        with self._lock:
            if self._closed:
                return False
            now = self.clock()
            camera = event["camera"]
            elapsed = now - self._last.get(camera, float("-inf"))
            if 0 <= elapsed < self.cooldown:
                return False
            destination = (
                self.root / f"{int(now * 1_000_000):020d}-{uuid.uuid4().hex[:8]}"
            )
            try:
                destination.mkdir(mode=0o700)
                record = {
                    key: event[key]
                    for key in ("event", "camera", "run", "previous_run", "observed_at")
                }
                record["captured_at"] = now
                metadata = destination / "incident.json"
                metadata.write_text(json.dumps(record), encoding="utf-8")
                metadata.chmod(0o600)
                for old in self._directories()[: -self.limit]:
                    shutil.rmtree(old)
            except OSError as exc:
                shutil.rmtree(destination, ignore_errors=True)
                self.emit(
                    {
                        "event": "restart_snapshot_failed",
                        "error_type": type(exc).__name__,
                    }
                )
                return False
            self._last[camera] = now
            self._capture(destination, "initial", camera)
            self._timers = [timer for timer in self._timers if timer.is_alive()]
            timer = self.timer(
                self.followup, self._capture, args=(destination, "post_restart", camera)
            )
            timer.daemon = True
            self._timers.append(timer)
            timer.start()
            return True

    def _capture(self, destination, phase, camera):
        with self._lock:
            if self._closed or not destination.is_dir() or destination.is_symlink():
                return  # A newer incident may have evicted this one.
            try:
                target = destination / phase
                target.mkdir(mode=0o700)
                self.snapshot(target)
            except OSError as exc:
                self.emit(
                    {
                        "event": "restart_snapshot_failed",
                        "camera": camera,
                        "phase": phase,
                        "error_type": type(exc).__name__,
                    }
                )
                return
            self.emit(
                {
                    "event": "recorder_restart_preserved",
                    "camera": camera,
                    "phase": phase,
                    "incident": destination.name,
                }
            )

    def close(self):
        """Cancel delayed copies before the observer closes its log handler."""
        with self._lock:
            self._closed = True
            for timer in self._timers:
                timer.cancel()
