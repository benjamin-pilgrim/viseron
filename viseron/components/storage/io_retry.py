"""Bound file-operation retries when a storage filesystem is unavailable."""

from __future__ import annotations

import errno
import logging
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

RETRY_ERRNOS = frozenset(
    (
        errno.EROFS,
        errno.EIO,
        errno.ENOSPC,
        errno.ESTALE,
        errno.ENODEV,
        errno.ENXIO,
        errno.ETIMEDOUT,
        errno.ENOTCONN,
        errno.ECONNRESET,
    )
)
INITIAL_DELAY = 5.0
MAX_DELAY = 60.0
LOG_INTERVAL = 60.0
PUBLICATION_MAX_DELAY = 300.0
PUBLICATION_CACHE_SIZE = 1024


class StorageRetryLater(Exception):
    """File work was retained for a later attempt, without touching its metadata."""

    def __init__(self, delay: float) -> None:
        super().__init__("storage operation deferred")
        self.delay = delay


@dataclass
class StorageAttempt:
    """An admitted operation confirms completion separately from deferral."""

    completed: bool = False
    recovering: bool = False


@dataclass
class _Failure:
    delay: float = 0.0
    until: float = 0.0
    warned_at: float | None = None
    probing: bool = False


class FilesystemRetry:
    """Share cooldown across files/cameras, without pausing unrelated operations.

    Keys describe the operation and its source/destination filesystems, rather
    than individual files. A failed NAS promotion does not block RAM eviction.
    Only one worker probes a failed operation after its cooldown expires.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._lock = threading.Lock()
        self._failures: dict[tuple[str, ...], _Failure] = {}
        self._mounts: list[tuple[str, str]] = []
        # Reading proc does not access the remote filesystem. Mount IDs remain
        # useful grouping keys after a remount: the next successful probe resets
        # the cooldown, regardless of the replacement filesystem's device ID.
        try:
            with open("/proc/self/mountinfo", encoding="utf-8") as mounts:
                for line in mounts:
                    fields = line.split()
                    root = fields[4].replace("\\040", " ").replace("\\134", "\\")
                    self._mounts.append((root, fields[2]))
        except OSError:
            self._mounts.append(("/", "/"))
        self._mounts.sort(key=lambda mount: len(mount[0]), reverse=True)

    def _filesystem(self, path: str) -> str:
        path = os.path.abspath(path)
        for root, device in self._mounts:
            if path == root or path.startswith(root.rstrip("/") + "/"):
                return device
        return "/"

    @contextmanager
    def attempt(self, operation: str, *paths: str) -> Iterator[StorageAttempt]:
        """Admit work, or defer it with a capped exponential delay."""
        key = (operation, *(self._filesystem(path) for path in paths))
        with self._lock:
            failure = self._failures.get(key)
            if failure is not None:
                remaining = failure.until - time.monotonic()
                if failure.probing:
                    raise StorageRetryLater(failure.delay)
                if remaining > 0:
                    raise StorageRetryLater(remaining)
                failure.probing = True
        attempt = StorageAttempt(recovering=failure is not None)
        try:
            yield attempt
        except OSError as error:
            with self._lock:
                if failure is not None:
                    failure.probing = False
                if error.errno not in RETRY_ERRNOS:
                    raise
                failure = self._failures.setdefault(key, _Failure())
                failure.delay = min(MAX_DELAY, failure.delay * 2 or INITIAL_DELAY)
                now = time.monotonic()
                failure.until = now + failure.delay
                if failure.warned_at is None or now - failure.warned_at >= LOG_INTERVAL:
                    failure.warned_at = now
                    self._logger.warning(
                        "Storage %s failed on filesystems %s: %s; retrying in %.0fs "
                        "(further failures logged at most once per minute)",
                        operation,
                        key[1:],
                        error,
                        failure.delay,
                    )
                delay = failure.delay
            raise StorageRetryLater(delay) from error
        except BaseException:
            with self._lock:
                if failure is not None:
                    failure.probing = False
            raise
        else:
            with self._lock:
                if failure is not None:
                    failure.probing = False
                if (
                    attempt.completed
                    and failure is not None
                    and self._failures.get(key) is failure
                ):
                    del self._failures[key]
                    self._logger.info(
                        "Storage %s recovered on filesystems %s", operation, key[1:]
                    )


class PublicationRetry:
    """Back off unchanged held paths without blocking fresh publications.

    Timer events, polling events and recovery discovery share this admission
    gate. Keep bounded in-memory state; durable journals retain recovery proof.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._failures: dict[str, tuple[float, float]] = {}
        self._warned_at: float | None = None

    def remaining(self, path: str) -> float:
        """Return a held path's remaining cooldown, or admit a new attempt."""
        return max(0.0, self._failures.get(path, (0.0, 0.0))[1] - time.monotonic())

    def defer(self, path: str, *, notify: bool = True) -> float:
        """Retain work and retry from five seconds up to five minutes apart."""
        delay = min(
            PUBLICATION_MAX_DELAY,
            self._failures.get(path, (0.0, 0.0))[0] * 2 or INITIAL_DELAY,
        )
        if path not in self._failures and len(self._failures) >= PUBLICATION_CACHE_SIZE:
            self._failures.pop(next(iter(self._failures)))
        now = time.monotonic()
        self._failures[path] = (delay, now + delay)
        if notify and (
            self._warned_at is None or now - self._warned_at >= LOG_INTERVAL
        ):
            self._warned_at = now
            self._logger.warning(
                "Publication recovery incomplete for %s; files and recovery records "
                "retained, retrying in %.0fs "
                "(further holds logged at most once per minute)",
                path,
                delay,
            )
        return delay

    def complete(self, path: str) -> None:
        """Forget a completed path so later work is immediately admissible."""
        self._failures.pop(path, None)
