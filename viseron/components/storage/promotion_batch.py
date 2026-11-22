"""One promotion quiet interval per storage root, shared by its cameras."""

from __future__ import annotations

import datetime
import threading
import time
from collections.abc import Callable

from viseron.helpers import utcnow


class PromotionBatch:
    """Drain a fixed batch, then wait from the final transfer completion."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._quiet_since = clock()
        self._cutoff: datetime.datetime | None = None
        self._members: set[str] = set()
        self._complete: set[str] = set()
        self._pending: set[str] = set()
        self._interval: int | None = None
        self._failed_until = 0.0
        self._had_transfers = False
        self.enabled = False

    def register(self, member: str, interval: int) -> None:
        """Use the shortest quiet interval requested by this root's cameras."""
        with self._lock:
            self._members.add(member)
            self._interval = (
                min(self._interval, interval)
                if self._interval is not None
                else interval
            )

    def request(self) -> tuple[datetime.datetime | None, list[str], bool]:
        """Return a fixed cutoff, transfers in flight and recent failure state."""
        with self._lock:
            if (
                self._members
                and self._cutoff is None
                and self._clock() - self._quiet_since >= (self._interval or 0)
            ):
                self._cutoff = utcnow()
                self._complete.clear()
                self._had_transfers = bool(self._pending)
            return self._cutoff, list(self._pending), self._clock() < self._failed_until

    def reserve(self, path: str) -> bool:
        """Admit a path once while its verified transfer is in flight."""
        with self._lock:
            if path in self._pending:
                return False
            self._pending.add(path)
            self._had_transfers = True
            return True

    def finish(self, path: str, success: bool) -> None:
        """Finish a transfer only after the move's verification/commit callback."""
        with self._lock:
            if path not in self._pending:
                return
            self._pending.discard(path)
            self._quiet_since = self._clock()
            if not success:
                self._failed_until = self._clock() + 15
            self._close_if_drained()

    def checked(self, member: str, exhausted: bool) -> None:
        """Each camera must drain the fixed cutoff before the root can rest."""
        with self._lock:
            if exhausted:
                self._complete.add(member)
            self._close_if_drained()

    def _close_if_drained(self) -> None:
        if (
            self._cutoff is not None
            and not self._pending
            and self._members <= self._complete
        ):
            self._cutoff = None
            if not self._had_transfers:
                self._quiet_since = self._clock()
