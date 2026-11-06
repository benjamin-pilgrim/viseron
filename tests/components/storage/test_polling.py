"""Tests for limiting recursive polling of large recording archives."""

import os
import queue
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import voluptuous as vol
from watchdog.events import FileCreatedEvent, FileDeletedEvent

from viseron.components.storage.config import (
    TIER_SCHEMA_RECORDER,
    TIER_SCHEMA_SNAPSHOTS,
)
from viseron.components.storage.tier_handler import TierHandler


@pytest.mark.parametrize("schema", [TIER_SCHEMA_RECORDER, TIER_SCHEMA_SNAPSHOTS])
def test_poll_interval_configuration(schema) -> None:
    """Both tier types accept seconds and retain the existing default."""
    assert schema({"path": "/recordings"})["poll_interval"] == 1
    assert schema({"path": "/recordings", "poll_interval": 30})["poll_interval"] == 30
    for invalid in (0, -1, 1.5, "30", None):
        with pytest.raises(vol.Invalid):
            schema({"path": "/recordings", "poll_interval": invalid})


def test_archive_polling_honors_interval_and_detects_changes(tmp_path: Path) -> None:
    """A slower poll retains recursive create/delete detection for run files."""
    archived = tmp_path / "old-run"
    archived.mkdir()
    for number in range(20):
        (archived / f"{number}.m4s").touch()
    handler = TierHandler.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._tier = {"poll": True, "poll_interval": 30}
    handler._storage = MagicMock()
    handler._storage.ignored_files = []
    handler._event_queue = queue.Queue()
    observer = handler._start_observer()
    try:
        assert observer.timeout == 30
        emitter = next(iter(observer.emitters))
        assert emitter.timeout == 30
        # Drive real snapshot diffs directly, avoiding a 30-second test wait.
        run = tmp_path / "new-run"
        run.mkdir()
        fragment = run / "1.m4s"
        fragment.write_bytes(b"complete")
        emitter.queue_events(0)
        event = handler._event_queue.get(timeout=2)
        assert isinstance(event, FileCreatedEvent)
        assert event.src_path == str(fragment)
        os.unlink(fragment)
        emitter.queue_events(0)
        while True:
            event = handler._event_queue.get(timeout=2)
            if isinstance(event, FileDeletedEvent):
                assert event.src_path == str(fragment)
                break
    finally:
        observer.stop()
        observer.join(timeout=2)
        assert not observer.is_alive()


def test_recovered_tier_retains_poll_interval(tmp_path: Path) -> None:
    """Replacing a watcher after an outage must retain the configured scan rate."""
    handler = TierHandler.__new__(TierHandler)
    handler._path = str(tmp_path)
    handler._tier = {"poll": True, "poll_interval": 30}
    handler._storage = MagicMock()
    handler._storage.tier_available.return_value = True
    handler._storage.ignored_files = []
    handler._event_queue = queue.Queue()
    handler._observer_lock = threading.Lock()
    handler._observer_stopping = False
    handler._replay_pending_publications = MagicMock()
    handler._observer = handler._start_observer()
    old = handler._observer
    try:
        assert handler.restart_for_tier(str(tmp_path))
        assert handler._observer is not old
        assert handler._observer.timeout == 30
        assert next(iter(handler._observer.emitters)).timeout == 30
        handler._replay_pending_publications.assert_called_once()
    finally:
        handler._observer.stop()
        handler._observer.join(timeout=2)
        old.stop()
        old.join(timeout=2)
