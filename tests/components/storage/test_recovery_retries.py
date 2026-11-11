"""Stalled historical publications must not starve current recording."""

import json
import logging
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from watchdog.events import FileCreatedEvent

from viseron.components.storage.check_tier import move_file
from viseron.components.storage.pending_journal import marker_for_path
from viseron.components.storage.tier_handler import TierHandler


def _handler():
    handler = object.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._storage = SimpleNamespace()
    handler._schedule_journal_retry = Mock()
    handler._on_created_impl = Mock()
    handler._journal_retry_lock = threading.Lock()
    return handler


def test_lost_source_run_does_not_rehash_historical_destination(tmp_path):
    """A reboot-lost RAM run fails cheaply before CIFS identity readback."""
    run = "20261106T131053.282747Z-f1df0ac79b78"
    source = tmp_path / "ram" / "camera" / run / "1.m4s"
    source.parent.mkdir(parents=True)
    (source.parent / "init.mp4").write_bytes(b"init")
    source.write_bytes(b"video")
    destination = tmp_path / "nas" / "camera" / run / source.name
    move_file(
        None,
        str(source),
        str(destination),
        logging.getLogger(__name__),
        camera_identifier="camera",
        publication_meta={
            "orig_ctime": "2026-11-06T13:10:53+00:00",
            "duration": 5,
            "file_key": 42,
        },
    )
    marker = Path(marker_for_path(str(destination)))
    record = json.loads(marker.read_text())
    record["identity"][1] += 1  # CIFS inode changed across the reboot.
    marker.write_text(json.dumps(record))
    shutil.rmtree(source.parent)
    handler = _handler()
    with patch(
        "viseron.components.storage.pending_journal.verify_file_contents",
        return_value=True,
    ) as readback:
        for _ in range(20):
            handler._on_created(FileCreatedEvent(str(destination)))
    assert readback.call_count == 0
    assert marker.exists() and destination.read_bytes() == b"video"
    handler._on_created_impl.assert_not_called()


def test_unchanged_hold_backs_off_without_blocking_fresh_publications(tmp_path):
    """Repeated watcher/replay events cannot bypass a held path's cooldown."""
    handler = _handler()
    handler._on_created_publication = Mock(return_value=False)
    old = tmp_path / "old.m4s"
    fresh = tmp_path / "fresh.m4s"
    clock = [0.0]
    with patch(
        "viseron.components.storage.tier_handler.time.monotonic",
        side_effect=lambda: clock[0],
    ):
        for tick in range(200):
            clock[0] = float(tick)
            handler._on_created(FileCreatedEvent(str(old)))
        assert handler._on_created_publication.call_count <= 8
        handler._on_created_publication.return_value = True
        handler._on_created(FileCreatedEvent(str(fresh)))
        assert handler._on_created_publication.call_args.args[0].src_path == str(fresh)


def test_fresh_retry_does_not_wait_for_historical_cooldown():
    """The shared timer must wake fresh failed work ahead of an old held file."""
    handler = object.__new__(TierHandler)
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_stopping = False
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_timer = None
    timers = []

    def timer(delay, callback):
        result = SimpleNamespace(
            delay=delay, callback=callback, start=Mock(), cancel=Mock()
        )
        timers.append(result)
        return result

    with patch(
        "viseron.components.storage.tier_handler.threading.Timer", side_effect=timer
    ):
        handler._schedule_journal_retry("/old.m4s", delay=300)
        handler._schedule_journal_retry("/fresh.m4s", delay=1)
    assert timers[-1].delay == 1
    assert timers[0].cancel.called
    timers[0].callback()
    assert handler._journal_retry_timer is timers[-1]


def test_a_held_publication_recovers_after_its_cooldown(tmp_path):
    """The backoff retains work, resumes automatically and clears on success."""
    handler = _handler()
    handler._on_created_publication = Mock(side_effect=[False, True, True])
    event = FileCreatedEvent(str(tmp_path / "fragment.m4s"))
    with patch(
        "viseron.components.storage.tier_handler.time.monotonic", return_value=0
    ):
        handler._on_created(event)
        handler._on_created(event)
        assert handler._on_created_publication.call_count == 1
    with patch(
        "viseron.components.storage.tier_handler.time.monotonic", return_value=5
    ):
        handler._on_created(event)
        handler._on_created(event)
        assert handler._on_created_publication.call_count == 3
