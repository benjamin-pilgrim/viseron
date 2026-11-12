"""Bound failed storage work across different files on the same tier."""

import contextlib
import errno
import logging
import threading
from unittest.mock import Mock, patch

import pytest

from watchdog.events import FileCreatedEvent

from viseron.components.storage import io_retry

from viseron.components.storage.source_move_intent import marker_for_source
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.storage_subprocess import (
    DataItemDeleteFile,
    DataItemMoveFile,
)
from viseron.components.storage.tier_handler import TierHandler, _schedule_source_retry


@pytest.mark.parametrize("failure", [errno.EROFS, errno.EHOSTDOWN, errno.EHOSTUNREACH])
def test_read_only_batch_does_not_retry_every_file(caplog, failure):
    """One unavailable NAS must not cause a write attempt and log per fragment."""
    worker = Worker()
    worker.move_file = Mock(side_effect=OSError(failure, "NAS unavailable"))
    items = [
        DataItemMoveFile(
            cmd="move_file",
            src=f"/segments/camera/run/{i}.m4s",
            dst=f"/archive/camera/run/{i}.m4s",
        )
        for i in range(1000)
    ]
    for item in items:
        worker.work_input(item)
    assert worker.move_file.call_count == 1
    assert all(item.deferred and item.error is None for item in items)
    assert len(caplog.records) == 1


def test_outage_backoff_recovers_and_leaves_other_storage_usable(monkeypatch, caplog):
    """Probe at 5/10/20/40/60 seconds, recover automatically, keep RAM usable."""

    now = [1000.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    worker = Worker()
    worker._io_retry._mounts = [("/nas", "nas"), ("/ram", "ram"), ("/", "root")]
    worker.move_file = Mock(side_effect=OSError(errno.EIO, "I/O error"))
    worker.delete_file = Mock()

    def move():
        item = DataItemMoveFile(
            cmd="move_file", src="/ram/segment.m4s", dst="/nas/segment.m4s"
        )
        worker.work_input(item)
        return item

    for expected, attempts in zip((5, 10, 20, 40, 60, 60), range(1, 7)):
        failed = move()
        assert failed.deferred and failed.retry_after == expected
        assert worker.move_file.call_count == attempts
        assert move().deferred
        assert worker.move_file.call_count == attempts
        healthy_delete = DataItemDeleteFile(cmd="delete_file", src="/ram/old.m4s")
        worker.work_input(healthy_delete)
        assert not healthy_delete.deferred and healthy_delete.error is None
        now[0] += expected
    worker.move_file.side_effect = None
    recovered = move()
    assert not recovered.deferred and recovered.error is None
    worker.move_file.side_effect = OSError(errno.EROFS, "Read-only file system")
    assert move().retry_after == 5
    # Repeated probes log at most once per minute during a continuing outage.
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 4


def test_only_one_concurrent_recovery_probe(monkeypatch):
    """Another worker cannot bypass the cooldown while its recovery probe hangs."""

    now = [100.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    worker = Worker()
    worker.move_file = Mock(side_effect=OSError(errno.EROFS, "read only"))
    first = DataItemMoveFile(
        cmd="move_file", src="/segments/first", dst="/archive/first"
    )
    worker.work_input(first)
    now[0] += 5
    started, release = threading.Event(), threading.Event()

    def probe(_item):
        started.set()
        assert release.wait(5)

    worker.move_file.side_effect = probe
    recovered = DataItemMoveFile(
        cmd="move_file", src="/segments/second", dst="/archive/second"
    )
    thread = threading.Thread(target=worker.work_input, args=(recovered,))
    thread.start()
    try:
        assert started.wait(5)
        another = DataItemMoveFile(
            cmd="move_file", src="/segments/third", dst="/archive/third"
        )
        worker.work_input(another)
        assert another.deferred
        assert worker.move_file.call_count == 2
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and not recovered.deferred


def test_failed_copy_keeps_real_source_and_database(tmp_path, monkeypatch):
    """The real move path must retain the only recording through an outage."""
    source = tmp_path / "fragment.m4s"
    source.write_bytes(b"irreplaceable footage")
    destination = tmp_path / "archive" / "fragment.m4s"
    worker = Worker()
    worker._get_session = Mock()
    fail = Mock(side_effect=OSError(errno.EROFS, "Read-only file system"))
    monkeypatch.setattr("tempfile.mkstemp", fail)
    for _ in range(20):
        item = DataItemMoveFile(cmd="move_file", src=str(source), dst=str(destination))
        worker.work_input(item)
        assert item.deferred
    assert fail.call_count == 1
    assert source.read_bytes() == b"irreplaceable footage"
    assert not destination.exists()
    worker._get_session.assert_not_called()


def test_non_outage_error_remains_visible(caplog):
    """Permission or validation errors must not be hidden as storage outages."""
    worker = Worker()
    worker.move_file = Mock(side_effect=PermissionError(errno.EACCES, "permissions"))
    item = DataItemMoveFile(cmd="move_file", src="source", dst="dest")
    worker.work_input(item)
    assert not item.deferred and "permissions" in item.error
    assert "Error processing command" in caplog.text


def test_pending_publication_events_are_coalesced_during_outage(caplog):
    """Fresh watcher events cannot bypass journal backoff or flood tracebacks."""

    handler = TierHandler.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._on_created_publication = Mock(side_effect=OSError(errno.EIO, "I/O error"))
    with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
        for i in range(1000):
            handler._on_created(FileCreatedEvent(f"/segments/camera/run/{i}.m4s"))
    assert handler._on_created_publication.call_count == 1
    assert len(caplog.records) == 1
    assert timer.call_count == 1 and timer.call_args.args[0] == 5


def test_source_retry_uses_worker_cooldown():
    """The parent must not turn a 60-second I/O cooldown into one-second polls."""

    handler = Mock()
    handler._check_tier_lock = threading.Lock()
    storage = Mock()
    storage.camera_tier_handlers = {"camera": {"recorder": [{"segments": handler}]}}
    _schedule_source_retry(
        storage,
        "camera",
        "recorder",
        0,
        "segments",
        "fragment",
        logging.getLogger(),
        retry_after=60,
    )
    handler._schedule_forced_tier_retry_locked.assert_called_once_with(60)


def test_cancelled_dispatch_is_not_a_recovery_probe(monkeypatch, caplog):
    """Cancellation must not reset an outage or renew its warning budget."""
    now = [100.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    worker = Worker()
    worker.move_file = Mock(side_effect=OSError(errno.EROFS, "read only"))

    def move(**kwargs):
        item = DataItemMoveFile(cmd="move_file", src="source", dst="dest", **kwargs)
        worker.work_input(item)
        return item

    assert move().retry_after == 5
    now[0] += 5
    with patch(
        "viseron.components.storage.check_tier.token_permits_start", return_value=False
    ):
        cancelled = move(dispatch_token="stopped")
    assert "cancelled" in cancelled.error
    assert move().retry_after == 10
    assert worker.move_file.call_count == 2
    assert len(caplog.records) == 1


def test_incomplete_publication_does_not_reset_outage(monkeypatch, caplog):
    """A busy/no-op publication must not count as successful storage recovery."""
    now = [100.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    handler = TierHandler.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._schedule_journal_retry = Mock()
    # The real publication method returns without completion when busy or when
    # validation fails. Its wrapper must retain the existing outage state.
    handler._on_created_publication = Mock(
        side_effect=[OSError(errno.EIO, "I/O"), None, OSError(errno.EIO, "I/O")]
    )
    event = FileCreatedEvent("/segments/camera/run/fragment.m4s")
    handler._on_created(event)
    now[0] += 5
    handler._on_created(event)
    now[0] += 5  # An incomplete attempt also has a per-path cooldown.
    handler._on_created(event)
    assert handler._schedule_journal_retry.call_args.kwargs["delay"] == 10
    assert len(caplog.records) == 1


def test_durable_hold_is_not_an_outage(tmp_path):
    """The worker must distinguish healthy durable replay from failed promotion."""
    source = str(tmp_path / "fragment.m4s")
    marker_for_source(source).write_text("held")
    worker = Worker()
    worker.move_file = Mock()
    item = DataItemMoveFile(cmd="move_file", src=source, dst="destination")
    worker.work_input(item)
    assert item.deferred and item.deferred_by_hold and item.error is None
    assert item.retry_after == 0
    worker.move_file.assert_not_called()
    assert marker_for_source(source).read_text() == "held"


def test_completed_publication_resets_backoff(monkeypatch, caplog):
    """Actual completion clears the outage so a new failure starts at five seconds."""
    now = [100.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    handler = TierHandler.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._schedule_journal_retry = Mock()
    handler._on_created_publication = Mock(
        side_effect=[OSError(errno.EIO, "I/O"), True, OSError(errno.EIO, "I/O")]
    )
    event = FileCreatedEvent("/segments/camera/run/fragment.m4s")
    handler._on_created(event)
    now[0] += 5
    handler._on_created(event)
    handler._on_created(event)
    assert handler._schedule_journal_retry.call_args.kwargs["delay"] == 5
    assert len(caplog.records) == 2


def test_real_publication_lock_contention_keeps_outage(monkeypatch, caplog, tmp_path):
    """The real publisher's handled busy result cannot reset an I/O failure."""
    now = [100.0]
    monkeypatch.setattr(io_retry.time, "monotonic", lambda: now[0])
    handler = TierHandler.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._schedule_journal_retry = Mock()
    handler._on_created_impl = Mock(side_effect=OSError(errno.EIO, "I/O"))
    run = tmp_path / "camera" / "20261108T020033.168132Z-4f85895de5de"
    run.mkdir(parents=True)
    event = FileCreatedEvent(str(run / "fragment.m4s"))
    with patch(
        "viseron.components.storage.tier_handler.camera_lock",
        side_effect=[
            contextlib.nullcontext(),
            BlockingIOError(errno.EAGAIN, "busy"),
            contextlib.nullcontext(),
        ],
    ):
        handler._on_created(event)
        now[0] += 5
        handler._on_created(event)
        now[0] += 5  # Lock contention must not trigger an immediate disk probe.
        handler._on_created(event)
    assert handler._on_created_impl.call_count == 2
    assert handler._schedule_journal_retry.call_args.kwargs["delay"] == 10
    assert len(caplog.records) == 1
