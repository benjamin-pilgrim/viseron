"""Verify retries for source holds left before destination publication."""

import datetime
import json
import logging
import threading
import time
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch
import pytest
from sqlalchemy import event, select, update
from watchdog.events import FileCreatedEvent
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.check_tier import move_file as do_move
from viseron.components.storage.models import Files
from viseron.components.storage.dispatch_fence import create_token
from viseron.components.storage.pending_journal import (
    marker_for_path,
    release_indexed_source,
)
from viseron.components.storage.source_move_intent import (
    marker_for_source,
    scan_source_intents,
)
from viseron.components.storage.tier_handler import TierHandler
from viseron.components.storage.run_lifecycle import camera_lock
from viseron.components.storage.storage_subprocess import DataItemMoveFile



def _replay_after_cooldown(handler, event):
    """Advance recovery phases without bypassing the publication admission gate."""
    now = max(time.monotonic(), getattr(handler, "_test_retry_time", 0)) + 301
    handler._test_retry_time = now
    with patch(
        "viseron.components.storage.tier_handler.time.monotonic", return_value=now
    ):
        handler._on_created(event)


def test_hold_retry_republishes_only_if_ownership_and_row_match(
    tmp_path, get_db_session
):
    """Restart replay retains source until the normal handoff completes."""
    run = "20261031T143215.482000Z-a7c93e12ab34"
    source_camera = tmp_path / "tier0" / "camera"
    cifs_root = os.environ.get("VISERON_TEST_CIFS_ROOT")
    cifs_tier = (
        Path(tempfile.mkdtemp(prefix=".hold-retry-", dir=cifs_root))
        if cifs_root
        else None
    )
    dest_camera = (cifs_tier if cifs_tier else tmp_path / "tier1") / "camera"
    src = source_camera / run / "1.m4s"
    src.parent.mkdir(parents=True)
    (src.parent / "init.mp4").write_bytes(b"init")
    src.write_bytes(b"video")
    dst = dest_camera / run / "1.m4s"
    ts = datetime.datetime(2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc)
    with get_db_session() as session:
        row = Files(
            tier_id=0,
            tier_path=str(tmp_path / "tier0"),
            camera_identifier="test",
            category="recorder",
            subcategory="segments",
            path=str(src),
            directory=str(src.parent),
            filename=src.name,
            size=5,
            orig_ctime=ts,
            duration=1.5,
            file_key=42,
        )
        session.add(row)
        session.commit()
        source_db = {"id": row.id, "tier_id": 0, "tier_path": row.tier_path}
    metadata = {
        "orig_ctime": ts.isoformat(),
        "duration": 1.5,
        "file_key": 42,
        "source_db": source_db,
    }
    with patch(
        "viseron.components.storage.check_tier.publish_run_init",
        side_effect=OSError("crash before publication"),
    ):
        try:
            do_move(
                None,
                str(src),
                str(dst),
                logging.getLogger("test"),
                camera_identifier="test",
                publication_meta=metadata,
            )
        except OSError:
            pass
        else:
            raise AssertionError("injection did not run")
    marker = marker_for_source(src)
    assert marker.exists() and src.exists() and not dst.exists()
    queue = []
    storage = SimpleNamespace(
        get_session=get_db_session,
        tier_available=lambda _: True,
        camera_tier_handlers={
            "test": {
                "recorder": [
                    {"segments": SimpleNamespace(_path=str(source_camera))},
                    {
                        "segments": SimpleNamespace(
                            _path=str(dest_camera), _schedule_journal_retry=Mock()
                        )
                    },
                ]
            }
        },
        tier_check_worker_send_command=lambda item, callback: queue.append(
            (item, callback)
        ),
    )
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test")
    handler._storage = storage
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._tier_id = 0
    handler._path = str(source_camera)
    handler._logger = logging.getLogger("test")
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_active = set()
    handler._hold_retry_token = create_token()
    handler._hold_retry_stopping = False
    handler._hold_retry_since = 0.0
    handler._hold_retry_warned = False
    # Stall manager IPC after the retry is submitted. Stopping must return
    # without waiting, and a late delivery must not start filesystem work.
    send_started = threading.Event()
    send_release = threading.Event()
    delayed = []

    def stalled_send(item, callback):
        send_started.set()
        assert send_release.wait(5)
        delayed.append((item, callback))

    storage.tier_check_worker_send_command = stalled_send
    handler._source_intent_job = None
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_paths = set()
    handler._journal_retry_timer = None
    handler._check_tier_lock = threading.Lock()
    handler._deletion_retry_stop = threading.Event()
    handler._tier_check_retry_timer = None
    handler._observer_lock = threading.Lock()
    handler._observer = None
    handler._pending_updates = {}
    handler._event_queue = Queue()
    handler._event_thread = Mock()
    handler._retry_held_move(str(marker), json.loads(marker.read_text()))
    assert send_started.wait(2)
    before_stop = time.monotonic()
    handler._stop_observer()
    assert time.monotonic() - before_stop < 1
    send_release.set()
    deadline = time.monotonic() + 3
    while not delayed and time.monotonic() < deadline:
        time.sleep(0.01)
    assert delayed
    item, callback = delayed.pop()
    worker = object.__new__(Worker)
    worker.move_file = Mock()
    worker.work_input(item)
    worker.move_file.assert_not_called()
    assert item.error == "retry cancelled after handler shutdown"
    callback(item)
    handler._hold_retry_token = create_token()
    handler._hold_retry_stopping = False
    storage.tier_check_worker_send_command = lambda item, callback: queue.append(
        (item, callback)
    )

    # A lost callback leaves the slot occupied, so surface the stall once.
    handler._hold_retry_active.add(str(src))
    handler._hold_retry_since = time.monotonic() - 600
    with patch.object(handler._logger, "warning") as warning:
        handler._retry_held_move(str(marker), json.loads(marker.read_text()))
        handler._retry_held_move(str(marker), json.loads(marker.read_text()))
    warning.assert_called_once()
    handler._hold_retry_active.clear()
    handler._hold_retry_warned = False

    probe_started = threading.Event()
    probe_release = threading.Event()

    def stalled_probe(_):
        probe_started.set()
        assert probe_release.wait(5)
        return True

    storage.tier_available = stalled_probe
    handler._retry_held_move(str(marker), json.loads(marker.read_text()))
    assert probe_started.wait(2)
    with handler._hold_retry_lock:
        handler._hold_retry_stopping = True
    probe_release.set()
    deadline = time.monotonic() + 3
    while handler._hold_retry_active and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not handler._hold_retry_active
    assert not queue
    with handler._hold_retry_lock:
        handler._hold_retry_stopping = False
    storage.tier_available = lambda _: True

    class ImmediateThread:
        def __init__(self, *, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    def scan():
        with patch(
            "viseron.components.storage.tier_handler.threading.Thread", ImmediateThread
        ):
            scan_source_intents(
                str(source_camera),
                get_db_session,
                lambda _: True,
                [str(dest_camera)],
                "/tmp/viseron-run-locks",
                retry_incomplete=handler._retry_held_move,
            )

    with camera_lock("/tmp/viseron-run-locks", str(dest_camera)):
        scan()
    assert not queue
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 7
        session.commit()
    scan()
    assert not queue and src.exists() and marker.exists()
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 0
        session.commit()
    scan()
    scan()
    assert len(queue) == 1
    item, callback = queue.pop()
    do_move(
        None,
        item.src,
        item.dst,
        logging.getLogger("test"),
        camera_identifier=item.camera_identifier,
        publication_meta=item.publication_meta,
    )
    callback(item)
    destination_handler = storage.camera_tier_handlers["test"]["recorder"][1][
        "segments"
    ]
    # Completion nudges indexing immediately; discovery still recovers a lost nudge.
    destination_handler._schedule_journal_retry.assert_called_once_with(str(dst))
    destination_handler._schedule_journal_retry.reset_mock()
    source_record = json.loads(marker.read_text())
    assert handler._retry_held_move_worker(str(marker), source_record) is False
    destination_handler._schedule_journal_retry.assert_called_once_with(str(dst))
    assert (
        dst.read_bytes() == b"video"
        and src.read_bytes() == b"video"
        and marker.exists()
    )
    assert not handler._hold_retry_active
    # Exercise the actual destination watcher and database acknowledgement.
    destination = object.__new__(TierHandler)
    destination._camera = SimpleNamespace(identifier="test")
    destination._storage = storage
    destination._storage.temporary_files_meta = {}
    destination._storage.pending_moves_lock = threading.Lock()
    destination._storage.tier_available = lambda _: True
    destination._category = "recorder"
    destination._subcategory = "segments"
    destination._tier_id = 1
    destination._tier = {"path": str(dest_camera.parent)}
    destination._logger = logging.getLogger("test")
    destination._schedule_journal_retry = Mock()
    destination._journal_retry_lock = threading.Lock()
    destination._journal_retry_timer = None
    destination._journal_retry_paths = set()
    destination._journal_retry_rescan = False
    destination._journal_retry_stopping = False
    destination._journal_reported_issues = set()
    destination._vis = Mock()
    destination.check_tier = Mock()
    # An old v2 journal may already have a destination row but no recorded
    # row ID. Do not silently adopt even a metadata-identical row.
    destination_marker = Path(marker_for_path(str(dst)))
    with get_db_session() as session:
        legacy = Files(
            tier_id=1,
            tier_path=str(dest_camera.parent),
            camera_identifier="test",
            category="recorder",
            subcategory="segments",
            path=str(dst),
            directory=str(dst.parent),
            filename=dst.name,
            size=5,
            orig_ctime=ts,
            duration=1.5,
            file_key=42,
        )
        session.add(legacy)
        session.commit()
        legacy_id = legacy.id
    with patch.object(destination._logger, "warning") as warning:
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    unbound = [
        call for call in warning.call_args_list if "unbound destination" in call.args[0]
    ]
    assert len(unbound) == 1
    assert "destination_id" not in json.loads(destination_marker.read_text())
    assert src.exists() and dst.exists() and marker.exists()
    with get_db_session() as session:
        legacy = session.execute(
            select(Files).where(Files.id == legacy_id)
        ).scalar_one()
        session.delete(legacy)
        session.commit()
    destination._schedule_journal_retry.reset_mock()

    # The source row changes while the completed destination awaits indexing.
    # The destination must not be indexed from stale journal metadata.
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 7
        session.commit()
    with patch.object(destination, "_schedule_journal_retry") as retry:
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    retry.assert_called_once()
    assert retry.call_args.args == (str(dst),)
    assert 5 <= retry.call_args.kwargs["delay"] <= 300
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(dst))
            ).scalar_one_or_none()
            is None
        )
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 0
        session.commit()
    assert src.exists() and marker.exists()

    # Simulate a process death after destination commit, then change the
    # source before replay. Replay must withdraw the stale destination row.
    with patch(
        "viseron.components.storage.tier_handler.release_indexed_source",
        side_effect=SystemExit("crash"),
    ):
        with pytest.raises(SystemExit):
            _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(dst))
            ).scalar_one_or_none()
            is not None
        )
        indexed = session.execute(
            select(Files).where(Files.path == str(dst))
        ).scalar_one()
        indexed_id = indexed.id
        values = {
            key: getattr(indexed, key)
            for key in (
                "tier_id",
                "tier_path",
                "camera_identifier",
                "category",
                "subcategory",
                "path",
                "directory",
                "filename",
                "size",
                "orig_ctime",
                "duration",
                "file_key",
            )
        }
        session.delete(indexed)
        session.commit()
        replacement = Files(**values)
        session.add(replacement)
        session.commit()
        replacement_id = replacement.id
        assert replacement_id != indexed_id
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 7
        session.commit()
    with patch.object(destination, "_schedule_journal_retry"):
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    with get_db_session() as session:
        replacement = session.execute(
            select(Files).where(Files.path == str(dst))
        ).scalar_one()
        assert replacement.id == replacement_id
        session.delete(replacement)
        session.commit()
        session.add(Files(id=indexed_id, **values))
        session.commit()
    with patch.object(destination, "_schedule_journal_retry"):
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(dst))
            ).scalar_one_or_none()
            is None
        )
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 0
        session.commit()
    assert src.exists() and dst.exists() and marker.exists()

    # A source row change immediately after destination commit must withdraw
    # the just-indexed row and leave all files and holds available for retry.
    def change_after_commit(*args, **kwargs):
        with get_db_session() as session:
            row = session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one()
            row.tier_id = 7
            session.commit()
        return release_indexed_source(*args, **kwargs)

    with (
        patch(
            "viseron.components.storage.tier_handler.release_indexed_source",
            side_effect=change_after_commit,
        ),
        patch.object(destination, "_schedule_journal_retry"),
    ):
        _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    assert src.exists() and dst.exists() and marker.exists()
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(dst))
            ).scalar_one_or_none()
            is None
        )
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 0
        session.commit()
    # Verify the source row is locked for the entire destination index
    # transaction, even against a database writer that ignores advisory locks.
    engine = get_db_session.kw["bind"]
    insert_started = threading.Event()
    insert_release = threading.Event()
    update_done = threading.Event()
    errors = []

    def gate_insert(_conn, _cursor, statement, _parameters, _context, _executemany):
        if (
            threading.current_thread().name == "index-destination"
            and statement.lstrip().upper().startswith("INSERT INTO FILES")
        ):
            insert_started.set()
            assert insert_release.wait(5)

    def index_destination():
        try:
            _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
        except Exception as exc:
            errors.append(exc)

    def update_source():
        try:
            with get_db_session() as session:
                session.execute(
                    update(Files).where(Files.path == str(src)).values(tier_id=7)
                )
                session.commit()
        except Exception as exc:
            errors.append(exc)
        finally:
            update_done.set()

    event.listen(engine, "before_cursor_execute", gate_insert)
    index_thread = threading.Thread(target=index_destination, name="index-destination")
    update_thread = threading.Thread(target=update_source, name="update-source")
    try:
        index_thread.start()
        assert insert_started.wait(3)
        update_thread.start()
        assert not update_done.wait(0.2)
    finally:
        insert_release.set()
        index_thread.join(5)
        if update_thread.ident is not None:
            update_thread.join(5)
        event.remove(engine, "before_cursor_execute", gate_insert)
    assert not index_thread.is_alive() and not update_thread.is_alive()
    assert not errors
    if src.exists():
        with get_db_session() as session:
            row = session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one()
            row.tier_id = 0
            session.commit()
        with patch.object(destination, "_schedule_journal_retry"):
            _replay_after_cooldown(destination, FileCreatedEvent(str(dst)))
    assert not src.exists() and not marker.exists()
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one_or_none()
            is None
        )
        assert (
            session.execute(select(Files).where(Files.path == str(dst)))
            .scalar_one()
            .file_key
            == 42
        )
    os.unlink(handler._hold_retry_token)
    if cifs_tier:
        shutil.rmtree(cifs_tier)


def test_stalled_source_scan_does_not_block_scheduler():
    """A single isolated scan owns the slot while a mount is stalled."""
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    handler = object.__new__(TierHandler)
    handler._source_scan_lock = threading.Lock()
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_stopping = False
    handler._hold_retry_active = set()
    handler._hold_retry_since = 0.0
    handler._hold_retry_warned = False
    calls = []

    def blocked_scan():
        calls.append(1)
        started.set()
        release.wait(5)
        finished.set()

    handler._replay_segment_moves = blocked_scan
    try:
        handler._reconcile_source_intents()
        assert started.wait(2)
        handler._reconcile_source_intents()
        assert calls == [1]
    finally:
        release.set()
        assert finished.wait(2)


def test_lost_callback_warned_without_another_incomplete_hold():
    """Periodic scans report a stuck slot even if its marker is now complete."""
    handler = object.__new__(TierHandler)
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_stopping = False
    handler._hold_retry_active = {"held-source"}
    handler._hold_retry_since = time.monotonic() - 600
    handler._hold_retry_warned = False
    handler._source_scan_lock = threading.Lock()
    handler._logger = Mock()
    handler._replay_segment_moves = Mock()
    handler._reconcile_source_intents()
    handler._reconcile_source_intents()
    handler._logger.warning.assert_called_once()


def test_open_token_unlinked_before_dispatch_is_rejected():
    """An open token that was unlinked before the start check is stale."""
    token = create_token()
    item = DataItemMoveFile(
        cmd="move_file", src="source", dst="destination", dispatch_token=token
    )
    worker = object.__new__(Worker)
    worker.move_file = Mock()
    real_fstat = os.fstat

    def unlink_before_check(fd):
        os.unlink(token)
        return real_fstat(fd)

    with patch(
        "viseron.components.storage.dispatch_fence.os.fstat",
        side_effect=unlink_before_check,
    ):
        worker.work_input(item)
    worker.move_file.assert_not_called()
    assert item.error == "retry cancelled after handler shutdown"


def test_work_started_before_stop_can_finish():
    """A started job is not claimed to be cancelled by token removal."""
    token = create_token()
    item = DataItemMoveFile(
        cmd="move_file", src="source", dst="destination", dispatch_token=token
    )
    worker = object.__new__(Worker)
    started = threading.Event()
    finish = threading.Event()

    def do_work(_item):
        started.set()
        assert finish.wait(3)

    worker.move_file = do_work
    thread = threading.Thread(target=worker.work_input, args=(item,))
    try:
        thread.start()
        assert started.wait(2)
        os.unlink(token)
    finally:
        finish.set()
        thread.join(3)
    assert not thread.is_alive()
    assert item.error is None


def test_killed_owner_invalidates_token_even_while_zombie():
    """A survivor must not start a retry after its token owner is killed."""
    owner = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "from viseron.components.storage.dispatch_fence import create_token; "
            "import time; print(create_token(), flush=True); time.sleep(60)",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    token = owner.stdout.readline().strip()
    try:
        assert token
        owner.kill()
        os.waitid(os.P_PID, owner.pid, os.WEXITED | os.WNOWAIT)
        item = DataItemMoveFile(
            cmd="move_file", src="source", dst="destination", dispatch_token=token
        )
        worker = object.__new__(Worker)
        worker.move_file = Mock()
        worker.work_input(item)
        worker.move_file.assert_not_called()
        assert item.error == "retry cancelled after handler shutdown"
    finally:
        if owner.poll() is None:
            owner.send_signal(signal.SIGKILL)
        owner.wait()
        if os.path.exists(token):
            os.unlink(token)


def test_reused_pid_does_not_authorize_old_token():
    """A live process with the same PID but a different start time is rejected."""
    token = create_token()
    try:
        with patch(
            "viseron.components.storage.dispatch_fence.process_identity",
            return_value=("S", -1),
        ):
            item = DataItemMoveFile(
                cmd="move_file", src="source", dst="destination", dispatch_token=token
            )
            worker = object.__new__(Worker)
            worker.move_file = Mock()
            worker.work_input(item)
        worker.move_file.assert_not_called()
        assert item.error == "retry cancelled after handler shutdown"
    finally:
        os.unlink(token)
