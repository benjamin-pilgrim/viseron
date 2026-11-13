"""Prompt, verified promotion from a full RAM tier, including active events."""

import contextlib
import errno
import logging
import os
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from watchdog.events import FileCreatedEvent

from viseron.components.storage import check_tier
from viseron.components.storage.check_tier import Worker, move_file as copy_file
from viseron.components.storage.dispatch_fence import create_token
from viseron.components.storage.models import Files, PendingSegmentMove
from viseron.components.storage.pending_journal import (
    marker_for_path,
    release_indexed_source,
)
from viseron.components.storage.pressure import reclaim
from viseron.components.storage.run_lifecycle import CameraLockBusy, camera_lock
from viseron.components.storage.source_move_intent import marker_for_source
from viseron.components.storage.tier_handler import (
    SegmentsTierHandler,
    TierHandler,
    move_file as queue_move,
)

from tests.components.storage.test_pressure_policy import (
    _configuration,
    _fill,
    _pressure_directory,
    _record,
)
from tests.components.storage.test_segment_moves import (
    promotion as promotion_fixture,
    start_promotion,
)

# Share the real source/publication fixture, rather than bypassing worker IPC.
promotion = promotion_fixture


def test_promote_old_fragment_from_active_event_to_healthy_next_tier(
    tmp_path, get_db_session
):
    """Active events can forward finalized fragments without allowing eviction."""
    root = tmp_path / "ram"
    root.mkdir()
    path = _record(
        get_db_session,
        root,
        "camera",
        datetime.now(UTC) - timedelta(minutes=2),
        1024,
        event=True,
        active=True,
    )
    config = _configuration(root, "preserve_events", maximum=512)
    config["reclaim"] = False
    config["cameras"][0]["has_next"] = True
    result = reclaim(config, get_db_session)
    assert path.exists()
    assert str(path) in [item["path"] for item in result.get("promote", [])], result


def test_copied_publication_notifies_destination_even_if_source_lock_busy(
    promotion, get_db_session
):
    """Final source-lock contention must not hide an already published copy."""
    source = promotion.storage.camera_tier_handlers["camera"]["recorder"][0]["segments"]
    target = promotion.storage.camera_tier_handlers["camera"]["recorder"][1]["segments"]
    source._check_tier_lock = threading.Lock()
    source._schedule_forced_tier_retry_locked = Mock()
    start_promotion(promotion, get_db_session)
    call = promotion.storage.tier_check_worker_send_command.call_args
    item = call.args[0]
    callback = call.kwargs["callback"]
    real = check_tier.camera_lock
    calls = [0]

    def source_busy_after_copy(root, key):
        if key == str(promotion.src.parent.parent):
            calls[0] += 1
            if calls[0] == 2:
                raise CameraLockBusy("injected final source contention")
        return real(root, key)

    worker = Worker()
    worker._get_session = get_db_session
    with patch(
        "viseron.components.storage.check_tier.camera_lock",
        side_effect=source_busy_after_copy,
    ):
        worker.work_input(item)
    assert item.deferred and item.error is None
    assert promotion.dst.exists() and os.path.exists(
        marker_for_path(str(promotion.dst))
    )
    assert item.publication_meta.get("pending_move_id") is not None
    callback(item)
    target._schedule_journal_retry.assert_called_once_with(str(promotion.dst))
    assert str(promotion.src) in promotion.storage.pending_moves
    assert str(promotion.dst) in promotion.storage.temporary_files_meta
    assert promotion.src.exists()


def test_busy_publication_lock_retries_promptly_instead_of_long_hold_backoff(tmp_path):
    """A busy mutex is retried promptly instead of backing off for minutes."""
    handler = object.__new__(TierHandler)
    handler._logger = logging.getLogger("pressure-investigation")
    handler._schedule_journal_retry = Mock()
    handler._on_created_impl = Mock()
    run = tmp_path / "camera" / "20261106T131053.282747Z-f1df0ac79b78"
    run.mkdir(parents=True)
    event = FileCreatedEvent(str(run / "1.m4s"))
    with patch(
        "viseron.components.storage.tier_handler.camera_lock",
        side_effect=CameraLockBusy("busy"),
    ):
        for _ in range(20):
            handler._on_created(event)
    assert handler._schedule_journal_retry.call_count == 20
    assert handler._schedule_journal_retry.call_args.kwargs.get("delay", 1) <= 1
    assert handler._publication_retry.remaining(str(run / "1.m4s")) == 0


@pytest.mark.parametrize("failure", ["before_reservation", "before_publication"])
def test_incomplete_copy_never_notifies_destination(promotion, get_db_session, failure):
    """A reservation or stale flag must not acknowledge unfinished media."""
    source = promotion.storage.camera_tier_handlers["camera"]["recorder"][0]["segments"]
    target = promotion.storage.camera_tier_handlers["camera"]["recorder"][1]["segments"]
    source._check_tier_lock = threading.Lock()
    source._schedule_forced_tier_retry_locked = Mock()
    start_promotion(promotion, get_db_session)
    call = promotion.storage.tier_check_worker_send_command.call_args
    item = call.args[0]
    worker = Worker()
    worker._get_session = get_db_session
    item.publication_meta["publication_ready"] = True  # Discard stale IPC state.
    if failure == "before_reservation":
        effect = patch(
            "viseron.components.storage.check_tier.begin_copy",
            side_effect=CameraLockBusy("busy"),
        )
    else:
        effect = patch(
            "viseron.components.storage.check_tier.publish_run_init",
            side_effect=OSError(errno.EIO, "NAS unavailable"),
        )
    with effect:
        worker.work_input(item)
    call.kwargs["callback"](item)
    target._schedule_journal_retry.assert_not_called()
    assert not item.publication_meta.get("publication_ready", False)
    assert promotion.src.read_bytes() == b"video"
    assert not promotion.dst.exists()


def test_busy_source_defers_before_destination_readback(promotion, get_db_session):
    """Skip costly remote readback when source release is already blocked."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    copy_file(
        get_db_session,
        item.src,
        item.dst,
        promotion.logger,
        camera_identifier="camera",
        publication_meta=item.publication_meta,
    )
    with (
        camera_lock("/tmp/viseron-run-locks", str(promotion.dst.parent.parent)),
        camera_lock("/tmp/viseron-run-locks", str(promotion.src.parent.parent)),
        patch(
            "viseron.components.storage.pending_journal.verify_file_contents",
            side_effect=AssertionError("unnecessary readback"),
        ),
        pytest.raises(CameraLockBusy),
    ):
        release_indexed_source(
            marker_for_path(str(promotion.dst)),
            [str(promotion.src.parent.parent)],
            lambda _: True,
            lambda _: True,
            "/tmp/viseron-run-locks",
            source_index_guard=Mock(side_effect=AssertionError("guard must not run")),
        )
    assert promotion.src.read_bytes() == b"video"


def _destination_handler(storage, root, handler_type=TierHandler):
    """Use real index/release logic with only event scheduling captured."""
    handler = object.__new__(handler_type)
    handler._storage = storage
    handler._camera = SimpleNamespace(identifier="camera")
    handler._tier_id = 1
    handler._tier = {"path": str(root)}
    handler._path = str(root / "segments" / "camera")
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._logger = logging.getLogger(__name__)
    handler._vis = Mock()
    handler.check_tier = Mock()
    handler._schedule_journal_retry = Mock()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_reported_issues = set()
    return handler


def test_publication_waits_for_recorder_before_index_and_source_release(
    promotion, get_db_session, caplog
):
    """Startup replay retains both copies until the camera recorder is ready."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    copy_file(
        get_db_session,
        item.src,
        item.dst,
        promotion.logger,
        camera_identifier="camera",
        publication_meta=item.publication_meta,
    )
    target = _destination_handler(
        promotion.storage, promotion.dst.parents[2], SegmentsTierHandler
    )

    class StartingCamera:
        identifier = "camera"

        @property
        def recorder(self):
            return self._recorder

    target._camera = StartingCamera()
    target.check_tier = lambda: target._camera.recorder.lookback
    event = FileCreatedEvent(str(promotion.dst))

    with caplog.at_level(logging.ERROR):
        target._on_created(event)
    assert "Pending publication failed" not in caplog.text
    with get_db_session() as session:
        assert [row.path for row in session.execute(select(Files)).scalars()] == [
            str(promotion.src)
        ]
        assert session.execute(select(PendingSegmentMove)).first()
    assert promotion.src.exists() and promotion.dst.exists()
    assert os.path.exists(marker_for_path(str(promotion.dst)))
    target._schedule_journal_retry.assert_called_once_with(
        str(promotion.dst), delay=1.0
    )

    target._camera._recorder = SimpleNamespace(lookback=5)
    target._on_created(event)
    with get_db_session() as session:
        assert [row.path for row in session.execute(select(Files)).scalars()] == [
            str(promotion.dst)
        ]
        assert session.execute(select(PendingSegmentMove)).first() is None
    assert not promotion.src.exists()
    assert not os.path.exists(marker_for_path(str(promotion.dst)))


@pytest.mark.parametrize(
    "outage", [False, True], ids=["nas-available", "nas-unavailable"]
)
@pytest.mark.parametrize("uncertain_staging", [False, True])
def test_full_ram_handoff_and_nas_recovery(
    get_db_session, tmp_path, outage, uncertain_staging
):
    """Real ENOSPC, worker, indexing and release retain every promised fragment.

    Set VISERON_HANDOFF_CIFS to exercise a mounted disposable CIFS share. The
    external coordinator stops Samba while the unavailable trial runs, then
    restarts it when this test writes its recovery-ready flag.
    """
    nas = Path(os.environ.get("VISERON_HANDOFF_CIFS", str(tmp_path)))
    control = os.environ.get("VISERON_HANDOFF_CONTROL")
    with (
        _pressure_directory() as ram,
        tempfile.TemporaryDirectory(prefix="handoff-", dir=nas) as remote,
    ):
        remote = Path(remote)
        now = datetime.now(UTC) - timedelta(hours=1)
        paths = [
            _record(
                get_db_session,
                ram,
                "camera",
                now + timedelta(seconds=i * 5),
                1024**2,
                event=(i == 0),
                active=(i == 0),
            )
            for i in range(4)
        ]
        source = SimpleNamespace(
            _path=str(paths[0].parent.parent),
            tier={"path": str(ram)},
            _check_tier_lock=threading.Lock(),
            _schedule_forced_tier_retry_locked=Mock(),
        )
        storage = SimpleNamespace(
            get_session=get_db_session,
            pressure_manager=None,
            pending_moves={},
            pending_moves_lock=threading.Lock(),
            temporary_files_meta={},
            tier_available=lambda _: True,
            tier_check_worker_send_command=Mock(),
        )
        target = _destination_handler(storage, remote)
        storage.camera_tier_handlers = {
            "camera": {"recorder": [{"segments": source}, {"segments": target}]}
        }
        config = _configuration(ram, "preserve_events", maximum="80%")
        config["volatile"] = True
        config["reclaim"] = False
        config["cameras"][0]["has_next"] = True
        held = None
        if uncertain_staging:
            spool = ram / "segments" / ".viseron-spool" / "camera"
            run = spool / "20261101T000000.000000Z-012345abcdef"
            run.mkdir(parents=True)
            held = run / "000000001-1793491200-0000005000000.m4s"
            held.write_bytes(b"uncertain" * (512 * 1024))
            (run / "init.mp4").write_bytes(b"held init")
            config["cameras"][0]["spool"] = str(spool)
        _fill(ram)
        assert os.statvfs(ram).f_bavail == 0
        # DB reservations and NAS receipts need no metadata headroom on RAM.
        plan = reclaim(config, get_db_session)
        assert {item["path"] for item in plan["promote"]} == {str(p) for p in paths}
        assert plan["deleted"] == 0
        if held is not None:
            assert held.exists()
            assert str(held) not in {item["path"] for item in plan["promote"]}
        originals = {p: p.read_bytes() for p in paths}
        if control and outage:
            Path(control, "stop-ready").touch()
            _wait_control(Path(control, "stopped"))
        for path in paths:
            dst = remote / path.relative_to(ram)
            queue_move(
                Mock(),
                storage,
                get_db_session,
                "camera",
                0,
                "recorder",
                "segments",
                str(path),
                str(dst),
                str(remote),
                logging.getLogger(__name__),
            )
            call = storage.tier_check_worker_send_command.call_args
            item = call.args[0]
            worker = Worker()
            worker._get_session = get_db_session
            assert not marker_for_source(path).exists()
            if outage:
                if control:
                    worker.work_input(item)  # Actual stopped Samba / CIFS failure.
                else:
                    with patch(
                        "viseron.components.storage.check_tier.publish_run_init",
                        side_effect=OSError(errno.EIO, "NAS unavailable"),
                    ):
                        worker.work_input(item)
                call.kwargs["callback"](item)
                assert not item.publication_meta.get("publication_ready", False)
                assert path.read_bytes() == originals[path]
                assert item.deferred and item.error is None
                # Do not probe an offline NAS to decide whether a copy exists.
                # Existing active-event fragments are never sacrificed to RAM pressure.
                config["reclaim"] = True
                result = reclaim(config, get_db_session)
                assert paths[2].exists() and paths[3].exists()
                assert result["deleted"] == 0
                assert result["state"] == "blocked"
                config["reclaim"] = False
                # Only exercise the failed copy once, then recover before draining.
                if control:
                    Path(control, "recovery-ready").touch()
                    _wait_control(Path(control, "recovered"))
                assert not dst.exists()
                # Exercise the actual held-move recovery callback, not the
                # original promotion callback that has already been completed.
                replay = object.__new__(TierHandler)
                replay._storage = storage
                replay._camera = SimpleNamespace(identifier="camera")
                replay._category = "recorder"
                replay._subcategory = "segments"
                replay._tier_id = 0
                replay._path = source._path
                replay._logger = logging.getLogger(__name__)
                replay._hold_retry_lock = threading.Lock()
                replay._hold_retry_active = {str(path)}
                replay._hold_retry_stopping = False
                replay._hold_retry_token = create_token()
                record = dict(
                    item.publication_meta,
                    source=str(path),
                    destination=str(dst),
                    size=len(originals[path]),
                )
                try:
                    assert replay._retry_held_move_worker(
                        str(marker_for_source(path)), record
                    )
                    call = storage.tier_check_worker_send_command.call_args
                    item = call.args[0]
                    worker = Worker()  # A fresh admitted recovery probe.
                    worker._get_session = get_db_session
                    _copy_with_source_contention(worker, item, source._path)
                finally:
                    os.unlink(replay._hold_retry_token)
                outage = False
            else:
                _copy_with_source_contention(worker, item, source._path)
            assert item.error is None and item.publication_meta["publication_ready"]
            assert path.read_bytes() == originals[path]  # Before index acknowledgement.
            assert dst.read_bytes() == originals[path]
            target._schedule_journal_retry.reset_mock()
            call.kwargs["callback"](item)
            target._schedule_journal_retry.assert_called_once_with(str(dst))
            target._on_created(FileCreatedEvent(str(dst)))
            assert not path.exists()
            assert dst.read_bytes() == originals[path]
            assert not os.path.exists(marker_for_path(str(dst)))
        assert not storage.pending_moves
        assert not storage.temporary_files_meta
        with get_db_session() as session:
            assert session.execute(select(PendingSegmentMove)).first() is None
            rows = session.execute(select(Files)).scalars().all()
            assert len(rows) == len(paths)
            assert all(row.tier_id == 1 for row in rows)
        (ram / "new-footage").write_bytes(b"n" * 1024**2)
        if held is not None:
            assert held.read_bytes() == b"uncertain" * (512 * 1024)
            assert (held.parent / "init.mp4").read_bytes() == b"held init"


def _wait_control(flag):

    deadline = time.monotonic() + 90
    while not flag.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(str(flag))
        time.sleep(0.1)


def _copy_with_source_contention(worker, item, source_path):
    """Contend only the final source step, after a real durable copy."""
    real = check_tier.camera_lock
    count = 0

    def busy_final_source(root, key):
        nonlocal count
        if key == source_path:
            count += 1
            if count == 2:
                raise CameraLockBusy("injected post-copy contention")
        return real(root, key)

    with patch(
        "viseron.components.storage.check_tier.camera_lock",
        side_effect=busy_final_source,
    ):
        worker.work_input(item)
    assert item.deferred and item.error is None


def test_database_transfer_recovers_crash_after_source_unlink(
    promotion, get_db_session
):
    """A failed DB finalize after unlink retries from the retained NAS receipt."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    copy_file(
        get_db_session,
        item.src,
        item.dst,
        promotion.logger,
        camera_identifier="camera",
        publication_meta=item.publication_meta,
    )
    target = _destination_handler(promotion.storage, promotion.dst.parents[2])
    original = target._guard_source_index

    @contextlib.contextmanager
    def interrupted(*args, **kwargs):
        with original(*args, **kwargs):

            def fail():
                raise SQLAlchemyError("crash after source unlink")

            yield fail

    event = FileCreatedEvent(str(promotion.dst))
    with patch.object(target, "_guard_source_index", side_effect=interrupted):
        target._on_created(event)
    assert not promotion.src.exists()
    assert promotion.dst.read_bytes() == b"video"
    assert not marker_for_source(promotion.src).exists()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first()
        assert session.execute(
            select(Files).where(Files.path == str(promotion.src))
        ).first()
    # A fresh handler models process restart, without the old retry cooldown.
    target = _destination_handler(promotion.storage, promotion.dst.parents[2])
    target._on_created(event)
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
        assert [r.path for r in session.execute(select(Files)).scalars()] == [
            str(promotion.dst)
        ]
