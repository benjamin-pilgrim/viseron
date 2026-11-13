"""Reboot-lost RAM must not leave permanent transfer retry work."""

import datetime
import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import select

from viseron.components.storage import lost_source_recovery as recovery, startup_chores
from viseron.components.storage.asset_moves import reserve as reserve_asset
from viseron.components.storage.check_tier import move_file as copy_file
from viseron.components.storage.lost_source_recovery import LostSourceRecovery
from viseron.components.storage.models import Files, PendingSegmentMove, Recordings
from viseron.components.storage.pending_journal import (
    assign_destination_id,
    marker_for_path,
)
from viseron.components.storage.segment_moves import reserve
from viseron.components.storage.tier_handler import TierHandler
from viseron.components.storage.util import tier_markers_available

from tests.components.storage.test_pressure_policy import _fill
from tests.components.storage.test_segment_moves import (
    promotion as promotion_fixture,
    source_row,
    start_promotion,
)

promotion = promotion_fixture


def _handler(storage, root, tier, kind="segments"):
    handler = object.__new__(TierHandler)
    handler._storage = storage
    handler._camera = SimpleNamespace(identifier="camera")
    handler._path = str(root / "camera")
    handler._tier_id = tier
    handler._tier = {"path": str(root)}
    handler._category = "recorder"
    handler._subcategory = kind
    handler._logger = logging.getLogger(__name__)
    handler._vis = Mock()
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_active = set()
    handler._hold_retry_stopping = False
    handler._source_intent_cursor = {}
    handler._tier_check_stopping = False
    handler._asset_move_cursor = 0
    handler._schedule_journal_retry = Mock()
    handler._retry_held_move = Mock()
    return handler


def _segment_handlers(promotion):
    storage = promotion.storage
    storage.config = {}
    source = _handler(storage, promotion.src.parents[2], 0)
    target = _handler(storage, promotion.dst.parents[2], 1)
    storage.camera_tier_handlers["camera"]["recorder"] = [
        {"segments": source},
        {"segments": target},
    ]
    nas = promotion.dst.parents[2]
    nas.mkdir(parents=True, exist_ok=True)
    (nas / ".test-volume").touch()
    storage.config = {"tier_markers": {str(nas): ".test-volume"}}
    storage.tier_available = lambda path: tier_markers_available(
        path, storage.config["tier_markers"]
    )
    return source, target


@pytest.mark.parametrize(
    "copied,indexed", [(False, False), (True, False), (True, True)]
)
def test_lost_ram_reconciles_through_bounded_replay(
    promotion, get_db_session, monkeypatch, copied, indexed
):
    """Restart recovery retires absent work or verifies and indexes its NAS copy."""
    source, _target = _segment_handlers(promotion)
    original = promotion.row
    when = original.orig_ctime
    event = Recordings(
        camera_identifier="camera",
        start_time=when,
        adjusted_start_time=when,
        end_time=when + datetime.timedelta(seconds=20),
    )
    with get_db_session() as session:
        session.add(event)
        session.commit()
        event_id = event.id
    if copied:
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
        if indexed:
            with get_db_session() as session:
                row = Files(
                    path=item.dst,
                    directory=str(promotion.dst.parent),
                    filename=promotion.dst.name,
                    tier_id=1,
                    tier_path=str(promotion.dst.parents[2]),
                    camera_identifier="camera",
                    category="recorder",
                    subcategory="segments",
                    file_key=42,
                    size=5,
                    orig_ctime=when,
                    duration=5,
                )
                session.add(row)
                session.commit()
                assign_destination_id(marker_for_path(item.dst), row.id)
    else:
        with get_db_session() as session:
            row = session.execute(select(Files)).scalar_one()
            reserve(session, row, str(promotion.dst), 1, str(promotion.dst.parents[2]))
            session.commit()
    shutil.rmtree(promotion.src.parent)
    mounts = {
        str(promotion.src.parents[2]): "tmpfs",
        str(promotion.dst.parents[2]): "cifs",
    }
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: mounts,
    )
    monkeypatch.setattr(os, "walk", Mock(side_effect=AssertionError("archive walk")))
    startup_chores(get_db_session)
    source._replay_segment_moves()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
        assert session.get(Recordings, event_id) is not None
        rows = session.execute(select(Files)).scalars().all()
        if copied:
            assert len(rows) == 1 and rows[0].path == str(promotion.dst)
            assert rows[0].file_key == 42 and rows[0].orig_ctime == when
            assert rows[0].duration == 5 and promotion.dst.read_bytes() == b"video"
            assert not Path(marker_for_path(str(promotion.dst))).exists()
        else:
            assert not rows
    source._retry_held_move.assert_not_called()


@pytest.mark.parametrize(
    "problem",
    [
        "segment",
        "init",
        "metadata",
        "unknown_mount",
        "offline",
        "source_alive",
        "other_key",
    ],
)
def test_uncertain_recovery_retains_all_evidence(
    promotion, get_db_session, monkeypatch, problem
):
    """Unknown mounts, conflicts and corrupt copies must not erase recovery proof."""
    source, _target = _segment_handlers(promotion)
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
    if problem != "source_alive":
        shutil.rmtree(promotion.src.parent)
    if problem == "segment":
        promotion.dst.write_bytes(b"other")
    if problem == "init":
        (promotion.dst.parent / "init.mp4").write_bytes(b"bad!")
    if problem == "metadata":
        with get_db_session() as session:
            pending = session.execute(select(PendingSegmentMove)).scalar_one()
            pending.metadata_record = dict(pending.metadata_record, file_key=999)
            session.commit()
    if problem == "other_key":
        with get_db_session() as session:
            other = Files(
                path=str(promotion.dst),
                directory=str(promotion.dst.parent),
                filename=promotion.dst.name,
                tier_id=1,
                tier_path=str(promotion.dst.parents[2]),
                camera_identifier="camera",
                category="recorder",
                subcategory="segments",
                file_key=999,
                size=5,
                orig_ctime=promotion.row.orig_ctime,
                duration=5,
            )
            session.add(other)
            session.commit()
        promotion.dst.unlink()
    mounts = {str(promotion.src.parents[2]): "tmpfs"}
    if problem != "unknown_mount":
        mounts[str(promotion.dst.parents[2])] = "cifs"
    if problem == "offline":
        promotion.storage.tier_available = lambda _: False
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: mounts,
    )
    startup_chores(get_db_session)
    source._replay_segment_moves()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first()
    assert Path(marker_for_path(str(promotion.dst))).exists()
    assert promotion.dst.exists() == (problem != "other_key")
    if problem == "source_alive":
        assert promotion.src.read_bytes() == b"video"


@pytest.mark.parametrize("destination", ["missing", "unproven_copy", "offline"])
def test_lost_thumbnail_reconciliation_preserves_event(
    tmp_path, get_db_session, monkeypatch, destination
):
    """Clear proven missing thumbnails; retain unverified copies and outages."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    src, dst = ram / "camera" / "1.jpg", nas / "camera" / "1.jpg"
    src.parent.mkdir(parents=True)
    nas.mkdir()
    src.write_bytes(b"jpeg")
    when = datetime.datetime(2026, 11, 6, tzinfo=datetime.timezone.utc)
    with get_db_session() as session:
        row = Files(
            path=str(src),
            directory=str(src.parent),
            filename=src.name,
            tier_id=0,
            tier_path=str(ram),
            category="recorder",
            subcategory="thumbnails",
            camera_identifier="camera",
            size=4,
            orig_ctime=when,
        )
        session.add(row)
        session.flush()
        row.move_target = reserve_asset(row, str(dst), 1, str(nas))
        event = Recordings(
            camera_identifier="camera",
            start_time=when,
            adjusted_start_time=when,
            end_time=when + datetime.timedelta(seconds=20),
            thumbnail_path=str(src),
        )
        session.add(event)
        session.commit()
        row_id, event_id = row.id, event.id
    src.unlink()
    (nas / ".test-volume").touch()
    if destination == "unproven_copy":
        dst.parent.mkdir()
        dst.write_bytes(b"jpeg")
    storage = SimpleNamespace(
        get_session=get_db_session,
        config={"tier_markers": {str(nas): ".test-volume"}},
        tier_available=lambda _: destination != "offline",
        pending_moves_lock=threading.Lock(),
        pending_moves={},
        temporary_files_meta={},
        pressure_manager=None,
        tier_check_worker_send_command=Mock(),
    )
    source, target = (
        _handler(storage, ram, 0, "thumbnails"),
        _handler(storage, nas, 1, "thumbnails"),
    )
    storage.camera_tier_handlers = {
        "camera": {"recorder": [{"thumbnails": source}, {"thumbnails": target}]}
    }
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: {str(ram): "tmpfs", str(nas): "cifs"},
    )
    startup_chores(get_db_session)
    source._replay_asset_moves()
    with get_db_session() as session:
        event = session.get(Recordings, event_id)
        assert event is not None
        if destination == "missing":
            assert session.get(Files, row_id) is None
            assert event.thumbnail_path is None
        else:
            assert session.get(Files, row_id).move_target is not None
            assert event.thumbnail_path == str(src)
    storage.tier_check_worker_send_command.assert_not_called()


@pytest.mark.parametrize("boundary", ["private_copy", "indexed", "receipt_removed"])
def test_reboot_recovery_survives_publication_crash_boundaries(
    promotion, get_db_session, monkeypatch, boundary
):
    """Each interrupted step stays discoverable and completes with one logical key."""

    source, _target = _segment_handlers(promotion)
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    if boundary == "private_copy":
        with patch(
            "viseron.components.storage.check_tier.publish_or_verify",
            side_effect=RuntimeError("crash"),
        ):
            with pytest.raises(RuntimeError):
                copy_file(
                    get_db_session,
                    item.src,
                    item.dst,
                    promotion.logger,
                    camera_identifier="camera",
                    publication_meta=item.publication_meta,
                )
    else:
        copy_file(
            get_db_session,
            item.src,
            item.dst,
            promotion.logger,
            camera_identifier="camera",
            publication_meta=item.publication_meta,
        )
    shutil.rmtree(promotion.src.parent)
    mounts = {
        str(promotion.src.parents[2]): "tmpfs",
        str(promotion.dst.parents[2]): "cifs",
    }
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: mounts,
    )
    startup_chores(get_db_session)
    if boundary != "private_copy":
        marker = Path(marker_for_path(item.dst))
        if boundary == "indexed":
            original = Path.unlink

            def fail_receipt(path, *args, **kwargs):
                if path == marker:
                    raise OSError(5, "crash after index commit")
                return original(path, *args, **kwargs)

            with patch.object(Path, "unlink", fail_receipt):
                source._replay_segment_moves()
        else:
            original = recovery._lost_run
            calls = [0]

            def fail_after_receipt(*args):
                calls[0] += 1
                if calls[0] == 3:
                    raise OSError(5, "crash after receipt removal")
                return original(*args)

            with patch.object(recovery, "_lost_run", fail_after_receipt):
                source._replay_segment_moves()
        with get_db_session() as session:
            assert session.execute(select(PendingSegmentMove)).first()
            assert len(session.execute(select(Files)).scalars().all()) == 1
        source, _target = _segment_handlers(promotion)  # A fresh process/retry gate.
    source._replay_segment_moves()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
        row = session.execute(select(Files)).scalar_one()
        assert row.path == item.dst and row.file_key == 42
    assert promotion.dst.read_bytes() == b"video"


def test_recovery_cooldowns_bound_unchanged_evidence_and_share_outages():
    """A thousand stale paths cannot turn one unavailable share into an I/O loop."""

    recovery = LostSourceRecovery(Mock())
    unavailable = Mock(side_effect=OSError(5, "offline"))
    for i in range(1000):
        assert recovery.attempt(f"/ram/{i}", f"/nas/{i}", unavailable)
    unavailable.assert_called_once()
    recovery = LostSourceRecovery(Mock())
    unresolved = Mock(return_value="deferred")
    with patch("viseron.components.storage.io_retry.time.monotonic", return_value=0):
        for _ in range(1000):
            assert recovery.attempt("/ram/a", "/nas/a", unresolved)
    unresolved.assert_called_once()
    assert recovery.attempt("/ram/new", "/nas/new", lambda: None) is False


def test_unavailable_nas_recovery_does_not_block_startup_or_spawn_more_workers(
    promotion, get_db_session, monkeypatch
):
    """Only the existing bounded daemon waits on a slow NAS."""

    source, _target = _segment_handlers(promotion)
    with get_db_session() as session:
        row = session.execute(select(Files)).scalar_one()
        reserve(session, row, str(promotion.dst), 1, str(promotion.dst.parents[2]))
        session.commit()
    shutil.rmtree(promotion.src.parent)
    mounts = {
        str(promotion.src.parents[2]): "tmpfs",
        str(promotion.dst.parents[2]): "cifs",
    }
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: mounts,
    )
    entered, release = threading.Event(), threading.Event()

    def available(path):
        if str(path).startswith(str(promotion.dst.parents[2])):
            entered.set()
            assert release.wait(5)
        return True

    promotion.storage.tier_available = available
    startup_chores(get_db_session)
    assert not entered.is_set()
    source._source_scan_lock = threading.Lock()
    started = time.monotonic()
    source._reconcile_source_intents()
    assert entered.wait(2)
    source._reconcile_source_intents()
    assert time.monotonic() - started < 2
    release.set()
    assert source._source_scan_lock.acquire(timeout=5)
    source._source_scan_lock.release()


def test_stale_destination_row_is_removed_only_when_mount_is_available(
    promotion, get_db_session, monkeypatch
):
    """Unavailable footage must stop advertising coverage without deleting events."""
    source, _target = _segment_handlers(promotion)
    with get_db_session() as session:
        row = session.execute(select(Files)).scalar_one()
        reserve(session, row, str(promotion.dst), 1, str(promotion.dst.parents[2]))
        clone = Files(
            path=str(promotion.dst),
            directory=str(promotion.dst.parent),
            filename=promotion.dst.name,
            tier_id=1,
            tier_path=str(promotion.dst.parents[2]),
            camera_identifier="camera",
            category="recorder",
            subcategory="segments",
            file_key=row.file_key,
            size=row.size,
            orig_ctime=row.orig_ctime,
            duration=row.duration,
        )
        session.add(clone)
        session.commit()
    shutil.rmtree(promotion.src.parent)
    mounts = {
        str(promotion.src.parents[2]): "tmpfs",
        str(promotion.dst.parents[2]): "cifs",
    }
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: mounts,
    )
    startup_chores(get_db_session)
    source._replay_segment_moves()
    with get_db_session() as session:
        assert session.execute(select(Files)).first() is None
        assert session.execute(select(PendingSegmentMove)).first() is None


@pytest.mark.parametrize("outage", [False, True], ids=["available", "outage-recovery"])
def test_real_cifs_recovery_after_ram_exhaustion(get_db_session, outage):
    """Replay actual copied data after RAM loss, including a stopped SMB server."""
    mount = os.environ.get("VISERON_LOST_RAM_CIFS")
    if not mount:
        pytest.skip("requires a disposable CIFS mount and outage coordinator")
    ram = Path("/pressure-test")
    control = Path("/test-control")
    assert os.path.ismount(ram)
    with tempfile.TemporaryDirectory(prefix="lost-ram-", dir=mount) as directory:
        nas = Path(directory)
        row = source_row(ram)
        src = Path(row.path)
        src.parent.mkdir(parents=True)
        original = b"0123456789abcdef" * 65536
        src.write_bytes(original)
        row.size = len(original)
        (src.parent / "init.mp4").write_bytes(b"init")
        dst = nas / "camera" / src.parent.name / src.name
        with get_db_session() as session:
            session.add(row)
            session.commit()
        storage = SimpleNamespace(
            get_session=get_db_session,
            config={},
            tier_available=lambda _: True,
            pending_moves={},
            pending_moves_lock=threading.Lock(),
            temporary_files_meta={},
            pressure_manager=None,
            tier_check_worker_send_command=Mock(),
        )
        (nas / ".test-volume").touch()
        storage.config = {"tier_markers": {str(nas): ".test-volume"}}
        source, target = _handler(storage, ram, 0), _handler(storage, nas, 1)
        target._tier = {"path": mount}
        storage.camera_tier_handlers = {
            "camera": {"recorder": [{"segments": source}, {"segments": target}]}
        }
        _fill(ram)
        assert os.statvfs(ram).f_bavail == 0
        filler = ram / "external-filler"
        with filler.open("r+b") as stream:
            stream.truncate(filler.stat().st_size - 65536)
        with get_db_session() as session:
            row = session.execute(select(Files)).scalar_one()
            job = reserve(session, row, str(dst), 1, mount)
            metadata = dict(
                job.metadata_record,
                pending_move_id=job.id,
                destination_tier_id=1,
                destination_tier_path=mount,
            )
            session.commit()
        copy_file(
            get_db_session,
            str(src),
            str(dst),
            logging.getLogger(__name__),
            camera_identifier="camera",
            publication_meta=metadata,
        )
        assert dst.read_bytes() == original and src.exists()
        shutil.rmtree(src.parent)  # The RAM run disappears; PostgreSQL survives.
        startup_chores(get_db_session)
        if outage:
            (control / "stop-ready").touch()
            _wait_flag(control / "stopped")
            source._replay_segment_moves()
            with get_db_session() as session:
                assert session.execute(select(PendingSegmentMove)).first()
                assert session.execute(select(Files)).first() is None
            (control / "recovery-ready").touch()
            _wait_flag(control / "recovered")
            time.sleep(5.1)  # First unavailable-filesystem cooldown expires.
        for _ in range(2):
            source._replay_segment_moves()
        with get_db_session() as session:
            assert session.execute(select(PendingSegmentMove)).first() is None
            restored = session.execute(select(Files)).scalar_one()
            assert restored.path == str(dst) and restored.file_key == 42
        assert dst.read_bytes() == original
        filler.unlink()
        (ram / "new-recording").write_bytes(original)
        (ram / "new-recording").unlink()


def _wait_flag(path):
    deadline = time.monotonic() + 90
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError(str(path))
        time.sleep(0.1)


def test_unmarked_empty_share_cannot_retire_a_transfer(
    promotion, get_db_session, monkeypatch
):
    """Mounted storage alone does not identify the expected NAS namespace."""
    source, _target = _segment_handlers(promotion)
    promotion.storage.config = {"tier_markers": {}}
    with get_db_session() as session:
        row = session.execute(select(Files)).scalar_one()
        reserve(session, row, str(promotion.dst), 1, str(promotion.dst.parents[2]))
        session.commit()
    shutil.rmtree(promotion.src.parent)
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: {
            str(promotion.src.parents[2]): "tmpfs",
            str(promotion.dst.parents[2]): "cifs",
        },
    )
    startup_chores(get_db_session)
    source._replay_segment_moves()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first()


@pytest.mark.parametrize(
    "kind,field", [("thumbnails", "thumbnail_path"), ("event_clips", "clip_path")]
)
def test_startup_clears_confirmed_missing_asset_links_without_nas_io(
    tmp_path, get_db_session, monkeypatch, kind, field
):
    """Ordinary RAM assets can disappear before a transfer was ever reserved."""
    ram, nas = tmp_path / "ram", tmp_path / "nas"
    ram.mkdir()
    path = ram / "camera" / "missing.asset"
    when = datetime.datetime(2026, 11, 6, tzinfo=datetime.timezone.utc)
    with get_db_session() as session:
        row = Files(
            path=str(path),
            directory=str(path.parent),
            filename=path.name,
            tier_id=0,
            tier_path=str(ram),
            camera_identifier="camera",
            category="recorder",
            subcategory=kind,
            size=5,
            orig_ctime=when,
        )
        event = Recordings(
            camera_identifier="camera",
            start_time=when,
            adjusted_start_time=when,
            end_time=when + datetime.timedelta(seconds=10),
            **{field: str(path)},
        )
        session.add_all([row, event])
        session.commit()
        event_id = event.id
    monkeypatch.setattr(
        "viseron.components.storage.startup_reconciliation._mount_filesystems",
        lambda: {str(ram): "tmpfs", str(nas): "cifs"},
    )
    real = os.stat

    def stat(candidate, *args, **kwargs):
        if str(candidate).startswith(str(nas)):
            raise AssertionError("startup contacted NAS")
        return real(candidate, *args, **kwargs)

    with patch(
        "viseron.components.storage.startup_reconciliation.os.stat", side_effect=stat
    ):
        startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.execute(select(Files)).first() is None
        event = session.get(Recordings, event_id)
        assert event is not None and getattr(event, field) is None
