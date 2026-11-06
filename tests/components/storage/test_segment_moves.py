"""Database discovery and protection of unfinished segment promotions."""

import contextlib
import datetime
import importlib
import logging
import threading
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from watchdog.events import FileCreatedEvent

from viseron.components.storage.check_tier import delete_file
from viseron.components.storage.check_tier import move_file as copy_file
from viseron.components.storage.models import Files, PendingSegmentMove
from viseron.components.storage.pressure import reclaim
from viseron.components.storage.segment_moves import (
    acknowledge,
    finish_completed,
    import_legacy,
    protects,
    reserve,
)
from viseron.components.storage.source_move_intent import marker_for_source
from viseron.components.storage.tier_handler import TierHandler, move_file


def source_row(root: Path) -> Files:
    """A closed indexed source fragment."""
    path = root / "camera" / "20261106T131053.282747Z-f1df0ac79b78" / "1.m4s"
    return Files(
        path=str(path),
        directory=str(path.parent),
        filename=path.name,
        tier_id=0,
        tier_path=str(root),
        camera_identifier="camera",
        category="recorder",
        subcategory="segments",
        size=5,
        file_key=42,
        duration=5,
        orig_ctime=datetime.datetime(2026, 11, 6, tzinfo=datetime.timezone.utc),
    )


def test_reservation_survives_source_row_removal(tmp_path, get_db_session):
    """A crash after source DB deletion must leave discoverable cleanup work."""
    row = source_row(tmp_path / "ram")
    source = row.path
    destination = str(
        tmp_path / "nas" / "camera" / Path(row.directory).name / row.filename
    )
    with get_db_session() as session:
        session.add(row)
        session.flush()
        pending = reserve(session, row, destination, 1, str(tmp_path / "nas"))
        identity = pending.id
        metadata = dict(pending.metadata_record, pending_move_id=identity)
        session.commit()
        assert protects(session, row.path) and protects(session, destination)
        session.delete(row)
        session.commit()
    with get_db_session() as session:
        assert session.get(PendingSegmentMove, identity) is not None
        acknowledge(session, source, destination, metadata)
        assert session.get(PendingSegmentMove, identity) is None


def test_reservation_rejects_changed_source(tmp_path, get_db_session):
    """A different file identity must not adopt an interrupted move."""
    with get_db_session() as session:
        row = source_row(tmp_path / "ram")
        session.add(row)
        session.flush()
        dst = str(tmp_path / "nas" / "camera" / Path(row.directory).name / row.filename)
        first = reserve(session, row, dst, 1, str(tmp_path / "nas"))
        session.commit()
        assert reserve(session, row, dst, 1, str(tmp_path / "nas")).id == first.id
        row.file_key = 43
        with pytest.raises(ValueError, match="conflicts"):
            reserve(session, row, dst, 1, str(tmp_path / "nas"))


def test_recovery_uses_only_pending_database_paths(
    tmp_path, get_db_session, monkeypatch
):
    """Empty NAS recovery must not enumerate archive directories."""

    handler = object.__new__(TierHandler)
    handler._path = str(tmp_path / "nas" / "camera")
    handler._camera = SimpleNamespace(identifier="camera")
    handler._storage = SimpleNamespace(
        get_session=get_db_session,
        tier_available=lambda _: True,
        camera_tier_handlers={"camera": {"recorder": [{"segments": handler}]}},
    )
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._tier_id = 1
    handler._logger = logging.getLogger("test")
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_active = set()
    handler._source_intent_cursor = {}
    handler._journal_replay_after = None
    handler._schedule_journal_retry = Mock()
    handler._tier_check_stopping = False
    handler._hold_retry_stopping = False

    def forbidden_walk(*_args, **_kwargs):
        raise AssertionError("recovery traversed the recording archive")

    monkeypatch.setattr("os.walk", forbidden_walk)
    handler._replay_segment_moves()
    handler._replay_pending_publications()


@pytest.fixture
def promotion(tmp_path, get_db_session):
    """Real source row and filesystem, with only worker IPC captured."""

    row = source_row(tmp_path / "ram")
    src = Path(row.path)
    src.parent.mkdir(parents=True)
    src.write_bytes(b"video")
    (src.parent / "init.mp4").write_bytes(b"init")
    dst = tmp_path / "nas" / "camera" / src.parent.name / src.name
    with get_db_session() as session:
        session.add(row)
        session.commit()
    source = SimpleNamespace(
        _path=str(src.parent.parent), tier={"path": str(tmp_path / "ram")}
    )
    destination = SimpleNamespace(
        _path=str(dst.parent.parent),
        tier={"path": str(tmp_path / "nas")},
        _schedule_journal_retry=Mock(),
    )
    storage = SimpleNamespace(
        get_session=get_db_session,
        pressure_manager=None,
        pending_moves={},
        pending_moves_lock=threading.Lock(),
        temporary_files_meta={},
        tier_available=lambda _: True,
        tier_check_worker_send_command=Mock(),
        camera_tier_handlers={
            "camera": {"recorder": [{"segments": source}, {"segments": destination}]}
        },
    )
    return SimpleNamespace(
        src=src, dst=dst, storage=storage, row=row, logger=logging.getLogger("test")
    )


def start_promotion(promotion, get_session):
    """Invoke the normal parent-side promotion entry point."""

    move_file(
        Mock(),
        promotion.storage,
        get_session,
        "camera",
        0,
        "recorder",
        "segments",
        str(promotion.src),
        str(promotion.dst),
        promotion.storage.camera_tier_handlers["camera"]["recorder"][1][
            "segments"
        ].tier["path"],
        promotion.logger,
    )


def interrupt_before_marker(promotion, get_session):
    """Crash an active worker after durable reservation but before its marker."""
    start_promotion(promotion, get_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    with patch(
        "viseron.components.storage.check_tier.prepare_source_intent",
        side_effect=RuntimeError("crash before marker"),
    ):
        with pytest.raises(RuntimeError, match="crash before marker"):
            copy_file(
                get_session,
                item.src,
                item.dst,
                promotion.logger,
                camera_identifier="camera",
                publication_meta=item.publication_meta,
            )
    return item


def pressure_config(promotion):
    """Force an ordinary-footage budget below this fixture's five-byte source."""
    root = str(promotion.src.parents[2])
    return {
        "path": root,
        "probe": root,
        "policy": "oldest_first",
        "max_size": 1,
        "markers": {},
        "reclaim": True,
        "cameras": [
            {
                "identifier": "camera",
                "tier_id": 0,
                "segments": str(promotion.src.parent.parent),
                "spool": None,
                "lookback": 0,
            }
        ],
    }


def test_planned_work_remains_reclaimable_until_copy_starts(promotion, get_db_session):
    """A worker backlog must not pin all old footage in the RAM buffer."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
    result = reclaim(pressure_config(promotion), get_db_session)
    assert result["deleted"] == 1 and not promotion.src.exists()
    # A late queued worker revalidates its source and never starts a write.
    with pytest.raises(NoResultFound):
        copy_file(
            get_db_session,
            item.src,
            item.dst,
            promotion.logger,
            camera_identifier="camera",
            publication_meta=item.publication_meta,
        )
    assert not promotion.dst.exists()


def test_crash_before_copy_is_discoverable_without_markers(
    promotion, get_db_session, monkeypatch
):
    """An active worker commits before writing; lost work has exact DB paths."""

    interrupt_before_marker(promotion, get_db_session)
    assert promotion.storage.tier_check_worker_send_command.call_count == 1
    assert not marker_for_source(promotion.src).exists()
    with pytest.raises(BlockingIOError, match="publication hold"):
        delete_file(get_db_session, str(promotion.src), promotion.logger)
    assert reclaim(pressure_config(promotion), get_db_session)["deleted"] == 0
    # Simulate losing every callback and in-memory pending move.
    promotion.storage.pending_moves.clear()
    handler = object.__new__(TierHandler)
    handler._storage = promotion.storage
    handler._camera = SimpleNamespace(identifier="camera")
    handler._path = str(promotion.src.parent.parent)
    handler._tier_id = 0
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_active = set()
    handler._source_intent_cursor = {}
    handler._hold_retry_stopping = False
    handler._logger = Mock()
    handler._retry_held_move = Mock()
    handler._pending_segment_destination = Mock(
        return_value=SimpleNamespace(_schedule_journal_retry=Mock())
    )
    handler._tier_id = 0

    def forbidden(*_args, **_kwargs):
        raise AssertionError("archive enumeration")

    monkeypatch.setattr("os.walk", forbidden)
    handler._replay_segment_moves()
    handler._logger.exception.assert_not_called()
    marker, record = handler._retry_held_move.call_args.args
    assert marker == str(marker_for_source(promotion.src))
    assert record["source"] == str(promotion.src) and record["destination"] == str(
        promotion.dst
    )
    assert record["pending_move_id"] > 0


def test_database_failure_prevents_copy_writes(promotion, get_db_session):
    """A worker cannot start filesystem writes if its reservation commit fails."""

    @contextlib.contextmanager
    def unavailable():
        with get_db_session() as session:
            with patch.object(
                session,
                "commit",
                side_effect=SQLAlchemyError("injected commit failure"),
            ):
                yield session

    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    with pytest.raises(SQLAlchemyError, match="injected"):
        copy_file(
            unavailable,
            item.src,
            item.dst,
            promotion.logger,
            camera_identifier="camera",
            publication_meta=item.publication_meta,
        )
    assert promotion.src.read_bytes() == b"video" and not promotion.dst.exists()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None


@pytest.mark.parametrize("acknowledgement_crash", [False, True])
def test_copy_then_lost_callback_replays_and_releases(
    promotion, get_db_session, monkeypatch, acknowledgement_crash
):
    """Real copy/proof/index/release succeeds without observer or IPC delivery."""

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
    assert promotion.src.exists() and promotion.dst.exists()
    replay = object.__new__(TierHandler)
    replay._storage = promotion.storage
    replay._camera = SimpleNamespace(identifier="camera")
    replay._tier_id = 0
    replay._category = "recorder"
    replay._subcategory = "segments"
    replay._path = str(promotion.src.parent.parent)
    replay._source_intent_cursor = {}
    replay._hold_retry_stopping = False
    replay._hold_retry_lock = threading.Lock()
    replay._hold_retry_active = {str(promotion.src)}
    replay._logger = Mock()
    replay._retry_held_move = Mock(side_effect=AssertionError("copy must not repeat"))
    replay._replay_segment_moves()
    replay._logger.exception.assert_not_called()
    target = promotion.storage.camera_tier_handlers["camera"]["recorder"][1]["segments"]
    target._schedule_journal_retry.assert_called_once_with(str(promotion.dst))
    promotion.storage.pending_moves.clear()
    promotion.storage.temporary_files_meta.clear()
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="camera")
    handler._storage = promotion.storage
    handler._logger = promotion.logger
    handler._vis = Mock()
    handler.check_tier = Mock()
    handler._path = str(promotion.dst.parent.parent)
    handler._tier_id = 1
    handler._tier = {"path": str(promotion.dst.parent.parent.parent)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._event_queue = Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_stopping = False
    handler._journal_retry_paths = set()
    handler._journal_retry_timer = None
    handler._journal_retry_rescan = False
    handler._journal_reported_issues = set()
    handler._journal_replay_after = None

    def forbidden(*_args, **_kwargs):
        raise AssertionError("archive enumeration")

    monkeypatch.setattr("os.walk", forbidden)
    handler._replay_pending_publications()
    event = handler._event_queue.get_nowait()
    assert isinstance(event, FileCreatedEvent) and event.src_path == str(promotion.dst)
    if acknowledgement_crash:

        def fail_ack(*_args, **_kwargs):
            raise SQLAlchemyError("crash after source release")

        monkeypatch.setattr(
            "viseron.components.storage.tier_handler.acknowledge_segment_move", fail_ack
        )
        handler._schedule_journal_retry = Mock()
    handler._on_created(event)
    if acknowledgement_crash:
        # Empty-run cleanup may happen before the queued acknowledgement retries.
        (promotion.src.parent / "init.mp4").unlink()
        promotion.src.parent.rmdir()
        with get_db_session() as session:
            pending = session.execute(select(PendingSegmentMove)).scalar_one()
            assert finish_completed(session, pending, lambda _: True)
    assert not promotion.src.exists() and promotion.dst.read_bytes() == b"video"
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
        rows = session.execute(select(Files)).scalars().all()
        assert (
            len(rows) == 1
            and rows[0].path == str(promotion.dst)
            and rows[0].file_key == 42
        )


def test_retry_batches_rotate_past_blocked_jobs(tmp_path, get_db_session, monkeypatch):
    """A bounded first batch cannot starve later jobs, even with NAS offline."""
    with get_db_session() as session:
        for number in range(25):
            row = source_row(tmp_path / "ram")
            row.filename = f"{number}.m4s"
            row.path = str(Path(row.directory) / row.filename)
            session.add(row)
            session.flush()
            destination = str(
                tmp_path / "nas" / "camera" / Path(row.directory).name / row.filename
            )
            reserve(session, row, destination, 1, str(tmp_path / "nas"))
        session.commit()
    handler = object.__new__(TierHandler)
    handler._storage = SimpleNamespace(
        get_session=get_db_session, tier_available=lambda _: False
    )
    handler._camera = SimpleNamespace(identifier="camera")
    handler._path = str(tmp_path / "ram" / "camera")
    handler._hold_retry_lock = threading.Lock()
    handler._hold_retry_active = set()
    handler._source_intent_cursor = {}
    handler._hold_retry_stopping = False
    handler._logger = Mock()
    handler._retry_held_move = Mock()
    handler._pending_segment_destination = Mock(
        return_value=SimpleNamespace(_schedule_journal_retry=Mock())
    )
    handler._tier_id = 0

    def forbidden(*_args, **_kwargs):
        raise AssertionError("archive enumeration")

    monkeypatch.setattr("os.walk", forbidden)
    handler._replay_segment_moves()
    assert handler._retry_held_move.call_count == 20
    handler._replay_segment_moves()
    assert handler._retry_held_move.call_count == 25
    handler._logger.exception.assert_not_called()


def test_pending_queue_blocks_unsafe_schema_downgrade(promotion, get_db_session):
    """An older image must not silently discard durable unfinished work."""

    interrupt_before_marker(promotion, get_db_session)
    migration = importlib.import_module(
        "viseron.components.storage.alembic.versions.c73e8b2a9104_add_pending_segment_moves"
    )
    with get_db_session().get_bind().connect() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            with pytest.raises(RuntimeError, match="Complete pending segment moves"):
                migration.downgrade()
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).scalar_one() is not None


def test_incoming_publication_must_finish_before_next_promotion(
    promotion, get_db_session
):
    """A newly indexed first-tier fragment cannot move before its receipt clears."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    marker = promotion.src.parent / (
        ".viseron-tier-move-pending-" + promotion.src.name + ".json"
    )
    marker.write_text("{}")
    with pytest.raises(BlockingIOError, match="Source publication"):
        copy_file(
            get_db_session,
            item.src,
            item.dst,
            promotion.logger,
            camera_identifier="camera",
            publication_meta=item.publication_meta,
        )
    with get_db_session() as session:
        assert session.execute(select(PendingSegmentMove)).first() is None
    assert promotion.src.exists() and not promotion.dst.exists()


def test_legacy_import_after_source_row_is_already_gone(promotion, get_db_session):
    """Destination evidence must discover cleanup interrupted after source deletion."""
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    # Simulate the previous version, before active copies had a DB discovery row.
    metadata = {
        key: item.publication_meta[key]
        for key in ("orig_ctime", "duration", "file_key", "source_db")
    }
    copy_file(
        get_db_session,
        item.src,
        item.dst,
        promotion.logger,
        camera_identifier="camera",
        publication_meta=metadata,
    )
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == item.src)).scalar_one()
        session.delete(row)
        session.commit()
    promotion.src.unlink()
    with get_db_session() as session:
        assert (
            import_legacy(
                session,
                "camera",
                str(promotion.src.parent.parent),
                str(promotion.dst.parent.parent),
                1,
                str(promotion.dst.parents[2]),
            )
            == 1
        )
        assert (
            session.execute(select(PendingSegmentMove))
            .scalar_one()
            .metadata_record["source_db"]
            == metadata["source_db"]
        )
    assert marker_for_source(promotion.src).exists()
    assert promotion.dst.read_bytes() == b"video"
