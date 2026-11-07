"""Asset moves keep recording links usable without watcher delivery."""

import logging
import os
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import scoped_session, sessionmaker
from watchdog.events import FileCreatedEvent, FileDeletedEvent

from viseron.components.storage import Storage, asset_moves
from viseron.components.storage.asset_moves import reserve
from viseron.components.storage.check_tier import delete_file, move_file
from viseron.components.storage.jobs import (
    OrphanedDatabaseFilesCleanup,
    OrphanedEventClipsCleanup,
    OrphanedFilesCleanup,
    OrphanedThumbnailsCleanup,
)
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.storage_subprocess import StorageCallbackCapacityError
from viseron.components.storage.tier_handler import (
    EventClipTierHandler,
    ThumbnailTierHandler,
    TierHandler,
    move_file as dispatch_move,
)


@pytest.fixture(params=[("thumbnails", "thumbnail_path"), ("event_clips", "clip_path")])
def case(request, tmp_path, get_db_session):
    """Create a recorded asset and a durable move reservation."""
    kind, field = request.param
    source = tmp_path / "tier0" / kind / "camera" / "1.jpg"
    destination = tmp_path / "tier1" / kind / "camera" / "1.jpg"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"asset contents")
    now = datetime.now(UTC)
    with get_db_session() as session:
        event = Recordings(
            camera_identifier="camera",
            start_time=now,
            adjusted_start_time=now,
            end_time=now + timedelta(seconds=1),
            **{field: str(source)},
        )
        row = Files(
            tier_id=0,
            tier_path=str(tmp_path / "tier0"),
            camera_identifier="camera",
            category="recorder",
            subcategory=kind,
            path=str(source),
            directory=str(source.parent),
            filename=source.name,
            size=14,
            orig_ctime=now,
        )
        session.add_all([event, row])
        session.commit()
        target = reserve(row, str(destination), 1, str(tmp_path / "tier1"))
        session.commit()
        result = SimpleNamespace(
            kind=kind,
            field=field,
            source=source,
            destination=destination,
            event_id=event.id,
            file_key=row.file_key,
            file_id=row.id,
            get_session=get_db_session,
            logger=logging.getLogger(__name__),
            meta={
                "orig_ctime": now.isoformat(),
                "duration": None,
                "file_key": row.file_key,
                "source_db": {"id": row.id, "tier_id": 0, "tier_path": row.tier_path},
                "asset_target": target,
            },
        )
    return result


def _worker(case):
    move_file(
        case.get_session,
        str(case.source),
        str(case.destination),
        case.logger,
        camera_identifier="camera",
        publication_meta=case.meta,
    )


def _assert_link(case, destination=False):
    path = case.destination if destination else case.source
    with case.get_session() as session:
        assert getattr(session.get(Recordings, case.event_id), case.field) == str(path)
        row = session.execute(select(Files)).scalar_one()
        assert row.path == str(path)
        assert row.file_key == case.file_key
        assert (row.move_target is None) == destination
    assert path.read_bytes() == b"asset contents"


def _handler(case, destination=False):
    cls = ThumbnailTierHandler if case.kind == "thumbnails" else EventClipTierHandler
    handler = cls.__new__(cls)
    handler._camera = SimpleNamespace(identifier="camera")
    handler._category = "recorder"
    handler._subcategory = case.kind
    handler._tier_id = int(destination)
    handler._tier = {
        "path": str((case.destination if destination else case.source).parents[2])
    }
    handler._logger = case.logger
    handler._vis = Mock()
    handler._tier_check_stopping = False
    handler._asset_move_cursor = 0
    storage = Mock()
    storage.get_session = case.get_session
    storage.tier_available.return_value = True
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    storage.temporary_files_meta = {}
    storage.camera_tier_handlers = {
        "camera": {
            "recorder": [
                {
                    case.kind: SimpleNamespace(
                        tier={"path": str(case.source.parents[2])}
                    )
                },
                {
                    case.kind: SimpleNamespace(
                        tier={"path": str(case.destination.parents[2])}
                    )
                },
            ]
        }
    }
    handler._storage = storage
    return handler


def test_worker_move_commits_recording_link_without_watchers(case):
    """The worker leaves a usable link even when every watcher event is lost."""
    _worker(case)
    _assert_link(case, destination=True)
    assert not case.source.exists()


@pytest.mark.parametrize("boundary", ["before-commit", "after-commit"])
def test_process_exit_preserves_a_usable_link_and_replays(case, boundary):
    """Restart on either side of the transaction without losing the asset."""
    pid = os.fork()
    if pid == 0:
        try:
            case.get_session.kw["bind"].dispose(close=False)
            real_commit = asset_moves._commit

            def interrupted(*args):
                if boundary == "after-commit":
                    real_commit(*args)
                os._exit(0)

            with patch.object(asset_moves, "_commit", side_effect=interrupted):
                _worker(case)
        except BaseException:
            os._exit(1)
        os._exit(2)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    _assert_link(case, destination=boundary == "after-commit")
    if boundary == "before-commit":
        # A fresh handler has no parent reservation or watcher history.
        handler = _handler(case)
        handler._replay_asset_moves()
        command = handler._storage.tier_check_worker_send_command.call_args
        item = command.args[0]
        move_file(
            case.get_session,
            item.src,
            item.dst,
            case.logger,
            camera_identifier=item.camera_identifier,
            publication_meta=item.publication_meta,
        )
        command.kwargs["callback"](item)
        assert handler._storage.pending_moves == {}
        assert handler._storage.temporary_files_meta == {}
        _assert_link(case, destination=True)
    else:
        runner = SimpleNamespace(
            name="cleanup",
            get_session=case.get_session,
            kill_event=threading.Event(),
            log_progress=Mock(),
            paths=[str(case.source.parent)],
            ignored_files=frozenset(),
            tier_markers={},
        )
        with patch("viseron.components.storage.jobs.time.sleep"):
            OrphanedFilesCleanup.work(runner)
        assert not case.source.exists()
        _assert_link(case, destination=True)


def test_failed_database_commit_keeps_source_and_retries(case):
    """A failed handoff retains its reservation and the usable source."""
    with patch.object(
        asset_moves, "_commit", side_effect=OSError("database unavailable")
    ):
        with pytest.raises(OSError, match="database unavailable"):
            _worker(case)
    _assert_link(case)
    _worker(case)
    _assert_link(case, destination=True)


@pytest.mark.parametrize("conflict", ["source", "destination", "database"])
def test_conflicts_never_destroy_the_source(case, conflict):
    """Conflicting bytes or database identities cannot authorize source deletion."""
    if conflict == "source":
        case.source.write_bytes(b"changed asset!")
    else:
        case.destination.parent.mkdir(parents=True)
        case.destination.write_bytes(
            b"other content" if conflict == "destination" else b"asset contents"
        )
        if conflict == "database":
            with case.get_session() as session:
                source = session.get(Files, case.file_id)
                session.add(
                    Files(
                        tier_id=1,
                        tier_path=str(case.destination.parents[2]),
                        camera_identifier="other-camera",
                        category="recorder",
                        subcategory=case.kind,
                        path=str(case.destination),
                        directory=str(case.destination.parent),
                        filename=case.destination.name,
                        size=14,
                        orig_ctime=source.orig_ctime,
                    )
                )
                session.commit()
    with pytest.raises(OSError):
        _worker(case)
    assert case.source.exists()
    with case.get_session() as session:
        assert getattr(session.get(Recordings, case.event_id), case.field) == str(
            case.source
        )
        assert session.get(Files, case.file_id).move_target is not None
    if conflict == "destination":
        assert case.destination.read_bytes() == b"other content"


def test_queue_rejection_persists_destination_for_restart(case):
    """Queue admission failure does not lose the intended asset destination."""
    with case.get_session() as session:
        session.get(Files, case.file_id).move_target = None
        session.commit()
    handler = _handler(case)
    handler._storage.tier_check_worker_send_command.side_effect = (
        StorageCallbackCapacityError("full")
    )
    with pytest.raises(StorageCallbackCapacityError):
        dispatch_move(
            handler._vis,
            handler._storage,
            case.get_session,
            "camera",
            0,
            "recorder",
            case.kind,
            str(case.source),
            str(case.destination),
            str(case.destination.parents[2]),
            case.logger,
        )
    _assert_link(case)
    assert handler._storage.pending_moves == {}
    fresh = _handler(case)
    fresh._replay_asset_moves()
    assert fresh._storage.tier_check_worker_send_command.call_count == 1


def test_cleanup_and_watchers_preserve_pending_copies(case, monkeypatch):
    """Cleanup and stale watcher delivery cannot undo the handoff."""
    case.destination.parent.mkdir(parents=True)
    case.destination.write_bytes(b"asset contents")
    handler = _handler(case, destination=True)
    with patch.object(TierHandler, "_on_created") as created:
        handler._on_created(FileCreatedEvent(str(case.destination)))
        created.assert_not_called()
    runner = SimpleNamespace(
        name="cleanup",
        get_session=case.get_session,
        kill_event=threading.Event(),
        log_progress=Mock(),
        paths=[str(case.source.parent), str(case.destination.parent)],
        ignored_files=frozenset(),
        tier_markers={},
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    OrphanedFilesCleanup.work(runner)
    cleanup = (
        OrphanedThumbnailsCleanup
        if case.kind == "thumbnails"
        else OrphanedEventClipsCleanup
    )
    cleanup.work(runner)
    assert case.source.exists() and case.destination.exists()
    with pytest.raises(BlockingIOError):
        delete_file(case.get_session, str(case.source), case.logger)
    with pytest.raises(BlockingIOError):
        delete_file(case.get_session, str(case.destination), case.logger)
    _handler(case)._on_deleted(FileDeletedEvent(str(case.source)))
    _assert_link(case)
    _worker(case)
    # A delayed event for the old location cannot undo the committed link.
    with patch.object(TierHandler, "_on_created"):
        _handler(case)._on_created(FileCreatedEvent(str(case.source)))
    _assert_link(case, destination=True)


def test_missing_source_does_not_discard_pending_metadata(case, monkeypatch):
    """Missing media is not evidence that a reserved move can be discarded."""
    case.source.unlink()
    runner = SimpleNamespace(
        name="cleanup",
        get_session=case.get_session,
        kill_event=threading.Event(),
        log_progress=Mock(),
        tier_markers={},
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    OrphanedDatabaseFilesCleanup.work(runner)
    with case.get_session() as session:
        assert session.get(Files, case.file_id).move_target is not None


def test_old_tier_cannot_move_an_asset_again_after_handoff(case):
    """Duplicate finalization from the old tier cannot retire the new asset."""
    _worker(case)
    handler = _handler(case)
    if case.kind == "thumbnails":
        handler.move_thumbnail(
            case.event_id, {"path": str(case.destination.parents[2])}
        )
    else:
        handler.move_event_clip(
            case.event_id, {"path": str(case.destination.parents[2])}
        )
    handler._storage.tier_check_worker_send_command.assert_not_called()
    _assert_link(case, destination=True)


def test_abandoned_private_copies_are_reclaimed_after_grace(case, monkeypatch):
    """Hard-exit temporary files expire without touching the pending source."""
    case.destination.parent.mkdir(parents=True)
    stale = case.destination.parent / ".viseron-tier-move-abcd1234"
    young = case.destination.parent / ".viseron-tier-move-abcd5678"
    stale.write_bytes(b"interrupted copy")
    young.write_bytes(b"recent copy")
    os.utime(stale, (1, 1))
    runner = SimpleNamespace(
        name="cleanup",
        get_session=case.get_session,
        kill_event=threading.Event(),
        log_progress=Mock(),
        paths=[str(case.destination.parent)],
        ignored_files=frozenset(),
        tier_markers={},
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    OrphanedFilesCleanup.work(runner)
    assert not stale.exists()
    assert young.exists()
    _assert_link(case)


def test_replay_batch_survives_scoped_session_move_commits(case):
    """Both reserved assets recover when each dispatch closes the shared session."""
    second_source = case.source.with_name("2.jpg")
    second_destination = case.destination.with_name("2.jpg")
    second_source.write_bytes(b"asset contents")
    now = datetime.now(UTC)
    with case.get_session() as session:
        recording = Recordings(
            camera_identifier="camera",
            start_time=now,
            adjusted_start_time=now,
            end_time=now + timedelta(seconds=1),
            **{case.field: str(second_source)},
        )
        row = Files(
            tier_id=0,
            tier_path=str(second_source.parents[2]),
            camera_identifier="camera",
            category="recorder",
            subcategory=case.kind,
            path=str(second_source),
            directory=str(second_source.parent),
            filename=second_source.name,
            size=14,
            orig_ctime=now,
        )
        session.add_all([recording, row])
        session.flush()
        reserve(row, str(second_destination), 1, str(second_destination.parents[2]))
        session.commit()
        second_event_id = recording.id
        second_file_key = row.file_key

    # Use the real Storage accessor and its thread-scoped, expiring session
    # factories. The ordinary test factory does not expire objects on commit.
    storage = Storage.__new__(Storage)
    storage._get_session = scoped_session(
        sessionmaker(bind=case.get_session.kw["bind"])
    )
    storage._get_session_expire = scoped_session(
        sessionmaker(bind=case.get_session.kw["bind"], expire_on_commit=True)
    )
    handler = _handler(case)
    handler._storage.get_session = storage.get_session
    try:
        handler._replay_asset_moves()
        commands = handler._storage.tier_check_worker_send_command.call_args_list
        assert [command.args[0].src for command in commands] == [
            str(case.source),
            str(second_source),
        ]
        for command in commands:
            item = command.args[0]
            move_file(
                storage.get_session,
                item.src,
                item.dst,
                case.logger,
                camera_identifier=item.camera_identifier,
                publication_meta=item.publication_meta,
            )
            command.kwargs["callback"](item)
        assert handler._storage.pending_moves == {}
        assert handler._storage.temporary_files_meta == {}
        for event_id, destination, file_key in (
            (case.event_id, case.destination, case.file_key),
            (second_event_id, second_destination, second_file_key),
        ):
            with storage.get_session() as session:
                assert getattr(session.get(Recordings, event_id), case.field) == str(
                    destination
                )
                moved = session.execute(
                    select(Files).where(Files.path == str(destination))
                ).scalar_one()
                assert moved.file_key == file_key
                assert moved.move_target is None
            assert destination.read_bytes() == b"asset contents"
        assert not case.source.exists()
        assert not second_source.exists()
        handler._replay_asset_moves()
        assert handler._asset_move_cursor == 0
    finally:
        storage._get_session.remove()
        storage._get_session_expire.remove()


@pytest.mark.parametrize("registration", ["source_constructing", "destination_missing"])
def test_replay_waits_for_asset_handler_registration(case, registration, caplog):
    """An early recovery pass preserves the move and succeeds after registration."""
    handler = _handler(case)
    tiers = handler._storage.camera_tier_handlers["camera"]["recorder"]
    complete = [dict(tier) for tier in tiers]
    if registration == "source_constructing":
        tiers[:] = [{}, {}]
    else:
        tiers[1].pop(case.kind)
    with caplog.at_level(logging.WARNING, logger=case.logger.name):
        handler._replay_asset_moves()
    handler._storage.tier_check_worker_send_command.assert_not_called()
    assert handler._storage.pending_moves == {}
    assert handler._storage.temporary_files_meta == {}
    _assert_link(case)
    assert not caplog.records

    tiers[:] = complete
    # The rotating cursor wraps on the next empty pass, then retries the
    # still-reserved asset without needing a new handler or a watcher event.
    handler._replay_asset_moves()
    handler._replay_asset_moves()
    command = handler._storage.tier_check_worker_send_command.call_args
    item = command.args[0]
    move_file(
        case.get_session,
        item.src,
        item.dst,
        case.logger,
        camera_identifier=item.camera_identifier,
        publication_meta=item.publication_meta,
    )
    command.kwargs["callback"](item)
    _assert_link(case, destination=True)
    assert not case.source.exists()


def test_unconfigured_asset_destination_is_still_rejected(case):
    """Complete registration cannot authorize a reservation for another root."""
    handler = _handler(case)
    destination = handler._storage.camera_tier_handlers["camera"]["recorder"][1]
    destination[case.kind].tier["path"] = "/different-tier"
    with pytest.raises(OSError, match="no longer configured"):
        dispatch_move(
            handler._vis,
            handler._storage,
            case.get_session,
            "camera",
            0,
            "recorder",
            case.kind,
            str(case.source),
            str(case.destination),
            str(case.destination.parents[2]),
            case.logger,
        )
    handler._storage.tier_check_worker_send_command.assert_not_called()
    _assert_link(case)
