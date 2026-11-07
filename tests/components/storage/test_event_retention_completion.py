"""Event retirement follows committed segment operations, not queue admission."""

import datetime
import logging
import os
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import numpy as np
import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import delete, func, select, update
from watchdog.events import FileDeletedEvent

from viseron.components.storage.check_tier import Worker
from viseron.components.storage.check_tier import delete_file as delete_segment
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.run_lifecycle import camera_lock, is_run_directory
from viseron.components.storage.storage_subprocess import (
    DataItem,
    StorageCallbackCapacityError,
)
from viseron.components.storage.tier_handler import SegmentsTierHandler, move_file


def _case(tmp_path, get_session, count=1):
    now = datetime.datetime.now(datetime.timezone.utc)
    run = (
        tmp_path
        if is_run_directory(tmp_path.name)
        else tmp_path / "20261102T000000.000000Z-012345abcdef"
    )
    run.mkdir(exist_ok=True)
    spool = run.parent / "staging"
    spool.mkdir(exist_ok=True)
    paths = [run / f"segment-{index}.m4s" for index in range(count)]
    for path in paths:
        path.write_bytes(b"only copy")
    with get_session() as session:
        recording = Recordings(
            camera_identifier="test",
            recorder_run_id=run.name,
            start_time=now,
            adjusted_start_time=now,
            end_time=now + datetime.timedelta(seconds=count),
        )
        rows = [
            Files(
                tier_id=0,
                tier_path=str(tmp_path),
                camera_identifier="test",
                category="recorder",
                subcategory="segments",
                path=str(path),
                directory=str(run),
                filename=path.name,
                size=9,
                orig_ctime=now + datetime.timedelta(seconds=index),
                duration=1,
            )
            for index, path in enumerate(paths)
        ]
        session.add_all([recording, *rows])
        session.commit()
        recording_id = recording.id
        data = np.array(
            [(recording_id, row.id, row.path, row.tier_path) for row in rows],
            dtype=[
                ("recording_id", np.int64),
                ("id", np.int64),
                ("path", "U512"),
                ("tier_path", "U512"),
            ],
        )
    storage = MagicMock()
    storage.get_session = get_session
    storage.tier_available.return_value = True
    storage.get_segments_path.return_value = [str(run.parent)]
    storage.tier_markers = {}
    storage.camera_requested_files_count = {"test": SimpleNamespace(filenames=set())}
    thumbnail, clip, segments = Mock(), Mock(), Mock()
    segments._check_tier_lock = threading.Lock()
    segments.tier = {"path": str(tmp_path)}
    storage.camera_tier_handlers = {
        "test": {
            "recorder": [
                {"segments": segments, "thumbnails": thumbnail, "event_clips": clip}
            ]
        }
    }
    storage.file_batch_size = 100
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    handler = SegmentsTierHandler.__new__(SegmentsTierHandler)
    handler._camera = SimpleNamespace(
        identifier="test",
        temp_segments_folder=str(spool),
        segments_folder=str(run.parent),
    )
    handler._storage = storage
    handler._vis = Mock()
    handler._tier_id = 0
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._tier = {"path": str(tmp_path)}
    handler._events_enabled = True
    handler._continuous_configured = False
    handler._logger = logging.getLogger(__name__)
    return SimpleNamespace(
        handler=handler,
        storage=storage,
        thumbnail=thumbnail,
        clip=clip,
        paths=paths,
        data=data,
        recording_id=recording_id,
    )


def _recording(get_session, recording_id):
    with get_session() as session:
        return session.execute(
            select(Recordings).where(Recordings.id == recording_id)
        ).scalar_one_or_none()


def _run_finalization(case):
    job = case.handler._vis.background_scheduler.add_job.call_args
    job.args[0](*job.kwargs["args"])


@pytest.mark.parametrize(
    "stage", ["queued", "capacity-rejected", "process-exit", "failed", "deferred"]
)
def test_unacknowledged_segment_cannot_retire_event(tmp_path, get_db_session, stage):
    """Dispatching, losing or rejecting deletion leaves events and assets available."""
    case = _case(tmp_path, get_db_session)
    if stage == "capacity-rejected":
        case.storage.tier_check_worker_send_command.side_effect = (
            StorageCallbackCapacityError("full")
        )
    if stage == "process-exit":
        pid = os.fork()
        if pid == 0:
            try:
                get_db_session.kw["bind"].dispose(close=False)
                case.handler._check_tier(get_db_session, case.data)
            except BaseException:
                os._exit(1)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
    else:
        case.handler._check_tier(get_db_session, case.data)
        if stage in ("failed", "deferred"):
            command = case.storage.tier_check_worker_send_command.call_args
            item = command.args[0]
            item.error = "storage unavailable" if stage == "failed" else None
            item.deferred = stage == "deferred"
            command.kwargs["callback"](item)
    assert _recording(get_db_session, case.recording_id) is not None
    assert all(path.exists() for path in case.paths)
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()
    case.handler._vis.background_scheduler.add_job.assert_not_called()


def test_successful_retention_waits_for_every_committed_delete(
    tmp_path, get_db_session
):
    """A completed batch retires normally, with duplicate replies coalesced."""
    case = _case(tmp_path, get_db_session, count=2)
    case.handler._check_tier(get_db_session, case.data)
    commands = case.storage.tier_check_worker_send_command.call_args_list
    assert len(commands) == 2
    for index, command in enumerate(commands):
        item = command.args[0]
        delete_segment(get_db_session, item.src, case.handler._logger)
        command.kwargs["callback"](item)
        command.kwargs["callback"](item)
        assert _recording(get_db_session, case.recording_id) is not None
        case.thumbnail.move_thumbnail.assert_not_called()
        case.clip.move_event_clip.assert_not_called()
        if index == 0:
            case.handler._vis.background_scheduler.add_job.assert_not_called()
    case.handler._vis.background_scheduler.add_job.assert_called_once()
    _run_finalization(case)
    assert _recording(get_db_session, case.recording_id) is None
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()


@pytest.mark.parametrize("active", [False, True])
def test_finalization_revalidates_source_rows_and_event_state(
    tmp_path, get_db_session, active
):
    """An early reply or a now-active event cannot authorize retirement."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    if active:
        delete_segment(get_db_session, command.args[0].src, case.handler._logger)
        with get_db_session() as session:
            session.execute(update(Recordings).values(end_time=None))
            session.commit()
    command.kwargs["callback"](command.args[0])
    _run_finalization(case)
    assert _recording(get_db_session, case.recording_id) is not None
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()


def test_move_acknowledgement_follows_database_handover(tmp_path, get_db_session):
    """A successful copy reply alone cannot finalize event assets."""
    case = _case(tmp_path, get_db_session)
    source = case.paths[0]
    destination = tmp_path / "next" / source.name
    destination.parent.mkdir()
    destination.write_bytes(source.read_bytes())
    case.storage.camera_tier_handlers["test"]["recorder"].append(
        {"segments": SimpleNamespace(tier={"path": str(destination.parent)})}
    )
    committed = Mock()
    move_file(
        case.handler._vis,
        case.storage,
        get_db_session,
        "test",
        0,
        "recorder",
        "segments",
        str(source),
        str(destination),
        str(destination.parent),
        case.handler._logger,
        on_complete=committed,
    )
    command = case.storage.tier_check_worker_send_command.call_args
    command.kwargs["callback"](command.args[0])
    committed.assert_not_called()
    source.unlink()

    def acknowledged():
        with get_db_session() as session:
            assert session.execute(select(Files.path)).scalar_one() == str(destination)
        committed()

    case.storage.pending_moves[str(source)].on_complete = acknowledged
    case.handler._on_deleted_impl(FileDeletedEvent(str(source)))
    committed.assert_called_once()


@pytest.mark.parametrize("stopping", [False, True])
def test_shutdown_or_scheduler_failure_keeps_event(tmp_path, get_db_session, stopping):
    """Losing the finalization scheduler never turns a reply into event deletion."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    case.handler._tier_check_stopping = stopping
    case.handler._vis.background_scheduler.add_job.side_effect = RuntimeError("stopped")
    command.kwargs["callback"](command.args[0])
    assert _recording(get_db_session, case.recording_id) is not None
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()


@pytest.mark.parametrize("pre_roll", [False, True])
def test_late_indexed_event_segment_prevents_finalization(
    tmp_path, get_db_session, pre_roll
):
    """Commit revalidation includes source fragments absent from the selection."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    event = _recording(get_db_session, case.recording_id)
    late = case.paths[0].parent / "late.m4s"
    late.write_bytes(b"late footage")
    with get_db_session() as session:
        session.add(
            Files(
                tier_id=0,
                tier_path=str(tmp_path),
                camera_identifier="test",
                category="recorder",
                subcategory="segments",
                path=str(late),
                directory=str(late.parent),
                filename=late.name,
                size=12,
                orig_ctime=event.adjusted_start_time
                + datetime.timedelta(seconds=-0.5 if pre_roll else 0.25),
                duration=1,
            )
        )
        session.commit()
    _run_finalization(case)
    assert _recording(get_db_session, case.recording_id) is not None
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()
    assert late.read_bytes() == b"late footage"


@pytest.mark.parametrize("arrival", ["fragment", "reopened-event"])
def test_terminal_retirement_rechecks_at_delete_without_retiring_assets(
    tmp_path, get_db_session, arrival
):
    """A change after eligibility selection cannot retire metadata or assets."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    recording = _recording(get_db_session, case.recording_id)
    injected = False

    def arrive(_connection, _cursor, statement, _parameters, _context, _many):
        nonlocal injected
        if injected or not statement.startswith("DELETE FROM recordings"):
            return
        injected = True
        with get_db_session() as session:
            if arrival == "reopened-event":
                session.execute(update(Recordings).values(end_time=None))
            else:
                late = case.paths[0].parent / "arriving.m4s"
                late.write_bytes(b"late video")
                session.add(
                    Files(
                        tier_id=0,
                        tier_path=str(tmp_path),
                        camera_identifier="test",
                        category="recorder",
                        subcategory="segments",
                        path=str(late),
                        directory=str(late.parent),
                        filename=late.name,
                        size=10,
                        orig_ctime=recording.adjusted_start_time,
                        duration=1,
                    )
                )
            session.commit()

    engine = get_db_session.kw["bind"]
    sqlalchemy_event.listen(engine, "before_cursor_execute", arrive)
    try:
        _run_finalization(case)
    finally:
        sqlalchemy_event.remove(engine, "before_cursor_execute", arrive)
    assert injected
    assert _recording(get_db_session, case.recording_id) is not None
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()


def test_terminal_assets_are_deleted_only_after_retirement_commits(
    tmp_path, get_db_session
):
    """Real asset deletion observes the committed retirement, not a selected ID."""
    case = _case(tmp_path, get_db_session)
    assets = [tmp_path / "event.jpg", tmp_path / "event.mp4"]
    for path in assets:
        path.write_bytes(b"asset")
    with get_db_session() as session:
        recording = session.get(Recordings, case.recording_id)
        recording.thumbnail_path, recording.clip_path = map(str, assets)
        for kind, path in zip(("thumbnails", "event_clips"), assets, strict=True):
            session.add(
                Files(
                    tier_id=0,
                    tier_path=str(tmp_path),
                    camera_identifier="test",
                    category="recorder",
                    subcategory=kind,
                    path=str(path),
                    directory=str(tmp_path),
                    filename=path.name,
                    size=5,
                    orig_ctime=recording.start_time,
                )
            )
        session.commit()
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    removed = []

    def remove_asset(item, **_kwargs):
        assert _recording(get_db_session, case.recording_id) is None
        assert os.path.exists(item.src)
        delete_segment(get_db_session, item.src, case.handler._logger)
        removed.append(item.src)

    case.storage.tier_check_worker_send_command.side_effect = remove_asset
    _run_finalization(case)
    assert sorted(removed) == sorted(map(str, assets))
    assert not any(path.exists() for path in assets)


def test_rejected_terminal_asset_cleanup_leaves_files_for_orphan_recovery(
    tmp_path, get_db_session
):
    """Queue pressure after retirement cannot lose the retained asset file."""
    case = _case(tmp_path, get_db_session)
    path = tmp_path / "event.jpg"
    path.write_bytes(b"asset")
    with get_db_session() as session:
        recording = session.get(Recordings, case.recording_id)
        recording.thumbnail_path = str(path)
        session.add(
            Files(
                tier_id=0,
                tier_path=str(tmp_path),
                camera_identifier="test",
                category="recorder",
                subcategory="thumbnails",
                path=str(path),
                directory=str(tmp_path),
                filename=path.name,
                size=5,
                orig_ctime=recording.start_time,
            )
        )
        session.commit()
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    case.storage.tier_check_worker_send_command.side_effect = (
        StorageCallbackCapacityError("full")
    )
    _run_finalization(case)
    assert _recording(get_db_session, case.recording_id) is None
    assert path.read_bytes() == b"asset"
    with get_db_session() as session:
        assert session.execute(select(Files.path)).scalar_one() == str(path)


@pytest.mark.parametrize("late_event", ["active", "ended", "pre-roll"])
@pytest.mark.parametrize("run_scoped", [False, True])
def test_queued_segment_delete_preserves_a_new_overlapping_event(
    tmp_path, get_db_session, late_event, run_scoped
):
    """An event created after selection must keep its only segment and metadata."""
    run_id = "20261102T000000.000000Z-012345abcdef" if run_scoped else None
    root = tmp_path / run_id if run_id else tmp_path
    root.mkdir(exist_ok=True)
    case = _case(root, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    item = command.args[0]
    with get_db_session() as session:
        fragment = session.execute(select(Files)).scalar_one()
        start = fragment.orig_ctime + datetime.timedelta(
            seconds=3 if late_event == "pre-roll" else 0
        )
        new_event = Recordings(
            camera_identifier="test",
            recorder_run_id=run_id,
            start_time=start,
            adjusted_start_time=fragment.orig_ctime,
            end_time=(
                None
                if late_event == "active"
                else start + datetime.timedelta(seconds=5)
            ),
        )
        session.add(new_event)
        session.commit()
        new_id = new_event.id
    worker = Worker.__new__(Worker)
    worker._get_session = get_db_session
    worker.work_input(item)
    command.kwargs["callback"](item)
    assert case.paths[0].exists(), "Queued cleanup deleted a new event's only footage"
    with get_db_session() as session:
        assert session.execute(select(Files.path)).scalar_one() == str(case.paths[0])
    assert _recording(get_db_session, new_id) is not None
    assert item.deferred
    case.handler._vis.background_scheduler.add_job.assert_not_called()


def _selected_expired_case(tmp_path, get_session, count=1):
    case = _case(tmp_path, get_session)
    old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    with get_session() as session:
        session.execute(update(Files).values(orig_ctime=old))
        session.execute(
            update(Recordings).values(
                start_time=old,
                adjusted_start_time=old,
                end_time=old + datetime.timedelta(seconds=2),
                created_at=old - datetime.timedelta(days=2),
            )
        )
        for _ in range(count - 1):
            session.add(
                Recordings(
                    camera_identifier="test",
                    start_time=old,
                    adjusted_start_time=old,
                    end_time=old + datetime.timedelta(seconds=2),
                    created_at=old - datetime.timedelta(days=2),
                )
            )
        session.commit()
    item = DataItem(
        cmd="check_tier",
        camera_identifier="test",
        tier_id=0,
        category="recorder",
        subcategories=["segments"],
        throttle_period=datetime.timedelta(0),
        max_bytes=0,
        min_bytes=0,
        min_age=datetime.timedelta(0),
        max_age=datetime.timedelta(0),
        drain=False,
        files_enabled=False,
        events_enabled=True,
        events_max_bytes=0,
        events_min_bytes=0,
        events_min_age=datetime.timedelta(0),
        events_max_age=datetime.timedelta(days=1),
    )
    worker = Worker.__new__(Worker)
    worker._get_session = get_session
    worker._check_tier(item)
    assert len(item.data) == 1
    return case, worker, item, old


def test_expired_overlapping_events_still_allow_normal_retention(
    tmp_path, get_db_session
):
    """File deduplication must not lose authorization for another expired event."""
    case, worker, selected, _ = _selected_expired_case(
        tmp_path, get_db_session, count=2
    )
    assert len(selected.retention_recording_ids) == 2
    case.handler._check_tier(
        get_db_session,
        selected.data,
        retention_recording_ids=selected.retention_recording_ids,
    )
    command = case.storage.tier_check_worker_send_command.call_args
    item = command.args[0]
    worker.work_input(item)
    assert item.error is None
    assert not item.deferred
    assert not case.paths[0].exists()


def test_event_created_before_dispatch_cannot_join_old_authorization(
    tmp_path, get_db_session
):
    """The parent's queueing step must use the worker's original eligibility set."""
    case, worker, selected, old = _selected_expired_case(tmp_path, get_db_session)
    with get_db_session() as session:
        session.add(
            Recordings(
                camera_identifier="test",
                start_time=old,
                adjusted_start_time=old,
                end_time=old + datetime.timedelta(seconds=5),
            )
        )
        session.commit()
    case.handler._check_tier(
        get_db_session,
        selected.data,
        retention_recording_ids=selected.retention_recording_ids,
    )
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    worker.work_input(item)
    assert item.deferred
    assert case.paths[0].exists()


def test_reopened_selected_event_keeps_its_segment(tmp_path, get_db_session):
    """An originally authorized ID is no longer eligible after reopening."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    with get_db_session() as session:
        session.execute(update(Recordings).values(end_time=None))
        session.commit()
    worker = Worker.__new__(Worker)
    worker._get_session = get_db_session
    worker.work_input(item)
    assert item.deferred
    assert case.paths[0].exists()


def test_event_creation_is_fenced_through_segment_unlink(
    tmp_path, get_db_session, monkeypatch
):
    """A creator cannot commit between the final event check and physical deletion."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    remove = os.remove
    checked = []

    def check_fence(path):
        with get_db_session() as session:
            obtained = session.execute(
                select(
                    func.pg_try_advisory_xact_lock(
                        func.hashtext("recording-events:test")
                    )
                )
            ).scalar_one()
            assert obtained is False
        checked.append(path)
        remove(path)

    monkeypatch.setattr("viseron.components.storage.check_tier.os.remove", check_fence)
    worker = Worker.__new__(Worker)
    worker._get_session = get_db_session
    worker.work_input(item)
    assert item.error is None
    assert checked == [str(case.paths[0])]
    with get_db_session() as session:
        assert (
            session.execute(
                select(
                    func.pg_try_advisory_xact_lock(
                        func.hashtext("recording-events:test")
                    )
                )
            ).scalar_one()
            is True
        )


@pytest.mark.parametrize("new_event", [False, True])
def test_continuous_only_retention_rechecks_new_events(
    tmp_path, get_db_session, new_event
):
    """Continuous expiry allows old closed events but not a new event claim."""
    case, worker, selected, old = _selected_expired_case(tmp_path, get_db_session)
    selected.files_enabled = True
    selected.events_enabled = False
    selected.max_age = datetime.timedelta(minutes=1)
    worker._check_tier(selected)
    assert len(selected.data) == 1
    assert selected.retention_recording_ids == (case.recording_id,)
    if new_event:
        with get_db_session() as session:
            session.add(
                Recordings(
                    camera_identifier="test",
                    start_time=old,
                    adjusted_start_time=old,
                    end_time=None,
                )
            )
            session.commit()
    case.handler._events_enabled = False
    case.handler._continuous_configured = True
    case.handler._check_tier(
        get_db_session,
        selected.data,
        retention_recording_ids=selected.retention_recording_ids,
    )
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    worker.work_input(item)
    assert item.error is None
    assert item.deferred == new_event
    assert case.paths[0].exists() == new_event


def test_event_on_another_run_does_not_block_old_run_retention(
    tmp_path, get_db_session
):
    """A new run's event cannot claim an older run even at the same clock time."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    with get_db_session() as session:
        row = session.execute(select(Files)).scalar_one()
        session.add(
            Recordings(
                camera_identifier="test",
                recorder_run_id="20261102T000001.000000Z-abcdef123456",
                start_time=row.orig_ctime,
                adjusted_start_time=row.orig_ctime,
                end_time=None,
            )
        )
        session.commit()
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    worker = Worker.__new__(Worker)
    worker._get_session = get_db_session
    worker.work_input(item)
    assert item.error is None
    assert not item.deferred
    assert not case.paths[0].exists()


def test_unassociated_footage_claimed_after_selection_is_kept(tmp_path, get_db_session):
    """An events-only tier cannot purge ordinary footage newly claimed by pre-roll."""
    case, worker, selected, old = _selected_expired_case(tmp_path, get_db_session)
    with get_db_session() as session:
        session.execute(delete(Recordings))
        session.commit()
    worker._check_tier(selected)
    assert selected.data[0]["recording_id"] == -1
    assert selected.retention_recording_ids == ()
    with get_db_session() as session:
        session.add(
            Recordings(
                camera_identifier="test",
                start_time=old,
                adjusted_start_time=old,
                end_time=None,
            )
        )
        session.commit()
    case.handler._check_tier(
        get_db_session,
        selected.data,
        retention_recording_ids=selected.retention_recording_ids,
    )
    item = case.storage.tier_check_worker_send_command.call_args.args[0]
    worker.work_input(item)
    assert item.error is None
    assert item.deferred
    assert case.paths[0].exists()


@pytest.mark.parametrize(
    "location",
    [
        "tier",
        "spool",
        "pending",
        "writer",
        "legacy",
        "missing-root",
        "offline",
        "unknown",
        "symlink",
        "publication",
        "empty",
    ],
)
def test_terminal_retirement_preserves_unindexed_run_state(
    tmp_path, get_db_session, location
):
    """A completed selected batch cannot erase recovery's event association."""
    run_id = "20261102T000000.000000Z-abcdef123456"
    root = tmp_path / "segments" / "test"
    spool = tmp_path / "spool" / "test"
    run = root / run_id
    run.mkdir(parents=True)
    spool.mkdir(parents=True)
    case = _case(run, get_db_session)
    case.handler._camera = SimpleNamespace(
        identifier="test",
        temp_segments_folder=str(spool),
        segments_folder=str(root),
    )
    case.storage.get_segments_path.return_value = [str(root)]
    case.storage.tier_markers = {}
    with get_db_session() as session:
        session.get(Recordings, case.recording_id).recorder_run_id = (
            None if location == "legacy" else run_id
        )
        session.commit()
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    directory = spool / run_id if location in ("spool", "writer") else run
    directory.mkdir(exist_ok=True)
    filename = {
        "tier": "unindexed.m4s",
        "spool": "staged.m4s",
        "pending": ".viseron-tier-move-pending-late.m4s.json",
        "writer": ".viseron-writer-lease",
    }.get(location, "unknown")
    retained = directory / filename
    if location == "missing-root":
        spool.rmdir()
    elif location == "offline":
        case.storage.tier_markers = {str(tmp_path): ".missing-mount-marker"}
    elif location == "symlink":
        retained.symlink_to(tmp_path / "missing")
    elif location not in ("empty", "publication", "legacy"):
        retained.write_bytes(b"recovery state")
    if location == "publication":
        with camera_lock("/tmp/viseron-run-locks", str(root)):
            _run_finalization(case)
    else:
        _run_finalization(case)
    assert (_recording(get_db_session, case.recording_id) is None) == (
        location == "empty"
    )
    if location in ("tier", "spool", "pending", "writer", "unknown"):
        assert retained.read_bytes() == b"recovery state"
    case.thumbnail.move_thumbnail.assert_not_called()
    case.clip.move_event_clip.assert_not_called()


def test_terminal_retirement_holds_publication_locks_through_commit(
    tmp_path, get_db_session
):
    """A cooperating indexer cannot commit between disk proof and retirement."""
    case = _case(tmp_path, get_db_session)
    case.handler._check_tier(get_db_session, case.data)
    command = case.storage.tier_check_worker_send_command.call_args
    delete_segment(get_db_session, command.args[0].src, case.handler._logger)
    command.kwargs["callback"](command.args[0])
    checked = []

    def assert_fenced():
        for root in case.storage.get_segments_path.return_value + [
            case.handler._camera.temp_segments_folder
        ]:
            with (
                pytest.raises(BlockingIOError),
                camera_lock("/tmp/viseron-run-locks", root),
            ):
                pytest.fail("Retirement released the publication fence")

    def deleting(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("DELETE FROM recordings"):
            assert_fenced()
            checked.append("delete")

    def committing(_connection):
        assert_fenced()
        checked.append("commit")

    engine = get_db_session.kw["bind"]
    sqlalchemy_event.listen(engine, "before_cursor_execute", deleting)
    sqlalchemy_event.listen(engine, "commit", committing)
    try:
        _run_finalization(case)
    finally:
        sqlalchemy_event.remove(engine, "before_cursor_execute", deleting)
        sqlalchemy_event.remove(engine, "commit", committing)
    assert checked == ["delete", "commit"]
    assert _recording(get_db_session, case.recording_id) is None
    with camera_lock(
        "/tmp/viseron-run-locks", case.handler._camera.temp_segments_folder
    ):
        pass
