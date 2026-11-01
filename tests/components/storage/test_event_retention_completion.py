"""Event retirement follows committed segment operations, not queue admission."""

import datetime
import logging
import os
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import numpy as np
import pytest
from sqlalchemy import select, update
from watchdog.events import FileDeletedEvent

from viseron.components.storage.check_tier import delete_file as delete_segment
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.storage_subprocess import StorageCallbackCapacityError
from viseron.components.storage.tier_handler import SegmentsTierHandler, move_file


def _case(tmp_path, get_session, count=1):
    now = datetime.datetime.now(datetime.timezone.utc)
    paths = [tmp_path / f"segment-{index}.m4s" for index in range(count)]
    for path in paths:
        path.write_bytes(b"only copy")
    with get_session() as session:
        recording = Recordings(
            camera_identifier="test",
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
                directory=str(tmp_path),
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
    storage.camera_requested_files_count = {"test": SimpleNamespace(filenames=set())}
    thumbnail, clip, segments = Mock(), Mock(), Mock()
    segments._check_tier_lock = threading.Lock()
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
    handler._camera = SimpleNamespace(identifier="test")
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


def test_successful_retention_waits_for_every_committed_delete(tmp_path, get_db_session):
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
    case.thumbnail.move_thumbnail.assert_called_once_with(case.recording_id, None)
    case.clip.move_event_clip.assert_called_once_with(case.recording_id, None)


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
    late = tmp_path / "late.m4s"
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
                directory=str(tmp_path),
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
