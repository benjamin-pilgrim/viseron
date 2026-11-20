"""Test the storage subprocess helpers."""

import datetime
from itertools import count
from queue import Empty, Queue
from threading import Event, Lock
from unittest.mock import Mock

import pytest

from viseron.components.storage.storage_subprocess import (
    DataItem,
    DataItemCopyFile,
    DataItemDeleteFile,
    DataItemMoveFile,
    DedupCheckQueue,
    DedupFileQueue,
    FileOperation,
    FileOperationKey,
    TierCheckWorker,
    file_operation_key,
)


def _make_item(
    camera_identifier: str = "test",
    subcategory: str = "segments",
    callback_id: str | None = "1",
) -> DataItem:
    """Create a check_tier DataItem for tests."""
    return DataItem(
        cmd="check_tier",
        camera_identifier=camera_identifier,
        tier_id=0,
        category="recorder",
        subcategories=[subcategory],
        throttle_period=datetime.timedelta(seconds=0),
        max_bytes=0,
        min_age=datetime.timedelta(seconds=0),
        max_age=datetime.timedelta(seconds=0),
        min_bytes=0,
        drain=False,
        callback_id=callback_id,
    )


class TestDedupCheckQueue:
    """Test DedupCheckQueue."""

    def test_dedupes_by_throttle_key(self) -> None:
        """Only the most recent job for a throttle_key is kept."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        first = _make_item(callback_id="1")
        second = _make_item(callback_id="2")
        queue.put(first)
        queue.put(second)

        assert queue.qsize() == 1
        assert queue.get(timeout=1) is second

    def test_superseded_job_is_acknowledged(self) -> None:
        """A superseded job is returned so the parent can release its callback."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        first = _make_item(callback_id="1")
        second = _make_item(callback_id="2")
        queue.put(first)
        queue.put(second)

        acked = output_queue.get(timeout=1)
        assert acked is first
        assert acked.callback_id == "1"
        # data is None so on_check_tier_result treats it as a no-op reply.
        assert acked.data is None
        # Only the superseded job is acknowledged, the live one is not.
        with pytest.raises(Empty):
            output_queue.get_nowait()

    def test_no_ack_without_callback_id(self) -> None:
        """Jobs sent without a callback need no acknowledgement."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        queue.put(_make_item(callback_id=None))
        queue.put(_make_item(callback_id=None))

        assert queue.qsize() == 1
        with pytest.raises(Empty):
            output_queue.get_nowait()

    def test_every_superseded_job_is_acknowledged(self) -> None:
        """A burst of duplicates leaks no callbacks."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        total = 50
        for i in range(total):
            queue.put(_make_item(callback_id=str(i)))

        # One job remains queued, every other one was acknowledged, so all
        # callback ids are accounted for exactly once.
        assert queue.qsize() == 1
        remaining = queue.get(timeout=1)
        acked_ids = []
        while True:
            try:
                acked_ids.append(output_queue.get_nowait().callback_id)
            except Empty:
                break

        assert sorted([*acked_ids, remaining.callback_id], key=int) == [
            str(i) for i in range(total)
        ]

    def test_distinct_keys_are_not_deduped(self) -> None:
        """Jobs with different throttle_keys coexist and are not acknowledged."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        queue.put(_make_item(camera_identifier="cam1", callback_id="1"))
        queue.put(_make_item(camera_identifier="cam2", callback_id="2"))
        queue.put(_make_item(subcategory="thumbnails", callback_id="3"))

        assert queue.qsize() == 3
        with pytest.raises(Empty):
            output_queue.get_nowait()

    def test_fifo_order_preserved_across_keys(self) -> None:
        """Jobs for distinct keys are returned in arrival order."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        queue.put(_make_item(camera_identifier="cam1", callback_id="1"))
        queue.put(_make_item(camera_identifier="cam2", callback_id="2"))

        assert queue.get(timeout=1).camera_identifier == "cam1"
        assert queue.get(timeout=1).camera_identifier == "cam2"

    def test_replacement_moves_to_tail(self) -> None:
        """A replacement is processed after keys that arrived before it."""
        output_queue: Queue = Queue()
        queue = DedupCheckQueue(output_queue)

        queue.put(_make_item(camera_identifier="cam1", callback_id="1"))
        queue.put(_make_item(camera_identifier="cam2", callback_id="2"))
        # Supersedes cam1, which should now be processed after cam2.
        queue.put(_make_item(camera_identifier="cam1", callback_id="3"))

        assert queue.get(timeout=1).camera_identifier == "cam2"
        assert queue.get(timeout=1).camera_identifier == "cam1"

    def test_get_raises_empty_on_timeout(self) -> None:
        """Get raises queue.Empty when no job arrives in time."""
        queue = DedupCheckQueue(Queue())

        with pytest.raises(Empty):
            queue.get(timeout=0.01)


def test_dedup_file_queue_keeps_distinct_operations() -> None:
    """Different file operations should remain queued independently."""
    queue = DedupFileQueue[FileOperation, FileOperationKey](file_operation_key)
    copy = DataItemCopyFile(cmd="copy_file", src="/src", dst="/dst")
    move = DataItemMoveFile(cmd="move_file", src="/src", dst="/dst")
    delete = DataItemDeleteFile(cmd="delete_file", src="/src")

    queue.put(copy)
    queue.put(move)
    queue.put(delete)

    assert queue.qsize() == 3
    assert queue.get_nowait() is copy
    assert queue.get_nowait() is move
    assert queue.get_nowait() is delete


def test_dedup_file_queue_keeps_callback_copy_jobs_distinct() -> None:
    """Copy callbacks can gate different moves and must not collapse."""
    queue = DedupFileQueue[FileOperation, FileOperationKey](file_operation_key)
    first = DataItemCopyFile(cmd="copy_file", src="/src", dst="/dst", callback_id="1")
    second = DataItemCopyFile(cmd="copy_file", src="/src", dst="/dst", callback_id="2")

    queue.put(first)
    queue.put(second)

    assert queue.qsize() == 2
    assert queue.get_nowait() is first
    assert queue.get_nowait() is second


def _worker() -> TierCheckWorker:
    worker = object.__new__(TierCheckWorker)
    worker._callbacks = {}
    worker._callbacks_lock = Lock()
    worker._next_callback_id = count(1)
    worker.input_queue = Queue()
    worker._process_queue = Queue()
    worker._server = Mock()
    return worker


def test_pending_duplicate_callbacks_receive_publication_result() -> None:
    """Both requesters receive the result of a single publication."""
    worker = _worker()
    pending = DedupFileQueue(file_operation_key)
    callbacks = [Mock(), Mock()]
    for callback in callbacks:
        worker.send_command(
            DataItemMoveFile(cmd="move_file", src="/a", dst="/b"), callback
        )
        pending.put(worker.input_queue.get_nowait())
    publication = pending.get_nowait()
    publication.published = True
    publication.size = 17
    worker.work_output(publication)
    for callback in callbacks:
        callback.assert_called_once()
        assert callback.call_args.args[0].published
        assert callback.call_args.args[0].size == 17
    assert pending.qsize() == 0


def test_in_flight_publication_keeps_its_callback() -> None:
    """A later request cannot discard a publication already in progress."""
    worker = _worker()
    first_result, second_result = Mock(), Mock()
    worker.send_command(
        DataItemMoveFile(cmd="move_file", src="/a", dst="/b"), first_result
    )
    first = worker.input_queue.get_nowait()
    worker.send_command(
        DataItemMoveFile(cmd="move_file", src="/a", dst="/b"), second_result
    )
    first.published = True
    worker.work_output(first)
    first_result.assert_called_once()
    assert first_result.call_args.args[0].published
    second_result.assert_not_called()


def test_capacity_defers_new_work_without_losing_accepted_result(monkeypatch) -> None:
    """Saturation preserves the accepted callback and never enqueues the extra move."""
    monkeypatch.setattr(
        "viseron.components.storage.storage_subprocess.MAX_PENDING_CALLBACKS", 1
    )
    worker = _worker()
    first_result, deferred_result = Mock(), Mock()
    worker.send_command(
        DataItemMoveFile(cmd="move_file", src="/a", dst="/b"), first_result
    )
    worker.send_command(
        DataItemMoveFile(cmd="move_file", src="/c", dst="/d"), deferred_result
    )
    deferred_result.assert_called_once()
    assert "retry" in deferred_result.call_args.args[0].error
    first = worker.input_queue.get_nowait()
    assert worker.input_queue.empty()
    worker.work_output(first)
    first_result.assert_called_once()


def test_restart_replays_commands_lost_with_the_child(monkeypatch) -> None:
    """A full worker can complete accepted jobs after its child restarts."""
    monkeypatch.setattr(
        "viseron.components.storage.storage_subprocess.MAX_PENDING_CALLBACKS", 2
    )
    worker = _worker()
    check_result, move_result, new_result = Mock(), Mock(), Mock()
    worker.send_command(_make_item(), check_result)
    worker.send_command(
        DataItemMoveFile(cmd="move_file", src="/a", dst="/b"), move_result
    )
    worker.forward_input(worker.input_queue.get_nowait())
    old_child_queue = worker._process_queue
    lost_check = old_child_queue.get_nowait()
    pending_move = worker.input_queue.get_nowait()

    worker.requeue_pending()
    worker.forward_input(pending_move)
    worker.work_output(lost_check)
    check_result.assert_not_called()
    assert old_child_queue.empty()

    child_queue = worker._server.process_queue
    worker.work_output(child_queue.get_nowait())
    worker.send_command(_make_item(), new_result)
    worker.forward_input(worker.input_queue.get_nowait())
    worker.work_output(child_queue.get_nowait())
    worker.work_output(child_queue.get_nowait())

    check_result.assert_called_once()
    move_result.assert_called_once()
    new_result.assert_called_once()
    assert not new_result.call_args.args[0].deferred
    assert child_queue.empty()


def test_storage_child_restart_processes_pending_copy(
    vis, tmp_path, monkeypatch
) -> None:
    """The watchdog can start a new child and deliver work through the manager."""
    monkeypatch.setattr(
        "viseron.components.storage.storage_subprocess.MAX_PENDING_CALLBACKS", 1
    )
    worker = TierCheckWorker(vis, 100, 1)
    source, destination = tmp_path / "init.mp4", tmp_path / "copied.mp4"
    source.write_bytes(b"init")
    complete = Event()
    results = []

    def result(item) -> None:
        results.append(item)
        complete.set()

    try:
        process = worker._process_frames_proc
        process.kill()
        process.wait(timeout=5)
        worker.send_command(
            DataItemCopyFile(cmd="copy_file", src=str(source), dst=str(destination)),
            result,
        )
        assert not complete.is_set()
        process.restart()
        assert complete.wait(10)
        assert results[0].published
        assert destination.read_bytes() == b"init"
    finally:
        worker.stop()
