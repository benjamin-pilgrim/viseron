"""Test the storage subprocess helpers."""

import datetime
from collections import OrderedDict
from itertools import count
from queue import Empty, Queue
from threading import Lock
from unittest.mock import Mock, patch

import pytest

from viseron.components.storage.storage_subprocess import (
    DataItem,
    DedupCheckQueue,
    TierCheckWorker,
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


def test_full_callback_capacity_rejects_new_work_without_losing_inflight() -> None:
    """Overload must not discard the callback that releases an active tier."""
    worker = TierCheckWorker.__new__(TierCheckWorker)
    worker._callbacks = OrderedDict()
    worker._callbacks_lock = Lock()
    worker._next_callback_id = count(1)
    worker.input_queue = Queue()
    first, second = _make_item(callback_id=None), _make_item(callback_id=None)
    first_callback, second_callback = Mock(), Mock()
    with patch(
        "viseron.components.storage.storage_subprocess.MAX_PENDING_CALLBACKS", 1
    ):
        worker.send_command(first, first_callback)
        with pytest.raises(RuntimeError, match="callback capacity"):
            worker.send_command(second, second_callback)
    assert worker.input_queue.get_nowait() is first
    assert worker.input_queue.empty()
    worker.work_output(first)
    first_callback.assert_called_once_with(first)
    second_callback.assert_not_called()


def test_queue_failure_releases_callback_capacity() -> None:
    """An enqueue error must not leak the callback reservation."""
    worker = TierCheckWorker.__new__(TierCheckWorker)
    worker._callbacks = OrderedDict()
    worker._callbacks_lock = Lock()
    worker._next_callback_id = count(1)
    worker.input_queue = Mock()
    worker.input_queue.put.side_effect = RuntimeError("queue failed")
    with pytest.raises(RuntimeError, match="queue failed"):
        worker.send_command(_make_item(callback_id=None), Mock())
    assert not worker._callbacks


def test_worker_marks_busy_check_as_deferred() -> None:
    """Distinguish a busy camera from a normal no-op check."""
    from viseron.components.storage.check_tier import Worker  # noqa: PLC0415

    worker = Worker.__new__(Worker)
    worker._check_locks = {"test": Lock()}
    worker._checks_in_progress = {"test": True}
    worker._last_call = {}
    item = _make_item()
    worker.check_tier(item)
    assert item.deferred
    assert item.data is None
