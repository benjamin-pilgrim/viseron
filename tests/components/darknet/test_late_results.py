"""A timed-out inference must never supply detections for a newer frame."""

import time
from queue import Empty, Queue
from unittest.mock import MagicMock

import pytest

from viseron.components.darknet import DarknetDNN, DarknetNative


@pytest.fixture(params=[DarknetDNN, DarknetNative], ids=["opencv", "native"])
def detector(request):
    """Exercise the real detection interface without loading a hardware model."""
    instance = object.__new__(request.param)
    instance._result_queues = {}
    instance.input_queue = Queue()
    instance._worker_activity = None
    instance._process_frames_proc = MagicMock()
    instance._process_frames_proc.poll.return_value = None
    instance._nms = 0.4
    return instance


def test_next_frame_recovers_after_timed_out_result(detector, monkeypatch):
    """A late result is skipped while waiting for the current frame's result."""
    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    results = Queue(maxsize=1)
    original_get = results.get
    first_request = []

    def receive(timeout):
        if not first_request:
            first_request.append(detector.input_queue.get_nowait())
            now[0] += timeout
            raise Empty
        if results.empty():
            current = detector.input_queue.get_nowait()
            detector.work_output({**current, "result": "current detections"})
        return original_get(timeout=timeout)

    monkeypatch.setattr(results, "get", receive)
    assert detector.detect("old frame", "camera", results, 0.5) is None
    # The slow worker finishes after its caller has given up. Its late output
    # is already waiting when the camera next submits a frame.
    detector.work_output({**first_request[0], "result": "expired detections"})
    assert detector.detect("new frame", "camera", results, 0.5) == "current detections"
    assert detector.input_queue.empty()


def test_expired_results_do_not_extend_detection_deadline(detector, monkeypatch):
    """Repeated stale output cannot restart the three-second wait budget."""
    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    results = Queue()

    def stale_output(timeout):
        now[0] += min(1.0, timeout)
        return {"request_id": "expired", "result": "expired detections"}

    monkeypatch.setattr(results, "get", stale_output)
    assert detector.detect("new frame", "camera", results, 0.5) is None
    assert now[0] == 103.0
