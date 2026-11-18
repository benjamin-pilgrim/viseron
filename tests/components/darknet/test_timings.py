"""Observable diagnostics for detector delays without logging camera frames."""

import time
from queue import Empty, Queue
from unittest.mock import MagicMock

import pytest

from viseron.components.darknet import DarknetDNN
from viseron.components.darknet import darknet_subprocess


def detector():
    """Build the detector interface without starting a worker process."""
    instance = object.__new__(DarknetDNN)
    instance._result_queues = {}
    instance._worker_activity = None
    instance.input_queue = Queue()
    instance._process_frames_proc = MagicMock()
    instance._process_frames_proc.poll.return_value = None
    instance._nms = 0.4
    return instance


def test_timeout_identifies_running_request(monkeypatch, caplog):
    """A timeout exposes which camera/job is occupying the shared worker."""
    instance = detector()
    instance.work_output(
        {
            "diagnostic": "started",
            "camera_identifier": "other_camera",
            "request_id": "busy-job",
            "started": 90.0,
        }
    )
    monkeypatch.setattr(time, "monotonic", lambda: 100.0)
    result_queue = MagicMock()
    result_queue.get.side_effect = Empty

    assert instance.detect("private-frame", "front_door", result_queue, 0.5) is None
    assert "Detector timeout camera=front_door" in caplog.text
    assert "busy-job" in caplog.text
    assert "other_camera" in caplog.text
    assert "running_seconds': 10.0" in caplog.text
    assert "worker_exit=None" in caplog.text
    assert "private-frame" not in caplog.text


@pytest.mark.parametrize(
    ("started", "finished", "received", "expected"),
    [
        (12.0, 12.1, 12.2, "queue=2.000s"),
        (10.0, 12.0, 12.2, "inference=2.000s"),
        (10.0, 10.1, 12.1, "delivery=2.000s"),
    ],
)
def test_slow_result_breaks_down_delay(
    monkeypatch, caplog, started, finished, received, expected
):
    """Late results retain detection output and identify the delayed stage."""
    instance = detector()
    result_queue = Queue()
    instance._result_queues["camera"] = result_queue
    monkeypatch.setattr(time, "monotonic", lambda: received)
    item = {
        "camera_identifier": "camera",
        "request_id": "late-job",
        "submitted": 10.0,
        "started": started,
        "finished": finished,
        "result": "detections",
    }
    instance.work_output(item)

    assert result_queue.get_nowait()["result"] == "detections"
    assert "Slow detector result camera=camera request=late-job" in caplog.text
    assert expected in caplog.text


def test_fast_result_is_quiet(monkeypatch, caplog):
    """Healthy throughput does not generate per-frame warning logs."""
    instance = detector()
    instance._result_queues["camera"] = Queue()
    monkeypatch.setattr(time, "monotonic", lambda: 10.2)
    instance.work_output(
        {
            "camera_identifier": "camera",
            "request_id": "job",
            "submitted": 10.0,
            "started": 10.05,
            "finished": 10.1,
            "result": [],
        }
    )
    assert not caplog.records
    assert instance._result_queues["camera"].get_nowait()["result"] == []


def test_child_reports_start_before_inference_and_returns_timings(monkeypatch):
    """Even a stalled inference leaves a start notification for timeout diagnosis."""
    input_queue, output_queue = MagicMock(), Queue()
    job = {"camera_identifier": "camera", "request_id": "job", "submitted": 10.0}
    input_queue.get.side_effect = [job, KeyboardInterrupt]
    model = MagicMock()

    def infer(item):
        started = output_queue.get_nowait()
        assert started == {
            "diagnostic": "started",
            "camera_identifier": "camera",
            "request_id": "job",
            "started": 11.0,
        }
        item["result"] = "detections"

    model.work_input.side_effect = infer
    monkeypatch.setattr(darknet_subprocess, "DarknetDNN", MagicMock(return_value=model))
    monkeypatch.setattr(
        darknet_subprocess, "connect", lambda *_args: (input_queue, output_queue)
    )
    monkeypatch.setattr(darknet_subprocess, "setup_logger", lambda *_: None)
    monkeypatch.setattr(darknet_subprocess, "get_parser", MagicMock())
    clock = iter([11.0, 11.1, 11.5])
    monkeypatch.setattr(time, "monotonic", lambda: next(clock))
    # Initialization is reported before any inference.
    original_infer = model.work_input.side_effect

    def infer_after_ready(item):
        assert output_queue.get_nowait() == "init_done"
        original_infer(item)

    model.work_input.side_effect = infer_after_ready
    with pytest.raises(KeyboardInterrupt):
        darknet_subprocess.main()
    result = output_queue.get_nowait()
    assert result["result"] == "detections"
    assert result["submitted"] == 10.0
    assert result["started"] == 11.1
    assert result["finished"] == 11.5


def test_successful_request_keeps_results_and_clears_activity(monkeypatch, caplog):
    """Timing notifications never enter the caller's detection result queue."""
    instance = detector()
    result_queue = Queue()
    monkeypatch.setattr(time, "monotonic", lambda: 100.0)
    original_get = result_queue.get

    def receive(**kwargs):
        job = instance.input_queue.get_nowait()
        instance.work_output(
            {
                "diagnostic": "started",
                "camera_identifier": job["camera_identifier"],
                "request_id": job["request_id"],
                "started": 100.0,
            }
        )
        assert result_queue.empty()
        instance.work_output(
            {**job, "started": 100.0, "finished": 100.0, "result": "detections"}
        )
        return original_get(**kwargs)

    monkeypatch.setattr(result_queue, "get", receive)
    assert instance.detect("private-frame", "camera", result_queue, 0.5) == "detections"
    assert not caplog.records

    def no_result(**_kwargs):
        raise Empty

    monkeypatch.setattr(result_queue, "get", no_result)
    assert instance.detect("private-frame", "camera", result_queue, 0.5) is None
    assert "active=None" in caplog.text
    assert "private-frame" not in caplog.text
