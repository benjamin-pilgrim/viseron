"""Darknet component tests."""

from __future__ import annotations

from queue import Empty, Queue
from unittest.mock import MagicMock

import pytest

from viseron.components.darknet import DarknetDNN


@pytest.mark.parametrize("expected", ["detections", None], ids=["result", "no_result"])
def test_dnn_detect_does_not_block_without_result(expected: str | None) -> None:
    """detect() returns the matching worker answer or gives up without one."""
    darknet = MagicMock(_result_queues={}, input_queue=Queue())
    result_queue = MagicMock()

    def receive(**_kwargs):
        if expected is None:
            raise Empty
        job = darknet.input_queue.get_nowait()
        return {**job, "result": expected}

    result_queue.get.side_effect = receive
    assert (
        DarknetDNN.detect(darknet, MagicMock(), "camera", result_queue, 0.5) == expected
    )
