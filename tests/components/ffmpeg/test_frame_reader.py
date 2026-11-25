"""Tests for viseron.components.ffmpeg.frame_reader."""

from __future__ import annotations

import logging
import pickle
import queue
import threading
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from viseron.components.ffmpeg import stream
from viseron.components.ffmpeg.const import (
    MAX_EMPTY_FRAMES,
)
from viseron.components.ffmpeg.frame_reader import (
    FrameReaderConfig,
    frame_reader_logger_name,
    run_frame_reader,
)


class _FakeQueue(queue.Queue):
    """queue.Queue plus the close() that multiprocessing queues have.

    run_frame_reader calls frame_queue.close() on shutdown; a plain queue.Queue
    would raise AttributeError.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.closed = False

    def close(self) -> None:
        """Record that the queue was closed."""
        self.closed = True


def _config(**overrides) -> FrameReaderConfig:
    defaults = {
        "camera_identifier": "cam",
        "decoder_command": ["ffmpeg", "-i", "rtsp://x"],
        "segment_command": None,
        "frame_bytes_size": 4,
        "ffmpeg_loglevel": "error",
        "recoverable_errors": [],
        "sensitive_strings": ("hunter2",),
        "log_level": logging.INFO,
    }
    defaults.update(overrides)
    return FrameReaderConfig(**defaults)


def _enter_patches(stack: ExitStack) -> tuple[MagicMock, MagicMock]:
    """Patch the child process side effects, returning the pipe and os.kill mocks."""
    module = "viseron.components.ffmpeg.frame_reader"
    ffmpeg_pipe = stack.enter_context(patch(f"{module}.FFmpegPipe"))
    stack.enter_context(patch(f"{module}.enable_child_logging"))
    stack.enter_context(patch(f"{module}.setproctitle"))
    kill = stack.enter_context(patch(f"{module}.os.kill"))
    return ffmpeg_pipe, kill


def test_config_is_picklable() -> None:
    """The payload crosses a forkserver boundary, so it must pickle."""
    config = _config()
    assert pickle.loads(pickle.dumps(config)) == config  # noqa: S301


def test_stop_during_retry_does_not_restart_decoder():
    """A stop received during camera retry must not launch another decoder."""
    capture = threading.Event()
    capture.set()
    errors = threading.Event()
    frames = _FakeQueue()
    with ExitStack() as stack:
        pipe, _ = _enter_patches(stack)
        pipe.return_value.read.return_value = None
        pipe.return_value.poll.return_value = 1
        stack.enter_context(
            patch(
                "viseron.components.ffmpeg.frame_reader.time.sleep",
                side_effect=lambda _delay: capture.clear(),
            )
        )
        run_frame_reader(_config(), frames, capture, errors)
    assert pipe.return_value.start.call_count == 1
    assert frames.closed


def test_stop_during_retry_close_does_not_restart_decoder():
    """Stopping while FFmpeg closes must cancel the replacement decoder too."""
    capture = threading.Event()
    capture.set()
    errors = threading.Event()
    frames = _FakeQueue()
    now = [0.0]
    with ExitStack() as stack:
        pipe, _ = _enter_patches(stack)
        pipe.return_value.read.return_value = None
        pipe.return_value.poll.return_value = 1
        pipe.return_value.close.side_effect = capture.clear
        stack.enter_context(
            patch(
                "viseron.components.ffmpeg.frame_reader.time.monotonic",
                side_effect=lambda: now[0],
            )
        )
        stack.enter_context(
            patch(
                "viseron.components.ffmpeg.frame_reader.time.sleep",
                side_effect=lambda delay: now.__setitem__(0, now[0] + delay),
            )
        )
        run_frame_reader(_config(), frames, capture, errors)
    assert pipe.return_value.start.call_count == 1
    assert frames.closed


def test_frame_reader_logger_name_matches_the_parent_stream_logger() -> None:
    """The child must configure the level of the logger the parent named."""
    assert frame_reader_logger_name("cam") == f"{stream.__name__}.cam"


def test_reads_frames_until_capture_frames_clears() -> None:
    """Frames flow to the queue until the capture event clears."""
    capture_frames = MagicMock()
    capture_frames.is_set.side_effect = [True, True, False]
    decode_error = MagicMock()
    decode_error.is_set.return_value = False
    frame_queue = _FakeQueue(maxsize=2)

    with ExitStack() as stack:
        ffmpeg_pipe, kill = _enter_patches(stack)
        ffmpeg_pipe.return_value.read.return_value = b"1234"
        run_frame_reader(_config(), frame_queue, capture_frames, decode_error)

    assert frame_queue.get_nowait() == b"1234"
    assert frame_queue.get_nowait() == b"1234"
    assert frame_queue.closed is True
    ffmpeg_pipe.return_value.start.assert_called_once()
    ffmpeg_pipe.return_value.close.assert_called_once()
    kill.assert_called_once()


def test_sets_decode_error_when_pipe_exits() -> None:
    """An exited ffmpeg process must raise decode_error."""
    capture_frames = MagicMock()
    capture_frames.is_set.side_effect = [True, False]
    decode_error = MagicMock()
    decode_error.is_set.return_value = False
    frame_queue = _FakeQueue(maxsize=2)

    with ExitStack() as stack:
        ffmpeg_pipe, _ = _enter_patches(stack)
        ffmpeg_pipe.return_value.read.return_value = None
        ffmpeg_pipe.return_value.poll.return_value = 1
        run_frame_reader(_config(), frame_queue, capture_frames, decode_error)

    decode_error.set.assert_called_once()


def test_sets_decode_error_after_max_empty_frames() -> None:
    """Repeated empty reads must raise decode_error."""
    capture_frames = MagicMock()
    capture_frames.is_set.side_effect = [True] * MAX_EMPTY_FRAMES + [False]
    decode_error = MagicMock()
    decode_error.is_set.return_value = False
    frame_queue = _FakeQueue(maxsize=2)

    with ExitStack() as stack:
        ffmpeg_pipe, _ = _enter_patches(stack)
        ffmpeg_pipe.return_value.read.return_value = None
        ffmpeg_pipe.return_value.poll.return_value = None
        run_frame_reader(_config(), frame_queue, capture_frames, decode_error)

    decode_error.set.assert_called_once()


def _run_retry_sequence(reads):
    """Observe decoder starts on a virtual clock while feeding camera outcomes."""
    capture = threading.Event()
    capture.set()
    errors = threading.Event()
    frames = _FakeQueue(maxsize=2)
    now = [0.0]
    starts = []
    outcomes = iter(reads)
    remaining = len(reads)

    def read(_size):
        nonlocal remaining
        outcome = next(outcomes)
        remaining -= 1
        if remaining == 0:
            capture.clear()
        return outcome

    def sleep(delay):
        now[0] += delay

    with ExitStack() as stack:
        pipe, _ = _enter_patches(stack)
        pipe.return_value.start.side_effect = lambda: starts.append(now[0])
        pipe.return_value.read.side_effect = read
        pipe.return_value.poll.return_value = 1
        stack.enter_context(
            patch(
                "viseron.components.ffmpeg.frame_reader.time.monotonic",
                side_effect=lambda: now[0],
            )
        )
        stack.enter_context(
            patch(
                "viseron.components.ffmpeg.frame_reader.time.sleep", side_effect=sleep
            )
        )
        run_frame_reader(_config(), frames, capture, errors)
    return starts, frames


def test_decode_error_recovers_frames() -> None:
    """A failed decoder is retried and its recovered frame reaches the consumer."""
    starts, frames = _run_retry_sequence([None, b"1234"])
    assert starts == [0, 5]
    assert frames.get_nowait() == b"1234"
    assert frames.closed


def test_restart_delay_backs_off_exponentially() -> None:
    """Repeated failures back off from five seconds to a sixty-second cap."""
    starts, _ = _run_retry_sequence([None] * 9)
    assert starts == [0, 5, 15, 35, 75, 135, 195, 255, 315]


def test_restart_delay_resets_after_a_frame() -> None:
    """A successful frame resets subsequent retries to a five-second delay."""
    starts, frames = _run_retry_sequence([None, None, b"1234", None, None])
    assert starts == [0, 5, 15, 20]
    assert frames.get_nowait() == b"1234"
