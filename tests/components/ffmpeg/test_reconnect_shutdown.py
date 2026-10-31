"""Frame-reader reconnect waits must respond to shutdown."""

import threading
from unittest.mock import Mock, patch
from viseron.components.ffmpeg.frame_reader import FrameReaderConfig, run_frame_reader


def test_shutdown_during_reconnect_backoff_never_restarts_ffmpeg():
    """Stop without restarting when shutdown interrupts reconnect backoff."""
    capture = threading.Event()
    capture.set()
    decode_error = threading.Event()
    frames = Mock()
    pipe = Mock()
    pipe.read.return_value = b""
    pipe.poll.return_value = 1
    config = FrameReaderConfig(
        camera_identifier="camera",
        decoder_command=["ffmpeg"],
        segment_command=None,
        frame_bytes_size=1,
        ffmpeg_loglevel="error",
        recoverable_errors=[],
        sensitive_strings=(),
        log_level=20,
    )
    with (
        patch("viseron.components.ffmpeg.frame_reader.FFmpegPipe", return_value=pipe),
        patch("viseron.components.ffmpeg.frame_reader.enable_child_logging"),
        patch("viseron.components.ffmpeg.frame_reader.setproctitle.setproctitle"),
        patch("viseron.components.ffmpeg.frame_reader.os.kill"),
        patch(
            "viseron.components.ffmpeg.frame_reader.time.sleep",
            side_effect=lambda _: capture.clear(),
        ),
    ):
        run_frame_reader(config, frames, capture, decode_error)
    pipe.start.assert_called_once()
    pipe.close.assert_called_once()
    frames.close.assert_called_once()


def test_shutdown_during_restart_does_not_read_again():
    """Close a restarted pipe if shutdown arrives while it starts."""
    capture = threading.Event()
    capture.set()
    decode_error = threading.Event()
    frames = Mock()
    pipe = Mock()
    pipe.read.return_value = b""
    pipe.poll.return_value = 1

    def start():
        if pipe.start.call_count == 2:
            capture.clear()

    pipe.start.side_effect = start
    config = FrameReaderConfig(
        camera_identifier="camera",
        decoder_command=["ffmpeg"],
        segment_command=None,
        frame_bytes_size=1,
        ffmpeg_loglevel="error",
        recoverable_errors=[],
        sensitive_strings=(),
        log_level=20,
    )
    with (
        patch("viseron.components.ffmpeg.frame_reader.FFmpegPipe", return_value=pipe),
        patch("viseron.components.ffmpeg.frame_reader.enable_child_logging"),
        patch("viseron.components.ffmpeg.frame_reader.setproctitle.setproctitle"),
        patch("viseron.components.ffmpeg.frame_reader.os.kill"),
        patch("viseron.components.ffmpeg.frame_reader.time.sleep"),
    ):
        run_frame_reader(config, frames, capture, decode_error)
    assert pipe.start.call_count == 2
    assert pipe.read.call_count == 1
    assert pipe.close.call_count == 2


def test_shutdown_during_pipe_close_does_not_close_twice():
    """Close the pipe once when shutdown occurs during reconnect cleanup."""
    capture = threading.Event()
    capture.set()
    decode_error = threading.Event()
    frames = Mock()
    pipe = Mock()
    pipe.read.return_value = b""
    pipe.poll.return_value = 1
    pipe.close.side_effect = capture.clear
    config = FrameReaderConfig(
        camera_identifier="camera",
        decoder_command=["ffmpeg"],
        segment_command=None,
        frame_bytes_size=1,
        ffmpeg_loglevel="error",
        recoverable_errors=[],
        sensitive_strings=(),
        log_level=20,
    )
    with (
        patch("viseron.components.ffmpeg.frame_reader.FFmpegPipe", return_value=pipe),
        patch("viseron.components.ffmpeg.frame_reader.enable_child_logging"),
        patch("viseron.components.ffmpeg.frame_reader.setproctitle.setproctitle"),
        patch("viseron.components.ffmpeg.frame_reader.os.kill"),
        patch("viseron.components.ffmpeg.frame_reader.time.sleep"),
    ):
        run_frame_reader(config, frames, capture, decode_error)
    pipe.start.assert_called_once()
    pipe.close.assert_called_once()
    frames.close.assert_called_once()
