"""Tests for viseron.components.ffmpeg.pipe."""

from __future__ import annotations

import logging
import multiprocessing
import sys
import subprocess as sp
from unittest.mock import patch

import pytest

from viseron.components.ffmpeg.pipe import FFmpegPipe

LOGGER = logging.getLogger(__name__)


def test_start_without_segment_command_starts_only_decoder() -> None:
    """No substream configured means no segment process."""
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        pipe = FFmpegPipe("cam", LOGGER, 10, decoder_command=["ffmpeg", "-i", "x"])
        pipe.start()

    assert popen.call_count == 1
    assert popen.call_args.args[0] == ["ffmpeg", "-i", "x"]
    assert popen.call_args.kwargs["name"] == "viseron.camera.cam.pipe"
    assert popen.call_args.kwargs["register"] is False
    assert pipe.segment_process is None


def test_start_with_segment_command_starts_both() -> None:
    """Segment process starts first, then the decoder."""
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        pipe = FFmpegPipe(
            "cam",
            LOGGER,
            10,
            decoder_command=["ffmpeg", "-i", "x"],
            segment_command=["ffmpeg", "-i", "seg"],
        )
        pipe.start()

    assert popen.call_count == 2
    assert popen.call_args_list[0].kwargs["name"] == "viseron.camera.cam.segments"
    assert popen.call_args_list[1].kwargs["name"] == "viseron.camera.cam.pipe"


@pytest.mark.parametrize(
    ("segment_command", "expected_popen_count"),
    [
        pytest.param(None, 1, id="single_stream"),
        pytest.param(["ffmpeg", "-i", "seg"], 2, id="substream"),
    ],
)
def test_start_keeps_ffmpeg_in_the_frame_reader_process_group(
    segment_command: list[str] | None, expected_popen_count: int
) -> None:
    """Ffmpeg must stay in the process group of the frame reader.

    The pipe runs inside the frame reader process, which is the only holder of a
    handle on the FFmpeg processes. Giving them their own session would make them
    unreachable, and they would keep running when the frame reader is killed.
    """
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        pipe = FFmpegPipe(
            "cam",
            LOGGER,
            10,
            decoder_command=["ffmpeg", "-i", "x"],
            segment_command=segment_command,
        )
        pipe.start()

    assert popen.call_count == expected_popen_count
    for call in popen.call_args_list:
        assert call.kwargs["start_new_session"] is False


def test_read_returns_requested_number_of_bytes() -> None:
    """read() forwards the frame size to the decoder stdout."""
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        popen.return_value.stdout.read.return_value = b"1234"
        pipe = FFmpegPipe("cam", LOGGER, 10, decoder_command=["ffmpeg"])
        pipe.start()
        assert pipe.read(4) == b"1234"
        popen.return_value.stdout.read.assert_called_once_with(4)


def test_read_swallows_errors() -> None:
    """A read error must not kill the frame reader loop."""
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        popen.return_value.stdout.read.side_effect = OSError("boom")
        pipe = FFmpegPipe("cam", LOGGER, 10, decoder_command=["ffmpeg"])
        pipe.start()
        assert pipe.read(4) is None


def test_poll_returns_none_before_start() -> None:
    """Polling before start() must not raise."""
    pipe = FFmpegPipe("cam", LOGGER, 10, decoder_command=["ffmpeg"])
    assert pipe.poll() is None


def test_close_terminates_and_kills_on_timeout() -> None:
    """A stubborn ffmpeg process gets killed after the terminate timeout."""
    with (
        patch("viseron.components.ffmpeg.pipe.RestartablePopen") as popen,
        patch("viseron.components.ffmpeg.pipe.LogPipe"),
    ):
        process = popen.return_value
        process.communicate.side_effect = [sp.TimeoutExpired("ffmpeg", 5), None]
        pipe = FFmpegPipe("cam", LOGGER, 10, decoder_command=["ffmpeg"])
        pipe.start()
        pipe.close()

    process.terminate.assert_called_once()
    process.kill.assert_called_once()


def test_recorder_launch_reports_new_run_after_restart(tmp_path):
    """Each actual recording process launch exposes its new run to the parent."""
    marker = multiprocessing.get_context("fork").Array("c", 64)
    command = [
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        str(tmp_path / "__VISERON_RUN__" / "index.m3u8"),
    ]
    pipe = FFmpegPipe("test", LOGGER, 10, decoder_command=command, run_marker=marker)
    try:
        pipe.start()
        first = marker.value.decode("ascii")
        assert (tmp_path / first).is_dir()
        pipe.close()
        pipe.start()
        second = marker.value.decode("ascii")
        assert (tmp_path / second).is_dir()
        assert first != second
    finally:
        pipe.close()


def test_segment_writer_watchdog_restart_publishes_new_run(tmp_path):
    """A separate segment writer updates the parent even if decoding continues."""
    marker = multiprocessing.get_context("fork").Array("c", 64)
    segment = [
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        str(tmp_path / "__VISERON_RUN__" / "index.m3u8"),
    ]
    decoder = [sys.executable, "-c", "import time; time.sleep(60)"]
    pipe = FFmpegPipe(
        "test",
        LOGGER,
        10,
        decoder_command=decoder,
        segment_command=segment,
        run_marker=marker,
    )
    try:
        pipe.start()
        first = marker.value.decode("ascii")
        assert (tmp_path / first).is_dir()
        pipe.segment_process.restart()
        second = marker.value.decode("ascii")
        assert first != second
        assert (tmp_path / second).is_dir()
    finally:
        pipe.close()
