"""Tests for GStreamer camera restart and shutdown."""

from threading import Event, Lock, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock

from viseron.components.gstreamer.camera import Camera


def _camera():
    stream = MagicMock()
    stream.read.return_value = None
    stream.poll.return_value = 1
    return SimpleNamespace(
        decode_error=Event(), _capture_stop=Event(), _capture_lock=Lock(),
        _poll_timer=0, _thread_stuck=False, _capture_frames=True,
        stream=stream, connected=False, still_image_available=False,
        still_image_configured=False, _logger=MagicMock(), current_frame=None,
    )


def test_stop_interrupts_restart_delay():
    """Stopping during the delay does not wait five seconds or start a new pipe."""
    camera = _camera()
    waiting = Event()
    wait = camera._capture_stop.wait

    def observed_wait(timeout):
        waiting.set()
        return wait(timeout)

    camera._capture_stop.wait = observed_wait
    reader = Thread(target=Camera.read_frames, args=(camera,))
    reader.start()
    assert waiting.wait(2)
    with camera._capture_lock:
        camera._capture_frames = False
        camera._capture_stop.set()
    reader.join(2)
    assert not reader.is_alive()
    assert camera.stream.start_pipe.call_count == 1
    camera.stream.close_pipe.assert_called_once()


def test_stop_during_pipe_close_does_not_restart():
    """Shutdown during a slow close prevents the subsequent start."""
    camera = _camera()
    camera._capture_stop.wait = lambda _timeout: False

    def stop_during_close():
        with camera._capture_lock:
            camera._capture_frames = False
            camera._capture_stop.set()

    camera.stream.close_pipe.side_effect = stop_during_close
    Camera.read_frames(camera)
    assert camera.stream.start_pipe.call_count == 1
    camera.stream.close_pipe.assert_called_once()
