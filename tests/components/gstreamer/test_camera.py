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
    reader = Thread(target=Camera.read_frames, args=(camera, camera._capture_stop))
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
    Camera.read_frames(camera, camera._capture_stop)
    assert camera.stream.start_pipe.call_count == 1
    camera.stream.close_pipe.assert_called_once()


def test_restart_waits_for_stopped_reader_to_finish_closing():
    """A later start cannot revive a reader blocked in its old close."""
    camera = _camera()
    entered = Event()
    release = Event()
    camera._capture_stop.wait = lambda _timeout: False
    original_stop = camera._capture_stop

    def close():
        entered.set()
        assert release.wait(3)

    camera.stream.close_pipe.side_effect = close
    old = Thread(target=Camera.read_frames, args=(camera, original_stop))
    old.stop = MagicMock()
    camera._frame_reader = old
    camera._start_after_previous = lambda previous, stop: Camera._start_after_previous(
        camera, previous, stop
    )
    launched = Event()
    new_reader = MagicMock()
    new_reader.start.side_effect = launched.set
    camera._create_frame_reader = MagicMock(return_value=new_reader)
    old.start()
    assert entered.wait(2)
    # Avoid the normal five-second bounded join in this deterministic test.
    old_join = old.join
    old.join = lambda timeout=None: old_join(0.01 if timeout else None)
    Camera._stop_camera(camera)
    Camera._start_camera(camera)
    assert not launched.wait(0.1)
    assert camera.stream.close_pipe.call_count == 1
    release.set()
    assert launched.wait(2)
    old_join(2)
    assert camera.stream.start_pipe.call_count == 1
    assert not old.is_alive()


def test_queue_timeouts_do_not_trigger_empty_frame_restart():
    """Slow live sources remain governed by the configured frame watchdog."""
    camera = _camera()
    camera.stream.poll.return_value = None
    camera.stream.frame_wait_timed_out = True
    reads = 0

    def read():
        nonlocal reads
        reads += 1
        if reads == 12:
            camera._capture_stop.set()
        return None

    camera.stream.read.side_effect = read
    Camera.read_frames(camera, camera._capture_stop)
    assert reads == 12
    assert camera.stream.start_pipe.call_count == 1
    assert not camera.decode_error.is_set()
