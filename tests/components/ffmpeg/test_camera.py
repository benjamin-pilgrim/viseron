"""Tests for viseron.components.ffmpeg.camera."""

from __future__ import annotations

import logging
import multiprocessing as mp
import threading
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

import pytest

from viseron.components.ffmpeg.camera import Camera
from viseron.components.ffmpeg.const import (
    CONFIG_FFMPEG_LOGLEVEL,
    CONFIG_FFMPEG_RECOVERABLE_ERRORS,
    CONFIG_RECORD_ONLY,
    CONFIG_SUBSTREAM,
)
from viseron.components.ffmpeg.frame_reader import frame_reader_logger_name
from viseron.components.storage.models import TriggerTypes
from viseron.domains.camera import AbstractCamera
from viseron.helpers.logs import refresh_child_log_levels
from viseron.watchdog.process_watchdog import ProcessWatchDog
from viseron.watchdog.thread_watchdog import ThreadWatchDog

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER_NAME = frame_reader_logger_name("cam")


@pytest.fixture(name="camera")
def camera_fixture() -> Iterator[Camera]:
    """Return a camera with only what the frame reader methods need."""
    camera = object.__new__(Camera)
    camera._identifier = "cam"
    camera._logger = MagicMock()
    camera._mp_context = cast("mp.context.ForkServerContext", mp.get_context())
    camera._recording_run_marker = camera._mp_context.Array("c", 64)
    camera._frame_queue = None  # type: ignore[assignment]
    camera._capture_frames = MagicMock()
    camera.decode_error = MagicMock()
    camera._frame_reader = None
    camera._frame_relay = None
    camera._check_segment_process_thread = None
    camera.stream = MagicMock()
    camera._config = {
        CONFIG_SUBSTREAM: None,
        CONFIG_FFMPEG_LOGLEVEL: "error",
        CONFIG_FFMPEG_RECOVERABLE_ERRORS: [],
        CONFIG_RECORD_ONLY: False,
    }

    registered_processes = ProcessWatchDog.registered_items[:]
    registered_threads = ThreadWatchDog.registered_items[:]
    try:
        yield camera
    finally:
        camera._stop_camera()
        ProcessWatchDog.registered_items[:] = registered_processes
        ThreadWatchDog.registered_items[:] = registered_threads
        logging.getLogger(LOGGER_NAME).setLevel(logging.NOTSET)


def test_create_frame_reader_registers_the_child_log_levels(camera: Camera) -> None:
    """A running frame reader must follow the log level of its camera."""
    frame_reader, _ = camera._create_frame_reader()
    child_log_levels = frame_reader._child_log_levels

    logging.getLogger(LOGGER_NAME).setLevel(logging.DEBUG)
    refresh_child_log_levels()
    # The child is still on the level it started with.
    logging.getLogger(LOGGER_NAME).setLevel(logging.INFO)

    assert (
        child_log_levels.wait_and_apply(  # type: ignore[attr-defined]
            timeout=5,
        )
        is True
    )
    assert logging.getLogger(LOGGER_NAME).level == logging.DEBUG


def test_stop_camera_unregisters_the_child_log_levels(camera: Camera) -> None:
    """A stopped camera must not be kept alive by the registry."""
    camera._frame_reader, _ = camera._create_frame_reader()
    camera._frame_relay = MagicMock()
    child_log_levels = camera._frame_reader._child_log_levels

    camera._stop_camera()
    refresh_child_log_levels()

    assert (
        child_log_levels.wait_and_apply(  # type: ignore[attr-defined]
            timeout=0.01,
        )
        is False
    )


def test_camera_stop_serializes_against_a_recording_start():
    """A start racing camera shutdown cannot leave an event active afterward."""
    entered = threading.Event()
    release = threading.Event()

    class BlockingEvent:
        def __init__(self):
            self.event = threading.Event()

        def is_set(self):
            observed = self.event.is_set()
            entered.set()
            release.wait(3)
            return observed

        def set(self):
            self.event.set()

    class CameraStub:
        start_recorder = Camera.start_recorder
        stop_recorder = Camera.stop_recorder
        stop_camera = AbstractCamera.stop_camera

    camera = CameraStub()
    camera._camera_state_lock = threading.RLock()
    camera.stopped = BlockingEvent()
    camera._recorder = MagicMock()
    camera._stop_camera = MagicMock()
    camera._vis = MagicMock()
    camera.identifier = "test"
    camera.still_image_configured = False
    camera.fragmenter = MagicMock()
    camera.is_recording = False
    camera._recorder.start.side_effect = lambda *_: setattr(
        camera, "is_recording", True
    )
    camera._recorder.stop_current.side_effect = lambda: setattr(
        camera, "is_recording", False
    )
    starter = threading.Thread(
        target=camera.start_recorder, args=(None, [], TriggerTypes.MOTION)
    )
    stopper = threading.Thread(target=camera.stop_camera)
    starter.start()
    assert entered.wait(2)
    stopper.start()
    release.set()
    starter.join(3)
    stopper.join(3)
    assert not starter.is_alive() and not stopper.is_alive()
    assert not camera.is_recording
    camera._recorder.start.assert_called_once()
    camera._recorder.stop_current.assert_called_once()
