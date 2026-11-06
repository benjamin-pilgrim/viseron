"""Test the TierHandler class."""

from __future__ import annotations

import datetime
import logging
import os
import shutil
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast
from unittest.mock import ANY, MagicMock, Mock, patch

import numpy as np
import pytest
from sqlalchemy import delete, event, insert, select, text, update
from sqlalchemy.exc import NoResultFound
from watchdog.events import FileCreatedEvent, FileDeletedEvent

from viseron.components.storage import TIER_CATEGORIES, Storage
from viseron.components.storage.const import (
    COMPONENT as STORAGE_COMPONENT,
    CONFIG_DRAIN,
    CONFIG_PATH,
    CONFIG_RECORDER,
    LATEST_SNAPSHOT_FILENAME,
    TIER_CATEGORY_RECORDER,
    TIER_CATEGORY_SNAPSHOTS,
    TIER_SUBCATEGORY_FACE_RECOGNITION,
    TIER_SUBCATEGORY_IMAGE_CLASSIFICATION,
    TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION,
    TIER_SUBCATEGORY_MOTION_DETECTOR,
    TIER_SUBCATEGORY_OBJECT_DETECTOR,
    TIER_SUBCATEGORY_SEGMENTS,
)
from viseron.components.storage.models import (
    Files,
    FilesMeta,
    Motion,
    Objects,
    PostProcessorResults,
    Recordings,
)
from viseron.components.storage.storage_subprocess import (
    DataItemMoveFile,
    StorageCallbackCapacityError,
)
from viseron.components.storage.tier_handler import (
    SNAPSHOT_REFERENCE_MODELS,
    EventClipTierHandler,
    SegmentsTierHandler,
    SnapshotTierHandler,
    ThumbnailTierHandler,
    TierHandler,
    find_next_tier_segments,
    handle_file,
    move_file,
)
from viseron.components.storage.util import EventFileCreated
from viseron.domains.camera.const import (
    CONFIG_CONTINUOUS_RECORDING,
    CONFIG_LOOKBACK,
    CONFIG_SCHEDULE,
    CONFIG_SCHEDULE_CONTINUOUS,
    CONFIG_SCHEDULE_TIMEZONE,
)
from viseron.helpers import utcnow

from tests.common import BaseTestWithRecordings

if TYPE_CHECKING:
    from pathlib import Path

    from numpy._typing._array_like import NDArray
    from sqlalchemy.engine import ExceptionContext
    from sqlalchemy.orm import Session, sessionmaker

    from viseron import Viseron

    from tests.conftest import MockViseron

_TierHandlerT = TypeVar("_TierHandlerT", bound=TierHandler)


@patch("viseron.components.storage.tier_handler.delete_file")
def test_handle_file_delete(mock_delete_file: Mock, vis: MockViseron) -> None:
    """Test handle_file."""
    file = "/tmp/tier1/file1"
    tier_1 = {
        "path": "/tmp/tier1",
    }
    tier_2 = None
    session = MagicMock()
    logger = MagicMock()
    storage = MagicMock()
    handle_file(
        vis,
        session,
        storage,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        tier_1,
        tier_2,
        file,
        "/tmp/tier1/",
        logger,
    )
    mock_delete_file.assert_called_once()
    assert mock_delete_file.call_args.args == (storage, file)


@patch("viseron.components.storage.tier_handler.move_file")
def test_handle_file_move(mock_move_file: Mock, vis: MockViseron) -> None:
    """Test handle_file."""
    tier_1_file = "/tmp/tier1/file1"
    tier_2_file = "/tmp/tier2/file1"
    tier_1 = {
        "path": "/tmp/tier1/",
    }
    tier_2 = {
        "path": "/tmp/tier2/",
    }
    storage = MagicMock()
    session = MagicMock()
    logger = MagicMock()
    completed = Mock()
    handle_file(
        vis,
        session,
        storage,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        tier_1,
        tier_2,
        tier_1_file,
        "/tmp/tier1/",
        logger,
        on_complete=completed,
    )
    mock_move_file.assert_called_once_with(
        vis,
        storage,
        session,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        tier_1_file,
        tier_2_file,
        "/tmp/tier2/",
        logger,
        on_complete=completed,
    )


@patch("viseron.components.storage.tier_handler.delete_file")
def test_handle_file_force_delete_ignores_unavailable_destination(
    mock_delete_file: Mock, vis: MockViseron
) -> None:
    """Already eligible deletions need no available next tier."""
    storage = MagicMock()
    storage.tier_available.side_effect = lambda path: path != "/tier2"
    handle_file(
        vis,
        MagicMock(),
        storage,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        {"path": "/tier1"},
        {"path": "/tier2"},
        "/tier1/old.m4s",
        "/tier1",
        MagicMock(),
        force_delete=True,
    )
    mock_delete_file.assert_called_once()
    assert mock_delete_file.call_args.args == (storage, "/tier1/old.m4s")


@patch("viseron.components.storage.tier_handler.move_file")
@patch("viseron.components.storage.tier_handler.delete_file")
def test_handle_file_unavailable_destination_preserves_promotion_source(
    mock_delete_file: Mock, mock_move_file: Mock, vis: MockViseron
) -> None:
    """A failed promotion does not make its source eligible for deletion."""
    storage = MagicMock()
    storage.tier_available.side_effect = lambda path: path != "/tier2"
    handle_file(
        vis,
        MagicMock(),
        storage,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        {"path": "/tier1"},
        {"path": "/tier2"},
        "/tier1/old.m4s",
        "/tier1",
        MagicMock(),
    )
    mock_delete_file.assert_not_called()
    mock_move_file.assert_not_called()


@patch("viseron.components.storage.tier_handler.move_file")
@patch("viseron.components.storage.tier_handler.delete_file")
def test_handle_file_skips_recently_requested_basename(
    mock_delete_file: Mock, mock_move_file: Mock, vis: MockViseron
) -> None:
    """A recently requested basename protects a file in any tier."""
    storage = MagicMock()
    storage.camera_requested_files_count["test"].filenames = ["old.m4s"]
    handle_file(
        vis,
        MagicMock(),
        storage,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        {"path": "/tier1"},
        {"path": "/tier2"},
        "/tier1/camera/run/old.m4s",
        "/tier1",
        MagicMock(),
    )
    mock_move_file.assert_not_called()
    mock_delete_file.assert_not_called()


def test_snapshot_tier_handler_ignores_the_latest_snapshot() -> None:
    """Test that latest_snapshot.jpg is exempt from the database and tier moves."""
    tier_handler = SnapshotTierHandler.__new__(SnapshotTierHandler)
    tier_handler._path = "/snapshots/face_recognition/test_camera"
    tier_handler._storage = MagicMock(spec=Storage)
    tier_handler.add_file_handler = MagicMock()  # type: ignore[method-assign]

    with patch.object(TierHandler, "initialize"):
        tier_handler.initialize()

    tier_handler._storage.ignore_file.assert_called_once_with(LATEST_SNAPSHOT_FILENAME)


@dataclass
class MockRecordingsQueryResult:
    """Mock query result."""

    recording_id: int | None
    file_id: int
    path: str
    tier_path: str


@dataclass
class MockFilesQueryResult:
    """Mock query result."""

    id: int
    path: str
    tier_path: str


def _get_tier_config(events: bool, continuous: bool) -> dict[str, Any]:
    """Get tier config for test."""
    max_age_events = None
    max_age_continuous = None
    if events:
        max_age_events = 1
    if continuous:
        max_age_continuous = 1
    return {
        "path": "/",
        "events": {
            "max_age": {"days": max_age_events, "hours": None, "minutes": None},
            "min_age": {"hours": None, "days": None, "minutes": None},
            "min_size": {"gb": None, "mb": None},
            "max_size": {"gb": None, "mb": None},
        },
        "move_on_shutdown": False,
        "poll": False,
        "continuous": {
            "max_age": {"minutes": max_age_continuous, "hours": None, "days": None},
            "min_age": {"minutes": None, "hours": None, "days": None},
            "min_size": {"gb": None, "mb": None},
            "max_size": {"gb": None, "mb": None},
        },
        "check_interval": {
            "days": 0,
            "hours": 0,
            "minutes": 1,
            "seconds": 0,
        },
    }


class TestSegmentsTierHandler(BaseTestWithRecordings):
    """Test the SegmentsTierHandler class."""

    @pytest.mark.parametrize(
        ("tier", "data", "recordings_amount", "first_recording_id"),
        [
            (
                _get_tier_config(events=True, continuous=False),
                np.array(
                    [
                        (
                            1,
                            1,
                            "/tmp/test1.mp4",
                            "/tmp/",
                        ),
                        (
                            1,
                            2,
                            "/tmp/test2.mp4",
                            "/tmp/",
                        ),
                    ],
                    dtype=(
                        [
                            ("recording_id", np.int64),
                            ("id", np.int64),
                            ("path", "U512"),
                            ("tier_path", "U512"),
                        ]
                    ),
                ),
                3,
                1,
            ),
            (
                _get_tier_config(events=True, continuous=True),
                np.array(
                    [
                        (
                            1,
                            1,
                            "/tmp/test1.mp4",
                            "/tmp/",
                        ),
                        (
                            1,
                            2,
                            "/tmp/test2.mp4",
                            "/tmp/",
                        ),
                    ],
                    dtype=(
                        [
                            ("recording_id", np.int64),
                            ("id", np.int64),
                            ("path", "U512"),
                            ("tier_path", "U512"),
                        ]
                    ),
                ),
                3,
                1,
            ),
            (
                _get_tier_config(events=False, continuous=True),
                np.array(
                    [
                        (
                            -1,
                            1,
                            "/tmp/test1.mp4",
                            "/tmp/",
                        ),
                        (
                            -1,
                            2,
                            "/tmp/test2.mp4",
                            "/tmp/",
                        ),
                    ],
                    dtype=(
                        [
                            ("recording_id", np.int64),
                            ("id", np.int64),
                            ("path", "U512"),
                            ("tier_path", "U512"),
                        ]
                    ),
                ),
                3,
                1,
            ),
        ],
    )
    def test__check_tier(
        self,
        vis: MockViseron,
        tier: dict[str, Any],
        data: NDArray[Any],
        recordings_amount: Literal[2, 3],
        first_recording_id: Literal[3, 1],
    ) -> None:
        """Test _check_tier."""
        mock_camera = Mock()
        mock_camera.identifier = "test"
        mock_camera.config = {
            CONFIG_RECORDER: {
                CONFIG_LOOKBACK: 5,
                CONFIG_CONTINUOUS_RECORDING: True,
                CONFIG_SCHEDULE: None,
            }
        }

        tier_handler = SegmentsTierHandler(
            vis,
            mock_camera,
            0,
            "recorder",
            "segments",
            tier,
            None,
        )

        with patch("viseron.components.storage.tier_handler.handle_file"):
            tier_handler._check_tier(self._get_db_session, data)

        with self._get_db_session() as session:
            stmt = select(Recordings).where(
                Recordings.camera_identifier == mock_camera.identifier
            )
            recordings = session.execute(stmt).scalars().fetchall()
            assert len(recordings) == recordings_amount
            assert recordings[0].id == first_recording_id

    @pytest.mark.parametrize(
        (
            "tiers_config",
            "recording_id",
            "force_delete",
            "next_tier_index",
            "move_thumbnail_called",
            "move_event_clip_called",
        ),
        [
            (  # Test that check_tier deletes the file if next tier is None
                [_get_tier_config(events=True, continuous=True)],
                1,
                True,
                None,
                False,
                False,
            ),
            # Test that check_tier deletes the file if its not part of a recording and
            # next tier does not store continuous
            (
                [
                    _get_tier_config(events=True, continuous=True),
                    _get_tier_config(events=True, continuous=False),
                ],
                None,
                True,
                None,
                False,
                False,
            ),
            # Test that check_tier moves the file if its part of a recording and
            # the next tier stores events
            (
                [
                    _get_tier_config(events=True, continuous=True),
                    _get_tier_config(events=True, continuous=False),
                ],
                1,
                False,
                1,
                True,
                True,
            ),
            # Test that check_tier moves the file to the correct tier when the next tier
            # does not store events but the next next tier does
            (
                [
                    _get_tier_config(events=True, continuous=True),
                    _get_tier_config(events=False, continuous=False),
                    _get_tier_config(events=True, continuous=False),
                    _get_tier_config(events=False, continuous=True),
                ],
                1,
                False,
                2,
                True,
                True,
            ),
        ],
    )
    def test__check_tier_next_tier(
        self,
        vis: Viseron,
        tiers_config: list[dict[str, Any]],
        recording_id: int,
        force_delete: bool,
        next_tier_index: int | None,
        move_thumbnail_called: bool,
        move_event_clip_called: bool,
    ):
        """Test that check_tier finds the correct tier."""
        mock_camera = Mock()
        mock_camera.identifier = "test"
        mock_camera.config = {
            CONFIG_RECORDER: {
                CONFIG_LOOKBACK: 5,
                CONFIG_CONTINUOUS_RECORDING: True,
                CONFIG_SCHEDULE: None,
            }
        }

        tier_handlers = []
        for i, tier_config in enumerate(tiers_config):
            tier_handler = SegmentsTierHandler(
                vis,
                mock_camera,
                i,
                TIER_CATEGORY_RECORDER,
                TIER_SUBCATEGORY_SEGMENTS,
                tier_config,
                None,
            )
            tier_handlers.append(tier_handler)
        recordings_tier_handler = MagicMock(spec=EventClipTierHandler)
        thumbnail_tier_handler = MagicMock(spec=ThumbnailTierHandler)
        vis.data[STORAGE_COMPONENT].camera_tier_handlers = {  # type: ignore[misc]
            "test": {
                "recorder": [
                    {
                        "segments": tier_handler,
                        "thumbnails": thumbnail_tier_handler,
                        "event_clips": recordings_tier_handler,
                    }
                    for tier_handler in tier_handlers
                ]
            }
        }

        with (
            patch(
                "viseron.components.storage.tier_handler.handle_file"
            ) as mock_handle_file,
            patch.object(
                vis.background_scheduler,
                "add_job",
                side_effect=lambda func, **kwargs: func(*kwargs["args"]),
            ),
        ):
            data = np.array(
                [
                    (
                        recording_id if recording_id is not None else -1,
                        1,
                        "/tmp/test1.mp4",
                        "/tmp/",
                    )
                ],
                dtype=[
                    ("recording_id", np.int64),
                    ("id", np.int64),
                    ("path", "U512"),
                    ("tier_path", "U512"),
                ],
            )
            tier_handlers[0]._check_tier(self._get_db_session, data)
            mock_handle_file.assert_called_once_with(
                tier_handlers[0]._vis,
                self._get_db_session,
                tier_handlers[0]._storage,
                tier_handlers[0]._camera.identifier,
                tier_handlers[0].tier_id,
                TIER_CATEGORY_RECORDER,
                TIER_SUBCATEGORY_SEGMENTS,
                tier_handlers[0].tier,
                tier_handlers[next_tier_index].tier if next_tier_index else None,
                "/tmp/test1.mp4",
                "/tmp/",
                tier_handlers[0]._logger,
                force_delete=force_delete,
                on_complete=ANY,
                retention_recording_ids=(recording_id,)
                if recording_id is not None
                else (),
            )
            thumbnail_tier_handler.move_thumbnail.assert_not_called()
            recordings_tier_handler.move_event_clip.assert_not_called()
            completed = mock_handle_file.call_args.kwargs["on_complete"]
            if completed is not None:
                # Simulate the committed source handoff before acknowledging it.
                with self._get_db_session() as session:
                    source_rows = (
                        Files.camera_identifier == "test",
                        Files.tier_id == 0,
                        Files.category == TIER_CATEGORY_RECORDER,
                        Files.subcategory == TIER_SUBCATEGORY_SEGMENTS,
                    )
                    if next_tier_index is None:
                        session.execute(delete(Files).where(*source_rows))
                    else:
                        session.execute(
                            update(Files)
                            .where(*source_rows)
                            .values(tier_id=next_tier_index)
                        )
                    session.commit()
                completed()
            if move_thumbnail_called:
                thumbnail_tier_handler.move_thumbnail.assert_called_once_with(
                    1, tier_handlers[next_tier_index].tier if next_tier_index else None
                )
            if move_event_clip_called:
                recordings_tier_handler.move_event_clip.assert_called_once_with(
                    1, tier_handlers[next_tier_index].tier if next_tier_index else None
                )


class TestCheckTierCallbackLifecycle:
    """Test that every check_tier request is matched by exactly one release."""

    @pytest.fixture(autouse=True)
    def use_temporary_tier(self, tmp_path: Path) -> None:
        """Keep watcher directories in the test's writable temporary directory."""
        self._tier_path = str(tmp_path)

    def _make_tier_handler(self, vis: MockViseron) -> SegmentsTierHandler:
        mock_camera = Mock()
        mock_camera.identifier = "test"
        mock_camera.recorder.lookback = 5
        mock_camera.config = {
            CONFIG_RECORDER: {
                CONFIG_LOOKBACK: 5,
                CONFIG_CONTINUOUS_RECORDING: True,
                CONFIG_SCHEDULE: None,
            }
        }
        # _create_dataitem reads CONFIG_DRAIN, which _get_tier_config omits.
        tier_config = _get_tier_config(events=True, continuous=True)
        tier_config[CONFIG_DRAIN] = False
        tier_config[CONFIG_PATH] = self._tier_path

        tier_handler = SegmentsTierHandler(
            vis,
            mock_camera,
            0,
            "recorder",
            "segments",
            tier_config,
            None,
        )
        tier_handler._storage = Mock(spec=Storage)
        # No throttling, so only the in-flight guard can suppress a request.
        tier_handler._throttle_period = datetime.timedelta(0)
        tier_handler._time_of_last_call = utcnow() - datetime.timedelta(hours=1)
        return tier_handler

    def test_forced_check_bypasses_both_throttles(self, vis: MockViseron) -> None:
        """A recovered tier is checked even during its normal throttle period."""
        tier_handler = self._make_tier_handler(vis)
        tier_handler._throttle_period = datetime.timedelta(minutes=5)
        tier_handler._time_of_last_call = utcnow()
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)
        tier_handler.check_tier(force=True)
        assert send.call_count == 1
        assert send.call_args.args[0].throttle_period == datetime.timedelta(0)

    def test_check_tier_sends_once_while_in_flight(self, vis: MockViseron) -> None:
        """Only one request is sent until the reply releases the slot."""
        tier_handler = self._make_tier_handler(vis)
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)

        # Simulates the burst of file events seen during startup.
        for _ in range(50):
            tier_handler.check_tier()

        assert send.call_count == 1

    def test_forced_requests_during_check_are_coalesced(self, vis: MockViseron) -> None:
        """Pressure while busy triggers exactly one forced follow-up."""
        tier_handler = self._make_tier_handler(vis)
        tier_handler._throttle_period = datetime.timedelta(minutes=5)
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            assert tier_handler.check_tier(force=True)
            for _ in range(10):
                assert tier_handler.check_tier(force=True)
            assert send.call_count == 1
            tier_handler._release_check_tier()
            assert timer.call_count == 1
            timer.call_args.args[1]()
            assert send.call_count == 2
            assert send.call_args.args[0].throttle_period == datetime.timedelta(0)
            tier_handler._release_check_tier()
            assert timer.call_count == 1

    def test_forced_worker_deferral_is_retried(self, vis: MockViseron) -> None:
        """Another check for this camera can defer a forced worker job."""
        tier_handler = self._make_tier_handler(vis)
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            assert tier_handler.check_tier(force=True)
            item = send.call_args.args[0]
            item.deferred = True
            tier_handler.on_check_tier_result(item)
            timer.call_args.args[1]()
            assert send.call_count == 2

    def test_forced_check_retries_worker_capacity(self, vis: MockViseron) -> None:
        """A forced submission rejected at capacity is retried."""
        tier_handler = self._make_tier_handler(vis)
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)
        send.side_effect = [StorageCallbackCapacityError("full"), None]
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            assert not tier_handler.check_tier(force=True)
            timer.call_args.args[1]()
            assert send.call_count == 2
            assert tier_handler._tier_check_in_progress

    def test_forced_retry_is_cancelled_at_stop(self, vis: MockViseron) -> None:
        """Shutting down cancels a pending retry."""
        tier_handler = self._make_tier_handler(vis)
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            assert tier_handler.check_tier(force=True)
            assert tier_handler.check_tier(force=True)
            tier_handler._release_check_tier()
            tier_handler._stop_observer()
            timer.return_value.cancel.assert_called_once()
            timer.call_args.args[1]()
            assert not tier_handler.check_tier(force=True)

    def test_no_op_reply_releases_slot(self, vis: MockViseron) -> None:
        """A reply with no data releases the slot and advances the throttle."""
        tier_handler = self._make_tier_handler(vis)
        send = cast("Mock", tier_handler._storage.tier_check_worker_send_command)

        tier_handler.check_tier()
        assert send.call_count == 1

        item = tier_handler._create_dataitem()
        item.data = None
        before = tier_handler._time_of_last_call
        tier_handler.on_check_tier_result(item)

        # Slot released and throttle advanced even though data was None.
        assert tier_handler._tier_check_in_progress is False
        assert tier_handler._time_of_last_call > before

        # A later event can now send again.
        tier_handler.check_tier()
        assert send.call_count == 2

    def test_empty_result_releases_slot_without_thread(self, vis: MockViseron) -> None:
        """An empty array releases the slot without spawning a worker thread."""
        tier_handler = self._make_tier_handler(vis)

        tier_handler.check_tier()
        item = tier_handler._create_dataitem()
        item.data = np.empty(0, dtype=[("id", np.int64)])

        with patch(
            "viseron.components.storage.tier_handler.RestartableThread"
        ) as mock_thread:
            tier_handler.on_check_tier_result(item)

        mock_thread.assert_not_called()
        assert tier_handler._tier_check_in_progress is False

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_slot_released_when_check_tier_raises(self, vis: MockViseron) -> None:
        """The slot is released even if processing the payload raises.

        The RuntimeError is raised on purpose inside the worker thread, so the
        resulting unhandled-thread-exception warning is expected.
        """
        tier_handler = self._make_tier_handler(vis)
        tier_handler.check_tier()

        item = tier_handler._create_dataitem()
        item.data = np.array(
            [(1, 1, "/tmp/test1.mp4", "/tmp/")],
            dtype=[
                ("recording_id", np.int64),
                ("id", np.int64),
                ("path", "U512"),
                ("tier_path", "U512"),
            ],
        )

        with patch.object(
            tier_handler, "_check_tier", side_effect=RuntimeError("boom")
        ):
            tier_handler.on_check_tier_result(item)
            # on_check_tier_result offloads to a thread; wait for it to finish.
            for thread in threading.enumerate():
                if thread.name.startswith("storage.tier_handler.check_tier."):
                    thread.join(timeout=5)

        assert tier_handler._tier_check_in_progress is False

    def test_slot_released_if_send_raises(self, vis: MockViseron) -> None:
        """The slot is released if the command never leaves the process."""
        tier_handler = self._make_tier_handler(vis)
        cast(
            "Mock", tier_handler._storage.tier_check_worker_send_command
        ).side_effect = RuntimeError("queue gone")

        with pytest.raises(RuntimeError):
            tier_handler.check_tier()

        assert tier_handler._tier_check_in_progress is False


def test_find_next_tier_segments(vis: Viseron):
    """Test find_next_tier_segments."""
    mock_storage = Mock(spec=Storage)
    mock_camera = Mock()
    mock_camera.identifier = "test_camera"
    mock_camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 5,
            CONFIG_CONTINUOUS_RECORDING: True,
            CONFIG_SCHEDULE: None,
        }
    }

    tier_handler_0 = SegmentsTierHandler(
        vis,
        mock_camera,
        0,
        "recorder",
        "segments",
        _get_tier_config(events=True, continuous=True),
        None,
    )
    tier_handler_1 = SegmentsTierHandler(
        vis,
        mock_camera,
        1,
        "recorder",
        "segments",
        _get_tier_config(events=False, continuous=False),
        None,
    )
    tier_handler_2 = SegmentsTierHandler(
        vis,
        mock_camera,
        2,
        "recorder",
        "segments",
        _get_tier_config(events=True, continuous=False),
        None,
    )

    tier_handler_3 = SegmentsTierHandler(
        vis,
        mock_camera,
        3,
        "recorder",
        "segments",
        _get_tier_config(events=False, continuous=True),
        None,
    )

    mock_camera.identifier = "test_camera"
    mock_storage.camera_tier_handlers = {
        "test_camera": {
            "recorder": [
                {"segments": tier_handler_0},
                {"segments": tier_handler_1},
                {"segments": tier_handler_2},
                {"segments": tier_handler_3},
            ]
        }
    }

    result = find_next_tier_segments(mock_storage, 0, mock_camera, "events")
    assert result == tier_handler_2

    result = find_next_tier_segments(mock_storage, 0, mock_camera, "continuous")
    assert result == tier_handler_3

    result = find_next_tier_segments(mock_storage, 2, mock_camera, "events")
    assert result is None


@pytest.fixture
def no_tier_observer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid starting filesystem observers in configuration-only tests."""
    monkeypatch.setattr(TierHandler, "_start_observer", lambda _: None)


@pytest.mark.usefixtures("no_tier_observer")
def test_continuous_enabled_is_structural_only(vis: MockViseron) -> None:
    """continuous_enabled reflects config only, never live schedule state.

    The schedule is applied per-file inside the storage subprocess (see
    get_continuous_files_to_move), not by disabling the whole continuous
    retention path here.
    Disabling it here would route orphan continuous
    files through the events-only force_delete path and destroy already
    -retained footage the moment the schedule closes.
    """
    mock_camera = Mock()
    mock_camera.identifier = "test"
    mock_camera.recorder.lookback = 5
    mock_camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 5,
            CONFIG_CONTINUOUS_RECORDING: True,
            CONFIG_SCHEDULE: {
                CONFIG_SCHEDULE_CONTINUOUS: [
                    {"start": "0 22 * * *", "end": "0 6 * * *"}
                ],
                CONFIG_SCHEDULE_TIMEZONE: "UTC",
            },
        }
    }

    tier = _get_tier_config(events=True, continuous=True)
    tier[CONFIG_DRAIN] = False
    tier_handler = SegmentsTierHandler(
        vis,
        mock_camera,
        0,
        "recorder",
        "segments",
        tier,
        None,
    )

    assert tier_handler.continuous_enabled is True
    assert tier_handler._create_dataitem().files_enabled is True


@pytest.mark.usefixtures("no_tier_observer")
def test_create_dataitem_passes_continuous_schedule_entries(
    vis: MockViseron,
) -> None:
    """_create_dataitem forwards the configured continuous schedule and lookback."""
    mock_camera = Mock()
    mock_camera.identifier = "test"
    mock_camera.recorder.lookback = 7
    schedule_entries = [{"start": "0 22 * * *", "end": "0 6 * * *"}]
    mock_camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 7,
            CONFIG_CONTINUOUS_RECORDING: True,
            CONFIG_SCHEDULE: {
                CONFIG_SCHEDULE_CONTINUOUS: schedule_entries,
                CONFIG_SCHEDULE_TIMEZONE: "Europe/Stockholm",
            },
        }
    }

    tier = _get_tier_config(events=True, continuous=True)
    tier[CONFIG_DRAIN] = False
    tier_handler = SegmentsTierHandler(
        vis,
        mock_camera,
        0,
        "recorder",
        "segments",
        tier,
        None,
    )

    item = tier_handler._create_dataitem()
    assert item.continuous_schedule == schedule_entries
    assert item.continuous_schedule_timezone == "Europe/Stockholm"
    assert item.continuous_lookback_seconds == 7


@pytest.mark.usefixtures("no_tier_observer")
def test_create_dataitem_continuous_schedule_none_when_omitted(
    vis: MockViseron,
) -> None:
    """Omitting the schedule preserves the pre-existing always-on behavior."""
    mock_camera = Mock()
    mock_camera.identifier = "test"
    mock_camera.recorder.lookback = 5
    mock_camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 5,
            CONFIG_CONTINUOUS_RECORDING: True,
            CONFIG_SCHEDULE: None,
        }
    }

    tier = _get_tier_config(events=True, continuous=True)
    tier[CONFIG_DRAIN] = False
    tier_handler = SegmentsTierHandler(
        vis,
        mock_camera,
        0,
        "recorder",
        "segments",
        tier,
        None,
    )

    assert tier_handler.continuous_enabled is True
    item = tier_handler._create_dataitem()
    assert item.continuous_schedule is None
    assert item.continuous_schedule_timezone is None
    assert item.files_enabled is True


@pytest.mark.usefixtures("no_tier_observer")
def test_continuous_enabled_now_false_when_continuous_recording_disabled(
    vis: MockViseron,
) -> None:
    """continuous_recording: false still disables continuous regardless of schedule."""
    mock_camera = Mock()
    mock_camera.identifier = "test"
    mock_camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 5,
            CONFIG_CONTINUOUS_RECORDING: False,
            CONFIG_SCHEDULE: None,
        }
    }

    tier_handler = SegmentsTierHandler(
        vis,
        mock_camera,
        0,
        "recorder",
        "segments",
        _get_tier_config(events=True, continuous=True),
        None,
    )

    assert tier_handler.continuous_enabled is False


def _make_bare_tier_handler(
    handler_class: type[_TierHandlerT],
    vis: MockViseron,
    storage: Mock,
    tier_id: int,
    tier_path: str,
    category: str,
    subcategory: str,
) -> _TierHandlerT:
    """Build a tier handler without starting its watchdog observer."""
    tier_handler = handler_class.__new__(handler_class)
    tier_handler._logger = logging.getLogger(__name__)
    tier_handler._vis = vis
    tier_handler._storage = storage
    tier_handler._camera = Mock(identifier="test")
    tier_handler._tier_id = tier_id
    tier_handler._tier = {CONFIG_PATH: tier_path}
    tier_handler._category = category
    tier_handler._subcategory = subcategory
    tier_handler.check_tier = Mock()  # type: ignore[method-assign]
    return tier_handler


@pytest.fixture(name="db_storage")
def fixture_db_storage(get_db_session: sessionmaker[Session]) -> Mock:
    """Storage mock backed by the test database."""
    storage = Mock(spec=Storage)
    storage.get_session = get_db_session
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    return storage


def _create_file(path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as file:
        file.write(b"data")
    return path


def _file_keys(storage: Mock) -> dict[str, int]:
    with storage.get_session() as session:
        rows = session.execute(select(Files.path, Files.file_key)).all()
    return {row.path: row.file_key for row in rows}


def _two_tiers(
    handler_class: type[_TierHandlerT],
    vis: MockViseron,
    storage: Mock,
    tmp_path: Path,
    category: str,
    subcategory: str,
) -> tuple[_TierHandlerT, _TierHandlerT]:
    return (
        _make_bare_tier_handler(
            handler_class,
            vis,
            storage,
            0,
            os.path.join(tmp_path, "tier0"),
            category,
            subcategory,
        ),
        _make_bare_tier_handler(
            handler_class,
            vis,
            storage,
            1,
            os.path.join(tmp_path, "tier1"),
            category,
            subcategory,
        ),
    )


def _tier_file(tier_handler: TierHandler, filename: str) -> str:
    return os.path.join(
        tier_handler.tier[CONFIG_PATH],
        tier_handler._category,
        tier_handler._subcategory,
        "test",
        filename,
    )


def _start_move(
    vis: MockViseron,
    storage: Mock,
    src_handler: TierHandler,
    dst_handler: TierHandler,
    filename: str,
) -> tuple[str, str]:
    """Run move_file, then replay the copy and delete done by the subprocess."""
    src = _tier_file(src_handler, filename)
    dst = _tier_file(dst_handler, filename)
    move_file(
        vis,
        storage,
        storage.get_session,
        "test",
        src_handler.tier_id,
        src_handler._category,
        src_handler._subcategory,
        src,
        dst,
        dst_handler.tier[CONFIG_PATH],
        logging.getLogger(__name__),
    )
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy(src, dst)
    os.remove(src)
    return src, dst


def _simulate_move(
    vis: MockViseron,
    storage: Mock,
    src_handler: TierHandler,
    dst_handler: TierHandler,
    filename: str,
    observer_order: tuple[str, str],
) -> None:
    """Run a move and both observers.

    The source and destination observers drain separate event queues, so the
    order in which their callbacks run is not guaranteed.
    """
    src, dst = _start_move(vis, storage, src_handler, dst_handler, filename)
    observers = {
        "created": lambda: dst_handler._on_created(FileCreatedEvent(dst)),
        "deleted": lambda: src_handler._on_deleted(FileDeletedEvent(src)),
    }
    for observer in observer_order:
        observers[observer]()


OBSERVER_ORDERS = [
    pytest.param(("created", "deleted"), id="created_first"),
    pytest.param(("deleted", "created"), id="deleted_first"),
]


@pytest.mark.parametrize("observer_order", OBSERVER_ORDERS)
def test_file_key_survives_move(
    vis: MockViseron,
    db_storage: Mock,
    tmp_path: Path,
    observer_order: tuple[str, str],
) -> None:
    """Test that the destination row inherits the file_key of the source row."""
    tier0, tier1 = _two_tiers(
        TierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    src = _create_file(_tier_file(tier0, "1.m4s"))
    tier0._on_created(FileCreatedEvent(src))
    original_key = _file_keys(db_storage)[src]

    _simulate_move(vis, db_storage, tier0, tier1, "1.m4s", observer_order)

    assert _file_keys(db_storage) == {_tier_file(tier1, "1.m4s"): original_key}
    assert db_storage.temporary_files_meta == {}
    assert db_storage.pending_moves == {}


def test_file_key_resolvable_before_destination_is_observed(
    vis: MockViseron, db_storage: Mock, tmp_path: Path
) -> None:
    """Test that the key still resolves when the source row is deleted first.

    A polled destination tier can observe the new file seconds after the source
    tier observes the delete.
    """
    tier0, tier1 = _two_tiers(
        TierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    src = _create_file(_tier_file(tier0, "1.m4s"))
    tier0._on_created(FileCreatedEvent(src))
    original_key = _file_keys(db_storage)[src]

    src, dst = _start_move(vis, db_storage, tier0, tier1, "1.m4s")
    tier0._on_deleted(FileDeletedEvent(src))

    assert _file_keys(db_storage) == {dst: original_key}


@pytest.mark.parametrize("observer_order", OBSERVER_ORDERS)
def test_move_dispatches_file_created(
    vis: MockViseron,
    db_storage: Mock,
    tmp_path: Path,
    observer_order: tuple[str, str],
) -> None:
    """Test that the destination still announces the file once it is observed."""
    tier0, tier1 = _two_tiers(
        TierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    src = _create_file(_tier_file(tier0, "1.m4s"))
    tier0._on_created(FileCreatedEvent(src))
    vis.dispatch_event.reset_mock()

    _simulate_move(vis, db_storage, tier0, tier1, "1.m4s", observer_order)

    created_paths = [
        call.args[1].path
        for call in vis.dispatch_event.call_args_list
        if isinstance(call.args[1], EventFileCreated)
    ]
    assert created_paths == [_tier_file(tier1, "1.m4s")]


def _wait_for_lock_waiter(storage: Mock) -> None:
    """Block until a backend is waiting on a database lock."""
    stmt = text(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with storage.get_session() as session:
            if session.execute(stmt).scalar_one():
                return
        time.sleep(0.01)
    pytest.fail("No backend started waiting on a lock")


def test_hand_over_row_concurrent_with_destination_insert(
    vis: MockViseron, db_storage: Mock, tmp_path: Path
) -> None:
    """Test that the hand-over does not violate files_path_key.

    The destination observer can insert its row while the source observer hands
    over the source row. Postgres logs every unique violation, even caught ones.
    """
    tier0, tier1 = _two_tiers(
        TierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    src = _create_file(_tier_file(tier0, "1.m4s"))
    tier0._on_created(FileCreatedEvent(src))
    original_key = _file_keys(db_storage)[src]
    src, dst = _start_move(vis, db_storage, tier0, tier1, "1.m4s")

    db_errors: list[BaseException] = []

    def on_db_error(context: ExceptionContext) -> None:
        db_errors.append(context.original_exception)

    engine = db_storage.get_session.kw["bind"]
    event.listen(engine, "handle_error", on_db_error)

    inserted = threading.Event()
    release_commit = threading.Event()

    def get_gated_session() -> Session:
        session = db_storage.get_session()
        commit = session.commit

        def gated_commit() -> None:
            inserted.set()
            release_commit.wait(10)
            commit()

        session.commit = gated_commit  # type: ignore[method-assign]
        return session

    dst_storage = Mock(spec=Storage)
    dst_storage.get_session = get_gated_session
    dst_storage.temporary_files_meta = db_storage.temporary_files_meta
    dst_storage.pending_moves = db_storage.pending_moves
    dst_storage.pending_moves_lock = db_storage.pending_moves_lock
    tier1._storage = dst_storage

    created = threading.Thread(target=tier1._on_created, args=(FileCreatedEvent(dst),))
    deleted = threading.Thread(target=tier0._on_deleted, args=(FileDeletedEvent(src),))
    try:
        created.start()
        assert inserted.wait(10)
        deleted.start()
        _wait_for_lock_waiter(db_storage)
    finally:
        release_commit.set()
        created.join(10)
        deleted.join(10)
        event.remove(engine, "handle_error", on_db_error)

    assert not db_errors
    assert _file_keys(db_storage) == {dst: original_key}


def test_failed_move_does_not_hand_over_row(
    vis: MockViseron, db_storage: Mock, tmp_path: Path
) -> None:
    """Test that the row is not moved to the destination after a failed move."""
    tier0, tier1 = _two_tiers(
        TierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    src = _create_file(_tier_file(tier0, "1.m4s"))
    tier0._on_created(FileCreatedEvent(src))
    dst = _tier_file(tier1, "1.m4s")

    move_file(
        vis,
        db_storage,
        db_storage.get_session,
        "test",
        0,
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
        src,
        dst,
        tier1.tier[CONFIG_PATH],
        logging.getLogger(__name__),
    )
    callback = db_storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    callback(DataItemMoveFile(cmd="move_file", src=src, dst=dst, error="boom"))
    # The source is later removed by something else, such as retention
    os.remove(src)
    tier0._on_deleted(FileDeletedEvent(src))

    assert not _file_keys(db_storage)


def test_new_files_get_distinct_file_keys(
    vis: MockViseron, db_storage: Mock, tmp_path: Path
) -> None:
    """Test that new files get fresh keys, including those with fragmenter meta."""
    tier0 = _make_bare_tier_handler(
        TierHandler,
        vis,
        db_storage,
        0,
        os.path.join(tmp_path, "tier0"),
        TIER_CATEGORY_RECORDER,
        TIER_SUBCATEGORY_SEGMENTS,
    )
    with_meta = _create_file(_tier_file(tier0, "1.m4s"))
    without_meta = _create_file(_tier_file(tier0, "2.m4s"))
    db_storage.temporary_files_meta[with_meta] = FilesMeta(
        orig_ctime=utcnow(), duration=5.0
    )

    tier0._on_created(FileCreatedEvent(with_meta))
    tier0._on_created(FileCreatedEvent(without_meta))

    keys = _file_keys(db_storage)
    assert set(keys) == {with_meta, without_meta}
    assert keys[with_meta] != keys[without_meta]


SNAPSHOT_REFERENCES = [
    pytest.param(
        TIER_SUBCATEGORY_MOTION_DETECTOR,
        Motion,
        {"start_time": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)},
        id="motion_detector",
    ),
    pytest.param(
        TIER_SUBCATEGORY_OBJECT_DETECTOR,
        Objects,
        {
            "label": "person",
            "confidence": 0.9,
            "width": 1.0,
            "height": 1.0,
            "x1": 0.0,
            "y1": 0.0,
            "x2": 1.0,
            "y2": 1.0,
        },
        id="object_detector",
    ),
    pytest.param(
        TIER_SUBCATEGORY_FACE_RECOGNITION,
        PostProcessorResults,
        {"domain": TIER_SUBCATEGORY_FACE_RECOGNITION, "data": {}},
        id="face_recognition",
    ),
    pytest.param(
        TIER_SUBCATEGORY_IMAGE_CLASSIFICATION,
        PostProcessorResults,
        {"domain": TIER_SUBCATEGORY_IMAGE_CLASSIFICATION, "data": {}},
        id="image_classification",
    ),
    pytest.param(
        TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION,
        PostProcessorResults,
        {"domain": TIER_SUBCATEGORY_LICENSE_PLATE_RECOGNITION, "data": {}},
        id="license_plate_recognition",
    ),
]

SnapshotModel = type[Motion] | type[Objects] | type[PostProcessorResults]


def _create_snapshot(
    tier_handler: SnapshotTierHandler,
    model: SnapshotModel,
    values: dict[str, Any],
) -> str:
    """Create a snapshot file with a Files row and a detector row referencing it."""
    path = _create_file(_tier_file(tier_handler, "1.jpg"))
    tier_handler._on_created(FileCreatedEvent(path))
    with tier_handler._storage.get_session() as session:
        session.execute(
            insert(model).values(camera_identifier="test", snapshot_path=path, **values)
        )
        session.commit()
    return path


def _snapshot_paths(storage: Mock, model: SnapshotModel) -> list[str]:
    with storage.get_session() as session:
        return list(session.execute(select(model.snapshot_path)).scalars())


def test_snapshot_reference_models_cover_all_snapshot_subcategories() -> None:
    """Test that every snapshot subcategory has a model to re-point on a move."""
    assert set(SNAPSHOT_REFERENCE_MODELS) == {
        subcategory["subcategory"]
        for subcategory in TIER_CATEGORIES[TIER_CATEGORY_SNAPSHOTS]
    }


@pytest.mark.parametrize("observer_order", OBSERVER_ORDERS)
@pytest.mark.parametrize(("subcategory", "model", "values"), SNAPSHOT_REFERENCES)
def test_snapshot_references_follow_move(
    vis: MockViseron,
    db_storage: Mock,
    tmp_path: Path,
    subcategory: str,
    model: SnapshotModel,
    values: dict[str, Any],
    observer_order: tuple[str, str],
) -> None:
    """Test that detector rows are re-pointed, not deleted, when a snapshot moves."""
    tier0, tier1 = _two_tiers(
        SnapshotTierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_SNAPSHOTS,
        subcategory,
    )
    _create_snapshot(tier0, model, values)

    _simulate_move(vis, db_storage, tier0, tier1, "1.jpg", observer_order)

    assert _snapshot_paths(db_storage, model) == [_tier_file(tier1, "1.jpg")]


@pytest.mark.parametrize(("subcategory", "model", "values"), SNAPSHOT_REFERENCES)
def test_snapshot_references_deleted_with_snapshot(
    vis: MockViseron,
    db_storage: Mock,
    tmp_path: Path,
    subcategory: str,
    model: SnapshotModel,
    values: dict[str, Any],
) -> None:
    """Test that detector rows are deleted when their snapshot leaves the last tier."""
    tier0 = _make_bare_tier_handler(
        SnapshotTierHandler,
        vis,
        db_storage,
        0,
        os.path.join(tmp_path, "tier0"),
        TIER_CATEGORY_SNAPSHOTS,
        subcategory,
    )
    path = _create_snapshot(tier0, model, values)

    os.remove(path)
    tier0._on_deleted(FileDeletedEvent(path))

    assert not _snapshot_paths(db_storage, model)


@pytest.mark.parametrize(("subcategory", "model", "values"), SNAPSHOT_REFERENCES)
def test_snapshot_references_untouched_on_move_failure(
    vis: MockViseron,
    db_storage: Mock,
    tmp_path: Path,
    subcategory: str,
    model: SnapshotModel,
    values: dict[str, Any],
) -> None:
    """Test that a failed move leaves detector rows pointing at the source."""
    tier0, tier1 = _two_tiers(
        SnapshotTierHandler,
        vis,
        db_storage,
        tmp_path,
        TIER_CATEGORY_SNAPSHOTS,
        subcategory,
    )
    src = _create_snapshot(tier0, model, values)
    dst = _tier_file(tier1, "1.jpg")

    move_file(
        vis,
        db_storage,
        db_storage.get_session,
        "test",
        0,
        TIER_CATEGORY_SNAPSHOTS,
        subcategory,
        src,
        dst,
        tier1.tier[CONFIG_PATH],
        logging.getLogger(__name__),
    )
    assert _snapshot_paths(db_storage, model) == [src]

    callback = db_storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    callback(DataItemMoveFile(cmd="move_file", src=src, dst=dst, error="boom"))

    assert _snapshot_paths(db_storage, model) == [src]


def test_rejected_move_clears_pending_metadata(
    vis: MockViseron, tmp_path: Path
) -> None:
    """A move that was never queued must not leave phantom pending state."""
    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(orig_ctime=utcnow(), duration=1.0, file_key=123)
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.return_value = row
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    storage.tier_check_worker_send_command.side_effect = RuntimeError(
        "callback capacity"
    )
    with pytest.raises(RuntimeError, match="callback capacity"):
        move_file(
            vis,
            storage,
            lambda: session,
            "test",
            0,
            "recorder",
            "segments",
            src,
            dst,
            str(tmp_path),
            logging.getLogger(__name__),
        )
    assert storage.temporary_files_meta == {}
    assert storage.pending_moves == {}


def test_full_worker_releases_tier_check_slot() -> None:
    """A deferred check can be scheduled after capacity becomes available."""
    handler = TierHandler.__new__(TierHandler)
    handler._check_tier_lock = threading.Lock()
    handler._tier_check_in_progress = False
    handler._tier_check_force_pending = False
    handler._tier_check_retry_timer = None
    handler._tier_check_stopping = False
    handler._time_of_last_call = utcnow()
    handler._throttle_period = datetime.timedelta(0)
    handler._logger = logging.getLogger(__name__)
    handler._storage = Mock()
    handler._storage.tier_check_worker_send_command.side_effect = (
        StorageCallbackCapacityError("full")
    )
    handler._create_dataitem = Mock()  # type: ignore[method-assign]
    assert handler.check_tier(force=True) is False
    assert handler._tier_check_in_progress is False
    handler._storage.tier_check_worker_send_command.side_effect = None
    assert handler.check_tier(force=True) is True


def test_missing_move_metadata_does_not_schedule_deletion_or_move(
    vis: MockViseron, tmp_path: Path
) -> None:
    """Missing DB metadata must not cause the only on-disk copy to be deleted."""
    src = tmp_path / "source.m4s"
    src.write_bytes(b"only recording")
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.side_effect = (
        NoResultFound()
    )
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    move_file(
        vis,
        storage,
        lambda: session,
        "test",
        0,
        "recorder",
        "segments",
        str(src),
        str(tmp_path / "dest.m4s"),
        str(tmp_path),
        logging.getLogger(__name__),
    )
    storage.tier_check_worker_send_command.assert_not_called()
    assert src.read_bytes() == b"only recording"
    assert not storage.pending_moves


@pytest.mark.parametrize(
    ("source_present", "destination_present", "destination_indexed", "warn"),
    [
        (False, True, True, False),
        (True, True, True, True),
        (False, False, True, True),
        (False, True, False, True),
        (False, True, "wrong-camera", True),
    ],
)
def test_stale_move_selection_requires_an_indexed_destination(
    vis, get_db_session, tmp_path, caplog,
    source_present, destination_present, destination_indexed, warn,
):
    """A completed move is quiet; uncertain or unindexed data still warns."""
    src = tmp_path / "local" / "run" / "one.m4s"
    dst = tmp_path / "nas" / "run" / "one.m4s"
    src.parent.mkdir(parents=True)
    dst.parent.mkdir(parents=True)
    if source_present:
        src.write_bytes(b"source footage")
    if destination_present:
        dst.write_bytes(b"destination footage")
    if destination_indexed:
        with get_db_session() as session:
            record = Files(
                tier_id=1, tier_path=str(tmp_path / "nas"),
                camera_identifier=(
                    "other_camera" if destination_indexed == "wrong-camera" else "test"
                ), category="recorder", subcategory="segments",
                path=str(dst), directory=str(dst.parent), filename=dst.name,
                size=19, duration=5.0, orig_ctime=utcnow(),
            )
            session.add(record)
            session.commit()
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    with caplog.at_level(logging.WARNING):
        move_file(
            vis, storage, get_db_session, "test", 0, "recorder", "segments",
            str(src), str(dst), str(tmp_path / "nas"), logging.getLogger(__name__),
        )
    assert ("source metadata is missing" in caplog.text) is warn
    assert src.exists() is source_present
    assert dst.exists() is destination_present
    storage.tier_check_worker_send_command.assert_not_called()


def test_tier_selection_can_outlive_a_completed_move(vis, get_db_session, tmp_path, caplog):
    """A batch selected before promotion can be processed after it commits."""
    local, nas = tmp_path / "local", tmp_path / "nas"
    src, dst = local / "run" / "one.m4s", nas / "run" / "one.m4s"
    src.parent.mkdir(parents=True)
    dst.parent.mkdir(parents=True)
    src.write_bytes(b"recording")
    with get_db_session() as session:
        row = Files(
            tier_id=0, tier_path=str(local), camera_identifier="test",
            category="recorder", subcategory="segments", path=str(src),
            directory=str(src.parent), filename=src.name, size=9, duration=5.0,
            orig_ctime=utcnow(),
        )
        session.add(row)
        session.commit()
        selected = np.array(
            [(row.path, row.tier_path)], dtype=[("path", "U512"), ("tier_path", "U512")]
        )
        src.rename(dst)
        row.path, row.directory, row.tier_path, row.tier_id = (
            str(dst), str(dst.parent), str(nas), 1
        )
        session.commit()

    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    storage.file_batch_size = 100
    storage.tier_available.return_value = True
    storage.camera_requested_files_count = {"test": Mock(filenames=set())}
    handler = TierHandler.__new__(TierHandler)
    handler._vis, handler._storage = vis, storage
    handler._camera = Mock(identifier="test")
    handler._tier_id, handler._category, handler._subcategory = 0, "recorder", "segments"
    handler._tier, handler._next_tier = {"path": str(local)}, {"path": str(nas)}
    handler._logger = logging.getLogger(__name__)
    with caplog.at_level(logging.WARNING):
        handler._check_tier(get_db_session, selected)
    assert "source metadata is missing" not in caplog.text
    assert dst.read_bytes() == b"recording"
    storage.tier_check_worker_send_command.assert_not_called()


def test_deferred_move_clears_own_state_and_schedules_source_retry(
    vis: MockViseron, tmp_path: Path
) -> None:
    """Contended moves retry from their source tier without losing replacements."""
    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(
        orig_ctime=utcnow(), duration=1.0, file_key=123, id=5, tier_id=0, tier_path="/"
    )
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.return_value = row
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    handler = Mock()
    handler._check_tier_lock = threading.Lock()
    storage.camera_tier_handlers = {"test": {"recorder": [{"segments": handler}]}}
    move_file(
        vis, storage, lambda: session, "test", 0, "recorder", "segments",
        src, dst, str(tmp_path), logging.getLogger(__name__),
    )
    callback = storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    replacement = object()
    storage.pending_moves[src] = replacement
    storage.temporary_files_meta[dst] = replacement
    callback(DataItemMoveFile(cmd="move_file", src=src, dst=dst, deferred=True))
    assert storage.pending_moves[src] is replacement
    assert storage.temporary_files_meta[dst] is replacement
    handler._schedule_forced_tier_retry_locked.assert_called_once()


def test_duplicate_move_is_not_dispatched(vis: MockViseron, tmp_path: Path) -> None:
    """Concurrent checks must not overwrite an existing move or submit another."""
    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(orig_ctime=utcnow(), duration=1.0, file_key=123, id=5, tier_id=0, tier_path="/")
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.return_value = row
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    args = (vis, storage, lambda: session, "test", 0, "recorder", "segments", src, dst, str(tmp_path), logging.getLogger(__name__))
    move_file(*args)
    pending = storage.pending_moves[src]
    meta = storage.temporary_files_meta[dst]
    move_file(*args)
    storage.tier_check_worker_send_command.assert_called_once()
    assert storage.pending_moves[src] is pending
    assert storage.temporary_files_meta[dst] is meta


def test_contended_deletion_retries_and_stop_cancels(tmp_path: Path, get_db_session) -> None:
    """A busy lock keeps the deletion event queued, with shutdown fencing it."""
    from queue import Queue
    from viseron.components.storage.run_lifecycle import camera_lock

    handler = TierHandler.__new__(TierHandler)
    handler._logger = logging.getLogger(__name__)
    handler._event_queue = Queue()
    handler._check_tier_lock = threading.Lock()
    handler._tier_check_stopping = False
    handler._deletion_retry_stop = threading.Event()
    handler._on_deleted_impl = Mock()
    handler._storage = Mock()
    handler._storage.get_session = get_db_session
    camera_dir = tmp_path / "camera"
    run_dir = camera_dir / "20261031T175537.057658Z-481707b6048c"
    run_dir.mkdir(parents=True)
    event = FileDeletedEvent(str(run_dir / "000000000.m4s"))
    with camera_lock("/tmp/viseron-run-locks", str(camera_dir)):
        handler._on_deleted(event)
        assert handler._event_queue.qsize() == 1
        handler._tier_check_stopping = True
        handler._deletion_retry_stop.set()
        handler._on_deleted(event)
        assert handler._event_queue.qsize() == 1
    handler._tier_check_stopping = False
    handler._on_deleted(handler._event_queue.get_nowait())
    handler._on_deleted_impl.assert_called_once_with(event)


def test_concurrent_duplicate_moves_send_one_command(
    vis: MockViseron, tmp_path: Path
) -> None:
    """Two tier scans racing on one source reserve just one move."""
    from concurrent.futures import ThreadPoolExecutor

    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(
        orig_ctime=utcnow(), duration=1.0, file_key=123, id=5, tier_id=0, tier_path="/"
    )
    barrier = threading.Barrier(2)

    def scalar_one():
        barrier.wait(timeout=5)
        return row

    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.side_effect = (
        scalar_one
    )
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    args = (
        vis,
        storage,
        lambda: session,
        "test",
        0,
        "recorder",
        "segments",
        src,
        dst,
        str(tmp_path),
        logging.getLogger(__name__),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(move_file, *args) for _ in range(2)]
        for future in futures:
            future.result(timeout=10)
    storage.tier_check_worker_send_command.assert_called_once()


def test_contended_retention_deletion_schedules_source_check(vis: MockViseron) -> None:
    """A locked retention target is reconsidered rather than silently lost."""
    from viseron.components.storage.storage_subprocess import DataItemDeleteFile

    storage = MagicMock()
    handler = Mock()
    handler._check_tier_lock = threading.Lock()
    storage.camera_tier_handlers = {"test": {"recorder": [{"segments": handler}]}}
    handle_file(
        vis,
        MagicMock(),
        storage,
        "test",
        0,
        "recorder",
        "segments",
        {"path": "/tier1"},
        None,
        "/tier1/old.m4s",
        "/tier1",
        MagicMock(),
    )
    callback = storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    callback(DataItemDeleteFile(cmd="delete_file", src="/tier1/old.m4s", deferred=True))
    handler._schedule_forced_tier_retry_locked.assert_called_once()


def test_durable_hold_deferral_does_not_schedule_scan(
    vis: MockViseron, tmp_path: Path
) -> None:
    """The intent reconciler owns held moves; don't spin tier scans."""
    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(
        orig_ctime=utcnow(), duration=1.0, file_key=123, id=5, tier_id=0, tier_path="/"
    )
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.return_value = row
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    handler = Mock()
    handler._check_tier_lock = threading.Lock()
    storage.camera_tier_handlers = {"test": {"recorder": [{"segments": handler}]}}
    move_file(
        vis,
        storage,
        lambda: session,
        "test",
        0,
        "recorder",
        "segments",
        src,
        dst,
        str(tmp_path),
        logging.getLogger(__name__),
    )
    callback = storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    callback(
        DataItemMoveFile(
            cmd="move_file", src=src, dst=dst, deferred=True, deferred_by_hold=True
        )
    )
    handler._schedule_forced_tier_retry_locked.assert_not_called()
    assert not storage.pending_moves
    assert not storage.temporary_files_meta


def test_deferred_cleanup_cannot_remove_new_move_metadata(
    vis: MockViseron, tmp_path: Path
) -> None:
    """A replacement reservation cannot interleave with deferred cleanup."""
    from concurrent.futures import ThreadPoolExecutor

    entered, release = threading.Event(), threading.Event()

    class BlockingDict(dict):
        def get(self, key, default=None):
            entered.set()
            assert release.wait(5)
            return super().get(key, default)

    src, dst = str(tmp_path / "source"), str(tmp_path / "destination")
    row = Mock(
        orig_ctime=utcnow(), duration=1.0, file_key=123, id=5, tier_id=0, tier_path="/"
    )
    session = MagicMock()
    session.__enter__.return_value.execute.return_value.scalar_one.return_value = row
    storage = Mock(spec=Storage)
    storage.temporary_files_meta = {}
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    handler = Mock()
    handler._check_tier_lock = threading.Lock()
    storage.camera_tier_handlers = {"test": {"recorder": [{"segments": handler}]}}
    args = (
        vis,
        storage,
        lambda: session,
        "test",
        0,
        "recorder",
        "segments",
        src,
        dst,
        str(tmp_path),
        logging.getLogger(__name__),
    )
    move_file(*args)
    callback = storage.tier_check_worker_send_command.call_args.kwargs["callback"]
    storage.temporary_files_meta = BlockingDict(storage.temporary_files_meta)
    with ThreadPoolExecutor(max_workers=2) as executor:
        cleanup = executor.submit(
            callback, DataItemMoveFile(cmd="move_file", src=src, dst=dst, deferred=True)
        )
        assert entered.wait(5)
        replacement = executor.submit(move_file, *args)
        assert not replacement.done()
        release.set()
        cleanup.result(timeout=5)
        replacement.result(timeout=5)
    assert src in storage.pending_moves
    assert dst in storage.temporary_files_meta


@pytest.mark.parametrize("maximum", ["80%", 4096, None])
def test_last_tier_warning_honors_shared_size_budget(vis, tmp_path, caplog, maximum):
    """Size-only NAS retention must not be advertised as unlimited storage."""
    camera = Mock()
    camera.identifier = "test"
    camera.config = {
        CONFIG_RECORDER: {
            CONFIG_LOOKBACK: 5,
            CONFIG_CONTINUOUS_RECORDING: True,
            CONFIG_SCHEDULE: None,
        }
    }
    tier = _get_tier_config(events=False, continuous=False)
    tier[CONFIG_PATH] = str(tmp_path)
    if maximum is not None:
        tier["retention"] = {"max_size": maximum}
    with caplog.at_level(logging.WARNING):
        SegmentsTierHandler(vis, camera, 0, "recorder", "segments", tier, None)
    warnings = [record.message for record in caplog.records]
    assert any("accumulate indefinitely" in message for message in warnings) == (
        maximum is None
    )
