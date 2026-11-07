"""Tests for viseron.components.storage.jobs."""

from __future__ import annotations

import contextlib
import errno
import logging
import os
import pickle
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from apscheduler.triggers.cron import CronTrigger

from viseron.components.storage.jobs import (
    CleanupJobConfig,
    EmptyFoldersCleanup,
    OrphanedDatabaseFilesCleanup,
    OrphanedEventClipsCleanup,
    OrphanedFilesCleanup,
    OrphanedRecordingsCleanup,
    OrphanedThumbnailsCleanup,
)
from viseron.components.storage.models import Files, Recordings
from viseron.exceptions import DomainNotRegisteredError
from viseron.viseron_types import SnapshotDomain

if TYPE_CHECKING:
    from collections.abc import Iterator

    from viseron.components.storage.jobs import BaseCleanupJob

ALL_PATHS = [
    "/clips",
    "/segments",
    "/thumbnails",
    "/timelapse",
    *[f"/snapshots/{domain.value}" for domain in SnapshotDomain],
]


def test_orphan_recording_cleanup_keeps_preroll(get_db_session, monkeypatch) -> None:
    """A recording with only its pre-roll indexed still has video."""
    now = datetime.now(UTC).replace(microsecond=0)
    with get_db_session() as session:
        recording = Recordings(
            camera_identifier="camera",
            adjusted_start_time=now - timedelta(seconds=20),
            start_time=now + timedelta(seconds=5),
            end_time=now + timedelta(seconds=10),
        )
        session.add(recording)
        session.add(
            Files(
                tier_id=0,
                tier_path="/tier",
                camera_identifier="camera",
                category="recorder",
                subcategory="segments",
                path="/tier/preroll.m4s",
                directory="/tier",
                filename="preroll.m4s",
                size=1,
                duration=1,
                orig_ctime=now,
            )
        )
        session.commit()
        recording_id = recording.id

    runner = SimpleNamespace(
        name="test",
        get_session=get_db_session,
        kill_event=threading.Event(),
        log_progress=MagicMock(),
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    OrphanedRecordingsCleanup.work(runner)
    with get_db_session() as session:
        assert session.get(Recordings, recording_id) is not None


def _storage() -> MagicMock:
    """Return a storage that resolves one path per kind of file."""
    storage = MagicMock()
    storage.ignored_files = ["init.mp4"]
    storage.get_event_clips_path.return_value = ["/clips"]
    storage.get_segments_path.return_value = ["/segments"]
    storage.get_thumbnails_path.return_value = ["/thumbnails"]
    storage.get_timelapse_path.return_value = ["/timelapse"]
    storage.get_snapshots_path.side_effect = lambda _camera, domain, **_kwargs: [
        f"/snapshots/{domain.value}"
    ]
    return storage


def _vis() -> MagicMock:
    """Return a Viseron instance with a single camera."""
    vis = MagicMock()
    vis.get_registered_identifiers.return_value = {"camera": MagicMock()}
    return vis


@pytest.fixture(name="storage_dir")
def fixture_storage_dir() -> Iterator[str]:
    """Return a temporary storage directory."""
    with tempfile.TemporaryDirectory() as storage_dir:
        yield storage_dir


def test_config_is_picklable() -> None:
    """The config crosses a forkserver boundary, so it must pickle."""
    config = CleanupJobConfig(
        name="cleanup_test",
        paths=["/clips"],
        ignored_files=frozenset({"init.mp4"}),
        sensitive_strings=("hunter2",),
        log_level=logging.INFO,
    )
    assert pickle.loads(pickle.dumps(config)) == config  # noqa: S301


@pytest.mark.parametrize(
    ("job_class", "expected_paths"),
    [
        pytest.param(OrphanedFilesCleanup, ALL_PATHS, id="orphaned_files"),
        pytest.param(EmptyFoldersCleanup, ALL_PATHS, id="empty_folders"),
        pytest.param(
            OrphanedThumbnailsCleanup, ["/thumbnails"], id="orphaned_thumbnails"
        ),
        pytest.param(OrphanedEventClipsCleanup, ["/clips"], id="orphaned_clips"),
        pytest.param(OrphanedDatabaseFilesCleanup, [], id="orphaned_db_files"),
    ],
)
def test_paths_are_resolved_in_the_parent(
    job_class: type[BaseCleanupJob], expected_paths: list[str]
) -> None:
    """The child can't reach the cameras, so the parent resolves their paths."""
    job = job_class(_vis(), _storage(), CronTrigger(hour=0))
    assert job._get_paths() == expected_paths


def test_no_paths_without_cameras() -> None:
    """Jobs must not fail before any camera has been registered."""
    vis = MagicMock()
    vis.get_registered_identifiers.side_effect = DomainNotRegisteredError("camera")
    job = OrphanedFilesCleanup(vis, _storage(), CronTrigger(hour=0))
    assert job._get_paths() == []


def test_run_executes_the_job_in_a_child_process(storage_dir: str) -> None:
    """The empty folders job needs no database, so the whole round trip can run."""
    empty_folder = os.path.join(storage_dir, "empty")
    os.makedirs(empty_folder)
    kept_folder = os.path.join(storage_dir, "kept")
    os.makedirs(kept_folder)
    with open(os.path.join(kept_folder, "file"), "w", encoding="utf-8") as file:
        file.write("content")
    storage = _storage()
    storage.get_event_clips_path.return_value = [storage_dir]
    storage.get_segments_path.return_value = []
    storage.get_thumbnails_path.return_value = []
    storage.get_timelapse_path.return_value = None
    storage.get_snapshots_path.side_effect = None
    storage.get_snapshots_path.return_value = []
    job = EmptyFoldersCleanup(_vis(), storage, CronTrigger(hour=0))

    job.run()

    assert not os.path.exists(empty_folder)
    assert os.path.exists(kept_folder)
    assert job.running is False


@pytest.mark.parametrize("busy", [True, False], ids=["camera_busy", "permission_error"])
def test_orphan_cleanup_defers_busy_camera_without_hiding_io_errors(
    tmp_path, monkeypatch, caplog, busy
) -> None:
    """Contention defers a camera once; actual I/O failures remain warnings."""
    from viseron.components.storage import jobs  # noqa: PLC0415
    from viseron.components.storage.run_lifecycle import camera_lock  # noqa: PLC0415

    cameras = [tmp_path / name for name in ("busy_camera", "other_camera")]
    files = []
    for camera, count in zip(cameras, (3, 1)):
        run = camera / "20261107T010000.000000Z-0123456789ab"
        run.mkdir(parents=True)
        for index in range(count):
            if index == 2:
                run = camera / "20261107T010001.000000Z-0123456789ab"
                run.mkdir()
            file = run / f"{index}.m4s"
            file.write_bytes(b"test segment")
            files.append(file)
    lock_root = str(tmp_path / "locks")
    attempts = []

    @contextlib.contextmanager
    def acquire(_root, camera):
        attempts.append(camera)
        if not busy and camera == str(cameras[0]):
            raise PermissionError(errno.EACCES, "permission denied")
        with camera_lock(lock_root, camera):
            yield

    session = MagicMock()
    session.execute.return_value.first.return_value = None
    runner = SimpleNamespace(
        name="test",
        paths=[str(camera) for camera in cameras],
        tier_markers={},
        ignored_files=frozenset(),
        kill_event=threading.Event(),
        get_session=lambda: contextlib.nullcontext(session),
        log_progress=MagicMock(),
    )
    monkeypatch.setattr(jobs, "camera_lock", acquire)
    monkeypatch.setattr(jobs, "tier_markers_available", lambda *_args: True)
    monkeypatch.setattr(jobs, "segment_move_pending", lambda *_args: False)
    with camera_lock(lock_root, str(cameras[0])):
        OrphanedFilesCleanup.work(runner)
    assert all(file.exists() for file in files[:3])
    assert not files[-1].exists()
    warnings = [
        record for record in caplog.records if record.levelno >= logging.WARNING
    ]
    if busy:
        assert not warnings
        assert attempts.count(str(cameras[0])) == 1
        # Deferral lasts only for this pass; a later pass can reclaim the files.
        OrphanedFilesCleanup.work(runner)
        assert not any(file.exists() for file in files)
    else:
        assert len(warnings) == 3
        assert all(
            isinstance(record.exc_info[1], PermissionError) for record in warnings
        )
