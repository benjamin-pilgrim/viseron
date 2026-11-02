"""Restart recovery must distinguish retired footage from unindexed media."""

import logging
import os
import pickle
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from apscheduler.triggers.cron import CronTrigger

from viseron.components.storage.check_tier import delete_file
from viseron.components.storage.jobs import CleanupJobConfig, OrphanedRecordingsCleanup
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.run_lifecycle import camera_lock

RUN = "20261101T120000.000000Z-012345abcdef"


@pytest.mark.parametrize(
    "location",
    [
        "tier",
        "spool",
        "pending",
        "writer",
        "unknown",
        "symlink",
        "offline",
        "missing-root",
        "legacy",
        "indexed-preroll",
        "empty",
    ],
)
def test_orphan_recovery_checks_disk_after_lost_callback(
    tmp_path, get_db_session, monkeypatch, location
):
    """Only an empty run can recover retirement after source rows were committed."""
    tier = tmp_path / "segments" / "camera"
    spool = tmp_path / "segments" / ".viseron-spool" / "camera"
    for root in (tier, spool):
        (root / RUN).mkdir(parents=True)
        (root / RUN / "init.mp4").write_bytes(b"init")
    if location == "tier":
        (tier / RUN / "lost.m4s").write_bytes(b"unindexed footage")
    elif location == "spool":
        (spool / RUN / "lost.m4s").write_bytes(b"staged footage")
    elif location == "pending":
        (tier / RUN / ".viseron-tier-move-pending-lost.m4s.json").write_text("{}")
    elif location == "writer":
        (spool / RUN / ".viseron-writer-lease").touch()
    elif location == "unknown":
        (tier / RUN / "unknown").touch()
    elif location == "symlink":
        (tier / RUN / "init.mp4").unlink()
        (tier / RUN / "init.mp4").symlink_to(spool / RUN / "init.mp4")

    elif location == "missing-root":
        (spool / RUN / "init.mp4").unlink()
        (spool / RUN).rmdir()
        spool.rmdir()

    now = datetime.now(UTC)
    with get_db_session() as session:
        recording = Recordings(
            camera_identifier="camera",
            recorder_run_id=None if location == "legacy" else RUN,
            adjusted_start_time=now,
            start_time=now,
            end_time=now + timedelta(seconds=10),
        )
        session.add(recording)
        if location == "indexed-preroll":
            session.add(
                Files(
                    tier_id=0,
                    tier_path=str(tmp_path),
                    camera_identifier="camera",
                    category="recorder",
                    subcategory="segments",
                    path=str(tier / RUN / "preroll.m4s"),
                    directory=str(tier / RUN),
                    filename="preroll.m4s",
                    size=1,
                    orig_ctime=now - timedelta(seconds=0.5),
                    duration=1,
                )
            )
        session.commit()
        recording_id = recording.id
    runner = SimpleNamespace(
        name="test",
        get_session=get_db_session,
        kill_event=threading.Event(),
        log_progress=Mock(),
        recording_paths={"camera": (str(tier), str(spool))},
        tier_markers=(
            {str(tmp_path): ".missing-mount-marker"} if location == "offline" else {}
        ),
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    OrphanedRecordingsCleanup.work(runner)
    with get_db_session() as session:
        assert (session.get(Recordings, recording_id) is None) == (location == "empty")


def test_crash_after_last_segment_commit_recovers_without_callback(
    tmp_path, get_db_session, monkeypatch
):
    """A fresh cleanup runner recovers an actual process-exit boundary."""
    root = tmp_path / "camera"
    run = root / RUN
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    source = run / "last.m4s"
    source.write_bytes(b"retired footage")
    now = datetime.now(UTC)
    with get_db_session() as session:
        recording = Recordings(
            camera_identifier="camera",
            recorder_run_id=RUN,
            adjusted_start_time=now,
            start_time=now,
            end_time=now + timedelta(seconds=1),
        )
        session.add(recording)
        session.add(
            Files(
                tier_id=0,
                tier_path=str(tmp_path),
                camera_identifier="camera",
                category="recorder",
                subcategory="segments",
                path=str(source),
                directory=str(run),
                filename=source.name,
                size=15,
                duration=1,
                orig_ctime=now,
            )
        )
        session.commit()
        recording_id = recording.id
    pid = os.fork()
    if pid == 0:
        try:
            get_db_session.kw["bind"].dispose(close=False)
            delete_file(get_db_session, str(source), logging.getLogger(__name__))
        except BaseException:
            os._exit(1)
        os._exit(0)  # Deliberately lose all in-memory completion callbacks.
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert not source.exists()
    with get_db_session() as session:
        assert session.get(Recordings, recording_id) is not None
    runner = SimpleNamespace(
        name="fresh-runner",
        get_session=get_db_session,
        kill_event=threading.Event(),
        log_progress=Mock(),
        recording_paths={"camera": (str(root),)},
        tier_markers={},
    )
    monkeypatch.setattr("viseron.components.storage.jobs.time.sleep", lambda _: None)
    # A concurrent publication must prevent recovery from declaring the run empty.
    with camera_lock("/tmp/viseron-run-locks", str(root)):
        OrphanedRecordingsCleanup.work(runner)
    with get_db_session() as session:
        assert session.get(Recordings, recording_id) is not None
    OrphanedRecordingsCleanup.work(runner)
    OrphanedRecordingsCleanup.work(runner)  # Repeated recovery is harmless.
    with get_db_session() as session:
        assert session.get(Recordings, recording_id) is None


def test_recovery_roots_cross_child_process_boundary(tmp_path, monkeypatch):
    """Resolve camera paths before forkserver; carry only serializable config."""
    tier = tmp_path / "tier" / "camera"
    spool = tmp_path / "spool" / "camera"
    legacy_prefix = str(tmp_path / "legacy")
    legacy = legacy_prefix + str(tier)
    os.makedirs(legacy)
    monkeypatch.setattr("viseron.components.storage.jobs.TEMP_DIR", legacy_prefix)
    camera = SimpleNamespace(segments_folder=str(tier), temp_segments_folder=str(spool))
    vis, storage = Mock(), Mock()
    vis.get_registered_identifiers.return_value = {"camera": camera}
    storage.get_segments_path.return_value = [str(tier)]
    job = OrphanedRecordingsCleanup(vis, storage, CronTrigger(hour=0))
    paths = job._get_recording_paths()
    assert paths == {"camera": tuple(sorted((str(tier), str(spool), legacy)))}
    config = CleanupJobConfig(
        name="recovery",
        paths=[],
        ignored_files=frozenset(),
        sensitive_strings=(),
        log_level=logging.INFO,
        recording_paths=paths,
    )
    assert pickle.loads(pickle.dumps(config)).recording_paths == paths  # noqa: S301
