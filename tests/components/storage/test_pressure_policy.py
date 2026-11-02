"""User-facing retention limits and full-storage priorities."""

import contextlib
import datetime
import errno
import multiprocessing
import os
import queue
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import voluptuous as vol

from viseron.components.storage import pressure
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.config import TIER_SCHEMA_RECORDER
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.pressure import reclaim
from viseron.components.storage.pressure_manager import PressureManager
from viseron.components.storage.storage_subprocess import DataItem
from viseron.components.storage.util import calculate_age, calculate_bytes
from viseron.domains.camera.fragmenter_process import FragmenterWork
from viseron.domains.camera.playlist_timing import capture_playlist_times
from viseron.helpers.recording_timing_contract import (
    TIMING_CONTRACT_CONTENT,
    TIMING_CONTRACT_FILE,
)


def test_simple_retention_and_full_disk_policy():
    """Accept the documented policy without per-class size allowances."""
    tier = TIER_SCHEMA_RECORDER(
        {
            "path": "/recordings",
            "retention": {"max_age": "30d", "max_size": "3TB"},
            "on_full": {"policy": "ordinary_first"},
        }
    )
    assert tier["retention"] == {"max_age": 2592000, "max_size": 3_000_000_000_000}
    assert tier["on_full"]["policy"] == "ordinary_first"


@pytest.mark.parametrize("value", ["-1TB", "0GB", "3TT", True, "NaNGB"])
def test_invalid_limit_is_rejected(value):
    """Reject typos and nonpositive budgets instead of silently disabling limits."""

    with pytest.raises(vol.Invalid):
        TIER_SCHEMA_RECORDER({"path": "/recordings", "retention": {"max_size": value}})


def _record(get_session, root, camera, timestamp, size, *, event=False, active=False):

    run = root / "segments" / camera / "20261102T000000.000000Z-012345abcdef"
    run.mkdir(parents=True, exist_ok=True)
    (run / "init.mp4").write_bytes(b"init")
    path = run / f"{int(timestamp.timestamp())}.m4s"
    path.write_bytes(b"x" * size)
    with get_session() as session:
        row = Files(
            tier_id=0,
            tier_path=str(root),
            camera_identifier=camera,
            category="recorder",
            subcategory="segments",
            path=str(path),
            directory=str(run),
            filename=path.name,
            size=size,
            duration=2,
            orig_ctime=timestamp,
        )
        session.add(row)
        if event:
            session.add(
                Recordings(
                    camera_identifier=camera,
                    start_time=timestamp,
                    adjusted_start_time=timestamp,
                    recorder_run_id=run.name,
                    end_time=None
                    if active
                    else timestamp + datetime.timedelta(seconds=2),
                )
            )
        session.commit()
    return path


def _configuration(root, policy, cameras=("camera",), maximum=0):
    return {
        "path": str(root),
        "probe": str(root),
        "policy": policy,
        "max_size": maximum,
        "markers": {},
        "reclaim": True,
        "cameras": [
            {
                "identifier": camera,
                "tier_id": 0,
                "segments": str(root / "segments" / camera),
                "spool": None,
                "lookback": 10,
            }
            for camera in cameras
        ],
    }


@contextlib.contextmanager
def _pressure_directory():
    if not os.path.ismount("/pressure-test"):
        pytest.skip("Full-disk acceptance requires a dedicated /pressure-test tmpfs")
    assert (
        os.statvfs("/pressure-test").f_blocks * os.statvfs("/pressure-test").f_frsize
        <= 32 * 1024**2
    )
    with tempfile.TemporaryDirectory(dir="/pressure-test") as directory:
        yield Path(directory)


def _fill(root):
    """Reach actual filesystem ENOSPC using an unowned filler file."""

    with open(root / "external-filler", "wb", buffering=0) as stream:
        try:
            while True:
                stream.write(b"p" * 1024 * 1024)
        except OSError as error:
            assert error.errno == errno.ENOSPC


@pytest.mark.parametrize(
    "policy,keep_event", [("oldest_first", False), ("ordinary_first", True)]
)
def test_genuinely_full_first_tier_reclaims_and_can_accept_new_footage(
    get_db_session, policy, keep_event
):
    """Native tmpfs exhaustion recovers according to the chosen event priority."""

    with _pressure_directory() as root:
        now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        event = _record(get_db_session, root, "camera", now, 1024**2, event=True)
        ordinary = [
            _record(
                get_db_session,
                root,
                "camera",
                now + datetime.timedelta(seconds=10 + i * 5),
                1024**2,
            )
            for i in range(4)
        ]
        _fill(root)
        result = reclaim(_configuration(root, policy), get_db_session)
        assert result["deleted"] > 0
        assert event.exists() == keep_event
        assert any(not path.exists() for path in ordinary)
        (root / "new-footage").write_bytes(b"n" * 1024**2)
        assert (event.parent / "init.mp4").read_bytes() == b"init"


def test_preserve_events_reports_blocked_when_only_events_remain(get_db_session):
    """The preservation choice keeps existing events even on a truly full tier."""

    with _pressure_directory() as root:
        now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        paths = [
            _record(
                get_db_session,
                root,
                "camera",
                now + datetime.timedelta(seconds=i * 5),
                1024**2,
                event=True,
            )
            for i in range(3)
        ]
        _fill(root)
        result = reclaim(_configuration(root, "preserve_events"), get_db_session)
        assert result["state"] == "blocked"
        assert result["deleted"] == 0
        assert all(path.exists() for path in paths)


def test_size_budget_is_shared_across_cameras(get_db_session, tmp_path):
    """Two cameras do not each receive a separate allowance for the same tier."""

    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    oldest = _record(get_db_session, tmp_path, "one", now, 1024)
    newest = _record(
        get_db_session, tmp_path, "two", now + datetime.timedelta(seconds=10), 1024
    )
    config = _configuration(tmp_path, "oldest_first", ("one", "two"), maximum=2000)
    result = reclaim(config, get_db_session)
    assert result["deleted"] == 1
    assert not oldest.exists()
    assert newest.exists()
    assert str(oldest) not in {item["path"] for item in result["promote"]}
    assert str(newest) in {item["path"] for item in result["promote"]}


def test_normalized_camera_override_is_idempotent():
    """Camera overrides can pass through storage schema validation again."""
    first = TIER_SCHEMA_RECORDER(
        {"path": "/recordings", "retention": {"max_size": "1GiB"}}
    )
    assert TIER_SCHEMA_RECORDER(first) == first


def _staged(root, timestamp, sequence=1):

    spool = root / "spool"
    run = spool / "20261102T000000.000000Z-012345abcdef"
    run.mkdir(parents=True, exist_ok=True)
    name = f"{sequence:09d}-{int(timestamp.timestamp())}-0000002000000.m4s"
    source = run / name
    source.write_bytes(b"s" * 1024)
    (run / "init.mp4").write_bytes(b"init")
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT)
    with open(run / "index.m3u8", "a", encoding="utf-8") as playlist:
        playlist.write(
            f"#EXTINF:2.000000,\n#EXT-X-PROGRAM-DATE-TIME:{timestamp.isoformat()}\n{name}\n"
        )
    capture_playlist_times(spool)
    return spool, source


def test_full_first_tier_can_reclaim_validated_staged_footage(get_db_session, tmp_path):
    """A full recording tier can free backlog before it has reached Files."""

    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    spool, source = _staged(tmp_path, now)
    config = _configuration(tmp_path, "ordinary_first", maximum=1024)
    config["cameras"][0]["spool"] = str(spool)
    result = reclaim(config, get_db_session)
    assert result["deleted"] == 1
    assert not source.exists()
    assert (source.parent / "init.mp4").exists()


def test_admission_preserves_buffer_drops_only_new_complete_media_then_resumes(
    tmp_path, monkeypatch
):
    """A blocked tier retains its old buffer and resumes without stopping the camera."""

    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    spool, buffered = _staged(tmp_path, now)
    work = FragmenterWork(
        "camera", str(spool), str(tmp_path / "segments"), None, None, threading.Event()
    )
    work._move_to_segments_folder = Mock()
    work.output_queue = queue.Queue()
    monkeypatch.setattr(
        "viseron.domains.camera.fragmenter_process._get_mp4_files_to_fragment",
        lambda _path: [str(buffered.relative_to(spool))],
    )
    work.work_input({"cmd": "fragment", "storage_blocked": True})
    assert buffered.exists()
    _, incoming = _staged(tmp_path, now + datetime.timedelta(seconds=5), sequence=2)
    work._move_to_segments_folder.reset_mock()
    work._handle_m4s(str(incoming.relative_to(spool)))
    assert not incoming.exists()
    assert buffered.exists()
    work._move_to_segments_folder.assert_not_called()
    monkeypatch.setattr(
        "viseron.domains.camera.fragmenter_process._get_mp4_files_to_fragment",
        lambda _path: [],
    )
    work.work_input({"cmd": "fragment"})
    _, resumed = _staged(tmp_path, now + datetime.timedelta(seconds=10), sequence=3)
    work._handle_m4s(str(resumed.relative_to(spool)))
    work._move_to_segments_folder.assert_called_once()


def _isolated_check(config, output):

    if config["path"] == "slow":
        time.sleep(30)
    result = {"state": "normal", "deleted": 0, "offset": 0}
    if config["path"] == "large":
        result["payload"] = b"x" * 256000
    output.send(result)
    output.close()


def test_stalled_remote_pressure_check_does_not_block_local_root(monkeypatch):
    """Real independent child processes let the local check finish first."""

    monkeypatch.setattr(
        "viseron.components.storage.pressure_manager.run_pressure", _isolated_check
    )
    monkeypatch.setattr(
        "viseron.components.storage.pressure_manager.get_child_process_context",
        lambda: multiprocessing.get_context("fork"),
    )
    storage = Mock()
    storage.pending_moves = {}
    storage.pending_moves_lock = threading.Lock()
    manager = PressureManager(storage)
    manager.configurations = lambda: {
        name: {"path": name, "policy": "ordinary_first", "cameras": []}
        for name in ("slow", "fast", "large")
    }
    try:
        manager.poll()
        deadline = time.monotonic() + 5
        while (
            not {"fast", "large"}.issubset(manager.status)
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
            manager.poll()
        assert manager.status["fast"]["state"] == "normal"
        assert manager.status["large"]["state"] == "normal"
        assert manager.pending["slow"][0].is_alive()
    finally:
        manager.stop()


def test_complete_spool_can_reclaim_without_space_for_a_timing_sidecar(
    get_db_session, tmp_path
):
    """An exact live playlist entry remains usable when timing persistence failed."""
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    spool, source = _staged(tmp_path, now)
    (source.parent / f".viseron-timing-{source.name}.json").unlink()
    config = _configuration(tmp_path, "oldest_first", maximum=1024)
    config["cameras"][0]["spool"] = str(spool)
    result = reclaim(config, get_db_session)
    assert result["deleted"] == 1
    assert not source.exists()


def test_inode_exhaustion_is_pressure_even_with_free_bytes(
    get_db_session, tmp_path, monkeypatch
):
    """An inode-pressure signal triggers reclamation despite plentiful free bytes."""
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    paths = [
        _record(
            get_db_session,
            tmp_path,
            "camera",
            now + datetime.timedelta(seconds=i * 5),
            1024,
        )
        for i in range(2)
    ]
    real_statvfs = os.statvfs

    def filesystem(path):
        if os.fspath(path) == str(tmp_path):
            return SimpleNamespace(
                f_blocks=1000000,
                f_bavail=900000,
                f_frsize=4096,
                f_files=10,
                f_favail=2 - sum(path.exists() for path in paths),
            )
        return real_statvfs(path)

    monkeypatch.setattr(os, "statvfs", filesystem)
    result = reclaim(_configuration(tmp_path, "oldest_first"), get_db_session)
    assert result["deleted"] == 2
    assert result["state"] == "normal"
    assert not any(path.exists() for path in paths)


def test_new_active_event_is_rechecked_before_pressure_deletion(
    get_db_session, tmp_path, monkeypatch
):
    """A newly protected event cannot be deleted using an earlier classification."""

    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    path = _record(get_db_session, tmp_path, "camera", now, 1024)
    original = pressure._candidates

    def protect(config, session, staged):
        candidates = original(config, session, staged)
        with get_db_session() as other:
            other.add(
                Recordings(
                    camera_identifier="camera", start_time=now, adjusted_start_time=now
                )
            )
            other.commit()
        return candidates

    monkeypatch.setattr(pressure, "_candidates", protect)
    result = reclaim(
        _configuration(tmp_path, "oldest_first", maximum=1000), get_db_session
    )
    assert result["state"] == "blocked"
    assert path.exists()


def test_preservation_cannot_be_bypassed_by_a_separate_event_size_limit(
    get_db_session, tmp_path
):
    """The shared size budget must not become an independent event-eviction rule."""
    tier = TIER_SCHEMA_RECORDER(
        {
            "path": str(tmp_path),
            "retention": {"max_size": "1KB", "max_age": "30d"},
            "on_full": {"policy": "preserve_events"},
        }
    )
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    path = _record(get_db_session, tmp_path, "camera", now, 1024, event=True)
    worker = Worker()
    worker._get_session = get_db_session
    item = DataItem(
        cmd="check_tier",
        camera_identifier="camera",
        tier_id=0,
        category="recorder",
        subcategories=["segments"],
        throttle_period=datetime.timedelta(0),
        max_bytes=calculate_bytes(tier["continuous"]["max_size"]),
        min_age=datetime.timedelta(0),
        max_age=calculate_age(tier["continuous"]["max_age"]),
        min_bytes=0,
        drain=False,
        files_enabled=True,
        events_enabled=True,
        events_max_bytes=calculate_bytes(tier["events"]["max_size"]),
        events_min_age=datetime.timedelta(0),
        events_max_age=calculate_age(tier["events"]["max_age"]),
        events_min_bytes=0,
    )
    assert calculate_bytes(tier["events"]["max_size"]) == 0
    assert calculate_bytes(tier["continuous"]["max_size"]) == 0
    worker.check_tier(item)
    assert item.data.size == 0
    result = reclaim(
        _configuration(tmp_path, "preserve_events", maximum=1000), get_db_session
    )
    assert result["state"] == "blocked"
    assert path.exists()


def test_preserved_event_can_promote_before_any_eviction(get_db_session, tmp_path):
    """Event preservation protects deletion, not forwarding to healthy storage."""
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    path = _record(get_db_session, tmp_path, "camera", now, 1024, event=True)
    config = _configuration(tmp_path, "preserve_events", maximum=1000)
    config["reclaim"] = False
    result = reclaim(config, get_db_session)
    assert result["state"] == "promoting"
    assert result["promote"][0]["path"] == str(path)
    assert path.exists()


def test_healthy_promotion_gets_time_before_eviction_and_failure_allows_fallback():
    """Progress postpones eviction; failed or stalled transfers permit fallback."""
    manager = PressureManager(Mock())
    config = {"cameras": [{"has_next": True}]}
    manager.last_progress["root"] = 100
    assert not manager._can_reclaim("root", config, 105)
    assert manager._can_reclaim("root", config, 116)
    manager.last_progress["root"] = 115
    assert not manager._can_reclaim("root", config, 116)
    manager.promotion_failed.add("root")
    assert manager._can_reclaim("root", config, 116)
