"""Exercise retention of spool fragments whose timing is ambiguous."""

import hashlib
import json
import os
import queue
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

from viseron.components.storage.run_lifecycle import camera_lock
from viseron.domains.camera.fragmenter_process import FragmenterWork
from viseron.domains.camera.playlist_timing import capture_playlist_times
from viseron.helpers.recording_timing_contract import (
    TIMING_CONTRACT_CONTENT,
    TIMING_CONTRACT_FILE,
)


def _fragment_bytes(payload=b"fragment"):
    """Supply complete box framing for timing and publication race tests.

    Codec decoding is covered separately using FFmpeg-generated fragments.
    """
    return (
        (9).to_bytes(4, "big")
        + b"moof"
        + b"x"
        + (8 + len(payload)).to_bytes(4, "big")
        + b"mdat"
        + payload
    )


def test_unmatched_playlist_filename_does_not_publish_staged_fragment(tmp_path, caplog):
    """A similar playlist name must not supply another fragment's timestamp."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "init.mp4").write_bytes(b"init")
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}.other\n",
        encoding="utf-8",
    )
    destination = tmp_path / "segments" / "cam"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work._first_missing_timing[os.path.relpath(str(run / name), str(stage))] = (
        time.monotonic() - 31
    )
    with caplog.at_level("WARNING"):
        work.work_input({"cmd": "fragment"})
    assert (run / name).exists()
    assert not (destination / run_id / name).exists()
    assert "retaining it and its init unindexed" in caplog.text


def test_captured_playlist_timing_survives_rollover_and_worker_restart(tmp_path):
    """A delayed consumer publishes a fragment using its recorded exact timestamp."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15.123+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work.work_input({"cmd": "fragment"})
    assert (destination / run_id / name).exists()
    metadata = work.output_queue.get_nowait()
    assert metadata["orig_ctime"].isoformat() == "2026-10-31T14:32:15.123000+00:00"
    assert metadata["duration"] == 1.0


def test_changed_staged_fragment_is_not_replayed_with_old_timing(tmp_path, caplog):
    """A replaced staged file cannot inherit a previously observed timestamp."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"old fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    fragment.unlink()
    fragment.write_bytes(_fragment_bytes(b"replacement"))
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work._first_missing_timing[os.path.relpath(str(run / name), str(stage))] = (
        time.monotonic() - 31
    )
    with caplog.at_level("WARNING"):
        work.work_input({"cmd": "fragment"})
    assert fragment.exists()
    assert not (destination / run_id / name).exists()
    assert "retaining it and its init unindexed" in caplog.text


def test_replacement_after_replay_decision_is_not_published(tmp_path):
    """Replacing a fragment during metadata acknowledgement fails closed."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"old fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    ready = threading.Event()
    work = FragmenterWork("cam", str(stage), str(destination), None, None, ready)
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    work.output_queue.get(timeout=3)
    fragment.unlink()
    fragment.write_bytes(_fragment_bytes(b"replacement"))
    ready.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert fragment.exists()
    assert not (destination / run_id / name).exists()


def test_finished_spool_reclaims_orphaned_timing_files(tmp_path):
    """Timing writes interrupted by a crash do not strand a finished run."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    (run / ".viseron-writer-lease").write_bytes(b"")
    (run / "init.mp4").write_bytes(b"init")
    (run / "index.m3u8").write_text("#EXTM3U\n", encoding="utf-8")
    (run / ".viseron-timing-tmp-ab12cd34").write_text("partial", encoding="utf-8")
    (run / ".viseron-timing-000000001-1760000000-0000001000000.m4s.json").write_text(
        "{}", encoding="utf-8"
    )
    work = FragmenterWork(
        "cam", str(stage), str(tmp_path / "segments"), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work.work_input({"cmd": "fragment"})
    assert not run.exists()


def test_captured_timing_is_pruned_after_source_is_acknowledged(tmp_path):
    """A long-running camera does not retain records for already released files."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"fragment"))
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    record = run / f".viseron-timing-{name}.json"
    assert record.exists()
    fragment.unlink()
    hold = run / f".viseron-source-move-{name}.json"
    hold.write_text("{}", encoding="utf-8")
    capture_playlist_times(stage)
    assert record.exists()
    hold.unlink()
    capture_playlist_times(stage)
    assert not record.exists()


def test_killed_timing_capture_leaves_no_partial_record(tmp_path):
    """A killed capture process leaves media recoverable and can be retried."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"fragment"))
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    code = """
import os, signal, sys
from pathlib import Path
from viseron.domains.camera import playlist_timing
playlist_timing.os.link = lambda *_: os.kill(os.getpid(), signal.SIGSTOP)
playlist_timing.capture_playlist_times(Path(sys.argv[1]))
"""
    process = subprocess.Popen([sys.executable, "-c", code, str(stage)])
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            pid, status = os.waitpid(process.pid, os.WNOHANG | os.WUNTRACED)
            if pid and os.WIFSTOPPED(status):
                break
            if pid:
                raise AssertionError("capture exited before the interruption")
            time.sleep(0.05)
        else:
            raise AssertionError("capture did not reach the interruption")
        process.kill()
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert fragment.read_bytes() == _fragment_bytes(b"fragment")
    assert not (run / f".viseron-timing-{name}.json").exists()
    assert list(run.glob(".viseron-timing-tmp-*"))
    capture_playlist_times(stage)
    assert (run / f".viseron-timing-{name}.json").exists()


def test_replacement_after_live_playlist_decision_is_not_published(tmp_path):
    """Replacing a fragment during metadata acknowledgement fails closed."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"old fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    destination = tmp_path / "segments" / "cam"
    ready = threading.Event()
    work = FragmenterWork("cam", str(stage), str(destination), None, None, ready)
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    work.output_queue.get(timeout=3)
    fragment.unlink()
    fragment.write_bytes(_fragment_bytes(b"replacement"))
    ready.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert fragment.exists()
    assert not (destination / run_id / name).exists()


def test_unmarked_run_does_not_capture_or_replay_timing(tmp_path):
    """An old or custom-command run cannot acquire an assumed contract."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    assert not list(run.glob(".viseron-timing-*.json"))


def test_replacement_does_not_create_timelapse_frame(tmp_path):
    """An unpublished replacement must not create a misleading timelapse frame."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    for color in ("red", "blue"):
        output = tmp_path / color
        output.mkdir()
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"color=c={color}:s=96x96:r=10",
                "-t",
                "1",
                "-c:v",
                "libx264",
                "-g",
                "10",
                "-f",
                "hls",
                "-hls_time",
                "1",
                "-hls_segment_type",
                "fmp4",
                "-hls_segment_filename",
                str(output / "%05d.m4s"),
                str(output / "index.m3u8"),
            ],
            check=True,
        )
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes((tmp_path / "red" / "00000.m4s").read_bytes())
    (run / "init.mp4").write_bytes((tmp_path / "red" / "init.mp4").read_bytes())
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    frames = tmp_path / "frames"
    temporary_frames = tmp_path / "temporary_frames"
    frames.mkdir()
    temporary_frames.mkdir()
    ready = threading.Event()
    work = FragmenterWork(
        "cam",
        str(stage),
        str(tmp_path / "segments"),
        str(frames),
        str(temporary_frames),
        ready,
    )
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    work.output_queue.get(timeout=3)
    fragment.unlink()
    fragment.write_bytes((tmp_path / "blue" / "00000.m4s").read_bytes())
    ready.set()
    worker.join(timeout=15)
    assert not worker.is_alive()
    assert not list(frames.glob("*.jpg"))


def test_valid_published_fragment_creates_timelapse_frame(tmp_path):
    """A successfully published fragment still produces a timelapse frame."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    for color in ("red", "blue"):
        output = tmp_path / color
        output.mkdir()
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"color=c={color}:s=96x96:r=10",
                "-t",
                "1",
                "-c:v",
                "libx264",
                "-g",
                "10",
                "-f",
                "hls",
                "-hls_time",
                "1",
                "-hls_segment_type",
                "fmp4",
                "-hls_segment_filename",
                str(output / "%05d.m4s"),
                str(output / "index.m3u8"),
            ],
            check=True,
        )
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes((tmp_path / "red" / "00000.m4s").read_bytes())
    (run / "init.mp4").write_bytes((tmp_path / "red" / "init.mp4").read_bytes())
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    frames = tmp_path / "frames"
    temporary_frames = tmp_path / "temporary_frames"
    frames.mkdir()
    temporary_frames.mkdir()
    ready = threading.Event()
    work = FragmenterWork(
        "cam",
        str(stage),
        str(tmp_path / "segments"),
        str(frames),
        str(temporary_frames),
        ready,
    )
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    work.output_queue.get(timeout=3)
    ready.set()
    work.output_queue.get(timeout=15)
    ready.set()
    worker.join(timeout=15)
    assert not worker.is_alive()
    assert len(list(frames.glob("*.jpg"))) == 1
    assert (tmp_path / "segments" / run.name / name).exists()


def test_timing_sidecar_without_contract_is_not_replayed(tmp_path, caplog):
    """An orphaned sidecar alone cannot authorize automatic indexing."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    marker = run / TIMING_CONTRACT_FILE
    marker.write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    assert (run / f".viseron-timing-{name}.json").exists()
    marker.unlink()
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work._first_missing_timing[os.path.relpath(str(run / name), str(stage))] = (
        time.monotonic() - 31
    )
    with caplog.at_level("WARNING"):
        work.work_input({"cmd": "fragment"})
    assert fragment.exists()
    assert not (destination / run.name / name).exists()
    assert "retaining it and its init unindexed" in caplog.text


def test_replay_after_filesystem_identity_changes(tmp_path):
    """A remounted spool can replay timing after checking its source content."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15.123+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    record_path = run / f".viseron-timing-{name}.json"
    record = json.loads(record_path.read_text())
    record["identity"][0] += 1
    record_path.write_text(json.dumps(record))
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    work.work_input({"cmd": "fragment"})
    assert (destination / run_id / name).exists()
    metadata = work.output_queue.get_nowait()
    assert metadata["orig_ctime"].isoformat() == "2026-10-31T14:32:15.123000+00:00"
    assert metadata["duration"] == 1.0


def test_changed_content_after_remount_recheck_is_not_published(tmp_path):
    """Content changes after remount recheck are detected during the copy."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(_fragment_bytes(b"old fragment"))
    (run / "init.mp4").write_bytes(b"init")
    playlist = run / "index.m3u8"
    playlist.write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    capture_playlist_times(stage)
    record_path = run / f".viseron-timing-{name}.json"
    record = json.loads(record_path.read_text())
    record["identity"][0] += 1
    record_path.write_text(json.dumps(record))
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    ready = threading.Event()
    work = FragmenterWork("cam", str(stage), str(destination), None, None, ready)
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    work.output_queue.get(timeout=3)
    snapshot = fragment.stat()
    fragment.write_bytes(_fragment_bytes(b"new fragment"))
    os.utime(fragment, ns=(snapshot.st_atime_ns, snapshot.st_mtime_ns))
    ready.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert fragment.exists()
    assert not (destination / run_id / name).exists()


def test_slow_timelapse_does_not_hold_publication_lock(tmp_path, monkeypatch):
    """A stalled timelapse process must not block storage operations."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "init.mp4").write_bytes(b"init")
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    destination = tmp_path / "segments"
    frames = tmp_path / "frames"
    tmp_frames = tmp_path / "tmp_frames"
    frames.mkdir()
    tmp_frames.mkdir()
    ready = threading.Event()
    started = threading.Event()
    release = threading.Event()

    def slow_process(*_args, **_kwargs):
        started.set()
        release.wait(5)
        return SimpleNamespace(returncode=1, stderr="failed")

    monkeypatch.setattr(
        "viseron.domains.camera.fragmenter_process.sp.run", slow_process
    )
    work = FragmenterWork(
        "cam", str(stage), str(destination), str(frames), str(tmp_frames), ready
    )
    work.output_queue = queue.Queue()
    worker = threading.Thread(target=work.work_input, args=({"cmd": "fragment"},))
    worker.start()
    try:
        work.output_queue.get(timeout=3)
        ready.set()
        assert started.wait(5)
        with camera_lock("/tmp/viseron-run-locks", str(destination.resolve())):
            pass
    finally:
        release.set()
        worker.join(timeout=10)
    assert not worker.is_alive()


def test_slow_source_hash_does_not_hold_camera_lock(tmp_path, monkeypatch):
    """Slow local reads do not prevent publication or storage operations."""
    stage = tmp_path / "stage"
    run = stage / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / TIMING_CONTRACT_FILE).write_text(TIMING_CONTRACT_CONTENT, encoding="utf-8")
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(_fragment_bytes(b"fragment"))
    (run / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:1.000000,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-10-31T14:32:15+0000\n"
        f"{name}\n",
        encoding="utf-8",
    )
    started = threading.Event()
    release = threading.Event()
    real_sha256 = hashlib.sha256

    class SlowHash:
        def __init__(self):
            self.digest = real_sha256()

        def update(self, chunk):
            started.set()
            release.wait(5)
            self.digest.update(chunk)

        def hexdigest(self):
            return self.digest.hexdigest()

    monkeypatch.setattr(
        "viseron.domains.camera.playlist_timing.hashlib",
        SimpleNamespace(sha256=SlowHash),
    )
    thread = threading.Thread(target=capture_playlist_times, args=(stage,))
    thread.start()
    try:
        assert started.wait(5)
        with camera_lock("/tmp/viseron-run-locks", str(stage.resolve())):
            pass
    finally:
        release.set()
        thread.join(timeout=10)
    assert not thread.is_alive()
    assert (run / f".viseron-timing-{name}.json").exists()
