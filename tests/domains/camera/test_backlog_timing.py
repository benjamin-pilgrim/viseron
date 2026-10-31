"""Exercise retention of spool fragments whose timing is ambiguous."""

import os
import queue
import subprocess
import sys
import threading
import time

from viseron.domains.camera.fragmenter_process import FragmenterWork
from viseron.domains.camera.playlist_timing import capture_playlist_times


def test_unmatched_playlist_filename_does_not_publish_staged_fragment(tmp_path, caplog):
    """A similar playlist name must not supply another fragment's timestamp."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(b"fragment")
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
    with caplog.at_level("WARNING"):
        work.work_input({"cmd": "fragment"})
    assert (run / name).exists()
    assert not (destination / run_id / name).exists()
    assert "retaining them and init unindexed" in caplog.text


def test_captured_playlist_timing_survives_rollover_and_worker_restart(tmp_path):
    """A delayed consumer publishes a fragment using its recorded exact timestamp."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    name = "000000001-1760000000-0000001000000.m4s"
    (run / name).write_bytes(b"fragment")
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
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(b"old fragment")
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
    fragment.write_bytes(b"replacement")
    playlist.write_text("#EXTM3U\n", encoding="utf-8")
    destination = tmp_path / "segments" / "cam"
    work = FragmenterWork(
        "cam", str(stage), str(destination), None, None, threading.Event()
    )
    work.output_queue = queue.Queue()
    with caplog.at_level("WARNING"):
        work.work_input({"cmd": "fragment"})
    assert fragment.exists()
    assert not (destination / run_id / name).exists()
    assert "retaining them and init unindexed" in caplog.text


def test_replacement_after_replay_decision_is_not_published(tmp_path):
    """Replacing a fragment during metadata acknowledgement fails closed."""
    stage = tmp_path / "stage"
    run_id = "20261031T143215.482000Z-a7c93e12ab34"
    run = stage / run_id
    run.mkdir(parents=True)
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(b"old fragment")
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
    fragment.write_bytes(b"replacement")
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
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(b"fragment")
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
    name = "000000001-1760000000-0000001000000.m4s"
    fragment = run / name
    fragment.write_bytes(b"fragment")
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
    assert fragment.read_bytes() == b"fragment"
    assert not (run / f".viseron-timing-{name}.json").exists()
    assert list(run.glob(".viseron-timing-tmp-*"))
    capture_playlist_times(stage)
    assert (run / f".viseron-timing-{name}.json").exists()
