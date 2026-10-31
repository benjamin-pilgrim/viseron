"""Exercise retention of spool fragments whose timing is ambiguous."""

import queue
import threading

from viseron.domains.camera.fragmenter_process import FragmenterWork


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
