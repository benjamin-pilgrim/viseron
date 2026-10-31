"""Recorder run identity and delayed fragment processing regression tests."""

from __future__ import annotations

import datetime
import os
import queue
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from watchdog.events import FileMovedEvent

from viseron.components.ffmpeg.pipe import _command_for_run
from viseron.components.storage.tier_handler import TierHandler
from viseron.components.storage.util import is_tier_move_temp_file
from viseron.domains.camera.fragmenter_process import (
    FragmenterWork,
    _get_mp4_files_to_fragment,
)
from viseron.watchdog.subprocess_watchdog import RestartablePopen


class RecorderRunsTest(unittest.TestCase):
    """Test RecorderRuns cases."""

    def test_restart_with_previous_run_pending(self):
        """Restart with previous run pending."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging"
            storage = root / "storage"
            staging.mkdir()
            storage.mkdir()
            template = [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=96x96:rate=10",
                "-t",
                "2",
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
                "-hls_list_size",
                "10",
                "-hls_flags",
                "program_date_time",
                "-hls_segment_filename",
                str(staging / "__VISERON_RUN__" / "%03d.m4s"),
                str(staging / "__VISERON_RUN__" / "index.m3u8"),
            ]
            process = RestartablePopen(
                template,
                command_factory=lambda: _command_for_run(template),
                register=False,
                stderr=subprocess.DEVNULL,
            )
            assert process.subprocess.wait(timeout=20) == 0
            process.restart()
            assert process.subprocess.wait(timeout=20) == 0
            runs = sorted(p for p in staging.iterdir() if p.is_dir())
            assert len(runs) == 2
            assert all(len(list(p.glob("*.m4s"))) == 2 for p in runs)

            worker = FragmenterWork(
                "test", str(staging), str(storage), None, None, None
            )
            worker._send_files_metadata = lambda *_: None
            for run in runs:
                for fragment in run.glob("*.m4s"):
                    worker._handle_m4s(f"{run.name}/{fragment.name}")
                saved = storage / run.name
                assert (saved / "init.mp4").read_bytes() == (
                    run / "init.mp4"
                ).read_bytes()
                assert len(list(saved.glob("*.m4s"))) == 2
                (saved / "index.m3u8").write_text((run / "index.m3u8").read_text())
                subprocess.run(
                    [
                        "ffmpeg",
                        "-v",
                        "error",
                        "-i",
                        str(saved / "index.m3u8"),
                        "-f",
                        "null",
                        "-",
                    ],
                    check=True,
                    timeout=20,
                )

    def test_expired_playlist_entry_is_kept_with_init(self):
        """Expired playlist entry is kept with init."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "staging" / "20261031T143215.482000Z-a7c93e12ab34"
            run.mkdir(parents=True)
            storage = root / "storage"
            storage.mkdir()
            subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=96x96:rate=10",
                    "-t",
                    "16",
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
                    "-hls_list_size",
                    "10",
                    "-hls_flags",
                    "program_date_time",
                    "-hls_segment_filename",
                    str(run / "%03d.m4s"),
                    str(run / "index.m3u8"),
                ],
                check=True,
                timeout=20,
            )
            worker = FragmenterWork(
                "test", str(run.parent), str(storage), None, None, None
            )
            worker._send_files_metadata = lambda *_: None
            with self.assertLogs(worker._logger, level="WARNING") as captured:
                worker._handle_m4s(f"{run.name}/000.m4s")
            assert "No known duration" in captured.output[0]
            assert (run / "000.m4s").is_file()
            assert (run / "init.mp4").is_file()
            worker._handle_m4s(f"{run.name}/015.m4s")
            assert (storage / run.name / "015.m4s").is_file()

    def test_legacy_flat_run_refreshes_init_using_private_temp(self):
        """Legacy flat run refreshes init using private temp."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stage = root / "stage"
            storage = root / "storage"
            stage.mkdir()
            storage.mkdir()
            worker = FragmenterWork("test", str(stage), str(storage), None, None, None)
            rename = os.replace
            for i in range(2):
                (stage / "init.mp4").write_bytes(f"init {i}".encode())
                (stage / f"{i}.m4s").write_bytes(b"fragment")
                with patch(
                    "viseron.domains.camera.fragmenter_process.os.replace", wraps=rename
                ) as publish:
                    worker._move_to_segments_folder(
                        f"{i}.m4s", datetime.datetime.now(datetime.timezone.utc)
                    )
                    assert is_tier_move_temp_file(publish.call_args.args[0])
                assert (storage / "init.mp4").read_bytes() == f"init {i}".encode()
                assert (storage / f"{i}.m4s").exists()

    def test_watcher_does_not_index_ignored_init_after_temp_rename(self):
        """Watcher does not index ignored init after temp rename."""
        handler = object.__new__(TierHandler)
        handler._storage = SimpleNamespace(config={}, ignored_files=["init.mp4"])
        handler._event_queue = queue.Queue()
        handler._path = "/recordings"
        handler.on_any_event(
            FileMovedEvent("/recordings/.viseron-tier-move-123", "/recordings/init.mp4")
        )
        assert handler._event_queue.empty()
        handler.on_any_event(
            FileMovedEvent("/recordings/.viseron-tier-move-456", "/recordings/123.m4s")
        )
        assert handler._event_queue.get_nowait().src_path == "/recordings/123.m4s"

    def test_scanner_finds_legacy_mp4_and_run_fragments(self):
        """Scanner finds legacy mp4 and run fragments."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "20261031T143215.482000Z-a7c93e12ab34"
            run.mkdir()
            for path in (
                root / "123.mp4",
                root / "init.mp4",
                run / "init.mp4",
                run / "456.m4s",
            ):
                path.write_bytes(b"test")
            with patch(
                "viseron.domains.camera.fragmenter_process.psutil.process_iter",
                return_value=[],
            ):
                files = set(_get_mp4_files_to_fragment(str(root)))
            assert files == {"123.mp4", f"{run.name}/456.m4s"}

    def test_unavailable_tier_marker_keeps_staged_fragment(self):
        """Unavailable tier marker keeps staged fragment."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "stage" / "20261031T143215.482000Z-a7c93e12ab34"
            staging.mkdir(parents=True)
            tier = root / "tier"
            tier.mkdir()
            (staging / "init.mp4").write_bytes(b"init")
            (staging / "123.m4s").write_bytes(b"fragment")
            worker = FragmenterWork(
                "test",
                str(staging.parent),
                str(tier),
                None,
                None,
                None,
                tier_markers={str(tier): ".mounted"},
            )
            worker._move_to_segments_folder(
                f"{staging.name}/123.m4s",
                datetime.datetime.now(datetime.timezone.utc),
                1.0,
            )
            assert (staging / "123.m4s").exists()
            assert not (tier / staging.name).exists()
            (tier / ".mounted").write_text("mounted")
            worker._move_to_segments_folder(
                f"{staging.name}/123.m4s",
                datetime.datetime.now(datetime.timezone.utc),
                1.0,
            )
            assert (tier / staging.name / "123.m4s").exists()

    def test_missing_metadata_does_not_starve_newer_files(self):
        """Missing metadata does not starve newer files."""
        worker = FragmenterWork("test", "/unused", "/unused", None, None, None)
        seen = []
        files = [f"run/{i:03}.m4s" for i in range(12)]
        with (
            patch(
                "viseron.domains.camera.fragmenter_process._get_mp4_files_to_fragment",
                return_value=files,
            ),
            patch.object(worker, "_handle_m4s", side_effect=seen.append),
        ):
            for _ in range(3):
                worker.work_input({"cmd": "fragment"})
        assert set(seen) == set(files)


if __name__ == "__main__":
    unittest.main()
