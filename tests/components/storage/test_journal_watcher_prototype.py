"""Tests of experimental marker replay; do not enable without tier-move support."""

import logging
import os
import time
import tempfile
import threading
import unittest
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch
from watchdog.events import FileCreatedEvent
from viseron.components.storage.jobs import OrphanedFilesCleanup
from viseron.components.storage.pending_journal import prepare, publish_or_verify
from viseron.components.storage.tier_handler import TierHandler


class WatcherTests(unittest.TestCase):
    """Test Watcher cases."""

    def setUp(self):
        """Setup."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run = self.root / "20261031T143215.482000Z-a7c93e12ab34"
        self.run.mkdir()
        (self.run / "init.mp4").write_bytes(b"init")
        temp = self.run / ".viseron-tier-move-testtemp"
        temp.write_bytes(b"data")
        self.meta = {
            "orig_ctime": "2026-10-31T14:32:15+00:00",
            "duration": 1.5,
            "file_key": 42,
        }
        self.marker = prepare(self.run, "1.m4s", self.meta, temp_path=temp)
        publish_or_verify(self.marker)
        self.handler = object.__new__(TierHandler)
        self.handler._camera = SimpleNamespace(identifier="test-journal")
        self.handler._logger = logging.getLogger("test")
        self.handler._event_queue = Queue()
        self.handler._journal_retry_lock = threading.Lock()
        self.handler._journal_retry_timer = None
        self.handler._journal_retry_paths = set()
        self.handler._journal_retry_rescan = False
        self.handler._journal_retry_stopping = False
        self.handler._journal_reported_issues = set()
        self.handler._journal_issue_overflow = False
        self.handler._on_created_impl = Mock()

    def test_acknowledges_only_after_index_callback(self):
        """Acknowledges only after index callback."""

        def index(_event, meta):
            """Index."""
            assert self.marker.exists()
            assert meta.duration == 1.5
            assert meta.file_key == 42

        self.handler._on_created_impl.side_effect = index
        self.handler._on_created(FileCreatedEvent(str(self.run / "1.m4s")))
        assert not self.marker.exists()

    def test_database_failure_retains_marker_and_retries(self):
        """Database failure retains marker and retries."""
        self.handler._on_created_impl.side_effect = RuntimeError("database down")
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            self.handler._on_created(FileCreatedEvent(str(self.run / "1.m4s")))
        assert self.marker.exists()
        timer.return_value.start.assert_called_once()

    def test_retry_is_deduplicated_bounded_and_stops(self):
        """Retry is deduplicated bounded and stops."""
        with patch("viseron.components.storage.tier_handler.threading.Timer") as timer:
            for _ in range(10):
                self.handler._schedule_journal_retry("/same")
            for i in range(2000):
                self.handler._schedule_journal_retry(f"/path/{i}")
            assert timer.call_count == 1
            assert len(self.handler._journal_retry_paths) <= 1024
            assert self.handler._journal_retry_rescan
            self.handler._journal_retry_stopping = True
            self.handler._retry_journal_publications()
            assert self.handler._event_queue.empty()

    def test_missing_media_reports_once_and_preserves_marker(self):
        """Report an incomplete publication without removing its marker."""
        self.handler._storage = SimpleNamespace(tier_available=lambda _: True)
        self.handler._path = str(self.root)
        (self.run / "1.m4s").unlink()
        with self.assertLogs(self.handler._logger, level="WARNING") as captured:
            self.handler._replay_pending_publications()
            self.handler._replay_pending_publications()
        assert len(captured.output) == 1
        assert "media is missing" in captured.output[0]
        assert self.marker.exists()

    def test_stale_marker_reports_but_still_replays(self):
        """Warn for an old pending publication and still queue recovery."""
        self.handler._storage = SimpleNamespace(tier_available=lambda _: True)
        self.handler._path = str(self.root)
        old = time.time() - 600
        os.utime(self.marker, (old, old))
        with self.assertLogs(self.handler._logger, level="WARNING") as captured:
            self.handler._replay_pending_publications()
        assert "over five minutes" in captured.output[0]
        assert self.handler._event_queue.get_nowait().src_path == str(
            self.run / "1.m4s"
        )
        assert self.marker.exists()

    def test_startup_replay(self):
        """Startup replay."""
        self.handler._storage = SimpleNamespace(tier_available=lambda _: True)
        self.handler._path = str(self.root)
        self.handler._replay_pending_publications()
        assert self.handler._event_queue.get_nowait().src_path == str(
            self.run / "1.m4s"
        )


if __name__ == "__main__":
    unittest.main()


class CleanupTests(unittest.TestCase):
    """Test Cleanup cases."""

    def test_pending_media_is_protected_then_eligible_after_ack(self):
        """Pending media is protected then eligible after ack."""

        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
            run.mkdir(parents=True)
            (run / "init.mp4").write_bytes(b"init")
            segment = run / "1.m4s"
            segment.write_bytes(b"data")
            marker = prepare(run, "1.m4s", {"duration": 1})
            session = Mock()
            session.execute.return_value.first.return_value = None
            cm = Mock()
            cm.__enter__ = Mock(return_value=session)
            cm.__exit__ = Mock(return_value=False)
            runner = SimpleNamespace(
                name="test",
                paths=[tmp],
                tier_markers={},
                ignored_files={"init.mp4"},
                kill_event=SimpleNamespace(is_set=lambda: False),
                get_session=lambda: cm,
                log_progress=lambda _: None,
            )
            OrphanedFilesCleanup.work(runner)
            assert segment.exists()
            marker.unlink()
            OrphanedFilesCleanup.work(runner)
            assert not segment.exists()

    def test_stale_private_files_removed_but_pending_and_unknown_preserved(self):
        """Stale private files removed but pending and unknown preserved."""

        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
            run.mkdir(parents=True)
            old = run / ".viseron-tier-move-12345678"
            young = run / ".viseron-tier-move-abcdefgh"
            unknown = run / ".viseron-tier-move-not-a-known-temp"
            marker = prepare(run, "1.m4s", {"duration": 1})
            for path in (old, young, unknown):
                path.write_bytes(b"partial")
            ancient = time.time() - 7200
            os.utime(old, (ancient, ancient))
            os.utime(unknown, (ancient, ancient))
            session = Mock()
            session.execute.return_value.first.return_value = None
            cm = Mock()
            cm.__enter__ = Mock(return_value=session)
            cm.__exit__ = Mock(return_value=False)
            runner = SimpleNamespace(
                name="test",
                paths=[tmp],
                tier_markers={},
                ignored_files=set(),
                kill_event=SimpleNamespace(is_set=lambda: False),
                get_session=lambda: cm,
                log_progress=lambda _: None,
            )
            OrphanedFilesCleanup.work(runner)
            # A pending journal could own any private temp; fail closed.
            assert old.exists()
            marker.unlink()
            OrphanedFilesCleanup.work(runner)
            assert not old.exists()
            assert young.exists()
            assert unknown.exists()
            assert not marker.exists()
