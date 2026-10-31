"""Tests for durable pending publication markers."""

import os
import tempfile
import unittest
import shutil
from pathlib import Path
from viseron.components.storage.pending_journal import prepare, reconcile
import pytest


class Tests(unittest.TestCase):
    """Test  cases."""

    def setUp(self):
        """Setup."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.run = Path(self.tmp.name) / "run"
        self.run.mkdir()
        (self.run / "init.mp4").write_bytes(b"init")
        self.db = {}

    def index(self, path, meta):
        """Index."""
        self.db[path] = meta

    def test_replay_after_crash(self):
        """Replay after crash."""
        marker = prepare(self.run, "a.m4s", {"duration": 1.25})
        (self.run / "a.m4s").write_bytes(b"x")
        assert reconcile(marker, lambda: True, self.index) == "indexed"
        assert self.db[str(self.run / "a.m4s")]["duration"] == 1.25
        assert not marker.exists()

    def test_missing_media_retains_marker(self):
        """Missing media retains marker."""
        marker = prepare(self.run, "a.m4s", {})
        assert (
            reconcile(marker, lambda: True, self.index) == "needs-source-reconciliation"
        )
        assert marker.exists()

    def test_db_failure_retains_marker(self):
        """Db failure retains marker."""
        marker = prepare(self.run, "a.m4s", {})
        (self.run / "a.m4s").write_bytes(b"x")

        def fail(*_a):
            """Fail."""
            raise RuntimeError("db offline")

        assert reconcile(marker, lambda: True, fail) == "retry"
        assert marker.exists()

    def test_conflicting_marker_fails_closed(self):
        """Conflicting marker fails closed."""
        prepare(self.run, "a.m4s", {"duration": 1})
        with pytest.raises(ValueError):
            prepare(self.run, "a.m4s", {"duration": 2})

    def test_cifs_replay(self):
        """Cifs replay."""
        base = Path("/tmp/viseron-generation-e2e/cifs-mount")
        if not os.path.ismount(base):
            self.skipTest("CIFS unavailable")
        run = Path(tempfile.mkdtemp(prefix=".journal-", dir=base))
        self.addCleanup(lambda: shutil.rmtree(run, ignore_errors=True))
        (run / "init.mp4").write_bytes(b"i")
        marker = prepare(run, "a.m4s", {})
        (run / "a.m4s").write_bytes(b"x")
        assert reconcile(marker, lambda: True, self.index) == "indexed"


if __name__ == "__main__":
    unittest.main()
