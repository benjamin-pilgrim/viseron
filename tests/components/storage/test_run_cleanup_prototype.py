"""Test run-init lifecycle cleanup under the publication lock."""

import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from viseron.components.storage.jobs import EmptyFoldersCleanup
from viseron.components.storage.pending_journal import prepare
from viseron.domains.camera.fragmenter_process import FragmenterWork

RUN = "20261031T143215.482000Z-a7c93e12ab34"


class RunCleanupTests(unittest.TestCase):
    """Test RunCleanup cases."""

    def setUp(self):
        """Setup."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.camera = self.root / "test-journal"
        self.run = self.camera / RUN
        self.run.mkdir(parents=True)
        (self.run / "init.mp4").write_bytes(b"init")
        session = Mock()
        session.execute.return_value.first.return_value = None
        cm = Mock()
        cm.__enter__ = Mock(return_value=session)
        cm.__exit__ = Mock(return_value=False)
        self.session = session
        self.runner = SimpleNamespace(
            name="test",
            paths=[str(self.camera)],
            tier_markers={},
            kill_event=SimpleNamespace(is_set=lambda: False),
            get_session=lambda: cm,
            log_progress=lambda _: None,
        )

    def clean(self):
        """Clean."""
        with patch("viseron.components.storage.jobs.time.sleep"):
            EmptyFoldersCleanup.work(self.runner)

    def test_empty_run_removed_and_later_fragment_republishes_init(self):
        """Empty run removed and later fragment republishes init."""
        self.clean()
        assert not self.run.exists()
        stage = self.root / "stage" / RUN
        stage.mkdir(parents=True)
        (stage / "init.mp4").write_bytes(b"init")
        (stage / "1.m4s").write_bytes(b"video")
        worker = FragmenterWork(
            "test-journal", str(stage.parent), str(self.camera), None, None, None
        )
        worker._move_to_segments_folder(
            f"{RUN}/1.m4s", datetime.datetime.now(datetime.timezone.utc), 1.5
        )
        assert (self.run / "init.mp4").read_bytes() == b"init"
        assert (self.run / "1.m4s").exists()

    def test_journal_prevents_reclaim(self):
        """Journal prevents reclaim."""
        prepare(self.run, "1.m4s", {"duration": 1})
        self.clean()
        assert (self.run / "init.mp4").exists()

    def test_database_reference_prevents_reclaim(self):
        """Database reference prevents reclaim."""
        self.session.execute.return_value.first.return_value = (1,)
        self.clean()
        assert (self.run / "init.mp4").exists()

    def test_database_failure_prevents_reclaim(self):
        """Database failure prevents reclaim."""
        self.session.execute.side_effect = RuntimeError("db down")
        self.clean()
        assert (self.run / "init.mp4").exists()


if __name__ == "__main__":
    unittest.main()


def test_orphan_cleanup_reaps_old_init_temporaries_but_keeps_owned(tmp_path):
    """Crash leftovers can be removed, while a prepared init remains replayable."""
    import json
    import os
    import time
    from viseron.components.storage.jobs import OrphanedFilesCleanup

    camera = tmp_path / "camera"
    run = camera / RUN
    run.mkdir(parents=True)
    owned = run / ".viseron-tier-move-init-aaaaaaaa"
    orphan = run / ".viseron-tier-move-init-bbbbbbbb"
    marker_temp = run / ".viseron-tier-move-init-marker-cccccccc"
    for path in (owned, orphan, marker_temp):
        path.write_bytes(b"init")
        old = time.time() - 7200
        os.utime(path, (old, old))
    (run / ".viseron-tier-move-run-init.json").write_text(
        json.dumps({"version": 2, "temp": owned.name, "source": [1, 2, 3, 4], "destination": [1, 2, 3, 4], "digest": "digest"})
    )
    session = Mock()
    cm = Mock()
    cm.__enter__ = Mock(return_value=session)
    cm.__exit__ = Mock(return_value=False)
    runner = SimpleNamespace(
        name="test",
        paths=[str(camera)],
        tier_markers={},
        ignored_files=[],
        kill_event=SimpleNamespace(is_set=lambda: False),
        get_session=lambda: cm,
        log_progress=lambda _: None,
    )
    with patch("viseron.components.storage.jobs.time.sleep"):
        OrphanedFilesCleanup.work(runner)
    assert owned.exists()
    assert not orphan.exists()
    assert not marker_temp.exists()
