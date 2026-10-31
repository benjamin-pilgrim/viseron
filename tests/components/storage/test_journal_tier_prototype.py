"""Experimental tier publication tests; a database is not exercised here."""

import logging
import multiprocessing
import signal
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from viseron.components.storage.check_tier import move_file
from viseron.components.storage.pending_journal import (
    marker_for_path,
    publish_or_verify,
)
import pytest


def stop_before_destination_rename(src, dst, meta, conn):
    """Wait with a fully copied but unpublished tier-move file."""

    def intercept(marker):
        conn.send("ready")
        conn.recv()
        return publish_or_verify(marker)

    with patch(
        "viseron.components.storage.check_tier.publish_or_verify", side_effect=intercept
    ):
        move_file(
            None,
            src,
            dst,
            logging.getLogger("test"),
            camera_identifier="test-journal",
            publication_meta=meta,
        )


class TierMoveTests(unittest.TestCase):
    """Test TierMove cases."""

    def setUp(self):
        """Setup."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run = "20261031T143215.482000Z-a7c93e12ab34"
        self.source = self.root / "src" / self.run
        self.source.mkdir(parents=True)
        self.dst = self.root / "dst" / self.run / "1.m4s"
        (self.source / "init.mp4").write_bytes(b"init")
        (self.source / "1.m4s").write_bytes(b"abc")
        self.meta = {
            "orig_ctime": "2026-10-31T14:32:15+00:00",
            "duration": 1.5,
            "file_key": 42,
        }

    def move(self, dst=None):
        """Move."""
        return move_file(
            None,
            str(self.source / "1.m4s"),
            str(dst or self.dst),
            logging.getLogger("test"),
            camera_identifier="test-journal",
            publication_meta=self.meta,
        )

    def test_success_preserves_marker_metadata(self):
        """Success preserves marker metadata."""
        self.move()
        assert self.dst.read_bytes() == b"abc"
        # Tier-to-tier source is held until database acknowledgement.
        assert (self.source / "1.m4s").exists()
        assert '"file_key": 42' in Path(marker_for_path(str(self.dst))).read_text()

    def test_failed_rename_preserves_source_and_marker(self):
        """Failed rename preserves source and marker."""
        with patch(
            "viseron.components.storage.check_tier.publish_or_verify",
            side_effect=OSError("offline"),
        ):
            with pytest.raises(OSError):
                self.move()
        assert (self.source / "1.m4s").exists()
        assert Path(marker_for_path(str(self.dst))).exists()

    def test_changed_source_is_not_unlinked_after_copy(self):
        """A replaced source is retained when final identity differs."""
        source = self.source / "1.m4s"

        def change_source(marker):
            result = publish_or_verify(marker)
            source.write_bytes(b"new")
            return result

        with patch(
            "viseron.components.storage.check_tier.publish_or_verify",
            side_effect=change_source,
        ):
            with pytest.raises(OSError, match="Source changed"):
                self.move()
        assert source.read_bytes() == b"new"
        assert Path(marker_for_path(str(self.dst))).exists()

    def test_process_killed_before_tier_publish_leaves_source_for_retry(self):
        """A killed copy leaves the source and marker for a later retry."""
        ctx = multiprocessing.get_context("fork")
        parent, child = ctx.Pipe()
        process = ctx.Process(
            target=stop_before_destination_rename,
            args=(str(self.source / "1.m4s"), str(self.dst), self.meta, child),
        )
        process.start()
        assert parent.poll(10)
        assert parent.recv() == "ready"
        process.kill()
        process.join(10)
        assert process.exitcode == -signal.SIGKILL
        assert (self.source / "1.m4s").read_bytes() == b"abc"
        assert Path(marker_for_path(str(self.dst))).exists()
        publish_or_verify(marker_for_path(str(self.dst)))
        assert self.dst.read_bytes() == b"abc"
        assert (self.source / "1.m4s").exists()

    def test_cifs_destination(self):
        """Cifs destination."""
        mount = Path("/test-cifs")
        if not mount.is_dir():
            self.skipTest("CIFS test mount unavailable")
        root = Path(tempfile.mkdtemp(prefix=".journal-tier-", dir=mount))
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        dst = root / self.run / "1.m4s"
        self.move(dst)
        assert dst.read_bytes() == b"abc"
        assert Path(marker_for_path(str(dst))).exists()


if __name__ == "__main__":
    unittest.main()
