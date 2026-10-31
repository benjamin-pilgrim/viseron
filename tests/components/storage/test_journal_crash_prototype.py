"""Process-kill tests for journal-backed recorder publication."""

import datetime
import multiprocessing
import signal
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from viseron.components.storage.pending_journal import (
    marker_for_path,
    publish_or_verify,
)
from viseron.domains.camera.fragmenter_process import FragmenterWork

WHEN = datetime.datetime(2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc)
RUN = "20261031T143215.482000Z-a7c93e12ab34"


def publish_until_rename(staging, storage, conn, after=False):
    """Publish until rename."""
    actual_publish = publish_or_verify

    def intercept(marker):
        if after:
            actual_publish(marker)
        conn.send("marker-written")
        conn.recv()
        if not after:
            return actual_publish(marker)
        return None

    worker = FragmenterWork("test-journal", staging, storage, None, None, None)
    with patch(
        "viseron.domains.camera.fragmenter_process.publish_or_verify",
        side_effect=intercept,
    ):
        worker._move_to_segments_folder(f"{RUN}/1.m4s", WHEN, 1.5)


class ProcessCrashTests(unittest.TestCase):
    """Test ProcessCrash cases."""

    def test_kill_after_marker_before_publish_can_retry(self):
        """Kill after marker before publish can retry."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage = root / "stage" / RUN
            stage.mkdir(parents=True)
            store = root / "storage"
            store.mkdir()
            (stage / "init.mp4").write_bytes(b"init")
            (stage / "1.m4s").write_bytes(b"video")
            ctx = multiprocessing.get_context("fork")
            parent, child = ctx.Pipe()
            process = ctx.Process(
                target=publish_until_rename, args=(str(stage.parent), str(store), child)
            )
            process.start()
            assert parent.poll(10)
            assert parent.recv() == "marker-written"
            process.kill()
            process.join(10)
            assert process.exitcode == -signal.SIGKILL
            dst = store / RUN / "1.m4s"
            assert Path(marker_for_path(str(dst))).exists()
            assert (stage / "1.m4s").exists()
            assert not dst.exists()
            publish_or_verify(marker_for_path(str(dst)))
            assert dst.read_bytes() == b"video"
            assert (stage / "1.m4s").exists()  # Held until database acknowledgement.

    def test_kill_after_publish_before_source_unlink_can_retry(self):
        """Kill after publish before source unlink can retry."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage = root / "stage" / RUN
            stage.mkdir(parents=True)
            store = root / "storage"
            store.mkdir()
            (stage / "init.mp4").write_bytes(b"init")
            (stage / "1.m4s").write_bytes(b"video")
            ctx = multiprocessing.get_context("fork")
            parent, child = ctx.Pipe()
            process = ctx.Process(
                target=publish_until_rename,
                args=(str(stage.parent), str(store), child, True),
            )
            process.start()
            assert parent.poll(10)
            assert parent.recv() == "marker-written"
            process.kill()
            process.join(10)
            assert process.exitcode == -signal.SIGKILL
            dst = store / RUN / "1.m4s"
            assert Path(marker_for_path(str(dst))).exists()
            assert (stage / "1.m4s").exists()
            assert dst.read_bytes() == b"video"
            publish_or_verify(marker_for_path(str(dst)))
            assert dst.read_bytes() == b"video"
            assert (stage / "1.m4s").exists()  # Held until database acknowledgement.

    def test_cifs_kill_after_marker_and_retry(self):
        """Cifs kill after marker and retry."""
        mount = Path("/test-cifs")
        if not mount.is_dir():
            self.skipTest("CIFS test mount unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage = root / "stage" / RUN
            stage.mkdir(parents=True)
            store = Path(tempfile.mkdtemp(prefix=".journal-crash-", dir=mount))
            self.addCleanup(lambda: shutil.rmtree(store, ignore_errors=True))
            (stage / "init.mp4").write_bytes(b"init")
            (stage / "1.m4s").write_bytes(b"video")
            ctx = multiprocessing.get_context("fork")
            parent, child = ctx.Pipe()
            process = ctx.Process(
                target=publish_until_rename, args=(str(stage.parent), str(store), child)
            )
            process.start()
            assert parent.poll(10)
            assert parent.recv() == "marker-written"
            process.kill()
            process.join(10)
            assert process.exitcode == -signal.SIGKILL
            dst = store / RUN / "1.m4s"
            assert Path(marker_for_path(str(dst))).exists()
            assert (stage / "1.m4s").exists()
            publish_or_verify(marker_for_path(str(dst)))
            assert dst.read_bytes() == b"video"


if __name__ == "__main__":
    unittest.main()
