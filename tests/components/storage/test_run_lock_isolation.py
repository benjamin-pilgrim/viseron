"""A blocked remote tier must not block local recording publication."""

import datetime
import logging
import multiprocessing
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from viseron.components.storage.check_tier import move_file
from viseron.domains.camera.fragmenter_process import FragmenterWork
from viseron.components.storage.check_tier import publish_run_init
from viseron.components.storage.run_init_prototype import ensure_init

RUN = "20261031T143215.482000Z-a7c93e12ab34"
WHEN = datetime.datetime(2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc)


def block_copy(src, dst, conn):
    """Block copy."""

    def delayed(*args, **kwargs):
        """Block destination I/O while the remote tier lock is held."""
        conn.send("remote copying")
        conn.recv()
        return publish_run_init(*args, **kwargs)

    with patch(
        "viseron.components.storage.check_tier.publish_run_init", side_effect=delayed
    ):
        move_file(
            None,
            src,
            dst,
            logging.getLogger("test"),
            camera_identifier="camera",
            publication_meta={
                "orig_ctime": WHEN.isoformat(),
                "duration": 1,
                "file_key": 42,
            },
        )


class IsolationTests(unittest.TestCase):
    """Test Isolation cases."""

    def test_remote_stall_does_not_block_local_publishing(self):
        """Remote stall does not block local publishing."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local = root / "local" / "camera"
            remote = root / "remote" / "camera"
            source = local / RUN
            source.mkdir(parents=True)
            (source / "0.m4s").write_bytes(b"old")
            stage = root / "stage" / RUN
            stage.mkdir(parents=True)
            (stage / "init.mp4").write_bytes(b"init")
            ensure_init(stage / "init.mp4", source)
            (stage / "1.m4s").write_bytes(b"new")
            ctx = multiprocessing.get_context("fork")
            parent, child = ctx.Pipe()
            process = ctx.Process(
                target=block_copy,
                args=(str(source / "0.m4s"), str(remote / RUN / "0.m4s"), child),
            )
            process.start()
            try:
                assert parent.poll(10)
                assert parent.recv() == "remote copying"
                worker = FragmenterWork(
                    "camera", str(stage.parent), str(local), None, None, None
                )
                worker._move_to_segments_folder(f"{RUN}/1.m4s", WHEN, 1)
                assert (source / "1.m4s").read_bytes() == b"new"
                assert process.is_alive()
                parent.send("continue")
                process.join(10)
                assert process.exitcode == 0
            finally:
                if process.is_alive():
                    process.kill()
                    process.join()


if __name__ == "__main__":
    unittest.main()
