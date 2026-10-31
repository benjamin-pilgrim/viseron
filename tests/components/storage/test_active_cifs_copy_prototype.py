"""Coordinated real-CIFS outage while a tier copy is in progress."""

import hashlib
import logging
import multiprocessing
import os
import shutil
import signal
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from viseron.components.storage.check_tier import move_file
from viseron.components.storage.pending_journal import marker_for_path

RUN = "20261031T143215.482000Z-a7c93e12ab34"
META = {"orig_ctime": "2026-10-31T14:32:15+00:00", "duration": 1.5, "file_key": 42}


def wait_flag(path):
    """Wait briefly for an external test coordinator."""
    deadline = time.monotonic() + 90
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError(str(path))
        time.sleep(0.1)


def copying(src, dst, control):
    """Pause after the first copied chunk, then continue on command."""

    actual_copy = shutil.copyfileobj

    def copy(inp, out):
        out.write(inp.read(1024 * 1024))
        out.flush()
        (control / "copy-active").touch()
        wait_flag(control / "continue")
        actual_copy(inp, out)

    with patch(
        "viseron.components.storage.check_tier.shutil.copyfileobj", side_effect=copy
    ):
        move_file(
            None,
            src,
            dst,
            logging.getLogger("test"),
            camera_identifier="camera",
            publication_meta=META,
        )


def test_active_cifs_copy_killed_and_retried(tmp_path):
    """Retain the source and marker when a remote copy is killed during outage."""
    if os.environ.get("CIFS_KILL_TEST") != "1":
        pytest.skip("requires an externally coordinated disposable Samba outage")
    control = Path("/test-control")
    mount = Path("/test-cifs")
    remote = Path(tempfile.mkdtemp(prefix=".journal-active-copy-", dir=mount))
    source_dir = tmp_path / "local" / "camera" / RUN
    source_dir.mkdir(parents=True)
    src = source_dir / "1.m4s"
    (source_dir / "init.mp4").write_bytes(b"init")
    # A real 64 MiB payload rather than a sparse file.
    with src.open("wb") as stream:
        for _ in range(64):
            stream.write(b"0123456789abcdef" * 65536)
    original_hash = hashlib.sha256(src.read_bytes()).hexdigest()
    dst = remote / "camera" / RUN / "1.m4s"
    process = multiprocessing.get_context("fork").Process(
        target=copying, args=(str(src), str(dst), control)
    )
    try:
        process.start()
        wait_flag(control / "kill")
        process.kill()
        process.join(10)
        assert process.exitcode == -signal.SIGKILL
        wait_flag(control / "server-up")
        assert src.is_file()
        assert Path(marker_for_path(str(dst))).is_file()
        move_file(
            None,
            str(src),
            str(dst),
            logging.getLogger("test"),
            camera_identifier="camera",
            publication_meta=META,
        )
        assert hashlib.sha256(dst.read_bytes()).hexdigest() == original_hash
        assert not src.exists()
    finally:
        if process.is_alive():
            process.kill()
            process.join(10)
        shutil.rmtree(remote, ignore_errors=True)
