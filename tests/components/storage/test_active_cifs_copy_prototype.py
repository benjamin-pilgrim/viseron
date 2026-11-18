"""Coordinated real-CIFS outage while a tier copy is in progress."""

import builtins
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
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from watchdog.events import FileCreatedEvent

from viseron.components.storage.check_tier import move_file
from viseron.components.storage.models import Files, PendingSegmentMove

from tests.components.storage.test_promotion_handoff import _destination_handler
from tests.components.storage.test_segment_moves import (
    promotion as promotion_fixture,
    start_promotion,
)

promotion = promotion_fixture


def wait_flag(path):
    """Wait briefly for an external test coordinator."""
    deadline = time.monotonic() + 90
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError(str(path))
        time.sleep(0.1)


def copying(src, dst, control, metadata, db_url):
    """Pause before reading a second chunk, after the first target write."""
    actual_open = builtins.open
    engine = create_engine(db_url)
    get_session = sessionmaker(bind=engine)

    class PausedReader:
        def __init__(self, stream):
            self.stream = stream
            self.read_once = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def fileno(self):
            return self.stream.fileno()

        def read(self, size=-1):
            if self.read_once:
                (control / "copy-active").touch()
                wait_flag(control / "continue")
            self.read_once = True
            return self.stream.read(size)

    def open_source(path, *args, **kwargs):
        stream = actual_open(path, *args, **kwargs)
        if str(path) == src and args and args[0] == "rb":
            return PausedReader(stream)
        return stream

    try:
        with patch(
            "viseron.components.storage.check_tier.open",
            side_effect=open_source,
            create=True,
        ):
            move_file(
                get_session,
                src,
                dst,
                logging.getLogger("test"),
                camera_identifier="camera",
                publication_meta=metadata,
            )
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.environ.get("CIFS_KILL_TEST") != "1",
    reason="requires an externally coordinated disposable Samba outage",
)
def test_active_cifs_copy_killed_and_retried(promotion, get_db_session):
    """An interrupted transfer retains its source until verified index handoff."""
    control = Path("/test-control")
    remote = Path(tempfile.mkdtemp(prefix=".journal-active-copy-", dir="/test-cifs"))
    src = promotion.src
    # A real 64 MiB payload rather than a sparse file.
    with src.open("wb") as stream:
        for _ in range(64):
            stream.write(b"0123456789abcdef" * 65536)
    original_hash = hashlib.sha256(src.read_bytes()).hexdigest()
    dst = remote / "camera" / src.parent.name / src.name
    promotion.dst = dst
    target = _destination_handler(promotion.storage, remote)
    target._path = str(dst.parent.parent)
    promotion.storage.camera_tier_handlers["camera"]["recorder"][1]["segments"] = target
    with get_db_session() as session:
        session.execute(select(Files)).scalar_one().size = src.stat().st_size
        session.commit()
    start_promotion(promotion, get_db_session)
    item = promotion.storage.tier_check_worker_send_command.call_args.args[0]
    assert item.dst == str(dst)
    db_url = get_db_session.kw["bind"].url.render_as_string(hide_password=False)
    process = multiprocessing.get_context("fork").Process(
        target=copying,
        args=(str(src), str(dst), control, item.publication_meta, db_url),
    )
    try:
        process.start()
        wait_flag(control / "kill")
        process.kill()
        process.join(10)
        assert process.exitcode == -signal.SIGKILL
        assert src.is_file()
        with get_db_session() as session:
            pending = session.execute(select(PendingSegmentMove)).scalar_one()
            assert pending.source == str(src) and pending.destination == str(dst)
            assert session.execute(select(Files)).scalar_one().path == str(src)
        wait_flag(control / "server-up")
        move_file(
            get_db_session,
            str(src),
            str(dst),
            logging.getLogger("test"),
            camera_identifier="camera",
            publication_meta=item.publication_meta,
        )
        assert hashlib.sha256(dst.read_bytes()).hexdigest() == original_hash
        assert src.is_file(), "Copy completion alone does not authorize source deletion"
        target._on_created(FileCreatedEvent(str(dst)))
        assert not src.exists()
        with get_db_session() as session:
            row = session.execute(select(Files)).scalar_one()
            assert row.path == str(dst) and row.file_key == 42 and row.tier_id == 1
            assert session.execute(select(PendingSegmentMove)).first() is None
    finally:
        if process.is_alive():
            process.kill()
            process.join(10)
        shutil.rmtree(remote, ignore_errors=True)
