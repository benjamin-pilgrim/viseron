"""Persistent PostgreSQL must not charge for reboot-lost tmpfs files."""

from datetime import UTC, datetime
from pathlib import Path
import errno
import os
import shutil
from unittest.mock import patch

import pytest

from tests.components.storage.test_segment_moves import source_row
from viseron.components.storage.pressure import reclaim
from viseron.components.storage import startup_chores
from viseron.components.storage.models import Files, PendingSegmentMove, Recordings
from viseron.components.storage.segment_moves import reserve


def test_startup_removes_lost_ram_location_but_preserves_transfer_and_event(
    tmp_path, get_db_session
):
    """Reconciliation runs before camera pressure checks, including held moves."""
    root = Path("/tmp")
    path = (
        tmp_path
        / "segments"
        / "camera"
        / "20261106T131053.282747Z-f1df0ac79b78"
        / "1.m4s"
    )
    path.parent.mkdir(parents=True)
    path.write_bytes(b"video")
    now = datetime.now(UTC)
    with get_db_session() as session:
        row = Files(
            camera_identifier="camera",
            tier_id=0,
            tier_path=str(root),
            category="recorder",
            subcategory="segments",
            path=str(path),
            directory=str(path.parent),
            filename=path.name,
            size=5,
            orig_ctime=now,
            duration=5,
        )
        event = Recordings(
            camera_identifier="camera", start_time=now, adjusted_start_time=now
        )
        session.add_all([row, event])
        session.flush()
        pending = reserve(
            session,
            row,
            str(tmp_path / "nas" / "camera" / path.parent.name / path.name),
            1,
            str(tmp_path / "nas"),
        )
        session.commit()
        row_id, pending_id, event_id = row.id, pending.id, event.id
    startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id) is not None
    shutil.rmtree(path.parent)  # Simulate tmpfs loss, retaining PostgreSQL.
    startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id) is None
        assert session.get(PendingSegmentMove, pending_id) is not None
        assert session.get(Recordings, event_id) is not None


@pytest.mark.parametrize("failure", [errno.EIO, errno.EACCES])
def test_startup_does_not_turn_io_errors_into_missing_files(
    tmp_path, get_db_session, failure
):
    """An inaccessible source retains its index rather than silently losing history."""

    row = source_row(tmp_path)
    row.tier_path = "/tmp"
    path = row.path
    with get_db_session() as session:
        session.add(row)
        session.commit()
        row_id = row.id
    original = os.stat

    def inaccessible(candidate, *args, **kwargs):
        if str(candidate) == path:
            raise OSError(failure, "unavailable")
        return original(candidate, *args, **kwargs)

    with patch(
        "viseron.components.storage.startup_reconciliation.os.stat",
        side_effect=inaccessible,
    ):
        startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id) is not None


def test_startup_never_probes_remote_or_missing_mount_roots(tmp_path, get_db_session):
    """A missing CIFS mount under tmpfs is not evidence of deleted footage."""

    row = source_row(tmp_path / "nas")
    with get_db_session() as session:
        session.add(row)
        session.commit()
        row_id = row.id
    with (
        patch(
            "viseron.components.storage.startup_reconciliation._mount_filesystems",
            return_value={"/tmp": "tmpfs"},
        ),
        patch(
            "viseron.components.storage.startup_reconciliation.os.stat",
            side_effect=AssertionError("probed unavailable NAS"),
        ),
    ):
        startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id) is not None


def test_retained_asset_intent_does_not_consume_volatile_capacity(
    tmp_path, get_db_session
):
    """Retain asset recovery proof without charging phantom bytes to RAM."""

    row = source_row(tmp_path)
    row.tier_path = "/tmp"
    row.subcategory = "thumbnails"
    row.size = 2**31 - 1
    target = {
        "path": str(tmp_path / "nas" / "asset.jpg"),
        "tier_id": 1,
        "tier_path": str(tmp_path / "nas"),
        "identity": [1, 2, row.size, 3],
    }
    row.move_target = target
    with get_db_session() as session:
        session.add(row)
        session.commit()
        row_id = row.id
    startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id).move_target == target
    info = os.statvfs("/tmp")
    allocated = (info.f_blocks - info.f_bfree) * info.f_frsize
    assert allocated < row.size
    result = reclaim(
        {
            "path": "/tmp",
            "probe": "/tmp",
            "max_size": (allocated + row.size) // 2,
            "policy": "preserve_events",
            "cameras": [],
            "volatile": True,
        },
        get_db_session,
    )
    assert result["state"] == "normal"
    assert result["used"] < row.size


def test_startup_does_not_resolve_a_nested_remote_mount(tmp_path, get_db_session):
    """Exclude submounts before resolving symlinks or accessing their metadata."""
    mount = tmp_path / "nas"
    row = source_row(mount)
    row.tier_path = "/tmp"
    with get_db_session() as session:
        session.add(row)
        session.commit()
        row_id = row.id
    original = os.lstat

    def inaccessible(path, *args, **kwargs):
        if str(path).startswith(str(mount)):
            raise AssertionError("resolved a remote submount")
        return original(path, *args, **kwargs)

    with (
        patch(
            "viseron.components.storage.startup_reconciliation._mount_filesystems",
            return_value={"/tmp": "tmpfs", str(mount): "cifs"},
        ),
        patch(
            "viseron.components.storage.startup_reconciliation.os.lstat",
            side_effect=inaccessible,
        ),
    ):
        startup_chores(get_db_session)
    with get_db_session() as session:
        assert session.get(Files, row_id) is not None
