"""Real tmpfs pressure through the worker, transfer commit and source cleanup."""

import datetime
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from viseron.components.storage import Storage
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.files import resolve_file_id, upsert_file_at_location
from viseron.components.storage.storage_subprocess import DataItemDeleteFile
from viseron.components.storage.tier_handler import move_file
from tests.components.storage.test_recording_policy import check_item, run_check


@pytest.mark.skipif(
    not os.environ.get("VISERON_POLICY_RAM_ROOT"),
    reason="requires dedicated local tmpfs",
)
@pytest.mark.parametrize("offline", [False, True])
def test_real_ram_exhaustion_promotes_or_reclaims(
    get_db_session, tmp_path, vis, monkeypatch, offline
):
    """Prefer verified NAS copies; during an outage stop deleting after recovery."""
    with tempfile.TemporaryDirectory(
        dir=os.environ["VISERON_POLICY_RAM_ROOT"]
    ) as ram_dir:
        ram, nas = Path(ram_dir), tmp_path / "nas"
        source = ram / "segments" / "camera"
        destination = nas / "segments" / "camera"
        source.mkdir(parents=True)
        destination.mkdir(parents=True)
        (source / "init.mp4").write_bytes(b"matching init")
        paths, ids = [], []
        now = datetime.datetime.now(datetime.UTC)
        for index in range(3):
            path = source / f"{index}.m4s"
            path.write_bytes(bytes([index + 1]) * (8 * 1024**2))
            published = upsert_file_at_location(
                get_db_session,
                "camera",
                "recorder",
                "segments",
                0,
                str(ram),
                str(path),
                orig_ctime=now - datetime.timedelta(seconds=240 - index * 30),
                duration=5,
            )
            paths.append(path)
            ids.append(published.file_id)
        (ram / "fill").write_bytes(b"f" * (36 * 1024**2))
        assert shutil.disk_usage(ram).free < 8 * 1024**2
        original_statvfs = os.statvfs

        def statvfs(path):
            if offline and str(path).startswith(str(nas)):
                raise OSError("NAS outage")
            return original_statvfs(path)

        monkeypatch.setattr(os, "statvfs", statvfs)
        item = check_item(ram, nas)
        actions = run_check(get_db_session, item)
        assert len(actions) == 3
        assert all(a.operation == ("delete" if offline else "move") for a in actions)
        worker = Worker()
        worker._get_session = get_db_session
        storage = Mock(spec=Storage)
        storage.get_session = get_db_session
        storage.temporary_files_meta = {}
        storage.temporary_moving_files = set()
        storage.camera_tier_handlers = {}
        storage.configured_tier_paths.return_value = [str(ram), str(nas)]
        storage.camera_requested_files_count = {
            "camera": SimpleNamespace(filenames=set())
        }
        verified = []

        def send(operation, callback=None):
            worker.work_input(operation)
            if getattr(operation, "published", False):
                assert Path(operation.src).exists()
                assert (
                    hashlib.sha256(Path(operation.src).read_bytes()).digest()
                    == hashlib.sha256(Path(operation.dst).read_bytes()).digest()
                )
                verified.append(operation.src)
            if callback is not None:
                callback(operation)

        storage.tier_check_worker_send_command.side_effect = send
        for action in actions:
            if offline:
                deletion = DataItemDeleteFile(
                    cmd="delete_file",
                    src=action.src,
                    camera_identifier="camera",
                    recording_policy="preserve_events",
                )
                worker.work_input(deletion)
                assert deletion.error is None
            else:
                move_file(
                    vis,
                    storage,
                    get_db_session,
                    "camera",
                    0,
                    "recorder",
                    "segments",
                    action.src,
                    str(destination / Path(action.src).name),
                    str(nas),
                    logging.getLogger(__name__),
                    dst_tier_id=1,
                )
        assert shutil.disk_usage(ram).free >= 16 * 1024**2
        if offline:
            assert not paths[0].exists() and not paths[1].exists()
            assert paths[2].exists(), (
                "Do not delete queued newest footage after pressure clears"
            )
        else:
            assert all(not path.exists() for path in paths)
            assert len(verified) >= 3
            for file_id, path in zip(ids, paths, strict=True):
                resolved = resolve_file_id(get_db_session, storage, file_id)
                assert resolved is not None and resolved.path == str(
                    destination / path.name
                )
                assert Path(resolved.path).read_bytes() == bytes(
                    [paths.index(path) + 1]
                ) * (8 * 1024**2)
