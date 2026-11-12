"""Database test for the experimental publication journal."""

import contextlib
import datetime
import shutil
import json
import tempfile
import time
import logging
import multiprocessing
import os
import threading
import pytest
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch
from sqlalchemy import select
from watchdog.events import FileCreatedEvent
from viseron.components.storage.check_tier import move_file
from viseron.components.storage.models import Files, PendingMove
from viseron.components.storage.pending_journal import (
    marker_for_path,
    prepare,
    publish_or_verify,
    release_indexed_source,
)
from viseron.components.storage.run_lifecycle import CameraLockBusy, camera_lock
from viseron.components.storage.source_move_intent import (
    marker_for_source,
    prepare_source_intent,
    scan_source_intents,
)
from viseron.domains.camera.fragmenter_process import FragmenterWork
from viseron.components.storage.segment_moves import import_legacy
from viseron.components.storage.tier_handler import TierHandler


def _replay_after_cooldown(handler, event):
    """Advance the retry clock while exercising the real publication handler."""
    now = max(time.monotonic(), getattr(handler, "_test_retry_time", 0)) + 301
    handler._test_retry_time = now
    with patch(
        "viseron.components.storage.tier_handler.time.monotonic", return_value=now
    ):
        handler._on_created(event)


def test_watcher_indexes_journal_metadata_and_acknowledges(tmp_path, get_db_session):
    """Watcher indexes journal metadata and acknowledges."""
    run = tmp_path / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    segment = run / "1.m4s"
    temporary = run / ".viseron-tier-move-test"
    temporary.write_bytes(b"video")
    marker = prepare(
        run,
        segment.name,
        {"orig_ctime": "2026-10-31T14:32:15+00:00", "duration": 1.5, "file_key": 42},
        temp_path=temporary,
    )

    publish_or_verify(marker)
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._event_queue = Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    # Simulate a crash/failure after database commit and before marker removal.
    with (
        patch(
            "viseron.components.storage.tier_handler.os.unlink",
            side_effect=OSError("crash window"),
        ),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        handler._on_created(FileCreatedEvent(str(segment)))
    assert marker.exists()
    with get_db_session() as session:
        assert (
            session.execute(select(Files).where(Files.path == str(segment)))
            .scalar_one()
            .file_key
            == 42
        )
    _replay_after_cooldown(handler, FileCreatedEvent(str(segment)))
    assert not marker.exists()
    with get_db_session() as session:
        row = session.execute(
            select(Files).where(Files.path == str(segment))
        ).scalar_one()
        assert row.duration == 1.5
        assert row.file_key == 42
        assert row.orig_ctime.isoformat().startswith("2026-10-31T14:32:15")


@pytest.mark.parametrize("replace_pending", [False, True])
@pytest.mark.parametrize("stale_destination_inode", [False, True])
def test_move_process_exit_then_startup_replay_indexes_preserved_key(
    tmp_path, get_db_session, replace_pending, stale_destination_inode
):
    """Move process exit then startup replay indexes preserved key."""
    run_name = "20261031T143215.482000Z-a7c93e12ab34"
    source = tmp_path / "tier0" / "camera" / run_name
    source.mkdir(parents=True)
    (source / "init.mp4").write_bytes(b"init")
    src = source / "1.m4s"
    src.write_bytes(b"video")
    cifs_root = os.environ.get("VISERON_TEST_CIFS_ROOT")
    if cifs_root:
        cifs_tier = Path(tempfile.mkdtemp(prefix=".tier-ack-", dir=cifs_root))
        dest = cifs_tier / "camera" / run_name / "1.m4s"
    else:
        dest = tmp_path / "tier1" / "camera" / run_name / "1.m4s"
    original_time = datetime.datetime(
        2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc
    )
    with get_db_session() as session:
        session.add(
            Files(
                tier_id=0,
                tier_path=str(tmp_path / "tier0"),
                camera_identifier="test-journal",
                category="recorder",
                subcategory="segments",
                path=str(src),
                directory=str(src.parent),
                filename=src.name,
                size=5,
                orig_ctime=original_time,
                duration=1.5,
                file_key=42,
            )
        )
        session.commit()
        source_id = session.execute(
            select(Files.id).where(Files.path == str(src))
        ).scalar_one()

    def child_move():
        """Child move."""
        move_file(
            None,
            str(src),
            str(dest),
            logging.getLogger("test"),
            camera_identifier="test-journal",
            publication_meta={
                "orig_ctime": original_time.isoformat(),
                "duration": 1.5,
                "file_key": 42,
                "source_db": {
                    "id": source_id,
                    "tier_id": 0,
                    "tier_path": str(tmp_path / "tier0"),
                },
            },
        )
        os._exit(0)

    process = multiprocessing.get_context("fork").Process(target=child_move)
    process.start()
    process.join(10)
    assert process.exitcode == 0
    assert dest.read_bytes() == b"video"
    # Promotion retains the source and intent until source-tier reconciliation.
    assert src.exists()
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._event_queue = Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={}, pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
        tier_available=lambda _: True,
    )
    completed = Mock()

    def acknowledged():
        with get_db_session() as session:
            assert session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one_or_none() is None
            assert session.execute(
                select(Files.file_key).where(Files.path == str(dest))
            ).scalar_one() == 42
        completed()

    pending = PendingMove(
        dst=str(dest),
        tier_id=1,
        tier_path=str(dest.parent.parent.parent),
        on_complete=acknowledged,
    )
    newer_pending = PendingMove(
        dst=str(dest),
        tier_id=1,
        tier_path=str(dest.parent.parent.parent),
        on_complete=Mock(),
    )
    handler._storage.pending_moves = {str(src): pending}
    handler._storage.pending_moves_lock = threading.Lock()
    handler._storage.camera_tier_handlers = {
        "test-journal": {
            "recorder": [
                {"segments": SimpleNamespace(_path=str(source.parent))},
                {"segments": SimpleNamespace(_path=str(dest.parent.parent))},
            ]
        }
    }
    handler._path = str(dest.parent.parent)
    handler._tier_id = 1
    handler._tier = {"path": str(dest.parent.parent.parent)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    # Legacy markers must be explicitly imported once on upgrade.
    with get_db_session() as session:
        import_legacy(
            session,
            "test-journal",
            str(source.parent),
            str(dest.parent.parent),
            1,
            handler._tier["path"],
        )
    handler._replay_pending_publications()
    event = handler._event_queue.get_nowait()
    # Older journals without source row identity must warn and retain the source.
    marker_path = Path(marker_for_path(str(dest)))
    journal = json.loads(marker_path.read_text())
    if stale_destination_inode:
        # CIFS noserverino can regenerate the client's inode after eviction.
        # Preserve the copied-byte checksum and every source/DB guard.
        journal["identity"][1] += 1
    source_db = journal["meta"].pop("source_db")
    replacement_marker = marker_path.with_name(
        ".viseron-tier-move-pending-test-replacement"
    )
    replacement_marker.write_text(json.dumps(journal))
    os.replace(replacement_marker, marker_path)
    with (
        patch("viseron.components.storage.tier_handler.threading.Timer"),
        patch.object(handler._logger, "warning") as warning,
    ):
        handler._on_created(event)
        handler._on_created(event)
    retained = [
        call for call in warning.call_args_list if "source retained" in call.args[0]
    ]
    assert len(retained) == 1
    assert src.exists() and marker_path.exists()
    # Legacy indexing has no journal-bound row ID. Simulate an operator
    # removing that DB row before restoring identity and retrying the hold.
    with get_db_session() as session:
        legacy_row = session.execute(
            select(Files).where(Files.path == str(dest))
        ).scalar_one()
        session.delete(legacy_row)
        session.commit()
    journal["meta"]["source_db"] = source_db
    replacement_marker.write_text(json.dumps(journal))
    os.replace(replacement_marker, marker_path)

    # Replacement row can have identical metadata but a different durable id.
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        values = {
            key: getattr(row, key)
            for key in (
                "tier_id",
                "tier_path",
                "camera_identifier",
                "category",
                "subcategory",
                "path",
                "directory",
                "filename",
                "size",
                "orig_ctime",
                "duration",
                "file_key",
            )
        }
        session.delete(row)
        session.commit()
        replacement = Files(**values)
        session.add(replacement)
        session.commit()
        assert replacement.id != source_id
    with patch("viseron.components.storage.tier_handler.threading.Timer"):
        _replay_after_cooldown(handler, event)
    assert src.exists()
    assert Path(marker_for_path(str(dest))).exists()
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        session.delete(row)
        session.commit()
        session.add(Files(id=source_id, **values))
        session.commit()

    # A changed tier assignment must prevent unlink, even with matching file key.
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 12
        session.commit()
    with patch("viseron.components.storage.tier_handler.threading.Timer"):
        _replay_after_cooldown(handler, event)
    assert src.exists()
    assert Path(marker_for_path(str(dest))).exists()
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(src))).scalar_one()
        row.tier_id = 0
        session.commit()

    # Content corruption with unchanged inode and length must never acknowledge
    # a RAM-to-NAS promotion or remove its source database row.
    for damaged in (dest, dest.parent / "init.mp4"):
        original = damaged.read_bytes()
        damaged.write_bytes(b"x" * len(original))
        with patch("viseron.components.storage.tier_handler.threading.Timer"):
            _replay_after_cooldown(handler, event)
        assert src.exists() and marker_path.exists() and marker_for_source(src).exists()
        completed.assert_not_called()
        with get_db_session() as session:
            assert (
                session.execute(
                    select(Files.id).where(Files.path == str(src))
                ).scalar_one()
                == source_id
            )
        damaged.write_bytes(original)

    with (
        patch(
            "viseron.components.storage.pending_journal.verify_file_contents",
            side_effect=OSError("CIFS readback failed"),
        ),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        _replay_after_cooldown(handler, event)
    assert src.exists() and marker_path.exists() and marker_for_source(src).exists()
    completed.assert_not_called()

    # Simulate a database outage after unlink; replay must finish safely.
    # Save original before replacing: wrapper above cannot refer to itself.
    real_release = release_indexed_source

    def inject(*args, **kwargs):
        original_guard = kwargs["source_index_guard"]

        @contextlib.contextmanager
        def fail_guard(record, present):
            with original_guard(record, present) as finalize:

                def fail():
                    raise OSError("database failure after unlink")

                yield fail if finalize is not None else None

        kwargs["source_index_guard"] = fail_guard
        return real_release(*args, **kwargs)

    with (
        patch(
            "viseron.components.storage.tier_handler.release_indexed_source",
            side_effect=inject,
        ),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        _replay_after_cooldown(handler, event)
    assert not src.exists()
    assert Path(marker_for_path(str(dest))).exists()
    assert marker_for_source(src).exists()
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one_or_none()
            is not None
        )

    completed.assert_not_called()

    def replace_during_release(*args, **kwargs):
        result = real_release(*args, **kwargs)
        if replace_pending and result == "released":
            with handler._storage.pending_moves_lock:
                handler._storage.pending_moves[str(src)] = newer_pending
        return result

    with patch(
        "viseron.components.storage.tier_handler.release_indexed_source",
        side_effect=replace_during_release,
    ):
        _replay_after_cooldown(handler, event)
    if replace_pending:
        assert handler._storage.pending_moves[str(src)] is newer_pending
        completed.assert_not_called()
    else:
        assert str(src) not in handler._storage.pending_moves
        completed.assert_called_once()
    newer_pending.on_complete.assert_not_called()
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(dest))).scalar_one()
        assert row.file_key == 42
        assert row.duration == 1.5
        assert row.orig_ctime == original_time
        assert (
            session.execute(
                select(Files).where(Files.path == str(src))
            ).scalar_one_or_none()
            is None
        )
    assert not src.exists()
    assert not Path(marker_for_path(str(dest))).exists()
    assert not marker_for_source(src).exists()
    if cifs_root:
        shutil.rmtree(cifs_tier)


def test_conflicting_destination_row_is_never_acknowledged(tmp_path, get_db_session):
    """An existing unrelated path must not be treated as an idempotent replay."""
    run = tmp_path / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    segment = run / "1.m4s"
    segment.write_bytes(b"video")
    meta = {"orig_ctime": "2026-10-31T14:32:15+00:00", "duration": 1.5, "file_key": 42}
    marker = prepare(run, segment.name, meta)
    with get_db_session() as session:
        session.add(
            Files(
                tier_id=0,
                tier_path=str(tmp_path),
                camera_identifier="test-journal",
                category="recorder",
                subcategory="segments",
                path=str(segment),
                directory=str(run),
                filename=segment.name,
                size=5,
                orig_ctime=datetime.datetime.fromisoformat(meta["orig_ctime"]),
                duration=1.5,
                file_key=99,
            )
        )
        session.commit()
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._event_queue = Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    with patch("viseron.components.storage.tier_handler.threading.Timer"):
        handler._on_created(FileCreatedEvent(str(segment)))
    assert marker.exists()
    with get_db_session() as session:
        assert (
            session.execute(select(Files).where(Files.path == str(segment)))
            .scalar_one()
            .file_key
            == 99
        )


def test_transfer_keeps_held_source_without_db_ack(tmp_path):
    """Successful filesystem copy alone does not remove the source."""

    run_name = "20261031T143215.482000Z-a7c93e12ab34"
    run = tmp_path / "tier0" / "camera" / run_name
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    source = run / "1.m4s"
    source.write_bytes(b"video")
    destination = tmp_path / "tier1" / "camera" / run_name / "1.m4s"
    move_file(
        None,
        str(source),
        str(destination),
        logging.getLogger("test"),
        camera_identifier="test-journal",
        publication_meta={
            "orig_ctime": "2026-10-31T14:32:15+00:00",
            "duration": 1.5,
            "file_key": 42,
        },
    )
    assert source.read_bytes() == b"video"
    assert destination.read_bytes() == b"video"
    assert marker_for_source(source).exists()


def test_v2_replay_after_crash_before_rename_with_actual_db(tmp_path, get_db_session):
    """A prepared temp is published and indexed once after a process interruption."""

    if os.environ.get("PUBLICATION_TEST_ROOT"):
        tmp_path = Path(tempfile.mkdtemp(dir=os.environ["PUBLICATION_TEST_ROOT"]))
    run = tmp_path / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    temp = run / ".viseron-tier-move-abcdef12"
    temp.write_bytes(b"video")
    meta = {"orig_ctime": "2026-10-31T14:32:15+00:00", "duration": 1.5, "file_key": 42}
    marker = prepare(run, "1.m4s", meta, temp_path=temp)
    segment = run / "1.m4s"
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._event_queue = Queue()
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    handler._on_created(FileCreatedEvent(str(segment)))
    assert segment.read_bytes() == b"video" and not marker.exists()
    with get_db_session() as session:
        assert (
            session.execute(select(Files).where(Files.path == str(segment)))
            .scalar_one()
            .file_key
            == 42
        )


def test_v2_collision_does_not_index_old_destination(tmp_path, get_db_session):
    """A competing destination cannot be associated with a new journal."""

    if os.environ.get("PUBLICATION_TEST_ROOT"):
        tmp_path = Path(tempfile.mkdtemp(dir=os.environ["PUBLICATION_TEST_ROOT"]))
    run = tmp_path / "camera" / "20261031T143215.482000Z-a7c93e12ab34"
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    temp = run / ".viseron-tier-move-abcdef12"
    temp.write_bytes(b"new")
    meta = {"orig_ctime": "2026-10-31T14:32:15+00:00", "duration": 1.5, "file_key": 42}
    marker = prepare(run, "1.m4s", meta, temp_path=temp)
    segment = run / "1.m4s"
    segment.write_bytes(b"old")
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    with patch("viseron.components.storage.tier_handler.threading.Timer"):
        handler._on_created(FileCreatedEvent(str(segment)))
    assert marker.exists() and temp.exists() and segment.read_bytes() == b"old"
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(segment))
            ).scalar_one_or_none()
            is None
        )


@pytest.mark.parametrize("crash_point", ["intent", "journal"])
def test_source_handover_recovers_after_source_unlink(
    tmp_path, get_db_session, monkeypatch, crash_point
):
    """Database ack and source unlink survive an interrupted intent cleanup."""

    destination_root = tmp_path
    if os.environ.get("PUBLICATION_TEST_ROOT"):
        destination_root = Path(
            tempfile.mkdtemp(dir=os.environ["PUBLICATION_TEST_ROOT"])
        )
        if os.environ.get("PUBLICATION_SOURCE_ON_CIFS"):
            tmp_path = Path(tempfile.mkdtemp(dir=os.environ["PUBLICATION_TEST_ROOT"]))
    run_name = "20261031T143215.482000Z-a7c93e12ab34"
    camera = tmp_path / "tier0" / "camera"
    run = camera / run_name
    run.mkdir(parents=True)
    (run / "init.mp4").write_bytes(b"init")
    source = run / "1.m4s"
    source.write_bytes(b"video")
    if os.environ.get("PUBLICATION_SOURCE_ON_CIFS") and not os.environ.get(
        "PUBLICATION_MTIME_DRIFT"
    ):
        time.sleep(2.5)  # Experiment only: waiting is not a safety guarantee.
    dest_camera = destination_root / "tier1" / "camera"
    dest = dest_camera / run_name / "1.m4s"
    move_file(
        None,
        str(source),
        str(dest),
        logging.getLogger("test"),
        camera_identifier="test-journal",
        publication_meta={
            "orig_ctime": "2026-10-31T14:32:15+00:00",
            "duration": 1.5,
            "file_key": 42,
        },
    )
    marker = Path(marker_for_path(str(dest)))
    intent = marker_for_source(source)
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
    )
    handler._tier_id = 1
    handler._tier = {"path": str(destination_root / "tier1")}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()

    def committed(record):
        with get_db_session() as session:
            row = session.execute(
                select(Files).where(Files.path == str(dest))
            ).scalar_one_or_none()
            return (
                row is not None
                and row.file_key == record["meta"]["file_key"]
                and row.size == record["identity"][2]
                and row.duration == record["meta"]["duration"]
            )

    def release():
        with camera_lock("/tmp/viseron-run-locks", str(dest_camera)):
            return release_indexed_source(
                marker,
                [str(camera)],
                lambda _: True,
                committed,
                "/tmp/viseron-run-locks",
            )

    assert release() == "uncommitted"
    assert source.exists() and intent.exists() and marker.exists()
    with patch.object(handler, "_schedule_journal_retry") as retry:
        handler._on_created(FileCreatedEvent(str(dest)))
    retry.assert_called_once_with(str(dest), delay=5.0)
    assert marker.exists() and source.exists() and intent.exists()
    # A cooperating source writer/cleanup owns this lock: defer release.
    with camera_lock("/tmp/viseron-run-locks", str(camera)):
        with pytest.raises(CameraLockBusy):
            release()
        assert source.exists() and marker.exists() and intent.exists()
    if os.environ.get("PUBLICATION_MTIME_DRIFT"):
        snapshot = json.loads(marker.read_text())["source_snapshot"]
        time.sleep(1.6)
        assert os.stat(source).st_mtime_ns != snapshot["mtime_ns"]
        # Same-size changed bytes must fail closed; restore to exercise retry.
        source.write_bytes(b"other")
        time.sleep(1.6)
        assert release() == "source-changed"
        assert source.read_bytes() == b"other" and marker.exists() and intent.exists()
        source.write_bytes(b"video")
        time.sleep(1.6)
    original_unlink = Path.unlink

    def fail_intent_unlink(path, *args, **kwargs):
        if path == (intent if crash_point == "intent" else marker):
            raise OSError("simulated crash")
        return original_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "unlink", fail_intent_unlink)
        assert release() == "retry"
    assert not source.exists() and marker.exists()
    assert intent.exists() == (crash_point == "intent")
    assert release() == "released"
    assert not source.exists() and not intent.exists() and not marker.exists()
    assert dest.read_bytes() == b"video"


def test_first_tier_journal_allocates_stable_key_and_keeps_staging(
    tmp_path, get_db_session
):
    """First-tier indexing survives post-commit failure with the same key."""

    run_name = "20261031T143215.482000Z-a7c93e12ab34"
    stage = tmp_path / "stage" / run_name
    stage.mkdir(parents=True)
    (stage / "init.mp4").write_bytes(b"init")
    source = stage / "1.m4s"
    source.write_bytes(b"video")
    tier = tmp_path / "segments" / "camera"
    worker = FragmenterWork(
        "test-journal", str(stage.parent), str(tier), None, None, None
    )
    original_time = datetime.datetime(
        2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc
    )
    worker._move_to_segments_folder(f"{run_name}/1.m4s", original_time, 1.5)
    dest = tier / run_name / "1.m4s"
    marker = Path(marker_for_path(str(dest)))
    intent = marker_for_source(source)
    assert source.read_bytes() == b"video" and intent.exists() and marker.exists()
    assert json.loads(marker.read_text())["meta"]["file_key"] is None
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    with (
        patch.object(
            handler,
            "_on_created_impl",
            side_effect=OSError("simulated database failure"),
        ),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        handler._on_created(FileCreatedEvent(str(dest)))
    assert source.exists() and intent.exists() and marker.exists()
    key = json.loads(marker.read_text())["meta"]["file_key"]
    assert isinstance(key, int)
    with get_db_session() as session:
        assert (
            session.execute(
                select(Files).where(Files.path == str(dest))
            ).scalar_one_or_none()
            is None
        )
    with patch.object(handler, "_schedule_journal_retry") as retry:
        _replay_after_cooldown(handler, FileCreatedEvent(str(dest)))
        assert json.loads(marker.read_text())["meta"]["file_key"] == key
        with get_db_session() as session:
            row = session.execute(
                select(Files).where(Files.path == str(dest))
            ).scalar_one()
            assert row.file_key == key and row.duration == 1.5
        _replay_after_cooldown(handler, FileCreatedEvent(str(dest)))
    assert retry.call_count == 2
    assert json.loads(marker.read_text())["meta"]["file_key"] == key
    with get_db_session() as session:
        assert (
            session.execute(select(Files).where(Files.path == str(dest)))
            .scalar_one()
            .file_key
            == key
        )

    def committed(record):
        with get_db_session() as session:
            row = session.execute(
                select(Files).where(Files.path == str(dest))
            ).scalar_one_or_none()
            return (
                row is not None
                and row.file_key == record["meta"]["file_key"]
                and row.size == record["identity"][2]
            )

    with camera_lock("/tmp/viseron-run-locks", str(tier)):
        assert (
            release_indexed_source(
                marker,
                [str(stage.parent)],
                lambda _: True,
                committed,
                "/tmp/viseron-run-locks",
            )
            == "released"
        )
    assert not source.exists() and not marker.exists() and not intent.exists()
    assert dest.read_bytes() == b"video"


@pytest.mark.parametrize("corruption", [None, "segment", "init"])
def test_first_tier_release_is_driven_by_real_handler(
    tmp_path, get_db_session, monkeypatch, corruption
):
    """The real watcher callback releases the spool only after committed indexing."""

    run = "20261031T143215.482000Z-a7c93e12ab34"
    camera = tmp_path / "segments" / "camera"
    stage = camera.parent / ".viseron-spool" / camera.name / run
    stage.mkdir(parents=True)
    (stage / "init.mp4").write_bytes(b"init")
    source = stage / "1.m4s"
    source.write_bytes(b"video")
    worker = FragmenterWork(
        "test-journal", str(stage.parent), str(camera), None, None, None
    )
    timestamp = datetime.datetime(
        2026, 10, 31, 14, 32, 15, tzinfo=datetime.timezone.utc
    )
    worker._move_to_segments_folder(f"{run}/1.m4s", timestamp, 1.5)
    dest = camera / run / "1.m4s"
    marker = marker_for_path(str(dest))
    intent = marker_for_source(source)
    handler = object.__new__(TierHandler)
    handler._camera = SimpleNamespace(identifier="test-journal")
    handler._logger = logging.getLogger("test")
    handler._journal_retry_lock = threading.Lock()
    handler._journal_retry_timer = None
    handler._journal_retry_paths = set()
    handler._journal_retry_rescan = False
    handler._journal_retry_stopping = False
    handler._storage = SimpleNamespace(
        temporary_files_meta={},
        pending_moves_lock=threading.Lock(),
        get_session=get_db_session,
        tier_available=lambda _: True,
    )
    handler._tier_id = 0
    handler._tier = {"path": str(tmp_path)}
    handler._category = "recorder"
    handler._subcategory = "segments"
    handler._vis = Mock()
    handler.check_tier = Mock()
    with (
        patch.object(handler, "_on_created_impl", side_effect=OSError("database down")),
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        handler._on_created(FileCreatedEvent(str(dest)))
    assert source.exists() and intent.exists() and os.path.exists(marker)

    if corruption is not None:
        damaged = dest if corruption == "segment" else dest.parent / "init.mp4"
        original = damaged.read_bytes()
        damaged.write_bytes(b"x" * len(original))
        with patch("viseron.components.storage.tier_handler.threading.Timer"):
            _replay_after_cooldown(handler, FileCreatedEvent(str(dest)))
        assert source.exists(), "Unverified destination must not release its source"
        assert intent.exists() and os.path.exists(marker)
        damaged.write_bytes(original)

    original_unlink = Path.unlink

    def fail_source_intent_unlink(path, *args, **kwargs):
        if path == intent:
            raise OSError("crash after source unlink")
        return original_unlink(path, *args, **kwargs)

    with (
        monkeypatch.context() as patcher,
        patch("viseron.components.storage.tier_handler.threading.Timer"),
    ):
        patcher.setattr(Path, "unlink", fail_source_intent_unlink)
        _replay_after_cooldown(handler, FileCreatedEvent(str(dest)))
    assert not source.exists() and intent.exists() and os.path.exists(marker)
    _replay_after_cooldown(handler, FileCreatedEvent(str(dest)))
    assert not source.exists() and not intent.exists() and not os.path.exists(marker)
    assert dest.read_bytes() == b"video"
    with get_db_session() as session:
        row = session.execute(select(Files).where(Files.path == str(dest))).scalar_one()
        assert row.duration == 1.5 and row.orig_ctime == timestamp


def test_incomplete_source_intent_is_reported_and_preserved(tmp_path):
    """Report stale incomplete holds while preserving the source."""

    run = "20261031T143215.482000Z-a7c93e12ab34"
    camera = tmp_path / "source" / "camera"
    source = camera / run / "1.m4s"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"video")
    dest = tmp_path / "destination" / "camera" / run / "1.m4s"
    marker = prepare_source_intent(str(source), str(dest), source.stat(), 42)
    reports = []
    assert (
        scan_source_intents(
            str(camera),
            None,
            lambda _: True,
            [str(dest.parent.parent)],
            str(tmp_path / "locks"),
            report_incomplete=reports.append,
        )
        == 1
    )
    assert reports == []
    old = time.time() - 600
    os.utime(marker, (old, old))
    with camera_lock(str(tmp_path / "locks"), str(dest.parent.parent)):
        assert (
            scan_source_intents(
                str(camera),
                None,
                lambda _: True,
                [str(dest.parent.parent)],
                str(tmp_path / "locks"),
                report_incomplete=reports.append,
            )
            == 1
        )
    assert reports == []
    assert (
        scan_source_intents(
            str(camera),
            None,
            lambda _: True,
            [str(dest.parent.parent)],
            str(tmp_path / "locks"),
            report_incomplete=reports.append,
        )
        == 1
    )
    assert reports == [str(marker)]
    assert source.read_bytes() == b"video"
    assert marker.exists()


def test_scanner_never_clears_hold_after_source_unlinked(tmp_path):
    """The scanner must leave release and acknowledgement to journal replay."""

    camera = tmp_path / "source" / "camera"
    source = camera / "20261031T143215.482000Z-a7c93e12ab34" / "1.m4s"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"video")
    dest = tmp_path / "dest" / "camera" / source.parent.name / source.name
    marker = prepare_source_intent(str(source), str(dest), source.stat(), 42)
    record = json.loads(marker.read_text())
    record.update(sha256="a" * 64, init_sha256="b" * 64)
    marker.write_text(json.dumps(record))
    source.unlink()
    old = time.time() - 600
    os.utime(marker, (old, old))
    reports = []
    with patch(
        "viseron.components.storage.source_move_intent.verify_destination_publication",
        side_effect=AssertionError("remote readback"),
    ):
        scan_source_intents(
            str(camera),
            None,
            lambda _: True,
            [str(dest.parent.parent)],
            str(tmp_path / "locks"),
            report_incomplete=reports.append,
        )
    assert reports == [str(marker)]
    assert marker.exists()


def test_retry_rejects_replaced_source_database_identity(tmp_path):
    """A changed source row identity cannot reuse a prior intent."""

    src = tmp_path / "src.m4s"
    src.write_bytes(b"video")
    dest = tmp_path / "dest.m4s"
    prepare_source_intent(
        str(src),
        str(dest),
        src.stat(),
        42,
        source_db={"id": 1, "tier_id": 0, "tier_path": "/tier0"},
    )
    with pytest.raises(ValueError, match="conflicting source move intent"):
        prepare_source_intent(
            str(src),
            str(dest),
            src.stat(),
            42,
            source_db={"id": 2, "tier_id": 0, "tier_path": "/tier0"},
        )
    assert src.read_bytes() == b"video"
