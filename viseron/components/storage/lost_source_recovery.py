"""Bounded reconciliation of indexed transfers whose volatile source run was lost."""

import datetime
import json
import os
from pathlib import Path

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import SQLAlchemyError

from viseron.components.storage import pending_journal as journal
from viseron.components.storage.asset_moves import ASSET_FIELDS, asset_locks
from viseron.components.storage.io_retry import (
    FilesystemRetry,
    PublicationRetry,
    StorageRetryLater,
)
from viseron.components.storage.models import Files, PendingSegmentMove, Recordings
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
)
from viseron.components.storage.source_move_intent import marker_for_source
from viseron.components.storage.startup_reconciliation import (
    missing_volatile_path,
    recovery_destination_available,
    recovery_destination_root,
)
from viseron.components.storage.util import tier_markers_available

LOCK_ROOT = "/tmp/viseron-run-locks"
RECEIPT_VERSION = 2
MAX_RECEIPT_BYTES = 65536


def _absent(path):
    try:
        os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return True
    return False


def _confirmed_namespace(path, markers, namespace_root):
    """A configured persistent marker identifies the expected storage namespace."""
    covered = {
        root: marker
        for root, marker in (markers or {}).items()
        if os.path.commonpath((os.path.abspath(root), os.path.abspath(path)))
        == os.path.abspath(root)
        and os.path.commonpath((namespace_root, os.path.abspath(root)))
        == namespace_root
    }
    return bool(covered) and tier_markers_available(path, covered)


def _lost_run(move, source_tier):
    return (
        missing_volatile_path(move.source, source_tier)
        and _absent(Path(move.source).parent)
        and _absent(marker_for_source(move.source))
    )


def _pending_identity(move):
    return tuple(
        getattr(move, key)
        for key in (
            "id",
            "source",
            "destination",
            "camera_identifier",
            "source_root",
            "destination_root",
            "destination_tier_id",
            "destination_tier_path",
            "metadata_record",
            "size",
        )
    )


def _destination_matches(row, move):
    meta = move.metadata_record
    return (
        row.camera_identifier,
        row.category,
        row.subcategory,
        row.file_key,
        row.tier_id,
        row.tier_path,
        row.size,
        row.duration,
        row.orig_ctime,
    ) == (
        move.camera_identifier,
        "recorder",
        "segments",
        meta["file_key"],
        move.destination_tier_id,
        move.destination_tier_path,
        move.size,
        meta["duration"],
        datetime.datetime.fromisoformat(meta["orig_ctime"]),
    )


def _trusted_receipt(record, move):
    meta, snapshot = record["meta"], record["source_snapshot"]
    return (
        record["version"] == RECEIPT_VERSION
        and record["filename"] == Path(move.destination).name
        and Path(record["temp_name"]).name == record["temp_name"]
        and record["temp_name"].startswith(".viseron-tier-move-")
        and record["source_intent"] == str(marker_for_source(move.source))
        and snapshot["source"] == move.source
        and snapshot["destination"] == move.destination
        and snapshot["size"] == record["identity"][2] == move.size
        and snapshot["file_key"] == move.metadata_record["file_key"]
        and snapshot["source_db"] == move.metadata_record["source_db"]
        and all(
            meta.get(key) == move.metadata_record[key]
            for key in ("file_key", "source_db", "orig_ctime", "duration")
        )
        and meta.get("pending_move_id", move.id) == move.id
    )


def reconcile_segment(
    get_session,
    move,
    *,
    source_tier,
    source_root,
    destination_tier,
    destination_root,
    destination_id,
    available,
    markers=None,
):
    """Return a terminal result, deferred evidence, or None for ordinary replay.

    Only exact database paths are inspected. The caller resolves both locations
    against configured tiers. This never deletes source or destination media.
    """
    if (
        move.size <= 0
        or move.metadata_record.get("duration") is None
        or move.metadata_record["duration"] <= 0
        or move.camera_identifier != Path(source_root).name
        or move.source_root != os.path.abspath(source_root)
        or move.destination_root != os.path.abspath(destination_root)
        or move.metadata_record["source_db"]["tier_path"] != source_tier
        or move.destination_tier_path != destination_tier
        or move.destination_tier_id != destination_id
        or Path(move.source).parent.parent != Path(source_root)
        or Path(move.destination).parent.parent != Path(destination_root)
        or not is_run_directory(Path(move.source).parent.name)
        or Path(move.source).parts[-2:] != Path(move.destination).parts[-2:]
    ):
        return "deferred"
    if not available(source_root):
        return "deferred"
    with camera_lock(LOCK_ROOT, os.path.abspath(source_root)):
        if not _lost_run(move, source_tier):
            return None
    if not recovery_destination_available(
        destination_root, destination_tier, available
    ):
        return "deferred"
    with (
        camera_lock(LOCK_ROOT, os.path.abspath(destination_root)),
        get_session() as session,
    ):
        if os.path.realpath(move.destination) != os.path.abspath(move.destination):
            return "deferred"
        saved = session.execute(
            select(PendingSegmentMove)
            .where(PendingSegmentMove.id == move.id)
            .with_for_update()
        ).scalar_one_or_none()
        if saved is None:
            return "completed"
        if _pending_identity(saved) != _pending_identity(move):
            return "deferred"
        if session.execute(select(Files.id).where(Files.path == move.source)).first():
            return "deferred"
        rows = (
            session.execute(
                select(Files)
                .where(
                    or_(
                        Files.file_key == move.metadata_record["file_key"],
                        Files.path == move.destination,
                    )
                )
                .with_for_update()
            )
            .scalars()
            .all()
        )
        if any(
            row.path != move.destination or not _destination_matches(row, move)
            for row in rows
        ):
            return "deferred"
        target = rows[0] if rows else None
        marker = Path(journal.marker_for_path(move.destination))
        record = None
        if not _absent(marker):
            if marker.stat().st_size > MAX_RECEIPT_BYTES:
                return "deferred"
            record = json.loads(marker.read_text())
            if not _trusted_receipt(record, move):
                return "deferred"
        present = not _absent(move.destination)
        temporary = marker.parent / record["temp_name"] if record else None
        has_temporary = temporary is not None and not _absent(temporary)
        if not present and not has_temporary:
            if not _confirmed_namespace(
                move.destination,
                markers,
                recovery_destination_root(destination_root, destination_tier),
            ):
                return "deferred"
            if not recovery_destination_available(
                destination_root, destination_tier, available
            ):
                return "deferred"
            # Keep the database job until its receipt has been removed. A crash
            # before commit remains discoverable by the next bounded replay.
            with camera_lock(LOCK_ROOT, os.path.abspath(source_root)):
                if not available(source_root) or not _lost_run(move, source_tier):
                    return "deferred"
            if record is not None:
                marker.unlink()
                journal.sync_dir(marker.parent)
            with camera_lock(LOCK_ROOT, os.path.abspath(source_root)):
                if (
                    not available(source_root)
                    or not _lost_run(move, source_tier)
                    or session.execute(
                        select(Files.id).where(Files.path == move.source)
                    ).first()
                ):
                    return "deferred"
                if target is not None:
                    session.delete(target)
                session.delete(saved)
                session.commit()
            return "missing"
        if record is None:
            # The ordinary already-completed handoff path owns journal-free rows.
            return None if target is not None else "deferred"
        if target is not None and record.get("destination_id") != target.id:
            return "deferred"  # Never adopt an unbound or conflicting existing row.
        if present and has_temporary:
            return "deferred"
        candidate = Path(move.destination) if present else temporary
        current = journal.identity(candidate)
        if (
            current[2] != move.size
            or not journal.verify_file_contents(
                candidate, record.get("copied_digest"), current
            )
            or not journal.verify_file_contents(
                marker.parent / "init.mp4", record.get("init_digest")
            )
        ):
            return "deferred"
        record = journal.publish_or_verify(marker)
        if target is None:
            if record.get("destination_id") is None:
                row_id = session.execute(
                    select(func.nextval(func.pg_get_serial_sequence("files", "id")))
                ).scalar_one()
                record = journal.assign_destination_id(marker, row_id)
            target = Files(
                id=record["destination_id"],
                path=move.destination,
                directory=str(marker.parent),
                filename=record["filename"],
                camera_identifier=move.camera_identifier,
                category="recorder",
                subcategory="segments",
                tier_id=move.destination_tier_id,
                tier_path=move.destination_tier_path,
                size=move.size,
                file_key=move.metadata_record["file_key"],
                duration=move.metadata_record["duration"],
                orig_ctime=datetime.datetime.fromisoformat(
                    move.metadata_record["orig_ctime"]
                ),
            )
            session.add(target)
        if not recovery_destination_available(
            destination_root, destination_tier, available
        ):
            return "deferred"
        with camera_lock(LOCK_ROOT, os.path.abspath(source_root)):
            if (
                not available(source_root)
                or not _lost_run(move, source_tier)
                or session.execute(
                    select(Files.id).where(Files.path == move.source)
                ).first()
            ):
                session.rollback()
                return "deferred"
            session.commit()  # Index durably before removing the publication receipt.
        marker.unlink()
        journal.sync_dir(marker.parent)
        # A crash here is covered by the existing journal-free completion path.
        with camera_lock(LOCK_ROOT, os.path.abspath(source_root)):
            if (
                not available(source_root)
                or not _lost_run(move, source_tier)
                or session.execute(
                    select(Files.id).where(Files.path == move.source)
                ).first()
            ):
                return "deferred"
            session.delete(saved)
            session.commit()
        return "completed"


def reconcile_asset(
    get_session,
    file_id,
    *,
    source_tier,
    source_root,
    destination_tier,
    destination_root,
    destination_id,
    available,
    markers=None,
):
    """Remove confirmed missing asset locations and links, preserving events."""
    with get_session() as session:
        row = session.get(Files, file_id)
        if row is None or row.move_target is None:
            return "completed"
        if (
            row.category != "recorder"
            or row.subcategory not in ASSET_FIELDS
            or row.camera_identifier != Path(source_root).name
        ):
            return "deferred"
        source, target = row.path, dict(row.move_target)
        expected = (
            row.file_key,
            row.camera_identifier,
            row.category,
            row.subcategory,
            row.tier_id,
            row.tier_path,
            row.size,
            row.orig_ctime,
        )
        if not available(source_root):
            return "deferred"
        if not missing_volatile_path(source, source_tier):
            return None
        if (
            row.tier_path != source_tier
            or Path(source).parent != Path(source_root)
            or target["tier_path"] != destination_tier
            or target["tier_id"] != destination_id
            or Path(target["path"]).parent != Path(destination_root)
        ):
            return "deferred"
    if not recovery_destination_available(
        destination_root, destination_tier, available
    ):
        return "deferred"
    with asset_locks(source, target["path"]), get_session() as session:
        row = session.execute(
            select(Files).where(Files.id == file_id).with_for_update()
        ).scalar_one_or_none()
        if row is None or row.move_target is None:
            return "completed"
        if (
            row.path != source
            or row.move_target != target
            or (
                row.file_key,
                row.camera_identifier,
                row.category,
                row.subcategory,
                row.tier_id,
                row.tier_path,
                row.size,
                row.orig_ctime,
            )
            != expected
            or not available(source_root)
            or not missing_volatile_path(source, source_tier)
        ):
            return "deferred"
        if not _absent(target["path"]):
            # Legacy asset intents contain no copied-byte digest. Size alone is
            # not proof that the surviving image/clip belongs to this transfer.
            return "deferred"
        if (
            row.subcategory not in ASSET_FIELDS
            or session.execute(
                select(Files.id).where(
                    or_(Files.file_key == row.file_key, Files.path == target["path"]),
                    Files.id != row.id,
                )
            ).first()
        ):
            return "deferred"
        if not recovery_destination_available(
            destination_root, destination_tier, available
        ):
            return "deferred"
        if not _confirmed_namespace(
            target["path"],
            markers,
            recovery_destination_root(destination_root, destination_tier),
        ):
            return "deferred"
        field = getattr(Recordings, ASSET_FIELDS[row.subcategory])
        session.execute(
            update(Recordings)
            .where(
                Recordings.camera_identifier == row.camera_identifier,
                field.in_((source, target["path"])),
            )
            .values({field: None})
        )
        session.delete(row)
        session.commit()
        return "missing"


class LostSourceRecovery:
    """Share outage cooldown and bounded held-path backoff for background replay."""

    def __init__(self, logger):
        self._logger = logger
        self._io = FilesystemRetry(logger)
        self._paths = PublicationRetry(logger)

    def attempt(self, source, destination, operation, probe=None):
        """Return whether this path was handled or retained for a later pass."""
        try:
            # Historical NAS cooldown must not delay a surviving source's
            # ordinary copy/replay. This probe only accesses proven local RAM.
            if probe is not None and not probe():
                return False
            if self._paths.remaining(source):
                return True
            with self._io.attempt("lost-source", source, destination) as attempt:
                result = operation()
                if result is None:
                    return False
                if result != "deferred":
                    attempt.completed = True
                    self._paths.complete(source)
                    self._logger.info(
                        "Reconciled %s volatile transfer: %s", result, source
                    )
                    return True
        except (CameraLockBusy, StorageRetryLater):
            return True
        except (OSError, ValueError, KeyError, TypeError, SQLAlchemyError):
            pass  # Retain all evidence; shared retry gates bound work and logs.
        self._paths.defer(source)
        return True
