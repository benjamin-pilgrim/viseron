"""Database discovery of pending promotions; filesystem journals retain proof."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from viseron.components.storage.const import ENGINE
from viseron.components.storage.models import Files, PendingSegmentMove
from viseron.components.storage.pending_journal import PREFIX as DESTINATION_PREFIX
from viseron.components.storage.pending_journal import (
    DATABASE_SOURCE_VERSION,
    marker_for_path,
    protected_by_pending,
)
from viseron.components.storage.run_lifecycle import (
    CameraLockBusy,
    camera_lock,
    is_run_directory,
)
from viseron.components.storage.source_move_intent import PREFIX as SOURCE_PREFIX
from viseron.components.storage.source_move_intent import marker_for_source


def reserve(
    session: Session, row: Files, destination: str, tier_id: int, tier_path: str
) -> PendingSegmentMove:
    """Commit this reservation before submitting any filesystem work."""
    if session.execute(
        select(PendingSegmentMove.id)
        .where(PendingSegmentMove.destination == row.path)
        .limit(1)
    ).first():
        raise BlockingIOError("Source still has an unfinished incoming promotion")
    metadata = {
        "orig_ctime": row.orig_ctime.isoformat(),
        "duration": row.duration,
        "file_key": row.file_key,
        "source_db": {"id": row.id, "tier_id": row.tier_id, "tier_path": row.tier_path},
    }
    pending = session.execute(
        select(PendingSegmentMove)
        .where(PendingSegmentMove.source == row.path)
        .with_for_update()
    ).scalar_one_or_none()
    if pending is not None:
        if (
            pending.destination,
            pending.destination_tier_id,
            pending.destination_tier_path,
            pending.camera_identifier,
            pending.metadata_record,
            pending.size,
        ) != (
            destination,
            tier_id,
            tier_path,
            row.camera_identifier,
            metadata,
            row.size,
        ):
            raise ValueError(
                "Pending segment move conflicts with source or destination"
            )
        return pending
    pending = PendingSegmentMove(
        source=row.path,
        destination=destination,
        camera_identifier=row.camera_identifier,
        source_root=os.path.dirname(row.directory),
        destination_root=os.path.dirname(os.path.dirname(destination)),
        destination_tier_id=tier_id,
        destination_tier_path=tier_path,
        metadata_record=metadata,
        size=row.size,
    )
    session.add(pending)
    session.flush()
    return pending


def begin_copy(session: Session, source: str, destination: str, metadata: dict) -> int:
    """Reserve an active copy under the caller's source camera lock, before writes.

    Planned but unstarted moves remain ordinary Files rows, so pressure may still
    reclaim them. The worker must revalidate that row before beginning any I/O.
    """
    if protected_by_pending(source):
        raise CameraLockBusy("Source publication has not completed")
    row = session.execute(
        select(Files).where(Files.path == source).with_for_update()
    ).scalar_one()
    if (
        metadata.get("source_db")
        != {"id": row.id, "tier_id": row.tier_id, "tier_path": row.tier_path}
        or metadata.get("file_key") != row.file_key
        or metadata.get("orig_ctime") != row.orig_ctime.isoformat()
        or metadata.get("duration") != row.duration
    ):
        raise ValueError("Segment source changed before copy")
    pending = reserve(
        session,
        row,
        destination,
        metadata["destination_tier_id"],
        metadata["destination_tier_path"],
    )
    if metadata.get("pending_move_id", pending.id) != pending.id:
        raise ValueError("Segment retry has another reservation identity")
    identity = pending.id
    session.commit()
    return identity


def receipt_matches(session: Session, receipt: dict) -> bool:
    """Lock and validate the reservation authorizing a marker-free source copy."""
    if receipt.get("source_tracking") != "database":
        return True  # Legacy transfers are authorized by their source intents.
    meta, snapshot = receipt["meta"], receipt["source_snapshot"]
    identity = meta.get("pending_move_id")
    if not isinstance(identity, int) or identity <= 0:
        return False
    pending = session.execute(
        select(PendingSegmentMove)
        .where(PendingSegmentMove.id == identity)
        .with_for_update()
    ).scalar_one_or_none()
    if pending is None:
        return False
    destination = str(Path(snapshot["destination"]))
    return (
        receipt.get("version") == DATABASE_SOURCE_VERSION
        and receipt.get("source_tracking") == "database"
        and "source_intent" not in receipt
        and pending.camera_identifier == Path(pending.source_root).name
        and pending.camera_identifier == Path(pending.destination_root).name
        and snapshot["source"] == pending.source
        and destination == pending.destination
        and receipt["filename"] == Path(destination).name
        and pending.source_root == str(Path(snapshot["source"]).parent.parent)
        and pending.destination_root == str(Path(destination).parent.parent)
        and snapshot["size"] == receipt["identity"][2] == pending.size
        and snapshot["file_key"] == pending.metadata_record["file_key"]
        and snapshot["source_db"] == pending.metadata_record["source_db"]
        and meta.get("destination_tier_id") == pending.destination_tier_id
        and meta.get("destination_tier_path") == pending.destination_tier_path
        and all(
            meta.get(key) == pending.metadata_record[key]
            for key in ("source_db", "file_key", "orig_ctime", "duration")
        )
    )


def protects(session: Session, path: str) -> bool:
    """Reservations protect both paths before filesystem markers exist."""
    return (
        session.execute(
            select(PendingSegmentMove.id)
            .where(
                or_(
                    PendingSegmentMove.source == path,
                    PendingSegmentMove.destination == path,
                )
            )
            .limit(1)
        ).first()
        is not None
    )


def acknowledge(
    session: Session, source: str, destination: str, metadata: dict
) -> None:
    """Called only after proven source release and journal cleanup."""
    if not metadata.get("source_db"):
        return
    pending = session.execute(
        select(PendingSegmentMove)
        .where(
            PendingSegmentMove.source == source,
            PendingSegmentMove.destination == destination,
        )
        .with_for_update()
    ).scalar_one_or_none()
    if pending is None:
        return
    if (
        metadata.get("pending_move_id", pending.id) != pending.id
        or metadata.get("source_db") != pending.metadata_record["source_db"]
        or metadata.get("file_key") != pending.metadata_record["file_key"]
    ):
        raise ValueError("Pending segment acknowledgement does not match reservation")
    session.delete(pending)
    session.commit()


def finish_completed(
    session: Session, pending: PendingSegmentMove, tier_available
) -> bool:
    """Clear only already-finished work; never authorize deletion from DB metadata."""

    if not tier_available(pending.destination) or not tier_available(pending.source):
        return False
    with camera_lock("/tmp/viseron-run-locks", pending.destination_root):
        if os.path.lexists(marker_for_path(pending.destination)) or not os.path.isfile(
            pending.destination
        ):
            return False
        with camera_lock("/tmp/viseron-run-locks", pending.source_root):
            if any(
                os.path.lexists(path)
                for path in (
                    pending.source,
                    marker_for_source(pending.source),
                )
            ):
                return False
            if not os.path.isdir(pending.source_root):
                return False
            if session.execute(
                select(Files.id).where(Files.path == pending.source)
            ).first():
                return False
            row = session.execute(
                select(Files).where(Files.path == pending.destination)
            ).scalar_one_or_none()
            meta = pending.metadata_record
            if row is None or (
                row.camera_identifier,
                row.category,
                row.subcategory,
                row.file_key,
                row.tier_id,
                row.tier_path,
                row.size,
                row.orig_ctime.isoformat(),
                row.duration,
            ) != (
                pending.camera_identifier,
                "recorder",
                "segments",
                meta["file_key"],
                pending.destination_tier_id,
                pending.destination_tier_path,
                pending.size,
                meta["orig_ctime"],
                meta["duration"],
            ):
                return False
            acknowledge(
                session,
                pending.source,
                pending.destination,
                dict(meta, pending_move_id=pending.id),
            )
            return True


def import_legacy(
    session: Session,
    camera: str,
    source_root: str,
    destination_root: str,
    tier_id: int,
    tier_path: str,
) -> int:
    """Explicit one-time import of old markers; never copy, release or delete media.

    Stop Viseron before importing. Normal recovery never calls this archive walk.
    """

    source_root, destination_root = (
        os.path.abspath(source_root),
        os.path.abspath(destination_root),
    )

    def unreadable_directory(error):
        raise error

    imported = 0
    # A source row can be gone while its hold still exists. Destination journals
    # retain the complete metadata needed to discover that interrupted cleanup.
    for root, prefix in (
        (destination_root, DESTINATION_PREFIX),
        (source_root, SOURCE_PREFIX),
    ):
        for directory, _, names in os.walk(root, onerror=unreadable_directory):
            if not is_run_directory(os.path.basename(directory)):
                continue
            for name in names:
                if not name.startswith(prefix) or not name.endswith(".m4s.json"):
                    continue
                record = json.loads((Path(directory) / name).read_text())
                source = record.get("source") or record.get("source_snapshot", {}).get(
                    "source"
                )
                destination = record.get("destination") or str(
                    Path(directory) / record["filename"]
                )
                if (
                    not isinstance(source, str)
                    or os.path.dirname(os.path.dirname(source)) != source_root
                    or os.path.dirname(os.path.dirname(destination)) != destination_root
                    or os.path.basename(source) != os.path.basename(destination)
                    or os.path.basename(os.path.dirname(source))
                    != os.path.basename(os.path.dirname(destination))
                ):
                    raise ValueError("Legacy move is outside configured roots")
                existing = session.execute(
                    select(PendingSegmentMove).where(
                        PendingSegmentMove.source == source
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    metadata = record.get("meta", record)
                    if (
                        existing.destination != destination
                        or existing.camera_identifier != camera
                        or existing.source_root != source_root
                        or existing.destination_root != destination_root
                        or existing.destination_tier_id != tier_id
                        or existing.destination_tier_path != tier_path
                        or metadata.get("source_db")
                        != existing.metadata_record["source_db"]
                        or metadata.get("file_key")
                        != existing.metadata_record["file_key"]
                    ):
                        raise ValueError("Conflicting legacy move identity")
                    continue
                row = session.execute(
                    select(Files).where(Files.path == source)
                ).scalar_one_or_none()
                if row is not None:
                    metadata = record.get("meta", record)
                    source_db = metadata.get("source_db")
                    if (
                        row.camera_identifier != camera
                        or row.category != "recorder"
                        or row.subcategory != "segments"
                        or source_db
                        != {
                            "id": row.id,
                            "tier_id": row.tier_id,
                            "tier_path": row.tier_path,
                        }
                        or metadata.get("file_key") != row.file_key
                    ):
                        raise ValueError("Legacy source differs from database identity")
                    reserve(session, row, destination, tier_id, tier_path)
                else:
                    metadata = record.get("meta")
                    snapshot = record.get("source_snapshot")
                    if (
                        not isinstance(metadata, dict)
                        or not isinstance(snapshot, dict)
                        or not isinstance(metadata.get("source_db"), dict)
                        or not metadata.get("file_key")
                        or snapshot.get("source_db") != metadata["source_db"]
                    ):
                        raise ValueError("Legacy move lacks source database identity")
                    session.add(
                        PendingSegmentMove(
                            source=source,
                            destination=destination,
                            camera_identifier=camera,
                            source_root=source_root,
                            destination_root=destination_root,
                            destination_tier_id=tier_id,
                            destination_tier_path=tier_path,
                            metadata_record={
                                k: metadata[k]
                                for k in (
                                    "orig_ctime",
                                    "duration",
                                    "file_key",
                                    "source_db",
                                )
                            },
                            size=snapshot["size"],
                        )
                    )
                    session.flush()
                imported += 1
    session.commit()
    return imported


def main() -> None:
    """Import legacy marker discovery once, before enabling indexed recovery."""

    parser = argparse.ArgumentParser(
        description=(
            "Stop Viseron, then import legacy segment move markers. "
            "No media is changed."
        )
    )
    parser.add_argument("--camera", required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--destination-root", required=True)
    parser.add_argument("--destination-tier-id", required=True, type=int)
    parser.add_argument("--destination-tier-path", required=True)
    args = parser.parse_args()
    with Session(ENGINE) as session:
        count = import_legacy(
            session,
            args.camera,
            args.source_root,
            args.destination_root,
            args.destination_tier_id,
            args.destination_tier_path,
        )
    print(f"Imported {count} pending segment moves")  # noqa: T201


if __name__ == "__main__":
    main()
