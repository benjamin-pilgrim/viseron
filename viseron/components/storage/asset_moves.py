"""Durable destinations and atomic database handoff for recording assets."""

from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import stat
import tempfile
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, or_, select, update

from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.pending_journal import sync_dir
from viseron.components.storage.run_lifecycle import camera_lock
from viseron.components.storage.util import require_tier_markers

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from sqlalchemy.orm import Session


ASSET_FIELDS = {"thumbnails": "thumbnail_path", "event_clips": "clip_path"}


def is_recording_asset(row: Files) -> bool:
    """Only these assets share the recording-link handoff protocol."""
    return row.category == "recorder" and row.subcategory in ASSET_FIELDS


def identity(info: os.stat_result) -> list[int]:
    """Identify the immutable source reserved for this move."""
    if not stat.S_ISREG(info.st_mode) or info.st_size <= 0:
        raise OSError("Asset source is not a nonempty regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def reserve(row: Files, dst: str, tier_id: int, tier_path: str) -> dict[str, Any]:
    """Persist this result on the existing file row before dispatching any work."""
    if row.move_target is not None:
        if (
            row.move_target["path"] != dst
            or row.move_target["tier_id"] != tier_id
            or row.move_target["tier_path"] != tier_path
        ):
            raise OSError("Another asset destination is already reserved")
        return row.move_target
    source_identity = identity(os.stat(row.path, follow_symlinks=False))
    if source_identity[2] != row.size:
        raise OSError("Asset size differs from its indexed source")
    target = {
        "path": dst,
        "tier_id": tier_id,
        "tier_path": tier_path,
        "identity": source_identity,
    }
    row.move_target = target
    return target


def destination_pending(session: Session, path: str) -> bool:
    """Watchers and orphan cleanup must leave a reserved destination alone."""
    return (
        session.execute(
            select(Files.id)
            .where(
                Files.move_target.is_not(None),
                Files.move_target["path"].astext == path,
            )
            .limit(1)
        ).first()
        is not None
    )


def asset_move_pending(session: Session, path: str) -> bool:
    """Preserve both copies while the source row still owns a retry destination."""
    return (
        session.execute(
            select(Files.id)
            .where(
                Files.move_target.is_not(None),
                or_(Files.path == path, Files.move_target["path"].astext == path),
            )
            .limit(1)
        ).first()
        is not None
    )


def _digest(path: str) -> bytes:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.digest()


@contextlib.contextmanager
def asset_locks(*paths: str) -> Iterator[None]:
    """Share source/destination ownership with deletion and orphan cleanup."""
    with contextlib.ExitStack() as locks:
        for path in sorted({os.path.abspath(path) for path in paths}):
            locks.enter_context(camera_lock("/tmp/viseron-run-locks", "asset:" + path))
        yield


def move_asset(
    get_session: Callable[..., Session],
    src: str,
    dst: str,
    meta: dict[str, Any],
    tier_markers: dict[str, str],
) -> None:
    """Publish bytes, commit both links, then remove the old copy.

    The nullable destination on the source row is the retry queue. No successful
    watcher delivery or in-memory callback is needed to complete the handoff.
    """
    target = meta["asset_target"]
    if target["path"] != dst or src == dst:
        raise OSError("Invalid asset destination")
    with asset_locks(src, dst):
        require_tier_markers(src, tier_markers)
        require_tier_markers(dst, tier_markers)
        if identity(os.stat(src, follow_symlinks=False)) != target["identity"]:
            raise OSError("Reserved asset source changed")
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=".viseron-tier-move-", dir=os.path.dirname(dst)
        )
        try:
            with os.fdopen(fd, "wb") as copied, open(src, "rb") as source:
                os.fchmod(
                    copied.fileno(), stat.S_IMODE(os.fstat(source.fileno()).st_mode)
                )
                shutil.copyfileobj(source, copied)
                copied.flush()
                os.fsync(copied.fileno())
            if identity(os.stat(src, follow_symlinks=False)) != target["identity"]:
                raise OSError("Reserved asset source changed during copy")
            if os.path.getsize(temporary) != target["identity"][2]:
                raise OSError("Incomplete asset copy")
            try:
                existing = os.stat(dst, follow_symlinks=False)
            except FileNotFoundError:
                require_tier_markers(dst, tier_markers)
                os.replace(temporary, dst)
                sync_dir(os.path.dirname(dst))
            else:
                if not stat.S_ISREG(existing.st_mode) or _digest(dst) != _digest(
                    temporary
                ):
                    raise OSError("Conflicting asset already exists at destination")
            require_tier_markers(src, tier_markers)
            require_tier_markers(dst, tier_markers)
            _commit(get_session, src, dst, meta)
            try:
                os.unlink(src)
                sync_dir(os.path.dirname(src))
            except FileNotFoundError:
                pass  # Orphan cleanup may already have removed the extra copy.
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def _commit(
    get_session: Callable[..., Session], src: str, dst: str, meta: dict[str, Any]
) -> None:
    """The destination and recording links become authoritative in one commit."""
    with get_session() as session:
        for path in sorted((src, dst)):
            session.execute(select(func.pg_advisory_xact_lock(func.hashtext(path))))
        row = session.execute(
            select(Files).where(Files.path == src).with_for_update()
        ).scalar_one_or_none()
        expected = meta["source_db"]
        target = meta["asset_target"]
        if (
            row is None
            or not is_recording_asset(row)
            or row.id != expected["id"]
            or row.file_key != meta["file_key"]
            or row.tier_id != expected["tier_id"]
            or row.tier_path != expected["tier_path"]
            or row.move_target != target
        ):
            raise OSError("Asset source database identity changed")
        if session.execute(select(Files.id).where(Files.path == dst)).first():
            raise OSError("Conflicting asset database destination")
        if identity(os.stat(src, follow_symlinks=False)) != target["identity"]:
            raise OSError("Reserved asset source changed before handoff")
        field = getattr(Recordings, ASSET_FIELDS[row.subcategory])
        session.execute(
            update(Recordings)
            .where(Recordings.camera_identifier == row.camera_identifier, field == src)
            .values({field: dst})
        )
        row.path = dst
        row.directory = os.path.dirname(dst)
        row.tier_id = target["tier_id"]
        row.tier_path = target["tier_path"]
        row.move_target = None
        session.commit()
