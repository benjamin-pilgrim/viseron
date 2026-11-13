"""Reconcile indexed locations lost with a volatile tier, without archive walks."""

import logging
import os
from pathlib import Path

from sqlalchemy import delete, select, update

from viseron.components.storage.asset_moves import ASSET_FIELDS, asset_locks
from viseron.components.storage.models import Files, Recordings
from viseron.components.storage.run_lifecycle import camera_lock, is_run_directory
from viseron.components.storage.util import tier_markers_available

LOGGER = logging.getLogger(__name__)
BATCH_SIZE = 100


def _mount_filesystems() -> dict[str, str]:
    """Read mount types without probing remote shares.

    Requiring an exact mount prevents mistaking a missing CIFS submount beneath
    /tmp for an empty RAM tier. Reading mountinfo never probes a remote share.
    """
    roots = {}
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as mounts:
            for line in mounts:
                mount, filesystem = line.split(" - ", 1)
                if filesystem:
                    path = mount.split()[4]
                    for code, char in [
                        ("\\040", " "),
                        ("\\011", "\t"),
                        ("\\012", "\n"),
                        ("\\134", "\\"),
                    ]:
                        path = path.replace(code, char)
                    roots[path] = filesystem.split()[0]
    except (OSError, ValueError, IndexError):
        return {}  # An unknown mount is not evidence of missing media.
    return roots


def volatile_roots() -> set[str]:
    """Return explicit tmpfs mount roots without probing storage."""
    return {
        path
        for path, filesystem in _mount_filesystems().items()
        if filesystem == "tmpfs"
    }


def reconcile_volatile_files(get_session, tier_markers=None) -> int:
    """Drop confirmed absent locations before cameras and pressure checks start.

    Independent PendingSegmentMove records and event history remain intact.
    Asset move intents live on Files itself, so retain those rows for recovery;
    volatile pressure uses actual tmpfs allocation, not these retained receipts.
    """
    filesystems = _mount_filesystems()
    mounts = {path for path, filesystem in filesystems.items() if filesystem == "tmpfs"}
    with get_session() as session:
        roots = session.execute(select(Files.tier_path).distinct()).scalars().all()
    deleted = 0
    for root in roots:
        if os.path.abspath(root) not in mounts:
            continue
        after = 0
        removed = 0
        while True:
            with get_session() as session:
                rows = session.execute(
                    select(Files.id, Files.path)
                    .where(
                        Files.tier_path == root,
                        Files.id > after,
                        Files.move_target.is_(None),
                    )
                    .order_by(Files.id)
                    .limit(BATCH_SIZE)
                ).all()
            if not rows:
                break
            after = rows[-1].id
            for file_id, path in rows:
                if any(
                    mount != os.path.abspath(root)
                    and mount.startswith(os.path.abspath(root).rstrip("/") + "/")
                    and (path == mount or path.startswith(mount.rstrip("/") + "/"))
                    for mount in filesystems
                ):
                    continue  # A nested remote or replacement mount is independent.
                # Neither an escaped/symlinked path nor an unavailable marker
                # can establish that a file on this volatile mount was lost.
                if os.path.commonpath((root, path)) != os.path.abspath(
                    root
                ) or os.path.realpath(path) != os.path.abspath(path):
                    continue
                run = Path(path).parent
                lock = (
                    camera_lock("/tmp/viseron-run-locks", str(run.parent))
                    if is_run_directory(run.name)
                    else asset_locks(path)
                )
                try:
                    with lock:
                        if not tier_markers_available(path, tier_markers or {}):
                            continue
                        try:
                            os.stat(path, follow_symlinks=False)
                        except FileNotFoundError:
                            with get_session() as session:
                                retired = session.execute(
                                    delete(Files)
                                    .where(
                                        Files.id == file_id,
                                        Files.path == path,
                                        Files.tier_path == root,
                                        Files.move_target.is_(None),
                                    )
                                    .returning(
                                        Files.camera_identifier,
                                        Files.category,
                                        Files.subcategory,
                                    )
                                ).one_or_none()
                                if retired is not None:
                                    camera, category, kind = retired
                                    if category == "recorder" and kind in ASSET_FIELDS:
                                        field = getattr(Recordings, ASSET_FIELDS[kind])
                                        session.execute(
                                            update(Recordings)
                                            .where(
                                                Recordings.camera_identifier == camera,
                                                field == path,
                                            )
                                            .values({field: None})
                                        )
                                    removed += 1
                                session.commit()
                except OSError:
                    LOGGER.warning("Deferred volatile file reconciliation for %s", path)
        if removed:
            LOGGER.info(
                "Removed %d missing file locations from volatile tier %s; "
                "event and transfer records retained",
                removed,
                root,
            )
        deleted += removed
    return deleted


def missing_volatile_path(path: str, tier_path: str) -> bool:
    """Prove local absence only on the exact configured, still-mounted RAM tier."""
    root, path = os.path.abspath(tier_path), os.path.abspath(path)
    filesystems = _mount_filesystems()
    if filesystems.get(root) != "tmpfs" or os.path.commonpath((root, path)) != root:
        return False
    if any(
        mount != root
        and mount.startswith(root.rstrip("/") + "/")
        and (path == mount or path.startswith(mount.rstrip("/") + "/"))
        for mount in filesystems
    ):
        return False
    if os.path.realpath(path) != path or not Path(root).is_dir():
        return False
    try:
        os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return True
    return False


def recovery_destination_available(camera_root: str, tier_path: str, available) -> bool:
    """An empty fallback directory cannot establish absence on a missing share.

    Require an explicit configured tier mount (or category mount for the root
    tier). Unknown mount layouts retain their work for manual investigation.
    """
    root = os.path.abspath(tier_path)
    anchor = str(Path(camera_root).parent) if root == "/" else root
    if anchor not in _mount_filesystems() or not available(camera_root):
        return False
    return Path(anchor).is_dir() and os.path.realpath(camera_root) == os.path.abspath(
        camera_root
    )
