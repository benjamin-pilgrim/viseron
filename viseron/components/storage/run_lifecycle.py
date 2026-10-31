"""Experimental coordination primitive: every publisher and cleanup must use it."""

import contextlib
import fcntl
import hashlib
import os
import re
import stat
import time
from pathlib import Path


class CameraLockBusy(BlockingIOError):
    """A cooperating publisher or cleanup currently owns the camera lock."""


@contextlib.contextmanager
def camera_lock(lock_root: str, camera_key: str):
    """Share a lock for one camera directory on one tier across processes.

    The shared local root must have the same identity for every writer. Lock files
    are intentionally not removed; each camera gets one bounded coordination file.
    """
    os.makedirs(lock_root, exist_ok=True)
    name = hashlib.sha256(camera_key.encode()).hexdigest() + ".lock"
    fd = os.open(os.path.join(lock_root, name), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err:
            raise CameraLockBusy(err.errno, str(err)) from err
        yield
    finally:
        os.close(fd)


def reclaim_empty_run(lock_root, camera_key, run_dir, check_tier, has_database_refs):
    """Attempt reclamation under the same lock held for complete publication.

    has_database_refs must include pending/in-flight segments. Staging activity
    must also be covered by the lock or republish init on each later segment.
    Unknown files, absent tier, DB errors and lock contention all fail closed.
    """
    try:
        with camera_lock(lock_root, camera_key):
            check_tier()
            entries = list(Path(run_dir).iterdir())
            # The init identity marker belongs to the init and can be removed
            # with it after the run has no media, DB references or journals.
            allowed = {"init.mp4", ".viseron-tier-move-run-init.json"}
            if any(
                entry.name not in allowed or not entry.is_file() or entry.is_symlink()
                for entry in entries
            ):
                return False
            if has_database_refs(run_dir):
                return False
            check_tier()
            for entry in entries:
                entry.unlink()
            os.rmdir(run_dir)
            return True
    except Exception:  # Prototype: caller must log failures in integration.
        return False


def is_run_directory(name: str) -> bool:
    """Recognize generated FFmpeg recording-run directory names."""
    return re.fullmatch(r"\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}", name) is not None


TEMP_MAX_AGE_SECONDS = 60 * 60
_PRIVATE_TEMP = re.compile(
    r"^\.viseron-tier-move-(?:pending-)?(?:[a-z0-9_]{8}|[0-9a-f]{32})$"
)


def remove_stale_temporary(path: str, check_tier) -> bool:
    """Remove a known private temporary after one hour; caller holds tier lock.

    Complete pending journal markers and unknown filenames are never removed.
    """
    if not _PRIVATE_TEMP.fullmatch(os.path.basename(path)):
        return False
    check_tier()
    try:
        # A durable publication may need this exact temporary on restart.
        # Keep all temporaries if any pending marker exists; malformed records
        # fail closed instead of guessing which temporary they own.
        if any(
            name.startswith(".viseron-tier-move-pending-") and name.endswith(".json")
            for name in os.listdir(os.path.dirname(path))
        ):
            return False
        entry = os.lstat(path)
        if (
            not stat.S_ISREG(entry.st_mode)
            or time.time() - entry.st_mtime < TEMP_MAX_AGE_SECONDS
        ):
            return False
        check_tier()
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False


def reclaim_spool_run(lock_root: str, camera_key: str, run_dir: str) -> bool:
    """Remove only a finished, fully handed-over run with no unknown files.

    The FFmpeg process inherits an exclusive flock on the lease from before
    launch until exit. Missing/inaccessible leases and any other file fail closed.
    """
    lease_name = ".viseron-writer-lease"
    allowed = {lease_name, "init.mp4", "index.m3u8"}
    try:
        with camera_lock(lock_root, camera_key):
            run = Path(run_dir)
            if not is_run_directory(run.name) or os.path.abspath(
                str(run.parent)
            ) != os.path.abspath(camera_key):
                return False
            fd = os.open(run / lease_name, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    return False
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                entries = list(run.iterdir())
                if any(
                    e.name not in allowed or e.is_symlink() or not e.is_file()
                    for e in entries
                ):
                    return False
                # Delete the lease last. An interrupted cleanup with no lease
                # never authorizes a later cleanup to guess that the run ended.
                for name in ("index.m3u8", "init.mp4"):
                    try:
                        (run / name).unlink()
                    except FileNotFoundError:
                        pass
                (run / lease_name).unlink()
                os.rmdir(run)
                return True
            finally:
                os.close(fd)
    except (OSError, ValueError):
        return False
