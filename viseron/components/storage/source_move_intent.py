"""Experimental durable source-move intents; fail closed on ambiguity."""

import datetime
import json
import re
import stat
import os
import tempfile
import time
from pathlib import Path
from viseron.components.storage.pending_journal import sync_dir
from viseron.helpers.recording_timing_contract import (
    TIMING_CONTRACT_CONTENT,
    TIMING_CONTRACT_FILE,
)

PREFIX = ".viseron-source-move-"
HOLD_WARNING_SECONDS = 300
MAX_SOURCE_TIMING_DURATION = 86400


def marker_for_source(path):  # noqa: D103
    path = Path(path)
    return path.parent / (PREFIX + path.name + ".json")


def is_source_move_marker(name):  # noqa: D103
    return Path(name).name.startswith(PREFIX)


def source_move_pending(path):
    """A malformed or incomplete marker still protects its source."""
    try:
        os.lstat(marker_for_source(path))
        return True
    except FileNotFoundError:
        return False


def prepare_source_intent(
    src, dst, stat_result, file_key, *, source_db=None, source_timing=None
):
    """Caller holds the local tier camera lock; no remote I/O is performed."""
    marker = marker_for_source(src)
    record = {
        "version": 1,
        "source": os.path.abspath(src),
        "destination": os.path.abspath(dst),
        "dev": stat_result.st_dev,
        "ino": stat_result.st_ino,
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
        "file_key": file_key,
    }
    if source_db is not None:
        record["source_db"] = source_db
    if source_timing is not None:
        record["source_timing"] = source_timing
    payload = json.dumps(record, sort_keys=True).encode()

    def existing_intent():
        try:
            existing = json.loads(marker.read_text())
            existing.pop("sha256", None)
            existing.pop("init_sha256", None)
            existing_timing = existing.pop("source_timing", None)
        except (ValueError, OSError):
            raise ValueError("incomplete source move intent") from None
        expected = json.loads(payload)
        expected.pop("source_timing", None)
        if existing != expected or (
            existing_timing is not None
            and source_timing is not None
            and (
                any(
                    existing_timing.get(k) != source_timing.get(k)
                    for k in ("program_date_time", "duration")
                )
                or (
                    existing_timing.get("sha256") is not None
                    and source_timing.get("sha256") is not None
                    and existing_timing["sha256"] != source_timing["sha256"]
                )
            )
        ):
            raise ValueError("conflicting source move intent")
        return marker

    if os.path.lexists(marker):
        return existing_intent()
    # A full tier or process interruption must not publish an empty hold. The
    # private file is durable before its complete inode becomes discoverable.
    fd, tmp = tempfile.mkstemp(prefix=PREFIX + "preparing-", dir=marker.parent)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        try:
            os.link(tmp, marker)  # Atomic and never replaces another intent.
        except FileExistsError:
            return existing_intent()
        os.unlink(tmp)
        sync_dir(marker.parent)
        return marker
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def set_source_digest(marker, digest, init_digest):
    """Durably bind copied content to an existing marker before source unlink."""
    marker = Path(marker)
    record = json.loads(marker.read_text())
    record["sha256"] = digest
    record["init_sha256"] = init_digest
    fd, tmp = tempfile.mkstemp(prefix=PREFIX + "updating-", dir=marker.parent)
    try:
        with os.fdopen(fd, "w") as file:
            json.dump(record, file, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp, marker)
        sync_dir(marker.parent)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def finish_source_intent(marker):
    """Caller has already verified/removes source under the same camera lock."""
    os.unlink(marker)
    sync_dir(Path(marker).parent)


def reconcile_source_intent(marker, lock_root, camera_dir, verify_destination):
    """Clear an absent-source marker only after verified destination publication.

    Caller MUST hold the destination camera lock for the entire call.
    verify_destination runs before the source lock and must verify the destination
    marker, contents, file_key and DB publication. Never retry ambiguous outcomes.
    """
    from viseron.components.storage.run_lifecycle import camera_lock  # noqa: PLC0415

    marker = Path(marker)
    try:
        raw = marker.read_bytes()
        record = json.loads(raw)
        source = record["source"]
        if (
            marker_for_source(source) != marker
            or not record.get("file_key")
            or not record.get("sha256")
            or not record.get("init_sha256")
        ):
            return "invalid"
        if not verify_destination(record):
            return "unverified"
        with camera_lock(lock_root, os.path.abspath(camera_dir)):
            if marker.read_bytes() != raw:
                return "changed"
            try:
                os.lstat(source)
                return "source-present"
            except FileNotFoundError:
                pass
            if not os.path.isdir(os.path.dirname(source)):
                return "source-unavailable"
            finish_source_intent(marker)
            return "complete"
    except (OSError, ValueError, KeyError, TypeError):
        return "retry"


def verify_destination_publication(record, get_session, tier_available, allowed_roots):
    """Verify destination and init contents plus committed matching Files row."""
    import hashlib  # noqa: PLC0415
    from sqlalchemy import select  # noqa: PLC0415
    from viseron.components.storage.models import Files  # noqa: PLC0415

    try:
        dst = os.path.abspath(record["destination"])
        source = os.path.abspath(record["source"])
        relative_source = os.path.join(
            os.path.basename(os.path.dirname(source)), os.path.basename(source)
        )
        if not any(
            os.path.commonpath((os.path.realpath(root), os.path.realpath(dst)))
            == os.path.realpath(root)
            and os.path.relpath(dst, root) == relative_source
            for root in allowed_roots
        ):
            return False
        init = os.path.join(os.path.dirname(dst), "init.mp4")
        if not tier_available(dst) or not tier_available(init):
            return False

        def digest(path):
            h = hashlib.sha256()
            with open(path, "rb") as f:
                while chunk := f.read(1024 * 1024):
                    h.update(chunk)
            return h.hexdigest()

        if (
            os.path.getsize(dst) != record["size"]
            or digest(dst) != record["sha256"]
            or digest(init) != record["init_sha256"]
        ):
            return False
        with get_session() as session:
            row = session.execute(
                select(Files).where(
                    Files.path == dst, Files.file_key == record["file_key"]
                )
            ).scalar_one_or_none()
            if row is None or row.size != record["size"]:
                return False
        return tier_available(dst)
    except Exception:
        return False


def scan_source_intents(
    camera_dir,
    get_session,  # noqa: ARG001
    tier_available,
    allowed_roots,  # noqa: ARG001
    lock_root,
    *,
    limit=100,
    cursor=None,
    report_incomplete=None,
    retry_incomplete=None,
):
    """One bounded pass; caller runs this on a background scheduler."""
    from viseron.components.storage.run_lifecycle import camera_lock, is_run_directory  # noqa: PLC0415

    if not tier_available(camera_dir):
        return 0
    seen = 0
    resume_after = cursor.get("after") if cursor is not None else None
    for root, dirs, files in os.walk(camera_dir):
        dirs.sort()
        if not is_run_directory(os.path.basename(root)):
            continue
        for name in sorted(files):
            if not is_source_move_marker(name) or not name.endswith(".m4s.json"):
                continue
            marker = Path(root) / name
            if resume_after is not None and str(marker) <= resume_after:
                continue
            if cursor is not None:
                cursor["after"] = str(marker)
            # Report only. A source intent is owned by destination-journal
            # replay, which must retain both holds until the DB handoff commits.
            # Probing destination content here would race that replay and do
            # unwanted remote readback on each scheduler pass.
            try:
                record = json.loads(marker.read_text())
                incomplete = not record.get("sha256") or not record.get("init_sha256")
                if incomplete and retry_incomplete is not None:
                    retry_incomplete(str(marker), record)
                if (
                    report_incomplete is not None
                    and time.time() - marker.stat().st_mtime > HOLD_WARNING_SECONDS
                ):
                    # The lock only detects this instance's active workers; an
                    # old hold could still belong to another container or host.
                    dest_camera = os.path.abspath(
                        str(Path(record["destination"]).parent.parent)
                    )
                    with camera_lock(lock_root, dest_camera):
                        report_incomplete(str(marker))
            except BlockingIOError:
                pass
            except (OSError, ValueError, KeyError, TypeError):
                pass
            seen += 1
            if seen >= limit:
                return seen
    if cursor is not None:
        cursor["after"] = None
    return seen


def cancel_abandoned_source_intent(
    marker, lock_root, camera_dir, dest_camera, *, exclusive_runtime
):
    """Cancel only with externally established exclusive runtime ownership.

    No destination I/O occurs while the source lock is held. Local locks alone
    cannot prove exclusivity across containers/hosts; callers must establish it
    independently. Returning 'cancelled' leaves the original source untouched.
    """
    from viseron.components.storage.run_lifecycle import camera_lock  # noqa: PLC0415

    if not exclusive_runtime:
        return "ownership-unproven"
    marker = Path(marker)
    try:
        raw = marker.read_bytes()
        record = json.loads(raw)
        src = record["source"]
        if marker_for_source(src) != marker or os.path.abspath(
            str(Path(src).parent.parent)
        ) != os.path.abspath(camera_dir):
            return "invalid"
        if os.path.abspath(
            str(Path(record["destination"]).parent.parent)
        ) != os.path.abspath(dest_camera):
            return "invalid"
        with camera_lock(lock_root, os.path.abspath(dest_camera)):
            with camera_lock(lock_root, os.path.abspath(camera_dir)):
                if marker.read_bytes() != raw:
                    return "changed"
                st = os.stat(src, follow_symlinks=False)
                if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != (
                    record["dev"],
                    record["ino"],
                    record["size"],
                    record["mtime_ns"],
                ):
                    return "source-changed"
                finish_source_intent(marker)
                return "cancelled"
    except BlockingIOError:
        return "busy"
    except (OSError, ValueError, KeyError, TypeError):
        return "retry"


def read_source_intent_time(fragment: Path, destination: Path):
    """Recover observed timing from an interrupted first publication's hold."""
    try:
        if (fragment.parent / TIMING_CONTRACT_FILE).read_text(
            encoding="utf-8"
        ) != TIMING_CONTRACT_CONTENT:
            return None
        record = json.loads(marker_for_source(fragment).read_text())
        timing = record["source_timing"]
        timestamp = datetime.datetime.fromisoformat(timing["program_date_time"])
        duration = float(timing["duration"])
        info = fragment.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            return None
        identity = [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]
        digest = timing.get("sha256")
        if (
            record["version"] != 1
            or record["source"] != os.path.abspath(fragment)
            or record["destination"] != os.path.abspath(destination)
            or record.get("file_key") is not None
            or record.get("source_db") is not None
            or [record[k] for k in ("dev", "ino", "size", "mtime_ns")] != identity
            or timestamp.utcoffset() is None
            or not 0 < duration < MAX_SOURCE_TIMING_DURATION
            or (digest is not None and not re.fullmatch(r"[0-9a-f]{64}", digest))
        ):
            return None
        return timestamp.astimezone(datetime.timezone.utc), duration, identity, digest
    except (OSError, ValueError, TypeError, KeyError):
        return None
