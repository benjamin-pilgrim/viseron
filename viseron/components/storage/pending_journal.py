"""Prototype only: durable per-file pending record, no Viseron integration."""

import hashlib
import contextlib
import json
import re
import ctypes
import errno
import os
import tempfile
import stat
from pathlib import Path


def sync_dir(path):
    """Persist directory entry changes when the filesystem supports it."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


PREFIX = ".viseron-tier-move-pending-"


def prepare(
    run, filename, meta, *, temp_path=None, source_intent=None, copied_digest=None
):
    """Persist before publishing media; caller must hold same camera lock as cleanup."""
    if Path(filename).name != filename or not filename.endswith(".m4s"):
        raise ValueError("invalid media filename")
    run = Path(run)
    run.mkdir(parents=True, exist_ok=True)
    marker = run / (PREFIX + filename + ".json")
    record = {"version": 1, "filename": filename, "meta": meta}
    if temp_path is not None:
        temp = Path(temp_path)
        if temp.parent != run or not temp.name.startswith(".viseron-tier-move-"):
            raise ValueError("invalid private temporary")
        if os.path.lexists(run / filename):
            raise FileExistsError("destination already exists")
        record.update(version=2, temp_name=temp.name, identity=identity(temp))
        if source_intent is not None:
            intent = Path(source_intent)
            source = json.loads(intent.read_text())
            if source.get("destination") != str(run / filename) or source.get(
                "file_key"
            ) != meta.get("file_key"):
                raise ValueError("source intent does not match publication")
            if not re.fullmatch(r"[0-9a-f]{64}", copied_digest or ""):
                raise ValueError("missing copied-byte digest")
            record["copied_digest"] = copied_digest
            record["source_intent"] = str(intent)
            record["source_snapshot"] = {
                k: source[k]
                for k in (
                    "source",
                    "destination",
                    "dev",
                    "ino",
                    "size",
                    "mtime_ns",
                    "file_key",
                )
            }
            if "source_db" in meta:
                if source.get("source_db") != meta["source_db"]:
                    raise ValueError("source database identity does not match intent")
                record["source_snapshot"]["source_db"] = source["source_db"]
    payload = json.dumps(record, sort_keys=True).encode()
    fd, tmp = tempfile.mkstemp(prefix=PREFIX, dir=run)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        rename = ctypes.CDLL(None, use_errno=True).renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(tmp), -100, os.fsencode(marker), 1):
            err = ctypes.get_errno()
            if err != errno.EEXIST:
                raise OSError(err, os.strerror(err))
            if marker.read_bytes() != payload:
                raise ValueError("conflicting pending marker")
        sync_dir(run)
        return marker
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def reconcile(marker, tier_available, index):
    """Replay if destination exists; don't discard missing targets or failed DB ops."""
    marker = Path(marker)
    if not tier_available():
        return "unavailable"
    try:
        record = json.loads(marker.read_text())
        filename = record["filename"]
        if Path(filename).name != filename or not filename.endswith(".m4s"):
            return "invalid"
        target = marker.parent / filename
        if not target.is_file():
            return "needs-source-reconciliation"
        if not (marker.parent / "init.mp4").is_file():
            return "missing-init"
        index(str(target), record["meta"])
        if not tier_available():
            return "unavailable"
        marker.unlink()
        sync_dir(marker.parent)
        return "indexed"
    except Exception:
        return "retry"


def marker_for_path(path: str) -> str:
    """Return the pending-publication marker corresponding to a media path."""
    media = Path(path)
    return str(media.parent / (PREFIX + media.name + ".json"))


def protected_by_pending(path: str) -> bool:
    """Fail closed when pending status cannot be checked."""
    try:
        os.stat(marker_for_path(path))
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def identity(path):
    """Identity of an immutable temp/publication, without destination readback."""
    info = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or not info.st_ino:
        raise ValueError("unusable identity")
    return [info.st_dev, info.st_ino, info.st_size]


def publish_or_verify(marker):
    """Caller holds destination lock; replay rename or fail closed on collision."""
    marker = Path(marker)
    record = json.loads(marker.read_text())
    name, temp_name = record["filename"], record["temp_name"]
    if (
        record["version"] != 2  # noqa: PLR2004
        or Path(name).name != name
        or Path(temp_name).name != temp_name
        or not temp_name.startswith(".viseron-tier-move-")
        or marker.name != PREFIX + name + ".json"
    ):
        raise ValueError("invalid publication marker")
    temp, target = marker.parent / temp_name, marker.parent / name
    expected = record["identity"]
    if os.path.lexists(temp):
        if identity(temp) != expected or os.path.lexists(target):
            raise FileExistsError("publication collision")
        rename = ctypes.CDLL(None, use_errno=True).renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(temp), -100, os.fsencode(target), 1):
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
        sync_dir(marker.parent)
    if identity(target) != expected:
        raise ValueError("publication identity changed")
    return record


def release_indexed_source(
    marker,
    allowed_source_roots,
    tier_available,
    index_is_committed,
    lock_root,
    *,
    source_index_guard=None,
):
    """Experimental caller must hold destination camera lock for entire call.

    Never perform destination I/O under the source lock. index_is_committed must
    verify the full journal identity and committed database row. allowed roots
    must be trusted configured source-tier camera paths, not journal input.
    """
    from viseron.components.storage.run_lifecycle import camera_lock, is_run_directory  # noqa: PLC0415
    from viseron.components.storage.source_move_intent import marker_for_source  # noqa: PLC0415

    marker = Path(marker)
    try:
        record = publish_or_verify(marker)
        snapshot = record["source_snapshot"]
        source = Path(snapshot["source"])
        intent = Path(record["source_intent"])
        destination = marker.parent / record["filename"]
        if (
            str(destination) != snapshot["destination"]
            or intent != marker_for_source(source)
            or not source.is_absolute()
            or not is_run_directory(source.parent.name)
            or source.name != destination.name
            or source.parent.name != destination.parent.name
            or not any(
                os.path.abspath(root) == os.path.abspath(source.parent.parent)
                and os.path.realpath(root) == os.path.realpath(source.parent.parent)
                for root in allowed_source_roots
            )
        ):
            return "invalid"
        if not tier_available(str(destination)) or not index_is_committed(record):
            return "uncommitted"
        # No destination I/O after acquiring source camera lock.
        camera = os.path.abspath(source.parent.parent)
        with camera_lock(lock_root, camera):
            if not tier_available(str(source)):
                return "source-unavailable"
            try:
                source_record = json.loads(intent.read_text())
            except FileNotFoundError:
                # Crash after source intent removal: never infer safety if source remains.  # noqa: E501
                if source.exists() or not source.parent.is_dir():
                    return "ambiguous"
            else:
                if any(source_record.get(k) != v for k, v in snapshot.items()):
                    return "source-intent-mismatch"
                source_present = os.path.lexists(source)
                guard = (
                    source_index_guard(record, source_present)
                    if source_index_guard is not None
                    else contextlib.nullcontext(lambda: None)
                )
                with guard as finalize_index:
                    if finalize_index is None:
                        return "source-index-conflict"
                    try:
                        st = os.stat(source, follow_symlinks=False)
                    except FileNotFoundError:
                        if not source.parent.is_dir():
                            return "source-unavailable"
                    else:
                        if not stat.S_ISREG(st.st_mode) or [
                            st.st_dev,
                            st.st_ino,
                            st.st_size,
                        ] != [snapshot["dev"], snapshot["ino"], snapshot["size"]]:
                            return "source-changed"
                        if st.st_mtime_ns != snapshot["mtime_ns"]:
                            # The destination digest was computed as bytes were copied.
                            # Re-read only the source, while all cooperating source
                            # writers remain fenced until unlink. Never read the
                            # destination.
                            expected = record.get("copied_digest", "")
                            if not re.fullmatch(r"[0-9a-f]{64}", expected):
                                return "source-unverified"
                            with os.fdopen(
                                os.open(source, os.O_RDONLY | os.O_NOFOLLOW), "rb"
                            ) as stream:
                                before = os.fstat(stream.fileno())
                                if [before.st_dev, before.st_ino, before.st_size] != [
                                    snapshot["dev"],
                                    snapshot["ino"],
                                    snapshot["size"],
                                ]:
                                    return "source-changed"
                                digest = hashlib.sha256()
                                while chunk := stream.read(1024 * 1024):
                                    digest.update(chunk)
                                after = os.fstat(stream.fileno())
                                named = os.stat(source, follow_symlinks=False)

                                def attrs(info):
                                    return (
                                        info.st_dev,
                                        info.st_ino,
                                        info.st_size,
                                        info.st_mtime_ns,
                                        info.st_ctime_ns,
                                    )

                                if (
                                    attrs(before) != attrs(after)
                                    or attrs(after) != attrs(named)
                                    or digest.hexdigest() != expected
                                ):
                                    return "source-changed"
                        source.unlink()
                        sync_dir(source.parent)
                    finalize_index()
                intent.unlink()
                sync_dir(intent.parent)
        # Destination lock is still held; only now remove its durable journal.
        marker.unlink()
        sync_dir(marker.parent)
        return "released"
    except (OSError, ValueError, KeyError, TypeError):
        return "retry"


def assign_file_key(marker, key):
    """Durably assign a newly allocated key before indexing first-tier media.

    Caller holds the destination camera lock. Replays reuse this exact key.
    """
    marker = Path(marker)
    record = json.loads(marker.read_text())
    current = record["meta"]["file_key"]
    if current is not None and current != key:
        raise ValueError("journal already has another file key")
    if current == key:
        return record
    record["meta"]["file_key"] = key
    fd, temporary = tempfile.mkstemp(prefix=PREFIX, dir=marker.parent)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(record, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, marker)
        sync_dir(marker.parent)
        return record
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def assign_destination_id(marker, destination_id):
    """Bind a journal to a unique database row before that row is inserted."""
    marker = Path(marker)
    record = json.loads(marker.read_text())
    current = record.get("destination_id")
    if current is not None and current != destination_id:
        raise ValueError("journal already has another destination id")
    if current == destination_id:
        return record
    record["destination_id"] = destination_id
    fd, temporary = tempfile.mkstemp(prefix=PREFIX, dir=marker.parent)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(record, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, marker)
        sync_dir(marker.parent)
        return record
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
