"""Durable init publication with replayable identity checks."""

import ctypes
import errno
import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path
from viseron.components.storage.pending_journal import sync_dir

MARKER = ".viseron-tier-move-run-init.json"


def _id(path):
    st = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(st.st_mode) or not st.st_ino:
        raise ValueError("unusable init identity")
    return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns]


def _hash(path):
    """Hash an init and reject concurrent changes while it is being read."""
    with open(path, "rb") as stream:
        before = os.fstat(stream.fileno())
        digest = hashlib.sha256()
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise ValueError("init changed during verification")
    return digest.hexdigest()


def _publish(src, dst):
    try:
        fn = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError as exc:
        raise OSError(errno.ENOSYS, "renameat2 unavailable") from exc
    fn.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    fn.restype = ctypes.c_int
    if fn(-100, os.fsencode(src), -100, os.fsencode(dst), 1):
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def ensure_init(source, directory, *, adopt_existing=False):
    """Publish a run init; callers coordinate destination writers and cleanup."""
    source, directory = Path(source), Path(directory)
    target, marker = directory / "init.mp4", directory / MARKER
    directory.mkdir(parents=True, exist_ok=True)
    if not marker.exists() and os.path.lexists(target):
        if not adopt_existing:
            raise FileExistsError("init exists without its identity marker")
        source_id, target_id = _id(source), _id(target)
        digest = _hash(source)
        if not source_id[2] or digest != _hash(target):
            raise ValueError("conflicting existing run init")
        if _id(source) != source_id or _id(target) != target_id:
            raise ValueError("existing run init changed during verification")
        record = {
            "version": 2,
            "source": source_id,
            "destination": target_id,
            "temp": ".viseron-tier-move-init-adopted",
            "digest": digest,
        }
        if _id(source) != source_id:
            raise ValueError("source init changed during verification")
        fd, name = tempfile.mkstemp(
            prefix=".viseron-tier-move-init-marker-", dir=directory
        )
        try:
            with os.fdopen(fd, "w") as out:
                json.dump(record, out, sort_keys=True)
                out.flush()
                os.fsync(out.fileno())
            _publish(name, marker)
            sync_dir(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)
    if not marker.exists():
        before = _id(source)
        if not before[2]:
            raise ValueError("empty run init")
        fd, name = tempfile.mkstemp(prefix=".viseron-tier-move-init-", dir=directory)
        temp = Path(name)
        protected = False
        try:
            with os.fdopen(fd, "wb") as out, open(source, "rb") as inp:
                digest = hashlib.sha256()
                while chunk := inp.read(1024 * 1024):
                    out.write(chunk)
                    digest.update(chunk)
                out.flush()
                os.fsync(out.fileno())
            after = _id(source)
            if after[:3] != before[:3] or _id(temp)[2] != before[2]:
                raise ValueError("source init changed")  # noqa: TRY301
            if after != before and _hash(source) != digest.hexdigest():
                raise ValueError("source init content changed")  # noqa: TRY301
            # Retain the identity checked against the copied bytes. A fresh
            # stat here could associate new source bytes with the old digest.
            before = after
            record = {
                "version": 2,
                "digest": digest.hexdigest(),
                "source": before,
                "destination": _id(temp),
                "temp": temp.name,
            }
            mfd, mname = tempfile.mkstemp(
                prefix=".viseron-tier-move-init-marker-", dir=directory
            )
            try:
                with os.fdopen(mfd, "w") as out:
                    json.dump(record, out, sort_keys=True)
                    out.flush()
                    os.fsync(out.fileno())
                _publish(mname, marker)
                protected = True
                sync_dir(directory)
            finally:
                if os.path.exists(mname):
                    os.unlink(mname)
        except Exception:
            if not protected and temp.exists():
                temp.unlink()
            raise
    record = json.loads(marker.read_text())
    version = record.get("version")
    if version not in (1, 2):
        raise ValueError("unsupported init marker")
    source_id = _id(source)
    if version == 1:
        # An existing marker predates the copied-byte digest. Its source must
        # still have the recorded identity before deriving a digest from it.
        if source_id != record["source"]:
            raise ValueError("run init source identity changed")
        digest = _hash(source)
    else:
        digest = record["digest"]
        if source_id != record["source"]:
            if _hash(source) != digest or _id(source) != source_id:
                raise ValueError("run init source content changed")
    name = record["temp"]
    if Path(name).name != name or not name.startswith(".viseron-tier-move-init-"):
        raise ValueError("invalid init temporary")
    temp = directory / name
    if temp.exists():
        temp_id = _id(temp)
        if temp_id[:3] != record["destination"][:3] or os.path.lexists(target):
            raise FileExistsError("init publication collision")
        if temp_id != record["destination"] and _hash(temp) != digest:
            raise ValueError("temporary init content changed")
        _publish(temp, target)
        sync_dir(directory)
    target_id = _id(target)
    if target_id[:3] != record["destination"][:3]:
        raise ValueError("run init destination identity changed")
    # Some CIFS servers change mtime during rename. Only this exceptional
    # metadata change requires destination readback; never trust size alone.
    if target_id != record["destination"] and _hash(target) != digest:
        raise ValueError("run init destination content changed")
    if (
        version == 1
        or target_id != record["destination"]
        or source_id != record["source"]
    ):
        record.update(version=2, digest=digest, source=source_id, destination=target_id)
        fd, replacement = tempfile.mkstemp(
            prefix=".viseron-tier-move-init-marker-", dir=directory
        )
        try:
            with os.fdopen(fd, "w") as out:
                json.dump(record, out, sort_keys=True)
                out.flush()
                os.fsync(out.fileno())
            os.replace(replacement, marker)
            sync_dir(directory)
        finally:
            if os.path.exists(replacement):
                os.unlink(replacement)
