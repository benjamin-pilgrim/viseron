"""Experimental init publication with a durable identity, not wired into Viseron."""

import ctypes
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


def _publish(src, dst):
    fn = ctypes.CDLL(None, use_errno=True).renameat2
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


def ensure_init(source, directory):
    """Caller holds destination and source writer locks; fail closed on ambiguity."""
    source, directory = Path(source), Path(directory)
    target, marker = directory / "init.mp4", directory / MARKER
    directory.mkdir(parents=True, exist_ok=True)
    if not marker.exists():
        if os.path.lexists(target):
            raise FileExistsError("init exists without its identity marker")
        before = _id(source)
        fd, name = tempfile.mkstemp(prefix=".viseron-tier-move-init-", dir=directory)
        temp = Path(name)
        protected = False
        try:
            with os.fdopen(fd, "wb") as out, open(source, "rb") as inp:
                while chunk := inp.read(1024 * 1024):
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
            if _id(source) != before or _id(temp)[2] != before[2]:
                raise ValueError("source init changed")  # noqa: TRY301
            record = {
                "version": 1,
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
    if record.get("version") != 1 or _id(source) != record["source"]:
        raise ValueError("run init source identity changed")
    name = record["temp"]
    if Path(name).name != name or not name.startswith(".viseron-tier-move-init-"):
        raise ValueError("invalid init temporary")
    temp = directory / name
    if temp.exists():
        if _id(temp) != record["destination"] or os.path.lexists(target):
            raise FileExistsError("init publication collision")
        _publish(temp, target)
        sync_dir(directory)
    if _id(target) != record["destination"]:
        raise ValueError("run init destination identity changed")
