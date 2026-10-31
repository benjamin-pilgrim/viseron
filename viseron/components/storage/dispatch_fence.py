"""Local process-lifetime fence for retried storage worker jobs."""

import json
import os
import stat
import tempfile


def process_identity(pid):
    """Return Linux process state and start ticks, or fail closed."""
    with open(f"/proc/{pid}/stat", encoding="ascii") as process:
        raw = process.read()
    # The parenthesized process name can contain spaces and parentheses.
    fields = raw[raw.rfind(")") + 2 :].split()
    return fields[0], int(fields[19])


def create_token():
    """Make a private token bound to the current process lifetime."""
    pid = os.getpid()
    _, start = process_identity(pid)
    fd, path = tempfile.mkstemp(prefix="viseron-tier-retry-", dir="/tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump({"pid": pid, "start": start}, stream)
            stream.flush()
        return path
    except Exception:
        os.unlink(path)
        raise


def token_permits_start(token):
    """Check token and owner. A successful check is the job-start boundary."""
    if os.path.dirname(token) != "/tmp" or not os.path.basename(token).startswith(
        "viseron-tier-retry-"
    ):
        return False
    try:
        fd = os.open(token, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            owner = json.load(stream)
            opened = os.fstat(stream.fileno())
            named = os.stat(token, follow_symlinks=False)
            if (
                not stat.S_ISREG(opened.st_mode)
                or not stat.S_ISREG(named.st_mode)
                or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            ):
                return False
        pid = owner["pid"]
        if not isinstance(pid, int) or pid <= 0:
            return False
        state, start = process_identity(pid)
        return state not in ("Z", "X") and start == owner["start"]
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        return False
