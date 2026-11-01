"""Decode a preview from a segment and the init in its own recorder run.

This helper does not publish or attach thumbnails. The caller must use the
artifact lifecycle's durable publication and event ownership protocol.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path

_RUN = re.compile(r"^\d{8}T\d{6}\.\d{6}Z-[0-9a-f]{12}$")


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def extract_run_thumbnail(segment: str, expected_run: str, *, timeout: float = 30) -> bytes:
    """Return JPEG bytes using only the segment's adjacent matching run init.

    Unknown legacy paths, different run IDs, symlinks, missing or changing inputs,
    and decoding failures are rejected. This is a read operation: no recording
    database row, target artifact, or source file is modified.
    """
    path = Path(segment)
    if (not path.is_absolute() or path.suffix != ".m4s"
        or not _RUN.fullmatch(expected_run) or path.parent.name != expected_run):
        raise ValueError("Segment does not belong to the requested recorder run")
    if os.path.realpath(path.parent) != os.path.abspath(path.parent):
        raise ValueError("Recorder run path contains a symlink")
    run_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        pinned = os.fstat(run_fd)
        with tempfile.TemporaryFile() as media:
            for name in ("init.mp4", path.name):
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=run_fd)
                with os.fdopen(fd, "rb") as inp:
                    before = os.fstat(inp.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                        raise ValueError("Invalid thumbnail media input")
                    while chunk := inp.read(1024 * 1024):
                        media.write(chunk)
                    after = os.fstat(inp.fileno())
                    named = os.stat(name, dir_fd=run_fd, follow_symlinks=False)
                    if _identity(before) != _identity(after) or _identity(after) != _identity(named):
                        raise ValueError("Thumbnail media input changed during reading")
            named_run = os.stat(path.parent, follow_symlinks=False)
            if (pinned.st_dev, pinned.st_ino) != (named_run.st_dev, named_run.st_ino):
                raise ValueError("Recorder run changed during thumbnail extraction")
            media.seek(0)
            result = subprocess.run(
                ["ffmpeg", "-v", "error", "-i", "pipe:0", "-frames:v", "1",
                 "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"],
                stdin=media, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=timeout, check=False,
            )
    finally:
        os.close(run_fd)
    if result.returncode or not result.stdout.startswith(b"\xff\xd8"):
        raise ValueError("Could not decode thumbnail from recorder run")
    return result.stdout


def main() -> int:
    """Recover a preview to a new file without changing the recording database."""
    import argparse  # noqa: PLC0415
    import sys  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("segment", help="absolute path of a run's m4s fragment")
    parser.add_argument("--run-id", required=True, help="expected recorder run ID")
    parser.add_argument("--output", required=True, type=Path, help="new JPEG path")
    args = parser.parse_args()
    temporary = None
    try:
        jpeg = extract_run_thumbnail(args.segment, args.run_id)
        fd, temporary = tempfile.mkstemp(prefix=".viseron-thumbnail-", dir=args.output.parent)
        with os.fdopen(fd, "wb") as stream:
            stream.write(jpeg)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, args.output, follow_symlinks=False)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"Thumbnail recovery failed: {error}", file=sys.stderr)
        return 1
    finally:
        if temporary is not None:
            os.unlink(temporary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
