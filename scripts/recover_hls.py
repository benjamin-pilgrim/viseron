"""Build an offline HLS playlist for one Viseron recorder run.

Pass each tier's copy of the same camera/run directory. The command only reads
recordings and writes the requested playlist; it does not use Viseron's database.
Run directories must have been created with the correct init association; probing
cannot prove that an init file belongs to the fragments.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import quote

GAP_TOLERANCE_SECONDS = 0.15


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def uri(path: Path) -> str:
    return "file://" + quote(str(path.resolve()), safe="/")


def probe(init: Path, segment: Path) -> tuple[float, float]:
    # A fragment needs its init to be parseable. A temporary combined file avoids
    # retaining the entire fragment in memory and handles large camera bitrates.
    with tempfile.TemporaryFile() as media:
        for path in (init, segment):
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    media.write(chunk)
        media.seek(0)
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_packets",
                "-show_entries",
                "packet=pts_time,duration_time",
                "-of",
                "json",
                "pipe:0",
            ],
            stdin=media,
            capture_output=True,
            timeout=60,
            check=False,
        )
    if result.returncode or result.stderr.strip():
        raise ValueError(
            result.stderr.decode(errors="replace").strip() or "ffprobe failed"
        )
    packets = json.loads(result.stdout).get("packets", [])
    valid = [
        (float(x["pts_time"]), float(x["duration_time"]))
        for x in packets
        if "pts_time" in x and "duration_time" in x and x["duration_time"] != "N/A"
    ]
    if not valid:
        raise ValueError("no video packets with timestamps and durations")
    start = min(pts for pts, _ in valid)
    end = max(pts + duration for pts, duration in valid)
    if end <= start or not math.isfinite(start) or not math.isfinite(end):
        raise ValueError("invalid packet timing")
    return start, end


def run() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("run_directory", nargs="+", type=Path)
    args = parser.parse_args()
    roots = args.run_directory
    if len({root.name for root in roots}) != 1:
        parser.error("all directories must have the same run identifier")
    names: dict[str, list[Path]] = {}
    for root in roots:
        if not root.is_dir():
            raise SystemExit(f"Run directory unavailable: {root}")
        for path in root.iterdir():
            if path.is_file() and (path.name == "init.mp4" or path.suffix == ".m4s"):
                names.setdefault(path.name, []).append(path)
    if "init.mp4" not in names:
        raise SystemExit("No init.mp4 found; cannot associate segments safely")
    # Never silently choose between different copies of the same run file.
    for name, copies in names.items():
        if len(copies) > 1 and len({digest(path) for path in copies}) != 1:
            raise SystemExit(f"Conflicting copies of {name}: {copies}")
    init = names["init.mp4"][0]
    entries = []
    failed = False
    for name in sorted(name for name in names if name.endswith(".m4s")):
        path = names[name][0]
        try:
            start, end = probe(init, path)
            entries.append((path, start, end))
        except (
            OSError,
            ValueError,
            subprocess.TimeoutExpired,
            json.JSONDecodeError,
        ) as error:
            failed = True
            print(f"UNREADABLE {path}: {error}", file=sys.stderr)
    if not entries:
        raise SystemExit("No readable segments")
    target_duration = max(1, math.ceil(max(end - start for _, start, end in entries)))
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:7",
        "#EXT-X-MEDIA-SEQUENCE:0",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        f'#EXT-X-MAP:URI="{uri(init)}"',
    ]
    previous_end = None
    for path, start, end in entries:
        if (
            previous_end is not None
            and abs(start - previous_end) > GAP_TOLERANCE_SECONDS
        ):
            print(
                f"TIMING GAP/OVERLAP before {path}: {start - previous_end:+.3f}s",
                file=sys.stderr,
            )
            lines.append("#EXT-X-DISCONTINUITY")
        lines.extend([f"#EXTINF:{end - start:.6f},", uri(path)])
        previous_end = end
    lines.append("#EXT-X-ENDLIST")
    try:
        # Exclusive creation also rejects dangling symlinks and closes the
        # race between checking the path and writing the playlist.
        with args.output.open("x", encoding="utf-8") as output:
            output.write("\n".join(lines) + "\n")
    except FileExistsError as error:
        raise SystemExit(f"Output exists: {args.output}") from error
    print(
        "Warning: init association is assumed from the directory, not verified.",
        file=sys.stderr,
    )
    print(
        "Without a retained index, missing files outside detected timing gaps "
        "cannot be identified.",
        file=sys.stderr,
    )
    print(
        f"Wrote {args.output} with {len(entries)} segments; "
        f"use ffplay -protocol_whitelist file,crypto {args.output}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(run())
