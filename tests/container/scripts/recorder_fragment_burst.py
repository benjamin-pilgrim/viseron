"""Verify recorder fragments flushed together retain all supplied video."""

import json
import os
import pathlib
import subprocess
import tempfile
import uuid
from types import SimpleNamespace

from viseron.components.ffmpeg.camera import CAMERA_SCHEMA
from viseron.components.ffmpeg.const import ENV_FFMPEG_PATH
from viseron.components.ffmpeg.stream import Stream


def main() -> None:
    """Exercise the recorder's FFmpeg output arguments with a buffered burst."""
    os.environ[ENV_FFMPEG_PATH] = "/ffmpeg/bin/ffmpeg"
    with tempfile.TemporaryDirectory(prefix="recorder-burst-") as folder:
        root = pathlib.Path(folder)
        fixture = root / "source.mp4"
        subprocess.run(
            [
                "/ffmpeg/bin/ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x120:rate=20",
                "-t",
                "20",
                "-c:v",
                "libx264",
                "-threads",
                "1",
                "-preset",
                "ultrafast",
                "-g",
                "20",
                str(fixture),
            ],
            check=True,
            timeout=30,
        )
        config = CAMERA_SCHEMA(
            {
                "name": "Burst",
                "host": "127.0.0.1",
                "port": 554,
                "path": "/unused",
                "width": 160,
                "height": 120,
                "fps": 20,
                "codec": "h264",
                "audio_codec": None,
                "recorder": {"codec": "copy", "audio_codec": None},
            }
        )
        stream = Stream(
            config,
            SimpleNamespace(config=config, temp_segments_folder=folder),
            f"burst_{uuid.uuid4().hex}",
        )
        subprocess.run(
            ["/ffmpeg/bin/ffmpeg", "-v", "warning", "-i", str(fixture)]
            + stream.segment_args(),
            check=True,
            timeout=30,
        )
        fragments = [
            line
            for line in (root / "index.m3u8").read_text().splitlines()
            if line and not line.startswith("#")
        ]
        assert len(fragments) == len(set(fragments)) == 4, fragments
        timestamps = []
        for fragment in fragments:
            media = root / "probe.mp4"
            media.write_bytes(
                (root / "init.mp4").read_bytes() + (root / fragment).read_bytes()
            )
            result = subprocess.run(
                [
                    "/ffmpeg/bin/ffprobe",
                    "-v",
                    "error",
                    "-show_packets",
                    "-show_entries",
                    "packet=pts_time",
                    "-of",
                    "json",
                    str(media),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            timestamps.extend(
                float(p["pts_time"]) for p in json.loads(result.stdout)["packets"]
            )
        assert len(timestamps) == 400, len(timestamps)
        assert all(
            abs(actual - frame / 20) < 0.001 for frame, actual in enumerate(timestamps)
        ), timestamps
        print(  # noqa: T201
            "PASS all 400 video frames retained across four fragments flushed together"
        )


if __name__ == "__main__":
    main()
