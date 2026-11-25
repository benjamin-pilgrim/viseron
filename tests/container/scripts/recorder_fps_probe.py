"""Verify early detection frame reduction preserves full-rate recorder output."""

import json
import os
import pathlib
import re
import subprocess
import tempfile
import uuid
from types import SimpleNamespace

from viseron.components.ffmpeg.camera import CAMERA_SCHEMA
from viseron.components.ffmpeg.const import ENV_FFMPEG_PATH
from viseron.components.ffmpeg.stream import Stream
from viseron.domains.camera.recording_run import RUN_TOKEN


def main() -> None:
    """Count transformed frames and independently decode the archived video."""
    os.environ[ENV_FFMPEG_PATH] = "/home/abc/bin/ffmpeg"
    with tempfile.TemporaryDirectory(prefix="recorder-fps-") as directory:
        root = pathlib.Path(directory)
        source = root / "source.mp4"
        subprocess.run(
            [
                "/home/abc/bin/ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x240:rate=30",
                "-t",
                "4",
                "-c:v",
                "libx264",
                "-threads",
                "1",
                "-preset",
                "ultrafast",
                "-bf",
                "0",
                "-g",
                "30",
                str(source),
            ],
            check=True,
            timeout=30,
        )
        results = []
        for before in (False, True):
            folder = root / str(before)
            folder.mkdir()
            config = CAMERA_SCHEMA(
                {
                    "name": "Synthetic driveway",
                    "host": "127.0.0.1",
                    "port": 8554,
                    "path": "/unused",
                    "width": 320,
                    "height": 240,
                    "fps": 30,
                    "codec": "h264",
                    "audio_codec": None,
                    "fps_before_filters": before,
                    "video_filters": [
                        "bench=start",
                        "transpose=1",
                        "scale=320:240",
                        "bench=stop",
                    ],
                    "recorder": {"codec": "copy", "audio_codec": None},
                }
            )
            camera = SimpleNamespace(config=config, temp_segments_folder=str(folder))
            stream = Stream(config, camera, f"fps_{uuid.uuid4().hex}")
            stream.output_fps = 1
            run_folder = folder / "synthetic-run"
            run_folder.mkdir()
            # This probe has one encoder launch; restart allocation is covered
            # by the packaged recorder-run acceptance gate.
            command = [
                "/home/abc/bin/ffmpeg",
                "-loglevel",
                "repeat+info",
                "-nostats",
                "-i",
                str(source),
                *stream.segment_args(),
                *stream.filter_args(),
                *stream.output_args,
            ]
            recorded = subprocess.run(
                [argument.replace(RUN_TOKEN, "synthetic-run") for argument in command],
                check=True,
                capture_output=True,
                timeout=30,
            )
            transformed = len(
                re.findall(
                    rb"\[(?:Parsed_)?bench(?:_\d+)? @ [^\]]+\] t:", recorded.stderr
                )
            )
            assert transformed == (4 if before else 120), recorded.stderr.decode()
            assert len(recorded.stdout) == 4 * 320 * 240 * 3 // 2
            fragments = [
                line
                for line in (run_folder / "index.m3u8").read_text().splitlines()
                if line and not line.startswith("#")
            ]
            assert len(fragments) == 1, fragments
            media = folder / "archived.mp4"
            media.write_bytes(
                (run_folder / "init.mp4").read_bytes()
                + (run_folder / fragments[0]).read_bytes()
            )
            probe = subprocess.run(
                [
                    "/home/abc/bin/ffprobe",
                    "-v",
                    "error",
                    "-count_frames",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=nb_read_frames,r_frame_rate",
                    "-of",
                    "json",
                    str(media),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=15,
            )
            video = json.loads(probe.stdout)["streams"][0]
            assert int(video["nb_read_frames"]) == 120, video
            assert video["r_frame_rate"] == "30/1", video
            results.append(
                {
                    "early_reduction": before,
                    "transformed_frames": transformed,
                    "saved_frames": 120,
                    "saved_fps": 30,
                }
            )
        print(json.dumps(results))  # noqa: T201


if __name__ == "__main__":
    main()
