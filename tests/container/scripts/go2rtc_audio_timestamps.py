"""Verify HTTP FLV source clocks survive Go2RTC and FFmpeg recording."""

import argparse
import http.server
import threading
import subprocess
import tempfile
import pathlib
import time
import json
import urllib.request
import os
import signal


def main() -> None:
    """Verify source-clock gaps survive relay and recording."""
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", nargs="?", default="/usr/local/bin/go2rtc")
    parser.add_argument(
        "--case",
        choices=("steady", "gap", "camera-catchup", "audio-dropout"),
        default="gap",
    )
    args = parser.parse_args()
    finished = threading.Event()
    root = pathlib.Path(tempfile.mkdtemp(prefix="viseron-flv-shadow-"))
    fixture = root / "source.flv"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x120:rate=20",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=16000",
            "-t",
            "5",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-bf",
            "0",
            "-g",
            "20",
            "-c:a",
            "aac",
            "-ar",
            "16000",
            "-f",
            "flv",
            str(fixture),
        ],
        check=True,
        timeout=20,
    )
    b = fixture.read_bytes()
    tags = []
    offset = 13
    while offset + 11 <= len(b):
        header = bytearray(b[offset : offset + 11])
        n = int.from_bytes(header[1:4], "big")
        body = b[offset + 11 : offset + 11 + n]
        previous = b[offset + 11 + n : offset + 15 + n]
        if len(previous) < 4:
            break
        timestamp = int.from_bytes(header[4:7], "big") + (header[7] << 24)
        if args.case == "camera-catchup" and (
            (header[0] == 8 and body[1:2] == b"\x01" and 1000 <= timestamp < 4214)
            or (header[0] == 9 and body[1:2] == b"\x01" and 2600 <= timestamp < 4199)
        ):
            offset += 15 + n
            continue
        if (
            args.case == "audio-dropout"
            and header[0] == 8
            and body[1:2] == b"\x01"
            and 1000 <= timestamp < 3500
        ):
            offset += 15 + n
            continue
        if args.case == "gap" and header[0] in (8, 9) and timestamp >= 2000:
            shifted = timestamp + 10000
            header[4:7] = (shifted & 0xFFFFFF).to_bytes(3, "big")
            header[7] = (shifted >> 24) & 255
        tags.append((timestamp, bytes(header) + body + previous))
        offset += 15 + n

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "video/x-flv")
            self.end_headers()
            try:
                self.wfile.write(b[:13])
                self.wfile.flush()
                start = time.monotonic()
                for timestamp, data in tags:
                    target = timestamp / 1000 + (1 if timestamp >= 2000 else 0)
                    delay = target - (time.monotonic() - start)
                    if delay > 0:
                        time.sleep(delay)
                    self.wfile.write(data)
                    self.wfile.flush()
                finished.wait(
                    15
                )  # Keep the producer connected; this test is not a reconnect.
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    config = root / "go2rtc.yaml"
    config.write_text(
        'api:\n  listen: "127.0.0.1:19844"\nrtsp:\n  listen: "127.0.0.1:18554"\n'
        'webrtc:\n  listen: ""\nstreams:\n  synthetic: http://127.0.0.1:'
        + str(server.server_port)
        + "/source.flv\n"
    )
    env = dict(os.environ)
    with (root / "go2rtc.log").open("w") as logs:
        process = subprocess.Popen(
            [args.binary, "-config", str(config)], env=env, stdout=logs, stderr=logs
        )
        try:
            for _ in range(40):
                try:
                    urllib.request.urlopen(
                        "http://127.0.0.1:19844/api/streams", timeout=1
                    ).close()
                    break
                except OSError:
                    time.sleep(0.1)
            recorder = subprocess.Popen(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-timeout",
                    "2000000",
                    "-analyzeduration",
                    "500000",
                    "-probesize",
                    "1000000",
                    "-i",
                    "rtsp://127.0.0.1:18554/synthetic",
                    "-map",
                    "0:v",
                    "-map",
                    "0:a",
                    "-c",
                    "copy",
                    "-movflags",
                    "+frag_keyframe+empty_moov",
                    "-y",
                    str(root / "recording.mp4"),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            try:
                recorder.communicate(timeout=8)
            except subprocess.TimeoutExpired:
                recorder.send_signal(signal.SIGINT)
                try:
                    recorder.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    recorder.kill()
                    recorder.communicate()
                    raise

            probe = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_packets",
                    "-show_entries",
                    "packet=stream_index,pts_time,duration_time",
                    "-of",
                    "json",
                    str(root / "recording.mp4"),
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            packets = json.loads(probe.stdout)["packets"]
            summary = {}
            for index in (0, 1):
                p = [
                    x for x in packets if x["stream_index"] == index and "pts_time" in x
                ]
                summary[str(index)] = {
                    "packets": len(p),
                    "start": min(float(x["pts_time"]) for x in p),
                    "end": max(
                        float(x["pts_time"]) + float(x.get("duration_time", 0))
                        for x in p
                    ),
                    "duration_sum": sum(float(x.get("duration_time", 0)) for x in p),
                }
            assert all(x["packets"] >= 10 for x in summary.values()), summary
            assert abs(summary["0"]["end"] - summary["1"]["end"]) < 0.2, summary
            assert abs(summary["0"]["start"] - summary["1"]["start"]) < 0.2, summary
            decoded = subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(root / "recording.mp4"),
                    "-map",
                    "0:v",
                    "-map",
                    "0:a",
                    "-f",
                    "null",
                    "-",
                ],
                capture_output=True,
                check=False,
                timeout=20,
            )
            assert decoded.returncode == 0, decoded.stderr.decode(errors="replace")
            print(  # noqa: T201
                json.dumps(
                    {
                        "passed": True,
                        "case": args.case,
                        "root": str(root),
                        "ffmpeg_returncode": recorder.returncode,
                        "media": summary,
                    },
                    indent=2,
                )
            )
        finally:
            finished.set()
            process.terminate()
            process.wait(timeout=5)
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()
