"""Serve timed, synthetic camera streams for the full recorder integration test."""

import argparse
import http.server
import itertools
import json
import pathlib
import subprocess
import tempfile
import threading
import time


def main() -> None:
    """Keep one video-only source and one late-audio source running until stopped."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--audio-dropout", action="store_true")
    args = parser.parse_args()
    scenarios = {}
    with tempfile.TemporaryDirectory(prefix="flv-cameras-") as folder:
        fixture = pathlib.Path(folder) / "camera.flv"
        subprocess.run(
            [
                "/ffmpeg/bin/ffmpeg",
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
                "60",
                "-c:v",
                "libx264",
                "-threads",
                "1",
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
            timeout=30,
        )
        data = fixture.read_bytes()
        tags = []
        offset = 13
        while offset + 15 <= len(data):
            size = int.from_bytes(data[offset + 1 : offset + 4], "big")
            tag = data[offset : offset + 15 + size]
            timestamp = int.from_bytes(tag[4:7], "big") + (tag[7] << 24)
            tags.append((timestamp, tag))
            offset += 15 + size
        # Let default recorder probing finish and close a real HLS fragment
        # before adding audio. The relay-only test covers a six-second delay.
        counts = {"late_audio": 0, "video_only": 0}
        lock = threading.Lock()

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):  # noqa: N802
                if self.path == "/scenario":
                    with lock:
                        payload = json.dumps(scenarios).encode()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if self.path == "/stats":
                    with lock:
                        payload = json.dumps(counts).encode()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                camera = self.path.lstrip("/")
                if camera not in counts:
                    self.send_error(404)
                    return
                with lock:
                    counts[camera] += 1
                steady = []
                selected = []
                for timestamp, original in tags:
                    tag = original
                    if camera == "video_only":
                        if tag[0] == 8:
                            continue
                        if tag[0] == 18:
                            tag = tag.replace(b"audiocodecid", b"xudiocodecid")
                    steady.append((timestamp, tag))
                    if camera == "late_audio" and tag[0] == 8 and args.audio_dropout:
                        if 30000 <= timestamp < 50000:
                            continue
                    elif camera == "late_audio" and tag[0] == 8:
                        if tag[12:13] == b"\x00":
                            timestamp = 15000
                        elif timestamp < 15000:
                            continue
                    selected.append((timestamp, tag))
                selected.sort(key=lambda item: item[0])
                self.send_response(200)
                self.send_header("Content-Type", "video/x-flv")
                self.end_headers()
                header = bytearray(data[:13])
                header[4] = 0
                try:
                    self.wfile.write(header)
                    started = time.monotonic()
                    with lock:
                        scenarios[camera] = {
                            "started": time.time(),
                            "audio_dropout": args.audio_dropout,
                        }
                    # AAC frames are 64 ms at this fixture's 16 kHz sample rate.
                    # Keep source clocks monotonic across loops; delay audio only
                    # on the first pass, rather than creating a recurring outage.
                    period_ms = max(timestamp for timestamp, _tag in tags) + 64
                    for cycle in itertools.count():
                        for timestamp, original in selected if cycle == 0 else steady:
                            timestamp += cycle * period_ms
                            delay = timestamp / 1000 - (time.monotonic() - started)
                            if delay > 0:
                                time.sleep(delay)
                            tag = bytearray(original)
                            tag[4:7] = (timestamp & 0xFFFFFF).to_bytes(3, "big")
                            tag[7] = (timestamp >> 24) & 0xFF
                            self.wfile.write(tag)
                            self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return

        http.server.ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()


if __name__ == "__main__":
    main()
