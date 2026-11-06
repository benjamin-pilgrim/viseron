"""Exercise real HTTP FLV -> Go2RTC -> restarting FFmpeg consumers."""

import argparse
import http.server
import json
import logging
import pathlib
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request
from types import SimpleNamespace

parser = argparse.ArgumentParser()
parser.add_argument("binary", nargs="?", default="/usr/local/bin/go2rtc")
parser.add_argument("--viseron-audio", action="store_true")
parser.add_argument(
    "--case",
    choices=("control", "metadata_omits_audio", "audio_delayed_6s", "video_only"),
)
args = parser.parse_args()
binary = args.binary
root = pathlib.Path(tempfile.mkdtemp(prefix="flv-audio-recovery-"))
print("RECOVERY_E2E_ARTIFACTS", str(root), flush=True)
fixture = root / "fixture.flv"
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
        "12",
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
original = []
pos = 13
while pos + 15 < len(b):
    n = int.from_bytes(b[pos + 1 : pos + 4], "big")
    tag = b[pos : pos + 15 + n]
    ts = int.from_bytes(b[pos + 4 : pos + 7], "big") + (b[pos + 7] << 24)
    original.append((ts, tag))
    pos += 15 + n
audio_args = []
if args.viseron_audio:
    from viseron.components.ffmpeg.stream import Stream
    from viseron.components.ffmpeg.const import (
        CONFIG_RECORDER_AUDIO_CODEC,
        DEFAULT_RECORDER_AUDIO_CODEC,
    )
    from viseron.domains.camera.const import CONFIG_RECORDER

    owner = SimpleNamespace(
        _config={
            CONFIG_RECORDER: {CONFIG_RECORDER_AUDIO_CODEC: DEFAULT_RECORDER_AUDIO_CODEC}
        },
        _logger=logging.getLogger("audio_recovery_test"),
    )
    audio_args = Stream.get_encoder_audio_codec(owner, None)
results = []
cases = (
    [args.case]
    if args.case
    else ["control", "metadata_omits_audio", "audio_delayed_6s", "video_only"]
)
for case in cases:
    stop = threading.Event()
    tags = []
    requests = [0]
    for ts, tag in original:
        if case in ["metadata_omits_audio", "video_only"] and tag[0] == 18:
            tag = tag.replace(b"audiocodecid", b"xudiocodecid")
        if case == "video_only" and tag[0] == 8:
            continue
        if case == "audio_delayed_6s" and tag[0] == 8:
            if tag[12:13] == b"\x00":
                ts = 6000
                tag = bytearray(tag)
                tag[4:7] = (ts & 0xFFFFFF).to_bytes(3, "big")
                tag[7] = ts >> 24
                tag = bytes(tag)
            elif ts < 6000:
                continue
        tags.append((ts, tag))
    tags.sort(key=lambda x: x[0])

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            requests[0] += 1
            self.send_response(200)
            self.send_header("Content-Type", "video/x-flv")
            self.end_headers()
            try:
                header = bytearray(b[:13])
                header[4] = 0
                self.wfile.write(header)
                self.wfile.flush()
                start = time.monotonic()
                for ts, tag in tags:
                    if stop.wait(max(0, ts / 1000 - (time.monotonic() - start))):
                        return
                    self.wfile.write(tag)
                    self.wfile.flush()
                stop.wait(4)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    cfg = root / (case + ".yaml")
    cfg.write_text(
        'log:\n  level: debug\napi:\n  listen: "127.0.0.1:19844"\nrtsp:\n  listen: "127.0.0.1:18554"\nwebrtc:\n  listen: ""\nstreams:\n  synthetic: http://127.0.0.1:'
        + str(server.server_port)
        + "/fixture.flv\n"
    )
    with (root / (case + ".log")).open("w") as log:
        proc = subprocess.Popen([binary, "-config", str(cfg)], stdout=log, stderr=log)
        consumer = None
        clips = []

        def record(attempt):
            clip = root / f"{case}-{attempt}.mp4"
            clips.append(clip)
            return subprocess.Popen(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-timeout",
                    "3000000",
                    "-analyzeduration",
                    "100000",
                    "-probesize",
                    "100000",
                    "-i",
                    "rtsp://127.0.0.1:18554/synthetic",
                    "-map",
                    "0:v",
                    "-map",
                    "0:a?",
                    "-c",
                    "copy",
                    *audio_args,
                    "-movflags",
                    "frag_keyframe+empty_moov",
                    "-y",
                    str(clip),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        try:
            for _ in range(40):
                try:
                    urllib.request.urlopen(
                        "http://127.0.0.1:19844/api/streams", timeout=0.2
                    ).close()
                    break
                except OSError:
                    time.sleep(0.1)
            start = time.monotonic()
            deadline = start + 15
            attempt = 0
            audio_since = None
            saw_video_only = False
            consumer = record(attempt)
            while time.monotonic() < deadline:
                if consumer.poll() is not None:
                    attempt += 1
                    assert attempt <= 5, ("reconnect loop", case)
                    consumer = record(attempt)
                with urllib.request.urlopen(
                    "http://127.0.0.1:19844/api/streams", timeout=1
                ) as response:
                    streams = json.load(response)
                medias = [
                    m
                    for p in streams.get("synthetic", {}).get("producers", [])
                    for m in p.get("medias", [])
                ]
                has_video = any("video" in m for m in medias)
                has_audio = any("audio" in m for m in medias)
                saw_video_only |= has_video and not has_audio
                if has_audio and audio_since is None:
                    audio_since = time.monotonic()
                if case == "video_only" and time.monotonic() - start > 4:
                    break
                if audio_since is not None and time.monotonic() - audio_since > 4:
                    break
                time.sleep(0.1)
            consumer.send_signal(signal.SIGINT)
            try:
                consumer.wait(timeout=3)
            except subprocess.TimeoutExpired:
                consumer.kill()
                consumer.wait()
            media = []
            for clip in clips:
                if not clip.exists() or not clip.stat().st_size:
                    continue
                p = subprocess.run(
                    [
                        "ffprobe",
                        "-v",
                        "error",
                        "-count_packets",
                        "-show_entries",
                        "stream=codec_type,nb_read_packets",
                        "-of",
                        "json",
                        str(clip),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if p.returncode:
                    continue
                tracks = {
                    r["codec_type"]: int(r.get("nb_read_packets", "0"))
                    for r in json.loads(p.stdout)["streams"]
                }
                if not tracks.get("video"):
                    continue
                decoded = subprocess.run(
                    ["ffmpeg", "-v", "error", "-i", str(clip), "-f", "null", "-"],
                    capture_output=True,
                    timeout=8,
                )
                assert decoded.returncode == 0, (
                    case,
                    decoded.stderr.decode(errors="replace"),
                )
                media.append(tracks)
            assert requests[0] == 1, ("camera was redialed", case, requests[0])
            assert any(r.get("video", 0) >= 10 for r in media), (case, media)
            if case == "video_only":
                assert (
                    audio_since is None
                    and attempt == 0
                    and all("audio" not in r for r in media)
                ), (case, media, attempt)
            else:
                assert any(
                    r.get("audio", 0) >= 10 and r.get("video", 0) >= 10 for r in media
                ), (case, media)
            if case == "audio_delayed_6s":
                assert any(
                    r.get("video", 0) >= 10 and "audio" not in r for r in media
                ), ("video not retained before audio", media)
            result = {
                "case": case,
                "camera_connections": requests[0],
                "consumer_restarts": attempt,
                "saw_video_only": saw_video_only,
                "clips": media,
            }
            results.append(result)
            print("RECOVERY_E2E", json.dumps(result), flush=True)
        finally:
            stop.set()
            if consumer is not None and consumer.poll() is None:
                consumer.kill()
                consumer.wait()
            proc.terminate()
            proc.wait(timeout=3)
            server.shutdown()
            server.server_close()
print("RECOVERY_E2E_PASSED", json.dumps(results), flush=True)
