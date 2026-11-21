"""Check indexed media through Viseron's public HLS endpoints."""

import argparse
import json
import pathlib
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def fetch(url):
    """Read a local test endpoint with a bounded deadline."""
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read()


def probe(camera, start, folder):
    """Decode independently playable fragments fetched from the HLS API."""
    url = (
        f"http://127.0.0.1:8888/api/v1/hls/{camera}/index.m3u8"
        f"?start_timestamp={start}&end_timestamp={int(time.time())}"
    )
    playlist = fetch(url).decode()
    tracks = []
    init_url = None
    for line in playlist.splitlines():
        if line.startswith("#EXT-X-MAP:"):
            init_url = urllib.parse.urljoin(url, re.search(r'URI="([^"]+)"', line)[1])
        elif line and not line.startswith("#"):
            assert init_url, playlist
            media = pathlib.Path(folder) / f"{camera}-{len(tracks)}.mp4"
            media.write_bytes(fetch(init_url) + fetch(urllib.parse.urljoin(url, line)))
            result = subprocess.run(
                [
                    "/ffmpeg/bin/ffprobe",
                    "-v",
                    "error",
                    "-count_packets",
                    "-show_entries",
                    "stream=codec_type,nb_read_packets",
                    "-of",
                    "json",
                    str(media),
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            streams = {
                item["codec_type"]: int(item.get("nb_read_packets", 0))
                for item in json.loads(result.stdout)["streams"]
            }
            subprocess.run(
                [
                    "/ffmpeg/bin/ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(media),
                    "-f",
                    "null",
                    "-",
                ],
                capture_output=True,
                check=True,
                timeout=10,
            )
            # Track changes can flush a valid audio-only fragment. Check video
            # availability before and after renegotiation below.
            assert any(packets > 0 for packets in streams.values()), streams
            tracks.append(streams)
    return tracks


def main() -> None:
    """Require pre-audio video, recovered audio and continued video-only recording."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, required=True)
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    result = {}
    with tempfile.TemporaryDirectory(prefix="hls-recording-") as folder:
        while time.monotonic() < deadline:
            try:
                result = {
                    camera: probe(camera, args.start, folder)
                    for camera in ("late_audio", "video_only")
                }
            except (urllib.error.HTTPError, urllib.error.URLError):
                time.sleep(1)
                continue
            late = result["late_audio"]
            only = result["video_only"]
            if (
                any(
                    stream.get("video", 0) > 0 and "audio" not in stream
                    for stream in late
                )
                and any(
                    stream.get("video", 0) > 0 and stream.get("audio", 0) > 0
                    for stream in late
                )
                and sum(stream.get("video", 0) > 0 for stream in only) >= 2
            ):
                assert all("audio" not in stream for stream in only), only
                counts = json.loads(fetch("http://127.0.0.1:18080/stats"))
                assert counts == {"late_audio": 1, "video_only": 1}, counts
                print(json.dumps({"tracks": result, "camera_connections": counts}))  # noqa: T201
                return
            time.sleep(1)
    raise AssertionError(f"expected video before and after audio recovery: {result}")


if __name__ == "__main__":
    main()
