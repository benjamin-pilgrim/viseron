#!/usr/bin/env python3
"""Read-only diagnostic for a sample expected to contain aligned audio and video.

Input is a standalone MP4, or an init concatenated with closed run fragments.
This is an investigation gate, not a production retention/admission policy.
"""

import argparse
import json
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sample")
    parser.add_argument("--max-start-skew", type=float, default=1.0)
    args = parser.parse_args()
    result = subprocess.run(
        [
            "ffprobe", "-v", "error", "-show_streams", "-show_packets",
            "-show_entries",
            "stream=index,codec_type:packet=stream_index,pts_time,duration_time",
            "-of", "json", args.sample,
        ], capture_output=True, text=True, timeout=30, check=False,
    )
    if result.returncode:
        print(json.dumps({"valid": False, "reason": "ffprobe failed"}))
        return 2
    media = json.loads(result.stdout)
    ranges = {}
    for stream in media["streams"]:
        kind = stream["codec_type"]
        if kind not in ("audio", "video"):
            continue
        packets = [
            packet for packet in media["packets"]
            if packet["stream_index"] == stream["index"] and "pts_time" in packet
        ]
        if not packets:
            continue
        ranges[kind] = {
            "start": min(float(packet["pts_time"]) for packet in packets),
            "end": max(
                float(packet["pts_time"]) + float(packet.get("duration_time", 0))
                for packet in packets
            ),
            "packets": len(packets),
        }
    if not all(kind in ranges for kind in ("audio", "video")):
        print(json.dumps({"valid": False, "reason": "missing timed audio/video", "ranges": ranges}))
        return 2
    audio, video = ranges["audio"], ranges["video"]
    skew = audio["start"] - video["start"]
    overlap = max(0, min(audio["end"], video["end"]) - max(audio["start"], video["start"]))
    valid = abs(skew) <= args.max_start_skew and overlap > 0
    print(json.dumps({"valid": valid, "audio_minus_video_seconds": round(skew, 6),
                      "overlap_seconds": round(overlap, 6), "ranges": ranges}))
    return 0 if valid else 2


if __name__ == "__main__":
    raise SystemExit(main())
