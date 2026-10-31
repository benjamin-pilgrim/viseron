"""Identify runs whose FFmpeg arguments support safe playlist observation."""

TIMING_CONTRACT_FILE = ".viseron-playlist-timing-contract"
TIMING_CONTRACT_CONTENT = "viseron-playlist-timing-v1\n"


def permits_timing_capture(config: dict) -> bool:
    """Reject arbitrary FFmpeg argument lists that can override the HLS output."""
    recorder = config.get("recorder") or {}
    substream = config.get("substream") or {}
    return (
        not config.get("raw_command")
        and config.get("global_args") == ["-hide_banner"]
        and not config.get("input_args")
        and not config.get("hwaccel_args")
        and not recorder.get("output_args")
        and not recorder.get("hwaccel_args")
        and not substream.get("raw_command")
        and not substream.get("input_args")
        and not substream.get("hwaccel_args")
    )
