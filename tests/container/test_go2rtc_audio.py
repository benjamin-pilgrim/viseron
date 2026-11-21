"""Exercise the packaged relay and recorder against synthetic FLV sources."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from . import _helpers as helpers

if TYPE_CHECKING:
    import testinfra


@pytest.mark.parametrize(
    ("script", "case", "options"),
    [
        ("go2rtc_audio_timestamps.py", "steady", ""),
        ("go2rtc_audio_timestamps.py", "gap", ""),
        ("go2rtc_audio_timestamps.py", "camera-catchup", ""),
        ("go2rtc_audio_timestamps.py", "audio-dropout", ""),
        ("go2rtc_audio_recovery.py", "control", "--viseron-audio"),
        ("go2rtc_audio_recovery.py", "metadata_omits_audio", "--viseron-audio"),
        ("go2rtc_audio_recovery.py", "audio_delayed_6s", "--viseron-audio"),
        ("go2rtc_audio_recovery.py", "video_only", "--viseron-audio"),
    ],
    ids=[
        "steady",
        "gap",
        "catchup",
        "dropout",
        "control",
        "metadata",
        "late-audio",
        "video-only",
    ],
)
def test_flv_audio_recording(
    image_container: Any,
    image_host: testinfra.host.Host,
    script: str,
    case: str,
    options: str,
) -> None:
    """Keep source clocks, recover late audio, and retain video-only recordings."""
    source = Path(__file__).parent / "scripts" / script
    destination = f"/tmp/{script}"
    helpers.put_abs(image_container, destination, source.read_bytes())
    command = image_host.run(
        "env PATH=/ffmpeg/bin:/usr/local/bin:/usr/bin:/bin PYTHONPATH=/src "
        "python3 %s --case %s" + (" --viseron-audio" if options else ""),
        destination,
        case,
    )
    assert command.rc == 0, f"{case}:\n{command.stdout}\n{command.stderr}"
