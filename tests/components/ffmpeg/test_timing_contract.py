"""Only known generated recorder configurations capture timing."""

import pytest

from viseron.components.ffmpeg.camera import get_default_hwaccel_args
from viseron.components.ffmpeg.const import HWACCEL_VAAPI
from viseron.helpers.recording_timing_contract import permits_timing_capture


@pytest.mark.parametrize(
    "override",
    [
        {"raw_command": "ffmpeg -i example"},
        {"global_args": ["-hide_banner", "-copyts"]},
        {"input_args": ["-copyts"]},
        {"hwaccel_args": ["-hls_flags", "append_list"]},
        {"hwaccel_args": HWACCEL_VAAPI + ["-hls_flags", "append_list"]},
        {"substream": {"hwaccel_args": HWACCEL_VAAPI + ["-copyts"]}},
        {"recorder": {"output_args": ["-hls_flags", "append_list"]}},
        {"recorder": {"hwaccel_args": ["-hls_flags", "append_list"]}},
        {"substream": {"raw_command": "ffmpeg"}},
        {"substream": {"input_args": ["-copyts"]}},
        {"substream": {"hwaccel_args": ["-copyts"]}},
    ],
)
def test_custom_arguments_fail_closed(override):
    """Overrides do not claim a generated timing contract."""
    config = {"global_args": ["-hide_banner"], "recorder": {}, "substream": None}
    config.update(override)
    assert not permits_timing_capture(config)


def test_default_generated_arguments_allow_timing():
    """Default generated FFmpeg settings permit exact playlist capture."""
    assert permits_timing_capture(
        {"global_args": ["-hide_banner"], "recorder": {}, "substream": None}
    )


@pytest.mark.parametrize("vaapi", ["false", "true"])
@pytest.mark.parametrize("substream", [False, True])
def test_generated_decoder_defaults_allow_timing(monkeypatch, vaapi, substream):
    """Automatic hardware decoding must not disable crash-recovery timing."""
    monkeypatch.setenv("VISERON_VAAPI_SUPPORTED", vaapi)
    monkeypatch.setenv("VISERON_CUDA_SUPPORTED", "false")
    config = {
        "global_args": ["-hide_banner"],
        "recorder": {},
        "substream": None,
        "hwaccel_args": get_default_hwaccel_args(),
    }
    if substream:
        config["substream"] = {"hwaccel_args": get_default_hwaccel_args()}
    assert permits_timing_capture(config)
