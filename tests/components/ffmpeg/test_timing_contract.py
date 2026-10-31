"""Only known generated recorder configurations capture timing."""

import pytest

from viseron.helpers.recording_timing_contract import permits_timing_capture


@pytest.mark.parametrize(
    "override",
    [
        {"raw_command": "ffmpeg -i example"},
        {"global_args": ["-hide_banner", "-copyts"]},
        {"input_args": ["-copyts"]},
        {"hwaccel_args": ["-hls_flags", "append_list"]},
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
