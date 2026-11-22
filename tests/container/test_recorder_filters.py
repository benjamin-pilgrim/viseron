"""Exercise detector filtering and recorder output with packaged FFmpeg."""

from pathlib import Path
from typing import Any

from . import _helpers as helpers


def test_early_detection_reduction_keeps_full_recording(
    image_container: Any, image_host: Any
) -> None:
    """Process fewer detection frames while retaining every recorded frame."""
    script = "recorder_fps_probe.py"
    helpers.put_file(
        image_container,
        "/tmp",
        script,
        (Path(__file__).parent / "scripts" / script).read_bytes(),
    )
    result = image_host.run("env PYTHONPATH=/src python3 /tmp/%s", script)
    assert result.rc == 0, result.stdout + result.stderr
