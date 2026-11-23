"""Recording shutdown must finish without abandoning camera workers."""

import pytest

from .test_storage_recovery import _exec, _recording_case, _segments, _wait


@pytest.mark.parametrize("stop_mode", ["healthy_camera", "camera_offline", "container"])
@pytest.mark.timeout(180)
def test_recording_shutdown_finishes_workers(
    docker_client, image, artifact_dir, request, stop_mode
):
    """Graceful shutdown must work with both a healthy and disconnected camera."""
    with _recording_case(docker_client, image, artifact_dir, request) as (
        container,
        _url,
    ):
        _wait(lambda: _segments(container), lambda rows: len(rows) >= 2)
        if stop_mode == "camera_offline":
            _exec(container, ["s6-svc", "-d", "/var/run/s6/services/flv-source"])
        # Signal only Viseron so the healthy camera remains available until
        # Viseron's finish script shuts down the other container services.
        if stop_mode == "container":
            container.stop(timeout=45)
        else:
            _exec(container, ["pkill", "-TERM", "-f", "^viseron -u -m viseron$"])
        result = container.wait(timeout=90)
        logs = container.logs().decode(errors="replace")
        assert result["StatusCode"] == 0, logs
        assert "Viseron exit code 0" in logs, logs
        assert "did not exit in time" not in logs, logs
        assert "Timed out fragmenting remaining segments" not in logs, logs
