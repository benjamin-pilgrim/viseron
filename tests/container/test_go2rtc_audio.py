"""Exercise the packaged relay and recorder against synthetic FLV sources."""

from __future__ import annotations

import json
import os
import time
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
    helpers.put_file(image_container, "/tmp", script, source.read_bytes())
    command = image_host.run(
        "env PATH=/ffmpeg/bin:/usr/local/bin:/usr/bin:/bin PYTHONPATH=/src "
        "python3 %s --case %s" + (" --viseron-audio" if options else ""),
        destination,
        case,
    )
    assert command.rc == 0, f"{case}:\n{command.stdout}\n{command.stderr}"


@pytest.mark.parametrize(
    "audio_dropout", [False, True], ids=["late-audio", "audio-dropout"]
)
@pytest.mark.timeout(360)
def test_recording_with_fresh_database(
    docker_client: Any,
    image: str,
    artifact_dir: Path,
    boot_timeout: float,
    request: pytest.FixtureRequest,
    audio_dropout: bool,
) -> None:
    """Index and serve playable video before and after audio returns on a fresh DB."""
    keep = request.config.getoption("--keep-container")
    name = helpers.unique_container_name("viseron-flv-recording")
    container = docker_client.containers.create(
        image,
        name=name,
        ports={"8888/tcp": ("127.0.0.1", None)} if keep else {},
        environment={
            "PUID": str(os.getuid()),
            "PGID": str(os.getgid()),
            "TZ": "UTC",
            "OPENBLAS_NUM_THREADS": "1",
            "OMP_NUM_THREADS": "1",
        },
    )
    directory = Path(__file__).parent
    helpers.put_abs(
        container,
        "/config/config.yaml",
        (directory / "configs" / "flv_recording.yaml").read_bytes(),
    )
    helpers.create_directories(
        container,
        "/recordings",
        "/segments",
        "/snapshots",
        "/thumbnails",
        "/event_clips",
    )
    for script in ("flv_camera_source.py", "flv_recording_probe.py"):
        helpers.put_file(
            container, "/tmp", script, (directory / "scripts" / script).read_bytes()
        )
    helpers.put_abs(
        container,
        "/etc/services.d/flv-source/run",
        (
            "#!/bin/sh\nexec python3 /tmp/flv_camera_source.py"
            + (" --audio-dropout" if audio_dropout else "")
            + "\n"
        ).encode(),
        mode=0o755,
    )
    start = int(time.time()) - 2
    previous_timeout = docker_client.api.timeout
    docker_client.api.timeout = 180
    try:
        container.start()
        helpers.wait_for_log(
            container, r"Viseron initialized in [\d.]+ seconds", timeout=boot_timeout
        )
        # /config belongs solely to this new container; no existing cluster or media
        # is mounted. API playback exercises fresh schema initialization and indexing.
        result = container.exec_run(
            ["python3", "/tmp/flv_recording_probe.py", "--start", str(start)]
            + (["--audio-dropout"] if audio_dropout else [])
        )
        (artifact_dir / f"{name}-recording.txt").write_bytes(result.output)
        assert result.exit_code == 0, result.output.decode(errors="replace")
    finally:
        docker_client.api.timeout = previous_timeout
        (artifact_dir / f"{name}.log").write_bytes(container.logs())
        if keep:
            container.reload()
            port = container.attrs["NetworkSettings"]["Ports"]["8888/tcp"][0][
                "HostPort"
            ]
            (artifact_dir / "browser-target.json").write_text(
                json.dumps(
                    {
                        "container": name,
                        "image": image,
                        "url": f"http://127.0.0.1:{port}",
                        "start": start,
                        "audioDropout": audio_dropout,
                    }
                )
            )
        else:
            container.stop(timeout=15)
            container.remove(force=True)


def test_recorder_retry_and_shutdown(
    image_container: Any, image_host: testinfra.host.Host
) -> None:
    """An exited recorder recovers promptly without a tight loop or shutdown restart."""
    script = "recorder_retry_probe.py"
    helpers.put_file(
        image_container,
        "/tmp",
        script,
        (Path(__file__).parent / "scripts" / script).read_bytes(),
    )
    result = image_host.run("env PYTHONPATH=/src python3 /tmp/recorder_retry_probe.py")
    assert result.rc == 0, result.stdout + result.stderr


def test_recorder_fragment_burst(
    image_container: Any, image_host: testinfra.host.Host
) -> None:
    """Fragments flushed together must preserve all video."""
    script = "recorder_fragment_burst.py"
    helpers.put_file(
        image_container,
        "/tmp",
        script,
        (Path(__file__).parent / "scripts" / script).read_bytes(),
    )
    result = image_host.run("env PYTHONPATH=/src python3 /tmp/%s", script)
    assert result.rc == 0, result.stdout + result.stderr
