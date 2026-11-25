"""Opt-in recorder-run acceptance: pass --run-model-acceptance with --image.

The current hash-sidecar implementation does not meet this gate. A real FFmpeg
restart must create a separate playable run and split/resume a timed manual
recording without extending its original deadline.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import PurePosixPath

import pytest

from .test_storage_recovery import _exec, _http, _recording_case, _segments, _wait

pytestmark = [pytest.mark.xdist_group("recorder_runs"), pytest.mark.timeout(300)]

DECODER_PIDS = """
import json
from pathlib import Path
pids = []
for entry in Path('/proc').glob('[0-9]*'):
    try:
        command = (entry / 'cmdline').read_bytes().split(b'\\0')
    except (FileNotFoundError, ProcessLookupError):
        continue
    if command and command[0].rsplit(b'/', 1)[-1] == b'ffmpeg_recovery':
        pids.append(int(entry.name))
print(json.dumps(pids))
"""


def _recordings(url):
    days = json.loads(_http(url + "/api/v1/recordings/recovery"))
    return sorted(
        (recording for day in days.values() for recording in day.values()),
        key=lambda recording: recording["start_timestamp"],
    )


@pytest.fixture(scope="module")
def restarted_recording(docker_client, image, artifact_dir, request):
    """Observe one real recorder restart, without restarting Viseron or its DB."""
    if not request.config.getoption("--run-model-acceptance"):
        pytest.skip("Opt in with --run-model-acceptance to check the missing run model")

    with _recording_case(docker_client, image, artifact_dir, request) as (
        container,
        url,
    ):
        before = _wait(lambda: _segments(container), lambda rows: len(rows) >= 2)[-1]
        duration = 60
        _http(
            url + "/api/v1/camera/recovery/manual_recording",
            {"action": "start", "duration": duration},
        )
        original = _wait(
            lambda: _recordings(url), lambda rows: rows and rows[0]["end_time"] is None
        )[0]
        deadline = original["start_timestamp"] + duration
        pids = json.loads(_exec(container, ["python3", "-c", DECODER_PIDS]))
        assert len(pids) == 1, f"Expected exactly one synthetic camera decoder: {pids}"
        interrupted_at = time.time()
        _exec(container, ["kill", "-KILL", str(pids[0])])
        replacement = _wait(
            lambda: json.loads(_exec(container, ["python3", "-c", DECODER_PIDS])),
            lambda current: len(current) == 1 and current[0] != pids[0],
            timeout=40,
        )
        after = _wait(
            lambda: [
                row
                for row in _segments(container)
                if datetime.fromisoformat(row["orig_ctime"])
                .replace(tzinfo=UTC)
                .timestamp()
                > interrupted_at
            ],
            bool,
            timeout=40,
        )[-1]
        resumed = _recordings(url)
        assert time.time() < deadline - 5, (
            "Recovery took too long to observe manual resume"
        )
        final = _wait(
            lambda: _recordings(url),
            lambda rows: (
                time.time() >= deadline + 2
                and rows
                and all(row["end_timestamp"] is not None for row in rows)
            ),
            timeout=max(1, deadline - time.time()) + 15,
        )
        # Transfers can change paths during the journey. Resolve known file IDs
        # again so a successful promotion does not look like missing footage.
        retained = {row["id"]: row for row in _segments(container)}
        evidence = {
            "image": image,
            "old_decoder_pid": pids[0],
            "replacement_decoder_pid": replacement[0],
            "interrupted_at": interrupted_at,
            "original_deadline": deadline,
            "before": retained[before["id"]],
            "after": retained[after["id"]],
            "original": original,
            "resumed": resumed,
            "final": final,
        }
        (artifact_dir / (container.name + "-restart.json")).write_text(
            json.dumps(evidence, indent=2)
        )
        yield container, evidence


def test_restart_preserves_separate_playable_run_directories(restarted_recording):
    """Old and new recorder output must have distinct, ordered, self-contained runs."""
    container, evidence = restarted_recording
    runs = []
    for segment in (evidence["before"], evidence["after"]):
        path = PurePosixPath(segment["path"])
        parts = path.parts
        camera_index = parts.index("segments") + 1
        assert parts[camera_index] == "recovery", path
        assert len(parts[camera_index + 1 :]) == 2, (
            f"Expected segments/recovery/<run>/<segment>, got {path}"
        )
        runs.append(path.parent.name)
        init = path.parent / "init.mp4"
        _exec(
            container,
            [
                "/ffmpeg/bin/ffmpeg",
                "-v",
                "error",
                "-i",
                f"concat:{init}|{path}",
                "-frames:v",
                "1",
                "-f",
                "null",
                "-",
            ],
        )
    assert runs[0] < runs[1], f"Run directories must sort by start order: {runs}"


def test_recorder_restart_splits_and_resumes_manual_event(restarted_recording):
    """Restart closes the original event and resumes manual recording in a new event."""
    _container, evidence = restarted_recording
    resumed = evidence["resumed"]
    assert len(resumed) == 2, f"Expected two event records across restart: {resumed}"
    first, second = resumed
    assert first["id"] == evidence["original"]["id"]
    assert first["end_timestamp"] is not None, "Original event still spans the restart"
    assert first["end_timestamp"] <= second["start_timestamp"]
    assert second["end_timestamp"] is None, "Manual recording did not resume"


def test_resumed_manual_event_keeps_original_deadline(restarted_recording):
    """Outage time must not extend the requested duration after manual resume."""
    _container, evidence = restarted_recording
    final = evidence["final"]
    assert len(final) == 2, f"Expected a separately resumed manual recording: {final}"
    assert final[-1]["id"] != evidence["original"]["id"]
    assert abs(final[-1]["end_timestamp"] - evidence["original_deadline"]) <= 2, (
        "Resumed recording must stop at the original deadline (two-second tolerance)"
    )
