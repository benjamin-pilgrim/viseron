"""Packaged recording recovery against a disappearing, unwritable archive."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest
from docker.types import LogConfig

from . import _helpers as helpers

CAMERA = "recovery"
OFFLINE = (
    "import os; os.rename('/archive','/archive.offline'); os.mkdir('/archive',0o555)"
)
ONLINE = (
    "import os; assert not os.listdir('/archive'); os.rmdir('/archive'); "
    "os.rename('/archive.offline','/archive')"
)


def _exec(container, command):
    result = container.exec_run(command)
    assert result.exit_code == 0, result.output.decode(errors="replace")
    return result.output


def _sql(container, query):
    statement = "SELECT coalesce(json_agg(row_to_json(x)), '[]') FROM (" + query + ") x"  # noqa: S608 - test-owned SQL
    result = _exec(
        container,
        ["s6-setuidgid", "postgres", "psql", "-tAc", statement, "-d", "viseron"],
    )
    return json.loads(result)


def _wait(operation, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    value = None
    while time.monotonic() < deadline:
        try:
            value = operation()
        except (urllib.error.URLError, OSError) as error:
            value = repr(error)
        else:
            if predicate(value):
                return value
        time.sleep(1)
    raise AssertionError(f"Recovery condition not reached: {value}")


def _http(url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.read()


@contextmanager
def _recording_case(docker_client, image, artifact_dir, request, *, offline=False):
    name = helpers.unique_container_name("viseron-storage-recovery")
    container = docker_client.containers.create(
        image,
        name=name,
        oom_score_adj=1000,
        log_config=LogConfig(type="json-file"),
        ports={"8888/tcp": ("127.0.0.1", None)},
        tmpfs={
            "/ram": "rw,size=64m,mode=1777",
            "/tmp/viseron": "rw,size=64m,mode=1777",
        },
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
        (directory / "configs/storage_recovery.yaml").read_bytes(),
    )
    helpers.create_directories(
        container,
        "/recordings",
        "/segments",
        "/snapshots",
        "/thumbnails",
        "/event_clips",
        "/archive",
        owner=(os.getuid(), os.getgid()),
    )
    helpers.put_file(
        container,
        "/tmp",
        "flv_camera_source.py",
        (directory / "scripts/flv_camera_source.py").read_bytes(),
    )
    helpers.put_abs(
        container,
        "/etc/services.d/flv-source/run",
        b"#!/bin/sh\nexec python3 /tmp/flv_camera_source.py\n",
        mode=0o755,
    )
    if offline:
        helpers.put_abs(
            container,
            "/etc/cont-init.d/85-test-archive",
            (
                '#!/usr/bin/with-contenv sh\npython3 -c "'
                + OFFLINE.replace('"', '\\"')
                + '"\n'
            ).encode(),
            mode=0o755,
        )
    try:
        container.start()
        helpers.wait_for_log(
            container, r"Viseron initialized in [\d.]+ seconds", timeout=120
        )
        container.reload()
        port = container.attrs["NetworkSettings"]["Ports"]["8888/tcp"][0]["HostPort"]
        yield container, f"http://127.0.0.1:{port}"
    finally:
        (artifact_dir / (name + ".log")).write_bytes(container.logs())
        if not request.config.getoption("--keep-container"):
            container.stop(timeout=20)
            container.remove(force=True)


def _segments(container):
    return _sql(
        container,
        (
            "SELECT f.id, f.orig_ctime, l.path, l.tier_id FROM files f "
            "JOIN file_locations l ON l.file_id=f.id "
            "WHERE f.camera_identifier='recovery' AND f.subcategory='segments' "
            "AND l.state='available' ORDER BY f.orig_ctime"
        ),
    )


def _decode_history(container, url, start):
    command = [
        "/ffmpeg/bin/ffmpeg",
        "-v",
        "error",
        "-allowed_extensions",
        "ALL",
        "-allowed_segment_extensions",
        "ALL",
        "-extension_picky",
        "0",
        "-i",
        f"{url}/api/v1/hls/recovery/index.m3u8?start_timestamp={start}&end_timestamp={int(time.time())}",
        "-t",
        "5",
        "-f",
        "null",
        "-",
    ]
    _exec(container, command)


@pytest.mark.timeout(240)
def test_archive_unavailable_at_startup(docker_client, image, artifact_dir, request):
    """An unavailable later tier cannot prevent local recording or later recovery."""
    with _recording_case(docker_client, image, artifact_dir, request, offline=True) as (
        container,
        url,
    ):
        rows = _wait(
            lambda: _segments(container), lambda rows: len(rows) >= 2, timeout=45
        )
        assert all(row["tier_id"] == 0 for row in rows)
        _exec(container, ["python3", "-c", ONLINE])
        _exec(container, ["rm", "/etc/cont-init.d/85-test-archive"])
        _wait(
            lambda: _segments(container),
            lambda rows: any(row["tier_id"] == 1 for row in rows),
        )
        _http(url + "/api/v1/cameras")


@pytest.mark.timeout(300)
def test_outage_and_restart_preserve_published_recording(
    docker_client, image, artifact_dir, request
):
    """Recording survives an archive outage and replays promoted data after restart."""
    with _recording_case(docker_client, image, artifact_dir, request) as (
        container,
        _url,
    ):
        initial = _wait(lambda: _segments(container), lambda rows: len(rows) >= 2)
        _exec(container, ["python3", "-c", OFFLINE])
        during = _wait(
            lambda: _segments(container), lambda rows: len(rows) >= len(initial) + 3
        )
        retained = [row for row in during if row["tier_id"] == 0]
        assert retained
        digest = _exec(container, ["sha256sum", retained[0]["path"]]).split()[0]
        _exec(container, ["python3", "-c", ONLINE])
        file_id = retained[0]["id"]
        rows = _wait(
            lambda: _segments(container),
            lambda rows: any(
                row["id"] == file_id and row["tier_id"] == 1 for row in rows
            ),
        )
        archived = next(
            row for row in rows if row["id"] == file_id and row["tier_id"] == 1
        )
        assert _exec(container, ["sha256sum", archived["path"]]).split()[0] == digest
        before = archived["orig_ctime"]
        container.restart(timeout=20)
        helpers.wait_for_log(
            container, r"Viseron initialized in [\d.]+ seconds", timeout=120
        )
        # Use the in-container port so a restart's host-port allocation cannot matter.
        start = int(datetime.fromisoformat(before).timestamp())
        container.reload()
        port = container.attrs["NetworkSettings"]["Ports"]["8888/tcp"][0]["HostPort"]
        _wait(
            lambda: _http(f"http://127.0.0.1:{port}/api/v1/cameras"),
            lambda data: b"recovery" in data,
        )
        _decode_history(container, "http://127.0.0.1:8888", start)


@pytest.mark.timeout(300)
def test_events_and_missing_thumbnails_recover(
    docker_client, image, artifact_dir, request
):
    """Closed event assets survive promotion and a missing thumbnail is rebuilt."""
    with _recording_case(docker_client, image, artifact_dir, request, offline=True) as (
        container,
        url,
    ):
        _wait(lambda: _segments(container), lambda rows: len(rows) >= 3)
        _exec(container, ["python3", "-c", ONLINE])
        _http(
            url + "/api/v1/camera/recovery/manual_recording",
            {"action": "start", "duration": 10},
        )
        records = _wait(
            lambda: _sql(
                container,
                (
                    "SELECT id, end_time, thumbnail_path, thumbnail_file_id, "
                    "clip_path, "
                    "clip_file_id FROM recordings WHERE camera_identifier='recovery' "
                    "ORDER BY id DESC LIMIT 1"
                ),
            ),
            lambda rows: (
                rows
                and rows[0]["end_time"]
                and rows[0]["clip_file_id"]
                and rows[0]["thumbnail_file_id"]
            ),
        )
        recording = records[0]
        thumbnail_url = url + f"/api/v1/recordings/recovery/{recording['id']}/thumbnail"
        assert _http(thumbnail_url).startswith(b"\xff\xd8")
        ids = (recording["thumbnail_file_id"], recording["clip_file_id"])
        assets = _wait(
            lambda: _sql(
                container,
                (
                    "SELECT l.file_id, l.path, l.tier_id FROM file_locations l "
                    "JOIN files f ON f.id=l.file_id "
                    "WHERE f.camera_identifier='recovery' "
                    "AND f.subcategory IN ('thumbnails','event_clips') "
                    "AND l.state='available'"
                ),
            ),
            lambda rows: (
                {row["file_id"] for row in rows if row["tier_id"] == 1} == set(ids)
            ),
        )
        thumbnail = next(
            row["path"]
            for row in assets
            if row["file_id"] == ids[0] and row["tier_id"] == 1
        )
        _exec(container, ["rm", thumbnail])
        assert _http(thumbnail_url).startswith(b"\xff\xd8"), (
            "Missing thumbnail must regenerate from retained segments"
        )
