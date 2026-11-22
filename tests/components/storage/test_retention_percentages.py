"""Percentages belong to separate retention rules and resolve in the worker."""

import datetime
from types import SimpleNamespace

import pytest
import numpy as np
import voluptuous as vol
from viseron.domains.camera.config import RECORDER_SCHEMA
from pydantic import ValidationError

from viseron.components.storage.check_tier import (
    FILES_DTYPE,
    RECORDINGS_DTYPE,
    Worker,
    get_recordings_to_move,
)
from viseron.components.storage.config import RecorderTier, SnapshotsTier
from viseron.components.storage.storage_subprocess import DataItem


@pytest.mark.parametrize("kind", ["continuous", "events"])
def test_percentage_retention_enables_the_recording_class(kind):
    """A percentage alone enables retention without probing storage at startup."""
    tier = RecorderTier(path="/archive", **{kind: {"max_size": {"percent": 80}}})
    assert getattr(tier, kind).enabled
    assert getattr(tier, kind).max_size.percent == 80


@pytest.mark.parametrize("value", [0, -1, 101, True, float("nan"), float("inf")])
def test_invalid_percentages_are_rejected(value):
    """Invalid percentages cannot silently disable a requested limit."""
    with pytest.raises(ValidationError):
        RecorderTier(path="/archive", events={"max_size": {"percent": value}})


@pytest.mark.parametrize("unit", ["gb", "mb"])
def test_percentage_cannot_mix_with_absolute_units(unit):
    """Each size rule has exactly one interpretation."""
    with pytest.raises(ValidationError, match="not both"):
        RecorderTier(path="/archive", continuous={"max_size": {"percent": 80, unit: 1}})


def test_snapshot_percentage_is_not_part_of_this_recorder_port():
    """Snapshot storage keeps its existing supported units."""
    with pytest.raises(ValidationError):
        SnapshotsTier(path="/archive", max_size={"percent": 80})


def test_worker_resolves_capacity_again_on_each_check(monkeypatch):
    """Use total filesystem capacity, not changing free space or a startup value."""
    item = DataItem(
        cmd="check_tier",
        camera_identifier="camera",
        tier_id=0,
        category="recorder",
        subcategories=["segments"],
        throttle_period=datetime.timedelta(0),
        max_bytes=0,
        min_bytes=0,
        min_age=datetime.timedelta(0),
        max_age=datetime.timedelta(0),
        drain=False,
    )
    item.size_path = "/archive/segments/camera"
    item.size_percentages = {"max_bytes": 80, "min_bytes": 5, "events_max_bytes": 20}
    worker = Worker()
    observed = []
    worker._check_tier = lambda check: observed.append(
        (check.max_bytes, check.min_bytes, check.events_max_bytes)
    )
    for blocks in (1000, 2000):
        monkeypatch.setattr(
            "os.statvfs",
            lambda _path, blocks=blocks: SimpleNamespace(f_blocks=blocks, f_frsize=100),
        )
        worker.check_tier(item)
    assert observed == [(80000, 5000, 20000), (160000, 10000, 40000)]


def test_unknown_capacity_does_not_run_retention(monkeypatch):
    """An unavailable capacity must never turn a percentage into no retention."""
    item = DataItem(
        cmd="check_tier",
        camera_identifier="camera",
        tier_id=0,
        category="recorder",
        subcategories=["segments"],
        throttle_period=datetime.timedelta(0),
        max_bytes=0,
        min_bytes=0,
        min_age=datetime.timedelta(0),
        max_age=datetime.timedelta(0),
        drain=False,
    )
    item.size_path = "/archive"
    item.size_percentages = {"max_bytes": 80}
    monkeypatch.setattr(
        "os.statvfs", lambda _path: SimpleNamespace(f_blocks=0, f_frsize=4096)
    )
    worker = Worker()
    observed = []
    worker._check_tier = observed.append
    with pytest.raises(ValueError, match="capacity"):
        worker.check_tier(item)
    assert not observed


@pytest.mark.parametrize("kind", ["continuous", "events"])
def test_camera_recording_override_supports_percentages(kind):
    """The camera override accepts the same separate percentage rules."""

    config = RECORDER_SCHEMA({kind: {"max_size": {"percent": 80}}})
    assert config[kind]["max_size"]["percent"] == 80


def test_camera_override_rejects_boolean_percentage():
    """Camera schema coercion cannot turn a boolean into a valid percentage."""

    with pytest.raises(vol.Invalid):
        RECORDER_SCHEMA({"events": {"max_size": {"percent": True}}})


def test_overlapping_events_count_shared_segments_once():
    """Two overlapping events must not consume two copies of the same bytes."""
    files = np.array([(1, 100, 108, "/tier/1.m4s", "/tier")], dtype=FILES_DTYPE)
    recordings = np.array(
        [(1, 100, 100, 110, 100), (2, 105, 105, 115, 105)], dtype=RECORDINGS_DTYPE
    )
    result = get_recordings_to_move(recordings, files, 5, 150, 1000, 0, 0, 1000, False)
    assert len(result) == 0


def test_unexpired_event_keeps_footage_shared_with_an_expired_event():
    """An old event cannot evict a segment still required by a retained event."""
    files = np.array([(1, 100, 108, "/tier/1.m4s", "/tier")], dtype=FILES_DTYPE)
    recordings = np.array(
        [(1, 100, 100, 110, 100), (2, 105, 105, 115, 105)], dtype=RECORDINGS_DTYPE
    )
    result = get_recordings_to_move(recordings, files, 5, 0, 1000, 0, 102, 1000, False)
    assert len(result) == 0
