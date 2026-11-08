"""Separate retention percentages without an additional tier quota."""

import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import voluptuous as vol

from viseron.components.storage import CONFIG_SCHEMA, Storage
from viseron.components.storage.check_tier import Worker
from viseron.components.storage.config import (
    TIER_SCHEMA_RECORDER,
    TIER_SCHEMA_SNAPSHOTS,
)
from viseron.components.storage.pressure_manager import PressureManager
from viseron.components.storage.tier_handler import SegmentsTierHandler
from viseron.domains.camera.config import RECORDER_SCHEMA as CAMERA_RECORDER_SCHEMA


def _tier():
    return TIER_SCHEMA_RECORDER(
        {
            "path": "/tier",
            "continuous": {"max_size": {"percent": 40}, "min_size": {"percent": 5}},
            "events": {"max_size": {"percent": 20}, "max_age": {"days": 30}},
            "on_full": {"policy": "preserve_events"},
        }
    )


def _handler(tier):
    handler = SegmentsTierHandler.__new__(SegmentsTierHandler)
    handler._tier = tier
    handler._next_tier = None
    handler._tier_id = 0
    handler._category, handler._subcategory = "recorder", "segments"
    handler._throttle_period = datetime.timedelta(0)
    handler._logger = Mock()
    handler._camera = SimpleNamespace(
        identifier="camera",
        recorder=SimpleNamespace(lookback=5),
        temp_segments_folder="/tier/.viseron-spool/camera",
        config={
            "recorder": {"continuous_recording": True, "schedule": None, "lookback": 5}
        },
    )
    handler.add_file_handler = Mock()
    handler.initialize()
    return handler


def test_percentages_reach_worker_without_startup_filesystem_access(monkeypatch):
    """Keep probes out of startup and resolve limits from current worker capacity."""

    def unavailable(_path):
        raise AssertionError("Startup must not probe remote capacity")

    monkeypatch.setattr("os.statvfs", unavailable)
    handler = _handler(_tier())
    assert handler.events_enabled
    assert handler.continuous_enabled
    item = handler._create_dataitem()
    assert item.size_path == "/tier/segments/camera"
    assert item.size_percentages == {
        "max_bytes": 40,
        "min_bytes": 5,
        "events_max_bytes": 20,
    }
    worker = Worker()
    worker._check_tier = Mock()
    for blocks in (1000, 2000):
        monkeypatch.setattr(
            "os.statvfs",
            lambda _path, blocks=blocks: SimpleNamespace(f_blocks=blocks, f_frsize=100),
        )
        worker.check_tier(item)
        assert item.max_bytes == blocks * 40
        assert item.min_bytes == blocks * 5
        assert item.events_max_bytes == blocks * 20
    assert worker._check_tier.call_count == 2


@pytest.mark.parametrize(
    "failure", ["unavailable", "unknown_capacity", "missing_marker"]
)
def test_unknown_capacity_cannot_select_footage(failure, monkeypatch, tmp_path):
    """Unavailable or untrusted storage must not yield retention candidates."""
    item = _handler(_tier())._create_dataitem()
    worker = Worker()
    worker._check_tier = Mock()
    if failure == "missing_marker":
        item.size_path = str(tmp_path)
        item.tier_markers = {str(tmp_path): ".not-mounted"}

    def capacity(_path):
        if failure == "unavailable":
            raise OSError("offline")
        return SimpleNamespace(f_blocks=0, f_frsize=4096)

    monkeypatch.setattr("os.statvfs", capacity)
    with pytest.raises((OSError, ValueError)):
        worker.check_tier(item)
    worker._check_tier.assert_not_called()
    assert not worker._checks_in_progress["camera"]


@pytest.mark.parametrize("value", [0, -1, 101, True, "NaN", "inf"])
def test_invalid_retention_percentage_is_rejected(value):
    """Reject malformed percentages rather than silently ignoring the limit."""
    with pytest.raises(vol.Invalid):
        TIER_SCHEMA_RECORDER(
            {"path": "/tier", "events": {"max_size": {"percent": value}}}
        )


@pytest.mark.parametrize("unit", ["gb", "mb"])
def test_percentage_and_absolute_units_cannot_mix(unit):
    """One size rule has one unit, so its meaning is unambiguous."""
    with pytest.raises(vol.Invalid, match="not both"):
        TIER_SCHEMA_RECORDER(
            {"path": "/tier", "continuous": {"max_size": {"percent": 40, unit: 1}}}
        )


def test_normalized_new_and_legacy_configuration_roundtrip():
    """Camera overrides preserve percentages and existing combined configurations."""
    tier = _tier()
    assert TIER_SCHEMA_RECORDER(tier) == tier
    assert "capacity" not in tier
    assert "retention" not in tier
    legacy = TIER_SCHEMA_RECORDER({"path": "/tier", "retention": {"max_size": "80%"}})
    assert TIER_SCHEMA_RECORDER(legacy) == legacy
    assert legacy["retention"] == {"max_size": "80%"}
    legacy["continuous"]["max_size"]["percent"] = 40
    with pytest.raises(vol.Invalid, match="not both"):
        TIER_SCHEMA_RECORDER(legacy)
    with pytest.raises(vol.Invalid, match="extra keys"):
        TIER_SCHEMA_RECORDER(
            {"path": "/tier", "retention": {}, "capacity": {"max_size": "80%"}}
        )
    with pytest.raises(vol.Invalid):
        TIER_SCHEMA_SNAPSHOTS({"path": "/tier", "max_size": {"percent": 40}})


def test_pressure_uses_no_extra_quota_for_separate_retention():
    """The shared controller handles real disk pressure without another quota."""
    handler = _handler(_tier())
    second = _handler(_tier())
    second._camera.identifier = "second"
    storage = SimpleNamespace(
        camera_tier_handlers={
            name: {"recorder": [{"segments": item}]}
            for name, item in (("camera", handler), ("second", second))
        },
        config={},
        camera_requested_files_count={},
    )
    config = PressureManager(storage).configurations()["/tier/"]
    assert config["max_size"] == 0
    assert config["policy"] == "preserve_events"
    assert len(config["cameras"]) == 2


def test_percentage_alone_enables_only_the_configured_class():
    """Size-only event retention does not implicitly enable continuous retention."""
    tier = TIER_SCHEMA_RECORDER(
        {"path": "/tier", "events": {"max_size": {"percent": 20}}}
    )
    handler = _handler(tier)
    assert handler.events_enabled
    assert not handler.continuous_enabled
    item = handler._create_dataitem()
    assert item.events_enabled
    assert not item.files_enabled


def test_percentage_first_tier_validates_later_class_rules(monkeypatch):
    """A percentage is a real first-tier retention rule during config validation."""
    monkeypatch.setattr("viseron.components.storage.config._check_path_exists", Mock())
    config = CONFIG_SCHEMA(
        {
            "storage": {
                "recorder": {
                    "tiers": [
                        {
                            "path": "/tier",
                            "continuous": {"max_size": {"percent": 40}},
                            "events": {"max_size": {"percent": 20}},
                        },
                        {
                            "path": "/archive",
                            "continuous": {"max_age": {"days": 7}},
                            "events": {"max_age": {"days": 30}},
                        },
                    ]
                }
            }
        }
    )
    assert (
        config["storage"]["recorder"]["tiers"][0]["events"]["max_size"]["percent"] == 20
    )


def test_check_commands_receive_mount_markers():
    """Percentage probes receive the same mount guards as copy/delete commands."""
    item = _handler(_tier())._create_dataitem()
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {"/tier": ".ready"}}
    storage.tier_check_worker = Mock()
    storage.tier_check_worker_send_command(item)
    assert item.tier_markers == {"/tier": ".ready"}
    storage.tier_check_worker.send_command.assert_called_once_with(item, None)


def test_camera_override_uses_the_same_percentage_validation():
    """Camera recorder overrides accept the unit without bypassing its guards."""
    config = CAMERA_RECORDER_SCHEMA({"events": {"max_size": {"percent": 0.00001}}})
    assert config["events"]["max_size"]["percent"] == 0.00001
    for size in ({"percent": True}, {"percent": 40, "gb": 1}):
        with pytest.raises(vol.Invalid):
            CAMERA_RECORDER_SCHEMA({"events": {"max_size": size}})
