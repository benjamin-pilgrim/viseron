"""Tests for the camera domain recording schedule config schema."""

from __future__ import annotations

import pytest
import voluptuous as vol

from viseron.components.storage.config import TIER_SCHEMA_TIMELAPSE
from viseron.domains.camera.config import BASE_CONFIG_SCHEMA, RECORDING_SCHEDULE_SCHEMA
from viseron.domains.camera.const import (
    CONFIG_SCHEDULE_EVENTS,
    CONFIG_SCHEDULE_TIMEZONE,
)
from viseron.helpers.validators import UNDEFINED


def test_timezone_defaults_to_undefined():
    """Omitting timezone leaves it UNDEFINED, to be resolved at runtime."""
    result = RECORDING_SCHEDULE_SCHEMA({CONFIG_SCHEDULE_EVENTS: None})
    # voluptuous calls the UNDEFINED class as a default factory, so the stored
    # value is an instance. UNDEFINED.__eq__ makes == the idiomatic check.
    assert result[CONFIG_SCHEDULE_TIMEZONE] == UNDEFINED


def test_timezone_can_be_overridden():
    """An explicit timezone is preserved."""
    result = RECORDING_SCHEDULE_SCHEMA({CONFIG_SCHEDULE_TIMEZONE: "America/New_York"})
    assert result[CONFIG_SCHEDULE_TIMEZONE] == "America/New_York"


def test_timezone_accepts_none():
    """An explicit null falls back to runtime resolution too."""
    result = RECORDING_SCHEDULE_SCHEMA({CONFIG_SCHEDULE_TIMEZONE: None})
    assert result[CONFIG_SCHEDULE_TIMEZONE] is None


def test_invalid_timezone_override_rejected():
    """An invalid timezone override fails schema validation."""
    with pytest.raises(vol.Invalid):
        RECORDING_SCHEDULE_SCHEMA({CONFIG_SCHEDULE_TIMEZONE: "Not/AZone"})


@pytest.mark.parametrize(
    ("storage", "expected_timelapse"),
    [
        pytest.param({}, UNDEFINED, id="defaults_to_undefined"),
        pytest.param({"timelapse": None}, None, id="accepts_none"),
        pytest.param(
            {"timelapse": {"tiers": [{"path": "/timelapse/"}]}},
            {"tiers": [TIER_SCHEMA_TIMELAPSE({"path": "/timelapse/"})]},
            id="override_tiers",
        ),
    ],
)
def test_storage_timelapse(
    storage: dict[str, object], expected_timelapse: object
) -> None:
    """Cameras can override the timelapse tiers."""
    config = BASE_CONFIG_SCHEMA({"name": "Camera", "storage": storage})
    assert config["storage"]["timelapse"] == expected_timelapse


@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
def test_playback_rotation_accepts_quarter_turns(rotation):
    """Recorded video can be oriented in the browser without encoding it."""
    config = BASE_CONFIG_SCHEMA({"name": "test", "playback_rotation": rotation})
    assert config["playback_rotation"] == rotation


@pytest.mark.parametrize("rotation", [-90, 45, 360])
def test_playback_rotation_rejects_unsupported_angles(rotation):
    """Reject ambiguous display orientations."""
    with pytest.raises(vol.Invalid):
        BASE_CONFIG_SCHEMA({"name": "test", "playback_rotation": rotation})
