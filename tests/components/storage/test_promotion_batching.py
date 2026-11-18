"""Batch completed footage without delaying emergency RAM draining."""

import datetime
import threading
import time
from types import SimpleNamespace

import pytest
import voluptuous as vol

from viseron.components.storage.config import TIER_SCHEMA_RECORDER
from viseron.components.storage.pressure import reclaim
from viseron.components.storage.pressure_manager import PressureManager

from tests.components.storage.test_pressure_policy import (
    _configuration,
    _record,
    _real_pressure_child,
)


def batch_config(root):
    """An explicit 60-second batch on top of safe prompt forwarding."""
    config = _configuration(root, "preserve_events", maximum="80%")
    config.update(promotion_batch_interval=60, promotion_cutoff=None)
    config["cameras"][0].update(has_next=True, promote_after=0)
    return config


def test_normal_root_waits_for_batch(get_db_session, tmp_path):
    """Already safe footage remains local while the batch timer is closed."""
    path = _record(
        get_db_session,
        tmp_path,
        "camera",
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=2),
        1024,
    )
    result = reclaim(batch_config(tmp_path), get_db_session)
    assert result["deleted"] == 0
    assert not result.get("promote")
    assert path.read_bytes() == b"x" * 1024


def test_batch_cutoff_does_not_follow_incoming_footage(get_db_session, tmp_path):
    """A draining batch excludes footage that became safe after it opened."""
    now = datetime.datetime.now(datetime.UTC)
    old = _record(
        get_db_session, tmp_path, "camera", now - datetime.timedelta(seconds=120), 1024
    )
    newer = _record(
        get_db_session, tmp_path, "camera", now - datetime.timedelta(seconds=40), 1024
    )
    config = batch_config(tmp_path)
    config["promotion_cutoff"] = now - datetime.timedelta(seconds=30)
    result = reclaim(config, get_db_session)
    assert [c["path"] for c in result["promote"]] == [str(old)]
    assert old.exists() and newer.exists()


@pytest.mark.parametrize("value", [-1, True, 0.5, "60"])
def test_invalid_batch_interval_is_rejected(value):
    """Batch cadence uses explicit nonnegative whole seconds."""
    with pytest.raises(vol.Invalid):
        TIER_SCHEMA_RECORDER(
            {"path": "/ram", "promote_after": 0, "promotion_batch_interval": value}
        )


def test_batch_requires_forwarding():
    """A batching setting must not be silently ignored on a retention-only tier."""
    with pytest.raises(vol.Invalid):
        TIER_SCHEMA_RECORDER({"path": "/ram", "promotion_batch_interval": 60})


def test_batch_configuration_round_trips():
    """Recorder tier settings survive normalization and camera overrides."""
    tier = TIER_SCHEMA_RECORDER(
        {"path": "/ram", "promote_after": 0, "promotion_batch_interval": 60}
    )
    assert tier["promotion_batch_interval"] == 60
    assert TIER_SCHEMA_RECORDER(tier) == tier


def test_manager_opens_batch_at_deadline(get_db_session, tmp_path, monkeypatch):
    """A root checks pressure during the quiet period but dispatches only at 60s."""
    now = datetime.datetime.now(datetime.UTC)
    paths = [
        _record(
            get_db_session,
            tmp_path,
            "camera",
            now - datetime.timedelta(seconds=120 - i),
            1024,
        )
        for i in range(17)
    ]
    config = batch_config(tmp_path)
    config.update(
        test_db_url=get_db_session.kw["bind"].url.render_as_string(hide_password=False),
        test_sync_error=False,
    )
    clock = [100.0]
    monkeypatch.setattr(
        "viseron.components.storage.pressure_manager.time",
        SimpleNamespace(monotonic=lambda: clock[0]),
    )
    monkeypatch.setattr(
        "viseron.components.storage.pressure_manager.run_pressure", _real_pressure_child
    )
    storage = SimpleNamespace(pending_moves={}, pending_moves_lock=threading.Lock())
    manager = PressureManager(storage)
    manager.configurations = lambda: {str(tmp_path): dict(config)}
    dispatched = []

    def dispatch(_config, candidates):
        dispatched.extend(candidates)
        with storage.pending_moves_lock:
            storage.pending_moves.update({item["path"]: True for item in candidates})

    manager._promote = dispatch

    def poll_once():
        manager.poll()
        deadline = time.monotonic() + 10
        while manager.pending and time.monotonic() < deadline:
            time.sleep(0.01)
            manager.poll()
        assert not manager.pending
        if str(tmp_path) in manager.promotions:
            manager.promotions[str(tmp_path)].join(timeout=5)

    try:
        poll_once()
        assert not dispatched
        clock[0] = 159.0
        poll_once()
        assert not dispatched
        clock[0] = 160.0
        poll_once()
        assert [c["path"] for c in dispatched] == [str(p) for p in paths[:16]]
        clock[0] = 161.0
        poll_once()
        assert [c["path"] for c in dispatched] == [str(p) for p in paths]
        clock[0] = 162.0
        poll_once()  # Exhaust the fixed batch; all transfers are now reserved.
        later = _record(
            get_db_session,
            tmp_path,
            "camera",
            now - datetime.timedelta(seconds=50),
            1024,
        )
        clock[0] = 163.0
        poll_once()
        assert len(dispatched) == 17
        clock[0] = 221.0
        poll_once()
        assert len(dispatched) == 17
        clock[0] = 222.0
        poll_once()
        assert [c["path"] for c in dispatched] == [str(p) for p in [*paths, later]]
    finally:
        manager.stop()


@pytest.mark.parametrize("intervals", [(60, 60), (60, 0)])
def test_cameras_share_one_batch_clock(tmp_path, intervals):
    """A shared source root uses one cadence and rejects conflicting overrides."""
    root = str(tmp_path)
    handlers = {}
    for identifier, interval in zip(("one", "two"), intervals):
        tier = TIER_SCHEMA_RECORDER(
            {"path": root, "promote_after": 0, "promotion_batch_interval": interval}
        )
        camera = SimpleNamespace(
            temp_segments_folder=root + "/.spool/" + identifier, config={}
        )
        handler = SimpleNamespace(
            tier=tier, _camera=camera, _path=root + "/segments/" + identifier
        )
        handlers[identifier] = {"recorder": [{"segments": handler}, {}]}
    storage = SimpleNamespace(
        camera_tier_handlers=handlers, config={}, camera_requested_files_count={}
    )
    manager = PressureManager(storage)
    if intervals[0] != intervals[1]:
        with pytest.raises(
            ValueError, match="shared tier needs one promotion batch interval"
        ):
            manager.configurations()
    else:
        roots = manager.configurations()
        assert len(roots) == 1
        configuration = next(iter(roots.values()))
        assert configuration["promotion_batch_interval"] == 60
        assert {c["identifier"] for c in configuration["cameras"]} == {"one", "two"}
