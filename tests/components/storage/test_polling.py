"""Storage monitoring must not repeatedly enumerate owned segment archives."""

import os
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from viseron.components.storage.config import RecorderTier, Tier
from viseron.components.storage.tier_handler import (
    SegmentsTierHandler,
    TierHandler,
    EventClipTierHandler,
    ThumbnailTierHandler,
)
from viseron.const import VISERON_SIGNAL_STOPPING


@pytest.mark.parametrize("poll", [False, True])
def test_segment_archive_startup_does_not_enumerate_history(
    vis, camera, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, poll: bool
) -> None:
    """Transferred archive files are indexed by exact path, without a tree walk."""
    archive = tmp_path / "segments" / camera.identifier
    history = archive / "old-run"
    history.mkdir(parents=True)
    (history / "1.m4s").write_bytes(b"historical video")
    scans = []
    scandir = os.scandir

    def counted_scandir(path):
        if os.path.commonpath((str(tmp_path), os.fsdecode(path))) == str(tmp_path):
            scans.append(os.fsdecode(path))
        return scandir(path)

    monkeypatch.setattr(os, "scandir", counted_scandir)
    try:
        SegmentsTierHandler(
            vis,
            camera,
            1,
            "recorder",
            "segments",
            RecorderTier(
                path=str(tmp_path), poll=poll, continuous={"max_age": {"days": 1}}
            ),
            None,
        )
        assert scans == [], f"archive startup enumerated historical footage: {scans}"
    finally:
        for call in vis.register_signal_handler.call_args_list:
            if call.args[0] == VISERON_SIGNAL_STOPPING:
                call.args[1]()


@pytest.mark.parametrize(
    "tier_id,category,subcategory",
    [
        (0, "recorder", "segments"),
        (1, "recorder", "thumbnails"),
        (1, "snapshots", "object_detector"),
    ],
)
def test_source_and_asset_pollers_still_deliver_changes(
    vis, camera, tmp_path, monkeypatch, tier_id, category, subcategory
):
    """Only later segment archives stop watching; other writers retain events."""

    events = []
    monkeypatch.setattr(
        TierHandler, "on_any_event", lambda _self, event: events.append(event)
    )
    tier = Tier(path=str(tmp_path), poll=True, poll_interval=0.05)
    try:
        TierHandler(vis, camera, tier_id, category, subcategory, tier, None)
        file = tmp_path / category / subcategory / camera.identifier / "new-file"
        file.write_bytes(b"new footage")
        deadline = time.monotonic() + 2
        while (
            not any(
                os.fsdecode(event.src_path) == str(file)
                and event.event_type == "created"
                for event in events
            )
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert any(
            os.fsdecode(event.src_path) == str(file) and event.event_type == "created"
            for event in events
        )
    finally:
        for call in vis.register_signal_handler.call_args_list:
            if call.args[0] == VISERON_SIGNAL_STOPPING:
                call.args[1]()


@pytest.mark.parametrize("interval", [0, -1, float("inf"), float("nan")])
def test_poll_interval_requires_finite_positive_seconds(interval):
    """Invalid scan intervals must fail validation before a watcher starts."""

    with pytest.raises(ValidationError):
        Tier(path="/recordings", poll_interval=interval)


@pytest.mark.parametrize("missing_root", [False, True])
@pytest.mark.parametrize("subcategory", ["segments", "thumbnails", "event_clips"])
def test_later_recorder_tier_can_start_when_archive_is_unavailable(
    vis, camera, tmp_path, monkeypatch, subcategory, missing_root
):
    """An unavailable archive must not prevent first-tier camera recording."""
    classes = {
        "segments": SegmentsTierHandler,
        "thumbnails": ThumbnailTierHandler,
        "event_clips": EventClipTierHandler,
    }
    real_makedirs = os.makedirs

    def unavailable(path, *args, **kwargs):
        if os.path.commonpath((str(tmp_path), str(path))) == str(tmp_path):
            raise PermissionError("archive is offline")
        return real_makedirs(path, *args, **kwargs)

    if not missing_root:
        monkeypatch.setattr(os, "makedirs", unavailable)
    root = tmp_path / "offline" if missing_root else tmp_path
    handler = None
    try:
        handler = classes[subcategory](
            vis,
            camera,
            1,
            "recorder",
            subcategory,
            RecorderTier(path=str(root)),
            None,
        )
        if missing_root:
            assert not root.exists()
    finally:
        if handler is not None:
            for call in vis.register_signal_handler.call_args_list:
                if call.args[0] == VISERON_SIGNAL_STOPPING:
                    call.args[1]()
