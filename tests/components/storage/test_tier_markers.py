"""Tests for opting storage paths into availability marker checks."""

import logging
import os
import queue
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import voluptuous as vol
from watchdog.events import FileDeletedEvent

from viseron.components.storage import Storage
from viseron.components.storage.check_tier import delete_file, move_file
from viseron.components.storage.config import (
    STORAGE_SCHEMA,
    _check_path_exists,
    _validate_recorder_tiers,
    validate_tier_markers,
    validate_tiers,
)
from viseron.components.storage.jobs import OrphanedFilesCleanup
from viseron.components.storage.storage_subprocess import DataItemDeleteFile
from viseron.components.storage.tier_handler import TierHandler
from viseron.components.storage.util import tier_markers_available


def test_missing_marker_blocks_only_protected_paths(tmp_path: Path) -> None:
    """An unmounted share can leave its empty mountpoint behind."""
    root = tmp_path / "nas"
    root.mkdir()
    markers = {str(root): ".viseron-ready"}
    assert not tier_markers_available(str(root / "segments"), markers)
    assert tier_markers_available(str(tmp_path / "nas-backup" / "file"), markers)
    (root / ".viseron-ready").write_text("ready")
    assert tier_markers_available(str(root / "segments"), markers)


def test_marker_must_be_a_regular_file_on_the_tier(tmp_path: Path) -> None:
    """A symlink to a file outside the tier cannot establish availability."""
    root = tmp_path / "nas"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.touch()
    (root / ".ready").symlink_to(outside)
    assert not tier_markers_available(str(root / "segment"), {str(root): ".ready"})


def test_marker_configuration_is_accepted(tmp_path: Path) -> None:
    """The component schema accepts normalized opt-in marker roots."""
    config = STORAGE_SCHEMA({"tier_markers": {f"{tmp_path}/": ".ready"}})
    assert config["tier_markers"] == {str(tmp_path): ".ready"}


def test_offline_later_tier_is_allowed_at_startup(tmp_path: Path) -> None:
    """An unavailable protected destination must not block a local first tier."""
    root = tmp_path / "offline"
    tier = {"path": str(root / "recordings")}
    markers = {str(root): ".ready"}
    _check_path_exists(tier, "recorder", markers, allow_unavailable=True)
    with pytest.raises(vol.Invalid):
        _check_path_exists(tier, "recorder", markers)


def test_recorder_validation_allows_offline_guarded_destination(tmp_path: Path) -> None:
    """The complete recorder validation accepts an offline protected second tier."""
    local = tmp_path / "local"
    local.mkdir()
    remote = tmp_path / "remote"
    config = STORAGE_SCHEMA(
        {
            "tier_markers": {str(remote): ".ready"},
            "recorder": {
                "tiers": [
                    {"path": str(local), "events": {"max_age": {"days": 1}}},
                    {
                        "path": str(remote / "recordings"),
                        "events": {"max_age": {"days": 2}},
                    },
                ]
            },
        }
    )
    _validate_recorder_tiers(config)
    camera_config = {"storage": {"recorder": config["recorder"]}}
    assert validate_tiers(camera_config, tier_markers=config["tier_markers"])
    assert "tier_markers" not in camera_config["storage"]


def test_start_deferred_watcher_on_recovery(tmp_path: Path) -> None:
    """A watcher absent during startup is created when its share returns."""
    handler = TierHandler.__new__(TierHandler)
    handler._path = str(tmp_path / "recordings")
    handler._observer_lock = threading.Lock()
    handler._observer_stopping = False
    handler._observer = None
    handler._storage = MagicMock()
    handler._storage.tier_available.return_value = True
    handler._start_observer = MagicMock()  # type: ignore[method-assign]
    assert handler.restart_for_tier(str(tmp_path))
    handler._start_observer.assert_called_once()  # type: ignore[attr-defined]


def test_worker_receives_configured_markers(tmp_path: Path) -> None:
    """Marker configuration must cross the storage subprocess boundary."""
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {str(tmp_path): ".ready"}}
    storage.tier_check_worker = MagicMock()
    item = DataItemDeleteFile(cmd="delete_file", src=str(tmp_path / "segment"))
    storage.tier_check_worker_send_command(item)
    assert item.tier_markers == {str(tmp_path): ".ready"}
    storage.tier_check_worker.send_command.assert_called_once_with(item, None)


def test_tier_health_logs_only_transitions(tmp_path: Path, caplog: object) -> None:
    """Repeated failures do not log each affected file; recovery is reported."""
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {str(tmp_path): ".ready"}}
    storage._tier_marker_states = {}
    storage._tier_marker_lock = threading.Lock()
    storage._tier_markers_need_retry = False
    storage._tier_markers_recovered_roots = set()
    caplog.set_level(logging.INFO)  # type: ignore[attr-defined]
    path = str(tmp_path / "segment")
    assert not storage.tier_available(path)
    assert not storage.tier_available(path)
    (tmp_path / ".ready").touch()
    assert storage.tier_available(path)
    assert caplog.text.count("is unavailable") == 1  # type: ignore[attr-defined]
    assert "is available again" in caplog.text  # type: ignore[attr-defined]


def test_marker_recovery_retries_tier_checks(tmp_path: Path) -> None:
    """An idle system retries deferred moves when a tier becomes available."""
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {str(tmp_path): ".ready"}}
    storage._tier_marker_states = {}
    storage._tier_marker_lock = threading.Lock()
    storage._tier_markers_need_retry = False
    storage._tier_markers_recovered_roots = set()
    handler = MagicMock()
    storage._camera_tier_handlers = {"camera": {"recorder": [{"segments": handler}]}}

    storage._poll_tier_markers()
    handler.check_tier.assert_not_called()
    (tmp_path / ".ready").touch()
    storage._poll_tier_markers()
    handler.check_tier.assert_called_once_with(force=True)
    storage._poll_tier_markers()
    handler.check_tier.assert_called_once()


def test_recovery_retries_again_when_check_already_running(tmp_path: Path) -> None:
    """Do not lose the recovery retry while a previous check is in flight."""
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {str(tmp_path): ".ready"}}
    storage._tier_marker_states = {}
    storage._tier_marker_lock = threading.Lock()
    storage._tier_markers_need_retry = False
    storage._tier_markers_recovered_roots = set()
    handler = MagicMock()
    handler.check_tier.side_effect = [False, True]
    storage._camera_tier_handlers = {"camera": {"recorder": [{"segments": handler}]}}
    storage._poll_tier_markers()
    (tmp_path / ".ready").touch()
    storage._poll_tier_markers()
    storage._poll_tier_markers()
    assert handler.check_tier.call_count == 2
    assert not storage._tier_markers_need_retry


def test_recovery_retries_failed_watcher_before_moving(tmp_path: Path) -> None:
    """Do not resume tier moves until the recovered watcher is attached."""
    storage = Storage.__new__(Storage)
    storage._config = {"tier_markers": {str(tmp_path): ".ready"}}
    storage._tier_marker_states = {}
    storage._tier_marker_lock = threading.Lock()
    storage._tier_markers_need_retry = False
    storage._tier_markers_recovered_roots = set()
    handler = MagicMock()
    handler.restart_for_tier.side_effect = [False, True]
    storage._camera_tier_handlers = {"camera": {"recorder": [{"segments": handler}]}}
    storage._poll_tier_markers()
    (tmp_path / ".ready").touch()
    storage._poll_tier_markers()
    handler.check_tier.assert_not_called()
    storage._poll_tier_markers()
    assert handler.restart_for_tier.call_count == 2
    handler.check_tier.assert_called_once_with(force=True)


@pytest.mark.parametrize("root,marker", [("relative", ".ready"), ("/mnt", "../x")])
def test_invalid_tier_marker_configuration(root: str, marker: str) -> None:
    """Avoid relative roots and marker names that escape their root."""
    with pytest.raises(vol.Invalid):
        validate_tier_markers({root: marker})


def test_failed_marker_check_preserves_source(tmp_path: Path) -> None:
    """Never move a file to a protected directory whose marker is missing."""
    root = tmp_path / "nas"
    root.mkdir()
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    with pytest.raises(OSError, match="marker unavailable"):
        move_file(
            MagicMock(),
            str(source),
            str(root / "dest.m4s"),
            logging.getLogger(),
            tier_markers={str(root): ".viseron-ready"},
        )
    assert source.read_bytes() == b"recording"
    assert not (root / "dest.m4s").exists()


def test_marker_disappearing_after_publish_keeps_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recheck availability before removing the last known source copy."""
    root = tmp_path / "nas"
    root.mkdir()
    marker = root / ".viseron-ready"
    marker.write_text("ready")
    source = tmp_path / "source.m4s"
    source.write_bytes(b"recording")
    destination = root / "dest.m4s"
    original_replace = os.replace

    def publish_then_disconnect(src: str, dst: str) -> None:
        original_replace(src, dst)
        marker.unlink()

    monkeypatch.setattr(os, "replace", publish_then_disconnect)
    with pytest.raises(OSError, match="marker unavailable"):
        move_file(
            MagicMock(),
            str(source),
            str(destination),
            logging.getLogger(),
            tier_markers={str(root): marker.name},
        )
    assert source.read_bytes() == b"recording"
    assert destination.read_bytes() == b"recording"


def test_cleanup_preserves_marker_and_skips_unavailable_tier(tmp_path: Path) -> None:
    """Cleanup cannot delete a marker or files on a failed tier."""
    marker = tmp_path / ".viseron-ready"
    marker.write_text("ready")
    runner = MagicMock()
    runner.paths = [str(tmp_path)]
    runner.ignored_files = frozenset()
    runner.tier_markers = {str(tmp_path): marker.name}
    runner.kill_event.is_set.return_value = False
    session = runner.get_session.return_value.__enter__.return_value
    session.execute.return_value.first.return_value = None

    OrphanedFilesCleanup.work(runner)
    assert marker.exists()
    marker.unlink()
    orphan = tmp_path / "orphan.m4s"
    orphan.write_bytes(b"recording")
    OrphanedFilesCleanup.work(runner)
    assert orphan.exists()


def test_delete_worker_refuses_to_remove_marker(tmp_path: Path) -> None:
    """The marker itself is never eligible for a tier delete."""
    marker = tmp_path / ".viseron-ready"
    marker.touch()
    with pytest.raises(OSError, match="Refusing to delete storage marker"):
        delete_file(
            MagicMock(),
            str(marker),
            logging.getLogger(),
            tier_markers={str(tmp_path): marker.name},
        )
    assert marker.exists()


def test_stale_deletion_after_remount_does_not_remove_record(tmp_path: Path) -> None:
    """An event from the old mount must not delete a file on the restored one."""
    path = tmp_path / "segment.m4s"
    path.write_bytes(b"recording")
    handler = TierHandler.__new__(TierHandler)
    handler._event_queue = queue.Queue()
    handler._event_queue.put(FileDeletedEvent(str(path)))
    handler._event_queue.put(None)
    handler._storage = MagicMock()
    handler._storage.tier_available.return_value = True
    handler._logger = MagicMock()
    handler._on_deleted = MagicMock()  # type: ignore[method-assign]
    handler._process_events()
    handler._on_deleted.assert_not_called()  # type: ignore[attr-defined]
