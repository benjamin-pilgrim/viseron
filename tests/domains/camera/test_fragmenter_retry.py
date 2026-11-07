"""Recovery retries must preserve footage without starving current segments."""

from __future__ import annotations

import logging

import pytest
from collections import Counter
from unittest.mock import MagicMock

from viseron.domains.camera import fragmenter_process
from viseron.domains.camera.fragmenter_process import FragmenterWork


def test_unknown_duration_retries_are_bounded_and_keep_new_segments_moving(
    tmp_path, monkeypatch, caplog
) -> None:
    """The real fragment work loop backs off a permanently uncertain fragment."""
    old = "1723111156.m4s"
    fresh = "1723111161.m4s"
    for name in (old, fresh, "init.mp4"):
        (tmp_path / name).write_bytes(b"preserved media")
    work = FragmenterWork(
        "camera", str(tmp_path), str(tmp_path / "target"), None, None, MagicMock()
    )
    clock = [0.0]
    reads = Counter()
    staged = [old, fresh]

    def playlist(name):
        reads[name] += 1
        return "#EXTM3U\n" if name == old else f"#EXTINF:5.0,\n{name}\n"

    monkeypatch.setattr(fragmenter_process.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        fragmenter_process, "_get_mp4_files_to_fragment", lambda _path: staged
    )
    monkeypatch.setattr(work, "_read_m3u8", playlist)
    monkeypatch.setattr(work, "_send_files_metadata", MagicMock())
    monkeypatch.setattr(work, "_move_to_segments_folder", MagicMock())
    for tick in range(200):
        clock[0] = float(tick)
        work.work_input({"cmd": "fragment"})
    assert reads[old] == 7
    assert reads[fresh] == 200
    warnings = [
        record for record in caplog.records if record.levelno >= logging.WARNING
    ]
    assert len(warnings) <= 4
    assert (tmp_path / old).read_bytes() == b"preserved media"
    assert (tmp_path / "init.mp4").read_bytes() == b"preserved media"
    # Removed/recovered files do not leave retry state for the camera's lifetime.
    staged.clear()
    work.work_input({"cmd": "fragment"})
    assert not work._metadata_retry


@pytest.mark.parametrize("command,tick", [("fragment", 5.0), ("drain", 1.0)])
def test_duration_recovery_and_shutdown_drain_bypass_retry_delay(
    tmp_path, monkeypatch, command, tick
) -> None:
    """Late metadata can recover the same source, including on final drain."""
    name = "1723111156.m4s"
    (tmp_path / name).write_bytes(b"preserved media")
    work = FragmenterWork(
        "camera", str(tmp_path), str(tmp_path / "target"), None, None, MagicMock()
    )
    clock = [0.0]
    playlist = ["#EXTM3U\n"]
    metadata = MagicMock()
    move = MagicMock()
    monkeypatch.setattr(fragmenter_process.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        fragmenter_process, "_get_mp4_files_to_fragment", lambda _path: [name]
    )
    monkeypatch.setattr(work, "_read_m3u8", lambda _name: playlist[0])
    monkeypatch.setattr(work, "_send_files_metadata", metadata)
    monkeypatch.setattr(work, "_move_to_segments_folder", move)
    work.work_input({"cmd": "fragment"})
    playlist[0] = f"#EXTINF:5.0,\n{name}\n"
    clock[0] = 1.0
    work.work_input({"cmd": "fragment"})
    metadata.assert_not_called()
    clock[0] = tick
    result = work.work_input({"cmd": command})
    assert result == ({"drained": True} if command == "drain" else None)
    metadata.assert_called_once()
    move.assert_called_once()
    assert not work._metadata_retry
    assert (tmp_path / name).exists()
