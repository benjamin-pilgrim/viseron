"""Test the util module."""

import errno
import os
from collections import namedtuple
from unittest.mock import patch

import pytest

from viseron.components.storage.util import (
    is_transient_filesystem_error,
    move_file_atomic,
    path_exists,
    tier_path_available,
)

EventsFiles = namedtuple("EventsFiles", "recording_id file_id path")
ContinuousFiles = namedtuple("ContinuousFiles", "id path")


def test_is_transient_filesystem_error() -> None:
    """Test transient filesystem error detection."""
    assert is_transient_filesystem_error(OSError(errno.ETIMEDOUT, "timed out"))
    assert is_transient_filesystem_error(
        OSError(errno.ECONNABORTED, "connection aborted")
    )
    assert not is_transient_filesystem_error(FileNotFoundError())
    assert not is_transient_filesystem_error(OSError(errno.ENOSPC, "no space left"))


def test_path_exists_propagates_transient_errors() -> None:
    """Test path_exists does not treat remote filesystem errors as missing."""
    with patch("os.stat", side_effect=OSError(errno.EIO, "io error")):
        with pytest.raises(OSError):
            path_exists("/remote/file")


def test_tier_path_available_uses_stat_not_scandir() -> None:
    """Test tier_path_available avoids directory enumeration."""
    stat_result = os.stat_result((0o040755, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    with (
        patch("os.stat", return_value=stat_result) as stat_mock,
        patch("os.scandir") as scandir_mock,
    ):
        assert tier_path_available("/remote/tier")

    stat_mock.assert_called_once_with("/remote/tier")
    scandir_mock.assert_not_called()


def test_failed_replace_preserves_source_with_same_size_destination(tmp_path) -> None:
    """A stale same-size file must never authorize removal of the source."""
    src, dst = tmp_path / "source", tmp_path / "destination"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    with (
        patch("os.replace", side_effect=OSError(errno.EIO, "rename failed")),
        pytest.raises(OSError, match="rename failed"),
    ):
        move_file_atomic(str(src), str(dst))

    assert src.read_bytes() == b"new"
    assert dst.read_bytes() == b"old"
    assert set(tmp_path.iterdir()) == {src, dst}
