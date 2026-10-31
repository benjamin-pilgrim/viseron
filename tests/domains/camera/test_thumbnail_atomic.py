"""Tests for atomic thumbnail publication."""

import datetime
import errno
from unittest.mock import MagicMock, patch

import cv2
import numpy as np
import pytest

from viseron.components.storage.util import TIER_MOVE_TEMP_PREFIX
from viseron.components.webserver.api.v1.events import EventsAPIHandler
from viseron.domains.camera.const import (
    CONFIG_RECORDER,
    CONFIG_SAVE_TO_DISK,
    CONFIG_THUMBNAIL,
)
from viseron.domains.camera.recorder import THUMBNAIL_TEMP_PREFIX, _recording_file_dict
from tests.domains.camera.test_recorder import ConcreteTestRecorder


@pytest.fixture
def recorder(tmp_path):
    """Make a recorder without starting its other services."""
    item = ConcreteTestRecorder.__new__(ConcreteTestRecorder)
    item._logger = MagicMock()
    item._camera = MagicMock(thumbnails_folder=str(tmp_path))
    item._config = {CONFIG_RECORDER: {CONFIG_THUMBNAIL: {CONFIG_SAVE_TO_DISK: True}}}
    return item


def test_thumbnail_is_published_and_decodes(recorder, tmp_path):
    """The final JPEGs are real images and no private files remain."""
    image = np.zeros((16, 16, 3), dtype=np.uint8)
    _, path = recorder.create_thumbnail(42, image, [])
    assert path == str(tmp_path / "42.jpg")
    for name in ("42.jpg", "latest_thumbnail.jpg"):
        decoded = cv2.imread(str(tmp_path / name))
        assert decoded is not None and decoded.shape == image.shape
    assert not list(tmp_path.glob(f"{TIER_MOVE_TEMP_PREFIX}*"))


@pytest.mark.parametrize("failure", ["false", "empty", "exception", "replace"])
def test_thumbnail_failure_preserves_old_file_and_leaves_no_temp(
    recorder, tmp_path, failure
):
    """An encoder or publication failure never clobbers an existing JPEG."""
    path = tmp_path / "latest_thumbnail.jpg"
    path.write_bytes(b"old")
    image = np.zeros((16, 16, 3), dtype=np.uint8)
    original_imencode = cv2.imencode

    def write(extension, image_, args):
        assert extension == ".jpg"
        if failure == "false":
            return False, np.array([], dtype=np.uint8)
        if failure == "empty":
            return True, np.array([], dtype=np.uint8)
        if failure == "exception":
            raise cv2.error("bad image")
        return original_imencode(extension, image_, args)

    with (
        patch("viseron.domains.camera.recorder.cv2.imencode", side_effect=write),
        patch(
            "viseron.domains.camera.recorder.os.replace",
            side_effect=OSError("rename failed"),
        )
        if failure == "replace"
        else patch(
            "viseron.domains.camera.recorder.os.replace", wraps=__import__("os").replace
        ),
    ):
        assert not recorder._write_thumbnail_atomic(str(path), image)
    assert path.read_bytes() == b"old"
    assert not list(tmp_path.glob(f"{TIER_MOVE_TEMP_PREFIX}*"))


def test_failed_primary_thumbnail_reports_missing_path(recorder):
    """A failed primary write does not provide a bogus database path."""
    with patch.object(recorder, "_write_thumbnail_atomic", return_value=False):
        _, path = recorder.create_thumbnail(42, np.zeros((2, 2, 3), dtype=np.uint8), [])
    assert path is None


def test_missing_thumbnail_serializes_as_empty_string():
    """Keep the existing API field's string type without a bogus files/None URL."""
    now = datetime.datetime.now(datetime.timezone.utc)
    record = MagicMock(
        id=1,
        camera_identifier="test",
        start_time=now,
        end_time=None,
        trigger_type=None,
        trigger_id=None,
        thumbnail_path=None,
    )
    assert _recording_file_dict(record)["thumbnail_path"] == ""


def test_missing_thumbnail_in_timeline_event():
    """The events API also keeps missing thumbnail paths empty."""
    now = datetime.datetime.now(datetime.timezone.utc)
    event = MagicMock(
        id=1,
        camera_identifier="test",
        start_time=now,
        end_time=None,
        trigger_type=None,
        thumbnail_path=None,
        created_at=now,
    )
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = [event]
    get_session = MagicMock()
    get_session.return_value.__enter__.return_value = session
    camera = MagicMock(identifier="test")
    events = EventsAPIHandler._recording_events(
        None,
        get_session,
        camera,
        int(now.timestamp()) - 10,
        int(now.timestamp()) + 10,
        "",
    )
    assert events[0]["thumbnail_path"] == ""


def test_new_thumbnail_observes_umask_like_cv2(recorder, tmp_path):
    """The normal umask determines permissions for new images."""
    image = np.zeros((8, 8, 3), dtype=np.uint8)
    baseline = tmp_path / "baseline.jpg"
    target = tmp_path / "new.jpg"
    assert cv2.imwrite(str(baseline), image)
    assert recorder._write_thumbnail_atomic(str(target), image)
    assert target.stat().st_mode & 0o777 == baseline.stat().st_mode & 0o777


def test_existing_thumbnail_preserves_its_mode(recorder, tmp_path):
    """Replacement retains the existing regular file's permission mode."""
    target = tmp_path / "existing.jpg"
    target.write_bytes(b"old")
    target.chmod(0o640)
    if target.stat().st_mode & 0o777 != 0o640:
        pytest.skip("Filesystem does not preserve requested permission bits")
    assert recorder._write_thumbnail_atomic(
        str(target), np.zeros((8, 8, 3), dtype=np.uint8)
    )
    assert target.stat().st_mode & 0o777 == 0o640


def test_temporary_symlink_collision_is_not_followed(recorder, tmp_path):
    """A pre-existing symlink is never opened or removed as a temp file."""
    victim = tmp_path / "victim"
    victim.write_bytes(b"do not overwrite")
    collision = tmp_path / f"{THUMBNAIL_TEMP_PREFIX}collision.jpg"
    try:
        collision.symlink_to(victim)
    except OSError as error:
        if error.errno in (errno.EOPNOTSUPP, errno.ENOSYS):
            pytest.skip("Filesystem does not support symlinks")
        raise
    target = tmp_path / "target.jpg"
    with patch(
        "viseron.domains.camera.recorder.secrets.token_hex",
        side_effect=["collision", "unique"],
    ) as random_token:
        assert recorder._write_thumbnail_atomic(
            str(target), np.zeros((8, 8, 3), dtype=np.uint8)
        )
    assert random_token.call_count == 2
    assert victim.read_bytes() == b"do not overwrite"
    assert collision.is_symlink()
    assert cv2.imread(str(target)) is not None


def test_existing_target_symlink_is_replaced_not_followed(recorder, tmp_path):
    """Publishing an image does not overwrite a symlink target."""
    victim = tmp_path / "victim"
    victim.write_bytes(b"do not overwrite")
    target = tmp_path / "target.jpg"
    try:
        target.symlink_to(victim)
    except OSError as error:
        if error.errno in (errno.EOPNOTSUPP, errno.ENOSYS):
            pytest.skip("Filesystem does not support symlinks")
        raise
    assert recorder._write_thumbnail_atomic(
        str(target), np.zeros((8, 8, 3), dtype=np.uint8)
    )
    assert victim.read_bytes() == b"do not overwrite"
    assert not target.is_symlink()
    assert cv2.imread(str(target)) is not None
