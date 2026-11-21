"""Incomplete media and unknown timing must not cause unsafe publication."""

import errno
import os
import struct
import time
from queue import Queue
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from viseron.domains.camera.fragmenter_process import FragmenterWork


def make_work(tmp_path):
    """Use the real worker with an acknowledged metadata receiver."""
    staging, archive = tmp_path / "staging", tmp_path / "archive"
    staging.mkdir()
    archive.mkdir()
    (staging / "init.mp4").write_bytes(b"init")
    work = FragmenterWork(
        "camera",
        str(staging),
        str(archive),
        None,
        None,
        Event(),
        SimpleNamespace(value=0),
    )
    work.output_queue = Queue()
    work._send_files_metadata = Mock(return_value=True)
    return work, staging, archive


@pytest.mark.parametrize(
    "fragment",
    [
        b"",
        b"short",
        struct.pack(">I4s", 1, b"moof") + b"short",
        struct.pack(">I4s", 8, b"moof"),
        struct.pack(">I4s", 9, b"moof") + b"x",
        struct.pack(">I4s", 9, b"mdat") + b"x",
        struct.pack(">I4s", 9, b"moof") + b"x" + struct.pack(">I4s", 8, b"mdat"),
        struct.pack(">I4s", 9, b"moof")
        + b"x"
        + struct.pack(">I4s", 0, b"mdat")
        + b"tiny",
        struct.pack(">I4s", 9, b"moof")
        + b"x"
        + struct.pack(">I4s", 40, b"mdat")
        + b"tiny",
    ],
    ids=[
        "empty",
        "header",
        "extended_header",
        "empty_moof",
        "missing_mdat",
        "missing_moof",
        "empty_mdat",
        "unbounded",
        "truncated_payload",
    ],
)
def test_truncated_fragment_is_not_published(tmp_path, fragment):
    """Finalized playlist timing cannot legitimize a truncated payload."""
    work, staging, archive = make_work(tmp_path)
    name = "1795291200.m4s"
    (staging / name).write_bytes(fragment)
    (staging / "index.m3u8").write_text(
        "#EXTM3U\n#EXT-X-PROGRAM-DATE-TIME:2026-11-21T20:00:00.000Z\n"
        "#EXTINF:5.0,\n" + name + "\n"
    )
    work.work_input({"cmd": "drain"})
    assert not (archive / name).exists(), "truncated media became a published segment"
    work._send_files_metadata.assert_not_called()
    assert any(
        path.read_bytes() == fragment for path in staging.rglob("*") if path.is_file()
    )


def test_missing_playlist_does_not_destroy_source(tmp_path):
    """Transiently unavailable timing leaves the only media copy intact."""
    work, staging, _archive = make_work(tmp_path)
    source = staging / "1795291200.m4s"
    source.write_bytes(b"footage whose timing is temporarily unavailable")
    work.work_input({"cmd": "drain"})
    assert source.exists(), "timing read failure destroyed the only recording copy"


def test_missing_timing_can_be_retried(tmp_path):
    """A later playlist makes retained footage publishable with its real timing."""
    work, staging, archive = make_work(tmp_path)
    name = "1795291200.m4s"
    media = struct.pack(">I4s", 9, b"moof") + b"x"
    media += struct.pack(">I4s", 9, b"mdat") + b"y"
    (staging / name).write_bytes(media)
    work.work_input({"cmd": "drain"})
    assert not (archive / name).exists()
    (staging / "index.m3u8").write_text(
        "#EXTM3U\n#EXT-X-PROGRAM-DATE-TIME:2026-11-21T20:00:00.000Z\n"
        "#EXTINF:5.0,\n" + name + "\n"
    )
    work.work_input({"cmd": "drain"})
    assert (archive / name).read_bytes() == media
    assert not (staging / name).exists()


def test_incomplete_fragment_retries_do_not_flood_warnings(
    tmp_path, monkeypatch, caplog
):
    """Persistent damaged input is retained with at most one warning per minute."""
    work, staging, _archive = make_work(tmp_path)
    name = "1795291200.m4s"
    (staging / name).write_bytes(b"truncated")
    (staging / "index.m3u8").write_text("#EXTM3U\n#EXTINF:5,\n" + name + "\n")
    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    for _attempt in range(20):
        work.work_input({"cmd": "drain"})
        now[0] += 1
    assert len(caplog.records) == 1
    now[0] = 160.0
    work.work_input({"cmd": "drain"})
    assert len(caplog.records) == 2
    assert (staging / name).read_bytes() == b"truncated"


def test_lost_staging_rename_reply_retains_package(tmp_path):
    """An ambiguous rename preserves a retryable package with original timing."""
    work, staging, archive = make_work(tmp_path)
    source = staging / "42.m4s"
    media = b"\x00\x00\x00\x09moofx\x00\x00\x00\x09mdaty"
    source.write_bytes(media)
    (staging / "index.m3u8").write_text("#EXTM3U\n#EXTINF:5,\n42.m4s\n")
    replace = os.replace

    def lose_reply(src, dst):
        replace(src, dst)
        if src == source:
            raise OSError(errno.EIO, "rename reply lost")

    with patch("os.replace", side_effect=lose_reply):
        work.work_input({"cmd": "drain"})
    assert any(p.read_bytes() == media for p in staging.rglob("*") if p.is_file())
    (staging / "index.m3u8").unlink()
    (staging / "init.mp4").write_bytes(b"replacement init")
    work.work_input({"cmd": "drain"})
    assert (archive / source.name).read_bytes() == media
    assert (archive / "init.mp4").read_bytes() == b"init"
