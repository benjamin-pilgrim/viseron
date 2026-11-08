"""Closed HLS entries must not turn incomplete MP4 data into archived footage."""

import struct
from unittest.mock import Mock

import pytest

from viseron.domains.camera.fragmenter_process import FragmenterWork


def _box(kind, payload=b"data", *, extended=False):
    if extended:
        return struct.pack(">I4sQ", 1, kind, 16 + len(payload)) + payload
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


COMPLETE = _box(b"styp") + _box(b"moof") + _box(b"mdat", b"media" * 20)


def _staged_worker(tmp_path, content):
    run = "20261108T170154.508308Z-dd2c9d86f766"
    name = "000000008-1794157355-0000005996178.m4s"
    stage = tmp_path / "stage"
    source = stage / run
    source.mkdir(parents=True)
    (source / "init.mp4").write_bytes(b"unchanged run init")
    segment = source / name
    segment.write_bytes(content)
    (source / "index.m3u8").write_text(
        "#EXTM3U\n#EXTINF:5.996178,\n"
        "#EXT-X-PROGRAM-DATE-TIME:2026-11-08T17:02:34+00:00\n" + name + "\n"
    )
    target = tmp_path / "archive"
    work = FragmenterWork("camera", str(stage), str(target), None, None, Mock())
    work.output_queue = Mock()
    return work, segment, target / run / name, run + "/" + name


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"short",
        COMPLETE[:-1],
        COMPLETE + b"partial",
        _box(b"moof"),
        _box(b"mdat"),
        _box(b"moof") + _box(b"mdat", b""),
        struct.pack(">I4s", 4, b"moof"),
        struct.pack(">I4s", 1, b"moof"),
        _box(b"moof") + struct.pack(">I4s", 0, b"mdat") + b"unbounded",
    ],
    ids=[
        "empty",
        "short-header",
        "truncated-mdat",
        "partial-tail",
        "missing-mdat",
        "missing-moof",
        "empty-mdat",
        "short-box",
        "short-extended-header",
        "unfinalized-size",
    ],
)
def test_incomplete_fragment_is_not_published_or_sent_to_indexer(
    tmp_path, content, caplog
):
    """The real publisher rejects the ENOSPC pattern before any NAS is involved."""
    work, source, destination, name = _staged_worker(tmp_path, content)
    work._handle_m4s(name)
    work._handle_m4s(name)
    assert not destination.exists()
    work.output_queue.put.assert_not_called()
    assert source.read_bytes() == content
    assert name in work._metadata_retry
    assert (
        len([r for r in caplog.records if "Incomplete staged fragment" in r.message])
        == 1
    )


@pytest.mark.parametrize(
    "content",
    [
        COMPLETE,
        _box(b"moof", extended=True) + _box(b"mdat", extended=True),
        COMPLETE + COMPLETE,
    ],
)
def test_complete_fragment_still_reaches_publication_and_indexer(tmp_path, content):
    """Finite complete boxes, including extended headers, retain normal behavior."""
    work, _source, destination, name = _staged_worker(tmp_path, content)
    work._handle_m4s(name)
    assert destination.read_bytes() == content
    work.output_queue.put.assert_called_once()


def test_late_completion_can_recover_without_losing_the_source(tmp_path):
    """A rejected source can later become complete and re-enter publication."""
    work, source, destination, name = _staged_worker(tmp_path, COMPLETE[:-1])
    work._handle_m4s(name)
    assert not destination.exists()
    source.write_bytes(COMPLETE)
    work._handle_m4s(name)
    assert destination.read_bytes() == COMPLETE
    assert name not in work._metadata_retry
    work.output_queue.put.assert_called_once()
