"""Exercise run-bound preview extraction against real fragmented media."""

import shutil
import subprocess

import pytest

from viseron.domains.camera.thumbnail_extract import extract_run_thumbnail

RUN = "20261101T000000.000000Z-abcdefabcdef"
OTHER_RUN = "20261101T000001.000000Z-abcdefabcdef"


@pytest.fixture
def fragment(tmp_path):
    if not shutil.which("ffmpeg"):
        pytest.skip("ffmpeg unavailable")
    directory = tmp_path / RUN
    directory.mkdir()
    subprocess.run([
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10",
        "-t", "1", "-c:v", "libx264", "-threads", "1", "-f", "hls",
        "-hls_time", "1", "-hls_list_size", "0", "-hls_segment_type", "fmp4",
        "-hls_segment_filename", str(directory / "%05d.m4s"), str(directory / "index.m3u8"),
    ], check=True)
    return directory / "00000.m4s"


def test_extracts_preview_with_adjacent_run_init(fragment):
    jpeg = extract_run_thumbnail(str(fragment), RUN)
    assert jpeg.startswith(b"\xff\xd8") and jpeg.endswith(b"\xff\xd9")


def test_never_falls_back_to_another_run_init(fragment):
    other = fragment.parent.parent / OTHER_RUN
    other.mkdir()
    (fragment.parent / "init.mp4").rename(other / "init.mp4")
    with pytest.raises(FileNotFoundError):
        extract_run_thumbnail(str(fragment), RUN)
    with pytest.raises(ValueError, match="requested recorder run"):
        extract_run_thumbnail(str(fragment), OTHER_RUN)


def test_rejects_symlink_init(fragment, tmp_path):
    init = fragment.parent / "init.mp4"
    target = tmp_path / "outside.mp4"
    init.rename(target)
    init.symlink_to(target)
    with pytest.raises(OSError):
        extract_run_thumbnail(str(fragment), RUN)
