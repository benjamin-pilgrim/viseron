"""Exercise run-bound preview extraction against real fragmented media."""

import shutil
import subprocess
import sys

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


def test_offline_command_recovers_a_jpeg(fragment, tmp_path):
    output = tmp_path / "recovered.jpg"
    result = subprocess.run(
        [sys.executable, "-m", "viseron.domains.camera.thumbnail_extract", str(fragment),
         "--run-id", RUN, "--output", str(output)],
        capture_output=True, check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert output.read_bytes().startswith(b"\xff\xd8")


@pytest.mark.parametrize("occupied", ["file", "symlink"])
def test_offline_command_preserves_existing_output(fragment, tmp_path, occupied):
    output = tmp_path / "recovered.jpg"
    target = tmp_path / "original.jpg"
    target.write_bytes(b"original")
    if occupied == "symlink":
        output.symlink_to(target)
    else:
        output.write_bytes(b"original")
    result = subprocess.run(
        [sys.executable, "-m", "viseron.domains.camera.thumbnail_extract", str(fragment),
         "--run-id", RUN, "--output", str(output)],
        capture_output=True, check=False,
    )
    assert result.returncode != 0
    assert output.read_bytes() == b"original"
    assert target.read_bytes() == b"original"
    assert not list(tmp_path.glob(".viseron-thumbnail-*"))


def test_offline_command_leaves_no_output_without_run_init(fragment, tmp_path):
    (fragment.parent / "init.mp4").unlink()
    output = tmp_path / "recovered.jpg"
    result = subprocess.run(
        [sys.executable, "-m", "viseron.domains.camera.thumbnail_extract", str(fragment),
         "--run-id", RUN, "--output", str(output)],
        capture_output=True, check=False,
    )
    assert result.returncode != 0
    assert not output.exists()


def test_rejects_fifo_without_waiting(fragment):
    import os

    fragment.unlink()
    os.mkfifo(fragment)
    result = subprocess.run(
        [sys.executable, "-m", "viseron.domains.camera.thumbnail_extract", str(fragment),
         "--run-id", RUN, "--output", str(fragment.parent / "recovered.jpg")],
        capture_output=True, check=False, timeout=5,
    )
    assert result.returncode != 0
    assert not (fragment.parent / "recovered.jpg").exists()
