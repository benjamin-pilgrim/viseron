"""Exercise database-free recovery using actual FFmpeg fragments."""

import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "recover_hls.py"


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg required"
)
class RecoveryTest(unittest.TestCase):
    """Exercise recovery with fragmented video."""

    def setUp(self):
        """Create independent synthetic recording tiers."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.a = self.root / "tier1" / "20261031T143215.482Z-a7c93e12"
        self.b = self.root / "tier2" / self.a.name
        self.a.mkdir(parents=True)
        self.b.mkdir(parents=True)
        self.output = self.root / "recovered.m3u8"
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x120:rate=10",
                "-t",
                "3",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "10",
                "-sc_threshold",
                "0",
                "-f",
                "hls",
                "-hls_time",
                "1",
                "-hls_list_size",
                "0",
                "-hls_segment_type",
                "fmp4",
                "-hls_segment_filename",
                str(self.a / "%05d.m4s"),
                str(self.a / "source.m3u8"),
            ],
            check=True,
        )
        (self.a / "source.m3u8").unlink()

    def run_recovery(self, *roots):
        """Invoke the recovery utility."""
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--output",
                str(self.output),
                *map(str, roots),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_recovers_across_tiers_without_original_playlist(self):
        """Recover fragments split across tiers without a database."""
        segment = sorted(self.a.glob("*.m4s"))[1]
        segment.rename(self.b / segment.name)
        result = self.run_recovery(self.a, self.b)
        assert result.returncode == 0, result.stderr
        assert "association is assumed" in result.stderr
        playlist = self.output.read_text()
        assert playlist.count("#EXTINF:") == 3
        result = subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-protocol_whitelist",
                "file,crypto",
                "-i",
                str(self.output),
                "-map",
                "0:v:0",
                "-fps_mode:v",
                "passthrough",
                "-enc_time_base:v",
                "demux",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert not result.stderr

    def test_conflicting_init_rejected(self):
        """Reject differing copies of initialization data."""
        (self.b / "init.mp4").write_bytes(b"different")
        result = self.run_recovery(self.a, self.b)
        assert result.returncode != 0
        assert "Conflicting copies" in result.stderr
        assert not self.output.exists()

    def test_missing_init_rejected(self):
        """Reject a run with no initialization data."""
        (self.a / "init.mp4").unlink()
        result = self.run_recovery(self.a)
        assert result.returncode != 0
        assert "No init.mp4" in result.stderr
        assert not self.output.exists()

    def test_refuses_dangling_output_symlink(self):
        """Recovery never creates or overwrites a symlink target."""
        target = self.root / "unrelated.m3u8"
        self.output.symlink_to(target)
        result = self.run_recovery(self.a)
        assert result.returncode != 0
        assert "Output exists" in result.stderr
        assert not target.exists()
        assert self.output.is_symlink()

    def test_refuses_existing_output(self):
        """A previous recovery or other file remains unchanged."""
        self.output.write_text("keep me")
        result = self.run_recovery(self.a)
        assert result.returncode != 0
        assert "Output exists" in result.stderr
        assert self.output.read_text() == "keep me"

    def test_gap_and_truncated_segment_are_reported(self):
        """Report missing and truncated media while retaining good fragments."""
        segments = sorted(self.a.glob("*.m4s"))
        segments[1].unlink()
        (self.a / "99999.m4s").write_bytes(b"truncated")
        result = self.run_recovery(self.a)
        assert result.returncode != 0
        assert "UNREADABLE" in result.stderr
        assert "TIMING GAP/OVERLAP" in result.stderr
        assert self.output.read_text().count("#EXTINF:") == 2


if __name__ == "__main__":
    unittest.main()
