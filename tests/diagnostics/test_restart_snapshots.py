"""Replay recorder transitions against the bounded diagnostic snapshot seam."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "restart_snapshots",
    Path(__file__).resolve().parents[2]
    / ".scratch/recording-timestamp-jitter/diagnostics/restart_snapshots.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
RUN_A = "20240101T000000.000000Z-0123456789ab"
RUN_B = "20240101T000001.000000Z-abcdef012345"


def row(run, when, camera="camera"):
    """Build a timing sample containing extra data that must never be preserved."""
    return {
        "event": "media_sample",
        "camera": camera,
        "run": run,
        "observed_at": when,
        "state": "waiting_for_closed_segments",
        "secret": "CANARY",
    }


class Timer:
    """Hold delayed callbacks until the test explicitly fires them."""

    def __init__(self, seconds, function, args):
        """Initialize the pending callback."""
        self.function, self.args, self.alive = function, args, False

    def start(self):
        """Mark this timer as pending."""
        self.alive = True

    def cancel(self):
        """Cancel the pending callback."""
        self.alive = False

    def is_alive(self):
        """Report whether the callback remains pending."""
        return self.alive

    def fire(self):
        """Execute the pending callback once."""
        self.function(*self.args)
        self.alive = False


class RestartTests(unittest.TestCase):
    """Exercise the real local log follower and bounded snapshot writer."""

    def test_initial_baseline_partial_write_rotation_and_stale_records(self):
        """Keep a baseline across partial writes, rotation and stale records."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "timing.jsonl"
            path.write_text(json.dumps(row(RUN_A, 1)) + "\n")
            follower = module.RecordingRunFollower(path)
            assert follower.poll() == []
            data = json.dumps(row(RUN_B, 2))
            with path.open("a") as output:
                output.write(data[:20])
            assert follower.poll() == []
            with path.open("a") as output:
                output.write(data[20:] + "\n")
            (change,) = follower.poll()
            assert change["previous_run"] == RUN_A
            assert change["run"] == RUN_B
            assert "CANARY" not in json.dumps(change)
            path.rename(path.with_suffix(".old"))
            path.write_text(
                json.dumps(row(RUN_A, 1)) + "\n" + json.dumps(row(RUN_A, 3)) + "\n"
            )
            assert len(follower.poll()) == 1
            assert follower.poll() == []

    def test_missing_log_and_existing_history_do_not_trigger_restarts(self):
        """Avoid treating startup or a missing timing file as a camera restart."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "timing.jsonl"
            follower = module.RecordingRunFollower(path)
            assert follower.poll() == []
            path.write_text(
                json.dumps(row(RUN_A, 1)) + "\n" + json.dumps(row(RUN_B, 2)) + "\n[]\n"
            )
            assert follower.poll() == []
            assert follower.poll() == []

    def test_real_follower_triggers_before_and_after_snapshots(self):
        """Preserve both sides of a detected run transition without extra fields."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "timing.jsonl"
            path.write_text(json.dumps(row(RUN_A, 1)) + "\n")
            follower = module.RecordingRunFollower(path)
            follower.poll()
            writes = []
            messages = []
            snapshots = module.RestartSnapshots(
                root / "restart-incidents",
                writes.append,
                messages.append,
                timer=Timer,
                clock=lambda: 100,
            )
            with path.open("a") as output:
                output.write(json.dumps(row(RUN_B, 2)) + "\n")
            (transition,) = follower.poll()
            assert snapshots.trigger(transition)
            snapshots._timers[-1].fire()
            assert [p.name for p in writes] == ["initial", "post_restart"]
            saved = next((root / "restart-incidents").glob("*/incident.json"))
            assert "CANARY" not in saved.read_text()
            assert saved.stat().st_mode & 0o777 == 0o600
            snapshots.close()

    def test_cooldown_retention_restart_and_evicted_followup(self):
        """Bound history without resurrecting an evicted incident."""
        with tempfile.TemporaryDirectory() as directory:
            now = [100.0]
            saved = []
            messages = []
            snapshots = module.RestartSnapshots(
                directory,
                saved.append,
                messages.append,
                timer=Timer,
                clock=lambda: now[0],
            )
            event = {
                "event": "recorder_run_changed",
                "camera": "camera",
                "previous_run": RUN_A,
                "run": RUN_B,
                "observed_at": 1,
            }
            assert snapshots.trigger(event)
            old_timer = snapshots._timers[0]
            assert not snapshots.trigger(event)
            restored = module.RestartSnapshots(
                directory,
                saved.append,
                messages.append,
                timer=Timer,
                clock=lambda: now[0],
            )
            assert not restored.trigger(event)
            restored.close()
            for _ in range(3):
                now[0] += 601
                assert snapshots.trigger(event)
            assert len(snapshots._directories()) == 3
            before = len(saved)
            old_timer.fire()
            assert len(saved) == before
            assert len(snapshots._directories()) == 3
            snapshots.close()

    def test_copy_failure_does_not_stop_observer(self):
        """Report copy failures without exposing paths or stopping capture."""
        with tempfile.TemporaryDirectory() as directory:
            messages = []

            def fail(_destination):
                """Simulate an unavailable destination."""
                raise OSError("CANARY_PRIVATE_PATH")

            snapshots = module.RestartSnapshots(
                directory, fail, messages.append, timer=Timer, clock=lambda: 100
            )
            snapshots.trigger(
                {
                    "event": "recorder_run_changed",
                    "camera": "camera",
                    "previous_run": RUN_A,
                    "run": RUN_B,
                    "observed_at": 1,
                }
            )
            assert messages[0]["event"] == "restart_snapshot_failed"
            assert "CANARY" not in json.dumps(messages)
            snapshots.close()


if __name__ == "__main__":
    unittest.main()
