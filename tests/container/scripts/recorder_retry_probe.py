"""Observe recorder restarts and shutdown through real child-process side effects."""

import logging
from itertools import pairwise
import pathlib
import sys
import tempfile
import time

from apscheduler.schedulers.background import BackgroundScheduler

from viseron.components.ffmpeg.pipe import FFmpegPipe
from viseron.watchdog.subprocess_watchdog import RestartablePopen, SubprocessWatchDog


def main() -> None:
    """Recover promptly, bound failed-start retries, and honour recorder shutdown."""
    with tempfile.TemporaryDirectory(prefix="recorder-retries-") as folder:
        launches = pathlib.Path(folder) / "launches"
        command = [
            sys.executable,
            "-c",
            (
                "import pathlib,sys,time; "
                "p=pathlib.Path(sys.argv[1]); "
                "p.open('a').write(str(time.monotonic())+'\\n')"
            ),
            str(launches),
        ]
        scheduler = BackgroundScheduler()
        SubprocessWatchDog(scheduler)
        pipe = FFmpegPipe(
            "retry_test",
            logging.getLogger(__name__),
            logging.ERROR,
            segment_command=command,
        )
        scheduler.start()
        other_launches = pathlib.Path(folder) / "ordinary-worker"
        ordinary = RestartablePopen([*command[:-1], str(other_launches)])
        started = time.monotonic()
        try:
            pipe.start()
            deadline = started + 4
            while time.monotonic() < deadline:
                times = (
                    [float(line) for line in launches.read_text().splitlines()]
                    if launches.exists()
                    else []
                )
                if len(times) >= 2:
                    break
                time.sleep(0.05)
            assert len(times) >= 2, (
                "exited recorder was not restarted within four seconds"
            )
            first_retry = times[1] - times[0]
            deadline = started + 8
            while time.monotonic() < deadline:
                times = [float(line) for line in launches.read_text().splitlines()]
                if len(times) >= 3:
                    break
                time.sleep(0.05)
            assert len(times) >= 3, "repeated failed starts were not retried"
            intervals = [b - a for a, b in pairwise(times)]
            assert all(interval >= 1.8 for interval in intervals), intervals
            assert len(other_launches.read_text().splitlines()) == 1, (
                "ordinary worker lost its startup grace"
            )
            pipe.close()
            count = len(launches.read_text().splitlines())
            time.sleep(3)
            assert len(launches.read_text().splitlines()) == count, (
                "shutdown recorder was restarted"
            )
            print(  # noqa: T201
                f"PASS recorder recovery={first_retry:.3f}s "
                f"retry_intervals={intervals} shutdown=stopped"
            )  # noqa: T201
        finally:
            ordinary.terminate()
            ordinary.communicate(timeout=2)
            pipe.close()
            scheduler.shutdown()


if __name__ == "__main__":
    main()
