"""Large retention catalogues must fit in memory and preserve complete paths."""

import os
import subprocess
import sys
import textwrap

import numpy as np
import pytest

from viseron.components.storage.check_tier import (
    FILES_DTYPE,
    RECORDINGS_DTYPE,
    get_recordings_to_move,
)


@pytest.mark.skipif(
    sys.platform != "linux", reason="Uses Linux address-space accounting"
)
def test_large_event_retention_with_limited_memory():
    """An unassociated 50,000-file catalogue must remain usable within 256 MiB."""
    script = textwrap.dedent(
        """
        import os
        import resource
        import numpy as np
        from viseron.components.storage.check_tier import (
            FILES_DTYPE, RECORDINGS_DTYPE, get_recordings_to_move,
        )
        with open('/proc/self/statm', encoding='ascii') as stream:
            baseline = int(stream.read().split()[0]) * os.sysconf('SC_PAGE_SIZE')
        ceiling = baseline + 256 * 1024**2
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
        count = 50_000
        files = np.array([
            (i, 1, i, f'/recordings/camera/run/{i:09d}.m4s', '/recordings')
            for i in range(1, count + 1)
        ], dtype=FILES_DTYPE)
        result = get_recordings_to_move(
            np.empty(0, dtype=RECORDINGS_DTYPE), files, 5,
            max_bytes=0, min_age_timestamp=count + 100,
            min_bytes=0, max_age_timestamp=0,
            file_min_age_timestamp=count + 100, drain=False,
        )
        assert list(result['id']) == list(range(1, count + 1))
        assert result['path'][-1] == '/recordings/camera/run/000050000.m4s'
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "OPENBLAS_NUM_THREADS": "1"},
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_retention_preserves_valid_paths_longer_than_512_characters():
    """A deep tier directory must not truncate or drop a selected media path."""
    path = "/recordings/" + "/".join(["a" * 200, "b" * 200, "c" * 100])
    path += "/segment.m4s"
    files = np.array([(1, 1, 1, path, "/recordings")], dtype=FILES_DTYPE)
    result = get_recordings_to_move(
        np.empty(0, dtype=RECORDINGS_DTYPE),
        files,
        5,
        max_bytes=0,
        min_age_timestamp=100,
        min_bytes=0,
        max_age_timestamp=0,
        file_min_age_timestamp=100,
        drain=False,
    )
    assert list(result["path"]) == [path]
