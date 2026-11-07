"""Event size limits count physical files once across overlapping recordings."""

import numpy as np
import pytest

from viseron.components.storage.check_tier import (
    FILES_DTYPE,
    RECORDINGS_DTYPE,
    get_recordings_to_move,
)


@pytest.mark.parametrize("overlap", [False, True])
@pytest.mark.parametrize(
    ("maximum", "minimum", "age_cutoff", "expected"),
    [
        (35, 0, 0, set()),
        (25, 0, 0, {1}),
        (0, 35, 1000, set()),
        (0, 25, 1000, {1}),
    ],
    ids=[
        "below_size_limit",
        "above_size_limit",
        "below_size_floor",
        "above_size_floor",
    ],
)
def test_event_size_uses_unique_retained_files(
    overlap: bool,
    maximum: int,
    minimum: int,
    age_cutoff: int,
    expected: set[int],
) -> None:
    """Thirty physical bytes remain thirty regardless of event overlap."""
    run = "20261102T000000.000000Z-0123456789ab"
    files = np.array(
        [
            (index, 10, index * 10, f"/tier/segments/camera/{run}/{index}.m4s", "/tier")
            for index in (1, 2, 3)
        ],
        dtype=FILES_DTYPE,
    )
    recordings = np.array(
        [
            (1, 10, 10, 21 if overlap else 11, 10, run, False),
            (2, 20, 20, 31, 20, run, False),
        ],
        dtype=RECORDINGS_DTYPE,
    )
    authorized: set[int] = set()
    selected = get_recordings_to_move(
        recordings_data=recordings,
        files_data=files,
        segment_length=5,
        max_bytes=maximum,
        min_age_timestamp=1000,
        min_bytes=minimum,
        max_age_timestamp=age_cutoff,
        file_min_age_timestamp=1000,
        drain=False,
        retention_recording_ids=authorized,
    )
    assert {int(row["id"]) for row in selected} == expected
    assert authorized == ({1} if expected else set())
    # File 2 is shared in the overlap case; the retained newer event needs it.
    assert not any(row["id"] in (2, 3) for row in selected)
