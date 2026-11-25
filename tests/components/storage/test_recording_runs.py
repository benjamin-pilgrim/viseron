"""Recorder runs keep file identity and event ownership separate."""

from datetime import UTC, datetime, timedelta

import numpy as np

from viseron.components.storage.models import Files, FileLocations, Recordings
from viseron.components.storage.queries import get_recording_fragments


def test_same_segment_name_in_two_runs_is_distinct_and_event_scoped(get_db_session):
    """A restart/clock reset must not merge footage with the same basename."""
    now = datetime(2026, 11, 25, tzinfo=UTC)
    runs = [
        "20261125T000000.000000Z-123456789abc",
        "20261125T000001.000000Z-123456789abd",
    ]
    files = []
    with get_db_session() as session:
        for run in runs:
            directory = f"/ram/segments/run_test/{run}"
            file = Files(
                camera_identifier="run_test",
                category="recorder",
                subcategory="segments",
                tier_id=0,
                tier_path="/ram",
                path=directory + "/100.m4s",
                directory=directory,
                filename="100.m4s",
                orig_ctime=now,
                duration=1,
                size=100,
            )
            session.add(file)
            session.flush()
            session.add(
                FileLocations(
                    file_id=file.id,
                    tier_id=0,
                    tier_path="/ram",
                    path=file.path,
                    directory=directory,
                    filename=file.filename,
                    size=100,
                    state="available",
                )
            )
            files.append(file)
        event = Recordings(
            camera_identifier="run_test",
            start_time=now,
            adjusted_start_time=now,
            end_time=now + timedelta(seconds=2),
            recorder_run_id=runs[0],
        )
        session.add(event)
        session.commit()
    assert files[0].file_key != files[1].file_key
    fragments = get_recording_fragments(event.id, 0, get_db_session)
    assert [fragment.file_key for fragment in fragments] == [files[0].file_key]


def test_event_retention_does_not_own_another_run_at_the_same_time():
    """Retaining an event must not also retain unrelated footage after a clock reset."""
    from viseron.components.storage.check_tier import (  # noqa: PLC0415
        FILES_DTYPE,
        RECORDINGS_DTYPE,
        get_recordings_to_move,
    )

    run1 = "20261125T000000.000000Z-123456789abc"
    run2 = "20261125T000001.000000Z-123456789abd"
    files = np.array(
        [
            (1, 100, 100, f"/ram/segments/test/{run1}/100.m4s", "/ram"),
            (2, 100, 100, f"/ram/segments/test/{run2}/100.m4s", "/ram"),
        ],
        dtype=FILES_DTYPE,
    )
    events = np.array([(1, 100, 95, 110, 100)], dtype=RECORDINGS_DTYPE)
    selected = get_recordings_to_move(
        events,
        files,
        segment_length=5,
        max_bytes=0,
        min_age_timestamp=0,
        min_bytes=0,
        max_age_timestamp=1,
        file_min_age_timestamp=200,
        drain=False,
        recording_runs={1: run1},
    )
    assert list(selected["id"]) == [2]
