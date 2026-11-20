"""Test database migrations."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from pytest_alembic.tests import *  # pylint: disable=unused-wildcard-import,wildcard-import # noqa: F401, F403
from sqlalchemy import text

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy import Engine

FILE_KEY_REVISION = "0aad2bea54e5"
TIMELAPSE_ORIG_CTIME_REVISION = "3de25eba5ef4"
INSERT_TIME = datetime.datetime(2026, 1, 1)


def _file_row(
    file_id: int,
    category: str = "recorder",
    subcategory: str = "segments",
    filename: str | None = None,
) -> dict[str, object]:
    filename = filename or f"{file_id}.m4s"
    directory = f"/tier0/{category}/{subcategory}/camera"
    return {
        "id": file_id,
        "tier_id": 0,
        "tier_path": "/tier0",
        "camera_identifier": "camera",
        "category": category,
        "subcategory": subcategory,
        "path": f"{directory}/{filename}",
        "directory": directory,
        "filename": filename,
        "size": 10,
        "orig_ctime": INSERT_TIME,
    }


def test_file_key_backfill(
    alembic_runner: MigrationContext, alembic_engine: Engine
) -> None:
    """Test that existing rows get distinct keys and new rows do not reuse them."""
    alembic_runner.migrate_up_before(FILE_KEY_REVISION)
    alembic_runner.insert_into("files", [_file_row(i) for i in (1, 2, 3)])
    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as connection:
        backfilled = connection.execute(text("SELECT file_key FROM files")).scalars()
        backfilled_keys = set(backfilled)
        connection.execute(
            text(
                "INSERT INTO files (id, tier_id, tier_path, camera_identifier, "
                "category, subcategory, path, directory, filename, size, orig_ctime) "
                "VALUES (4, 0, '/tier0', 'camera', 'recorder', 'segments', "
                "'/tier0/4.m4s', '/tier0', '4.m4s', 10, now())"
            )
        )
        new_key = connection.execute(
            text("SELECT file_key FROM files WHERE id = 4")
        ).scalar_one()

    assert len(backfilled_keys) == 3
    assert None not in backfilled_keys
    assert new_key not in backfilled_keys


def test_timelapse_orig_ctime_backfill(
    alembic_runner: MigrationContext, alembic_engine: Engine
) -> None:
    """Test that timelapse frames get the timestamp from their filename."""
    alembic_runner.migrate_up_before(TIMELAPSE_ORIG_CTIME_REVISION)
    alembic_runner.insert_into(
        "files",
        [
            _file_row(1, "timelapse", "timelapse", "1723111156.jpg"),
            _file_row(2, "timelapse", "timelapse", "not_an_epoch.jpg"),
            _file_row(1723111161),
        ],
    )
    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as connection:
        rows = dict(
            connection.execute(text("SELECT id, orig_ctime FROM files")).tuples().all()
        )

    assert rows == {
        1: datetime.datetime(2024, 8, 8, 9, 59, 16),
        2: INSERT_TIME,
        1723111161: INSERT_TIME,
    }


def test_upgrade_from_upstream_retains_keys_for_both_locations(
    alembic_runner: MigrationContext, alembic_engine: Engine
) -> None:
    """Upgrading upstream preserves both publicly visible keys after deduplication."""
    alembic_runner.migrate_up_to(TIMELAPSE_ORIG_CTIME_REVISION)
    source = _file_row(1)
    destination = _file_row(2, filename="1.m4s")
    destination.update(
        tier_id=1, tier_path="/tier1", path="/tier1/segments/camera/1.m4s"
    )
    alembic_runner.insert_into(
        "files", [{**source, "file_key": 100}, {**destination, "file_key": 200}]
    )

    alembic_runner.migrate_up_to("95d084e17032")

    with alembic_engine.begin() as connection:
        assert connection.execute(text("SELECT id, file_key FROM files")).one() == (
            1,
            100,
        )
        assert connection.execute(
            text("SELECT file_key, file_id FROM file_key_aliases")
        ).one() == (200, 1)
        assert connection.execute(
            text("SELECT file_id, tier_id FROM file_locations ORDER BY tier_id")
        ).all() == [(1, 0), (1, 1)]


def test_upgrade_from_fork_preserves_logical_file_and_locations(
    alembic_runner: MigrationContext, alembic_engine: Engine
) -> None:
    """An existing fork recording keeps its logical file ID and init hash."""
    alembic_runner.migrate_up_to("6c7d2f8e9a10")
    # Reproduce the deployed fork schema rather than replaying the improved
    # deduplication migration that a fresh upstream upgrade will execute.
    with alembic_engine.begin() as connection:
        connection.execute(text("DROP INDEX uq_files_logical_key"))
        connection.execute(text("ALTER TABLE files DROP COLUMN logical_path"))
        connection.execute(
            text(
                "CREATE UNIQUE INDEX uq_files_logical_key ON files "
                "(camera_identifier, category, subcategory, filename)"
            )
        )
    file = _file_row(37)
    alembic_runner.insert_into("files", [{**file, "hls_init_hash": "a" * 64}])
    alembic_runner.insert_into(
        "file_locations",
        [
            {
                "file_id": 37,
                "tier_id": 0,
                "tier_path": file["tier_path"],
                "path": file["path"],
                "directory": file["directory"],
                "filename": file["filename"],
                "size": 10,
                "state": "available",
            }
        ],
    )

    alembic_runner.migrate_up_to("95d084e17032")

    with alembic_engine.begin() as connection:
        row = connection.execute(
            text("SELECT id, file_key, hls_init_hash FROM files")
        ).one()
        assert row.id == 37
        assert row.file_key is not None
        assert row.hls_init_hash == "a" * 64
        assert connection.execute(
            text("SELECT file_id, state, path FROM file_locations")
        ).one() == (37, "available", file["path"])


def test_upgrade_preserves_same_named_clips_from_different_dates(
    alembic_runner: MigrationContext, alembic_engine: Engine
) -> None:
    """Clips with distinct relative paths retain distinct public identities."""
    alembic_runner.migrate_up_to(TIMELAPSE_ORIG_CTIME_REVISION)
    rows = []
    for i in (1, 2):
        row = _file_row(i, "recorder", "event_clips", "clip.mp4")
        row["directory"] = f"/tier0/event_clips/camera/2026-01-0{i}"
        row["path"] = f"{row['directory']}/clip.mp4"
        rows.append({**row, "file_key": 100 + i})
    alembic_runner.insert_into("files", rows)

    alembic_runner.migrate_up_to("95d084e17032")

    with alembic_engine.begin() as connection:
        assert connection.execute(
            text("SELECT file_key FROM files ORDER BY id")
        ).scalars().all() == [101, 102]
