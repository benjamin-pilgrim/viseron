"""Legacy import preserves footage identity, event links and current data."""

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from viseron.components.storage.models import Base
from sqlalchemy.exc import DBAPIError

SQL = (Path(__file__).parents[2] / "scripts/import_legacy_storage.sql").read_text()


@pytest.fixture
def merge_session(db_session):
    """Exercise the unmodified operator script in a genuinely separate database."""
    engine = db_session.get_bind()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as control:
        control.exec_driver_sql("CREATE DATABASE legacy_test_merge_copy")
        copy_engine = create_engine(engine.url.set(database="legacy_test_merge_copy"))
        try:
            Base.metadata.create_all(copy_engine)
            with Session(copy_engine) as session:
                yield session
        finally:
            copy_engine.dispose()
            control.exec_driver_sql("DROP DATABASE legacy_test_merge_copy WITH (FORCE)")


def prepare(connection):
    """Represent the frozen old schema with local read-only source equivalents."""
    connection.exec_driver_sql("CREATE SCHEMA legacy_source")
    for table in (
        "files",
        "recordings",
        "motion",
        "motion_contours",
        "objects",
        "post_processor_results",
        "events",
    ):
        connection.exec_driver_sql(
            f"CREATE TABLE legacy_source.{table} "  # noqa: S608
            f"AS SELECT * FROM public.{table} WITH NO DATA"
        )
    connection.exec_driver_sql(
        "ALTER TABLE legacy_source.files DROP COLUMN logical_path, DROP "
        "COLUMN hls_init_hash, ADD COLUMN move_target jsonb"
    )
    for table, column in (
        ("recordings", "thumbnail_file_id"),
        ("recordings", "clip_file_id"),
        ("motion", "snapshot_file_id"),
        ("objects", "snapshot_file_id"),
        ("post_processor_results", "snapshot_file_id"),
    ):
        connection.exec_driver_sql(
            f"ALTER TABLE legacy_source.{table} DROP COLUMN {column}"
        )
    connection.exec_driver_sql(
        "CREATE TABLE legacy_source.pending_segment_moves(id integer)"
    )
    connection.exec_driver_sql(
        "CREATE TABLE legacy_source.media_audit(path text PRIMARY KEY,size "
        "bigint,hls_init_hash text)"
    )
    connection.exec_driver_sql(
        "CREATE TABLE public.alembic_version(version_num text); INSERT INTO "
        "public.alembic_version VALUES ('0d7a91c6e482')"
    )


def add_file(connection, schema, file_id, key, path, size=10):
    """Insert a file through the original supported storage fields."""
    filename = path.rsplit("/", 1)[-1]
    subcategory = "thumbnails" if filename.endswith(".jpg") else "segments"
    connection.execute(
        text(f"""INSERT INTO {schema}.files
        (id,file_key,tier_id,tier_path,camera_identifier,category,subcategory,
         path,directory,filename,size,orig_ctime) VALUES
        (:id,:key,1,'/','camera','recorder',:subcategory,:path,:directory,:filename,:size,now())"""),  # noqa: S608
        {
            "id": file_id,
            "key": key,
            "subcategory": subcategory,
            "path": path,
            "directory": path.rsplit("/", 1)[0],
            "filename": filename,
            "size": size,
        },
    )


def run_import(connection):
    """Run the operator script on a disposable fixture database."""
    connection.execution_options(no_parameters=True).exec_driver_sql(SQL)


def test_import_retains_current_files_links_events_and_marks_missing(merge_session):
    """Colliding file keys must not redirect playback or break event thumbnails."""
    with merge_session as session:
        connection = session.connection()
        prepare(connection)
        add_file(connection, "public", 1, 1, "/segments/camera/new/one.m4s")
        add_file(connection, "legacy_source", 1, 1, "/segments/camera/old/one.m4s")
        add_file(connection, "legacy_source", 2, 2, "/thumbnails/camera/7.jpg")
        add_file(connection, "legacy_source", 3, 3, "/segments/camera/old/missing.m4s")
        add_file(
            connection, "legacy_source", 4, 4, "/segments/camera/old/truncated.m4s"
        )
        add_file(connection, "legacy_source", 5, 5, "/segments/camera/no-init/one.m4s")
        connection.exec_driver_sql(
            "INSERT INTO legacy_source.media_audit VALUES "
            "('/segments/camera/old/one.m4s',10,'abc'),('/thumbnails/camera/7.jpg',10,NULL),('/segments/camera/old/truncated.m4s',2,'abc'),"
            "('/snapshots/motion_detector/camera/eight.jpg',10,NULL),"
            "('/segments/camera/no-init/one.m4s',10,NULL)"
        )
        connection.exec_driver_sql(
            "INSERT INTO legacy_source.motion(id,camera_identifier,"
            "start_time,snapshot_path) "
            "VALUES(8,'camera',now(),'/snapshots/motion_detector/camera/eight.jpg'); "
            "INSERT INTO "
            "legacy_source.motion_contours(id,motion_id,contour) "
            "VALUES(9,8,'\\x01'); INSERT INTO "
            "legacy_source.recordings(id,camera_identifier,start_time,end_time,"
            "adjusted_start_time,trigger_type,trigger_id,"
            "thumbnail_path,recorder_run_id) "
            "VALUES(7,'camera',now(),now(),now(),'MOTION',8,'/tmp/tier1/thumbnails/camera/7.jpg','old')"
        )
        connection.exec_driver_sql("SELECT setval('files_file_key_seq',10000,true)")
        connection.exec_driver_sql("SELECT setval('files_id_seq',10000,true)")
        run_import(connection)
        result = connection.execute(
            text(
                "SELECT r.id,r.trigger_id,r.recorder_run_id,f.path FROM recordings r "
                "JOIN files f ON f.id=r.thumbnail_file_id"
            )
        ).one()
        assert tuple(result) == (7, 8, "old", "/thumbnails/camera/7.jpg")
        assert (
            connection.execute(
                text("SELECT motion_id FROM motion_contours")
            ).scalar_one()
            == 8
        )
        assert (
            connection.execute(
                text("SELECT path FROM files WHERE file_key=1")
            ).scalar_one()
            == "/segments/camera/new/one.m4s"
        )
        states = dict(
            connection.execute(text("SELECT path,state FROM file_locations")).all()
        )
        assert states == {
            "/segments/camera/old/one.m4s": "available",
            "/thumbnails/camera/7.jpg": "available",
            "/segments/camera/old/missing.m4s": "unavailable",
            "/segments/camera/old/truncated.m4s": "unavailable",
            "/segments/camera/no-init/one.m4s": "unavailable",
            "/snapshots/motion_detector/camera/eight.jpg": "available",
        }
        assert (
            connection.execute(
                text("SELECT count(*) FROM legacy_source.files")
            ).scalar_one()
            == 5
        )
        assert (
            connection.execute(
                text(
                    "SELECT f.path FROM motion m JOIN files f "
                    "ON f.id=m.snapshot_file_id"
                )
            ).scalar_one()
            == "/snapshots/motion_detector/camera/eight.jpg"
        )
        keys = (
            connection.execute(text("SELECT file_key FROM files ORDER BY file_key"))
            .scalars()
            .all()
        )
        assert len(set(keys)) == 7
        run_import(connection)
        assert connection.execute(text("SELECT count(*) FROM files")).scalar_one() == 7
        assert (
            connection.execute(text("SELECT count(*) FROM recordings")).scalar_one()
            == 1
        )
        assert connection.execute(
            text("SELECT nextval('files_file_key_seq')")
        ).scalar_one() > max(10000, *keys)
        assert (
            connection.execute(text("SELECT nextval('files_id_seq')")).scalar_one()
            > 10000
        )
        # Tests own their disposable schema; the importer never removes sources.
        connection.exec_driver_sql(
            "DROP SCHEMA legacy_source CASCADE; DROP SCHEMA legacy_import "
            "CASCADE; DROP TABLE alembic_version"
        )
        session.commit()


def test_import_rejects_non_copy_database(db_session):
    """The operator cannot accidentally point the import at the live database."""
    with db_session as session:
        with pytest.raises(DBAPIError, match="disposable database"):
            session.connection().execution_options(no_parameters=True).exec_driver_sql(
                SQL
            )
        session.rollback()


def test_import_refuses_detection_collisions_atomically(merge_session):
    """A conflicting detection must not attach an old recording to a new event."""
    with merge_session as session:
        connection = session.connection()
        prepare(connection)
        add_file(connection, "legacy_source", 1, 1, "/segments/camera/old/one.m4s")
        connection.exec_driver_sql(
            "INSERT INTO motion(id,camera_identifier,start_time) "
            "VALUES(8,'camera',now()); INSERT INTO "
            "legacy_source.motion(id,camera_identifier,start_time) "
            "VALUES(8,'camera',now())"
        )
        session.commit()
        with pytest.raises(DBAPIError, match="ID collision in motion"):
            run_import(session.connection())
        session.rollback()
        assert session.execute(text("SELECT count(*) FROM files")).scalar_one() == 0
        assert session.execute(text("SELECT count(*) FROM motion")).scalar_one() == 1
        assert (
            session.execute(
                text("SELECT count(*) FROM legacy_source.files")
            ).scalar_one()
            == 1
        )
        session.execute(
            text("DROP SCHEMA legacy_source CASCADE; DROP TABLE alembic_version")
        )
        session.commit()
