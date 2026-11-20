"""Add physical file locations table.

Revision ID: e9f3a2c1d4b5
Revises: d4f5c8a92b13
Create Date: 2026-07-07 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str | None = "e9f3a2c1d4b5"
down_revision: str | None = "d4f5c8a92b13"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create file_locations and backfill current physical file rows."""
    op.add_column(
        "files",
        sa.Column(
            "logical_path",
            sa.String(),
            sa.Computed(
                """CASE WHEN category = 'recorder' AND subcategory = 'event_clips'
THEN substr(path, length(rtrim(tier_path, '/')) + 2)
ELSE filename END""",
                persisted=True,
            ),
            nullable=False,
        ),
    )
    op.create_table(
        "file_locations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("file_id", sa.Integer(), nullable=False),
        sa.Column("tier_id", sa.Integer(), nullable=False),
        sa.Column("tier_path", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("directory", sa.String(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["file_id"], ["files.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_file_locations_file_id", "file_locations", ["file_id"], unique=False
    )
    op.create_index("idx_file_locations_path", "file_locations", ["path"], unique=True)
    op.create_index(
        "idx_file_locations_state", "file_locations", ["state"], unique=False
    )
    op.create_index(
        "idx_file_locations_tier_lookup",
        "file_locations",
        ["tier_id", "state"],
        unique=False,
    )

    op.execute(
        """
        CREATE TEMPORARY TABLE file_logical_merge_map AS
        WITH retained AS (
            SELECT
                camera_identifier,
                category,
                subcategory,
                logical_path,
                MIN(id) AS retained_id
            FROM files
            GROUP BY camera_identifier, category, subcategory, logical_path
        )
        SELECT files.id AS old_id, retained.retained_id
        FROM files
        JOIN retained
            ON files.camera_identifier = retained.camera_identifier
            AND files.category = retained.category
            AND files.subcategory = retained.subcategory
            AND files.logical_path = retained.logical_path
        """
    )

    op.execute(
        """
        UPDATE recordings
        SET thumbnail_file_id = file_logical_merge_map.retained_id
        FROM file_logical_merge_map
        WHERE recordings.thumbnail_file_id = file_logical_merge_map.old_id
            AND recordings.thumbnail_file_id != file_logical_merge_map.retained_id
        """
    )
    op.execute(
        """
        UPDATE recordings
        SET clip_file_id = file_logical_merge_map.retained_id
        FROM file_logical_merge_map
        WHERE recordings.clip_file_id = file_logical_merge_map.old_id
            AND recordings.clip_file_id != file_logical_merge_map.retained_id
        """
    )
    op.execute(
        """
        UPDATE objects
        SET snapshot_file_id = file_logical_merge_map.retained_id
        FROM file_logical_merge_map
        WHERE objects.snapshot_file_id = file_logical_merge_map.old_id
            AND objects.snapshot_file_id != file_logical_merge_map.retained_id
        """
    )
    op.execute(
        """
        UPDATE motion
        SET snapshot_file_id = file_logical_merge_map.retained_id
        FROM file_logical_merge_map
        WHERE motion.snapshot_file_id = file_logical_merge_map.old_id
            AND motion.snapshot_file_id != file_logical_merge_map.retained_id
        """
    )
    op.execute(
        """
        UPDATE post_processor_results
        SET snapshot_file_id = file_logical_merge_map.retained_id
        FROM file_logical_merge_map
        WHERE post_processor_results.snapshot_file_id = file_logical_merge_map.old_id
            AND post_processor_results.snapshot_file_id
                != file_logical_merge_map.retained_id
        """
    )

    op.execute(
        """
        INSERT INTO file_locations (
            file_id,
            tier_id,
            tier_path,
            path,
            directory,
            filename,
            size,
            state,
            created_at,
            updated_at
        )
        SELECT
            file_logical_merge_map.retained_id,
            files.tier_id,
            files.tier_path,
            files.path,
            files.directory,
            files.filename,
            files.size,
            'available',
            files.created_at,
            files.updated_at
        FROM files
        JOIN file_logical_merge_map
            ON files.id = file_logical_merge_map.old_id
        """
    )

    # An upstream installation already has keys. Save aliases before deleting
    # the old physical rows. Older fork installations reach this step without them.
    if any(
        col["name"] == "file_key"
        for col in sa.inspect(op.get_bind()).get_columns("files")
    ):
        op.execute(
            """
            CREATE TEMPORARY TABLE migrated_file_keys ON COMMIT DROP AS
            SELECT DISTINCT ON (files.file_key)
                files.file_key, mapping.retained_id AS file_id
            FROM files
            JOIN file_logical_merge_map AS mapping ON files.id = mapping.old_id
            ORDER BY files.file_key, mapping.retained_id
            """
        )
        op.execute(
            """
            CREATE TABLE file_key_aliases (
                file_key BIGINT PRIMARY KEY,
                file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE
            )
            """
        )
        op.execute(
            """
            INSERT INTO file_key_aliases (file_key, file_id)
            SELECT keys.file_key, keys.file_id
            FROM migrated_file_keys AS keys
            JOIN files ON files.id = keys.file_id
            WHERE keys.file_key <> files.file_key
            """
        )

    op.execute(
        """
        DELETE FROM files
        USING file_logical_merge_map
        WHERE files.id = file_logical_merge_map.old_id
            AND file_logical_merge_map.old_id
                != file_logical_merge_map.retained_id
        """
    )
    op.execute("DROP TABLE file_logical_merge_map")

    op.create_index(
        "uq_files_logical_key",
        "files",
        ["camera_identifier", "category", "subcategory", "logical_path"],
        unique=True,
    )


def downgrade() -> None:
    """Drop file_locations and logical uniqueness."""
    if sa.inspect(op.get_bind()).has_table("file_key_aliases"):
        op.drop_table("file_key_aliases")
    op.drop_index("uq_files_logical_key", table_name="files")
    if any(
        col["name"] == "logical_path"
        for col in sa.inspect(op.get_bind()).get_columns("files")
    ):
        op.drop_column("files", "logical_path")
    op.drop_index("idx_file_locations_tier_lookup", table_name="file_locations")
    op.drop_index("idx_file_locations_state", table_name="file_locations")
    op.drop_index("idx_file_locations_path", table_name="file_locations")
    op.drop_index("idx_file_locations_file_id", table_name="file_locations")
    op.drop_table("file_locations")
