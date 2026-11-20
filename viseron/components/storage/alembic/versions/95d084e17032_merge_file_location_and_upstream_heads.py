"""Join the file location and upstream migration histories."""

import sqlalchemy as sa
from alembic import op

revision = "95d084e17032"
down_revision = ("6c7d2f8e9a10", "3de25eba5ef4")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the aliases table on databases without pre-existing aliases."""
    if not any(
        col["name"] == "logical_path"
        for col in sa.inspect(op.get_bind()).get_columns("files")
    ):
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
        op.drop_index("uq_files_logical_key", table_name="files")
        op.create_index(
            "uq_files_logical_key",
            "files",
            ["camera_identifier", "category", "subcategory", "logical_path"],
            unique=True,
        )
    if not sa.inspect(op.get_bind()).has_table("file_key_aliases"):
        op.create_table(
            "file_key_aliases",
            sa.Column(
                "file_key", sa.BigInteger(), primary_key=True, autoincrement=False
            ),
            sa.Column(
                "file_id",
                sa.Integer(),
                sa.ForeignKey("files.id", ondelete="CASCADE"),
                nullable=False,
            ),
        )
    op.create_index("ix_file_key_aliases_file_id", "file_key_aliases", ["file_id"])


def downgrade() -> None:
    """Remove the compatibility index."""
    op.drop_index("ix_file_key_aliases_file_id", table_name="file_key_aliases")
