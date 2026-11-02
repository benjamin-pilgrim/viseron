"""Persist pending recording-asset destinations.

Revision ID: a41e6752c801
Revises: f2d9a605b301
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "a41e6752c801"
down_revision = "f2d9a605b301"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Store at most one outstanding destination per existing file."""
    op.add_column("files", sa.Column("move_target", postgresql.JSONB(), nullable=True))
    op.create_index(
        "idx_files_move_target_path",
        "files",
        [sa.text("(move_target ->> 'path')")],
        postgresql_where=sa.text("move_target IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove pending asset destinations."""
    op.drop_index("idx_files_move_target_path", table_name="files")
    op.drop_column("files", "move_target")
