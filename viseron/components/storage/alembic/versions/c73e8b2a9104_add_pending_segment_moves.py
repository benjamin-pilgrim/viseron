"""Index unfinished segment promotions on local PostgreSQL.

Revision ID: c73e8b2a9104
Revises: a41e6752c801
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "c73e8b2a9104"
down_revision = "a41e6752c801"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create a retry queue which outlives source-file database rows."""
    op.create_table(
        "pending_segment_moves",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(), nullable=False, unique=True),
        sa.Column("destination", sa.String(), nullable=False, unique=True),
        sa.Column("camera_identifier", sa.String(), nullable=False),
        sa.Column("source_root", sa.String(), nullable=False),
        sa.Column("destination_root", sa.String(), nullable=False),
        sa.Column("destination_tier_id", sa.Integer(), nullable=False),
        sa.Column("destination_tier_path", sa.String(), nullable=False),
        sa.Column("metadata_record", postgresql.JSONB(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
    )
    op.create_index(
        "idx_segment_moves_source",
        "pending_segment_moves",
        ["camera_identifier", "source_root", "id"],
    )
    op.create_index(
        "idx_segment_moves_destination",
        "pending_segment_moves",
        ["camera_identifier", "destination_root", "id"],
    )


def downgrade() -> None:
    """Keep queued work from being silently discarded on downgrade."""
    bind = op.get_bind()
    if bind.execute(sa.text("SELECT 1 FROM pending_segment_moves LIMIT 1")).first():
        raise RuntimeError("Complete pending segment moves before downgrading")
    op.drop_table("pending_segment_moves")
