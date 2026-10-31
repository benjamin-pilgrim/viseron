"""Associate new events with one recorder run.

Revision ID: f2d9a605b301
Revises: 3de25eba5ef4
"""

from alembic import op
import sqlalchemy as sa

revision = "f2d9a605b301"
down_revision = "3de25eba5ef4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep legacy events unscoped and attach new events to their run."""
    op.add_column(
        "recordings", sa.Column("recorder_run_id", sa.String(64), nullable=True)
    )


def downgrade() -> None:
    """Remove the run association."""
    op.drop_column("recordings", "recorder_run_id")
