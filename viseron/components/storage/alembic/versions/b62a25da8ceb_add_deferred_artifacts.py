"""Store deferred recording artifact operations."""

import sqlalchemy as sa
from alembic import op

revision = "b62a25da8ceb"
down_revision = "95d084e17032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the durable artifact retry queue."""
    op.create_table(
        "deferred_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("operation_key", sa.String(64), nullable=False, unique=True),
        sa.Column("camera_identifier", sa.String(), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=False),
        sa.Column("src", sa.String(), nullable=False),
        sa.Column("dst", sa.String()),
        sa.Column("dst_tier_id", sa.Integer()),
        sa.Column("dst_tier_path", sa.String()),
        sa.Column("retry_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_deferred_artifacts_retry_at", "deferred_artifacts", ["retry_at"]
    )


def downgrade() -> None:
    """Remove the artifact retry queue."""
    op.drop_table("deferred_artifacts")
