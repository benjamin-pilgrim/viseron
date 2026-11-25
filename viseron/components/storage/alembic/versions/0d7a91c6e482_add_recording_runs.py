"""Bind new events to their recorder run, leaving historical events unchanged."""

import sqlalchemy as sa
from alembic import op

revision = "0d7a91c6e482"
down_revision = "b62a25da8ceb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add an optional recorder run to event metadata."""
    _replace_logical_path("subcategory IN ('event_clips', 'segments')")
    op.add_column(
        "recordings", sa.Column("recorder_run_id", sa.String(64), nullable=True)
    )


def downgrade() -> None:
    """Remove event run metadata without deleting recordings."""
    duplicates = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT 1 FROM files WHERE category = 'recorder' "
                "AND subcategory = 'segments' GROUP BY camera_identifier, filename "
                "HAVING count(*) > 1 LIMIT 1"
            )
        )
        .first()
    )
    if duplicates:
        raise RuntimeError(
            "Cannot downgrade while different recorder runs reuse segment names"
        )
    op.drop_column("recordings", "recorder_run_id")
    _replace_logical_path("subcategory = 'event_clips'")


def _replace_logical_path(subcategory_condition: str) -> None:
    # Both expressions are fixed migration code, never user input.
    expression = (
        "CASE WHEN category = 'recorder' AND "
        + subcategory_condition
        + " THEN substr(path, length(rtrim(tier_path, '/')) + 2) ELSE filename END"
    )
    op.drop_index("uq_files_logical_key", table_name="files")
    op.drop_column("files", "logical_path")
    op.add_column(
        "files",
        sa.Column(
            "logical_path",
            sa.String(),
            sa.Computed(expression, persisted=True),
            nullable=False,
        ),
    )
    op.create_index(
        "uq_files_logical_key",
        "files",
        ["camera_identifier", "category", "subcategory", "logical_path"],
        unique=True,
    )
