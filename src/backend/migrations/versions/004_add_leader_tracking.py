"""Add leader tracking and thresholds to competitions

Revision ID: 004_add_leader_tracking
Revises: 003
Create Date: 2025-09-17 07:13:25.988115

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004_add_leader_tracking"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add scoring threshold columns
    op.add_column(
        "competitions",
        sa.Column("min_avg_reward", sa.Float(), nullable=False, server_default="0.0"),
    )
    op.add_column(
        "competitions",
        sa.Column("win_margin_pct", sa.Float(), nullable=False, server_default="0.05"),
    )

    # Add leader tracking columns
    op.add_column(
        "competitions",
        sa.Column("current_leader_hotkey", sa.String(length=48), nullable=True),
    )
    op.add_column(
        "competitions", sa.Column("current_leader_reward", sa.Float(), nullable=True)
    )
    op.add_column(
        "competitions",
        sa.Column("leader_updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    # Remove leader tracking columns
    op.drop_column("competitions", "leader_updated_at")
    op.drop_column("competitions", "current_leader_reward")
    op.drop_column("competitions", "current_leader_hotkey")

    # Remove scoring threshold columns
    op.drop_column("competitions", "win_margin_pct")
    op.drop_column("competitions", "min_avg_reward")
