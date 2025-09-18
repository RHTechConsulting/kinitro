"""Add min_success_rate to competitions

Revision ID: 005_add_min_success_rate
Revises: 004_add_leader_tracking
Create Date: 2025-01-18

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005_add_min_success_rate"
down_revision: Union[str, None] = "004_add_leader_tracking"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add min_success_rate column to competitions table."""
    op.add_column(
        "competitions",
        sa.Column("min_success_rate", sa.Float(), nullable=False, server_default="1.0"),
    )


def downgrade() -> None:
    """Remove min_success_rate column from competitions table."""
    op.drop_column("competitions", "min_success_rate")
