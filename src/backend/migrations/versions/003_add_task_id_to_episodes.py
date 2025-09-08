"""Add task_id to episode tables

Revision ID: 003
Revises: 002
Create Date: 2025-01-08 00:01:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add task_id column to episode_data table
    op.add_column(
        "episode_data",
        sa.Column("task_id", sa.String(length=128), nullable=False, server_default=""),
    )

    # Add task_id column to episode_step_data table
    op.add_column(
        "episode_step_data",
        sa.Column("task_id", sa.String(length=128), nullable=False, server_default=""),
    )

    # Create indexes for task_id
    op.create_index("ix_episode_data_task", "episode_data", ["task_id"])
    op.create_index(
        "ix_episode_data_submission_task", "episode_data", ["submission_id", "task_id"]
    )
    op.create_index("ix_episode_step_task", "episode_step_data", ["task_id"])


def downgrade() -> None:
    # Drop indexes
    op.drop_index("ix_episode_step_task", table_name="episode_step_data")
    op.drop_index("ix_episode_data_submission_task", table_name="episode_data")
    op.drop_index("ix_episode_data_task", table_name="episode_data")

    # Drop columns
    op.drop_column("episode_step_data", "task_id")
    op.drop_column("episode_data", "task_id")
