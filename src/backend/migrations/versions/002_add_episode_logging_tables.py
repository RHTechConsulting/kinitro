"""Add episode logging tables

Revision ID: 002
Revises: 001
Create Date: 2025-01-08 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create episode_data table
    op.create_table(
        "episode_data",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("submission_id", sa.String(length=128), nullable=False),
        sa.Column("episode_id", sa.Integer(), nullable=False),
        sa.Column("env_name", sa.String(length=128), nullable=False),
        sa.Column("benchmark_name", sa.String(length=128), nullable=False),
        sa.Column("final_reward", sa.Float(), nullable=False),
        sa.Column("success", sa.Boolean(), nullable=False),
        sa.Column("steps", sa.Integer(), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("extra_metrics", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["backend_evaluation_jobs.id"], name="fk_episode_data_job_id"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for episode_data
    op.create_index("ix_episode_data_submission", "episode_data", ["submission_id"])
    op.create_index("ix_episode_data_episode", "episode_data", ["episode_id"])
    op.create_index("ix_episode_data_success", "episode_data", ["success"])

    # Create episode_step_data table
    op.create_table(
        "episode_step_data",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("episode_id", sa.BigInteger(), nullable=False),
        sa.Column("submission_id", sa.String(length=128), nullable=False),
        sa.Column("step", sa.Integer(), nullable=False),
        sa.Column("action", sa.JSON(), nullable=False),
        sa.Column("reward", sa.Float(), nullable=False),
        sa.Column("done", sa.Boolean(), nullable=False),
        sa.Column("truncated", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("observation_refs", sa.JSON(), nullable=False),
        sa.Column("info", sa.JSON(), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["episode_id"], ["episode_data.id"], name="fk_episode_step_data_episode_id"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("episode_id", "step", name="uq_episode_step"),
    )

    # Create indexes for episode_step_data
    op.create_index(
        "ix_episode_step_submission", "episode_step_data", ["submission_id"]
    )
    op.create_index("ix_episode_step_step", "episode_step_data", ["step"])


def downgrade() -> None:
    # Drop episode_step_data table and its indexes
    op.drop_index("ix_episode_step_step", table_name="episode_step_data")
    op.drop_index("ix_episode_step_submission", table_name="episode_step_data")
    op.drop_table("episode_step_data")

    # Drop episode_data table and its indexes
    op.drop_index("ix_episode_data_success", table_name="episode_data")
    op.drop_index("ix_episode_data_episode", table_name="episode_data")
    op.drop_index("ix_episode_data_submission", table_name="episode_data")
    op.drop_table("episode_data")
