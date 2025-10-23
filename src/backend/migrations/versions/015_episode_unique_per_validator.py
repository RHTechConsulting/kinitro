"""Adjust episode uniqueness to include validator

Revision ID: 015_episode_unique_per_validator
Revises: 014_episode_data_unique
Create Date: 2024-07-01 00:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "015_episode_unique_per_validator"
down_revision = "014_episode_data_unique"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_episode_data_submission_task_episode",
        "episode_data",
        type_="unique",
        if_exists=True,
    )
    op.create_unique_constraint(
        "uq_episode_data_submission_task_episode_validator",
        "episode_data",
        ["submission_id", "task_id", "episode_id", "validator_hotkey"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_episode_data_submission_task_episode_validator",
        "episode_data",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_episode_data_submission_task_episode",
        "episode_data",
        ["submission_id", "task_id", "episode_id"],
    )
