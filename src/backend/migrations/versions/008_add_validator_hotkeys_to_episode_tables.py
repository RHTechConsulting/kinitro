"""Add validator hotkeys to episode tables

Revision ID: 008_validator_hotkeys_episode
Revises: 007_add_api_key_to_validators
Create Date: 2025-01-08 00:03:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "008_validator_hotkeys_episode"
down_revision: Union[str, None] = "007_add_api_key_to_validators"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "episode_data",
        sa.Column("validator_hotkey", sa.String(length=48), nullable=True),
    )
    op.add_column(
        "episode_step_data",
        sa.Column("validator_hotkey", sa.String(length=48), nullable=True),
    )

    op.create_index(
        "ix_episode_data_validator",
        "episode_data",
        ["validator_hotkey"],
    )
    op.create_index(
        "ix_episode_step_validator",
        "episode_step_data",
        ["validator_hotkey"],
    )


def downgrade() -> None:
    op.drop_index("ix_episode_step_validator", table_name="episode_step_data")
    op.drop_index("ix_episode_data_validator", table_name="episode_data")

    op.drop_column("episode_step_data", "validator_hotkey")
    op.drop_column("episode_data", "validator_hotkey")
