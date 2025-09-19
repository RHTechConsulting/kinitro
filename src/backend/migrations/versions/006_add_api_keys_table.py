"""Add API keys table for authentication

Revision ID: 006_add_api_keys_table
Revises: 005_add_min_success_rate
Create Date: 2025-01-20

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006_add_api_keys_table"
down_revision: Union[str, None] = "005_add_min_success_rate"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create api_keys table."""
    op.create_table(
        "api_keys",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("associated_hotkey", sa.String(length=48), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash"),
        sa.CheckConstraint(
            "role IN ('admin', 'validator', 'viewer')", name="ck_api_keys_valid_role"
        ),
    )

    # Create indexes
    op.create_index("ix_api_keys_active", "api_keys", ["is_active"])
    op.create_index("ix_api_keys_expires", "api_keys", ["expires_at"])
    op.create_index("ix_api_keys_key_hash", "api_keys", ["key_hash"])
    op.create_index("ix_api_keys_role", "api_keys", ["role"])
    op.create_index("ix_api_keys_associated_hotkey", "api_keys", ["associated_hotkey"])


def downgrade() -> None:
    """Drop api_keys table."""
    op.drop_index("ix_api_keys_associated_hotkey", "api_keys")
    op.drop_index("ix_api_keys_role", "api_keys")
    op.drop_index("ix_api_keys_key_hash", "api_keys")
    op.drop_index("ix_api_keys_expires", "api_keys")
    op.drop_index("ix_api_keys_active", "api_keys")
    op.drop_table("api_keys")
