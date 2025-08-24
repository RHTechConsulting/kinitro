"""add validator state tables

Revision ID: add_validator_state
Revises: 06def8c6c1c0
Create Date: 2025-08-24 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "add_validator_state"
down_revision = "06def8c6c1c0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create validator_state table
    op.create_table(
        "validator_state",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("validator_hotkey", sa.String(length=48), nullable=False),
        sa.Column(
            "last_seen_block", sa.BigInteger(), server_default="0", nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "last_seen_block >= 0", name="ck_last_seen_block_non_negative"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "validator_hotkey", name="validator_state_validator_hotkey_key"
        ),
    )
    op.create_index(
        "ix_validator_state_hotkey_block",
        "validator_state",
        ["validator_hotkey", "last_seen_block"],
        unique=False,
    )
    op.create_index(
        op.f("ix_validator_state_validator_hotkey"),
        "validator_state",
        ["validator_hotkey"],
        unique=True,
    )

    # Create commitment_fingerprints table
    op.create_table(
        "commitment_fingerprints",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("miner_hotkey", sa.String(length=48), nullable=False),
        sa.Column("fingerprint", sa.String(length=512), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "miner_hotkey", name="commitment_fingerprints_miner_hotkey_key"
        ),
    )
    op.create_index(
        "ix_commitment_fingerprints_miner",
        "commitment_fingerprints",
        ["miner_hotkey"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_commitment_fingerprints_miner", table_name="commitment_fingerprints"
    )
    op.drop_table("commitment_fingerprints")

    op.drop_index(
        op.f("ix_validator_state_validator_hotkey"), table_name="validator_state"
    )
    op.drop_index("ix_validator_state_hotkey_block", table_name="validator_state")
    op.drop_table("validator_state")
