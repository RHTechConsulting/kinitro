"""Add unique constraint for episode identity

Revision ID: 014_episode_data_unique
Revises: 013_competition_leader_candidates
Create Date: 2024-07-01 00:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "014_episode_data_unique"
down_revision = "013_comp_leader_candidates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove duplicate episode rows before adding the constraint
    op.execute(
        "ALTER TABLE episode_step_data DROP CONSTRAINT IF EXISTS uq_episode_step;"
    )

    op.execute(
        """
        WITH duplicates AS (
            SELECT
                submission_id,
                task_id,
                episode_id,
                MIN(id) AS keep_id,
                ARRAY_AGG(id) AS all_ids
            FROM episode_data
            GROUP BY submission_id, task_id, episode_id
            HAVING COUNT(*) > 1
        )
        UPDATE episode_step_data AS es
        SET episode_id = dup.keep_id
        FROM duplicates dup
        WHERE es.episode_id = ANY(dup.all_ids)
          AND es.episode_id <> dup.keep_id;
        """
    )

    op.execute(
        """
        WITH step_dupes AS (
            SELECT
                id,
                ROW_NUMBER() OVER (PARTITION BY episode_id, step ORDER BY id) AS rn
            FROM episode_step_data
        )
        DELETE FROM episode_step_data
        WHERE id IN (
            SELECT id FROM step_dupes WHERE rn > 1
        );
        """
    )

    op.execute(
        """
        DELETE FROM episode_data ed
        USING (
            SELECT submission_id, task_id, episode_id, MIN(id) AS keep_id
            FROM episode_data
            GROUP BY submission_id, task_id, episode_id
            HAVING COUNT(*) > 1
        ) dup
        WHERE ed.submission_id = dup.submission_id
          AND ed.task_id = dup.task_id
          AND ed.episode_id = dup.episode_id
          AND ed.id <> dup.keep_id;
        """
    )

    op.execute(
        """
        ALTER TABLE episode_step_data
        ADD CONSTRAINT uq_episode_step UNIQUE (episode_id, step);
        """
    )

    op.create_unique_constraint(
        "uq_episode_data_submission_task_episode",
        "episode_data",
        ["submission_id", "task_id", "episode_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_episode_data_submission_task_episode",
        "episode_data",
        type_="unique",
    )
