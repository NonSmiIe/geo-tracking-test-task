from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DELETE FROM consumer_progress")
    op.alter_column("consumer_progress", "topic", new_column_name="topic_id")
    op.execute("ALTER TABLE consumer_progress ALTER COLUMN topic_id TYPE varchar(32)")


def downgrade():
    op.execute("DELETE FROM consumer_progress")
    op.execute("ALTER TABLE consumer_progress ALTER COLUMN topic_id TYPE varchar(249)")
    op.alter_column("consumer_progress", "topic_id", new_column_name="topic")
