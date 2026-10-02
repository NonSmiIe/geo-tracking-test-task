from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DROP TABLE demo_runs")


def downgrade():
    op.execute(
        "CREATE TABLE demo_runs (user_id varchar(96) PRIMARY KEY, started_at timestamptz NOT NULL)"
    )
