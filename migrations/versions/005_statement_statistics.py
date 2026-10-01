from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


def downgrade():
    op.execute("DROP EXTENSION pg_stat_statements")
