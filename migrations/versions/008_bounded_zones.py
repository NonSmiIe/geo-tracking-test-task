from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE geozones DROP CONSTRAINT finite_radius")
    op.execute(
        "ALTER TABLE geozones ADD CONSTRAINT bounded_radius "
        "CHECK (radius_m > 0 AND radius_m <= 500000)"
    )


def downgrade():
    op.execute("ALTER TABLE geozones DROP CONSTRAINT bounded_radius")
    op.execute(
        "ALTER TABLE geozones ADD CONSTRAINT finite_radius "
        "CHECK (radius_m > 0 AND radius_m < 'Infinity'::float8)"
    )
