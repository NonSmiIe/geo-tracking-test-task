import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.add_column(
        "geozones",
        sa.Column(
            "radius_bucket",
            sa.SmallInteger(),
            sa.Computed(
                "least(25, greatest(0, floor(ln(radius_m)/ln(2.0))::int + 1))", persisted=True
            ),
            nullable=False,
        ),
    )
    op.drop_index("geozones_active_center", table_name="geozones")
    op.create_index(
        "geozones_active_spatial",
        "geozones",
        ["radius_bucket", "center"],
        postgresql_using="gist",
        postgresql_where=sa.text("active"),
    )
    op.create_index(
        "geozones_active_bucket", "geozones", ["radius_bucket"], postgresql_where=sa.text("active")
    )


def downgrade():
    op.drop_index("geozones_active_bucket", table_name="geozones")
    op.drop_index("geozones_active_spatial", table_name="geozones")
    op.drop_column("geozones", "radius_bucket")
    op.create_index(
        "geozones_active_center",
        "geozones",
        ["center"],
        postgresql_using="gist",
        postgresql_where=sa.text("active"),
    )
