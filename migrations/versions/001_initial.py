import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geography

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.create_table(
        "geozones",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("user_id", sa.String(96), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("center", Geography("POINT", srid=4326, spatial_index=False), nullable=False),
        sa.Column("radius_m", sa.Float(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.CheckConstraint("radius_m > 0 AND radius_m < 'Infinity'::float8", name="finite_radius"),
    )
    op.create_index("geozones_owner", "geozones", ["user_id"])
    op.create_index(
        "geozones_active_center",
        "geozones",
        ["center"],
        postgresql_using="gist",
        postgresql_where=sa.text("active"),
    )
    op.create_table(
        "device_latest",
        sa.Column("device_id", sa.String(96), primary_key=True),
        sa.Column("position", Geography("POINT", srid=4326, spatial_index=False), nullable=False),
        sa.Column("reported_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("device_latest")
    op.drop_table("geozones")
