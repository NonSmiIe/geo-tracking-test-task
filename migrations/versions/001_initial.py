import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geography, Geometry

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

FOOTPRINT = """
CREATE FUNCTION zone_footprint(center geography, radius_m double precision)
RETURNS geometry LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT CASE
    WHEN f.polar THEN ST_MakeEnvelope(
        -180, greatest(-90, f.lat - f.dlat), 180, least(90, f.lat + f.dlat), 4326)
    WHEN f.lon - f.dlon < -180 THEN ST_Collect(
        ST_MakeEnvelope(-180, f.lat - f.dlat, f.lon + f.dlon, f.lat + f.dlat, 4326),
        ST_MakeEnvelope(f.lon - f.dlon + 360, f.lat - f.dlat, 180, f.lat + f.dlat, 4326))
    WHEN f.lon + f.dlon > 180 THEN ST_Collect(
        ST_MakeEnvelope(f.lon - f.dlon, f.lat - f.dlat, 180, f.lat + f.dlat, 4326),
        ST_MakeEnvelope(-180, f.lat - f.dlat, f.lon + f.dlon - 360, f.lat + f.dlat, 4326))
    ELSE ST_MakeEnvelope(f.lon - f.dlon, f.lat - f.dlat, f.lon + f.dlon, f.lat + f.dlat, 4326)
END
FROM (
    SELECT g.lon, g.lat, g.dlat, g.polar,
           CASE WHEN g.polar THEN 180
                ELSE degrees(asin(least(1, sin(radians(g.dlat)) / cos(radians(g.lat))))) + 1e-7
           END AS dlon
    FROM (
        SELECT p.lon, p.lat, p.dlat, p.lat + p.dlat >= 90 OR p.lat - p.dlat <= -90 AS polar
        FROM (
            SELECT ST_X(center::geometry) AS lon, ST_Y(center::geometry) AS lat,
                   least(180, degrees(radius_m / 6335439.0 * 1.01)) + 1e-7 AS dlat
        ) p
    ) g
) f
$$
"""


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute(FOOTPRINT)
    op.create_table(
        "geozones",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("user_id", sa.String(96), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("center", Geography("POINT", srid=4326, spatial_index=False), nullable=False),
        sa.Column("radius_m", sa.Float(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "footprint",
            Geometry("GEOMETRY", srid=4326, spatial_index=False),
            sa.Computed("zone_footprint(center, radius_m)", persisted=True),
            nullable=False,
        ),
        sa.CheckConstraint("radius_m > 0 AND radius_m < 'Infinity'::float8", name="finite_radius"),
    )
    op.create_index("geozones_owner", "geozones", ["user_id"])
    op.create_index(
        "geozones_active_footprint",
        "geozones",
        ["footprint"],
        postgresql_using="gist",
        postgresql_where=sa.text("active"),
    )
    op.create_table(
        "device_latest",
        sa.Column("device_id", sa.String(96), primary_key=True),
        sa.Column("position", Geometry("POINT", srid=4326, spatial_index=False), nullable=False),
        sa.Column("reported_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "device_latest_position", "device_latest", ["position"], postgresql_using="gist"
    )
    op.create_table(
        "demo_runs",
        sa.Column("user_id", sa.String(96), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("demo_runs")
    op.drop_table("device_latest")
    op.drop_table("geozones")
    op.execute("DROP FUNCTION zone_footprint(geography, double precision)")
