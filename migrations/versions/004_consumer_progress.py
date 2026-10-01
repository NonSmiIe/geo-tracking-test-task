import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None

ZONE_OCCUPANCY = """
CREATE OR REPLACE FUNCTION zone_occupancy(
    center geography, radius_m double precision, footprint geometry, since timestamptz
) RETURNS bigint LANGUAGE plpgsql STABLE PARALLEL SAFE
SET plan_cache_mode = force_custom_plan AS $$
DECLARE
    cells integer[] := grid_cells(footprint);
    inside bigint;
BEGIN
    SELECT count(*) INTO inside FROM device_latest AS device
    WHERE (cells IS NULL OR device.cell = ANY(cells))
      AND device.reported_at >= since
      AND device.position && footprint
      AND ST_DWithin(center, device.position::geography, radius_m);
    RETURN inside;
END
$$
"""


def upgrade():
    op.create_table(
        "consumer_progress",
        sa.Column("topic", sa.String(249), primary_key=True),
        sa.Column("partition", sa.Integer, primary_key=True),
        sa.Column("persisted", sa.BigInteger, nullable=False),
    )
    op.execute(ZONE_OCCUPANCY)


def downgrade():
    op.drop_table("consumer_progress")
