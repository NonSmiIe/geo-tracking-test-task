from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None

GRID_CELLS = """
CREATE OR REPLACE FUNCTION grid_cells(area geometry) RETURNS integer[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
WITH corners AS (
    SELECT grid_cell(ST_SetSRID(ST_MakePoint(ST_XMin(part.geom), ST_YMin(part.geom)), 4326)) AS low,
           grid_cell(ST_SetSRID(ST_MakePoint(ST_XMax(part.geom), ST_YMax(part.geom)), 4326)) AS high
    FROM ST_Dump(area) AS part
), spans AS (
    SELECT low / 1440 AS row_low, high / 1440 AS row_high,
           low % 1440 AS col_low, high % 1440 AS col_high
    FROM corners
)
SELECT CASE
    WHEN (SELECT sum((row_high - row_low + 1) * (col_high - col_low + 1)) FROM spans) > 4096
        THEN NULL
    ELSE ARRAY(
        SELECT DISTINCT grid_row * 1440 + grid_col
        FROM spans,
             generate_series(row_low, row_high) AS grid_row,
             generate_series(col_low, col_high) AS grid_col
    )
END
$$
"""

ZONE_OCCUPANCY = """
CREATE FUNCTION zone_occupancy(
    center geography, radius_m double precision, footprint geometry, since timestamptz
) RETURNS bigint LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
    cells integer[] := grid_cells(footprint);
    inside bigint;
BEGIN
    IF cells IS NULL THEN
        SELECT count(*) INTO inside FROM device_latest AS device
        WHERE device.reported_at >= since
          AND device.position && footprint
          AND ST_DWithin(center, device.position::geography, radius_m);
    ELSE
        SELECT count(*) INTO inside FROM device_latest AS device
        WHERE device.cell = ANY(cells)
          AND device.reported_at >= since
          AND device.position && footprint
          AND ST_DWithin(center, device.position::geography, radius_m);
    END IF;
    RETURN inside;
END
$$
"""

PREVIOUS_GRID_CELLS = """
CREATE OR REPLACE FUNCTION grid_cells(area geometry) RETURNS integer[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
WITH span AS (
    SELECT floor((ST_YMin(area) + 90) * 4)::integer AS row_low,
           least(719, floor((ST_YMax(area) + 90) * 4)::integer) AS row_high,
           floor((ST_XMin(area) + 180) * 4)::integer AS col_low,
           least(1439, floor((ST_XMax(area) + 180) * 4)::integer) AS col_high
)
SELECT CASE
    WHEN (row_high - row_low + 1) * (col_high - col_low + 1) > 4096 THEN NULL
    ELSE ARRAY(
        SELECT grid_row * 1440 + grid_col
        FROM generate_series(row_low, row_high) AS grid_row,
             generate_series(col_low, col_high) AS grid_col
    )
END
FROM span
$$
"""


def upgrade():
    op.execute(GRID_CELLS)
    op.execute(ZONE_OCCUPANCY)


def downgrade():
    op.execute("DROP FUNCTION zone_occupancy(geography, double precision, geometry, timestamptz)")
    op.execute(PREVIOUS_GRID_CELLS)
