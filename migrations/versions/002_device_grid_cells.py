from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None

GRID_CELL = """
CREATE FUNCTION grid_cell(point geometry) RETURNS integer
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
SELECT least(719, floor((ST_Y(point) + 90) * 4)::integer) * 1440
     + least(1439, floor((ST_X(point) + 180) * 4)::integer)
$$
"""

GRID_CELLS = """
CREATE FUNCTION grid_cells(area geometry) RETURNS integer[]
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
    op.execute(GRID_CELL)
    op.execute(GRID_CELLS)
    op.drop_index("device_latest_position", table_name="device_latest")
    op.execute(
        "ALTER TABLE device_latest ADD COLUMN cell integer "
        "GENERATED ALWAYS AS (grid_cell(position)) STORED"
    )
    op.create_index("device_latest_cell", "device_latest", ["cell"])
    op.execute("ALTER TABLE device_latest SET (fillfactor = 70)")


def downgrade():
    op.execute("ALTER TABLE device_latest RESET (fillfactor)")
    op.drop_index("device_latest_cell", table_name="device_latest")
    op.drop_column("device_latest", "cell")
    op.create_index(
        "device_latest_position", "device_latest", ["position"], postgresql_using="gist"
    )
    op.execute("DROP FUNCTION grid_cells(geometry)")
    op.execute("DROP FUNCTION grid_cell(geometry)")
