import random

import pytest
from geoalchemy2 import WKTElement
from sqlalchemy import text

from geo_tracking.db import Database
from geo_tracking.models import Zone
from geo_tracking.settings import Settings
from geo_tracking.spatial import match_records


def zone(latitude: float, longitude: float, radius: float, **changes: object) -> Zone:
    values = {"user_id": "alice", "name": "zone", "active": True, "version": 1}
    return Zone(
        center=WKTElement(f"POINT({longitude} {latitude})", srid=4326),
        radius_m=radius,
        **(values | changes),
    )


@pytest.mark.parametrize("latitude,longitude", [(0, 0), (80, 25), (0, 179.9999), (-89.9, 10)])
async def test_metres_and_boundary_with_projected_ground_truth(
    settings: Settings, latitude: float, longitude: float
) -> None:
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            session.add(zone(latitude, longitude, 100.000001))
            await session.flush()
            points = (
                await session.execute(
                    text("""
                SELECT ST_Y(point::geometry) AS latitude, ST_X(point::geometry) AS longitude
                FROM (SELECT ST_Project(ST_MakePoint(:longitude, :latitude)::geography,
                                        distance, pi()/2) AS point
                      FROM unnest(ARRAY[99.0,100.0,101.0]) AS distance) projected
            """),
                    {"latitude": latitude, "longitude": longitude},
                )
            ).all()
            records = [(f"d{i}", lat, lon, 0) for i, (lat, lon) in enumerate(points)]
            matches = await match_records(session, records)
            assert {row["report_index"] for row in matches} == {0, 1}
    finally:
        await db.close()


async def test_variable_radius_overlap_paused_zones_and_coordinate_order(
    settings: Settings,
) -> None:
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            session.add(zone(56.9496, 24.1052, 100, user_id="alice"))
            session.add(zone(56.9496, 24.1052, 1000, user_id="bob"))
            session.add(zone(56.9496, 24.1052, 100000, user_id="paused", active=False))
            await session.flush()
            records = [
                ("inside", 56.9496, 24.1052, 0),
                ("middle", 56.9515, 24.1052, 0),
                ("swapped", 24.1052, 56.9496, 0),
            ]
            matches = await match_records(session, records)
            assert {(row["report_index"], row["user_id"]) for row in matches} == {
                (0, "alice"),
                (0, "bob"),
                (1, "bob"),
            }
    finally:
        await db.close()


async def test_footprint_candidates_equal_exact_geography_everywhere(settings: Settings) -> None:
    rng = random.Random(7)
    centres = [(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(220)]
    centres += [(89.99, 0), (-89.99, 45), (0, 180), (0, -180), (60, 179.999), (-60, -179.999)]
    radii = [1e-100, 0.5, 30, 2500, 80000, 700000, 5e6, 2.2e7, 1e100]
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            for latitude, longitude in centres:
                session.add(zone(latitude, longitude, rng.choice(radii)))
            await session.flush()
            near = (
                await session.execute(
                    text("""
                SELECT ST_Y(p::geometry), ST_X(p::geometry)
                FROM (SELECT ST_Project(center, least(radius_m, 1.9e7) * factor, azimuth) AS p
                      FROM geozones,
                           unnest(ARRAY[0.999999, 1.000001]) AS factor,
                           unnest(ARRAY[0, pi()/2, pi(), 3*pi()/2]) AS azimuth) projected
            """)
                )
            ).all()
            points = [(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(1500)]
            points += [(lat, lon) for lat, lon in near]
            points += [(90, 0), (-90, 0), (0, 180), (0, -180), (89.9999, 179.9999)]
            records = [(f"d{i}", lat, lon, 0) for i, (lat, lon) in enumerate(points)]
            actual = {
                (row["report_index"], row["zone_id"])
                for row in await match_records(session, records)
            }
            expected = set(
                (
                    await session.execute(
                        text("""
                    SELECT sample.ordinality - 1, zone.id
                    FROM unnest(CAST(:lons AS float8[]), CAST(:lats AS float8[]))
                         WITH ORDINALITY AS sample(longitude, latitude, ordinality)
                    JOIN geozones AS zone ON zone.active AND ST_DWithin(
                        zone.center,
                        ST_MakePoint(sample.longitude, sample.latitude)::geography,
                        zone.radius_m)
                """),
                        {"lons": [p[1] for p in points], "lats": [p[0] for p in points]},
                    )
                ).tuples()
            )
            assert expected and actual == expected
    finally:
        await db.close()


async def test_grid_cells_never_exclude_a_point_inside_the_box(settings: Settings) -> None:
    rng = random.Random(11)
    points = [(90, 0), (-90, 0), (0, 180), (0, -180), (90, 180), (-90, -180), (56.75, 24.25)]
    points += [(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(1500)]
    boxes = [(90, -10, 90, 10), (-90, 170, -89, 180), (-5, 180, 5, 180), (-5, 179.9, 5, 180)]
    boxes += [(56.5, 23.5, 57.5, 25), (56.75, 24.25, 56.75, 24.25), (-90, -180, 90, 180)]
    for _ in range(200):
        south, north = sorted(rng.uniform(-90, 90) for _ in range(2))
        west, east = sorted(rng.uniform(-180, 180) for _ in range(2))
        boxes.append((south, west, north, east))
    db = Database(settings.model_copy(update={"database_timeout_ms": 60000}))
    try:
        async with db.sessions() as session, session.begin():
            missed = (
                await session.execute(
                    text("""
                WITH box AS MATERIALIZED (
                    SELECT area, grid_cells(area) AS cells
                    FROM unnest(CAST(:s AS float8[]), CAST(:w AS float8[]),
                                CAST(:n AS float8[]), CAST(:e AS float8[])) AS b(s, w, n, e),
                         LATERAL (SELECT ST_MakeEnvelope(b.w, b.s, b.e, b.n, 4326) AS area) AS g
                ), point AS MATERIALIZED (
                    SELECT geom, grid_cell(geom) AS cell
                    FROM unnest(CAST(:lats AS float8[]), CAST(:lons AS float8[])) AS p(lat, lon),
                         LATERAL (SELECT ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326) AS geom) AS g
                )
                SELECT count(*) FROM box, point
                WHERE box.cells IS NOT NULL
                  AND ST_Intersects(point.geom, box.area)
                  AND NOT point.cell = ANY(box.cells)
            """),
                    {
                        "lats": [p[0] for p in points],
                        "lons": [p[1] for p in points],
                        "s": [b[0] for b in boxes],
                        "w": [b[1] for b in boxes],
                        "n": [b[2] for b in boxes],
                        "e": [b[3] for b in boxes],
                    },
                )
            ).scalar_one()
            assert missed == 0
    finally:
        await db.close()
