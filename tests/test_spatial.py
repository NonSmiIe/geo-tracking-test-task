from datetime import UTC, datetime

import pytest
from geoalchemy2 import WKTElement
from sqlalchemy import text

from geo_tracking.db import Database
from geo_tracking.models import Zone
from geo_tracking.schemas import Report
from geo_tracking.spatial import match_reports


@pytest.mark.parametrize("latitude,longitude", [(0, 0), (80, 25), (0, 179.9999)])
async def test_metres_and_boundary_with_projected_ground_truth(settings, latitude, longitude):
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            center = f"SRID=4326;POINT({longitude} {latitude})"
            zone = Zone(
                user_id="alice",
                name="boundary",
                center=WKTElement(center, extended=True),
                radius_m=100.000001,
                active=True,
            )
            session.add(zone)
            await session.flush()
            points = (
                (
                    await session.execute(
                        text("""
                SELECT distance, ST_X(point::geometry) AS longitude,
                       ST_Y(point::geometry) AS latitude
                FROM (SELECT distance,
                             ST_Project(CAST(:center AS geography), distance, pi()/2) AS point
                      FROM unnest(ARRAY[99.0,100.0,101.0]) AS distance) projected
            """),
                        {"center": center},
                    )
                )
                .mappings()
                .all()
            )
            reports = [
                Report(
                    device_id=f"device-{index}",
                    timestamp=datetime.now(UTC),
                    latitude=point["latitude"],
                    longitude=point["longitude"],
                )
                for index, point in enumerate(points)
            ]
            matches = await match_reports(session, reports, 10)
            assert {row["report_index"] for row in matches} == {0, 1}
    finally:
        await db.engine.dispose()


async def test_exact_variable_radius_overlap_and_coordinate_order(settings):
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            for owner, radius in (("alice", 100), ("bob", 1000)):
                session.add(
                    Zone(
                        user_id=owner,
                        name=owner,
                        center=WKTElement("POINT(24.1052 56.9496)", srid=4326),
                        radius_m=radius,
                        active=True,
                    )
                )
            session.add(
                Zone(
                    user_id="paused",
                    name="paused",
                    center=WKTElement("POINT(24.1052 56.9496)", srid=4326),
                    radius_m=100000,
                    active=False,
                )
            )
            await session.flush()
            reports = [
                Report(
                    device_id="inside",
                    latitude=56.9496,
                    longitude=24.1052,
                    timestamp=datetime.now(UTC),
                ),
                Report(
                    device_id="middle",
                    latitude=56.9515,
                    longitude=24.1052,
                    timestamp=datetime.now(UTC),
                ),
                Report(
                    device_id="swapped",
                    latitude=24.1052,
                    longitude=56.9496,
                    timestamp=datetime.now(UTC),
                ),
            ]
            matches = await match_reports(session, reports, 10)
            assert {(row["report_index"], row["user_id"]) for row in matches} == {
                (0, "alice"),
                (0, "bob"),
                (1, "bob"),
            }
    finally:
        await db.engine.dispose()


async def test_radius_bucket_results_equal_exact_geography_even_for_global_outlier(settings):
    db = Database(settings)
    try:
        async with db.sessions() as session, session.begin():
            for radius in (1e-100, 0.5, 1, 2, 1024, 10000, 30000000, 1e100):
                session.add(
                    Zone(
                        user_id="alice",
                        name=str(radius),
                        center=WKTElement("POINT(179.9999 80)", srid=4326),
                        radius_m=radius,
                        active=True,
                    )
                )
            await session.flush()
            for latitude, longitude in ((80, 179.9999), (80, -179.99), (0, 0), (-80, 0)):
                point = f"SRID=4326;POINT({longitude} {latitude})"
                expected = set(
                    (
                        await session.execute(
                            text("""
                    SELECT id FROM geozones
                    WHERE active AND ST_DWithin(center,CAST(:point AS geography),radius_m)
                """),
                            {"point": point},
                        )
                    )
                    .scalars()
                    .all()
                )
                actual = await match_reports(
                    session,
                    [
                        Report(
                            device_id="test",
                            latitude=latitude,
                            longitude=longitude,
                            timestamp=datetime.now(UTC),
                        )
                    ],
                    20,
                )
                assert {row["zone_id"] for row in actual} == expected
    finally:
        await db.engine.dispose()
