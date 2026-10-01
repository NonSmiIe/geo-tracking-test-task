from sqlalchemy import text

MATCH_SQL = """
WITH samples AS MATERIALIZED (
    SELECT ordinality - 1 AS report_index,
           ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography AS position
    FROM unnest(CAST(:longitudes AS float8[]), CAST(:latitudes AS float8[]))
         WITH ORDINALITY AS report(longitude, latitude, ordinality)
), bounds AS MATERIALIZED (
    SELECT DISTINCT radius_bucket, power(2.0, radius_bucket) AS radius
    FROM geozones WHERE active
)
SELECT samples.report_index, zone.id AS zone_id, zone.user_id, zone.version AS zone_version
FROM samples CROSS JOIN bounds
JOIN geozones AS zone
  ON zone.active
 AND zone.radius_bucket = bounds.radius_bucket
 AND ST_DWithin(zone.center, samples.position, bounds.radius)
 AND ST_DWithin(zone.center, samples.position, zone.radius_m)
LIMIT :match_limit
"""

UPSERT_SQL = """
INSERT INTO device_latest (device_id, position, reported_at)
SELECT device_id, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, timestamp
FROM unnest(CAST(:device_ids AS text[]), CAST(:longitudes AS float8[]),
            CAST(:latitudes AS float8[]), CAST(:timestamps AS timestamptz[]))
     AS report(device_id, longitude, latitude, timestamp)
ON CONFLICT (device_id) DO UPDATE
SET position = excluded.position, reported_at = excluded.reported_at
"""


async def match_reports(session, reports, limit):
    result = await session.execute(
        text(MATCH_SQL),
        {
            "longitudes": [report.longitude for report in reports],
            "latitudes": [report.latitude for report in reports],
            "match_limit": limit + 1,
        },
    )
    return result.mappings().all()


async def persist_latest(session, reports):
    latest = {}
    for report in reports:
        previous = latest.get(report.device_id)
        if previous is None or report.timestamp > previous.timestamp:
            latest[report.device_id] = report
    values = list(latest.values())
    if values:
        await session.execute(
            text(UPSERT_SQL),
            {
                "device_ids": [report.device_id for report in values],
                "longitudes": [report.longitude for report in values],
                "latitudes": [report.latitude for report in values],
                "timestamps": [report.timestamp for report in values],
            },
        )
