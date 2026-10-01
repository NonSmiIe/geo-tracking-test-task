from collections.abc import Sequence

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncSession

Record = tuple[str, float, float, int]

WATERMARK_SQL = text("""
SELECT device_id, (extract(epoch FROM reported_at) * 1000000)::bigint AS reported_us
FROM device_latest WHERE device_id = ANY(CAST(:device_ids AS text[]))
""")

MATCH_SQL = text("""
WITH samples AS MATERIALIZED (
    SELECT ordinality - 1 AS report_index,
           ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS point
    FROM unnest(CAST(:longitudes AS float8[]), CAST(:latitudes AS float8[]))
         WITH ORDINALITY AS report(longitude, latitude, ordinality)
)
SELECT samples.report_index, zone.id AS zone_id, zone.user_id, zone.version AS zone_version
FROM samples
JOIN geozones AS zone
  ON zone.active
 AND ST_Intersects(zone.footprint, samples.point)
 AND ST_DWithin(zone.center, samples.point::geography, zone.radius_m)
""")

UPSERT_SQL = text("""
INSERT INTO device_latest AS device (device_id, position, reported_at)
SELECT device_id, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
       timestamptz 'epoch' + reported_us * interval '1 microsecond'
FROM unnest(CAST(:device_ids AS text[]), CAST(:longitudes AS float8[]),
            CAST(:latitudes AS float8[]), CAST(:reported_us AS bigint[]))
     AS report(device_id, longitude, latitude, reported_us)
ON CONFLICT (device_id) DO UPDATE
SET position = excluded.position, reported_at = excluded.reported_at
WHERE device.reported_at < excluded.reported_at
""")


async def watermarks(session: AsyncSession, device_ids: Sequence[str]) -> dict[str, int]:
    result = await session.execute(WATERMARK_SQL, {"device_ids": list(device_ids)})
    return dict(result.tuples().all())


async def match_records(session: AsyncSession, records: Sequence[Record]) -> Sequence[RowMapping]:
    result = await session.execute(
        MATCH_SQL,
        {
            "longitudes": [record[2] for record in records],
            "latitudes": [record[1] for record in records],
        },
    )
    return result.mappings().all()


async def persist_latest(session: AsyncSession, records: Sequence[Record]) -> None:
    latest: dict[str, Record] = {}
    for record in records:
        previous = latest.get(record[0])
        if previous is None or record[3] > previous[3]:
            latest[record[0]] = record
    if latest:
        values = list(latest.values())
        await session.execute(
            UPSERT_SQL,
            {
                "device_ids": [record[0] for record in values],
                "longitudes": [record[2] for record in values],
                "latitudes": [record[1] for record in values],
                "reported_us": [record[3] for record in values],
            },
        )
