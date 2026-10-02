from collections.abc import Sequence

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncSession

Record = tuple[str, float, float, int]

PROGRESS_SQL = text("""
SELECT partition, persisted FROM consumer_progress
WHERE topic_id = :topic_id AND partition = ANY(CAST(:partitions AS int[]))
""")

ADVANCE_SQL = text("""
INSERT INTO consumer_progress AS progress (topic_id, partition, persisted)
SELECT :topic_id, partition, persisted
FROM unnest(CAST(:partitions AS int[]), CAST(:offsets AS bigint[])) AS batch(partition, persisted)
ON CONFLICT (topic_id, partition) DO UPDATE
SET persisted = greatest(progress.persisted, excluded.persisted)
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
RETURNING device.device_id, (extract(epoch FROM old.reported_at) * 1000000)::bigint
""")


async def persisted(
    session: AsyncSession, topic_id: str, partitions: Sequence[int]
) -> dict[int, int]:
    result = await session.execute(
        PROGRESS_SQL, {"topic_id": topic_id, "partitions": list(partitions)}
    )
    return {partition: -1 for partition in partitions} | dict(result.tuples().all())


async def advance(session: AsyncSession, topic_id: str, offsets: dict[int, int]) -> None:
    await session.execute(
        ADVANCE_SQL,
        {"topic_id": topic_id, "partitions": list(offsets), "offsets": list(offsets.values())},
    )


async def match_records(session: AsyncSession, records: Sequence[Record]) -> Sequence[RowMapping]:
    result = await session.execute(
        MATCH_SQL,
        {
            "longitudes": [record[2] for record in records],
            "latitudes": [record[1] for record in records],
        },
    )
    return result.mappings().all()


async def persist_latest(session: AsyncSession, records: Sequence[Record]) -> dict[str, int | None]:
    latest: dict[str, Record] = {}
    for record in records:
        previous = latest.get(record[0])
        if previous is None or record[3] > previous[3]:
            latest[record[0]] = record
    if not latest:
        return {}
    values = list(latest.values())
    result = await session.execute(
        UPSERT_SQL,
        {
            "device_ids": [record[0] for record in values],
            "longitudes": [record[2] for record in values],
            "latitudes": [record[1] for record in values],
            "reported_us": [record[3] for record in values],
        },
    )
    return dict(result.tuples().all())
