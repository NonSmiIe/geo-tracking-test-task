from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncSession

from geo_tracking.membership import Inside, Key, Origin, Transition, Walk

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


MEMBERSHIP_SQL = text("""
SELECT membership.device_id, membership.zone_id, zone.user_id, membership.zone_version,
       (extract(epoch FROM membership.entered_at) * 1000000)::bigint AS entered_us,
       zone.active AND zone.version = membership.zone_version AS valid
FROM zone_membership AS membership
JOIN geozones AS zone ON zone.id = membership.zone_id
WHERE membership.device_id = ANY(CAST(:devices AS text[]))
""")

ENTER_SQL = text("""
INSERT INTO zone_membership AS membership (device_id, zone_id, zone_version, entered_at)
SELECT device_id, zone_id, zone_version, timestamptz 'epoch' + entered_us * interval '1 microsecond'
FROM unnest(CAST(:device_ids AS text[]), CAST(:zone_ids AS uuid[]),
            CAST(:zone_versions AS int[]), CAST(:entered_us AS bigint[]))
     AS entered(device_id, zone_id, zone_version, entered_us)
ON CONFLICT (device_id, zone_id) DO UPDATE
SET zone_version = excluded.zone_version, entered_at = excluded.entered_at
""")

LEAVE_SQL = text("""
DELETE FROM zone_membership AS membership
USING unnest(CAST(:device_ids AS text[]), CAST(:zone_ids AS uuid[]))
      AS left_zone(device_id, zone_id)
WHERE membership.device_id = left_zone.device_id AND membership.zone_id = left_zone.zone_id
""")

EVENT_COLUMNS = """id, user_id, zone_id, zone_version, device_id, kind,
       (extract(epoch FROM at) * 1000000)::bigint AS timestamp, dwell_us"""

RECORD_EVENTS_SQL = text(f"""
INSERT INTO zone_events
    (user_id, zone_id, zone_version, device_id, kind, at, dwell_us, topic_id, partition, "offset")
SELECT user_id, zone_id, zone_version, device_id, kind,
       timestamptz 'epoch' + at_us * interval '1 microsecond', dwell_us,
       :topic_id, partition, "offset"
FROM unnest(CAST(:user_ids AS text[]), CAST(:zone_ids AS uuid[]), CAST(:zone_versions AS int[]),
            CAST(:device_ids AS text[]), CAST(:kinds AS text[]), CAST(:at_us AS bigint[]),
            CAST(:dwell_us AS bigint[]), CAST(:partitions AS int[]), CAST(:offsets AS bigint[]))
     AS event(user_id, zone_id, zone_version, device_id, kind, at_us, dwell_us, partition, "offset")
RETURNING {EVENT_COLUMNS}
""")

REPLAYED_EVENTS_SQL = text(f"""
SELECT {EVENT_COLUMNS}
FROM zone_events
WHERE topic_id = :topic_id
  AND (partition, "offset") IN (
      SELECT * FROM unnest(CAST(:partitions AS int[]), CAST(:offsets AS bigint[])))
ORDER BY id
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


async def membership(
    session: AsyncSession, devices: Sequence[str]
) -> tuple[dict[str, dict[UUID, Inside]], set[Key]]:
    rows = (await session.execute(MEMBERSHIP_SQL, {"devices": list(devices)})).mappings()
    held: dict[str, dict[UUID, Inside]] = {}
    stale: set[Key] = set()
    for row in rows:
        if row["valid"]:
            held.setdefault(row["device_id"], {})[row["zone_id"]] = Inside(
                row["user_id"], row["zone_version"], row["entered_us"]
            )
        else:
            stale.add((row["device_id"], row["zone_id"]))
    return held, stale


async def apply_membership(session: AsyncSession, moved: Walk) -> None:
    if moved.left:
        left = list(moved.left)
        await session.execute(
            LEAVE_SQL,
            {"device_ids": [key[0] for key in left], "zone_ids": [key[1] for key in left]},
        )
    if moved.entered:
        entered = list(moved.entered.items())
        await session.execute(
            ENTER_SQL,
            {
                "device_ids": [key[0] for key, _ in entered],
                "zone_ids": [key[1] for key, _ in entered],
                "zone_versions": [inside.version for _, inside in entered],
                "entered_us": [inside.entered_us for _, inside in entered],
            },
        )


async def record_events(
    session: AsyncSession, topic_id: str, transitions: Sequence[Transition]
) -> Sequence[RowMapping]:
    if not transitions:
        return []
    result = await session.execute(
        RECORD_EVENTS_SQL,
        {
            "topic_id": topic_id,
            "user_ids": [event.user_id for event in transitions],
            "zone_ids": [event.zone_id for event in transitions],
            "zone_versions": [event.zone_version for event in transitions],
            "device_ids": [event.device_id for event in transitions],
            "kinds": [event.kind for event in transitions],
            "at_us": [event.at_us for event in transitions],
            "dwell_us": [event.dwell_us for event in transitions],
            "partitions": [event.origin[0] for event in transitions],
            "offsets": [event.origin[1] for event in transitions],
        },
    )
    return result.mappings().all()


async def replayed_events(
    session: AsyncSession, topic_id: str, origins: Sequence[Origin]
) -> Sequence[RowMapping]:
    result = await session.execute(
        REPLAYED_EVENTS_SQL,
        {
            "topic_id": topic_id,
            "partitions": [origin[0] for origin in origins],
            "offsets": [origin[1] for origin in origins],
        },
    )
    return result.mappings().all()


def zone_event(row: RowMapping) -> dict[str, Any]:
    return {
        "id": row["id"],
        "kind": row["kind"],
        "device_id": row["device_id"],
        "zone_id": str(row["zone_id"]),
        "zone_version": row["zone_version"],
        "timestamp": row["timestamp"],
        "dwell_us": row["dwell_us"],
    }
