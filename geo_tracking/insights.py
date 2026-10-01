from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

FLEET_SQL = text("""
SELECT count(*) AS total_devices,
       count(*) FILTER (WHERE reported_at >= now() - interval '60 seconds') AS active_devices,
       max(reported_at) AS last_report_at
FROM device_latest
""")

ZONE_SQL = text("""
SELECT zone.id, zone.name, zone.radius_m, zone.active, zone.version,
       ST_Y(zone.center::geometry) AS latitude, ST_X(zone.center::geometry) AS longitude,
       CASE
           WHEN NOT zone.active THEN 0
           WHEN area.cells IS NULL THEN (
               SELECT count(*) FROM device_latest AS device
               WHERE device.reported_at >= now() - interval '60 seconds'
                 AND ST_DWithin(zone.center, device.position::geography, zone.radius_m)
           )
           ELSE (
               SELECT count(*) FROM device_latest AS device
               WHERE device.cell = ANY(area.cells)
                 AND device.reported_at >= now() - interval '60 seconds'
                 AND ST_DWithin(zone.center, device.position::geography, zone.radius_m)
           )
       END AS devices_inside
FROM geozones AS zone
CROSS JOIN LATERAL (SELECT grid_cells(zone.footprint) AS cells) AS area
WHERE zone.user_id = :user_id
ORDER BY zone.id
LIMIT 51
""")

COUNT_SQL = text("""
SELECT count(*) AS total_zones, count(*) FILTER (WHERE active) AS active_zones
FROM geozones WHERE user_id = :user_id
""")


async def insights(session: AsyncSession, user_id: str) -> dict:
    fleet = dict((await session.execute(FLEET_SQL)).mappings().one())
    zones = (await session.execute(ZONE_SQL, {"user_id": user_id})).mappings().all()
    counts = (await session.execute(COUNT_SQL, {"user_id": user_id})).mappings().one()
    return {
        "fleet": fleet
        | {
            "stale_devices": fleet["total_devices"] - fleet["active_devices"],
            "active_window_seconds": 60,
        },
        "zones": [dict(zone) for zone in zones[:50]],
        "zones_truncated": len(zones) > 50,
        **dict(counts),
    }
