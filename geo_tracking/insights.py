from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

ACTIVE_WINDOW_SECONDS = 60
ZONE_PAGE = 50

FLEET_SQL = text("""
SELECT count(*) AS total_devices,
       count(*) FILTER (
           WHERE reported_at >= now() - make_interval(secs => :window)
       ) AS active_devices,
       max(reported_at) AS last_report_at
FROM device_latest
""")

ZONE_SQL = text("""
SELECT zone.id, zone.name, zone.radius_m, zone.active, zone.version,
       ST_Y(zone.center::geometry) AS latitude, ST_X(zone.center::geometry) AS longitude,
       CASE WHEN zone.active
            THEN zone_occupancy(zone.center, zone.radius_m, zone.footprint,
                                now() - make_interval(secs => :window))
            ELSE 0 END AS devices_inside
FROM geozones AS zone
WHERE zone.user_id = :user_id
ORDER BY zone.id
LIMIT :limit
""")

COUNT_SQL = text("""
SELECT count(*) AS total_zones, count(*) FILTER (WHERE active) AS active_zones
FROM geozones WHERE user_id = :user_id
""")


async def insights(session: AsyncSession, user_id: str) -> dict[str, Any]:
    window = {"window": ACTIVE_WINDOW_SECONDS}
    fleet = dict((await session.execute(FLEET_SQL, window)).mappings().one())
    page = {"user_id": user_id, "limit": ZONE_PAGE + 1} | window
    zones = (await session.execute(ZONE_SQL, page)).mappings().all()
    counts = (await session.execute(COUNT_SQL, {"user_id": user_id})).mappings().one()
    return {
        "fleet": fleet
        | {
            "stale_devices": fleet["total_devices"] - fleet["active_devices"],
            "active_window_seconds": ACTIVE_WINDOW_SECONDS,
        },
        "zones": [dict(zone) for zone in zones[:ZONE_PAGE]],
        "zones_truncated": len(zones) > ZONE_PAGE,
        **dict(counts),
    }
