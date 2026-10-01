from sqlalchemy import text

FLEET_SQL = """
SELECT count(*) AS total_devices,
       count(*) FILTER (WHERE reported_at >= now() - interval '60 seconds') AS active_devices,
       max(reported_at) AS last_report_at
FROM device_latest
"""

ZONE_SQL = """
SELECT zone.id, zone.name, zone.radius_m, zone.active, zone.version,
       ST_Y(zone.center::geometry) AS latitude, ST_X(zone.center::geometry) AS longitude,
       CASE WHEN zone.active THEN (
           SELECT count(*) FROM device_latest AS device
           WHERE device.reported_at >= now() - interval '60 seconds'
             AND ST_DWithin(device.position, zone.center, zone.radius_m)
       ) ELSE 0 END AS devices_inside
FROM geozones AS zone
WHERE zone.user_id = :user_id
ORDER BY zone.id
LIMIT 51
"""


async def insights(session, user_id):
    fleet = dict((await session.execute(text(FLEET_SQL))).mappings().one())
    zones = (await session.execute(text(ZONE_SQL), {"user_id": user_id})).mappings().all()
    counts = (
        (
            await session.execute(
                text("""
        SELECT count(*) AS total_zones, count(*) FILTER (WHERE active) AS active_zones
        FROM geozones WHERE user_id = :user_id
    """),
                {"user_id": user_id},
            )
        )
        .mappings()
        .one()
    )
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
