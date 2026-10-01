import asyncio
import math
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from fastapi import HTTPException
from geoalchemy2 import WKTElement
from sqlalchemy import select

from geo_tracking.models import Zone
from geo_tracking.pipeline import Failure
from geo_tracking.schemas import Report


class Demo:
    def __init__(self, db, pipeline, slots):
        self.db, self.pipeline, self.slots = db, pipeline, slots
        self.tasks: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    def status(self, user):
        zone_id = uuid5(NAMESPACE_URL, f"fleetline-demo:{user}")
        return {
            "running": user in self.tasks,
            "device_prefix": f"demo-{zone_id.hex[:12]}-",
            "zone_id": str(zone_id),
            "latitude": 56.9496,
            "longitude": 24.1052,
            "radius_m": 250,
            "devices": min(6, self.pipeline.settings.batch_reports),
            "duration_seconds": 120,
        }

    async def start(self, user):
        async with self.lock:
            if user in self.tasks:
                return self.status(user)
            if len(self.tasks) >= 4:
                raise HTTPException(429, "demo_capacity")
            state = self.status(user)
            async with self.slots, self.db.sessions() as session, session.begin():
                zone = (
                    await session.execute(
                        select(Zone).where(
                            Zone.id == uuid5(NAMESPACE_URL, f"fleetline-demo:{user}"),
                            Zone.user_id == user,
                        )
                    )
                ).scalar_one_or_none()
                if zone is None:
                    zone = Zone(
                        id=uuid5(NAMESPACE_URL, f"fleetline-demo:{user}"),
                        user_id=user,
                        name="Демо: склад",
                        version=1,
                    )
                    session.add(zone)
                else:
                    zone.version += 1
                zone.center = WKTElement("POINT(24.1052 56.9496)", srid=4326)
                zone.radius_m = 250
                zone.active = True
            self.tasks[user] = asyncio.create_task(self.move(user, state))
            return self.status(user)

    async def move(self, user, state):
        try:
            for tick in range(120):
                reports = [
                    Report(
                        device_id=state["device_prefix"] + str(index + 1),
                        latitude=state["latitude"] + math.sin(tick * math.pi / 15 + index) * 0.003,
                        longitude=state["longitude"]
                        + math.cos(tick * math.pi / 15 + index) * 0.0015,
                        timestamp=datetime.now(UTC),
                    )
                    for index in range(state["devices"])
                ]
                if isinstance(await self.pipeline.submit(reports), Failure):
                    break
                await asyncio.sleep(1)
        finally:
            self.tasks.pop(user, None)

    async def stop(self, user):
        async with self.lock:
            task = self.tasks.get(user)
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            return self.status(user)

    async def close(self):
        for user in tuple(self.tasks):
            await self.stop(user)
