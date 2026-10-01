import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from fastapi import HTTPException
from geoalchemy2 import WKTElement
from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert

from geo_tracking.db import Database
from geo_tracking.ingest import Ingest
from geo_tracking.models import DemoRun, Zone
from geo_tracking.schemas import Report
from geo_tracking.settings import Settings

LATITUDE, LONGITUDE, RADIUS = 56.9496, 24.1052, 250


class Demo:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        ingest: Ingest,
        zones_changed: Callable[[str], Awaitable[None]],
    ):
        self.settings, self.db, self.ingest, self.zones_changed = (
            settings,
            db,
            ingest,
            zones_changed,
        )
        self.tasks: dict[str, asyncio.Task[None]] = {}

    def describe(self, user_id: str, running: bool) -> dict[str, Any]:
        zone_id = uuid5(NAMESPACE_URL, f"fleetline-demo:{user_id}")
        return {
            "running": running,
            "device_prefix": f"demo-{zone_id.hex[:12]}-",
            "zone_id": str(zone_id),
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "radius_m": RADIUS,
            "devices": self.settings.demo_devices,
            "duration_seconds": self.settings.demo_seconds,
        }

    def horizon(self) -> datetime:
        return datetime.now(UTC) - timedelta(seconds=self.settings.demo_seconds)

    async def status(self, user_id: str) -> dict[str, Any]:
        async with self.db.sessions() as session:
            started = await session.scalar(
                select(DemoRun.started_at).where(
                    DemoRun.user_id == user_id, DemoRun.started_at > self.horizon()
                )
            )
        return self.describe(user_id, started is not None)

    async def start(self, user_id: str) -> dict[str, Any]:
        state = self.describe(user_id, True)
        async with self.db.sessions() as session, session.begin():
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtext('demo_runs'))"))
            running = await session.scalar(
                select(DemoRun.started_at).where(
                    DemoRun.user_id == user_id, DemoRun.started_at > self.horizon()
                )
            )
            if running is not None:
                return state
            others = (
                await session.execute(
                    select(func.count()).where(
                        DemoRun.user_id != user_id, DemoRun.started_at > self.horizon()
                    )
                )
            ).scalar_one()
            if others >= self.settings.demo_limit:
                raise HTTPException(429, "demo_capacity")
            started = datetime.now(UTC)
            await session.execute(
                insert(DemoRun)
                .values(user_id=user_id, started_at=started)
                .on_conflict_do_update(
                    index_elements=[DemoRun.user_id], set_={"started_at": started}
                )
            )
            center = WKTElement(f"POINT({LONGITUDE} {LATITUDE})", srid=4326)
            await session.execute(
                insert(Zone)
                .values(
                    id=state["zone_id"],
                    user_id=user_id,
                    name="Демо: склад",
                    center=center,
                    radius_m=RADIUS,
                    active=True,
                    version=1,
                )
                .on_conflict_do_update(
                    index_elements=[Zone.id],
                    set_={
                        "center": center,
                        "radius_m": RADIUS,
                        "active": True,
                        "version": Zone.version + 1,
                    },
                )
            )
        self.tasks[user_id] = asyncio.create_task(self.move(user_id, started, state))
        await self.zones_changed(user_id)
        return state

    async def current(self, user_id: str, started: datetime) -> bool:
        async with self.db.sessions() as session:
            return (
                await session.scalar(
                    select(DemoRun.user_id).where(
                        DemoRun.user_id == user_id, DemoRun.started_at == started
                    )
                )
                is not None
            )

    async def move(self, user_id: str, started: datetime, state: dict[str, Any]) -> None:
        try:
            for tick in range(self.settings.demo_seconds):
                if not await self.current(user_id, started):
                    return
                phase = tick * math.pi / 15
                await self.ingest.publish(
                    [
                        Report(
                            device_id=state["device_prefix"] + str(index + 1),
                            latitude=LATITUDE + math.sin(phase + index) * 0.003,
                            longitude=LONGITUDE + math.cos(phase + index) * 0.0015,
                            timestamp=datetime.now(UTC),
                        )
                        for index in range(state["devices"])
                    ]
                )
                await asyncio.sleep(1)
        finally:
            if self.tasks.get(user_id) is asyncio.current_task():
                del self.tasks[user_id]

    async def stop(self, user_id: str) -> dict[str, Any]:
        async with self.db.sessions() as session, session.begin():
            await session.execute(delete(DemoRun).where(DemoRun.user_id == user_id))
        task = self.tasks.get(user_id)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return self.describe(user_id, False)

    async def close(self) -> None:
        tasks = tuple(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
