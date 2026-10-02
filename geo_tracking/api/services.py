from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, cast

import aiohttp
from fastapi import Depends, Header
from nats.aio.client import Client
from sqlalchemy import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import HTTPConnection

from geo_tracking.db import Database
from geo_tracking.ingest import Ingest
from geo_tracking.schemas import Identifier
from geo_tracking.settings import Settings
from geo_tracking.subjects import Subjects

ZONES_CHANGED = b'{"type":"zones_changed"}'


@dataclass
class Services:
    settings: Settings
    db: Database
    nats: Client
    subjects: Subjects
    prometheus: aiohttp.ClientSession
    ingest: Ingest

    async def zones_changed(self, user_id: str) -> None:
        await self.nats.publish(self.subjects.zones(user_id), ZONES_CHANGED)


def page(rows: Sequence[RowMapping], limit: int, key: str) -> dict[str, Any]:
    return {
        "items": [dict(row) for row in rows[:limit]],
        "next_cursor": str(rows[limit - 1][key]) if len(rows) > limit else None,
    }


def services(connection: HTTPConnection) -> Services:
    return cast(Services, connection.app.state.services)


ServicesDep = Annotated[Services, Depends(services)]


async def database_session(services: ServicesDep) -> AsyncIterator[AsyncSession]:
    async with services.db.sessions() as session:
        yield session


def identity(x_user_id: Annotated[Identifier, Header()]) -> str:
    return x_user_id


Session = Annotated[AsyncSession, Depends(database_session)]
User = Annotated[str, Depends(identity)]
