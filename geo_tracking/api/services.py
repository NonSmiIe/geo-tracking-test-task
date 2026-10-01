from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header
from nats.aio.client import Client
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import HTTPConnection

from geo_tracking.bus import Subjects
from geo_tracking.db import Database
from geo_tracking.demo import Demo
from geo_tracking.ingest import Ingest
from geo_tracking.metrics import Metrics
from geo_tracking.schemas import Identifier
from geo_tracking.settings import Settings

ZONES_CHANGED = b'{"type":"zones_changed"}'


@dataclass
class Services:
    settings: Settings
    db: Database
    nats: Client
    subjects: Subjects
    metrics: Metrics
    ingest: Ingest
    demo: Demo


async def zones_changed(nats: Client, subjects: Subjects, user_id: str) -> None:
    await nats.publish(subjects.zones(user_id), ZONES_CHANGED)


def services(connection: HTTPConnection) -> Services:
    return connection.app.state.services


ServicesDep = Annotated[Services, Depends(services)]


async def database_session(services: ServicesDep) -> AsyncIterator[AsyncSession]:
    async with services.db.sessions() as session:
        yield session


def identity(x_user_id: Annotated[Identifier, Header()]) -> str:
    return x_user_id


Session = Annotated[AsyncSession, Depends(database_session)]
User = Annotated[str, Depends(identity)]
