import asyncio

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import make_url

from geo_tracking.db import Database
from geo_tracking.main import create_app
from geo_tracking.settings import Settings


@pytest.fixture
def settings():
    value = Settings()
    if make_url(value.database_url).database != "geo_test":
        raise RuntimeError("tests require a dedicated geo_test database")
    return value


@pytest.fixture(autouse=True)
def clean_database(settings):
    async def reset():
        db = Database(settings)
        try:
            async with db.engine.begin() as connection:
                await connection.execute(text("TRUNCATE geozones, device_latest"))
        finally:
            await db.engine.dispose()

    asyncio.run(reset())


@pytest.fixture
def client(settings):
    with TestClient(create_app(settings)) as client:
        yield client
