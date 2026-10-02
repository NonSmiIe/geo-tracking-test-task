import asyncio
from collections.abc import Iterator
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from geo_tracking.db import Database
from geo_tracking.settings import Settings
from tests.stack import Stack, free_port


@pytest.fixture
def settings() -> Settings:
    token = uuid4().hex[:10]
    value = Settings(
        kafka_topic=f"reports-{token}",
        kafka_group=f"processors-{token}",
        kafka_partitions=4,
        subject_prefix=f"test-{token}",
        processor_poll_ms=20,
        ack_interval_seconds=0.05,
        processor_retry_seconds=0.2,
        metrics_port=free_port(),
    )
    if make_url(value.database_url).database != "geo_test":
        raise RuntimeError("tests require a dedicated geo_test database")
    return value


@pytest.fixture(autouse=True)
def clean_database(settings: Settings) -> None:
    async def reset() -> None:
        db = Database(settings)
        try:
            async with db.engine.begin() as connection:
                await connection.execute(
                    text("TRUNCATE geozones, device_latest, consumer_progress")
                )
        finally:
            await db.close()

    asyncio.run(reset())


@pytest.fixture
def stack(settings: Settings) -> Iterator[Stack]:
    with Stack(settings) as running:
        yield running


@pytest.fixture
def http(stack: Stack) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=stack.url, timeout=10) as client:
        yield client
