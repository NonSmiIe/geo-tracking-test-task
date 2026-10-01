import asyncio
import socket
import threading
from collections.abc import Iterator
from uuid import uuid4

import httpx
import pytest
import uvicorn
from sqlalchemy import text
from sqlalchemy.engine import make_url

from geo_tracking.api.app import create_app
from geo_tracking.db import Database
from geo_tracking.processor import serve
from geo_tracking.settings import Settings


@pytest.fixture
def settings() -> Settings:
    token = uuid4().hex[:10]
    value = Settings(
        kafka_topic=f"reports-{token}",
        kafka_group=f"processors-{token}",
        kafka_partitions=4,
        subject_prefix=f"test-{token}",
        api_workers=1,
        processor_poll_ms=20,
        ack_interval_seconds=0.05,
        processor_retry_seconds=0.2,
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
                await connection.execute(text("TRUNCATE geozones, device_latest, demo_runs"))
        finally:
            await db.close()

    asyncio.run(reset())


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Worker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ready = threading.Event()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.task: asyncio.Task | None = None
        self.thread = threading.Thread(target=asyncio.run, args=(self.main(),), daemon=True)

    async def main(self) -> None:
        self.loop = asyncio.get_running_loop()
        assigned = asyncio.Event()
        self.task = asyncio.create_task(serve(self.settings, assigned))
        await assigned.wait()
        self.ready.set()
        try:
            await self.task
        except asyncio.CancelledError:
            pass

    def start(self) -> "Worker":
        self.thread.start()
        assert self.ready.wait(30), "processor never received partitions"
        return self

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.task.cancel)
        self.thread.join(15)


class Stack:
    def __init__(self, settings: Settings):
        self.settings = settings
        port = free_port()
        self.url = f"http://127.0.0.1:{port}"
        self.ws_url = f"ws://127.0.0.1:{port}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                create_app(settings),
                host="127.0.0.1",
                port=port,
                log_level="warning",
                ws_max_size=4096,
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.processor: Worker | None = None

    def __enter__(self) -> "Stack":
        self.thread.start()
        for _ in range(300):
            if self.server.started:
                break
            threading.Event().wait(0.05)
        assert self.server.started, "API server did not start"
        self.processor = Worker(self.settings).start()
        return self

    def restart_processor(self) -> None:
        self.processor.stop()
        self.processor = Worker(self.settings).start()

    def __exit__(self, *exc: object) -> None:
        self.processor.stop()
        self.server.should_exit = True
        self.thread.join(15)


@pytest.fixture
def stack(settings: Settings) -> Iterator[Stack]:
    with Stack(settings) as running:
        yield running


@pytest.fixture
def http(stack: Stack) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=stack.url, timeout=10) as client:
        yield client
