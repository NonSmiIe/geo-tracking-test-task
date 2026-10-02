import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import msgspec
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from geo_tracking.api import devices, geozones, health, locations
from geo_tracking.api.context import RequestContext
from geo_tracking.api.limits import BodyLimit
from geo_tracking.api.services import Services
from geo_tracking.bus import Producer, connect_nats, ensure_topic
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.ingest import Ingest
from geo_tracking.logs import configure
from geo_tracking.metrics import monitor_loop
from geo_tracking.schemas import Report
from geo_tracking.settings import Settings
from geo_tracking.subjects import Subjects

STATIC = Path(__file__).parent.parent / "static"


async def database_unavailable(request: Request, error: Exception) -> JSONResponse:
    return JSONResponse(
        {"detail": "database_unavailable"}, status_code=503, headers={"Retry-After": "1"}
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure("api")
        await ensure_topic(settings)
        db = Database(settings)
        producer = Producer(settings)
        await producer.start()
        nats = await connect_nats(settings)
        subjects = Subjects(settings.subject_prefix)
        ingest = Ingest(settings, producer)
        prometheus = aiohttp.ClientSession(
            settings.prometheus_url, timeout=aiohttp.ClientTimeout(total=2)
        )
        services = Services(
            settings=settings,
            db=db,
            nats=nats,
            subjects=subjects,
            prometheus=prometheus,
            ingest=ingest,
        )
        app.state.services = services
        monitor = asyncio.create_task(monitor_loop("api"))
        try:
            yield
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            await prometheus.close()
            await producer.stop()
            await nats.drain()
            await db.close()

    app = FastAPI(title="Fleetline", lifespan=lifespan)
    app.add_middleware(BodyLimit, limit=settings.body_bytes)
    app.add_middleware(RequestContext)
    for error_type in DATABASE_ERRORS:
        app.add_exception_handler(error_type, database_unavailable)
    for module in (locations, geozones, devices, health):
        app.include_router(module.router)

    original = app.openapi

    def openapi() -> dict[str, Any]:
        schema = original()
        (_,), components = msgspec.json.schema_components(
            [Report], ref_template="#/components/schemas/{name}"
        )
        schema.setdefault("components", {}).setdefault("schemas", {}).update(components)
        return schema

    app.openapi = openapi  # type: ignore[method-assign]

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


app = create_app()
