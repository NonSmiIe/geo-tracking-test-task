import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from geo_tracking.api import dashboard, devices, geozones, health, locations
from geo_tracking.api.services import Services, zones_changed
from geo_tracking.bus import Subjects, connect_nats, ensure_topic, kafka_producer, serve_metrics
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.demo import Demo
from geo_tracking.gateway import Gateway
from geo_tracking.ingest import Ingest
from geo_tracking.metrics import Metrics, monitor_loop
from geo_tracking.schemas import Report
from geo_tracking.settings import Settings

STATIC = Path(__file__).parent.parent / "static"


async def database_unavailable(request: Request, error: Exception) -> JSONResponse:
    return JSONResponse(
        {"detail": "database_unavailable"}, status_code=503, headers={"Retry-After": "1"}
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await ensure_topic(settings)
        db = Database(settings)
        producer = kafka_producer(settings)
        await producer.start()
        nats = await connect_nats(settings)
        subjects = Subjects(settings.subject_prefix)
        metrics = Metrics("api")
        ingest = Ingest(settings, producer, metrics)
        services = Services(
            settings=settings,
            db=db,
            nats=nats,
            subjects=subjects,
            metrics=metrics,
            ingest=ingest,
            gateway=Gateway(settings, nats, metrics),
            demo=Demo(settings, db, ingest, partial(zones_changed, nats, subjects)),
        )
        app.state.services = services
        responder = await serve_metrics(nats, subjects, metrics.snapshot)
        monitor = asyncio.create_task(monitor_loop(metrics))
        try:
            yield
        finally:
            services.gateway.close()
            await services.demo.close()
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            await responder.unsubscribe()
            await producer.stop()
            await nats.drain()
            await db.close()

    app = FastAPI(title="Fleetline", lifespan=lifespan)
    for error_type in DATABASE_ERRORS:
        app.add_exception_handler(error_type, database_unavailable)
    for module in (locations, geozones, devices, dashboard, health):
        app.include_router(module.router)

    original = app.openapi

    def openapi() -> dict:
        schema = original()
        schema.setdefault("components", {}).setdefault("schemas", {})["Report"] = (
            Report.model_json_schema()
        )
        return schema

    app.openapi = openapi

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


app = create_app()
