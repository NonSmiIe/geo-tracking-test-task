import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from time import monotonic
from typing import Annotated
from uuid import UUID

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, WebSocket
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import cast, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from geo_tracking.admission import Admission
from geo_tracking.assistant import Assistant, AssistantRequest, selected_center
from geo_tracking.db import DATABASE_ERRORS, Database
from geo_tracking.demo import Demo
from geo_tracking.events import Frame
from geo_tracking.insights import insights
from geo_tracking.metrics import Metrics
from geo_tracking.models import DeviceLatest, Zone
from geo_tracking.pipeline import Failure, Pipeline
from geo_tracking.schemas import Identifier, Report, ZoneCreate, ZoneUpdate
from geo_tracking.sessions import Sessions
from geo_tracking.settings import Settings

STATIC = Path(__file__).parent / "static"


async def monitor_loop(metrics):
    while True:
        expected = monotonic() + 0.25
        await asyncio.sleep(0.25)
        metrics.loop_lag_ms.append(max(0, (monotonic() - expected) * 1000))


def zone_query():
    point = cast(Zone.center, Geometry("POINT", srid=4326))
    return select(
        Zone.id,
        Zone.name,
        Zone.radius_m,
        Zone.active,
        Zone.version,
        func.ST_Y(point).label("latitude"),
        func.ST_X(point).label("longitude"),
    )


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    db = Database(settings)
    metrics = Metrics()
    sessions = Sessions(settings, metrics)
    pipeline = Pipeline(db, sessions, metrics, settings)
    assistant = Assistant(settings)
    crud_slots = asyncio.Semaphore(4)
    demo = Demo(db, pipeline, crud_slots)

    @asynccontextmanager
    async def lifespan(app):
        async with db.engine.connect() as connection:
            await connection.execute(text("SELECT 1 FROM device_latest LIMIT 1"))
        pipeline.start()
        monitor = asyncio.create_task(monitor_loop(metrics))
        try:
            yield
        finally:
            await demo.close()
            await pipeline.stop()
            await sessions.close()
            await assistant.close()
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            await db.engine.dispose()

    app = FastAPI(title="Real-time geo tracking", lifespan=lifespan)
    app.add_middleware(Admission, settings=settings)
    app.state.pipeline = pipeline
    app.state.sessions = sessions
    app.state.db = db
    app.state.metrics = metrics
    app.state.assistant = assistant
    app.state.demo = demo

    async def database_error(request, error):
        return JSONResponse(
            {"detail": "database_unavailable"}, status_code=503, headers={"Retry-After": "1"}
        )

    for error_type in DATABASE_ERRORS:
        app.add_exception_handler(error_type, database_error)

    async def database_session():
        async with crud_slots, db.sessions() as session:
            yield session

    def identity(x_user_id: Annotated[Identifier, Header()]):
        return x_user_id

    Session = Annotated[AsyncSession, Depends(database_session)]
    User = Annotated[str, Depends(identity)]

    async def ingest(reports):
        outcome = await pipeline.submit(reports)
        if isinstance(outcome, Failure):
            raise HTTPException(503, outcome.reason, headers={"Retry-After": "1"})
        return outcome

    async def zones_changed(user):
        await sessions.broadcast((), {user: (Frame(b'{"type":"zones_changed"}'),)})

    @app.get("/demo")
    async def demo_status(user: User):
        return demo.status(user)

    @app.post("/demo/start")
    async def start_demo(user: User):
        result = await demo.start(user)
        await zones_changed(user)
        return result

    @app.post("/demo/stop")
    async def stop_demo(user: User):
        return await demo.stop(user)

    @app.post("/locations")
    async def location(report: Report):
        return await ingest([report])

    @app.post("/locations/batch")
    async def batch(
        reports: Annotated[list[Report], Body(min_length=1, max_length=settings.batch_reports)],
    ):
        return await ingest(reports)

    @app.post("/geozones", status_code=201)
    async def create_zone(payload: ZoneCreate, user: User, session: Session):
        async with session.begin():
            zone = Zone(
                user_id=user,
                name=payload.name,
                radius_m=payload.radius_m,
                active=payload.active,
                center=WKTElement(f"POINT({payload.longitude} {payload.latitude})", srid=4326),
            )
            session.add(zone)
            await session.flush()
            row = (await session.execute(zone_query().where(Zone.id == zone.id))).mappings().one()
        await zones_changed(user)
        return dict(row)

    @app.get("/geozones")
    async def list_zones(
        user: User,
        session: Session,
        after: UUID | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ):
        query = zone_query().where(Zone.user_id == user).order_by(Zone.id).limit(limit + 1)
        if after:
            query = query.where(Zone.id > after)
        rows = (await session.execute(query)).mappings().all()
        return {
            "items": [dict(row) for row in rows[:limit]],
            "next_cursor": str(rows[limit - 1]["id"]) if len(rows) > limit else None,
        }

    @app.get("/geozones/{zone_id}")
    async def get_zone(zone_id: UUID, user: User, session: Session):
        row = (
            (await session.execute(zone_query().where(Zone.id == zone_id, Zone.user_id == user)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise HTTPException(404, "zone_not_found")
        return dict(row)

    @app.patch("/geozones/{zone_id}")
    async def update_zone(zone_id: UUID, payload: ZoneUpdate, user: User, session: Session):
        async with session.begin():
            zone = (
                await session.execute(
                    select(Zone).where(Zone.id == zone_id, Zone.user_id == user).with_for_update()
                )
            ).scalar_one_or_none()
            if zone is None:
                raise HTTPException(404, "zone_not_found")
            values = payload.model_dump(exclude_unset=True)
            if "latitude" in values:
                zone.center = WKTElement(
                    f"POINT({values.pop('longitude')} {values.pop('latitude')})", srid=4326
                )
            for key, value in values.items():
                setattr(zone, key, value)
            zone.version += 1
            await session.flush()
            row = (await session.execute(zone_query().where(Zone.id == zone.id))).mappings().one()
        await zones_changed(user)
        return dict(row)

    @app.delete("/geozones/{zone_id}", status_code=204)
    async def delete_zone(zone_id: UUID, user: User, session: Session):
        async with session.begin():
            zone = (
                await session.execute(
                    select(Zone).where(Zone.id == zone_id, Zone.user_id == user).with_for_update()
                )
            ).scalar_one_or_none()
            if zone is None:
                raise HTTPException(404, "zone_not_found")
            await session.delete(zone)
        await zones_changed(user)

    @app.get("/devices/latest")
    async def latest(
        user: User,
        session: Session,
        after: str | None = None,
        limit: int = Query(1000, ge=1, le=1000),
    ):
        point = cast(DeviceLatest.position, Geometry("POINT", srid=4326))
        query = (
            select(
                DeviceLatest.device_id,
                DeviceLatest.reported_at.label("timestamp"),
                func.ST_Y(point).label("latitude"),
                func.ST_X(point).label("longitude"),
            )
            .order_by(DeviceLatest.device_id)
            .limit(limit + 1)
        )
        if after:
            query = query.where(DeviceLatest.device_id > after)
        rows = (await session.execute(query)).mappings().all()
        return {
            "items": [dict(row) for row in rows[:limit]],
            "next_cursor": rows[limit - 1]["device_id"] if len(rows) > limit else None,
        }

    @app.websocket("/ws")
    async def websocket(socket: WebSocket, user_id: Identifier):
        await sessions.serve(socket, user_id)

    @app.get("/insights")
    async def current_insights(user: User, session: Session):
        return await insights(session, user)

    @app.get("/assistant")
    async def assistant_status(user: User):
        return {
            "available": assistant.client is not None,
            "model": assistant.model,
            "mutates_data": False,
        }

    @app.post("/assistant")
    async def ask_assistant(payload: AssistantRequest, user: User):
        if assistant.client is None:
            raise HTTPException(503, "assistant_not_configured")
        async with crud_slots, db.sessions() as session:
            context = await insights(session, user)
            context["selected_center"] = await selected_center(session, payload)
        return await assistant.propose(payload, context)

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready():
        if not pipeline.running:
            raise HTTPException(503, "processor_unavailable")
        try:
            async with asyncio.timeout(0.5), crud_slots, db.sessions() as session:
                await session.execute(text("SELECT 1"))
        except DATABASE_ERRORS:
            raise HTTPException(503, "database_unavailable") from None
        return {"status": "ready"}

    @app.get("/metrics")
    async def stats():
        return metrics.snapshot() | {
            "pending_reports": pipeline.pending_reports,
            "connections": sessions.count,
            "database_connections_checked_out": db.engine.pool.checkedout(),
        }

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


app = create_app()
