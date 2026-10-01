import asyncio

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from geo_tracking.api.services import ServicesDep
from geo_tracking.bus import gather_metrics
from geo_tracking.db import DATABASE_ERRORS
from geo_tracking.metrics import merge

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def live() -> dict:
    return {"status": "alive"}


@router.get("/health/ready")
async def ready(services: ServicesDep) -> dict:
    if not services.nats.is_connected:
        raise HTTPException(503, "nats_unavailable")
    try:
        async with asyncio.timeout(1):
            await services.ingest.producer.client.fetch_all_metadata()
    except Exception:
        raise HTTPException(503, "broker_unavailable") from None
    try:
        async with asyncio.timeout(1), services.db.sessions() as session:
            await session.execute(text("SELECT 1"))
    except (TimeoutError, *DATABASE_ERRORS):
        raise HTTPException(503, "database_unavailable") from None
    return {"status": "ready"}


@router.get("/metrics")
async def metrics(services: ServicesDep) -> dict:
    snapshots = await gather_metrics(
        services.nats, services.subjects, services.settings.metrics_gather_seconds
    )
    return merge(snapshots)
