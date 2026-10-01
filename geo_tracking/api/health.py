import asyncio
import math
from typing import Any

import aiohttp
from fastapi import APIRouter, HTTPException, Response

from geo_tracking.api.services import ServicesDep
from geo_tracking.metrics import exposition

router = APIRouter(tags=["health"])

STATS = {
    "freshness_p95_seconds": "fleet:freshness:p95",
    "reports_per_second": "fleet:ingest_accepted:rate1m",
    "consumer_lag": "fleet:consumer_lag:records",
}


@router.get("/health/live")
async def live() -> dict[str, Any]:
    return {"status": "alive"}


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    body, content_type = exposition("api")
    return Response(body, media_type=content_type)


@router.get("/stats")
async def stats(services: ServicesDep) -> dict[str, Any]:
    async def value(expression: str) -> float | None:
        async with services.prometheus.get(
            "/api/v1/query", params={"query": expression}
        ) as response:
            response.raise_for_status()
            result = (await response.json())["data"]["result"]
        number = float(result[0]["value"][1]) if result else math.nan
        return None if math.isnan(number) else number

    try:
        values = await asyncio.gather(*map(value, STATS.values()))
    except (aiohttp.ClientError, TimeoutError):
        raise HTTPException(503, "prometheus_unavailable") from None
    return dict(zip(STATS, values, strict=True))
