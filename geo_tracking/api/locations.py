from typing import Any

import msgspec
from fastapi import APIRouter, HTTPException, Request, WebSocket

from geo_tracking.api.services import Services, ServicesDep
from geo_tracking.bus import ProduceFailed
from geo_tracking.ingest import Overloaded
from geo_tracking.schemas import REPORT, Report, batch_decoder

router = APIRouter(tags=["locations"])
REPORT_SCHEMA = {"$ref": "#/components/schemas/Report"}


async def bounded_body(request: Request, limit: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(413, "body_capacity")
    return bytes(body)


async def publish(services: Services, reports: list[Report]) -> dict:
    try:
        await services.ingest.publish(reports)
    except Overloaded:
        raise HTTPException(503, "ingest_capacity", headers={"Retry-After": "1"}) from None
    except ProduceFailed:
        raise HTTPException(503, "broker_unavailable", headers={"Retry-After": "1"}) from None
    return {"accepted": len(reports)}


def parse(decoder: msgspec.json.Decoder, body: bytes) -> Any:
    try:
        return decoder.decode(body)
    except msgspec.DecodeError as error:
        raise HTTPException(422, str(error)) from None


@router.post(
    "/locations",
    status_code=202,
    openapi_extra={"requestBody": {"content": {"application/json": {"schema": REPORT_SCHEMA}}}},
)
async def location(request: Request, services: ServicesDep) -> dict:
    body = await bounded_body(request, services.settings.body_bytes)
    return await publish(services, [parse(REPORT, body)])


@router.post(
    "/locations/batch",
    status_code=202,
    openapi_extra={
        "requestBody": {
            "content": {"application/json": {"schema": {"type": "array", "items": REPORT_SCHEMA}}}
        }
    },
)
async def batch(request: Request, services: ServicesDep) -> dict:
    body = await bounded_body(request, services.settings.body_bytes)
    return await publish(services, parse(batch_decoder(services.settings.batch_reports), body))


@router.websocket("/ingest")
async def ingest(socket: WebSocket, services: ServicesDep) -> None:
    await services.ingest.stream(socket)
