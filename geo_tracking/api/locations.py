from functools import cache
from typing import Any

from aiokafka.errors import KafkaError
from fastapi import APIRouter, HTTPException, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from pydantic import TypeAdapter, ValidationError

from geo_tracking.api.services import Services, ServicesDep
from geo_tracking.ingest import Overloaded
from geo_tracking.schemas import Report, ReportAdapter, batch_adapter

router = APIRouter(tags=["locations"])
REPORT_SCHEMA = {"$ref": "#/components/schemas/Report"}


@cache
def batches(limit: int) -> TypeAdapter:
    return batch_adapter(limit)


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
    except KafkaError:
        raise HTTPException(503, "broker_unavailable", headers={"Retry-After": "1"}) from None
    return {"accepted": len(reports)}


def parse(adapter: TypeAdapter, body: bytes) -> Any:
    try:
        return adapter.validate_json(body)
    except ValidationError as error:
        raise RequestValidationError(error.errors(include_url=False)) from None


@router.post(
    "/locations",
    status_code=202,
    openapi_extra={"requestBody": {"content": {"application/json": {"schema": REPORT_SCHEMA}}}},
)
async def location(request: Request, services: ServicesDep) -> dict:
    body = await bounded_body(request, services.settings.body_bytes)
    return await publish(services, [parse(ReportAdapter, body)])


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
    return await publish(services, parse(batches(services.settings.batch_reports), body))


@router.websocket("/ingest")
async def ingest(socket: WebSocket, services: ServicesDep) -> None:
    await services.ingest.stream(socket)
