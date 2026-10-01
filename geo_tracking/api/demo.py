from typing import Any

from fastapi import APIRouter

from geo_tracking.api.services import ServicesDep, User

router = APIRouter(tags=["demo"])


@router.get("/demo")
async def demo_status(user: User, services: ServicesDep) -> dict[str, Any]:
    return await services.demo.status(user)


@router.post("/demo/start")
async def start_demo(user: User, services: ServicesDep) -> dict[str, Any]:
    return await services.demo.start(user)


@router.post("/demo/stop")
async def stop_demo(user: User, services: ServicesDep) -> dict[str, Any]:
    return await services.demo.stop(user)
