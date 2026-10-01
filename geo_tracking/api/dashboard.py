from fastapi import APIRouter, WebSocket

from geo_tracking.api.services import ServicesDep, User
from geo_tracking.schemas import Identifier

router = APIRouter(tags=["dashboard"])


@router.get("/demo")
async def demo_status(user: User, services: ServicesDep) -> dict:
    return await services.demo.status(user)


@router.post("/demo/start")
async def start_demo(user: User, services: ServicesDep) -> dict:
    return await services.demo.start(user)


@router.post("/demo/stop")
async def stop_demo(user: User, services: ServicesDep) -> dict:
    return await services.demo.stop(user)


@router.websocket("/ws")
async def dashboard(socket: WebSocket, user_id: Identifier, services: ServicesDep) -> None:
    await services.gateway.serve(socket, user_id)
