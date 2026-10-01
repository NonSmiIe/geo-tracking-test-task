import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import FastAPI, Response, WebSocket
from starlette.requests import HTTPConnection

from geo_tracking.bus import connect_nats
from geo_tracking.gateway import Gateway
from geo_tracking.logs import configure
from geo_tracking.metrics import exposition, monitor_loop
from geo_tracking.schemas import Identifier
from geo_tracking.settings import Settings


def create_gateway_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure("gateway")

        async def reconnected() -> None:
            await app.state.gateway.resync()

        async def dropped(subject: str) -> None:
            await app.state.gateway.dropped(subject)

        nats = await connect_nats(settings, reconnected, dropped)
        app.state.gateway = Gateway(settings, nats)
        monitor = asyncio.create_task(monitor_loop("gateway"))
        try:
            yield
        finally:
            app.state.gateway.close()
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            await nats.drain()

    app = FastAPI(title="Fleetline gateway", lifespan=lifespan)

    def gateway(connection: HTTPConnection) -> Gateway:
        return cast(Gateway, connection.app.state.gateway)

    @app.websocket("/ws")
    async def dashboard(socket: WebSocket, user_id: Identifier) -> None:
        await gateway(socket).serve(socket, user_id)

    @app.get("/health/live")
    async def live() -> dict[str, Any]:
        return {"status": "alive"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        body, content_type = exposition("gateway")
        return Response(body, media_type=content_type)

    return app


app = create_gateway_app()
