import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from starlette.requests import HTTPConnection

from geo_tracking.bus import Subjects, connect_nats, serve_metrics
from geo_tracking.gateway import Gateway
from geo_tracking.metrics import Metrics, monitor_loop
from geo_tracking.schemas import Identifier
from geo_tracking.settings import Settings


def create_gateway_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nats = await connect_nats(settings)
        metrics = Metrics("gateway")
        app.state.gateway = Gateway(settings, nats, metrics)
        responder = await serve_metrics(nats, Subjects(settings.subject_prefix), metrics.snapshot)
        monitor = asyncio.create_task(monitor_loop(metrics))
        try:
            yield
        finally:
            app.state.gateway.close()
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            await responder.unsubscribe()
            await nats.drain()

    app = FastAPI(title="Fleetline gateway", lifespan=lifespan)

    def gateway(connection: HTTPConnection) -> Gateway:
        return connection.app.state.gateway

    @app.websocket("/ws")
    async def dashboard(socket: WebSocket, user_id: Identifier) -> None:
        await gateway(socket).serve(socket, user_id)

    @app.get("/health/live")
    async def live() -> dict:
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready() -> dict:
        return {"status": "ready"}

    return app


app = create_gateway_app()
