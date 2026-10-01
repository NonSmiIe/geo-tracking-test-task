import asyncio
from time import monotonic

from starlette.responses import JSONResponse

from geo_tracking.settings import Settings


class Admission:
    def __init__(self, app, settings: Settings):
        self.app, self.settings = app, settings
        self.inflight = 0
        self.buffered_bytes = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if self.inflight >= self.settings.http_concurrency:
            await JSONResponse(
                {"detail": "http_capacity"}, status_code=503, headers={"Retry-After": "1"}
            )(scope, receive, send)
            return
        self.inflight += 1
        body = bytearray()
        try:
            deadline = monotonic() + 5
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError
                message = await asyncio.wait_for(receive(), remaining)
                if message["type"] == "http.disconnect":
                    return
                chunk = message.get("body", b"")
                if len(body) + len(chunk) > self.settings.body_bytes:
                    await JSONResponse({"detail": "body_capacity"}, status_code=413)(
                        scope, receive, send
                    )
                    return
                if self.buffered_bytes + len(chunk) > self.settings.http_buffer_bytes:
                    await JSONResponse(
                        {"detail": "http_buffer_capacity"},
                        status_code=503,
                        headers={"Retry-After": "1"},
                    )(scope, receive, send)
                    return
                self.buffered_bytes += len(chunk)
                body.extend(chunk)
                if not message.get("more_body", False):
                    break
            delivered = False

            async def buffered_receive():
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, buffered_receive, send)
        except TimeoutError:
            await JSONResponse({"detail": "body_timeout"}, status_code=408)(scope, receive, send)
        finally:
            self.inflight -= 1
            self.buffered_bytes -= len(body)
