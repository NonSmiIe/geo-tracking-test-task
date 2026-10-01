import asyncio

from geo_tracking.admission import Admission
from geo_tracking.settings import Settings


async def test_aggregate_body_budget_rejects_and_releases_after_response():
    started, release = asyncio.Event(), asyncio.Event()

    async def app(scope, receive, send):
        await receive()
        started.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = Admission(
        app, Settings(body_bytes=1024, http_buffer_bytes=1024, http_concurrency=2)
    )

    def request():
        async def receive():
            return {"type": "http.request", "body": b"x" * 768, "more_body": False}

        return receive

    first_messages, second_messages = [], []

    async def first_send(message):
        first_messages.append(message)

    async def second_send(message):
        second_messages.append(message)

    first = asyncio.create_task(middleware({"type": "http"}, request(), first_send))
    try:
        await started.wait()
        await middleware({"type": "http"}, request(), second_send)
        assert second_messages[0]["status"] == 503
        assert middleware.buffered_bytes == 768 and middleware.inflight == 1
        release.set()
        await first
        assert first_messages[0]["status"] == 200
        assert middleware.buffered_bytes == 0 and middleware.inflight == 0
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
