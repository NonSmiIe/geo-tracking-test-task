import uvicorn

from geo_tracking.settings import Settings

uvicorn.run(
    "geo_tracking.api.app:app",
    host="0.0.0.0",
    port=8000,
    workers=Settings().api_workers,
    access_log=False,
    ws_max_size=4096,
    limit_concurrency=4096,
    timeout_graceful_shutdown=15,
)
