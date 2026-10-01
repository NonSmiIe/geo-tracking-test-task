import uvicorn

uvicorn.run(
    "geo_tracking.main:app",
    host="0.0.0.0",
    port=8000,
    workers=1,
    access_log=False,
    ws_max_size=1024,
    ws_max_queue=4,
    timeout_graceful_shutdown=15,
)
