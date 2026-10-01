from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GEO_", extra="ignore")

    database_url: str = "postgresql+asyncpg://geo:local-demo-password@localhost:5497/geo"
    database_pool: int = Field(default=5, ge=1)
    database_timeout_ms: int = Field(default=2000, ge=1)

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "reports"
    kafka_partitions: int = Field(default=24, ge=1)
    kafka_group: str = "processors"

    nats_url: str = "nats://localhost:4222"
    subject_prefix: str = Field(default="fleet", pattern=r"^[a-z0-9-]+$")

    api_workers: int = Field(default=4, ge=1)
    body_bytes: int = Field(default=262144, ge=1024)
    batch_reports: int = Field(default=200, ge=1, le=1000)
    produce_window: int = Field(default=8192, ge=1)
    ingest_window: int = Field(default=1024, ge=1)
    ack_interval_seconds: float = Field(default=0.2, gt=0)

    processor_batch: int = Field(default=2000, ge=1)
    processor_poll_ms: int = Field(default=50, ge=1)
    processor_retry_seconds: float = Field(default=1, gt=0)
    alert_frame_items: int = Field(default=1000, ge=1)

    max_zones_per_user: int = Field(default=1000, ge=1)
    max_connections: int = Field(default=128, ge=1)
    viewport_tiles: int = Field(default=16, ge=1)
    websocket_queue_bytes: int = Field(default=8388608, ge=1024)
    send_timeout_seconds: float = Field(default=2, gt=0)
    metrics_gather_seconds: float = Field(default=0.3, gt=0)

    demo_devices: int = Field(default=6, ge=1)
    demo_seconds: int = Field(default=120, ge=1)
    demo_limit: int = Field(default=4, ge=1)
