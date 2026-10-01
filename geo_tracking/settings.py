from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GEO_", extra="ignore")

    database_url: str = "postgresql+asyncpg://geo:local-demo-password@localhost:5497/geo"
    ingress_reports: int = Field(default=4096, ge=1)
    batch_reports: int = Field(default=200, ge=1, le=1000)
    batch_delay_seconds: float = Field(default=0.025, gt=0, le=1)
    http_concurrency: int = Field(default=512, ge=1)
    body_bytes: int = Field(default=262144, ge=1024)
    http_buffer_bytes: int = Field(default=16777216, ge=1024)
    max_connections: int = Field(default=128, ge=1)
    database_timeout_ms: int = Field(default=2000, ge=1)
    max_matches: int = Field(default=4000, ge=1)
    output_bytes: int = Field(default=1048576, ge=1024)
    frame_bytes: int = Field(default=65536, ge=2048)
    websocket_queue_bytes: int = Field(default=2097152, ge=1024)
    websocket_queue_frames: int = Field(default=128, ge=1)
    send_timeout_seconds: float = Field(default=2, gt=0)
    shutdown_timeout_seconds: float = Field(default=10, gt=0)
    openai_api_key: SecretStr | None = None
    assistant_model: str = "gpt-4.1-mini"

    @field_validator("openai_api_key", mode="before")
    @classmethod
    def empty_key(cls, value):
        return value or None

    @model_validator(mode="after")
    def check_budgets(self):
        if self.body_bytes > self.http_buffer_bytes:
            raise ValueError("body_bytes must fit http_buffer_bytes")
        if self.batch_reports > self.ingress_reports:
            raise ValueError("batch_reports must fit ingress_reports")
        if self.frame_bytes > self.output_bytes:
            raise ValueError("frame_bytes must fit output_bytes")
        if self.websocket_queue_bytes < self.output_bytes:
            raise ValueError("a WebSocket queue must fit a complete processing burst")
        minimum = 2 * (self.output_bytes // self.frame_bytes + 1) + 2
        if self.websocket_queue_frames < minimum:
            raise ValueError("WebSocket frame capacity is too small for a maximum burst")
        return self
