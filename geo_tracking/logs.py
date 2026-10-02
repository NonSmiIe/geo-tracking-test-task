import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

import orjson

REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)
RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def __init__(self, role: str) -> None:
        super().__init__()
        self.role = role

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "role": self.role,
            "logger": record.name,
            "event": record.getMessage(),
        }
        if (request_id := REQUEST_ID.get()) is not None:
            entry["request_id"] = request_id
        entry.update({k: v for k, v in record.__dict__.items() if k not in RECORD_FIELDS})
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return orjson.dumps(entry, default=str, option=orjson.OPT_NON_STR_KEYS).decode()


def configure(role: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(role))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "aiokafka", "nats"):
        logger = logging.getLogger(name)
        logger.handlers[:] = []
        logger.propagate = True
        logger.setLevel(logging.WARNING)
