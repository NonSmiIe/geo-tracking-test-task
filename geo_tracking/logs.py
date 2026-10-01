import logging
from datetime import UTC, datetime
from typing import Any

import orjson

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
        entry.update({k: v for k, v in record.__dict__.items() if k not in RECORD_FIELDS})
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return orjson.dumps(entry, default=str).decode()


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
