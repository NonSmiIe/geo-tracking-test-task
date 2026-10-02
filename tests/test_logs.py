import logging

import orjson

from geo_tracking.api.context import RequestContext
from geo_tracking.logs import JsonFormatter


def test_a_log_line_is_one_json_object_with_role_and_context() -> None:
    record = logging.makeLogRecord(
        {
            "name": "geo_tracking.processor",
            "levelno": 30,
            "levelname": "WARNING",
            "msg": "batch failed",
        }
    )
    record.partition = 7
    line = orjson.loads(JsonFormatter("processor").format(record))
    assert line["role"] == "processor" and line["level"] == "warning"
    assert line["event"] == "batch failed" and line["partition"] == 7


async def test_lines_logged_while_serving_a_request_carry_its_id() -> None:
    lines: list[dict] = []
    formatter = JsonFormatter("api")

    async def app(scope: dict, receive: object, send: object) -> None:
        record = logging.makeLogRecord({"name": "geo_tracking.api", "msg": "served"})
        lines.append(orjson.loads(formatter.format(record)))

    traced = RequestContext(app)  # type: ignore[arg-type]
    await traced({"type": "http", "headers": [(b"x-request-id", b"edge-7")]}, None, None)  # type: ignore[arg-type]
    await traced({"type": "http", "headers": []}, None, None)  # type: ignore[arg-type]
    assert lines[0]["request_id"] == "edge-7"
    assert "request_id" not in lines[1]
