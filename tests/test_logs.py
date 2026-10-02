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


def test_context_with_integer_keys_is_still_one_json_line() -> None:
    record = logging.makeLogRecord({"name": "geo_tracking.processor", "msg": "batch failed"})
    record.offsets = {3: 120, 7: 44}
    assert orjson.loads(JsonFormatter("processor").format(record))["offsets"] == {"3": 120, "7": 44}


def test_every_outcome_series_exists_before_its_first_increment() -> None:
    from geo_tracking.metrics import PROCESSOR

    for outcome in ("committed", "failed"):
        assert (
            PROCESSOR.get_sample_value("fleet_processor_batches_total", {"outcome": outcome})
            is not None
        )
