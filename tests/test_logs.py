import logging

import orjson

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
