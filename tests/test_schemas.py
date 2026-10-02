from datetime import UTC, datetime, timedelta

import msgspec
import orjson
import pytest
from pydantic import ValidationError

from geo_tracking.schemas import (
    ID_LENGTH,
    REPORT,
    Flush,
    Report,
    ViewportAdapter,
    ZoneCreate,
    ZoneUpdate,
    batch_decoder,
    frame_decoder,
    microseconds,
)
from tests.helpers import report


def encoded(**changes: object) -> bytes:
    return orjson.dumps(report(**changes))


def test_a_report_keeps_its_timestamp_to_the_microsecond() -> None:
    decoded = REPORT.decode(encoded(timestamp="2026-01-01T02:00:00.123456+02:00"))
    assert decoded.record() == [
        "device-1",
        56.9496,
        24.1052,
        microseconds(datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)),
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(1970, 1, 1, tzinfo=UTC), 0),
        (datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), -1),
        (datetime(2026, 1, 1, 0, 0, 1, 7, tzinfo=UTC), 1_767_225_601_000_007),
    ],
)
def test_microseconds_are_exact_on_both_sides_of_the_epoch(value: datetime, expected: int) -> None:
    assert microseconds(value) == expected


def test_the_clock_skew_allowance_is_thirty_seconds() -> None:
    soon = (datetime.now(UTC) + timedelta(seconds=25)).isoformat()
    later = (datetime.now(UTC) + timedelta(seconds=40)).isoformat()
    assert REPORT.decode(encoded(timestamp=soon)).device_id == "device-1"
    with pytest.raises(msgspec.ValidationError, match="future"):
        REPORT.decode(encoded(timestamp=later))


@pytest.mark.parametrize(
    "changes",
    [
        {"timestamp": "2026-01-01T00:00:00"},
        {"timestamp": "yesterday"},
        {"latitude": 90.0001},
        {"latitude": -90.0001},
        {"longitude": 180.0001},
        {"longitude": -180.0001},
        {"latitude": "56.9"},
        {"device_id": ""},
        {"device_id": "x" * (ID_LENGTH + 1)},
        {"device_id": "has space"},
        {"device_id": "line\n"},
        {"device_id": "a/b"},
        {"device_id": "a*"},
        {"speed": 3},
    ],
)
def test_a_malformed_report_is_refused(changes: dict) -> None:
    with pytest.raises(msgspec.ValidationError):
        REPORT.decode(encoded(**changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"latitude": 90, "longitude": 180},
        {"latitude": -90, "longitude": -180},
        {"device_id": "x" * ID_LENGTH},
        {"device_id": "truck_7.eu-west"},
        {"device_id": "Ünïcode"},
    ],
)
def test_reports_on_the_bounds_are_accepted(changes: dict) -> None:
    assert isinstance(REPORT.decode(encoded(**changes)), Report)


def test_a_batch_holds_one_to_the_configured_number_of_reports() -> None:
    decoder = batch_decoder(3)
    assert len(decoder.decode(orjson.dumps([report(offset=i) for i in range(3)]))) == 3
    for size in (0, 4):
        with pytest.raises(msgspec.ValidationError):
            decoder.decode(orjson.dumps([report(offset=i) for i in range(size)]))
    assert batch_decoder(3) is decoder


def test_a_stream_frame_is_a_batch_or_a_flush() -> None:
    decoder = frame_decoder(2)
    assert isinstance(decoder.decode(b'{"type":"flush"}'), Flush)
    assert len(decoder.decode(orjson.dumps([report()]))) == 1
    for invalid in (b'{"type":"flush","now":1}', b'{"type":"ack"}', b"[]", b"not json"):
        with pytest.raises(msgspec.DecodeError):
            decoder.decode(invalid)


def test_a_new_zone_is_active_unless_said_otherwise() -> None:
    created = ZoneCreate(name="Depot", latitude=0, longitude=0, radius_m=1)
    assert created.active is True
    assert (
        ZoneCreate(name="D", latitude=0, longitude=0, radius_m=500_000, active=False).active
        is False
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"name": ""},
        {"name": "x" * 121},
        {"radius_m": 0},
        {"radius_m": -1},
        {"radius_m": 500_000.5},
        {"radius_m": float("inf")},
        {"latitude": float("nan")},
        {"owner": "bob"},
    ],
)
def test_a_zone_outside_its_bounds_is_refused(changes: dict) -> None:
    with pytest.raises(ValidationError):
        ZoneCreate(**{"name": "Depot", "latitude": 0, "longitude": 0, "radius_m": 1} | changes)


@pytest.mark.parametrize(
    "patch",
    [
        {},
        {"name": None},
        {"active": None},
        {"latitude": 1},
        {"longitude": 1},
        {"latitude": 1, "longitude": None},
        {"version": 3},
    ],
)
def test_an_ambiguous_zone_patch_is_refused(patch: dict) -> None:
    with pytest.raises(ValidationError):
        ZoneUpdate(**patch)


@pytest.mark.parametrize(
    "patch",
    [{"name": "Renamed"}, {"active": False}, {"latitude": 1, "longitude": 2}, {"radius_m": 5}],
)
def test_a_patch_names_exactly_the_fields_it_changes(patch: dict) -> None:
    assert ZoneUpdate(**patch).model_fields_set == set(patch)


def test_a_viewport_must_be_ordered_south_to_north_but_may_wrap_east_to_west() -> None:
    wrapped = ViewportAdapter.validate_json(
        b'{"type":"viewport","south":-20,"west":175,"north":-10,"east":-175}'
    )
    assert wrapped.west > wrapped.east
    for invalid in (
        b'{"type":"viewport","south":10,"west":0,"north":0,"east":1}',
        b'{"type":"viewport","south":0,"west":0,"north":91,"east":1}',
        b'{"type":"zoom","south":0,"west":0,"north":1,"east":1}',
        b'{"type":"viewport","south":0,"west":0,"north":1}',
        b'{"type":"viewport","south":0,"west":0,"north":1,"east":1,"zoom":3}',
    ):
        with pytest.raises(ValidationError):
            ViewportAdapter.validate_json(invalid)
