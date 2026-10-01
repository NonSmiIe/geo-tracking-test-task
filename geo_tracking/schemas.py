from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Annotated, Any, Literal, Self

import msgspec
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

Latitude = Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)]
Longitude = Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)]
MAX_RADIUS_M = 500_000
Radius = Annotated[float, Field(gt=0, le=MAX_RADIUS_M, allow_inf_nan=False)]
IDENTIFIER = r"^[\w.-]+$"
Identifier = Annotated[str, Field(min_length=1, max_length=96, pattern=IDENTIFIER)]

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CLOCK_SKEW = timedelta(minutes=5)


def microseconds(value: datetime) -> int:
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


class Report(msgspec.Struct, forbid_unknown_fields=True, frozen=True):
    device_id: Annotated[str, msgspec.Meta(min_length=1, max_length=96, pattern=r"^[\w.-]+\Z")]
    latitude: Annotated[float, msgspec.Meta(ge=-90, le=90)]
    longitude: Annotated[float, msgspec.Meta(ge=-180, le=180)]
    timestamp: Annotated[datetime, msgspec.Meta(tz=True)]

    def __post_init__(self) -> None:
        if self.timestamp > datetime.now(UTC) + CLOCK_SKEW:
            raise ValueError("timestamp is in the future")

    def record(self) -> list[Any]:
        return [self.device_id, self.latitude, self.longitude, microseconds(self.timestamp)]


class Flush(msgspec.Struct, tag_field="type", tag="flush", forbid_unknown_fields=True):
    pass


def reports(limit: int) -> Any:
    return Annotated[list[Report], msgspec.Meta(min_length=1, max_length=limit)]


REPORT = msgspec.json.Decoder(Report)


@cache
def batch_decoder(limit: int) -> msgspec.json.Decoder[Any]:
    return msgspec.json.Decoder(reports(limit))


@cache
def frame_decoder(limit: int) -> msgspec.json.Decoder[Any]:
    return msgspec.json.Decoder(reports(limit) | Flush)


class ZoneCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    latitude: Latitude
    longitude: Longitude
    radius_m: Radius
    active: bool = True


class ZoneUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=120)
    latitude: Latitude | None = None
    longitude: Longitude | None = None
    radius_m: Radius | None = None
    active: bool | None = None

    @model_validator(mode="after")
    def valid_patch(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("provide at least one field")
        if any(getattr(self, key) is None for key in self.model_fields_set):
            raise ValueError("zone fields cannot be null")
        if ("latitude" in self.model_fields_set) != ("longitude" in self.model_fields_set):
            raise ValueError("update latitude and longitude together")
        return self


class Viewport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["viewport"]
    south: Latitude
    west: Longitude
    north: Latitude
    east: Longitude

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.south > self.north:
            raise ValueError("south must not exceed north")
        return self


ViewportAdapter = TypeAdapter(Viewport)
