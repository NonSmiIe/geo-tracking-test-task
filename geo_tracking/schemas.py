from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

Latitude = Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)]
Longitude = Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)]
Radius = Annotated[float, Field(gt=0, allow_inf_nan=False)]
Identifier = Annotated[str, Field(min_length=1, max_length=96, pattern=r"^[\w.-]+$")]

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def microseconds(value: datetime) -> int:
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


class Report(BaseModel):
    model_config = ConfigDict(extra="forbid")
    device_id: Identifier
    latitude: Latitude
    longitude: Longitude
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp requires a timezone")
        return value.astimezone(UTC)

    def record(self) -> list:
        return [self.device_id, self.latitude, self.longitude, microseconds(self.timestamp)]


ReportAdapter = TypeAdapter(Report)


def batch_adapter(limit: int) -> TypeAdapter:
    return TypeAdapter(Annotated[list[Report], Field(min_length=1, max_length=limit)])


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
