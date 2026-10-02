from datetime import datetime
from uuid import UUID, uuid4

from geoalchemy2 import Geography, Geometry, WKBElement, WKTElement
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    Index,
    Integer,
    String,
)
from sqlalchemy import text as sql
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from geo_tracking.schemas import ID_LENGTH, MAX_RADIUS_M


class Base(DeclarativeBase):
    pass


class Zone(Base):
    __tablename__ = "geozones"
    __table_args__ = (
        CheckConstraint(f"radius_m > 0 AND radius_m <= {MAX_RADIUS_M}", name="bounded_radius"),
        Index("geozones_owner", "user_id"),
        Index(
            "geozones_active_footprint",
            "footprint",
            postgresql_using="gist",
            postgresql_where=sql("active"),
        ),
    )
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[str] = mapped_column(String(ID_LENGTH))
    name: Mapped[str] = mapped_column(String(120))
    center: Mapped[WKBElement | WKTElement] = mapped_column(
        Geography("POINT", srid=4326, spatial_index=False)
    )
    radius_m: Mapped[float] = mapped_column(Float)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    footprint: Mapped[WKBElement] = mapped_column(
        Geometry("GEOMETRY", srid=4326, spatial_index=False),
        Computed("zone_footprint(center, radius_m)", persisted=True),
    )


class DeviceLatest(Base):
    __tablename__ = "device_latest"
    __table_args__ = (Index("device_latest_cell", "cell"),)
    device_id: Mapped[str] = mapped_column(String(ID_LENGTH, collation="C"), primary_key=True)
    position: Mapped[WKBElement] = mapped_column(Geometry("POINT", srid=4326, spatial_index=False))
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    cell: Mapped[int] = mapped_column(Integer, Computed("grid_cell(position)", persisted=True))


class ConsumerProgress(Base):
    __tablename__ = "consumer_progress"
    topic_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    persisted: Mapped[int] = mapped_column(BigInteger)
