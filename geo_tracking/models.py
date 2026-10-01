from datetime import datetime
from uuid import UUID, uuid4

from geoalchemy2 import Geography
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    Index,
    Integer,
    SmallInteger,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Zone(Base):
    __tablename__ = "geozones"
    __table_args__ = (
        CheckConstraint("radius_m > 0 AND radius_m < 'Infinity'::float8", name="finite_radius"),
        Index("geozones_owner", "user_id"),
        Index(
            "geozones_active_spatial",
            "radius_bucket",
            "center",
            postgresql_using="gist",
            postgresql_where=text("active"),
        ),
        Index("geozones_active_bucket", "radius_bucket", postgresql_where=text("active")),
    )
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[str] = mapped_column(String(96))
    name: Mapped[str] = mapped_column(String(120))
    center: Mapped[object] = mapped_column(Geography("POINT", srid=4326, spatial_index=False))
    radius_m: Mapped[float] = mapped_column(Float)
    radius_bucket: Mapped[int] = mapped_column(
        SmallInteger,
        Computed("least(25, greatest(0, floor(ln(radius_m)/ln(2.0))::int + 1))", persisted=True),
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    version: Mapped[int] = mapped_column(Integer, default=1)


class DeviceLatest(Base):
    __tablename__ = "device_latest"
    __table_args__ = (Index("device_latest_position", "position", postgresql_using="gist"),)
    device_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    position: Mapped[object] = mapped_column(Geography("POINT", srid=4326, spatial_index=False))
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
