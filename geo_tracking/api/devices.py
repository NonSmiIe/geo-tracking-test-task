from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2 import Geometry
from sqlalchemy import ColumnElement, and_, any_, cast, func, or_, select, text

from geo_tracking.api.services import Session, User, page
from geo_tracking.insights import insights
from geo_tracking.models import DeviceLatest
from geo_tracking.schemas import Latitude, Longitude
from geo_tracking.subjects import spans

router = APIRouter(tags=["devices"])


Envelope = ColumnElement[object]


def bounds(
    south: Latitude | None = None,
    west: Longitude | None = None,
    north: Latitude | None = None,
    east: Longitude | None = None,
) -> list[Envelope]:
    edges = (south, west, north, east)
    if all(edge is None for edge in edges):
        return []
    if south is None or west is None or north is None or east is None or south > north:
        raise HTTPException(422, "bbox_requires_ordered_south_west_north_east")
    return [func.ST_MakeEnvelope(low, south, high, north, 4326) for low, high in spans(west, east)]


def area(envelopes: list[Envelope]) -> ColumnElement[bool]:
    return or_(
        *[
            and_(
                or_(
                    func.grid_cells(item).is_(None),
                    DeviceLatest.cell == any_(func.grid_cells(item)),
                ),
                func.ST_Intersects(DeviceLatest.position, item),
            )
            for item in envelopes
        ]
    )


@router.get("/devices/latest")
async def latest(
    user: User,
    session: Session,
    envelopes: Annotated[list[Envelope], Depends(bounds)],
    after: str | None = None,
    limit: int = Query(1000, ge=1, le=1000),
) -> dict[str, Any]:
    point = cast(DeviceLatest.position, Geometry("POINT", srid=4326))
    query = (
        select(
            DeviceLatest.device_id,
            DeviceLatest.reported_at.label("timestamp"),
            func.ST_Y(point).label("latitude"),
            func.ST_X(point).label("longitude"),
        )
        .order_by(DeviceLatest.device_id)
        .limit(limit + 1)
    )
    if envelopes:
        await session.execute(text("SET LOCAL plan_cache_mode = force_custom_plan"))
        query = query.where(area(envelopes))
    if after:
        query = query.where(DeviceLatest.device_id > after)
    return page((await session.execute(query)).mappings().all(), limit, "device_id")


@router.get("/insights")
async def current_insights(user: User, session: Session) -> dict[str, Any]:
    return await insights(session, user)
