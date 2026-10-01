from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2 import Geometry
from sqlalchemy import ColumnElement, cast, func, or_, select

from geo_tracking.api.services import Session, User
from geo_tracking.insights import insights
from geo_tracking.models import DeviceLatest
from geo_tracking.schemas import Latitude, Longitude

router = APIRouter(tags=["devices"])


def bounds(
    south: Latitude | None = None,
    west: Longitude | None = None,
    north: Latitude | None = None,
    east: Longitude | None = None,
) -> ColumnElement[bool] | None:
    edges = (south, west, north, east)
    if all(edge is None for edge in edges):
        return None
    if any(edge is None for edge in edges) or south > north:
        raise HTTPException(422, "bbox_requires_ordered_south_west_north_east")
    spans = [(west, east)] if west <= east else [(west, 180.0), (-180.0, east)]
    return or_(
        *[
            func.ST_Intersects(
                DeviceLatest.position, func.ST_MakeEnvelope(low, south, high, north, 4326)
            )
            for low, high in spans
        ]
    )


@router.get("/devices/latest")
async def latest(
    user: User,
    session: Session,
    area: Annotated[ColumnElement[bool] | None, Depends(bounds)],
    after: str | None = None,
    limit: int = Query(1000, ge=1, le=1000),
) -> dict:
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
    if area is not None:
        query = query.where(area)
    if after:
        query = query.where(DeviceLatest.device_id > after)
    rows = (await session.execute(query)).mappings().all()
    return {
        "items": [dict(row) for row in rows[:limit]],
        "next_cursor": rows[limit - 1]["device_id"] if len(rows) > limit else None,
    }


@router.get("/insights")
async def current_insights(user: User, session: Session) -> dict:
    return await insights(session, user)
