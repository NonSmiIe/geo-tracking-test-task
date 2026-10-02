from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import Select, cast, func, select, text

from geo_tracking.api.services import ServicesDep, Session, User, page
from geo_tracking.models import Zone
from geo_tracking.schemas import ZoneCreate, ZoneUpdate

router = APIRouter(prefix="/geozones", tags=["geozones"])


def zone_query() -> Select[Any]:
    point = cast(Zone.center, Geometry("POINT", srid=4326))
    return select(
        Zone.id,
        Zone.name,
        Zone.radius_m,
        Zone.active,
        Zone.version,
        func.ST_Y(point).label("latitude"),
        func.ST_X(point).label("longitude"),
    )


def center(latitude: float, longitude: float) -> WKTElement:
    return WKTElement(f"POINT({longitude} {latitude})", srid=4326)


async def owned(session: Session, zone_id: UUID, user: str) -> Zone:
    zone = (
        await session.execute(
            select(Zone).where(Zone.id == zone_id, Zone.user_id == user).with_for_update()
        )
    ).scalar_one_or_none()
    if zone is None:
        raise HTTPException(404, "zone_not_found")
    return zone


OVERLAP_SQL = text("""
SELECT count(*) FROM geozones AS other, geozones AS zone
WHERE zone.id = :zone_id AND other.user_id = zone.user_id AND other.id <> zone.id
  AND other.active AND other.footprint && zone.footprint
  AND ST_DWithin(other.center, zone.center, other.radius_m + zone.radius_m)
""")


async def bound_overlap(session: Session, zone: Zone, limit: int) -> None:
    if not zone.active:
        return
    overlapping = (await session.execute(OVERLAP_SQL, {"zone_id": zone.id})).scalar_one()
    if overlapping > limit:
        raise HTTPException(409, "zone_overlap_exceeded")


async def present(session: Session, zone_id: UUID, user: str) -> dict[str, Any]:
    query = zone_query().where(Zone.id == zone_id, Zone.user_id == user)
    row = (await session.execute(query)).mappings().one_or_none()
    if row is None:
        raise HTTPException(404, "zone_not_found")
    return dict(row)


async def lock_owner(session: Session, user: str) -> None:
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:user))"), {"user": user})


@router.post("", status_code=201)
async def create_zone(
    payload: ZoneCreate, user: User, session: Session, services: ServicesDep
) -> dict[str, Any]:
    async with session.begin():
        await lock_owner(session, user)
        total = (
            await session.execute(select(func.count()).where(Zone.user_id == user))
        ).scalar_one()
        if total >= services.settings.max_zones_per_user:
            raise HTTPException(409, "zone_quota_exceeded")
        zone = Zone(
            user_id=user,
            name=payload.name,
            radius_m=payload.radius_m,
            active=payload.active,
            center=center(payload.latitude, payload.longitude),
        )
        session.add(zone)
        await session.flush()
        await bound_overlap(session, zone, services.settings.max_zone_overlap)
        row = await present(session, zone.id, user)
    await services.zones_changed(user)
    return row


@router.get("")
async def list_zones(
    user: User,
    session: Session,
    after: UUID | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    query = zone_query().where(Zone.user_id == user).order_by(Zone.id).limit(limit + 1)
    if after:
        query = query.where(Zone.id > after)
    return page((await session.execute(query)).mappings().all(), limit, "id")


@router.get("/{zone_id}")
async def get_zone(zone_id: UUID, user: User, session: Session) -> dict[str, Any]:
    return await present(session, zone_id, user)


@router.patch("/{zone_id}")
async def update_zone(
    zone_id: UUID, payload: ZoneUpdate, user: User, session: Session, services: ServicesDep
) -> dict[str, Any]:
    async with session.begin():
        await lock_owner(session, user)
        zone = await owned(session, zone_id, user)
        values = payload.model_dump(exclude_unset=True)
        if "latitude" in values:
            zone.center = center(values.pop("latitude"), values.pop("longitude"))
        for key, value in values.items():
            setattr(zone, key, value)
        zone.version += 1
        await session.flush()
        await bound_overlap(session, zone, services.settings.max_zone_overlap)
        row = await present(session, zone.id, user)
    await services.zones_changed(user)
    return row


@router.delete("/{zone_id}", status_code=204)
async def delete_zone(zone_id: UUID, user: User, session: Session, services: ServicesDep) -> None:
    async with session.begin():
        await session.delete(await owned(session, zone_id, user))
    await services.zones_changed(user)
