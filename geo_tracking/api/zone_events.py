from typing import Annotated, Any

from fastapi import APIRouter, Query
from sqlalchemy import text

from geo_tracking.api.services import Session, User
from geo_tracking.spatial import EVENT_COLUMNS, zone_event

router = APIRouter(prefix="/zone-events", tags=["zone events"])

FOLLOW_SQL = text(f"""
SELECT {EVENT_COLUMNS} FROM zone_events
WHERE user_id = :user AND id > :after
ORDER BY id LIMIT :limit
""")

TAIL_SQL = text(f"""
SELECT * FROM (
    SELECT {EVENT_COLUMNS} FROM zone_events WHERE user_id = :user ORDER BY id DESC LIMIT :limit
) AS newest
ORDER BY id
""")


@router.get("")
async def list_zone_events(
    user: User,
    session: Session,
    after: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> dict[str, Any]:
    if after is None:
        rows = await session.execute(TAIL_SQL, {"user": user, "limit": limit})
    else:
        rows = await session.execute(FOLLOW_SQL, {"user": user, "after": after, "limit": limit})
    items = [zone_event(row) for row in rows.mappings()]
    return {"items": items, "cursor": items[-1]["id"] if items else after}
