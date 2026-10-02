import asyncio

import pytest
from sqlalchemy import text

from geo_tracking.db import Database
from geo_tracking.processor import Processor, PublishStalled
from tests.helpers import collect, dashboard, micros, report, silent, wait_for, zone

ALICE = {"X-User-ID": "alice"}
OUTSIDE = 57.5


def recorded(http, **params) -> list[dict]:
    return http.get("/zone-events", headers=ALICE, params=params).json()["items"]


def memberships(settings) -> int:
    async def count() -> int:
        db = Database(settings)
        try:
            async with db.sessions() as session:
                result = await session.execute(text("SELECT count(*) FROM zone_membership"))
                return int(result.scalar_one())
        finally:
            await db.close()

    return asyncio.run(count())


def test_an_entry_and_its_exit_are_recorded_once_with_the_dwell(stack, http) -> None:
    created = zone(http)
    zone(http, owner="bob")
    alice, bob = dashboard(stack, "alice"), dashboard(stack, "bob")
    http.post("/locations", json=report(offset=1))
    entered = collect(alice, "zone_event", 1)[0]
    assert (entered["kind"], entered["zone_id"], entered["timestamp"]) == (
        "entered",
        created["id"],
        micros(1),
    )
    http.post("/locations", json=report(offset=2))
    collect(alice, "positions", 1)
    silent(alice, "zone_event", 0.5)
    http.post("/locations", json=report(offset=4, latitude=OUTSIDE))
    exited = collect(alice, "zone_event", 1)[0]
    assert (exited["kind"], exited["timestamp"], exited["dwell_us"]) == (
        "exited",
        micros(4),
        micros(4) - micros(1),
    )
    assert [event["id"] for event in recorded(http)] == [entered["id"], exited["id"]]
    assert recorded(http, after=entered["id"]) == [exited]
    assert {event["kind"] for event in collect(bob, "zone_event", 2)} == {"entered", "exited"}
    for socket in (alice, bob):
        socket.close()


def test_in_and_out_inside_one_batch_records_both(stack, http) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    http.post("/locations/batch", json=[report(offset=2, latitude=OUTSIDE), report(offset=1)])
    events = collect(socket, "zone_event", 2)
    assert [(event["kind"], event["timestamp"]) for event in events] == [
        ("entered", micros(1)),
        ("exited", micros(2)),
    ]
    assert events[1]["dwell_us"] == 1_000_000
    socket.close()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_a_replay_re_emits_the_recorded_events_and_records_nothing_new(
    stack, http, monkeypatch
) -> None:
    zone(http)
    socket = dashboard(stack, "alice")
    delivered = Processor.publish

    async def stalled(self: Processor, messages: list) -> None:
        raise PublishStalled

    monkeypatch.setattr(Processor, "publish", stalled)
    http.post("/locations/batch", json=[report(offset=1), report(offset=2, latitude=OUTSIDE)])
    assert wait_for(lambda: len(recorded(http)) == 2)
    silent(socket, "zone_event", 0.5)
    monkeypatch.setattr(Processor, "publish", delivered)
    stack.restart_processor()
    assert collect(socket, "zone_event", 2) == recorded(http)
    socket.close()


def test_a_rename_is_silent_a_pause_ends_nothing_and_a_resume_re_enters(stack, http) -> None:
    created = zone(http)
    path = f"/geozones/{created['id']}"
    socket = dashboard(stack, "alice")
    http.post("/locations", json=report(offset=1))
    collect(socket, "zone_event", 1)
    assert http.patch(path, headers=ALICE, json={"name": "Depot"}).json()["version"] == 1
    http.post("/locations", json=report(offset=2))
    collect(socket, "positions", 1)
    silent(socket, "zone_event", 0.5)
    http.patch(path, headers=ALICE, json={"active": False})
    http.post("/locations", json=report(offset=3, latitude=OUTSIDE))
    collect(socket, "positions", 1)
    silent(socket, "zone_event", 0.5)
    http.patch(path, headers=ALICE, json={"active": True})
    http.post("/locations", json=report(offset=4))
    resumed = collect(socket, "zone_event", 1)[0]
    assert (resumed["kind"], resumed["zone_version"]) == ("entered", 3)
    assert [event["kind"] for event in recorded(http)] == ["entered", "entered"]
    socket.close()


def test_deleting_a_zone_drops_its_memberships_and_keeps_its_history(stack, http) -> None:
    created = zone(http)
    http.post("/locations", json=report(offset=1))
    assert wait_for(lambda: memberships(stack.settings) == 1)
    http.delete(f"/geozones/{created['id']}", headers=ALICE)
    assert memberships(stack.settings) == 0
    assert [event["zone_id"] for event in recorded(http)] == [created["id"]]
