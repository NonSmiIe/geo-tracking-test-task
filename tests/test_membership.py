from uuid import uuid4

from geo_tracking.membership import Inside, Sample, walk

DEPOT, YARD = uuid4(), uuid4()


def sample(at: int, *zones, device: str = "truck") -> Sample:
    return Sample(device, at, (0, at), {zone: ("alice", 1) for zone in zones})


def test_samples_are_walked_in_time_order_whatever_the_batch_order() -> None:
    moved = walk({}, set(), [sample(3), sample(1, DEPOT), sample(2, DEPOT, YARD)])
    assert [(event.kind, event.zone_id, event.at_us) for event in moved.transitions] == [
        ("entered", DEPOT, 1),
        ("entered", YARD, 2),
        ("exited", DEPOT, 3),
        ("exited", YARD, 3),
    ]
    assert [event.dwell_us for event in moved.transitions if event.kind == "exited"] == [2, 1]
    assert moved.entered == {} and moved.left == set()


def test_a_held_zone_is_left_and_a_stale_row_is_replaced_or_dropped() -> None:
    held = {"truck": {DEPOT: Inside("alice", 1, 0)}}
    moved = walk(held, {("truck", YARD), ("van", YARD)}, [sample(5, YARD)])
    assert [(event.kind, event.zone_id) for event in moved.transitions] == [
        ("exited", DEPOT),
        ("entered", YARD),
    ]
    assert moved.entered == {("truck", YARD): Inside("alice", 1, 5)}
    assert moved.left == {("truck", DEPOT), ("van", YARD)}


def test_staying_inside_changes_nothing() -> None:
    held = {"truck": {DEPOT: Inside("alice", 1, 0)}}
    moved = walk(held, set(), [sample(5, DEPOT), sample(10, DEPOT)])
    assert moved.transitions == [] and moved.entered == {} and moved.left == set()
