import random

import pytest

from geo_tracking.subjects import TILE_LEVEL, Subjects, quadkey, spans, tile

FLEET = Subjects("fleet")


def matches(pattern: str, subject: str) -> bool:
    wanted, tokens = pattern.split("."), subject.split(".")
    if wanted[-1] == ">":
        return tokens[: len(wanted) - 1] == wanted[:-1] and len(tokens) >= len(wanted)
    return wanted == tokens


def covered(patterns: set[str], latitude: float, longitude: float) -> bool:
    return any(matches(p, FLEET.position(latitude, longitude)) for p in patterns)


def test_city_viewport_uses_finest_tiles_and_excludes_far_points() -> None:
    patterns = FLEET.viewport(56.9, 24.0, 57.0, 24.2, 16)
    assert all(len(p.split(".")) == 2 + TILE_LEVEL for p in patterns)
    assert covered(patterns, 56.95, 24.1)
    assert not covered(patterns, 10, 24.1)


def test_wide_viewport_coarsens_to_bounded_prefixes() -> None:
    patterns = FLEET.viewport(40, -10, 70, 40, 16)
    assert len(patterns) <= 16 and all(p.endswith(".>") for p in patterns)
    assert covered(patterns, 56.95, 24.1) and covered(patterns, 48.85, 2.35)


def test_antimeridian_viewport_covers_both_sides() -> None:
    patterns = FLEET.viewport(-20, 175, -10, -175, 16)
    assert covered(patterns, -15, 179.5) and covered(patterns, -15, -179.5)
    assert not covered(patterns, -15, 0)


def test_world_viewport_and_poles() -> None:
    assert FLEET.viewport(-90, -180, 90, 180, 16) == {"fleet.pos.>"}
    patterns = FLEET.viewport(89, -1, 90, 1, 16)
    assert covered(patterns, 89.9, 0)


def test_a_position_subject_is_built_once_per_tile() -> None:
    subjects = Subjects("cache")
    first = subjects.position(56.95, 24.1)
    assert subjects.position(56.9501, 24.1001) is first
    assert subjects.position(10, 10) != first and len(subjects.tiles) == 2


def test_user_subjects_are_one_token_whatever_the_id() -> None:
    for user in ("alice", "a.b", "*", ">", "ünï code"):
        alerts, zones = FLEET.alerts(user), FLEET.zones(user)
        assert alerts.split(".")[:2] == ["fleet", "alerts"] and len(alerts.split(".")) == 3
        assert zones == alerts.replace(".alerts.", ".zones.")
        assert bytes.fromhex(alerts.rsplit(".", 1)[1]).decode() == user


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected"),
    [
        (90, 180, (255, 0)),
        (-90, -180, (0, 255)),
        (0, 0, (128, 128)),
        (85.06, -180, (0, 0)),
        (-0.0001, -0.0001, (127, 128)),
    ],
)
def test_tiles_clamp_to_the_mercator_square(latitude, longitude, expected) -> None:
    assert tile(latitude, longitude) == expected


def test_a_quadkey_interleaves_one_digit_per_level() -> None:
    assert quadkey(0b011, 0b101, 3) == ["2", "1", "3"]
    assert quadkey(0, 0, 0) == []
    assert (
        FLEET.tile(1, 0, 1) == "fleet.pos.1" and FLEET.tile(1, 0, 1, below=True) == "fleet.pos.1.>"
    )


def test_a_wrapping_span_splits_at_the_antimeridian() -> None:
    assert spans(-10, 10) == [(-10, 10)]
    assert spans(170, -170) == [(170, 180.0), (-180.0, -170)]


def test_every_viewport_covers_its_points_within_the_subscription_limit() -> None:
    rng = random.Random(8)
    for _ in range(400):
        south = rng.uniform(-85, 84)
        north = rng.uniform(south, min(85, south + rng.choice((0.01, 1, 20, 90))))
        west = rng.uniform(-180, 180)
        east = (west + 180 + rng.choice((0.01, 2, 45, 200, 350))) % 360 - 180
        limit = rng.randint(1, 32)
        patterns = FLEET.viewport(south, west, north, east, limit)
        assert len(patterns) <= limit or patterns == {FLEET.everywhere}
        for _ in range(20):
            low, high = rng.choice(spans(west, east))
            point = rng.uniform(south, north), rng.uniform(low, high)
            assert covered(patterns, *point), (south, west, north, east, limit, point)
