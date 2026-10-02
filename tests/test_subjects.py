from geo_tracking.subjects import TILE_LEVEL, Subjects

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
