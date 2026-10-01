import math

TILE_LEVEL = 8
MAX_LATITUDE = 85.05112878


def tile(latitude: float, longitude: float, level: int = TILE_LEVEL) -> tuple[int, int]:
    count = 1 << level
    latitude = max(-MAX_LATITUDE, min(MAX_LATITUDE, latitude))
    x = int((longitude + 180) / 360 * count)
    sin = math.sin(math.radians(latitude))
    y = int((0.5 - math.log((1 + sin) / (1 - sin)) / (4 * math.pi)) * count)
    return min(count - 1, max(0, x)), min(count - 1, max(0, y))


def quadkey(x: int, y: int, level: int) -> list[str]:
    digits = []
    for bit in range(level, 0, -1):
        mask = 1 << (bit - 1)
        digits.append(str((1 if x & mask else 0) + (2 if y & mask else 0)))
    return digits


def position_subject(prefix: str, latitude: float, longitude: float) -> str:
    return ".".join([prefix, "pos", *quadkey(*tile(latitude, longitude), TILE_LEVEL)])


def viewport_subjects(
    prefix: str, south: float, west: float, north: float, east: float, limit: int
) -> set[str]:
    spans = [(west, east)] if west <= east else [(west, 180.0), (-180.0, east)]
    for level in range(TILE_LEVEL, -1, -1):
        tiles = set()
        for low, high in spans:
            x0, y0 = tile(north, low, level)
            x1, y1 = tile(south, high, level)
            if (x1 - x0 + 1) * (y1 - y0 + 1) > limit:
                break
            tiles.update((x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1))
        else:
            if len(tiles) == 1 << (2 * level):
                return {f"{prefix}.pos.>"}
            if len(tiles) <= limit:
                suffix = [] if level == TILE_LEVEL else [">"]
                return {".".join([prefix, "pos", *quadkey(x, y, level), *suffix]) for x, y in tiles}
    return {f"{prefix}.pos.>"}
