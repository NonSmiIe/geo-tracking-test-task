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


def spans(west: float, east: float) -> list[tuple[float, float]]:
    return [(west, east)] if west <= east else [(west, 180.0), (-180.0, east)]


class Subjects:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.everywhere = f"{prefix}.pos.>"
        self.tiles: dict[tuple[int, int], str] = {}

    def alerts(self, user_id: str) -> str:
        return f"{self.prefix}.alerts.{user_id.encode().hex()}"

    def zones(self, user_id: str) -> str:
        return f"{self.prefix}.zones.{user_id.encode().hex()}"

    def tile(self, x: int, y: int, level: int, below: bool = False) -> str:
        return ".".join([self.prefix, "pos", *quadkey(x, y, level), *([">"] if below else [])])

    def position(self, latitude: float, longitude: float) -> str:
        key = tile(latitude, longitude)
        subject = self.tiles.get(key)
        if subject is None:
            subject = self.tiles[key] = self.tile(*key, TILE_LEVEL)
        return subject

    def viewport(
        self, south: float, west: float, north: float, east: float, limit: int
    ) -> set[str]:
        for level in range(TILE_LEVEL, -1, -1):
            tiles: set[tuple[int, int]] = set()
            for low, high in spans(west, east):
                x0, y0 = tile(north, low, level)
                x1, y1 = tile(south, high, level)
                if (x1 - x0 + 1) * (y1 - y0 + 1) > limit:
                    break
                tiles.update((x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1))
            else:
                if len(tiles) == 1 << (2 * level):
                    return {self.everywhere}
                if len(tiles) <= limit:
                    return {self.tile(x, y, level, level < TILE_LEVEL) for x, y in tiles}
        return {self.everywhere}
