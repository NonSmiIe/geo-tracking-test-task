from dataclasses import dataclass

import orjson


class OutputOverload(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Frame:
    data: bytes

    @property
    def size(self) -> int:
        return len(self.data)


def frames(kind: str, items: list[dict], limit: int) -> tuple[Frame, ...]:
    prefix = b'{"type":' + orjson.dumps(kind) + b',"items":['
    chunks = []
    parts = []
    size = len(prefix) + 2
    for item in items:
        encoded = orjson.dumps(item, option=orjson.OPT_UTC_Z)
        if len(encoded) > limit // 2:
            raise OutputOverload("fanout_budget_exceeded")
        if size + len(encoded) + bool(parts) > limit:
            data = prefix + b",".join(parts) + b"]}"
            chunks.append(Frame(data))
            parts, size = [], len(prefix) + 2
        size += len(encoded) + bool(parts)
        parts.append(encoded)
    if parts:
        data = prefix + b",".join(parts) + b"]}"
        chunks.append(Frame(data))
    return tuple(chunks)
