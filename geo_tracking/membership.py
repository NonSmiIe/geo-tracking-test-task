from dataclasses import dataclass, field
from uuid import UUID

Key = tuple[str, UUID]
Origin = tuple[int, int]


@dataclass(frozen=True)
class Inside:
    user_id: str
    version: int
    entered_us: int


@dataclass(frozen=True)
class Sample:
    device_id: str
    at_us: int
    origin: Origin
    matched: dict[UUID, tuple[str, int]]


@dataclass(frozen=True)
class Transition:
    kind: str
    device_id: str
    zone_id: UUID
    user_id: str
    zone_version: int
    at_us: int
    dwell_us: int | None
    origin: Origin


@dataclass
class Walk:
    transitions: list[Transition] = field(default_factory=list)
    entered: dict[Key, Inside] = field(default_factory=dict)
    left: set[Key] = field(default_factory=set)


def walk(held: dict[str, dict[UUID, Inside]], stale: set[Key], samples: list[Sample]) -> Walk:
    state = {device_id: dict(zones) for device_id, zones in held.items()}
    result = Walk()
    for sample in sorted(samples, key=lambda sample: (sample.device_id, sample.at_us)):
        inside = state.setdefault(sample.device_id, {})
        for zone_id in [zone_id for zone_id in inside if zone_id not in sample.matched]:
            was = inside.pop(zone_id)
            result.transitions.append(
                Transition(
                    "exited",
                    sample.device_id,
                    zone_id,
                    was.user_id,
                    was.version,
                    sample.at_us,
                    sample.at_us - was.entered_us,
                    sample.origin,
                )
            )
        for zone_id, (user_id, version) in sample.matched.items():
            if zone_id not in inside:
                inside[zone_id] = Inside(user_id, version, sample.at_us)
                result.transitions.append(
                    Transition(
                        "entered",
                        sample.device_id,
                        zone_id,
                        user_id,
                        version,
                        sample.at_us,
                        None,
                        sample.origin,
                    )
                )
    for device_id, zones in state.items():
        before = held.get(device_id, {})
        for zone_id, now in zones.items():
            if before.get(zone_id) != now:
                result.entered[device_id, zone_id] = now
        result.left.update((device_id, zone_id) for zone_id in before if zone_id not in zones)
    result.left |= stale - result.entered.keys()
    return result
