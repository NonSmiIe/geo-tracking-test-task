import asyncio
from time import time
from typing import Any

import orjson
import pytest
from aiokafka import TopicPartition

from geo_tracking.processor import Processor, Stalled, frames
from geo_tracking.settings import Settings

PARTITION = TopicPartition("reports", 0)


class Fetch:
    def __init__(self, hold: bool = True) -> None:
        self.release = asyncio.Event()
        if not hold:
            self.release.set()

    async def getmany(self, **options: Any) -> dict[Any, Any]:
        await self.release.wait()
        await asyncio.sleep(0.01)
        return {}


def processor(consumer: Any) -> Processor:
    return Processor(Settings(publish_deadline_seconds=1), None, None, consumer, "t")  # type: ignore[arg-type]


async def test_a_processor_that_stops_polling_goes_down() -> None:
    stalled = processor(Fetch())
    asyncio.get_running_loop().call_later(0.2, setattr, stalled, "polled", time() - 60)
    with pytest.raises(Stalled):
        await asyncio.wait_for(stalled.run(asyncio.Event()), 2)


async def test_a_processor_that_keeps_polling_stays_up() -> None:
    healthy = processor(Fetch(hold=False))
    await healthy.on_partitions_assigned({PARTITION})
    stopping = asyncio.Event()
    asyncio.get_running_loop().call_later(0.5, stopping.set)
    await asyncio.wait_for(healthy.run(stopping), 2)
    assert healthy.owned == set()


async def test_revoke_waits_for_the_batch_in_flight() -> None:
    handing = processor(Fetch())
    handing.start()
    await handing.on_partitions_assigned({PARTITION})
    handing.begin()
    revoked = asyncio.create_task(handing.on_partitions_revoked({PARTITION}))
    await asyncio.sleep(0.1)
    assert not revoked.done()
    handing.end()
    await asyncio.wait_for(revoked, 1)
    await handing.halt()


async def test_a_handover_past_the_deadline_drops_the_batch_in_flight() -> None:
    stuck = processor(Fetch())
    stuck.start()
    before = list(stuck.stages)
    stuck.begin()
    await asyncio.wait_for(stuck.on_partitions_revoked({PARTITION}), 2)
    assert all(stage.cancelled() for stage in before)
    assert stuck.inflight == 0 and stuck.settled.is_set()
    assert not any(stage.done() for stage in stuck.stages)
    await stuck.halt()


def test_frames_are_bounded_by_bytes_and_keep_every_item() -> None:
    items = [[f"device-{index:05d}", 56.9, 24.1, 1_790_000_000 + index] for index in range(5000)]
    payloads = frames("positions", items, 16384)
    decoded = [orjson.loads(payload) for payload in payloads]
    assert len(payloads) > 1
    assert all(len(payload) <= 16384 + 64 for payload in payloads)
    assert {frame["type"] for frame in decoded} == {"positions"}
    assert [item for frame in decoded for item in frame["items"]] == items
