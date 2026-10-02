import asyncio
from time import time
from typing import Any

import pytest
from aiokafka import TopicPartition

from geo_tracking.processor import Processor, Stalled
from geo_tracking.settings import Settings

PARTITION = TopicPartition("reports", 0)


class Fetch:
    def __init__(self, result: dict[Any, Any] | None = None, hold: bool = True) -> None:
        self.result = result or {}
        self.fetching = asyncio.Event()
        self.release = asyncio.Event()
        if not hold:
            self.release.set()

    async def getmany(self, **options: Any) -> dict[Any, Any]:
        self.fetching.set()
        await self.release.wait()
        await asyncio.sleep(0.01)
        return self.result


def processor(consumer: Any) -> Processor:
    return Processor(Settings(publish_deadline_seconds=1), None, None, consumer, "t")  # type: ignore[arg-type]


async def test_a_processor_that_stops_polling_goes_down() -> None:
    stalled = processor(Fetch())
    await stalled.on_partitions_assigned({PARTITION})
    stalled.polled = time() - 60
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
    await handing.on_partitions_assigned({PARTITION})
    handing.begin()
    revoked = asyncio.create_task(handing.on_partitions_revoked({PARTITION}))
    await asyncio.sleep(0.1)
    assert not revoked.done()
    handing.end()
    await asyncio.wait_for(revoked, 1)


async def test_records_fetched_across_a_revoke_are_left_to_the_new_owner() -> None:
    consumer = Fetch({PARTITION: ["record"]})
    leaving = processor(consumer)

    async def write(fetched: Any) -> Any:
        raise AssertionError("a revoked partition was written")

    leaving.write = write  # type: ignore[method-assign]
    await leaving.on_partitions_assigned({PARTITION})
    consuming = asyncio.create_task(leaving.consume())
    await consumer.fetching.wait()
    await leaving.on_partitions_revoked({PARTITION})
    consumer.release.set()
    await asyncio.sleep(0.1)
    assert not consuming.done()
    assert leaving.inflight == 0
    consuming.cancel()
