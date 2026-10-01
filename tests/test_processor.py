import asyncio
from time import time
from typing import Any

import pytest
from aiokafka import TopicPartition

from geo_tracking.processor import Lane, LaneStalled, Processor
from geo_tracking.settings import Settings

PARTITION = TopicPartition("reports", 0)


class HeldFetch:
    def __init__(self) -> None:
        self.fetching = asyncio.Event()
        self.release = asyncio.Event()

    async def getmany(self, *partitions: TopicPartition, **options: Any) -> dict[Any, Any]:
        self.fetching.set()
        await self.release.wait()
        return {}


def processor(consumer: Any = None) -> Processor:
    return Processor(Settings(publish_deadline_seconds=1), None, None, consumer, "t")  # type: ignore[arg-type]


def idle_lane(polled: float) -> Lane:
    lane = Lane(PARTITION, polled=polled)
    lane.task = asyncio.create_task(asyncio.Event().wait())
    return lane


async def test_a_lane_that_stops_polling_takes_the_processor_down() -> None:
    stalled = processor()
    stalled.lanes[PARTITION] = idle_lane(time() - 60)
    with pytest.raises(LaneStalled):
        await asyncio.wait_for(stalled.run(asyncio.Event()), 2)
    stalled.lanes[PARTITION].task.cancel()


async def test_lanes_that_keep_polling_keep_the_processor_up() -> None:
    healthy = processor()
    healthy.lanes[PARTITION] = lane = idle_lane(time())
    stopping = asyncio.Event()
    asyncio.get_running_loop().call_later(0.5, stopping.set)
    await asyncio.wait_for(healthy.run(stopping), 3)
    assert lane.task.cancelled()


async def test_a_lane_revoked_mid_fetch_leaves_nothing_for_the_watchdog() -> None:
    consumer = HeldFetch()
    revoking = processor(consumer)
    await revoking.on_partitions_assigned({PARTITION})
    await consumer.fetching.wait()
    revoked = asyncio.create_task(revoking.on_partitions_revoked({PARTITION}))
    await asyncio.sleep(0)
    consumer.release.set()
    await revoked
    assert revoking.lanes == {}
