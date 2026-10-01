import asyncio
from time import time

import pytest
from aiokafka import TopicPartition

from geo_tracking.processor import LaneStalled, Processor
from geo_tracking.settings import Settings


async def test_a_lane_that_stops_polling_takes_the_processor_down() -> None:
    processor = Processor(
        Settings(publish_deadline_seconds=1),
        None,
        None,
        None,
        "t",  # type: ignore[arg-type]
    )
    processor.polls[TopicPartition("reports", 0)] = time() - 60
    with pytest.raises(LaneStalled):
        await asyncio.wait_for(processor.run(asyncio.Event()), 2)


async def test_lanes_that_keep_polling_keep_the_processor_up() -> None:
    processor = Processor(
        Settings(publish_deadline_seconds=1),
        None,
        None,
        None,
        "t",  # type: ignore[arg-type]
    )
    processor.polls[TopicPartition("reports", 0)] = time()
    stopping = asyncio.Event()
    asyncio.get_running_loop().call_later(0.5, stopping.set)
    await asyncio.wait_for(processor.run(stopping), 2)
