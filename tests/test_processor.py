import asyncio
from time import monotonic
from typing import Any

import orjson
import pytest
from aiokafka import TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from nats.errors import ConnectionClosedError

from geo_tracking.processor import Batch, Processor, PublishStalled, Stalled, frames
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
    asyncio.get_running_loop().call_later(0.2, setattr, stalled, "polled", monotonic() - 60)
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
    payloads = frames("positions", [orjson.dumps(item) for item in items], 16384)
    decoded = [orjson.loads(payload) for payload in payloads]
    assert len(payloads) > 1
    assert all(len(payload) <= 16384 + 64 for payload in payloads)
    assert {frame["type"] for frame in decoded} == {"positions"}
    assert [item for frame in decoded for item in frame["items"]] == items


def test_an_item_larger_than_the_frame_still_ships_alone() -> None:
    big, small = orjson.dumps(["x" * 4000, 0, 0, 1]), orjson.dumps(["y", 0, 0, 1])
    payloads = frames("positions", [small, big, small], 1024)
    assert [len(orjson.loads(payload)["items"]) for payload in payloads] == [1, 1, 1]
    assert frames("positions", [], 1024) == []


class Nats:
    def __init__(self, failures: dict[str, int]) -> None:
        self.failures = failures
        self.published: list[tuple[str, bytes]] = []

    def fail(self, step: str) -> None:
        if self.failures.get(step, 0):
            self.failures[step] -= 1
            raise ConnectionClosedError

    async def publish(self, subject: str, payload: bytes) -> None:
        self.fail(subject)
        self.published.append((subject, payload))

    async def flush(self, **options: float) -> None:
        self.fail("flush")


def publisher(nats: Nats, deadline: float = 5) -> Processor:
    settings = Settings(publish_deadline_seconds=deadline, processor_retry_seconds=0.01)
    return Processor(settings, None, nats, None, "t")  # type: ignore[arg-type]


MESSAGES = [("a", b"1"), ("b", b"2"), ("c", b"3")]


async def test_publishing_resumes_where_nats_failed_and_sends_each_message_once() -> None:
    nats = Nats({"b": 2, "flush": 1})
    await asyncio.wait_for(publisher(nats).publish(MESSAGES), 2)
    assert nats.published == MESSAGES and nats.failures == {"b": 0, "flush": 0}


async def test_publishing_gives_up_past_the_deadline() -> None:
    with pytest.raises(PublishStalled):
        await asyncio.wait_for(publisher(Nats({"a": 10_000}), deadline=0.05).publish(MESSAGES), 2)


class Committer:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.commits: list[dict] = []

    async def commit(self, offsets: dict) -> None:
        self.commits.append(offsets)
        if self.error:
            raise self.error


@pytest.mark.parametrize("error", [None, CommitFailedError(), IllegalStateError()])
async def test_delivery_commits_after_publishing_and_settles_even_if_the_commit_is_lost(
    error: Exception | None,
) -> None:
    nats, consumer = Nats({}), Committer(error)
    settings = Settings(processor_retry_seconds=0.01)
    delivering = Processor(settings, None, nats, consumer, "t")  # type: ignore[arg-type]
    delivering.start()
    delivering.stages[0].cancel()
    delivering.begin()
    await delivering.outbox.put(Batch({PARTITION: 7}, MESSAGES, None, monotonic()))
    await asyncio.wait_for(delivering.settled.wait(), 2)
    assert nats.published == MESSAGES and consumer.commits == [{PARTITION: 7}]
    assert not delivering.stages[1].done()
    await delivering.halt()
