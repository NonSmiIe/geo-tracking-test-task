import asyncio
from collections.abc import Callable

import nats
import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from geo_tracking.settings import Settings


class Subjects:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.metrics = f"{prefix}.metrics"

    def alerts(self, user_id: str) -> str:
        return f"{self.prefix}.alerts.{user_id.encode().hex()}"

    def zones(self, user_id: str) -> str:
        return f"{self.prefix}.zones.{user_id.encode().hex()}"


async def connect_nats(settings: Settings) -> Client:
    return await nats.connect(settings.nats_url, max_reconnect_attempts=-1, reconnect_time_wait=0.5)


def kafka_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
        max_batch_size=262144,
        request_timeout_ms=10000,
    )


async def ensure_topic(settings: Settings) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    await admin.start()
    try:
        if settings.kafka_topic not in await admin.list_topics():
            await admin.create_topics(
                [NewTopic(settings.kafka_topic, settings.kafka_partitions, 1)]
            )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()


async def serve_metrics(
    client: Client, subjects: Subjects, snapshot: Callable[[], dict]
) -> Subscription:
    async def respond(message: Msg) -> None:
        await message.respond(orjson.dumps(snapshot()))

    return await client.subscribe(subjects.metrics, cb=respond)


async def gather_metrics(client: Client, subjects: Subjects, seconds: float) -> list[dict]:
    replies: list[dict] = []

    async def collect(message: Msg) -> None:
        replies.append(orjson.loads(message.data))

    inbox = client.new_inbox()
    subscription = await client.subscribe(inbox, cb=collect)
    try:
        await client.publish(subjects.metrics, b"", reply=inbox)
        await asyncio.sleep(seconds)
    finally:
        await subscription.unsubscribe()
    return replies
