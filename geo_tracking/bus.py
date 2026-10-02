import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import confluent_kafka
import nats
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from nats.aio.client import Client
from nats.errors import SlowConsumerError

from geo_tracking.settings import Settings


async def connect_nats(
    settings: Settings,
    reconnected: Callable[[], Awaitable[None]] | None = None,
    dropped: Callable[[str], Awaitable[None]] | None = None,
) -> Client:
    async def failed(error: Exception) -> None:
        if dropped is not None and isinstance(error, SlowConsumerError):
            await dropped(error.subject)

    return await nats.connect(
        servers=settings.nats_servers.split(","),
        max_reconnect_attempts=-1,
        reconnect_time_wait=0.5,
        error_cb=failed,
        reconnected_cb=reconnected,
    )


class ProduceFailed(Exception):
    pass


class Producer:
    def __init__(self, settings: Settings) -> None:
        self.client = confluent_kafka.Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "partitioner": "murmur2_random",
                "compression.type": "lz4",
                "linger.ms": settings.produce_linger_ms,
                "message.timeout.ms": 10000,
                "logger": logging.getLogger("librdkafka"),
            }
        )
        self.poller: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.poller = asyncio.create_task(self.poll())

    async def poll(self) -> None:
        while True:
            self.client.poll(0)
            await asyncio.sleep(0.002)

    def send(self, topic: str, key: bytes, value: bytes) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def delivered(error: confluent_kafka.KafkaError | None, message: object) -> None:
            if future.done():
                return
            if error is None:
                future.set_result(None)
            else:
                future.set_exception(ProduceFailed(error.str()))

        try:
            self.client.produce(topic, value, key, on_delivery=delivered)
        except (BufferError, confluent_kafka.KafkaException) as error:
            raise ProduceFailed(str(error)) from error
        return future

    async def stop(self) -> None:
        if self.poller is not None:
            self.poller.cancel()
            await asyncio.gather(self.poller, return_exceptions=True)
        await asyncio.to_thread(self.client.flush, 10)


async def ensure_topic(settings: Settings) -> str:
    def ensure() -> str:
        config: dict[str, Any] = {
            "bootstrap.servers": settings.kafka_bootstrap,
            "logger": logging.getLogger("librdkafka"),
        }
        admin = AdminClient(config)
        topic = settings.kafka_topic
        wanted = NewTopic(topic, settings.kafka_partitions, settings.kafka_replication)
        try:
            admin.create_topics([wanted])[topic].result(timeout=10)
        except confluent_kafka.KafkaException as error:
            if error.args[0].code() != confluent_kafka.KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        for _ in range(50):
            try:
                found = admin.describe_topics(confluent_kafka.TopicCollection([topic]))[topic]
                return str(found.result(timeout=10).topic_id)
            except confluent_kafka.KafkaException as error:
                if error.args[0].code() != confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise
                time.sleep(0.1)
        raise TimeoutError(f"topic {topic} was created but never became visible")

    return await asyncio.to_thread(ensure)
