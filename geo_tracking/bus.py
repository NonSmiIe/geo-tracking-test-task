import asyncio

import confluent_kafka
import nats
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from nats.aio.client import Client
from nats.errors import SlowConsumerError

from geo_tracking.metrics import SLOW_CONSUMERS
from geo_tracking.settings import Settings


class Subjects:
    def __init__(self, prefix: str):
        self.prefix = prefix

    def alerts(self, user_id: str) -> str:
        return f"{self.prefix}.alerts.{user_id.encode().hex()}"

    def zones(self, user_id: str) -> str:
        return f"{self.prefix}.zones.{user_id.encode().hex()}"


async def counted(error: Exception) -> None:
    if isinstance(error, SlowConsumerError):
        SLOW_CONSUMERS.inc()


async def connect_nats(settings: Settings) -> Client:
    return await nats.connect(
        servers=settings.nats_servers.split(","),
        max_reconnect_attempts=-1,
        reconnect_time_wait=0.5,
        error_cb=counted,
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


async def ensure_topic(settings: Settings) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    await admin.start()
    try:
        if settings.kafka_topic not in await admin.list_topics():
            await admin.create_topics(
                [
                    NewTopic(
                        settings.kafka_topic, settings.kafka_partitions, settings.kafka_replication
                    )
                ]
            )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()
