import nats
from aiokafka import AIOKafkaProducer
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
