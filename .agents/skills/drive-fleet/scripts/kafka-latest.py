import asyncio
import os
import sys

import orjson
from aiokafka import AIOKafkaConsumer, TopicPartition

PREFIX, START_MS, END_MS = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
TOPIC = os.environ.get("GEO_KAFKA_TOPIC", "reports")


async def main() -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=os.environ.get("GEO_KAFKA_BOOTSTRAP", "kafka:9092"),
        enable_auto_commit=False,
    )
    await consumer.start()
    partitions = [TopicPartition(TOPIC, p) for p in consumer.partitions_for_topic(TOPIC) or ()]
    consumer.assign(partitions)
    tails = await consumer.end_offsets(partitions)
    starts = await consumer.offsets_for_times({tp: START_MS for tp in partitions})
    ends = await consumer.offsets_for_times({tp: END_MS for tp in partitions})
    stop = {tp: ends[tp].offset if ends[tp] else tails[tp] for tp in partitions}
    pending = set()
    for tp in partitions:
        start = starts[tp].offset if starts[tp] else tails[tp]
        if start < stop[tp]:
            consumer.seek(tp, start)
            pending.add(tp)
    latest: dict[str, tuple[int, int, int]] = {}
    records = 0
    while pending:
        batch = await consumer.getmany(*pending, timeout_ms=1000, max_records=50000)
        for tp, items in batch.items():
            for item in items:
                if item.offset >= stop[tp]:
                    break
                record = orjson.loads(item.value)
                if not record[0].startswith(PREFIX):
                    continue
                records += 1
                if record[0] not in latest or record[3] > latest[record[0]][0]:
                    latest[record[0]] = (record[3], tp.partition, item.offset)
            if items and items[-1].offset + 1 >= stop[tp]:
                pending.discard(tp)
    await consumer.stop()
    sys.stdout.buffer.write(orjson.dumps({"records": records, "latest": latest}))


asyncio.run(main())
