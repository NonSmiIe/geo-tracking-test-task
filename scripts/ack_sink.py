import argparse
import asyncio

import orjson
import websockets
from websockets.asyncio.server import ServerConnection

ACK = '{{"type":"ack","count":{}}}'


async def handle(socket: ServerConnection) -> None:
    received = 0
    async for message in socket:
        if message == '{"type":"flush"}':
            await socket.send(ACK.format(received))
            continue
        before = received
        received += len(orjson.loads(message))
        if received // 1000 != before // 1000:
            await socket.send(ACK.format(received))


async def main(port: int) -> None:
    async with websockets.serve(handle, "127.0.0.1", port, max_size=65536):
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ack-only /ingest sink that measures the generator alone"
    )
    parser.add_argument("--port", type=int, default=8099)
    asyncio.run(main(parser.parse_args().port))
