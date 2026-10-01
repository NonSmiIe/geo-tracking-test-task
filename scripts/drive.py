import argparse
import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aiohttp
import orjson
import websockets

RIGA = {"south": 56.5, "west": 23.5, "north": 57.5, "east": 25}


async def frames(socket, kind: str, device: str, seconds: float = 5) -> list:
    async with asyncio.timeout(seconds):
        while True:
            message = orjson.loads(await socket.recv())
            if message["type"] != kind:
                continue
            items = [
                item
                for item in message["items"]
                if (item[0] if isinstance(item, list) else item["device_id"]) == device
            ]
            if items:
                return items


async def quiet(socket, kind: str, device: str, seconds: float = 1.5) -> None:
    try:
        await frames(socket, kind, device, seconds)
    except TimeoutError:
        return
    raise AssertionError(f"unexpected {kind} for {device}")


async def verify(url: str) -> dict:
    token = f"check-{uuid4().hex[:8]}"
    owner, other = token + "-owner", token + "-other"
    sockets = []
    zone_id = None
    async with aiohttp.ClientSession() as client:
        try:
            async with client.post(
                url + "/geozones",
                headers={"X-User-ID": owner},
                json={"name": "Check", "latitude": 56.9496, "longitude": 24.1052, "radius_m": 100},
            ) as response:
                response.raise_for_status()
                zone_id = (await response.json())["id"]
            for user in (owner, owner, other):
                socket = await websockets.connect(
                    url.replace("http", "ws", 1) + f"/ws?user_id={user}"
                )
                assert orjson.loads(await socket.recv())["type"] == "ready"
                await socket.send(orjson.dumps({"type": "viewport", **RIGA}).decode())
                while orjson.loads(await socket.recv())["type"] != "subscribed":
                    pass
                sockets.append(socket)
            start = datetime.now(UTC).replace(microsecond=0)
            reports = [
                {
                    "device_id": token,
                    "latitude": 56.9496,
                    "longitude": longitude,
                    "timestamp": (start + timedelta(microseconds=offset)).isoformat(),
                }
                for longitude, offset in ((24.1052, 100), (24.3, 900))
            ]
            async with client.post(url + "/locations/batch", json=reports) as response:
                assert response.status == 202, await response.text()
            for socket in sockets:
                assert len(await frames(socket, "positions", token)) == 2
            for socket in sockets[:2]:
                alerts = await frames(socket, "inside_report", token)
                assert len(alerts) == 1 and alerts[0]["zone_id"] == zone_id
            await quiet(sockets[2], "inside_report", token)
            await sockets[1].close()
            later = reports[0] | {"timestamp": (start + timedelta(seconds=1)).isoformat()}
            async with client.post(url + "/locations", json=later) as response:
                assert response.status == 202
            assert len(await frames(sockets[0], "inside_report", token)) == 1
            async with client.get(
                url + f"/geozones/{zone_id}", headers={"X-User-ID": other}
            ) as response:
                assert response.status == 404
            async with client.get(url + "/metrics") as response:
                roles = (await response.json())["roles"]
            return {
                "passed": True,
                "inside_then_outside_preserved": True,
                "both_owner_sessions_received_alert": True,
                "other_user_received_no_alert": True,
                "sibling_survives_disconnect": True,
                "private_crud": True,
                "api_instances": roles["api"]["instances"],
                "processor_instances": roles["processor"]["instances"],
                "consumer_lag": roles["processor"]["consumer_lag"],
            }
        finally:
            for socket in sockets:
                await socket.close()
            if zone_id:
                async with client.delete(
                    url + f"/geozones/{zone_id}", headers={"X-User-ID": owner}
                ):
                    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the running service without resetting it")
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    args = parser.parse_args()
    print(orjson.dumps(asyncio.run(verify(args.url)), option=orjson.OPT_INDENT_2).decode())
