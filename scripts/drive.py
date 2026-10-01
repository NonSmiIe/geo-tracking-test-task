import argparse
import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aiohttp
import orjson
import websockets


async def verify(url):
    token = f"check-{uuid4().hex[:8]}"
    owner, other = token + "-owner", token + "-other"
    sockets = []
    zone_id = None
    async with aiohttp.ClientSession() as client:
        try:
            async with client.post(
                url + "/geozones",
                headers={"X-User-ID": owner},
                json={
                    "name": "Verification zone",
                    "latitude": 56.9496,
                    "longitude": 24.1052,
                    "radius_m": 100,
                },
            ) as response:
                response.raise_for_status()
                zone_id = (await response.json())["id"]
            for user in (owner, owner, other):
                socket = await websockets.connect(
                    url.replace("http", "ws", 1) + f"/ws?user_id={user}"
                )
                assert orjson.loads(await asyncio.wait_for(socket.recv(), 2))["type"] == "ready"
                sockets.append(socket)
            start = datetime.now(UTC)
            start = start.replace(microsecond=start.microsecond // 1000 * 1000)
            reports = [
                {
                    "device_id": token,
                    "latitude": 56.9496,
                    "longitude": longitude,
                    "timestamp": (start + timedelta(microseconds=offset)).isoformat(),
                }
                for longitude, offset in ((24.1052, 100), (25.1, 900))
            ]
            async with client.post(url + "/locations/batch", json=reports) as response:
                assert response.status == 200
                assert (await response.json())["statuses"] == ["accepted", "accepted"]

            async def relevant(socket, kind):
                async with asyncio.timeout(3):
                    while True:
                        message = orjson.loads(await socket.recv())
                        matching = [
                            item for item in message.get("items", []) if item["device_id"] == token
                        ]
                        if socket is sockets[2] and message["type"] == "inside_report" and matching:
                            raise AssertionError("private alert leaked to another user")
                        if message["type"] == kind and matching:
                            return matching

            for socket in sockets:
                assert len(await relevant(socket, "locations")) == 2
            for socket in sockets[:2]:
                matches = await relevant(socket, "inside_report")
                assert len(matches) == 1 and matches[0]["zone_id"] == zone_id
            await sockets[1].close()
            async with client.post(
                url + "/locations",
                json=reports[0]
                | {
                    "timestamp": (start + timedelta(seconds=1)).isoformat(),
                },
            ) as response:
                assert response.status == 200
            assert len(await relevant(sockets[0], "locations")) == 1
            assert len(await relevant(sockets[0], "inside_report")) == 1
            assert len(await relevant(sockets[2], "locations")) == 1
            async with client.get(
                url + f"/geozones/{zone_id}", headers={"X-User-ID": other}
            ) as response:
                assert response.status == 404
            async with client.get(url + "/metrics") as response:
                metrics = await response.json()
            return {
                "passed": True,
                "inside_then_outside_preserved": True,
                "both_owner_sessions_received_alert": True,
                "sibling_survives_disconnect": True,
                "private_crud": True,
                "pending_reports": metrics["pending_reports"],
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
    parser = argparse.ArgumentParser(
        description="Verify the running fleet service without resetting it"
    )
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    args = parser.parse_args()
    print(orjson.dumps(asyncio.run(verify(args.url)), option=orjson.OPT_INDENT_2).decode())
