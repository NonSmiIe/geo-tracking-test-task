import json

from tests.test_api import report, zone


def test_insights_are_private_and_freshness_is_real(client):
    created = zone(client)
    zone(client, owner="bob", name="private-bob")
    client.post("/locations", json=report())
    insight = client.get("/insights", headers={"X-User-ID": "alice"}).json()
    assert insight["fleet"]["total_devices"] == 1
    assert insight["fleet"]["stale_devices"] == 1
    assert insight["active_zones"] == 1
    assert [item["id"] for item in insight["zones"]] == [created["id"]]
    assert "private-bob" not in json.dumps(insight)
    assert insight["zones"][0]["devices_inside"] == 0
