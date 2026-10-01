import json

from tests.helpers import report, wait_for, zone


def test_insights_are_private_and_freshness_is_real(stack, http) -> None:
    created = zone(http)
    zone(http, owner="bob", name="private-bob")
    http.post("/locations", json=report())
    insight = wait_for(
        lambda: (
            (value := http.get("/insights", headers={"X-User-ID": "alice"}).json())["fleet"][
                "total_devices"
            ]
            and value
        )
    )
    assert insight["fleet"]["stale_devices"] == 1
    assert insight["active_zones"] == 1
    assert [item["id"] for item in insight["zones"]] == [created["id"]]
    assert "private-bob" not in json.dumps(insight)
    assert insight["zones"][0]["devices_inside"] == 0
