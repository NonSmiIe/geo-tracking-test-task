import json

import httpx
import pytest
from fastapi import HTTPException
from openai import AsyncOpenAI

from geo_tracking.assistant import Assistant, AssistantRequest
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


def test_unconfigured_ai_is_explicit_and_read_only(client):
    status = client.get("/assistant", headers={"X-User-ID": "alice"}).json()
    assert status["available"] is False and status["mutates_data"] is False
    response = client.post(
        "/assistant", headers={"X-User-ID": "alice"}, json={"prompt": "create a zone"}
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "assistant_not_configured"
    assert client.get("/geozones", headers={"X-User-ID": "alice"}).json()["items"] == []


def provider(reply, captured):
    def handler(request):
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "resp_test",
                "object": "response",
                "created_at": 1,
                "status": "completed",
                "model": "gpt-4.1-mini",
                "output": [
                    {
                        "id": "msg_test",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {"type": "output_text", "text": json.dumps(reply), "annotations": []}
                        ],
                    }
                ],
            },
        )

    return AsyncOpenAI(
        api_key="test-only",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        max_retries=0,
    )


def test_real_sdk_structured_proposal_is_owner_scoped_and_does_not_create(client):
    zone(client, owner="bob", name="secret-bob")
    captured = []
    client.app.state.assistant.client = provider(
        {
            "summary": "Review this 500 metre zone at your selected map point.",
            "proposed_zone": {
                "name": "Depot",
                "latitude": 56.95,
                "longitude": 24.1,
                "radius_m": 500,
            },
        },
        captured,
    )
    response = client.post(
        "/assistant",
        headers={"X-User-ID": "alice"},
        json={
            "prompt": "Suggest a depot zone",
            "latitude": 56.95,
            "longitude": 24.1,
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["proposed_zone"]["radius_m"] == 500
    assert captured[0]["store"] is False
    assert captured[0]["text"]["format"]["strict"] is True
    assert "secret-bob" not in captured[0]["input"]
    assert client.get("/geozones", headers={"X-User-ID": "alice"}).json()["items"] == []


@pytest.mark.parametrize(
    "proposal",
    [
        {"name": "wrong centre", "latitude": 0, "longitude": 0, "radius_m": 500},
        {"name": "bad radius", "latitude": 56.95, "longitude": 24.1, "radius_m": -1},
    ],
)
def test_invalid_model_proposal_is_rejected(client, proposal):
    client.app.state.assistant.client = provider(
        {"summary": "draft", "proposed_zone": proposal}, []
    )
    response = client.post(
        "/assistant",
        headers={"X-User-ID": "alice"},
        json={
            "prompt": "Suggest a zone",
            "latitude": 56.95,
            "longitude": 24.1,
        },
    )
    assert response.status_code == 422


async def test_ai_concurrency_limit_rejects_without_queue(settings):
    assistant = Assistant(settings)
    assistant.client = provider({"summary": "read only", "proposed_zone": None}, [])
    assistant.inflight = 2
    try:
        with pytest.raises(HTTPException) as failure:
            await assistant.propose(AssistantRequest(prompt="inspect"), {})
        assert failure.value.status_code == 429
    finally:
        await assistant.close()
