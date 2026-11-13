"""Integration tests for FastAPI REST endpoints."""

import pytest
from httpx import AsyncClient

from src.models.delivery import WebhookCreate
from src.models.enums import WebhookStatus
from src.storage.repository import WebhookRepository


@pytest.mark.asyncio
async def test_healthz_endpoint(async_client: AsyncClient) -> None:
    """Verify healthz endpoint returns ok status."""
    response = await async_client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["database"] == "healthy"


@pytest.mark.asyncio
async def test_ingest_webhook_endpoint(async_client: AsyncClient) -> None:
    """Verify POST /api/v1/webhooks/ingest returns 201 Created."""
    payload = {
        "destination_url": "https://api.example.com/listener",
        "http_method": "POST",
        "payload": {"event": "subscription.renewed", "user_id": "usr_101"},
        "headers": {"X-Test": "1"},
        "secret_token": "token_abc",
        "idempotency_key": "idemp_test_999",
    }

    response = await async_client.post("/api/v1/webhooks/ingest", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["id"] is not None
    assert data["status"] == "PENDING"
    assert data["idempotency_key"] == "idemp_test_999"

    # Ingest again with identical idempotency key -> returns 200 OK with same ID
    dup_response = await async_client.post("/api/v1/webhooks/ingest", json=payload)
    assert dup_response.status_code == 200
    assert dup_response.json()["id"] == data["id"]


@pytest.mark.asyncio
async def test_list_and_detail_endpoints(
    async_client: AsyncClient,
    repository: WebhookRepository,
) -> None:
    """Verify listing deliveries and fetching detailed delivery information."""
    inbound = WebhookCreate(
        destination_url="https://api.example.com/hook",  # type: ignore[arg-type]
        payload={"msg": "list_test"},
    )
    delivery, _ = await repository.create_delivery(inbound)

    # Test list endpoint
    list_response = await async_client.get("/api/v1/deliveries")
    assert list_response.status_code == 200
    items = list_response.json()
    assert len(items) >= 1
    assert any(item["id"] == delivery.id for item in items)

    # Test detail endpoint
    detail_response = await async_client.get(f"/api/v1/deliveries/{delivery.id}")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["delivery"]["id"] == delivery.id
    assert detail["attempts"] == []


@pytest.mark.asyncio
async def test_dlq_endpoints(
    async_client: AsyncClient,
    repository: WebhookRepository,
) -> None:
    """Verify listing DLQ and triggering replay."""
    inbound = WebhookCreate(
        destination_url="https://api.example.com/dlq_test",  # type: ignore[arg-type]
        payload={"msg": "to_dlq"},
    )
    delivery, _ = await repository.create_delivery(inbound)
    await repository.move_to_dead_letter(delivery.id, "Target host unreachable", 3)

    # List DLQ
    dlq_res = await async_client.get("/api/v1/dlq")
    assert dlq_res.status_code == 200
    dlq_items = dlq_res.json()
    assert len(dlq_items) >= 1
    assert dlq_items[0]["delivery_id"] == delivery.id

    # Replay DLQ
    replay_res = await async_client.post(
        f"/api/v1/dlq/{delivery.id}/replay", params={"reset_attempts": True}
    )
    assert replay_res.status_code == 200
    replay_data = replay_res.json()
    assert replay_data["status"] == WebhookStatus.PENDING
    assert replay_data["delivery_id"] == delivery.id


@pytest.mark.asyncio
async def test_not_found_endpoint(async_client: AsyncClient) -> None:
    """Verify 404 for non-existent delivery ID."""
    response = await async_client.get("/api/v1/deliveries/non-existent-uuid")
    assert response.status_code == 404
