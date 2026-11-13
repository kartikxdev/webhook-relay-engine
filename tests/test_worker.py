"""Integration tests for AsyncRelayWorker lifecycle and state machine."""

import httpx
import pytest
from pydantic import HttpUrl

from src.config import RetryConfig, WorkerConfig
from src.engine.worker import AsyncRelayWorker
from src.models.delivery import WebhookCreate
from src.models.enums import WebhookStatus
from src.storage.repository import WebhookRepository


@pytest.mark.asyncio
async def test_worker_processes_successful_delivery(
    repository: WebhookRepository,
    worker_config: WorkerConfig,
    retry_config: RetryConfig,
) -> None:
    """Verify worker picks up due delivery, succeeds on HTTP 200, and marks DELIVERED."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://api.example.com/target"),
        payload={"event": "order.paid"},
    )
    delivery, _ = await repository.create_delivery(inbound)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    worker = AsyncRelayWorker(repository, worker_config, retry_config)
    worker._http_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    # Single tick to claim and process
    processed_count = await worker.run_single_tick()
    assert processed_count == 1

    # Drain any in-flight async tasks
    await worker.stop()

    updated = await repository.get_delivery(delivery.id)
    assert updated is not None
    assert updated.status == WebhookStatus.DELIVERED
    assert updated.attempt_count == 1

    detail = await repository.get_delivery_detail(delivery.id)
    assert detail is not None
    assert len(detail.attempts) == 1
    assert detail.attempts[0].status_code == 200


@pytest.mark.asyncio
async def test_worker_schedules_retry_on_failure(
    repository: WebhookRepository,
    worker_config: WorkerConfig,
    retry_config: RetryConfig,
) -> None:
    """Verify worker reschedules delivery to PENDING when attempts < max_attempts."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://api.example.com/target"),
        payload={"event": "order.failed"},
        max_attempts=3,
    )
    delivery, _ = await repository.create_delivery(inbound)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="Bad Gateway")

    worker = AsyncRelayWorker(repository, worker_config, retry_config)
    worker._http_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    await worker.run_single_tick()
    await worker.stop()

    updated = await repository.get_delivery(delivery.id)
    assert updated is not None
    assert updated.status == WebhookStatus.PENDING
    assert updated.attempt_count == 1
    assert updated.next_retry_at > updated.created_at


@pytest.mark.asyncio
async def test_worker_moves_to_dlq_when_retries_exhausted(
    repository: WebhookRepository,
    worker_config: WorkerConfig,
    retry_config: RetryConfig,
) -> None:
    """Verify worker routes failing delivery to DLQ when max_attempts is reached."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://api.example.com/target"),
        payload={"event": "order.permanent_fail"},
        max_attempts=1,  # Exhaust after single failure
    )
    delivery, _ = await repository.create_delivery(inbound)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    worker = AsyncRelayWorker(repository, worker_config, retry_config)
    worker._http_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    await worker.run_single_tick()
    await worker.stop()

    updated = await repository.get_delivery(delivery.id)
    assert updated is not None
    assert updated.status == WebhookStatus.DEAD_LETTER
    assert updated.attempt_count == 1

    dlq_records = await repository.list_dead_letters()
    assert len(dlq_records) == 1
    assert dlq_records[0].delivery_id == delivery.id
    assert "500" in dlq_records[0].failure_reason
