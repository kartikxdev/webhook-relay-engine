"""Unit tests for SQLite storage layer and WebhookRepository."""


import pytest
from pydantic import HttpUrl

from src.models.delivery import DeliveryAttempt, WebhookCreate
from src.models.enums import AttemptOutcome, HttpMethod, WebhookStatus
from src.storage.repository import WebhookRepository


@pytest.mark.asyncio
async def test_create_and_get_delivery(repository: WebhookRepository) -> None:
    """Verify storing and retrieving a webhook delivery record."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        http_method=HttpMethod.POST,
        payload={"event": "user.signup", "id": 100},
        headers={"X-Custom": "test-header"},
    )

    delivery, created = await repository.create_delivery(inbound)
    assert created is True
    assert delivery.id is not None
    assert delivery.status == WebhookStatus.PENDING
    assert delivery.attempt_count == 0
    assert delivery.payload == {"event": "user.signup", "id": 100}
    assert delivery.headers == {"X-Custom": "test-header"}

    fetched = await repository.get_delivery(delivery.id)
    assert fetched is not None
    assert fetched.id == delivery.id
    assert fetched.destination_url == "https://httpbin.org/post"


@pytest.mark.asyncio
async def test_idempotency_key_deduplication(repository: WebhookRepository) -> None:
    """Verify duplicate requests with the same idempotency key return existing delivery."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        payload={"order": 123},
        idempotency_key="idemp_abc_12345",
    )

    del1, created1 = await repository.create_delivery(inbound)
    assert created1 is True

    # Duplicate call with identical idempotency key
    del2, created2 = await repository.create_delivery(inbound)
    assert created2 is False
    assert del1.id == del2.id


@pytest.mark.asyncio
async def test_claim_due_deliveries_atomic(repository: WebhookRepository) -> None:
    """Verify claiming due deliveries transitions state to PROCESSING and prevents double claiming."""
    inbound1 = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        payload={"item": 1},
    )
    inbound2 = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        payload={"item": 2},
    )

    d1, _ = await repository.create_delivery(inbound1)
    d2, _ = await repository.create_delivery(inbound2)

    claimed = await repository.claim_due_deliveries(batch_size=10)
    assert len(claimed) == 2
    claimed_ids = {c.id for c in claimed}
    assert d1.id in claimed_ids
    assert d2.id in claimed_ids

    for c in claimed:
        assert c.status == WebhookStatus.PROCESSING

    # Second claim attempt immediately finds 0 due items
    second_claim = await repository.claim_due_deliveries(batch_size=10)
    assert len(second_claim) == 0


@pytest.mark.asyncio
async def test_record_attempt_and_mark_delivered(repository: WebhookRepository) -> None:
    """Verify recording an execution attempt and marking delivery as DELIVERED."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        payload={"msg": "success"},
    )
    d, _ = await repository.create_delivery(inbound)

    attempt = DeliveryAttempt(
        delivery_id=d.id,
        attempt_number=1,
        outcome=AttemptOutcome.SUCCESS,
        status_code=200,
        response_body='{"ok":true}',
        execution_duration_ms=45.2,
    )
    await repository.record_attempt(attempt)
    await repository.mark_delivered(d.id, attempt_count=1)

    detail = await repository.get_delivery_detail(d.id)
    assert detail is not None
    assert detail.delivery.status == WebhookStatus.DELIVERED
    assert detail.delivery.attempt_count == 1
    assert len(detail.attempts) == 1
    assert detail.attempts[0].status_code == 200


@pytest.mark.asyncio
async def test_move_to_dead_letter_and_requeue(repository: WebhookRepository) -> None:
    """Verify dead-letter transition and subsequent replay operation."""
    inbound = WebhookCreate(
        destination_url=HttpUrl("https://httpbin.org/post"),
        payload={"msg": "failing"},
        max_attempts=2,
    )
    d, _ = await repository.create_delivery(inbound)

    # Move to DLQ
    dlq_record = await repository.move_to_dead_letter(
        delivery_id=d.id,
        failure_reason="Destination returned HTTP 500 repeatedly",
        attempt_count=2,
    )
    assert dlq_record.delivery_id == d.id
    assert dlq_record.replay_count == 0

    dlq_list = await repository.list_dead_letters()
    assert len(dlq_list) == 1
    assert dlq_list[0].delivery_id == d.id

    # Requeue DLQ item
    replayed = await repository.requeue_dead_letter(d.id, reset_attempts=True)
    assert replayed is True

    updated_delivery = await repository.get_delivery(d.id)
    assert updated_delivery is not None
    assert updated_delivery.status == WebhookStatus.PENDING
    assert updated_delivery.attempt_count == 0

    # DLQ record should reflect replay
    dlq_updated = (await repository.list_dead_letters())[0]
    assert dlq_updated.replay_count == 1
    assert dlq_updated.last_replayed_at is not None
