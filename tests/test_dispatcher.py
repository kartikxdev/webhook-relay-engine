"""Unit tests for WebhookDispatcher with mocked HTTP transport."""

import httpx
import pytest

from src.engine.dispatcher import MAX_RESPONSE_BODY_LENGTH, WebhookDispatcher
from src.models.delivery import WebhookDelivery
from src.models.enums import AttemptOutcome, HttpMethod


@pytest.mark.asyncio
async def test_dispatcher_success_200() -> None:
    """Verify successful delivery recording status code 200 and SUCCESS outcome."""
    delivery = WebhookDelivery(
        destination_url="https://api.example.com/webhook",
        http_method=HttpMethod.POST,
        payload_raw='{"event":"order.paid"}',
        headers_raw="{}",
        secret_token=None,
    )

    def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"] == "application/json"
        assert request.headers["x-relayforge-attempt"] == "1"
        return httpx.Response(200, json={"received": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))
    dispatcher = WebhookDispatcher(timeout_seconds=5.0)

    attempt = await dispatcher.execute_delivery(delivery, client=client)
    assert attempt.outcome == AttemptOutcome.SUCCESS
    assert attempt.status_code == 200
    assert attempt.error_message is None
    assert attempt.execution_duration_ms > 0
    await client.aclose()


@pytest.mark.asyncio
async def test_dispatcher_http_error_500() -> None:
    """Verify 5xx server error results in HTTP_ERROR outcome."""
    delivery = WebhookDelivery(
        destination_url="https://api.example.com/webhook",
        payload_raw='{"event":"test"}',
        headers_raw="{}",
    )

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))
    dispatcher = WebhookDispatcher()

    attempt = await dispatcher.execute_delivery(delivery, client=client)
    assert attempt.outcome == AttemptOutcome.HTTP_ERROR
    assert attempt.status_code == 503
    assert "Received HTTP 503" in (attempt.error_message or "")
    await client.aclose()


@pytest.mark.asyncio
async def test_dispatcher_response_truncation() -> None:
    """Verify very large response bodies are truncated to avoid database bloat."""
    delivery = WebhookDelivery(
        destination_url="https://api.example.com/webhook",
        payload_raw='{}',
        headers_raw="{}",
    )

    huge_text = "A" * (MAX_RESPONSE_BODY_LENGTH + 500)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=huge_text)

    client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))
    dispatcher = WebhookDispatcher()

    attempt = await dispatcher.execute_delivery(delivery, client=client)
    assert attempt.response_body is not None
    assert len(attempt.response_body) == MAX_RESPONSE_BODY_LENGTH
    await client.aclose()


@pytest.mark.asyncio
async def test_dispatcher_timeout() -> None:
    """Verify network timeouts are classified as TIMEOUT outcome."""
    delivery = WebhookDelivery(
        destination_url="https://api.example.com/webhook",
        payload_raw='{}',
        headers_raw="{}",
    )

    def mock_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Socket read timed out")

    client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))
    dispatcher = WebhookDispatcher()

    attempt = await dispatcher.execute_delivery(delivery, client=client)
    assert attempt.outcome == AttemptOutcome.TIMEOUT
    assert attempt.status_code is None
    assert "timed out" in (attempt.error_message or "").lower()
    await client.aclose()
