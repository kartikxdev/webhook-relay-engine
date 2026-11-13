"""HTTP Webhook Dispatcher executing individual forward attempts."""

import logging
import time

import httpx

from src.engine.signature import prepare_relay_headers
from src.models.delivery import DeliveryAttempt, WebhookDelivery, utc_now
from src.models.enums import AttemptOutcome

logger = logging.getLogger("relayforge.dispatcher")

MAX_RESPONSE_BODY_LENGTH = 4096


class WebhookDispatcher:
    """Dispatches HTTP requests to webhook destinations and captures detailed telemetry."""

    def __init__(self, timeout_seconds: float = 15.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def execute_delivery(
        self,
        delivery: WebhookDelivery,
        client: httpx.AsyncClient | None = None,
    ) -> DeliveryAttempt:
        """
        Execute a single outbound HTTP forwarding attempt.

        Returns:
            DeliveryAttempt containing roundtrip latency, status code, outcome, and response preview.
        """
        attempt_number = delivery.attempt_count + 1
        headers = prepare_relay_headers(
            base_headers=delivery.headers,
            payload_str=delivery.payload_raw,
            secret_token=delivery.secret_token,
            delivery_id=delivery.id,
            attempt_number=attempt_number,
        )

        start_time = time.perf_counter()
        attempt_timestamp = utc_now()

        should_close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=self.timeout_seconds)
            should_close_client = True

        try:
            response = await client.request(
                method=delivery.http_method.value,
                url=delivery.destination_url,
                content=delivery.payload_raw.encode("utf-8"),
                headers=headers,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000.0

            # Truncate response preview to prevent DB bloat
            response_text = response.text[:MAX_RESPONSE_BODY_LENGTH] if response.text else None

            if 200 <= response.status_code < 300:
                outcome = AttemptOutcome.SUCCESS
                error_message = None
            else:
                outcome = AttemptOutcome.HTTP_ERROR
                error_message = f"Received HTTP {response.status_code} from destination endpoint"

            return DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=attempt_number,
                outcome=outcome,
                status_code=response.status_code,
                response_body=response_text,
                error_message=error_message,
                execution_duration_ms=round(duration_ms, 2),
                attempted_at=attempt_timestamp,
            )

        except httpx.TimeoutException as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            logger.warning("Delivery %s timed out after %.1f ms: %s", delivery.id, duration_ms, exc)
            return DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=attempt_number,
                outcome=AttemptOutcome.TIMEOUT,
                status_code=None,
                response_body=None,
                error_message=f"Request timed out: {exc}",
                execution_duration_ms=round(duration_ms, 2),
                attempted_at=attempt_timestamp,
            )

        except httpx.RequestError as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            logger.warning("Delivery %s network error: %s", delivery.id, exc)
            return DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=attempt_number,
                outcome=AttemptOutcome.NETWORK_ERROR,
                status_code=None,
                response_body=None,
                error_message=f"Network connection failed: {exc}",
                execution_duration_ms=round(duration_ms, 2),
                attempted_at=attempt_timestamp,
            )

        finally:
            if should_close_client:
                await client.aclose()
