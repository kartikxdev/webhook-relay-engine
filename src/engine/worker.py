"""Async Relay Worker Engine for processing and retrying webhooks."""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

import httpx

from src.config import RetryConfig, WorkerConfig
from src.engine.backoff import calculate_next_retry_time
from src.engine.dispatcher import WebhookDispatcher
from src.models.delivery import DeliveryAttempt, WebhookDelivery
from src.models.enums import AttemptOutcome
from src.storage.repository import WebhookRepository

logger = logging.getLogger("relayforge.worker")


class AsyncRelayWorker:
    """
    Background worker that continuously claims due deliveries, dispatches HTTP requests,
    handles exponential backoff retries, and moves exhausted deliveries to the DLQ.
    """

    def __init__(
        self,
        repository: WebhookRepository,
        worker_config: WorkerConfig,
        retry_config: RetryConfig,
    ) -> None:
        self.repository = repository
        self.worker_config = worker_config
        self.retry_config = retry_config
        self.dispatcher = WebhookDispatcher(timeout_seconds=worker_config.request_timeout_seconds)
        self._semaphore = asyncio.Semaphore(worker_config.concurrency)
        self._stop_event = asyncio.Event()
        self._http_client: httpx.AsyncClient | None = None
        self._in_flight_tasks: set[asyncio.Task[None]] = set()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                timeout=self.worker_config.request_timeout_seconds,
                limits=httpx.Limits(max_connections=self.worker_config.concurrency * 2),
            )
        return self._http_client

    async def process_delivery(self, delivery: WebhookDelivery) -> DeliveryAttempt:
        """Process a single delivery attempt, evaluate outcome, and update persistence."""
        client = await self._get_client()
        attempt = await self.dispatcher.execute_delivery(delivery, client=client)

        # 1. Record the attempt telemetry
        await self.repository.record_attempt(attempt)

        # 2. Evaluate state transition
        if attempt.outcome == AttemptOutcome.SUCCESS:
            logger.info(
                "Delivery %s succeeded (HTTP %s, duration %.1fms)",
                delivery.id,
                attempt.status_code,
                attempt.execution_duration_ms,
            )
            await self.repository.mark_delivered(delivery.id, attempt.attempt_number)
            return attempt

        # Delivery failed: check if retries exhausted
        if attempt.attempt_number >= delivery.max_attempts:
            failure_reason = (
                attempt.error_message
                or f"Max attempts ({delivery.max_attempts}) reached with status {attempt.status_code}"
            )
            logger.warning(
                "Delivery %s exhausted %d attempts. Moving to DLQ. Reason: %s",
                delivery.id,
                attempt.attempt_number,
                failure_reason,
            )
            await self.repository.move_to_dead_letter(
                delivery_id=delivery.id,
                failure_reason=failure_reason,
                attempt_count=attempt.attempt_number,
            )
        else:
            next_retry_at = calculate_next_retry_time(
                attempt=attempt.attempt_number,
                config=self.retry_config,
                from_time=datetime.now(UTC),
            )
            logger.info(
                "Delivery %s failed attempt %d/%d (%s). Scheduling retry at %s",
                delivery.id,
                attempt.attempt_number,
                delivery.max_attempts,
                attempt.outcome.value,
                next_retry_at.isoformat(),
            )
            await self.repository.schedule_retry(
                delivery_id=delivery.id,
                attempt_count=attempt.attempt_number,
                next_retry_at=next_retry_at,
            )

        return attempt

    async def _safe_process_delivery(self, delivery: WebhookDelivery) -> None:
        """Worker task wrapper bounded by concurrency semaphore."""
        async with self._semaphore:
            try:
                await self.process_delivery(delivery)
            except Exception as exc:
                logger.exception("Unexpected error processing delivery %s: %s", delivery.id, exc)

    async def run_single_tick(self) -> int:
        """
        Execute a single polling iteration.

        Returns:
            Number of deliveries claimed and dispatched.
        """
        deliveries = await self.repository.claim_due_deliveries(
            batch_size=self.worker_config.batch_size
        )
        if not deliveries:
            return 0

        logger.debug("Claimed %d due deliveries for processing", len(deliveries))
        for delivery in deliveries:
            task = asyncio.create_task(self._safe_process_delivery(delivery))
            self._in_flight_tasks.add(task)
            task.add_done_callback(self._in_flight_tasks.discard)

        return len(deliveries)

    async def run(self) -> None:
        """Main worker loop running until stop() is called."""
        logger.info(
            "AsyncRelayWorker started (concurrency=%d, poll_interval=%.2fs)",
            self.worker_config.concurrency,
            self.worker_config.poll_interval_seconds,
        )
        self._stop_event.clear()

        while not self._stop_event.is_set():
            try:
                count = await self.run_single_tick()
                # If no work was found, sleep for the poll interval
                if count == 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=self.worker_config.poll_interval_seconds,
                        )
                else:
                    # Brief yield so other tasks can execute
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Error in worker tick loop: %s", exc)
                await asyncio.sleep(self.worker_config.poll_interval_seconds)

        await self._drain_and_close()

    async def stop(self) -> None:
        """Signal the worker loop to stop gracefully and drain in-flight tasks."""
        logger.info("Signaling AsyncRelayWorker to stop...")
        self._stop_event.set()
        await self._drain_and_close()

    async def _drain_and_close(self) -> None:
        """Drain any in-flight execution tasks and close HTTP client."""
        if self._in_flight_tasks:
            logger.info("Waiting for %d in-flight tasks to finish...", len(self._in_flight_tasks))
            await asyncio.gather(*list(self._in_flight_tasks), return_exceptions=True)

        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
        logger.info("AsyncRelayWorker shut down cleanly.")
