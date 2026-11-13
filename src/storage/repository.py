"""Repository layer for webhook deliveries, telemetry attempts, and dead-letter records."""

import json
import logging
import sqlite3
import uuid
from datetime import UTC, datetime

import aiosqlite

from src.models.delivery import (
    DeadLetterRecord,
    DeliveryAttempt,
    DeliveryDetail,
    WebhookCreate,
    WebhookDelivery,
    utc_now,
)
from src.models.enums import AttemptOutcome, HttpMethod, WebhookStatus
from src.storage.connection import DatabaseManager

logger = logging.getLogger("relayforge.repository")


class WebhookRepository:
    """Data access layer backed by SQLite."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db = db_manager

    @staticmethod
    def _parse_iso(iso_str: str) -> datetime:
        """Parse ISO formatted timestamp string into UTC datetime."""
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    def _row_to_delivery(self, row: aiosqlite.Row) -> WebhookDelivery:
        """Convert a SQLite row into a WebhookDelivery domain model."""
        return WebhookDelivery(
            id=row["id"],
            destination_url=row["destination_url"],
            http_method=HttpMethod(row["http_method"]),
            payload_raw=row["payload_raw"],
            headers_raw=row["headers_raw"],
            secret_token=row["secret_token"],
            status=WebhookStatus(row["status"]),
            attempt_count=row["attempt_count"],
            max_attempts=row["max_attempts"],
            idempotency_key=row["idempotency_key"],
            next_retry_at=self._parse_iso(row["next_retry_at"]),
            created_at=self._parse_iso(row["created_at"]),
            updated_at=self._parse_iso(row["updated_at"]),
        )

    def _row_to_attempt(self, row: aiosqlite.Row) -> DeliveryAttempt:
        """Convert a SQLite row into a DeliveryAttempt domain model."""
        return DeliveryAttempt(
            id=row["id"],
            delivery_id=row["delivery_id"],
            attempt_number=row["attempt_number"],
            outcome=AttemptOutcome(row["outcome"]),
            status_code=row["status_code"],
            response_body=row["response_body"],
            error_message=row["error_message"],
            execution_duration_ms=row["execution_duration_ms"],
            attempted_at=self._parse_iso(row["attempted_at"]),
        )

    def _row_to_dlq(self, row: aiosqlite.Row) -> DeadLetterRecord:
        """Convert a SQLite row into a DeadLetterRecord domain model."""
        last_replayed = (
            self._parse_iso(row["last_replayed_at"]) if row["last_replayed_at"] else None
        )
        return DeadLetterRecord(
            id=row["id"],
            delivery_id=row["delivery_id"],
            failure_reason=row["failure_reason"],
            moved_to_dlq_at=self._parse_iso(row["moved_to_dlq_at"]),
            replay_count=row["replay_count"],
            last_replayed_at=last_replayed,
        )

    async def create_delivery(
        self,
        inbound: WebhookCreate,
        default_max_attempts: int = 5,
    ) -> tuple[WebhookDelivery, bool]:
        """
        Store a new webhook delivery task.

        Returns:
            A tuple of (WebhookDelivery, created: bool). If idempotency key already exists,
            returns the existing delivery record and created=False.
        """
        now = utc_now()
        now_iso = now.isoformat()
        delivery_id = str(uuid.uuid4())
        max_retries = inbound.max_attempts or default_max_attempts
        payload_str = json.dumps(inbound.payload)
        headers_str = json.dumps(inbound.headers)

        async with self.db.connect() as conn:
            if inbound.idempotency_key:
                cursor = await conn.execute(
                    "SELECT * FROM webhook_deliveries WHERE idempotency_key = ?",
                    (inbound.idempotency_key,),
                )
                existing = await cursor.fetchone()
                if existing:
                    logger.debug("Idempotency hit for key: %s", inbound.idempotency_key)
                    return self._row_to_delivery(existing), False

            try:
                await conn.execute(
                    """
                    INSERT INTO webhook_deliveries (
                        id, destination_url, http_method, payload_raw, headers_raw,
                        secret_token, status, attempt_count, max_attempts,
                        idempotency_key, next_retry_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        delivery_id,
                        str(inbound.destination_url),
                        inbound.http_method.value,
                        payload_str,
                        headers_str,
                        inbound.secret_token,
                        WebhookStatus.PENDING.value,
                        0,
                        max_retries,
                        inbound.idempotency_key,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
                await conn.commit()
            except sqlite3.IntegrityError:
                # Concurrent insertion with same idempotency key
                if inbound.idempotency_key:
                    cursor = await conn.execute(
                        "SELECT * FROM webhook_deliveries WHERE idempotency_key = ?",
                        (inbound.idempotency_key,),
                    )
                    existing = await cursor.fetchone()
                    if existing:
                        return self._row_to_delivery(existing), False
                raise

            cursor = await conn.execute(
                "SELECT * FROM webhook_deliveries WHERE id = ?",
                (delivery_id,),
            )
            created_row = await cursor.fetchone()
            assert created_row is not None
            return self._row_to_delivery(created_row), True

    async def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        """Retrieve a webhook delivery by its primary ID."""
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                "SELECT * FROM webhook_deliveries WHERE id = ?",
                (delivery_id,),
            )
            row = await cursor.fetchone()
            return self._row_to_delivery(row) if row else None

    async def list_deliveries(
        self,
        status: WebhookStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WebhookDelivery]:
        """Fetch a paginated list of deliveries, optionally filtered by status."""
        query = "SELECT * FROM webhook_deliveries"
        params: list[str | int] = []

        if status:
            query += " WHERE status = ?"
            params.append(status.value)

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        async with self.db.connect() as conn:
            cursor = await conn.execute(query, tuple(params))
            rows = await cursor.fetchall()
            return [self._row_to_delivery(r) for r in rows]

    async def claim_due_deliveries(self, batch_size: int = 20) -> list[WebhookDelivery]:
        """
        Atomically find and claim due PENDING deliveries, transitioning them to PROCESSING.

        This ensures worker processes do not process the same delivery concurrently.
        """
        now_iso = utc_now().isoformat()
        async with self.db.connect() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            cursor = await conn.execute(
                """
                SELECT id FROM webhook_deliveries
                WHERE status = ? AND next_retry_at <= ?
                ORDER BY next_retry_at ASC
                LIMIT ?
                """,
                (WebhookStatus.PENDING.value, now_iso, batch_size),
            )
            rows = await cursor.fetchall()
            if not rows:
                await conn.commit()
                return []

            ids = [r["id"] for r in rows]
            placeholders = ",".join("?" for _ in ids)
            updated_iso = utc_now().isoformat()

            await conn.execute(
                f"""
                UPDATE webhook_deliveries
                SET status = ?, updated_at = ?
                WHERE id IN ({placeholders})
                """,
                (WebhookStatus.PROCESSING.value, updated_iso, *ids),
            )
            await conn.commit()

            cursor = await conn.execute(
                f"SELECT * FROM webhook_deliveries WHERE id IN ({placeholders})",
                tuple(ids),
            )
            claimed = await cursor.fetchall()
            return [self._row_to_delivery(r) for r in claimed]

    async def record_attempt(self, attempt: DeliveryAttempt) -> None:
        """Insert an execution attempt record."""
        async with self.db.connect() as conn:
            await conn.execute(
                """
                INSERT INTO delivery_attempts (
                    id, delivery_id, attempt_number, outcome, status_code,
                    response_body, error_message, execution_duration_ms, attempted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.id,
                    attempt.delivery_id,
                    attempt.attempt_number,
                    attempt.outcome.value,
                    attempt.status_code,
                    attempt.response_body,
                    attempt.error_message,
                    attempt.execution_duration_ms,
                    attempt.attempted_at.isoformat(),
                ),
            )
            await conn.commit()

    async def mark_delivered(self, delivery_id: str, attempt_count: int) -> None:
        """Update delivery record to terminal DELIVERED status."""
        now_iso = utc_now().isoformat()
        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = ?, attempt_count = ?, updated_at = ?
                WHERE id = ?
                """,
                (WebhookStatus.DELIVERED.value, attempt_count, now_iso, delivery_id),
            )
            await conn.commit()

    async def schedule_retry(
        self,
        delivery_id: str,
        attempt_count: int,
        next_retry_at: datetime,
    ) -> None:
        """Re-queue delivery to PENDING with a future scheduled execution time."""
        now_iso = utc_now().isoformat()
        next_iso = next_retry_at.isoformat()
        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = ?, attempt_count = ?, next_retry_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (WebhookStatus.PENDING.value, attempt_count, next_iso, now_iso, delivery_id),
            )
            await conn.commit()

    async def move_to_dead_letter(
        self,
        delivery_id: str,
        failure_reason: str,
        attempt_count: int,
    ) -> DeadLetterRecord:
        """Transition delivery to DEAD_LETTER status and create a DLQ entry."""
        now = utc_now()
        now_iso = now.isoformat()
        dlq_id = str(uuid.uuid4())

        async with self.db.connect() as conn:
            await conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = ?, attempt_count = ?, updated_at = ?
                WHERE id = ?
                """,
                (WebhookStatus.DEAD_LETTER.value, attempt_count, now_iso, delivery_id),
            )
            await conn.execute(
                """
                INSERT OR REPLACE INTO dead_letter_records (
                    id, delivery_id, failure_reason, moved_to_dlq_at, replay_count, last_replayed_at
                ) VALUES (?, ?, ?, ?, 0, NULL)
                """,
                (dlq_id, delivery_id, failure_reason, now_iso),
            )
            await conn.commit()

        return DeadLetterRecord(
            id=dlq_id,
            delivery_id=delivery_id,
            failure_reason=failure_reason,
            moved_to_dlq_at=now,
            replay_count=0,
            last_replayed_at=None,
        )

    async def list_dead_letters(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> list[DeadLetterRecord]:
        """Fetch paginated dead letter records."""
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM dead_letter_records
                ORDER BY moved_to_dlq_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
            rows = await cursor.fetchall()
            return [self._row_to_dlq(r) for r in rows]

    async def requeue_dead_letter(
        self,
        delivery_id: str,
        reset_attempts: bool = False,
    ) -> bool:
        """
        Re-queue a dead-lettered delivery back into active PENDING state for immediate processing.
        """
        now = utc_now()
        now_iso = now.isoformat()
        async with self.db.connect() as conn:
            cursor = await conn.execute(
                "SELECT * FROM dead_letter_records WHERE delivery_id = ?",
                (delivery_id,),
            )
            dlq_row = await cursor.fetchone()
            if not dlq_row:
                return False

            replay_count = dlq_row["replay_count"] + 1
            await conn.execute(
                """
                UPDATE dead_letter_records
                SET replay_count = ?, last_replayed_at = ?
                WHERE delivery_id = ?
                """,
                (replay_count, now_iso, delivery_id),
            )

            if reset_attempts:
                await conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = ?, attempt_count = 0, next_retry_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (WebhookStatus.PENDING.value, now_iso, now_iso, delivery_id),
                )
            else:
                # Keep attempt count, but schedule for immediate pickup
                await conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = ?, next_retry_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (WebhookStatus.PENDING.value, now_iso, now_iso, delivery_id),
                )

            await conn.commit()
            return True

    async def get_delivery_detail(self, delivery_id: str) -> DeliveryDetail | None:
        """Fetch delivery domain model, all associated attempts, and DLQ record if present."""
        delivery = await self.get_delivery(delivery_id)
        if not delivery:
            return None

        async with self.db.connect() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM delivery_attempts
                WHERE delivery_id = ?
                ORDER BY attempt_number ASC
                """,
                (delivery_id,),
            )
            attempt_rows = await cursor.fetchall()
            attempts = [self._row_to_attempt(r) for r in attempt_rows]

            dlq_cursor = await conn.execute(
                "SELECT * FROM dead_letter_records WHERE delivery_id = ?",
                (delivery_id,),
            )
            dlq_row = await dlq_cursor.fetchone()
            dlq_record = self._row_to_dlq(dlq_row) if dlq_row else None

        return DeliveryDetail(
            delivery=delivery,
            attempts=attempts,
            dead_letter_record=dlq_record,
        )
