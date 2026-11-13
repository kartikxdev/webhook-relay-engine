"""Core domain and validation models for webhooks, attempts, and DLQ records."""

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator

from src.models.enums import AttemptOutcome, HttpMethod, WebhookStatus


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(UTC)


class WebhookCreate(BaseModel):
    """Schema for ingesting a new webhook relay request."""

    destination_url: HttpUrl = Field(
        ...,
        description="The target HTTP endpoint where the webhook payload will be relayed.",
        examples=["https://api.example.com/webhooks/listener"],
    )
    http_method: HttpMethod = Field(
        default=HttpMethod.POST,
        description="HTTP method used for outbound forwarding.",
    )
    payload: dict[str, Any] = Field(
        ...,
        description="The JSON payload to deliver to the destination endpoint.",
        examples=[{"event": "order.completed", "order_id": "ord_98765", "amount": 149.99}],
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Custom HTTP headers to attach to the outbound request.",
    )
    secret_token: str | None = Field(
        default=None,
        description="Secret key used to compute an HMAC-SHA256 signature for payload verification.",
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=128,
        description="Optional unique client idempotency key to prevent duplicate ingestions.",
    )
    max_attempts: int | None = Field(
        default=None,
        ge=1,
        le=20,
        description="Override the default maximum retry attempts for this specific delivery.",
    )

    @field_validator("destination_url")
    @classmethod
    def validate_url(cls, v: HttpUrl) -> HttpUrl:
        scheme = v.scheme.lower() if v.scheme else ""
        if scheme not in ("http", "https"):
            raise ValueError("Destination URL must use http or https scheme")
        return v


class WebhookDelivery(BaseModel):
    """Domain model representing a stored webhook delivery task."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    destination_url: str
    http_method: HttpMethod = HttpMethod.POST
    payload_raw: str
    headers_raw: str
    secret_token: str | None = None
    status: WebhookStatus = WebhookStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 5
    idempotency_key: str | None = None
    next_retry_at: datetime = Field(default_factory=utc_now)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @property
    def payload(self) -> dict[str, Any]:
        """Parsed dictionary payload."""
        try:
            return json.loads(self.payload_raw)
        except Exception:
            return {}

    @property
    def headers(self) -> dict[str, str]:
        """Parsed dictionary headers."""
        try:
            return json.loads(self.headers_raw)
        except Exception:
            return {}


class DeliveryAttempt(BaseModel):
    """Execution telemetry record for an individual HTTP delivery attempt."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    delivery_id: str
    attempt_number: int
    outcome: AttemptOutcome
    status_code: int | None = None
    response_body: str | None = None
    error_message: str | None = None
    execution_duration_ms: float
    attempted_at: datetime = Field(default_factory=utc_now)


class DeadLetterRecord(BaseModel):
    """Dead-Letter Queue (DLQ) persistent item."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    delivery_id: str
    failure_reason: str
    moved_to_dlq_at: datetime = Field(default_factory=utc_now)
    replay_count: int = 0
    last_replayed_at: datetime | None = None


class DeliveryDetail(BaseModel):
    """Comprehensive delivery view including audit trail of attempts."""

    delivery: WebhookDelivery
    attempts: list[DeliveryAttempt] = Field(default_factory=list)
    dead_letter_record: DeadLetterRecord | None = None


class ReplayResult(BaseModel):
    """Response payload following a replay trigger."""

    delivery_id: str
    status: WebhookStatus
    previous_attempt_count: int
    message: str
