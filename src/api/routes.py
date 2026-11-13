"""REST API route handlers for Webhook Ingestion, Inspection, and DLQ Replay."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from src.api.dependencies import get_app_settings, get_repository, verify_api_key
from src.config import Settings
from src.models.delivery import (
    DeadLetterRecord,
    DeliveryDetail,
    ReplayResult,
    WebhookCreate,
    WebhookDelivery,
)
from src.models.enums import WebhookStatus
from src.storage.repository import WebhookRepository

router = APIRouter(prefix="/api/v1", tags=["Webhooks"], dependencies=[Depends(verify_api_key)])


@router.post(
    "/webhooks/ingest",
    response_model=WebhookDelivery,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest a new webhook for reliable relay",
)
async def ingest_webhook(
    inbound: WebhookCreate,
    response: Response,
    repo: Annotated[WebhookRepository, Depends(get_repository)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> WebhookDelivery:
    """
    Ingest a webhook relay job.

    - Supports client idempotency: duplicate requests return HTTP 200 with the existing record.
    - Queues payload in SQLite WAL storage for background dispatch.
    """
    delivery, created = await repo.create_delivery(
        inbound=inbound,
        default_max_attempts=settings.retry.max_attempts,
    )
    if not created:
        response.status_code = status.HTTP_200_OK
    return delivery


@router.get(
    "/deliveries",
    response_model=list[WebhookDelivery],
    summary="List queued and processed deliveries",
)
async def list_deliveries(
    repo: Annotated[WebhookRepository, Depends(get_repository)],
    status_filter: Annotated[WebhookStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[WebhookDelivery]:
    """Retrieve paginated webhook deliveries, optionally filtered by status."""
    return await repo.list_deliveries(status=status_filter, limit=limit, offset=offset)


@router.get(
    "/deliveries/{delivery_id}",
    response_model=DeliveryDetail,
    summary="Retrieve delivery details with full attempt telemetry",
)
async def get_delivery_detail(
    delivery_id: str,
    repo: Annotated[WebhookRepository, Depends(get_repository)],
) -> DeliveryDetail:
    """Fetch complete delivery record, execution attempts history, and DLQ status."""
    detail = await repo.get_delivery_detail(delivery_id)
    if not detail:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Webhook delivery '{delivery_id}' not found",
        )
    return detail


@router.get(
    "/dlq",
    response_model=list[DeadLetterRecord],
    summary="List exhausted dead-letter queue records",
)
async def list_dead_letters(
    repo: Annotated[WebhookRepository, Depends(get_repository)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[DeadLetterRecord]:
    """Fetch dead-letter queue entries awaiting manual inspection or replay."""
    return await repo.list_dead_letters(limit=limit, offset=offset)


@router.post(
    "/dlq/{delivery_id}/replay",
    response_model=ReplayResult,
    summary="Replay a dead-lettered webhook delivery",
)
async def replay_dead_letter(
    delivery_id: str,
    repo: Annotated[WebhookRepository, Depends(get_repository)],
    reset_attempts: Annotated[bool, Query(description="Reset attempt counter to 0")] = False,
) -> ReplayResult:
    """Requeue a dead-letter delivery back into active PENDING state for immediate worker pickup."""
    delivery = await repo.get_delivery(delivery_id)
    if not delivery:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Webhook delivery '{delivery_id}' not found",
        )

    success = await repo.requeue_dead_letter(delivery_id, reset_attempts=reset_attempts)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Delivery '{delivery_id}' is not in the Dead-Letter Queue",
        )

    return ReplayResult(
        delivery_id=delivery_id,
        status=WebhookStatus.PENDING,
        previous_attempt_count=delivery.attempt_count,
        message="Delivery successfully re-queued for processing",
    )


# System healthcheck router (unauthenticated)
health_router = APIRouter(tags=["Health"])


@health_router.get("/healthz", summary="Health check endpoint")
async def health_check(
    repo: Annotated[WebhookRepository, Depends(get_repository)],
) -> dict[str, str]:
    """Verify API liveness and database connectivity."""
    try:
        # Perform quick select 1 on the database
        async with repo.db.connect() as conn:
            cursor = await conn.execute("SELECT 1")
            await cursor.fetchone()
        db_status = "healthy"
    except Exception as exc:
        db_status = f"unhealthy: {exc}"

    return {
        "status": "ok" if db_status == "healthy" else "degraded",
        "database": db_status,
        "service": "RelayForge",
    }
