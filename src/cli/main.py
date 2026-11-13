"""RelayForge developer CLI for managing workers, inspection, and DLQ operations."""

import asyncio
import json
import logging
import sys

import click
import uvicorn

from src.config import get_settings
from src.engine.worker import AsyncRelayWorker
from src.models.delivery import WebhookCreate
from src.models.enums import HttpMethod
from src.storage.connection import DatabaseManager
from src.storage.repository import WebhookRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("relayforge.cli")


@click.group()
def cli() -> None:
    """RelayForge: Production-grade webhook relay and retry engine."""


@cli.command("worker")
@click.option("--concurrency", "-c", type=int, default=None, help="Worker concurrency override")
def run_worker(concurrency: int | None) -> None:
    """Start the asynchronous background retry worker loop."""
    settings = get_settings()
    if concurrency:
        settings.worker.concurrency = concurrency

    async def _async_worker_main() -> None:
        db = DatabaseManager(settings.database)
        await db.initialize_schema()
        repo = WebhookRepository(db)
        worker = AsyncRelayWorker(repo, settings.worker, settings.retry)

        click.echo(
            f"Starting RelayForge Worker (concurrency={settings.worker.concurrency}, db='{settings.database.db_path}')..."
        )
        try:
            await worker.run()
        except KeyboardInterrupt:
            click.echo("\nInterrupted by user. Shutting down worker...")
            await worker.stop()

    try:
        asyncio.run(_async_worker_main())
    except KeyboardInterrupt:
        click.echo("Worker stopped.")


@cli.command("api")
@click.option("--host", default="127.0.0.1", help="Bind host address")
@click.option("--port", "-p", type=int, default=8000, help="Bind port number")
@click.option("--reload/--no-reload", default=False, help="Enable auto-reload for development")
def run_api(host: str, port: int, reload: bool) -> None:
    """Run the RelayForge FastAPI server."""
    click.echo(f"Starting RelayForge REST API on http://{host}:{port}...")
    uvicorn.run("src.api.app:app", host=host, port=port, reload=reload)


@cli.command("ingest")
@click.option("--url", "-u", required=True, help="Destination endpoint URL")
@click.option("--payload", "-d", required=True, help="JSON payload string or @filename")
@click.option("--method", "-m", type=click.Choice(["POST", "PUT", "PATCH"]), default="POST")
@click.option("--secret", "-s", default=None, help="HMAC secret token")
@click.option("--idempotency-key", "-k", default=None, help="Unique idempotency key")
def ingest_command(
    url: str,
    payload: str,
    method: str,
    secret: str | None,
    idempotency_key: str | None,
) -> None:
    """Enqueue a new webhook delivery into RelayForge."""
    settings = get_settings()

    # Load payload
    if payload.startswith("@"):
        with open(payload[1:], encoding="utf-8") as f:
            payload_data = json.load(f)
    else:
        try:
            payload_data = json.loads(payload)
        except json.JSONDecodeError as exc:
            click.echo(f"Error parsing JSON payload: {exc}", err=True)
            sys.exit(1)

    webhook_in = WebhookCreate(
        destination_url=url,  # type: ignore[arg-type]
        http_method=HttpMethod(method),
        payload=payload_data,
        secret_token=secret,
        idempotency_key=idempotency_key,
    )

    async def _do_ingest() -> None:
        db = DatabaseManager(settings.database)
        await db.initialize_schema()
        repo = WebhookRepository(db)
        delivery, created = await repo.create_delivery(webhook_in, settings.retry.max_attempts)
        if created:
            click.echo(f"Enqueued new webhook delivery: {delivery.id} (status={delivery.status})")
        else:
            click.echo(
                f"Idempotency hit: Returned existing delivery {delivery.id} (status={delivery.status})"
            )

    asyncio.run(_do_ingest())


@cli.command("status")
@click.argument("delivery_id")
def status_command(delivery_id: str) -> None:
    """Inspect delivery execution status and attempt telemetry."""
    settings = get_settings()

    async def _do_status() -> None:
        db = DatabaseManager(settings.database)
        repo = WebhookRepository(db)
        detail = await repo.get_delivery_detail(delivery_id)
        if not detail:
            click.echo(f"Delivery '{delivery_id}' not found.", err=True)
            sys.exit(1)

        d = detail.delivery
        click.echo("=" * 60)
        click.echo(f"Delivery ID:      {d.id}")
        click.echo(f"Status:           {d.status}")
        click.echo(f"Destination:      {d.destination_url} [{d.http_method}]")
        click.echo(f"Attempts:         {d.attempt_count} / {d.max_attempts}")
        click.echo(f"Next Retry At:    {d.next_retry_at.isoformat()}")
        click.echo(f"Created At:       {d.created_at.isoformat()}")

        if detail.dead_letter_record:
            dlq = detail.dead_letter_record
            click.echo(f"DLQ Reason:       {dlq.failure_reason}")
            click.echo(f"DLQ Moved At:     {dlq.moved_to_dlq_at.isoformat()}")
            click.echo(f"DLQ Replay Count: {dlq.replay_count}")

        click.echo("-" * 60)
        click.echo(f"Execution History ({len(detail.attempts)} attempts):")
        for att in detail.attempts:
            status_text = att.status_code if att.status_code is not None else "N/A"
            err = f" | Err: {att.error_message}" if att.error_message else ""
            click.echo(
                f"  Attempt #{att.attempt_number}: {att.outcome} (HTTP {status_text}, {att.execution_duration_ms:.1f}ms){err}"
            )
        click.echo("=" * 60)

    asyncio.run(_do_status())


@cli.group("dlq")
def dlq_group() -> None:
    """Manage and inspect Dead-Letter Queue (DLQ) records."""


@dlq_group.command("list")
@click.option("--limit", "-n", type=int, default=20, help="Maximum records to list")
def dlq_list_command(limit: int) -> None:
    """List dead-letter queue records."""
    settings = get_settings()

    async def _do_list() -> None:
        db = DatabaseManager(settings.database)
        repo = WebhookRepository(db)
        records = await repo.list_dead_letters(limit=limit)
        if not records:
            click.echo("Dead-Letter Queue is empty. No failed deliveries.")
            return

        click.echo(f"Found {len(records)} dead-lettered deliveries:")
        click.echo(f"{'Delivery ID':<38} {'Moved At':<26} {'Replays':<8} {'Reason'}")
        click.echo("-" * 90)
        for r in records:
            click.echo(
                f"{r.delivery_id:<38} {r.moved_to_dlq_at.isoformat()[:25]:<26} {r.replay_count:<8} {r.failure_reason}"
            )

    asyncio.run(_do_list())


@dlq_group.command("replay")
@click.argument("delivery_id")
@click.option("--reset-attempts", is_flag=True, default=False, help="Reset attempt counter to 0")
def dlq_replay_command(delivery_id: str, reset_attempts: bool) -> None:
    """Requeue a dead-letter delivery for immediate processing."""
    settings = get_settings()

    async def _do_replay() -> None:
        db = DatabaseManager(settings.database)
        repo = WebhookRepository(db)
        success = await repo.requeue_dead_letter(delivery_id, reset_attempts=reset_attempts)
        if success:
            click.echo(f"Successfully re-queued delivery {delivery_id} into PENDING queue.")
        else:
            click.echo(f"Delivery {delivery_id} not found in DLQ.", err=True)
            sys.exit(1)

    asyncio.run(_do_replay())


if __name__ == "__main__":
    cli()
