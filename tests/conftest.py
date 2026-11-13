"""Shared pytest fixtures for RelayForge test suite."""

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import create_app
from src.api.dependencies import get_repository
from src.config import DatabaseConfig, RetryConfig, Settings, WorkerConfig
from src.storage.connection import DatabaseManager
from src.storage.repository import WebhookRepository


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    """Return fast testing configuration using isolated temp database."""
    db_file = tmp_path / "test.db"
    settings = Settings(
        app_env="testing",
        log_level="DEBUG",
        database=DatabaseConfig(
            db_path=str(db_file),
            busy_timeout_ms=1000,
            enable_wal=True,
        ),
        retry=RetryConfig(
            max_attempts=3,
            base_backoff_seconds=0.05,
            max_backoff_seconds=0.5,
            backoff_factor=2.0,
            jitter=False,
        ),
        worker=WorkerConfig(
            concurrency=4,
            poll_interval_seconds=0.05,
            batch_size=10,
            request_timeout_seconds=2.0,
        ),
    )
    return settings


@pytest.fixture
def worker_config(test_settings: Settings) -> WorkerConfig:
    """Worker configuration fixture."""
    return test_settings.worker


@pytest.fixture
def retry_config(test_settings: Settings) -> RetryConfig:
    """Retry configuration fixture."""
    return test_settings.retry


@pytest.fixture
async def db_manager(test_settings: Settings) -> AsyncGenerator[DatabaseManager, None]:
    """Provide an initialized in-memory database manager."""
    manager = DatabaseManager(test_settings.database)
    await manager.initialize_schema()
    yield manager


@pytest.fixture
async def repository(db_manager: DatabaseManager) -> AsyncGenerator[WebhookRepository, None]:
    """Provide WebhookRepository backed by in-memory SQLite."""
    repo = WebhookRepository(db_manager)
    yield repo


@pytest.fixture
async def async_client(
    test_settings: Settings,
    repository: WebhookRepository,
) -> AsyncGenerator[AsyncClient, None]:
    """Provide an HTTPX AsyncClient bound to the FastAPI app with test overrides."""
    app = create_app(test_settings)

    # Override repository dependency to use our in-memory test instance
    app.dependency_overrides[get_repository] = lambda: repository

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
