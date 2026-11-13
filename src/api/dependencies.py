"""FastAPI dependency injection providers."""

from collections.abc import AsyncGenerator

from fastapi import Header, HTTPException, status

from src.config import Settings, get_settings
from src.storage.connection import DatabaseManager
from src.storage.repository import WebhookRepository

# Cached application instances
_db_manager: DatabaseManager | None = None
_repository: WebhookRepository | None = None


def get_app_settings() -> Settings:
    """Dependency provider for global application settings."""
    return get_settings()


def get_database_manager() -> DatabaseManager:
    """Dependency provider for DatabaseManager singleton."""
    global _db_manager
    if _db_manager is None:
        settings = get_app_settings()
        _db_manager = DatabaseManager(settings.database)
    return _db_manager


async def get_repository() -> AsyncGenerator[WebhookRepository, None]:
    """Dependency provider for WebhookRepository."""
    global _repository
    if _repository is None:
        db = get_database_manager()
        _repository = WebhookRepository(db)
    yield _repository


async def verify_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Verify API key authentication if enabled in security settings."""
    settings = get_app_settings()
    if not settings.security.require_api_key:
        return

    if not x_api_key or x_api_key != settings.security.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key authentication header",
        )
