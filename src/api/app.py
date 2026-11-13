"""FastAPI application factory and lifespan manager."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.dependencies import get_database_manager
from src.api.routes import health_router
from src.api.routes import router as api_v1_router
from src.config import Settings, get_settings

logger = logging.getLogger("relayforge.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and shutdown lifecycle handler."""
    logger.info("Initializing database schema on API startup...")
    db = get_database_manager()
    await db.initialize_schema()
    yield
    logger.info("RelayForge API shut down cleanly.")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Instantiate and configure the FastAPI application."""
    if settings is None:
        settings = get_settings()

    app = FastAPI(
        title="RelayForge Webhook Relay Engine",
        version="0.1.0",
        description="Resilient, self-hosted webhook relay with exponential backoff and DLQ.",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router)
    app.include_router(api_v1_router)

    return app


# Default ASGI app instance for uvicorn
app = create_app()
