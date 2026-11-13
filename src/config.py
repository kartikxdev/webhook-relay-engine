"""Configuration loader using Pydantic Settings."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseModel):
    """Database connection and concurrency settings."""

    db_path: str = Field(
        default="data/relayforge.db",
        description="Path to SQLite database file. Use ':memory:' for transient in-memory databases.",
    )
    busy_timeout_ms: int = Field(
        default=5000,
        ge=500,
        le=60000,
        description="SQLite busy timeout in milliseconds to handle concurrent writers gracefully.",
    )
    enable_wal: bool = Field(
        default=True,
        description="Enable Write-Ahead Logging (WAL) mode for high-concurrency read/write operations.",
    )


class RetryConfig(BaseModel):
    """Exponential backoff and jitter configuration."""

    max_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum delivery attempts before dispatching to Dead-Letter Queue (DLQ).",
    )
    base_backoff_seconds: float = Field(
        default=1.0,
        gt=0.0,
        description="Base backoff multiplier in seconds.",
    )
    max_backoff_seconds: float = Field(
        default=300.0,
        gt=1.0,
        description="Cap on maximum delay between retries in seconds.",
    )
    backoff_factor: float = Field(
        default=2.0,
        ge=1.1,
        description="Exponential growth factor for backoff calculation.",
    )
    jitter: bool = Field(
        default=True,
        description="Whether to apply full decorrelated jitter to avoid thundering herds.",
    )


class WorkerConfig(BaseModel):
    """Worker pool and dispatch engine settings."""

    concurrency: int = Field(
        default=8,
        ge=1,
        le=64,
        description="Number of concurrent dispatch tasks in the async worker pool.",
    )
    poll_interval_seconds: float = Field(
        default=0.5,
        gt=0.05,
        le=10.0,
        description="Polling cadence for checking scheduled retries and new events.",
    )
    batch_size: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of pending deliveries claimed in a single batch.",
    )
    request_timeout_seconds: float = Field(
        default=15.0,
        gt=1.0,
        le=120.0,
        description="Outbound HTTP client timeout per delivery attempt.",
    )


class SecurityConfig(BaseModel):
    """API security and signing credentials."""

    api_key: str | None = Field(
        default=None,
        description="Optional API key for authenticating incoming API ingest requests.",
    )
    require_api_key: bool = Field(
        default=False,
        description="Whether to strictly enforce API key authentication on protected endpoints.",
    )


class Settings(BaseSettings):
    """Global application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="RELAYFORGE_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "RelayForge"
    app_env: Literal["development", "testing", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)

    def ensure_data_directory(self) -> None:
        """Create the parent directory for SQLite database if it does not exist."""
        if self.database.db_path != ":memory:":
            db_file = Path(self.database.db_path)
            db_file.parent.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    """Factory helper to obtain settings instance."""
    settings = Settings()
    settings.ensure_data_directory()
    return settings
