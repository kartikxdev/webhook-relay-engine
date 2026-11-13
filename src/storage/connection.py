"""SQLite database connection management with WAL configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from src.config import DatabaseConfig
from src.storage.schema import CREATE_TABLES_SQL

logger = logging.getLogger("relayforge.storage")


class DatabaseManager:
    """Manages SQLite connection lifecycle, PRAGMA optimizations, and migrations."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._db_path = config.db_path

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Context manager providing an configured aiosqlite connection."""
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row

            # Apply robust high-concurrency pragmas
            await conn.execute(f"PRAGMA busy_timeout = {self.config.busy_timeout_ms};")
            await conn.execute("PRAGMA foreign_keys = ON;")

            if self.config.enable_wal and self._db_path != ":memory:":
                await conn.execute("PRAGMA journal_mode = WAL;")
                await conn.execute("PRAGMA synchronous = NORMAL;")

            yield conn

    async def initialize_schema(self) -> None:
        """Execute DDL statements to set up tables and indexes."""
        async with self.connect() as conn:
            await conn.executescript(CREATE_TABLES_SQL)
            await conn.commit()
            logger.info("SQLite schema initialized successfully at '%s'", self._db_path)
