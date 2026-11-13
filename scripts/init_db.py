"""Utility script to verify and initialize the SQLite database schema."""

import asyncio
import logging

from src.config import get_settings
from src.storage.connection import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("relayforge.scripts.init_db")


async def main() -> None:
    settings = get_settings()
    logger.info("Initializing database at: %s", settings.database.db_path)
    db_manager = DatabaseManager(settings.database)
    await db_manager.initialize_schema()
    logger.info("Database schema initialized and verified successfully.")


if __name__ == "__main__":
    asyncio.run(main())
