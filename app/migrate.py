"""Apply app/schema.sql. Run once before the API and workers start."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import psycopg

from app.config import settings

SCHEMA = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")


async def apply_schema(database_url: str) -> None:
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        await conn.execute("SELECT pg_advisory_lock(727001)")
        try:
            await conn.execute(SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(727001)")


async def main() -> int:
    for attempt in range(30):
        try:
            await apply_schema(settings.database_url)
            print("schema applied", flush=True)
            return 0
        except psycopg.OperationalError as e:
            print(f"database not ready ({e.__class__.__name__}), retrying", flush=True)
            await asyncio.sleep(1)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
