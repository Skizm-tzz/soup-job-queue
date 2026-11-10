"""Connections. Every connection is autocommit: one statement = one transaction."""

from __future__ import annotations

import psycopg
from psycopg_pool import AsyncConnectionPool

from app.config import settings


async def _configure(conn: psycopg.AsyncConnection) -> None:
    # psycopg opens a transaction on the first statement unless autocommit is on;
    # an open transaction would hold row locks across round trips. Refuse to run.
    if not conn.autocommit:
        raise RuntimeError("connection is not in autocommit mode")
    await conn.execute(f"SET default_transaction_isolation = '{settings.isolation}'")
    await conn.execute(f"SET statement_timeout = {int(settings.statement_timeout_ms)}")


async def connect(application_name: str) -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(
        settings.database_url, autocommit=True, application_name=application_name, connect_timeout=5
    )
    await _configure(conn)
    return conn


def make_pool(application_name: str) -> AsyncConnectionPool:
    return AsyncConnectionPool(
        settings.database_url,
        min_size=2,
        max_size=settings.api_pool_max,
        kwargs={"autocommit": True, "application_name": application_name, "connect_timeout": 5},
        configure=_configure,
        check=AsyncConnectionPool.check_connection,
        open=False,
    )


async def session_settings(conn: psycopg.AsyncConnection) -> dict:
    cur = await conn.execute("SHOW transaction_isolation")
    iso = (await cur.fetchone())[0]
    return {"autocommit": conn.autocommit, "transaction_isolation": iso}
