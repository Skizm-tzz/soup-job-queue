"""The queue protocol: one function per operation, one SQL statement per state transition.

Every statement runs in autocommit and carries its precondition in WHERE
(compare-and-set). Under READ COMMITTED, an UPDATE that waited on a row lock
re-checks WHERE against the newest committed row version, so a transition either
applies to the state it expects or matches 0 rows. No function reads a row,
decides in Python, and writes it back (except where a seeded bug does exactly
that, see app/faults.py).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app import faults
from app.config import settings

JOB_COLUMNS = (
    "id, idempotency_key, payload, payload_hash, state, attempt, max_attempts, "
    "lease_owner, lease_expires_at, created_at, started_at, finished_at, error"
)
STATES = ("queued", "running", "succeeded", "failed", "cancelled")

# Errors after which the statement definitely did not apply, so retrying it is safe.
# (A lost connection or a timeout is different: the outcome is unknown, and the
# caller must treat it as such.)
_RETRYABLE = (psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected)
_MAX_RETRIES = 8

retry_count = 0  # serialization/deadlock retries in this process, for diagnostics


class IdempotencyConflict(Exception):
    """Same Idempotency-Key, different request body."""

    def __init__(self, job: dict):
        super().__init__("idempotency key reused with a different payload")
        self.job = job


async def _one(conn: psycopg.AsyncConnection, sql: str, params: Any = None) -> dict | None:
    global retry_count
    for i in range(_MAX_RETRIES):
        try:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                return await cur.fetchone() if cur.description else None
        except _RETRYABLE:
            if i == _MAX_RETRIES - 1:
                raise
            retry_count += 1
            await asyncio.sleep(random.uniform(0, 0.005 * 2**i))
    raise AssertionError("unreachable")


async def _all(conn: psycopg.AsyncConnection, sql: str, params: Any = None) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


def payload_hash(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


# --------------------------------------------------------------------- create


async def create_job(conn, key: str, payload: dict, max_attempts: int) -> tuple[dict, bool]:
    """Returns (job, created). Raises IdempotencyConflict on a different body."""
    h = payload_hash(payload)
    params = {"key": key, "payload": Jsonb(payload), "hash": h, "max": max_attempts}

    if faults.on("CREATE_CHECK_THEN_INSERT"):
        job = await _one(conn, f"SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = %(key)s", params)
        created = job is None
        if created:
            job = await _one(
                conn,
                f"""INSERT INTO jobs (idempotency_key, payload, payload_hash, max_attempts)
                    VALUES (%(key)s, %(payload)s, %(hash)s, %(max)s) RETURNING {JOB_COLUMNS}""",
                params,
            )
    else:
        job = await _one(
            conn,
            f"""INSERT INTO jobs (idempotency_key, payload, payload_hash, max_attempts)
                VALUES (%(key)s, %(payload)s, %(hash)s, %(max)s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING {JOB_COLUMNS}""",
            params,
        )
        created = job is not None
        if not created:
            # A separate statement, so it gets a fresh snapshot that includes the
            # row whose insert made ours conflict (ON CONFLICT waited for it).
            job = await _one(conn, f"SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = %(key)s", params)
            if job is None:
                raise RuntimeError("conflicting row not visible; this should be impossible")

    if job["payload_hash"] != h:
        raise IdempotencyConflict(job)
    return job, created


# ----------------------------------------------------------------------- read


async def get_job(conn, job_id: int) -> dict | None:
    return await _one(conn, f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,))


async def list_jobs(conn, state: str | None, after_id: int, limit: int) -> list[dict]:
    """Keyset pagination by id: stable under concurrent inserts, unlike OFFSET."""
    if state is None:
        return await _all(
            conn, f"SELECT {JOB_COLUMNS} FROM jobs WHERE id > %s ORDER BY id LIMIT %s", (after_id, limit)
        )
    return await _all(
        conn,
        f"SELECT {JOB_COLUMNS} FROM jobs WHERE state = %s AND id > %s ORDER BY id LIMIT %s",
        (state, after_id, limit),
    )


# ---------------------------------------------------------------------- claim

_CLAIM = """
WITH c AS MATERIALIZED (
  SELECT id FROM jobs
  WHERE state IN ('queued','running')
    AND (state = 'queued' OR lease_expires_at <= {now})
  ORDER BY id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET state = 'running',
    attempt = j.attempt + 1,
    lease_owner = %(worker)s,
    lease_expires_at = clock_timestamp() + make_interval(secs => %(ttl)s),
    started_at = clock_timestamp()
FROM c WHERE j.id = c.id
RETURNING j.id, j.attempt, j.max_attempts, j.payload
"""


async def claim(conn, worker_id: str, ttl_s: float) -> dict | None:
    """Take the next queued job, or take over a running job whose lease expired."""
    params = {"worker": worker_id, "ttl": float(ttl_s)}

    if faults.on("CLAIM_READ_THEN_WRITE"):
        row = await _one(
            conn,
            """SELECT id FROM jobs
               WHERE state IN ('queued','running')
                 AND (state = 'queued' OR lease_expires_at <= now())
               ORDER BY id LIMIT 1""",
        )
        if row is None:
            return None
        return await _one(
            conn,
            """UPDATE jobs SET state = 'running', attempt = attempt + 1, lease_owner = %(worker)s,
                      lease_expires_at = clock_timestamp() + make_interval(secs => %(ttl)s),
                      started_at = clock_timestamp()
               WHERE id = %(id)s
               RETURNING id, attempt, max_attempts, payload""",
            {**params, "id": row["id"]},
        )

    if faults.on("WORKER_CLOCK"):
        params["client_now"] = datetime.now(timezone.utc) + timedelta(seconds=settings.clock_skew_s)
        return await _one(conn, _CLAIM.format(now="%(client_now)s"), params)

    # now() is the statement's start time on the DB clock; no client time is used.
    return await _one(conn, _CLAIM.format(now="now()"), params)


async def heartbeat(conn, job_id: int, attempt: int, ttl_s: float) -> bool:
    """Renew the lease. False means the job was cancelled or taken over."""
    row = await _one(
        conn,
        """UPDATE jobs SET lease_expires_at = clock_timestamp() + make_interval(secs => %s)
           WHERE id = %s AND attempt = %s AND state = 'running'
           RETURNING id""",
        (float(ttl_s), job_id, attempt),
    )
    return row is not None


async def release_slot(conn, slot: str, own_worker_id: str) -> int:
    """Fast path after a restart: expire leases still held by earlier processes of this slot.

    Safe even if wrong: the lease only decides when a job may be taken over; a
    stale holder is fenced off by attempt.
    """
    row = await _one(
        conn,
        """WITH r AS (
             UPDATE jobs SET lease_expires_at = clock_timestamp()
             WHERE state = 'running' AND split_part(lease_owner, ':', 1) = %s AND lease_owner <> %s
             RETURNING id)
           SELECT count(*) AS n FROM r""",
        (slot, own_worker_id),
    )
    return row["n"]


# -------------------------------------------------------------------- finish


async def complete(conn, job_id: int, attempt: int, worker_id: str) -> bool:
    """running -> succeeded and the effect, in one statement. False = stale, nothing written."""
    p = {"id": job_id, "attempt": attempt, "worker": worker_id}

    if faults.on("SPLIT_EFFECT"):
        await _one(conn, "INSERT INTO effects (job_id, attempt, worker_id) VALUES (%(id)s, %(attempt)s, %(worker)s)", p)
        row = await _one(
            conn,
            """UPDATE jobs SET state = 'succeeded', finished_at = clock_timestamp(), lease_owner = NULL
               WHERE id = %(id)s AND attempt = %(attempt)s AND state = 'running' RETURNING id""",
            p,
        )
        return row is not None

    guard = "id = %(id)s" if faults.on("NO_FENCE") else "id = %(id)s AND attempt = %(attempt)s AND state = 'running'"
    row = await _one(
        conn,
        f"""WITH done AS (
              UPDATE jobs SET state = 'succeeded', finished_at = clock_timestamp(), lease_owner = NULL
              WHERE {guard}
              RETURNING id)
            INSERT INTO effects (job_id, attempt, worker_id)
            SELECT id, %(attempt)s, %(worker)s FROM done
            RETURNING job_id""",
        p,
    )
    return row is not None


async def fail(conn, job_id: int, attempt: int, error: str) -> bool:
    row = await _one(
        conn,
        """UPDATE jobs SET state = 'failed', finished_at = clock_timestamp(), error = %s, lease_owner = NULL
           WHERE id = %s AND attempt = %s AND state = 'running'
           RETURNING id""",
        (error, job_id, attempt),
    )
    return row is not None


async def cancel(conn, job_id: int) -> tuple[str, dict | None]:
    """Returns (outcome, job); outcome is cancelled | already_cancelled | conflict | not_found."""
    guard = "id = %s" if faults.on("CANCEL_BLIND") else "id = %s AND state IN ('queued','running')"
    job = await _one(
        conn,
        f"""UPDATE jobs SET state = 'cancelled', finished_at = clock_timestamp(), lease_owner = NULL
            WHERE {guard}
            RETURNING {JOB_COLUMNS}""",
        (job_id,),
    )
    if job is not None:
        return "cancelled", job
    job = await get_job(conn, job_id)
    if job is None:
        return "not_found", None
    if job["state"] == "cancelled":
        return "already_cancelled", job
    return "conflict", job


# ---------------------------------------------------------------- bookkeeping


async def record_execution(conn, job_id: int, attempt: int, worker_id: str) -> None:
    await _one(
        conn,
        "INSERT INTO executions (job_id, attempt, worker_id) VALUES (%s, %s, %s)",
        (job_id, attempt, worker_id),
    )


async def diag(conn, kind: str, job_id: int | None, attempt: int | None, actor: str, detail: dict | None = None) -> None:
    await _one(
        conn,
        "INSERT INTO diag_events (kind, job_id, attempt, actor, detail) VALUES (%s, %s, %s, %s, %s)",
        (kind, job_id, attempt, actor, Jsonb(detail) if detail is not None else None),
    )
