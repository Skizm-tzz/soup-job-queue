"""HTTP API. Stateless: every handler is one call into app.queue."""

from __future__ import annotations

import base64
import faulthandler
import signal
from contextlib import asynccontextmanager
from datetime import datetime

import psycopg
from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse

from app import db, faults, queue
from app.config import settings

pool = db.make_pool("api")

# `kill -USR1 <pid>` dumps every thread's stack to stderr: lets the harness see
# where a hung API is stuck without stopping it.
if hasattr(signal, "SIGUSR1"):
    faulthandler.register(signal.SIGUSR1, all_threads=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await pool.open(wait=True, timeout=30)
    async with pool.connection() as conn:
        info = await db.session_settings(conn)
    print(f"api ready: {info}, faults={sorted(faults.ENABLED)}", flush=True)
    yield
    await pool.close()


app = FastAPI(title="Soup job queue", lifespan=lifespan)


def job_json(job: dict) -> dict:
    out = {}
    for k, v in job.items():
        if k == "payload_hash":
            continue
        out[k] = v.isoformat() if isinstance(v, datetime) else v
    return out


def error(status: int, message: str, job: dict | None = None) -> JSONResponse:
    body = {"error": message}
    if job is not None:
        body["job"] = job_json(job)
    return JSONResponse(status_code=status, content=body)


@app.exception_handler(psycopg.OperationalError)
async def db_unavailable(_: Request, exc: psycopg.OperationalError) -> JSONResponse:
    # Connection lost or statement timed out: the outcome is unknown. Every
    # endpoint is safe to retry (create via Idempotency-Key, cancel is idempotent).
    return error(503, f"database unavailable, outcome unknown; retry the same request ({exc.__class__.__name__})")


@app.post("/jobs")
async def create_job(request: Request, idempotency_key: str | None = Header(default=None)):
    if not idempotency_key or len(idempotency_key) > 200:
        return error(400, "Idempotency-Key header is required (1-200 characters)")
    try:
        payload = await request.json()
    except ValueError:
        return error(400, "body must be a JSON object")
    if not isinstance(payload, dict):
        return error(400, "body must be a JSON object")
    async with pool.connection() as conn:
        try:
            job, created = await queue.create_job(conn, idempotency_key, payload, settings.max_attempts)
        except queue.IdempotencyConflict as e:
            return error(409, "Idempotency-Key was already used with a different body", e.job)
    return JSONResponse(status_code=201 if created else 200, content=job_json(job))


@app.get("/jobs/{job_id}")
async def get_job(job_id: int):
    async with pool.connection() as conn:
        job = await queue.get_job(conn, job_id)
    return error(404, "job not found") if job is None else job_json(job)


def _encode_cursor(job_id: int) -> str:
    return base64.urlsafe_b64encode(str(job_id).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> int:
    padded = cursor + "=" * (-len(cursor) % 4)
    return int(base64.urlsafe_b64decode(padded.encode()).decode())


@app.get("/jobs")
async def list_jobs(
    state: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None),
):
    if state is not None and state not in queue.STATES:
        return error(400, f"state must be one of {', '.join(queue.STATES)}")
    try:
        after_id = _decode_cursor(cursor) if cursor else 0
    except ValueError:
        return error(400, "invalid cursor")
    async with pool.connection() as conn:
        rows = await queue.list_jobs(conn, state, after_id, limit + 1)
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "items": [job_json(r) for r in rows],
        "next_cursor": _encode_cursor(rows[-1]["id"]) if has_more else None,
    }


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: int):
    async with pool.connection() as conn:
        outcome, job = await queue.cancel(conn, job_id)
    if outcome == "not_found":
        return error(404, "job not found")
    if outcome == "conflict":
        return error(409, f"job already {job['state']}", job)
    return job_json(job)


@app.get("/healthz")
async def healthz():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"ok": True}
