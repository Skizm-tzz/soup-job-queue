"""Worker: claim -> record execution -> work with heartbeats -> fenced complete.

Rule: when in doubt, abandon. On a lost lease, a lost connection or a timeout
the worker stops working on the job and writes nothing. The lease expires, the
job is taken over with attempt+1, and anything this worker might still write is
fenced off by attempt.
"""

from __future__ import annotations

import asyncio
import json
import random
import secrets
import signal
import socket
import sys
import time

import psycopg

from app import db, faults, queue
from app.config import settings

IDLE_MIN_S, IDLE_MAX_S = 0.05, 1.0


class Worker:
    def __init__(self) -> None:
        self.slot = settings.worker_slot or socket.gethostname()
        if ":" in self.slot:
            raise ValueError("WORKER_SLOT must not contain ':'")
        self.id = f"{self.slot}:{secrets.token_hex(3)}"
        self.conn: psycopg.AsyncConnection | None = None
        self.stopping = False

    def log(self, event: str, **fields) -> None:
        print(json.dumps({"t": round(time.time(), 3), "worker": self.id, "event": event, **fields}), flush=True)

    async def _connect(self) -> psycopg.AsyncConnection:
        backoff = 0.1
        while True:
            try:
                return await db.connect(application_name=f"worker {self.id}")
            except psycopg.OperationalError as e:
                self.log("db_connect_failed", error=e.__class__.__name__)
                await asyncio.sleep(backoff * random.uniform(0.5, 1.5))
                backoff = min(backoff * 2, 2.0)

    async def _conn(self) -> psycopg.AsyncConnection:
        if self.conn is None or self.conn.closed:
            self.conn = await self._connect()
        return self.conn

    async def _drop_conn(self) -> None:
        if self.conn is not None:
            try:
                await self.conn.close()
            except Exception:
                pass
        self.conn = None

    async def run(self) -> None:
        conn = await self._conn()
        self.log("started", faults=sorted(faults.ENABLED), **(await db.session_settings(conn)))
        if settings.release_on_start:
            n = await queue.release_slot(conn, self.slot, self.id)
            self.log("released_previous_leases", count=n)

        idle = IDLE_MIN_S
        while not self.stopping:
            try:
                conn = await self._conn()
                job = await queue.claim(conn, self.id, settings.lease_ttl_s)
                if job is None:
                    await asyncio.sleep(idle * random.uniform(0.5, 1.0))
                    idle = min(idle * 2, IDLE_MAX_S)
                    continue
                idle = IDLE_MIN_S
                await self.process(job)
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                # Outcome of the last statement is unknown: abandon whatever we held.
                self.log("db_error_abandon", error=f"{e.__class__.__name__}: {str(e)[:120]}")
                await self._drop_conn()
                await asyncio.sleep(random.uniform(0.05, 0.2))
        self.log("stopped")

    async def process(self, job: dict) -> None:
        conn = await self._conn()
        job_id, attempt = job["id"], job["attempt"]

        if attempt > job["max_attempts"]:
            ok = await queue.fail(conn, job_id, attempt, "attempts exhausted")
            await queue.diag(conn, "attempts_exhausted", job_id, attempt, self.id, {"applied": ok})
            self.log("attempts_exhausted", job=job_id, attempt=attempt, applied=ok)
            return

        await queue.record_execution(conn, job_id, attempt, self.id)
        payload = job["payload"] or {}
        work_s = max(0, int(payload.get("work_ms", 100))) / 1000
        outcome = payload.get("outcome", "ok")

        lease_lost = await self._work_with_heartbeat(job_id, attempt, work_s)
        if lease_lost:
            await queue.diag(conn, "lease_lost", job_id, attempt, self.id)
            self.log("lease_lost", job=job_id, attempt=attempt)
            if not settings.chaos_ignore_lease_loss:
                return

        if outcome == "fail":
            ok = await queue.fail(conn, job_id, attempt, "job reported failure")
        else:
            ok = await queue.complete(conn, job_id, attempt, self.id)
        if ok:
            self.log("finished", job=job_id, attempt=attempt, outcome=outcome)
        else:
            await queue.diag(conn, "stale_complete_rejected", job_id, attempt, self.id, {"outcome": outcome})
            self.log("stale_complete_rejected", job=job_id, attempt=attempt)

    async def _work_with_heartbeat(self, job_id: int, attempt: int, work_s: float) -> bool:
        """Emulated work (a sleep) with lease renewal. Returns True if the lease was lost."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + work_s
        lost = False
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return lost
            await asyncio.sleep(min(remaining, settings.heartbeat_s))
            if loop.time() >= deadline or faults.on("NO_HEARTBEAT") or lost:
                continue
            if not await queue.heartbeat(await self._conn(), job_id, attempt, settings.lease_ttl_s):
                lost = True
                if not settings.chaos_ignore_lease_loss:
                    return True


async def main() -> None:
    worker = Worker()
    loop = asyncio.get_running_loop()

    def stop() -> None:
        worker.stopping = True  # finish the current job, then exit

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:  # Windows dev shell
            pass
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
