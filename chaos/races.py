"""Deterministic races on a real Postgres: each scenario forces one exact
interleaving of the statements in app/queue.py and checks the outcome.

A statement is "frozen" half-way by running it inside an explicit transaction on
a test connection (the service never does this: it is autocommit everywhere). The
moment another session is blocked on that row lock is read from pg_stat_activity
(wait_event_type = 'Lock'), not guessed with a sleep.

Seeded bugs are switched on per step by setting app.faults.ENABLED, so the buggy
variants run the exact code the chaos harness runs with FAULT_<NAME>=1.

Run:  docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.races
"""

from __future__ import annotations

import os
import sys

ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgresql://soup:soup@postgres:5432/jobs")
RACES_URL = ADMIN_URL.rsplit("/", 1)[0] + "/races"
os.environ["DATABASE_URL"] = RACES_URL  # app.config reads it at import
os.environ["CLOCK_SKEW_S"] = "30"  # used only while WORKER_CLOCK is switched on (R6)

import asyncio  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from contextlib import contextmanager  # noqa: E402
from pathlib import Path  # noqa: E402

import psycopg  # noqa: E402

from app import db, faults, queue  # noqa: E402
from app.migrate import apply_schema  # noqa: E402

UNION_CREATE = """
WITH ins AS (
  INSERT INTO jobs (idempotency_key, payload, payload_hash) VALUES (%(k)s, '{}', 'h')
  ON CONFLICT (idempotency_key) DO NOTHING RETURNING id)
SELECT id FROM ins UNION ALL SELECT id FROM jobs WHERE idempotency_key = %(k)s
"""


class Transcript:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.failed: list[str] = []
        self.current = ""

    def out(self, s: str = "") -> None:
        print(s, flush=True)
        self.lines.append(s)

    def section(self, name: str, what: str) -> None:
        self.current = name
        self.out()
        self.out(f"== {name}: {what}")

    def note(self, s: str) -> None:
        self.out(f"   . {s}")

    def step(self, desc: str, got, expected, ok: bool | None = None) -> None:
        ok = (got == expected) if ok is None else ok
        self.out(f"   [{'ok ' if ok else 'FAIL'}] {desc}: got {got!r}, expected {expected!r}")
        if not ok:
            self.failed.append(f"{self.current}: {desc}")


@contextmanager
def fault(name: str):
    saved = faults.ENABLED
    faults.ENABLED = frozenset({name})
    try:
        yield
    finally:
        faults.ENABLED = saved


class Conns:
    async def open(self) -> None:
        self.A = await db.connect("race A")
        self.B = await db.connect("race B")
        self.C = await db.connect("race C")
        self.O = await db.connect("race observer")

    async def close(self) -> None:
        for c in (self.A, self.B, self.C, self.O):
            await c.close()


async def fresh(O) -> None:
    await O.execute("TRUNCATE jobs, effects, executions, job_events, diag_events RESTART IDENTITY")


async def new_job(conn, key: str | None = None) -> int:
    job, _ = await queue.create_job(conn, key or f"race-{uuid.uuid4()}", {"work_ms": 1}, 5)
    return job["id"]


async def row(conn, jid: int) -> tuple[str, int]:
    j = await queue.get_job(conn, jid)
    return j["state"], j["attempt"]


async def effects(conn, jid: int) -> int:
    return (await (await conn.execute("SELECT count(*) FROM effects WHERE job_id = %s", (jid,))).fetchone())[0]


async def blocked(O, conn, timeout: float = 5.0) -> bool:
    """True once `conn`'s backend is waiting on a lock."""
    pid = conn.info.backend_pid
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        r = await (await O.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,))).fetchone()
        if r and r[0] == "Lock":
            return True
        await asyncio.sleep(0.005)
    return False


def claimed(j) -> tuple | None:
    return (j["id"], j["attempt"]) if j else None


# ------------------------------------------------------------------ scenarios


async def r1_double_claim(t: Transcript, c: Conns) -> None:
    t.section("R1 double claim", "two claims on one job while the first is uncommitted; then 50-way contention")
    await fresh(c.O)
    j = await new_job(c.O)
    await c.A.execute("BEGIN")
    t.step("A claims inside an open transaction", claimed(await queue.claim(c.A, "A:1", 5)), (j, 1))
    t0 = time.monotonic()
    got = await asyncio.wait_for(queue.claim(c.B, "B:1", 5), 2)
    t.step(f"B claims meanwhile (SKIP LOCKED, returned in {1000 * (time.monotonic() - t0):.0f} ms)", got, None)
    await c.A.execute("COMMIT")
    t.step("B claims after A committed (lease is live)", await queue.claim(c.B, "B:1", 5), None)
    t.step("job row", await row(c.O, j), ("running", 1))

    many = [await db.connect(f"race s{i}") for i in range(50)]
    try:
        for variant, rounds in (("correct", 1000), ("CLAIM_READ_THEN_WRITE", 200)):
            await fresh(c.O)
            multi, max_winners = 0, 0

            async def go():
                return await asyncio.gather(*(queue.claim(cn, f"s{i}:x", 60) for i, cn in enumerate(many)))

            for _ in range(rounds):
                await new_job(c.O)
                if variant == "correct":
                    res = await go()
                else:
                    with fault(variant):
                        res = await go()
                winners = sum(1 for r in res if r)
                multi += winners != 1
                max_winners = max(max_winners, winners)
            if variant == "correct":
                t.step(f"{variant}: {rounds} rounds x 50 concurrent claims of one new job, rounds without exactly one winner",
                       multi, 0)
            else:
                t.step(f"{variant} (seeded bug): {rounds} rounds, rounds without exactly one winner (max winners {max_winners})",
                       multi, "> 0", ok=multi > 0)
    finally:
        for cn in many:
            await cn.close()


async def r2_zombie(t: Transcript, c: Conns) -> None:
    t.section("R2 zombie worker", "A's lease expires, B takes over, then A tries to complete")
    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "A:1", 0.3)
    await asyncio.sleep(0.5)
    t.step("B claims after A's lease expired", claimed(await queue.claim(c.B, "B:1", 5)), (j, 2))
    t.step("zombie A completes attempt 1", await queue.complete(c.A, j, 1, "A:1"), False)
    t.step("effects after the zombie's complete", await effects(c.O, j), 0)
    t.step("B completes attempt 2", await queue.complete(c.B, j, 2, "B:1"), True)
    t.step("final row, effects", (await row(c.O, j), await effects(c.O, j)), (("succeeded", 2), 1))

    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "A:1", 0.3)
    await asyncio.sleep(0.5)
    t.note("late but alone: A's lease expired and nobody took the job over")
    t.step("late A completes attempt 1", await queue.complete(c.A, j, 1, "A:1"), True)
    t.step("final row, effects", (await row(c.O, j), await effects(c.O, j)), (("succeeded", 1), 1))

    await fresh(c.O)
    j = await new_job(c.O)
    with fault("NO_FENCE"):
        await queue.claim(c.A, "A:1", 0.3)
        await asyncio.sleep(0.5)
        await queue.claim(c.B, "B:1", 5)
        a = await queue.complete(c.A, j, 1, "A:1")
        b = await queue.complete(c.B, j, 2, "B:1")
    t.step("NO_FENCE (seeded bug): zombie A and B both complete", (a, b), (True, True))
    t.step("NO_FENCE (seeded bug): effects", await effects(c.O, j), 2)


async def r3_cancel_complete(t: Transcript, c: Conns) -> None:
    t.section("R3 cancel vs complete", "both orders, with a real lock wait between them")
    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "W:1", 5)
    await c.C.execute("BEGIN")
    t.step("C cancels (uncommitted)", (await queue.cancel(c.C, j))[0], "cancelled")
    task = asyncio.create_task(queue.complete(c.A, j, 1, "W:1"))
    t.step("worker's complete waits for C's row lock", await blocked(c.O, c.A), True)
    await c.C.execute("COMMIT")
    t.step("complete after the cancel committed (WHERE re-checked)", await task, False)
    t.step("final row, effects", (await row(c.O, j), await effects(c.O, j)), (("cancelled", 1), 0))

    for variant in (None, "CANCEL_BLIND"):
        await fresh(c.O)
        j = await new_job(c.O)
        await queue.claim(c.A, "W:1", 5)
        await c.A.execute("BEGIN")
        t.step(f"{variant or 'correct'}: worker completes (uncommitted)", await queue.complete(c.A, j, 1, "W:1"), True)
        if variant:
            with fault(variant):
                task = asyncio.create_task(queue.cancel(c.C, j))
                was_blocked = await blocked(c.O, c.C)
                await c.A.execute("COMMIT")
                outcome, job = await task
        else:
            task = asyncio.create_task(queue.cancel(c.C, j))
            was_blocked = await blocked(c.O, c.C)
            await c.A.execute("COMMIT")
            outcome, job = await task
        t.step(f"{variant or 'correct'}: cancel waits for the worker's row lock", was_blocked, True)
        final = (await row(c.O, j), await effects(c.O, j))
        if variant is None:
            t.step("cancel after the complete committed -> API 409 with state", (outcome, job["state"]), ("conflict", "succeeded"))
            t.step("final row, effects", final, (("succeeded", 1), 1))
        else:
            t.step("CANCEL_BLIND (seeded bug): cancel acknowledged over a succeeded job", outcome, "cancelled")
            t.step("CANCEL_BLIND (seeded bug): final row, effects -> cancelled job with an effect", final, (("cancelled", 1), 1))


async def r4_duplicate_create(t: Transcript, c: Conns) -> None:
    t.section("R4 duplicate create", "same Idempotency-Key from two sessions, the first uncommitted")
    await fresh(c.O)
    for end in ("COMMIT", "ROLLBACK"):
        k = f"dup-{end}"
        await c.A.execute("BEGIN")
        ja, ca = await queue.create_job(c.A, k, {"work_ms": 1}, 5)
        task = asyncio.create_task(queue.create_job(c.B, k, {"work_ms": 1}, 5))
        t.step(f"[{end}] B's create waits on the unique index", await blocked(c.O, c.B), True)
        await c.A.execute(end)
        jb, cb = await task
        if end == "COMMIT":
            t.step("[COMMIT] B returns A's job, created=False", (jb["id"] == ja["id"], cb), (True, False))
        else:
            t.step("[ROLLBACK] B creates the job itself, created=True", (cb, jb["id"] != ja["id"]), (True, True))
    n = (await (await c.O.execute("SELECT count(*) FROM jobs")).fetchone())[0]
    t.step("rows for the two keys", n, 2)

    k = "dup-union"
    await c.A.execute("BEGIN")
    await queue.create_job(c.A, k, {"work_ms": 1}, 5)
    task = asyncio.create_task(c.B.execute(UNION_CREATE, {"k": k}))
    t.step("rejected design, INSERT..ON CONFLICT + UNION SELECT in one statement: waits", await blocked(c.O, c.B), True)
    await c.A.execute("COMMIT")
    rows = await (await task).fetchall()
    t.step("rejected design returns no row at all (one snapshot, taken before A committed)", len(rows), 0)

    k = "dup-check-then-insert"
    sel = "SELECT id FROM jobs WHERE idempotency_key = %s"
    ins = "INSERT INTO jobs (idempotency_key, payload, payload_hash) VALUES (%s, '{}', 'h')"
    a = await (await c.A.execute(sel, (k,))).fetchone()
    b = await (await c.B.execute(sel, (k,))).fetchone()
    t.step("check-then-insert: both sessions see no row", (a, b), (None, None))
    await c.A.execute(ins, (k,))
    try:
        await c.B.execute(ins, (k,))
        got = "inserted a second row"
    except psycopg.errors.UniqueViolation:
        got = "UniqueViolation"
    t.step("check-then-insert: B's INSERT is stopped only by the UNIQUE constraint", got, "UniqueViolation")


async def r5_isolation(t: Transcript, c: Conns) -> None:
    t.section("R5 isolation level", "a complete racing a committed cancel, at READ COMMITTED and REPEATABLE READ")
    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "W:1", 5)
    await c.C.execute("BEGIN")
    await queue.cancel(c.C, j)
    task = asyncio.create_task(queue.complete(c.A, j, 1, "W:1"))
    await blocked(c.O, c.A)
    await c.C.execute("COMMIT")
    t.step("READ COMMITTED, guarded complete: clean 0 rows", await task, False)
    t.step("final row", await row(c.O, j), ("cancelled", 1))

    rr = await db.connect("race RR")
    await rr.execute("SET default_transaction_isolation = 'repeatable read'")
    try:
        j = await new_job(c.O)
        await queue.claim(c.A, "W:1", 5)
        await c.C.execute("BEGIN")
        await queue.cancel(c.C, j)
        before = queue.retry_count
        task = asyncio.create_task(queue.complete(rr, j, 1, "W:1"))
        await blocked(c.O, rr)
        await c.C.execute("COMMIT")
        ok = await task
        t.step("REPEATABLE READ, guarded complete: 40001, retried by _one, then 0 rows",
               (ok, queue.retry_count - before), (False, 1))
    finally:
        await rr.close()

    j = await new_job(c.O)
    await queue.claim(c.A, "W:1", 5)
    st = (await (await c.A.execute("SELECT state FROM jobs WHERE id = %s", (j,))).fetchone())[0]
    t.note(f"read-then-write without a guard: worker reads state={st!r}, then decides in Python")
    t.step("C cancels and commits; API tells the client", (await queue.cancel(c.C, j))[0], "cancelled")
    cur = await c.A.execute("UPDATE jobs SET state = 'succeeded', finished_at = now(), lease_owner = NULL WHERE id = %s", (j,))
    t.step("READ COMMITTED, unguarded write: 1 row, the acknowledged cancel is silently lost",
           (cur.rowcount, (await row(c.O, j))[0]), (1, "succeeded"))

    j = await new_job(c.O)
    await queue.claim(c.A, "W:1", 5)
    await c.A.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    await c.A.execute("SELECT state FROM jobs WHERE id = %s", (j,))
    await queue.cancel(c.C, j)
    try:
        await c.A.execute("UPDATE jobs SET state = 'succeeded' WHERE id = %s", (j,))
        got = "updated"
    except psycopg.errors.SerializationFailure:
        got = "SerializationFailure"
    await c.A.execute("ROLLBACK")
    t.step("REPEATABLE READ, same read-then-write in one transaction: the write is refused", got, "SerializationFailure")
    t.step("final row", await row(c.O, j), ("cancelled", 1))


async def r6_clocks(t: Transcript, c: Conns) -> None:
    t.section("R6 clock skew", "lease alive on the DB clock; a claim judged by a worker clock 30 s ahead")
    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "A:1", 5)
    t.step("correct claim (now() on the DB clock) leaves the live lease alone", await queue.claim(c.B, "B:1", 5), None)
    with fault("WORKER_CLOCK"):
        got = claimed(await queue.claim(c.B, "B:1", 5))
    t.step("WORKER_CLOCK (seeded bug): claim takes over the live lease", got, (j, 2))
    r = await (await c.O.execute(
        """SELECT old_lease_expires_at > at FROM job_events
           WHERE job_id = %s AND old_state = 'running' AND new_state = 'running'""", (j,))).fetchone()
    t.step("audit trail shows the takeover happened before expiry (what I7 checks)", r[0] if r else None, True)


async def r7_heartbeat_takeover(t: Transcript, c: Conns) -> None:
    t.section("R7 heartbeat vs takeover", "both orders at the moment the lease expires")
    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "A:1", 0.3)
    await asyncio.sleep(0.5)
    await c.A.execute("BEGIN")
    t.step("A's heartbeat first (uncommitted)", await queue.heartbeat(c.A, j, 1, 5), True)
    t.step("B's claim meanwhile skips the locked row", await asyncio.wait_for(queue.claim(c.B, "B:1", 5), 2), None)
    await c.A.execute("COMMIT")
    t.step("B's claim after the heartbeat committed: lease renewed, no takeover", await queue.claim(c.B, "B:1", 5), None)
    t.step("row", await row(c.O, j), ("running", 1))

    await fresh(c.O)
    j = await new_job(c.O)
    await queue.claim(c.A, "A:1", 0.3)
    await asyncio.sleep(0.5)
    await c.B.execute("BEGIN")
    t.step("B's takeover first (uncommitted)", claimed(await queue.claim(c.B, "B:1", 5)), (j, 2))
    task = asyncio.create_task(queue.heartbeat(c.A, j, 1, 5))
    t.step("A's heartbeat waits for B's row lock", await blocked(c.O, c.A), True)
    await c.B.execute("COMMIT")
    t.step("A's heartbeat after the takeover committed: lease lost", await task, False)


SCENARIOS = [r1_double_claim, r2_zombie, r3_cancel_complete, r4_duplicate_create, r5_isolation, r6_clocks,
             r7_heartbeat_takeover]


async def main() -> int:
    async with await psycopg.AsyncConnection.connect(ADMIN_URL, autocommit=True) as a:
        await a.execute('DROP DATABASE IF EXISTS "races" WITH (FORCE)')
        await a.execute('CREATE DATABASE "races"')
    await apply_schema(RACES_URL)

    t = Transcript()
    t.out(f"deterministic races, {time.strftime('%Y-%m-%d %H:%M:%S %Z')}, database 'races'")
    c = Conns()
    await c.open()
    t.out(f"session: {await db.session_settings(c.A)}")
    try:
        for s in SCENARIOS:
            try:
                await s(t, c)
            except Exception as e:  # a crashed scenario is a failure, and the next one gets clean connections
                t.out(f"   [FAIL] scenario crashed: {type(e).__name__}: {e}")
                t.failed.append(f"{t.current}: crashed ({type(e).__name__})")
                await c.close()
                await c.open()
    finally:
        await c.close()
    t.out()
    t.out("RACES PASS" if not t.failed else f"RACES FAIL ({len(t.failed)}): {t.failed}")
    out = Path("/app/results") / f"races-{time.strftime('%Y%m%d-%H%M%S')}.txt"
    out.write_text("\n".join(t.lines) + "\n", encoding="utf-8")
    print(f"saved {out}")
    return 0 if not t.failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
