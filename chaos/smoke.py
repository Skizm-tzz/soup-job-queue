"""Smoke test of the running stack: API contract plus a few DB-level checks.

Run:  docker compose run --rm --no-deps api python -m chaos.smoke
Not a correctness proof (that is the chaos harness); it checks that every
endpoint behaves as documented on a live system.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx
import psycopg

API = os.environ.get("API_URL", "http://api:8000")
DB = os.environ.get("DATABASE_URL", "postgresql://soup:soup@postgres:5432/jobs")

failures: list[str] = []


def check(cond: bool, what: str) -> None:
    print(f"  [{'ok ' if cond else 'FAIL'}] {what}", flush=True)
    if not cond:
        failures.append(what)


def key() -> str:
    return f"smoke-{uuid.uuid4()}"


async def wait_state(c: httpx.AsyncClient, job_id: int, states: set[str], timeout: float = 15) -> dict:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        job = (await c.get(f"/jobs/{job_id}")).json()
        if job["state"] in states or loop.time() > end:
            return job
        await asyncio.sleep(0.05)


async def main() -> int:
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        print("create / idempotency")
        r = await c.post("/jobs", json={"work_ms": 50})
        check(r.status_code == 400, "POST without Idempotency-Key -> 400")

        k = key()
        r1 = await c.post("/jobs", json={"work_ms": 50}, headers={"Idempotency-Key": k})
        r2 = await c.post("/jobs", json={"work_ms": 50}, headers={"Idempotency-Key": k})
        r3 = await c.post("/jobs", json={"work_ms": 51}, headers={"Idempotency-Key": k})
        check(r1.status_code == 201, "first POST -> 201")
        check(r2.status_code == 200 and r2.json()["id"] == r1.json()["id"], "repeat POST, same body -> 200, same id")
        check(r3.status_code == 409, "same key, different body -> 409")

        k = key()
        burst = await asyncio.gather(
            *[c.post("/jobs", json={"work_ms": 50}, headers={"Idempotency-Key": k}) for _ in range(20)]
        )
        codes = [r.status_code for r in burst]
        ids = {r.json()["id"] for r in burst}
        check(codes.count(201) == 1 and codes.count(200) == 19, f"20 concurrent POSTs, one key -> one 201 (got {codes.count(201)})")
        check(len(ids) == 1, "20 concurrent POSTs -> one job id")

        print("execution")
        job = await wait_state(c, r1.json()["id"], {"succeeded", "failed", "cancelled"})
        check(job["state"] == "succeeded" and job["attempt"] == 1, f"job runs to succeeded on attempt 1 ({job['state']}, {job['attempt']})")
        r = await c.post("/jobs", json={"work_ms": 50, "outcome": "fail"}, headers={"Idempotency-Key": key()})
        job = await wait_state(c, r.json()["id"], {"succeeded", "failed", "cancelled"})
        check(job["state"] == "failed", "job reporting failure ends failed")

        print("cancel")
        r = await c.post("/jobs", json={"work_ms": 4000}, headers={"Idempotency-Key": key()})
        jid = r.json()["id"]
        running = await wait_state(c, jid, {"running"})
        check(running["state"] == "running", "long job reaches running")
        rc = await c.post(f"/jobs/{jid}/cancel")
        check(rc.status_code == 200 and rc.json()["state"] == "cancelled", "cancel running job -> 200 cancelled")
        rc2 = await c.post(f"/jobs/{jid}/cancel")
        check(rc2.status_code == 200, "repeat cancel -> 200")
        rc3 = await c.post(f"/jobs/{r1.json()['id']}/cancel")
        check(rc3.status_code == 409 and rc3.json()["job"]["state"] == "succeeded", "cancel succeeded job -> 409 with its state")
        check((await c.post("/jobs/999999999/cancel")).status_code == 404, "cancel unknown job -> 404")
        await asyncio.sleep(5)  # longer than the job's work: a worker that ignored the cancel would try to complete now
        after = (await c.get(f"/jobs/{jid}")).json()
        check(after["state"] == "cancelled", "cancelled running job stays cancelled after its work time")

        print("pagination")
        for _ in range(25):
            await c.post("/jobs", json={"work_ms": 1}, headers={"Idempotency-Key": key()})
        seen: list[int] = []
        cursor = None
        while True:
            params = {"limit": 7}
            if cursor:
                params["cursor"] = cursor
            page = (await c.get("/jobs", params=params)).json()
            seen += [j["id"] for j in page["items"]]
            cursor = page["next_cursor"]
            if not cursor:
                break
        check(seen == sorted(seen) and len(seen) == len(set(seen)), f"keyset walk: {len(seen)} ids, ascending, no duplicates")
        check((await c.get("/jobs", params={"state": "bogus"})).status_code == 400, "bad state filter -> 400")
        check((await c.get("/jobs", params={"cursor": "%%%"})).status_code == 400, "bad cursor -> 400")

    print("database")
    async with await psycopg.AsyncConnection.connect(DB, autocommit=True) as conn:

        async def scalar(sql: str) -> int:
            return (await (await conn.execute(sql)).fetchone())[0]

        total_db = await scalar("SELECT count(*) FROM jobs")
        check(total_db == len(seen), f"keyset walk saw every job ({len(seen)} of {total_db})")
        dup = await scalar("SELECT count(*) FROM (SELECT job_id FROM effects GROUP BY job_id HAVING count(*) > 1) d")
        check(dup == 0, "no job has more than one effect")
        bad = await scalar(
            """SELECT count(*) FROM jobs j
               WHERE (j.state = 'succeeded') <> EXISTS (SELECT 1 FROM effects e WHERE e.job_id = j.id)"""
        )
        check(bad == 0, "effect exists iff succeeded")
        hb = await scalar(
            "SELECT count(*) FROM job_events WHERE old_state = 'running' AND new_state = 'running' AND old_attempt = new_attempt"
        )
        check(hb == 0, "heartbeats do not write audit rows")

    print()
    print("SMOKE PASS" if not failures else f"SMOKE FAIL: {failures}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
