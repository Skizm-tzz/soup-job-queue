"""Open-loop load test against the standard stack (`docker compose up`: API + 4 workers).

Requests go out on a fixed Poisson schedule whatever the responses do, and latency
is measured from the *scheduled* send time, so a slow server cannot hide behind a
client that waits for it (coordinated omission). Mix: 60% create, 30% get,
5% list, 5% cancel. Unknown outcomes are retried like a real client would (same
Idempotency-Key); retries count towards the latency.

After the load: wait for the queue to drain, then run the chaos invariants on the
jobs of this run only (the database is shared with the dev stack).

Run:
  docker compose up -d
  docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.loadtest --rate 100 --duration 600 --label L1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import psycopg

from chaos.harness import Log, git_commit, source_hash
from chaos.invariants import Checker
from chaos.load import Client, GaveUp, Spec

RESULTS = Path("/app/results/load")
API_URL = os.environ.get("API_URL", "http://api:8000")
DB_URL = os.environ.get("ADMIN_DATABASE_URL", "postgresql://soup:soup@postgres:5432/jobs")
MIX = (("create", 0.60), ("get", 0.30), ("list", 0.05), ("cancel", 0.05))


def pct(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    xs = sorted(xs)

    def q(p: float) -> float:
        return round(1000 * xs[min(len(xs) - 1, int(p * len(xs)))], 1)

    return {"n": len(xs), "p50_ms": q(0.50), "p95_ms": q(0.95), "p99_ms": q(0.99), "max_ms": round(1000 * xs[-1], 1)}


async def sample(conn) -> dict:
    cur = await conn.execute(
        """SELECT count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'running')
           FROM jobs WHERE state IN ('queued','running')""")
    queued, running = await cur.fetchone()
    cur = await conn.execute(
        """SELECT n_live_tup, n_dead_tup, n_tup_upd, n_tup_hot_upd,
                  pg_relation_size('jobs'), pg_relation_size('jobs_active'), pg_relation_size('jobs_state_id')
           FROM pg_stat_user_tables WHERE relname = 'jobs'""")
    live, dead, upd, hot, rel, active, state_idx = await cur.fetchone()
    return {"queued": queued, "running": running, "live": live, "dead": dead, "upd": upd, "hot_upd": hot,
            "jobs_bytes": rel, "jobs_active_bytes": active, "jobs_state_id_bytes": state_idx}


async def run(args) -> int:
    label = re.sub(r"[^A-Za-z0-9-]", "-", args.label)
    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{label}"
    out = RESULTS / run_id
    out.mkdir(parents=True)
    log = Log(out / "harness.log")
    config = {"run_id": run_id, "rate": args.rate, "duration_s": args.duration, "mix": dict(MIX),
              "work_ms": [args.work_min, args.work_max], "seed": args.seed, "api": API_URL,
              "git_commit": git_commit(), "source_sha256": source_hash(), "note": args.note}
    (out / "config.json").write_text(json.dumps(config, indent=2))
    log(f"load test {run_id}: {args.rate} rps for {args.duration}s against {API_URL}, mix {dict(MIX)}")

    rng = random.Random(args.seed)
    sched, t = [], 0.0
    while True:
        t += rng.expovariate(args.rate)
        if t >= args.duration:
            break
        r, acc = rng.random(), 0.0
        for op, w in MIX:
            acc += w
            if r < acc:
                break
        sched.append((t, op))

    conn = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True, application_name="loadtest")
    client = Client(API_URL, t0=time.monotonic(), retry_deadline_s=60)
    lat: dict[str, list[float]] = defaultdict(list)
    statuses: Counter = Counter()
    specs: list[Spec] = []
    created: list[int] = []
    late = 0
    samples: list[dict] = []

    async def one(k: int, ts: float, op: str) -> None:
        nonlocal late
        due = client.t0 + ts
        delay = due - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        elif -delay > 0.05:
            late += 1  # the generator itself fell behind: reported, not hidden
        try:
            if op == "create":
                key = f"{run_id}-{k}"
                payload = {"work_ms": rng.randint(args.work_min, args.work_max), "outcome": "ok"}
                specs.append(Spec(i=k, key=key, t=ts, payload=payload))
                r = await client.call("create", "POST", "/jobs", key=key, json=payload)
                j = r.json()
                client.rec(op="create", key=key, idx=0, diff=False, status=r.status_code, job=j.get("id"), state=j.get("state"))
                if r.status_code in (200, 201):
                    created.append(j["id"])
            elif op == "get" and created:
                r = await client.call("get", "GET", f"/jobs/{rng.choice(created)}")
                j = r.json()
                if j.get("state") in ("succeeded", "failed", "cancelled"):
                    client.rec(op="get", job=j["id"], status=r.status_code, state=j["state"])
            elif op == "list":
                params = {"limit": 50}
                if rng.random() < 0.5:
                    params["state"] = rng.choice(["queued", "running", "succeeded"])
                r = await client.call("list", "GET", "/jobs", params=params)
            elif op == "cancel" and created:
                jid = rng.choice(created[-200:])  # recent jobs: some are still queued or running
                r = await client.call("cancel", "POST", f"/jobs/{jid}/cancel")
                j = r.json()
                state = j.get("state") if r.status_code == 200 else (j.get("job") or {}).get("state")
                client.rec(op="cancel", job=jid, kind="load", seen=None, status=r.status_code, state=state)
            else:
                return
            statuses[f"{op}:{r.status_code}"] += 1
        except GaveUp:
            statuses[f"{op}:gave_up"] += 1
        lat[op].append(time.monotonic() - due)

    async def sampler() -> None:
        while True:
            s = await sample(conn)
            s["t"] = round(time.monotonic() - client.t0, 1)
            samples.append(s)
            if len(samples) % 6 == 1:
                done = sum(len(v) for v in lat.values())
                log(f"t={s['t']}s requests done {done}/{len(sched)} queued={s['queued']} running={s['running']} "
                    f"dead_tuples={s['dead']} hot={s['hot_upd']}/{s['upd']}")
            await asyncio.sleep(5)

    client.t0 = time.monotonic()
    sam = asyncio.create_task(sampler())
    await asyncio.gather(*(one(k, ts, op) for k, (ts, op) in enumerate(sched)))
    wall = time.monotonic() - client.t0
    t_drain = time.monotonic()
    while time.monotonic() - t_drain < 300:
        s = await sample(conn)
        if s["queued"] + s["running"] == 0:
            break
        await asyncio.sleep(1)
    drain_s = time.monotonic() - t_drain
    sam.cancel()
    log(f"load sent in {wall:.1f}s; queue drained {drain_s:.1f}s after the last request")

    cur = await conn.execute(
        """SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at)),
                  percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at))
           FROM jobs WHERE idempotency_key LIKE %s AND started_at IS NOT NULL AND state = 'succeeded'""", (run_id + "%",))
    wait_q, e2e = await cur.fetchone()

    checker = Checker(specs, client.journal, None, None, has_process_faults=False, lease_ttl_s=5.0)
    await checker.load(conn, key_prefix=run_id)
    checker.run()
    await conn.close()
    await client.close()

    counts = checker.counts()
    failed = sorted(k for k, n in counts.items() if n)
    summary = {
        "run_id": run_id, "target_rps": args.rate, "scheduled_requests": len(sched),
        "achieved_rps": round(sum(len(v) for v in lat.values()) / wall, 1), "wall_s": round(wall, 1),
        "generator_late_sends": late, "latency_from_schedule": {op: pct(v) for op, v in lat.items()},
        "statuses": dict(statuses), "client_errors": dict(client.errors), "drain_s": round(drain_s, 1),
        "queue_wait_s_p50_p95_p99": [round(x, 3) for x in wait_q] if wait_q else None,
        "create_to_finish_s_p50_p95_p99": [round(x, 3) for x in e2e] if e2e else None,
        "max_queued": max((s["queued"] for s in samples), default=0),
        "final_table": samples[-1] if samples else None,
        "invariants": counts, "failed_invariants": failed, "coverage": checker.cov,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (out / "samples.json").write_text(json.dumps(samples))
    if failed:
        (out / "violations.json").write_text(json.dumps(
            {"samples": {k: v[:25] for k, v in checker.violations.items() if v}, "histories": checker.histories()},
            indent=2, default=str))
    for op, v in summary["latency_from_schedule"].items():
        log(f"{op:7s} {v}")
    log(f"achieved {summary['achieved_rps']} rps of {args.rate}; late sends {late}; statuses {dict(statuses)}; "
        f"client errors {dict(client.errors)}")
    log(f"queue wait p50/p95/p99 {summary['queue_wait_s_p50_p95_p99']} s; create->finish {summary['create_to_finish_s_p50_p95_p99']} s; "
        f"max queued {summary['max_queued']}")
    log("invariants (this run's jobs; I10 not checked): "
        + "  ".join(f"{k}={'ok' if n == 0 else f'FAIL({n})'}" for k, n in counts.items() if k != "I10"))
    log(f"results in results/load/{run_id}/")
    return 1 if failed else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--duration", type=float, default=600)
    ap.add_argument("--work-min", type=int, default=5)
    ap.add_argument("--work-max", type=int, default=40)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--label", default="L1")
    ap.add_argument("--note", default="")
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
