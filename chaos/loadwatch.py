"""Database-side companion of the k6 load test (load/mix.js).

Start it together with k6. While the load runs it samples queue depth and table
statistics every 5 s; afterwards it waits for the queue to drain, measures queue
wait and create-to-finish times on the DB clock, runs the invariants on this
run's jobs (DB-side only: k6 keeps no per-request journal), and merges k6's
summary into results/load/<run>/summary.json.

  docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.loadwatch --run <id> --duration 600
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg

from chaos.harness import Log, git_commit, source_hash
from chaos.invariants import Checker

RESULTS = Path("/app/results/load")
DB_URL = os.environ.get("ADMIN_DATABASE_URL", "postgresql://soup:soup@postgres:5432/jobs")


async def one(conn, sql: str, params=None):
    return await (await conn.execute(sql, params)).fetchone()


async def sample(conn) -> dict:
    queued, running = await one(conn, """SELECT count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'running')
                                         FROM jobs WHERE state IN ('queued','running')""")
    live, dead, upd, hot = await one(conn, """SELECT n_live_tup, n_dead_tup, n_tup_upd, n_tup_hot_upd
                                              FROM pg_stat_user_tables WHERE relname = 'jobs'""")
    commits, = await one(conn, "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")
    return {"queued": queued, "running": running, "live": live, "dead": dead, "upd": upd, "hot_upd": hot, "xact_commit": commits}


async def run(args) -> int:
    if not re.fullmatch(r"[A-Za-z0-9-]+", args.run):
        raise SystemExit("--run must be letters, digits and '-' (it is used in a LIKE prefix)")
    out = RESULTS / args.run
    out.mkdir(parents=True, exist_ok=True)
    log = Log(out / "harness.log")
    conn = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True, application_name="loadwatch")
    workers, = await one(conn, "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'worker %'")
    sync_commit, = await one(conn, "SHOW synchronous_commit")
    config = {"run_id": args.run, "duration_s": args.duration, "workers_connected": workers,
              "synchronous_commit": sync_commit, "generator": "k6 load/mix.js (constant-arrival-rate)",
              "git_commit": git_commit(), "source_sha256": source_hash(), "note": args.note}
    (out / "config.json").write_text(json.dumps(config, indent=2))
    log(f"watching {args.run}: {workers} workers connected, synchronous_commit={sync_commit}")

    samples = []
    t0 = time.monotonic()
    c0 = (await sample(conn))["xact_commit"]
    while time.monotonic() - t0 < args.duration + 10:
        s = await sample(conn)
        s["t"] = round(time.monotonic() - t0, 1)
        s["commits_per_s"] = round((s["xact_commit"] - c0) / max(s["t"], 0.1), 1)
        samples.append(s)
        if len(samples) % 6 == 1:
            log(f"t={s['t']}s queued={s['queued']} running={s['running']} dead={s['dead']} commits/s={s['commits_per_s']}")
        await asyncio.sleep(5)

    t_drain = time.monotonic()
    while time.monotonic() - t_drain < args.drain:
        s = await sample(conn)
        if s["queued"] + s["running"] == 0:
            break
        await asyncio.sleep(1)
    drain_s = round(time.monotonic() - t_drain, 1)
    log(f"load window over; queue drained {drain_s}s later")

    prefix = args.run + "-"
    n, n_done, first, last = await one(conn, """
        SELECT count(*), count(*) FILTER (WHERE state IN ('succeeded','failed','cancelled')),
               min(created_at), max(finished_at) FROM jobs WHERE idempotency_key LIKE %s""", (prefix + "%",))
    wait_q, e2e = await one(conn, """
        SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at)),
               percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at))
        FROM jobs WHERE idempotency_key LIKE %s AND state = 'succeeded'""", (prefix + "%",))
    span = (last - first).total_seconds() if first and last else None

    checker = Checker([], None, None, None, has_process_faults=False, lease_ttl_s=5.0)
    await checker.load(conn, key_prefix=prefix)
    checker.run()
    await conn.close()
    counts = {k: n for k, n in checker.counts().items() if k not in ("I1", "I6", "I10")}
    failed = sorted(k for k, v in counts.items() if v)

    k6 = None
    k6_path = out / "k6-summary.json"
    for _ in range(30):  # k6 writes its summary when it exits, normally before the drain ends
        if k6_path.exists():
            break
        await asyncio.sleep(1)
    if k6_path.exists():
        m = json.loads(k6_path.read_text())["metrics"]
        k6 = {
            "requests": m["http_reqs"]["values"]["count"], "rps": round(m["http_reqs"]["values"]["rate"], 1),
            "dropped_iterations": m.get("dropped_iterations", {}).get("values", {}).get("count", 0),
            "max_vus": m["vus_max"]["values"]["max"],
            "latency_ms": {op: {k: round(v, 1) for k, v in m[f"http_req_duration{{op:{op}}}"]["values"].items()}
                           for op in ("create", "get", "list", "cancel") if f"http_req_duration{{op:{op}}}" in m},
            "statuses": {k[7:]: v["values"]["count"] for k, v in m.items() if k.startswith("status_")},
        }

    summary = {
        "run_id": args.run, "config": config, "k6": k6, "jobs": n, "jobs_finished": n_done,
        "jobs_per_s_end_to_end": round(n_done / span, 1) if span else None,
        "queue_wait_s_p50_p95_p99": [round(x, 3) for x in wait_q] if wait_q and wait_q[0] is not None else None,
        "create_to_finish_s_p50_p95_p99": [round(x, 3) for x in e2e] if e2e and e2e[0] is not None else None,
        "max_queued": max((s["queued"] for s in samples), default=0), "drain_s": drain_s,
        "commits_per_s_avg": samples[-1]["commits_per_s"] if samples else None,
        "invariants_db_side": counts, "failed_invariants": failed, "coverage": checker.cov,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (out / "samples.json").write_text(json.dumps(samples))
    if failed:
        (out / "violations.json").write_text(json.dumps(
            {"samples": {k: v[:25] for k, v in checker.violations.items() if v}, "histories": checker.histories()},
            indent=2, default=str))
    if k6:
        for op, v in k6["latency_ms"].items():
            log(f"k6 {op:7s} {v}")
        log(f"k6 requests {k6['requests']} ({k6['rps']}/s), dropped iterations {k6['dropped_iterations']}, "
            f"max VUs {k6['max_vus']}, statuses {k6['statuses']}")
    else:
        log("k6 summary not found")
    log(f"jobs {n} ({n_done} finished, {summary['jobs_per_s_end_to_end']}/s end to end); "
        f"queue wait p50/p95/p99 {summary['queue_wait_s_p50_p95_p99']} s; max queued {summary['max_queued']}; "
        f"commits/s {summary['commits_per_s_avg']}")
    log("invariants (DB side; I1, I6, I10 need a client journal): "
        + "  ".join(f"{k}={'ok' if v == 0 else f'FAIL({v})'}" for k, v in counts.items()))
    return 1 if failed else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True)
    ap.add_argument("--duration", type=float, required=True, help="seconds; the same as k6's DURATION")
    ap.add_argument("--drain", type=float, default=600)
    ap.add_argument("--note", default="")
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
