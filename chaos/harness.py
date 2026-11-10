"""Chaos harness. One run = a fresh database, its own API and 8 worker processes,
a seeded load with duplicate POSTs and cancels, fault sources, a drain, then the
invariant checks in chaos/invariants.py.

Run (only the postgres service is needed):
  docker compose --profile chaos run --rm chaos canonical --seed 1 --label C1
  docker compose --profile chaos run --rm chaos baseline --fault NO_FENCE --label F2

Output: results/<run_id>/ is created before anything starts (config.json with the
expected outcome), so an aborted run is still listed in results/INDEX.md.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import random
import re
import shutil
import signal
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import psycopg

from app import faults as fault_flags
from app import queue
from app.migrate import apply_schema
from chaos import index
from chaos.invariants import INVARIANTS, Checker
from chaos.load import Client, make_plan

ROOT = Path("/app")
RESULTS = ROOT / "results"
ADMIN_URL = os.environ.get("ADMIN_DATABASE_URL", "postgresql://soup:soup@postgres:5432/jobs")
API_PORT = 8100
N_WORKERS = 8


@dataclass(frozen=True)
class Scenario:
    jobs: int
    rate: float  # mean submissions per second
    sources: tuple[str, ...] = ()
    profile: str = "normal"
    ignore_lease_loss: bool = False
    release_on_start: bool = False
    kill_every: tuple[float, float] = (1.5, 2.5)


SCENARIOS = {
    "baseline": Scenario(jobs=2000, rate=40),
    "canonical": Scenario(jobs=10000, rate=40, sources=("killer",)),
    "extended": Scenario(jobs=10000, rate=40, sources=("killer", "staller", "revoker", "conn_killer", "api_killer"),
                         ignore_lease_loss=True),
    "soak": Scenario(jobs=100000, rate=250, profile="dense", ignore_lease_loss=True, kill_every=(0.5, 1.0),
                     sources=("killer", "staller", "revoker", "conn_killer")),
}

# Counters a scenario must move; a zero means the run did not test that path.
REQUIRED = {
    "baseline": ["posts", "repeat_posts", "concurrent_bursts", "diff_body_409", "cancelled_from_running",
                 "long_jobs_succeeded_first_attempt"],
    "canonical": ["kills", "kills_orphaning_a_job", "takeovers", "repeat_posts", "concurrent_bursts",
                  "cancelled_from_running", "cancelled_from_queued"],
    "extended": ["kills", "takeovers", "stalls", "revokes", "conn_kills", "api_kills", "stale_completes_rejected",
                 "lease_lost_detected", "cancelled_from_running"],
    "soak": ["kills", "takeovers", "stalls", "revokes", "conn_kills", "stale_completes_rejected"],
}

# Seeded bugs: a run with the flag must FAIL on (at least one of) these.
EXPECTED_BY_FAULT = {
    "CLAIM_READ_THEN_WRITE": ["I7", "B1"],
    "NO_FENCE": ["I3", "I5", "I6"],
    "SPLIT_EFFECT": ["I3"],
    "WORKER_CLOCK": ["I7"],
    "CREATE_CHECK_THEN_INSERT": ["I2"],
    "NO_HEARTBEAT": ["B1"],
    "CANCEL_BLIND": ["I3", "I5"],
}


def git_commit() -> str | None:
    try:
        git = ROOT / ".git"
        head = (git / "HEAD").read_text().strip()
        if not head.startswith("ref: "):
            return head
        ref = head[5:]
        if (git / ref).exists():
            return (git / ref).read_text().strip()
        for line in (git / "packed-refs").read_text().splitlines():
            if line.endswith(" " + ref):
                return line.split()[0]
    except OSError:
        pass
    return None


def source_hash() -> str:
    """sha256 over the code this run executed (the image's app/ and chaos/)."""
    h = hashlib.sha256()
    for p in sorted(list((ROOT / "app").glob("*")) + list((ROOT / "chaos").glob("*.py"))):
        if p.is_file():
            h.update(p.name.encode() + b"\0" + p.read_bytes())
    return h.hexdigest()[:16]


class Log:
    def __init__(self, path: Path):
        self.f = path.open("a", encoding="utf-8")
        self.t0 = time.monotonic()

    def __call__(self, msg: str) -> None:
        line = f"[{time.monotonic() - self.t0:8.2f}s] {msg}"
        print(line, flush=True)
        self.f.write(line + "\n")
        self.f.flush()


# ---------------------------------------------------------------- processes


class Procs:
    def __init__(self, db_url: str, tmp: Path, sc: Scenario, flags: list[str], isolation: str, lease_ttl_s: float,
                 log: Log):
        self.tmp, self.sc, self.flags, self.log = tmp, sc, flags, log
        base = {k: v for k, v in os.environ.items() if not k.startswith("FAULT_")}
        base.update(DATABASE_URL=db_url, DB_ISOLATION=isolation, LEASE_TTL_S=str(lease_ttl_s), PYTHONUNBUFFERED="1")
        for f in flags:
            base[f"FAULT_{f}"] = "1"
        self.base = base
        self.workers: dict[int, asyncio.subprocess.Process] = {}
        self.api: asyncio.subprocess.Process | None = None
        self.files: dict[str, object] = {}
        self.spawned = 0

    def _out(self, name: str):
        if name not in self.files:
            self.files[name] = (self.tmp / f"{name}.log").open("ab")
        return self.files[name]

    async def spawn_worker(self, slot: int) -> None:
        env = dict(self.base, WORKER_SLOT=f"w{slot}",
                   RELEASE_ON_START="1" if self.sc.release_on_start else "0",
                   CHAOS_IGNORE_LEASE_LOSS="1" if self.sc.ignore_lease_loss else "0")
        if "WORKER_CLOCK" in self.flags and slot % 2 == 0:
            env["CLOCK_SKEW_S"] = "30"  # half of the workers think it is 30 s later
        out = self._out(f"worker-w{slot}")
        self.workers[slot] = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "app.worker", env=env, stdout=out, stderr=asyncio.subprocess.STDOUT, cwd=ROOT)
        self.spawned += 1

    async def kill_worker(self, slot: int) -> int:
        p = self.workers[slot]
        if p.returncode is None:
            p.kill()  # SIGKILL: no cleanup, no goodbye
        await p.wait()
        return p.pid

    async def start_api(self) -> None:
        self.api = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "uvicorn", "app.api:app", "--host", "127.0.0.1", "--port", str(API_PORT),
            "--no-access-log", env=self.base, stdout=self._out("api"), stderr=asyncio.subprocess.STDOUT, cwd=ROOT)

    async def wait_api(self, client, timeout: float = 30) -> None:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                r = await client.http.get("/healthz", timeout=2)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            if self.api.returncode is not None:
                raise RuntimeError(f"API exited with {self.api.returncode}; see api.log")
            await asyncio.sleep(0.1)
        raise RuntimeError("API did not become healthy")

    async def stop_all(self, grace: float = 10) -> None:
        procs = [p for p in [*self.workers.values(), self.api] if p is not None and p.returncode is None]
        for p in procs:
            try:
                p.send_signal(signal.SIGCONT)
                p.terminate()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(asyncio.gather(*(p.wait() for p in procs)), grace)
        except asyncio.TimeoutError:
            self.kill_all()

    def kill_all(self) -> None:
        for p in [*self.workers.values(), self.api]:
            if p is not None and p.returncode is None:
                try:
                    p.kill()
                except ProcessLookupError:
                    pass

    def close_files(self) -> None:
        for f in self.files.values():
            f.close()


# ------------------------------------------------------------- fault sources


@dataclass
class Ctx:
    procs: Procs
    conn: psycopg.AsyncConnection
    client: Client
    seed: int
    lease_ttl_s: float
    log: Log
    sc: Scenario
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    counts: dict = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)  # one statement at a time on the harness connection

    def rng(self, name: str) -> random.Random:
        return random.Random(f"{self.seed}:{name}")

    async def pause(self, seconds: float) -> bool:
        """Sleep, or return False early if the run is stopping."""
        try:
            await asyncio.wait_for(self.stop.wait(), seconds)
            return False
        except asyncio.TimeoutError:
            return True

    async def diag(self, kind: str, detail: dict, job_id=None, attempt=None) -> None:
        async with self.lock:
            await queue.diag(self.conn, kind, job_id, attempt, "harness", detail)
        self.counts[kind] = self.counts.get(kind, 0) + 1

    async def sql(self, statement: str):
        async with self.lock:
            cur = await self.conn.execute(statement)
            return await cur.fetchall()


async def killer(ctx: Ctx) -> None:
    rng = ctx.rng("killer")
    while await ctx.pause(rng.uniform(*ctx.sc.kill_every)):
        slot = rng.randrange(N_WORKERS)
        pid = await ctx.procs.kill_worker(slot)
        # Record first (DB clock), then respawn: a claim by the replacement can
        # never be mistaken for one made by the process that was killed.
        await ctx.diag("kill", {"slot": f"w{slot}", "pid": pid})
        await ctx.procs.spawn_worker(slot)


async def staller(ctx: Ctx) -> None:
    """SIGSTOP a worker past its lease, then SIGCONT: a zombie that wakes up holding a stale attempt."""
    rng = ctx.rng("staller")
    while await ctx.pause(rng.uniform(3, 6)):
        slot = rng.randrange(N_WORKERS)
        p = ctx.procs.workers[slot]
        if p.returncode is not None:
            continue
        p.send_signal(signal.SIGSTOP)
        await ctx.diag("stop", {"slot": f"w{slot}", "pid": p.pid})
        try:
            await asyncio.sleep(ctx.lease_ttl_s + rng.uniform(1, 3))
        finally:
            if p.returncode is None:
                p.send_signal(signal.SIGCONT)
                await ctx.diag("cont", {"slot": f"w{slot}", "pid": p.pid})


async def revoker(ctx: Ctx) -> None:
    """Move a running job's lease into the past (a DB clock jump, or any sudden lease loss).
    The revoke and its diag row are one statement, so the record never trails the takeover."""
    rng = ctx.rng("revoker")
    while await ctx.pause(rng.uniform(0.5, 2.0)):
        await ctx.sql("""
            WITH pick AS (SELECT id FROM jobs WHERE state = 'running' ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED),
                 r AS (UPDATE jobs j SET lease_expires_at = clock_timestamp() - interval '1 second'
                       FROM pick WHERE j.id = pick.id AND j.state = 'running' RETURNING j.id, j.attempt)
            INSERT INTO diag_events (kind, job_id, attempt, actor) SELECT 'revoke', id, attempt, 'harness' FROM r
            RETURNING job_id""")
        ctx.counts["revoke"] = ctx.counts.get("revoke", 0) + 1


async def conn_killer(ctx: Ctx) -> None:
    """pg_terminate_backend on a random worker or API connection: the client cannot know
    whether its last statement committed."""
    rng = ctx.rng("conn_killer")
    while await ctx.pause(rng.uniform(2, 4)):
        await ctx.sql("""
            INSERT INTO diag_events (kind, actor, detail)
            SELECT 'conn_kill', 'harness',
                   jsonb_build_object('app', application_name, 'pid', pid, 'ok', pg_terminate_backend(pid))
            FROM (SELECT pid, application_name FROM pg_stat_activity
                  WHERE datname = current_database() AND pid <> pg_backend_pid()
                    AND (application_name LIKE 'worker %' OR application_name = 'api')
                  ORDER BY random() LIMIT 1) v
            RETURNING seq""")
        ctx.counts["conn_kill"] = ctx.counts.get("conn_kill", 0) + 1


async def api_killer(ctx: Ctx) -> None:
    rng = ctx.rng("api_killer")
    while await ctx.pause(rng.uniform(20, 40)):
        p = ctx.procs.api
        p.kill()
        await p.wait()
        await ctx.diag("api_kill", {"pid": p.pid})
        await ctx.procs.start_api()
        await ctx.procs.wait_api(ctx.client)


SOURCES = {"killer": killer, "staller": staller, "revoker": revoker, "conn_killer": conn_killer,
           "api_killer": api_killer}


# --------------------------------------------------------------------- run


async def create_db(name: str, flags: list[str]) -> str:
    async with await psycopg.AsyncConnection.connect(ADMIN_URL, autocommit=True) as c:
        await c.execute(f'CREATE DATABASE "{name}"')
    url = ADMIN_URL.rsplit("/", 1)[0] + "/" + name
    await apply_schema(url)
    if "CREATE_CHECK_THEN_INSERT" in flags:
        # The seeded bug is only observable without the UNIQUE constraint behind it.
        async with await psycopg.AsyncConnection.connect(url, autocommit=True) as c:
            await c.execute("ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_key")
    return url


async def drop_db(name: str) -> None:
    async with await psycopg.AsyncConnection.connect(ADMIN_URL, autocommit=True) as c:
        await c.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


async def state_counts(ctx: Ctx) -> dict:
    rows = await ctx.sql("SELECT state, count(*) FROM jobs GROUP BY state")
    return {s: n for s, n in rows}


async def progress(ctx: Ctx, specs_total: int) -> None:
    while await ctx.pause(10):
        sc = await state_counts(ctx)
        sent = sum(1 for e in ctx.client.journal if e["op"] == "create" and e.get("idx") == 0 and "status" in e)
        ctx.log(f"progress: keys sent {sent}/{specs_total}, states {sc}, faults {ctx.counts}")


async def run(args) -> int:
    sc = SCENARIOS[args.scenario]
    if args.jobs:
        sc = replace(sc, jobs=args.jobs)
    if args.rate:
        sc = replace(sc, rate=args.rate)
    if args.release_on_start:
        sc = replace(sc, release_on_start=True)
    if args.no_sources:
        sc = replace(sc, sources=())
    flags = sorted(set(args.fault))
    for f in flags:
        if f not in fault_flags.KNOWN:
            raise SystemExit(f"unknown fault {f}; known: {sorted(fault_flags.KNOWN)}")

    label = args.label or args.scenario
    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{re.sub(r'[^A-Za-z0-9_-]', '_', label)}"
    out = RESULTS / run_id
    out.mkdir(parents=True)
    tmp = Path("/tmp/chaos") / run_id
    tmp.mkdir(parents=True)
    log = Log(out / "harness.log")
    db_name = "chaos_" + re.sub(r"\W", "_", run_id).lower()

    if args.expect:
        expected = {"verdict": "FAIL", "invariants": sorted(args.expect)}
    elif flags:
        expected = {"verdict": "FAIL", "invariants": sorted({i for f in flags for i in EXPECTED_BY_FAULT[f]})}
    else:
        expected = {"verdict": "PASS"}
    config = {
        "run_id": run_id, "label": label, "scenario": args.scenario, "seed": args.seed, "flags": flags,
        "isolation": args.isolation, "lease_ttl_s": args.lease_ttl, "workers": N_WORKERS, "params": asdict(sc),
        "expected": expected, "required_coverage": REQUIRED[args.scenario] if sc.sources or args.scenario == "baseline" else [],
        "git_commit": git_commit(), "source_sha256": source_hash(), "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "database": db_name, "note": args.note,
    }
    (out / "config.json").write_text(json.dumps(config, indent=2))
    log(f"run {run_id}: {args.scenario} seed={args.seed} jobs={sc.jobs} rate={sc.rate}/s sources={list(sc.sources)} "
        f"flags={flags} isolation={args.isolation} release_on_start={sc.release_on_start} "
        f"ignore_lease_loss={sc.ignore_lease_loss}; expect {expected}")

    summary: dict = {"run_id": run_id, "completed": False}
    procs = client = ctx = None
    checker = None
    keep_db = True
    t_start = time.monotonic()
    try:
        db_url = await create_db(db_name, flags)
        procs = Procs(db_url, tmp, sc, flags, args.isolation, args.lease_ttl, log)
        client = Client(f"http://127.0.0.1:{API_PORT}", t0=time.monotonic())
        await procs.start_api()
        await procs.wait_api(client)
        for slot in range(N_WORKERS):
            await procs.spawn_worker(slot)
        conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True, application_name="harness")
        ctx = Ctx(procs=procs, conn=conn, client=client, seed=args.seed, lease_ttl_s=args.lease_ttl, log=log, sc=sc)

        specs = make_plan(sc.jobs, sc.rate, args.seed, prefix=run_id, profile=sc.profile)
        log(f"plan: {len(specs)} keys over ~{specs[-1].t:.0f}s; bursts={sum(s.burst > 1 for s in specs)} "
            f"delayed-repeats={sum(len(s.delayed) for s in specs)} diff-body={sum(s.diff_body for s in specs)} "
            f"cancels={sum(1 for s in specs if s.cancel)} long(>TTL)={sum(s.payload['work_ms'] > args.lease_ttl * 1000 for s in specs)}")

        client.t0 = time.monotonic()
        sources = [asyncio.create_task(SOURCES[name](ctx), name=name) for name in sc.sources]
        reporter = asyncio.create_task(progress(ctx, len(specs)))
        await asyncio.gather(*(client.run_spec(s) for s in specs))
        t_load = time.monotonic() - client.t0
        log(f"load done in {t_load:.1f}s; stopping fault sources")
        ctx.stop.set()
        for t in sources:
            await t  # a source that crashed re-raises here: the run is a harness failure, not a pass
        reporter.cancel()

        t_drain = time.monotonic()
        drained = False
        while time.monotonic() - t_drain < args.drain_timeout:
            c = await state_counts(ctx)
            if c.get("queued", 0) + c.get("running", 0) == 0:
                drained = True
                break
            await asyncio.sleep(0.5)
        log(f"drain {'complete' if drained else 'TIMED OUT'} after {time.monotonic() - t_drain:.1f}s: {await state_counts(ctx)}")

        walk_all = await client.walk()
        walk_succ = await client.walk(state="succeeded")
        await procs.stop_all()
        log(f"processes stopped; worker processes spawned in total: {procs.spawned}")

        checker = Checker(specs, client.journal, walk_all, walk_succ,
                          has_process_faults=bool(set(sc.sources) & {"killer", "staller", "revoker", "conn_killer"}),
                          lease_ttl_s=args.lease_ttl)
        await checker.load(conn)
        checker.run()
        await conn.close()

        counts = checker.counts()
        failed = sorted(k for k, n in counts.items() if n)
        not_covered = [k for k in config["required_coverage"] if not checker.cov.get(k)]
        verdict = "FAIL" if failed else ("NOT COVERED" if not_covered else "PASS")
        if expected["verdict"] == "PASS":
            as_expected = verdict == "PASS"
        else:
            as_expected = verdict == "FAIL" and bool(set(expected["invariants"]) & set(failed))
        summary.update(
            completed=True, verdict=verdict, expected=expected, as_expected=as_expected,
            violations=counts, failed_invariants=failed, not_covered=not_covered, coverage=checker.cov,
            client_errors=dict(client.errors), gets_nonterminal=client.gets_nonterminal, drained=drained,
            durations_s={"load": round(t_load, 1), "total": round(time.monotonic() - t_start, 1)},
            worker_processes_spawned=procs.spawned, fault_actions=ctx.counts,
        )
        keep_db = verdict != "PASS"
        print_summary(log, summary)
        return 0 if as_expected else 1
    except BaseException as e:
        summary["harness_error"] = f"{type(e).__name__}: {e}"
        summary["traceback"] = traceback.format_exc()
        log(f"HARNESS ERROR: {summary['harness_error']}\n{summary['traceback']}")
        return 2
    finally:
        if procs:
            procs.kill_all()
            procs.close_files()
        if client:
            await client.close()
        save(out, tmp, summary, checker, client)
        if keep_db:
            log(f"database {db_name} kept for inspection")
        else:
            await drop_db(db_name)
        index.write(RESULTS)
        log(f"results in results/{run_id}/")


def print_summary(log: Log, s: dict) -> None:
    log("invariants: " + "  ".join(f"{k}={'ok' if n == 0 else f'FAIL({n})'}" for k, n in s["violations"].items()))
    c = s["coverage"]
    log(f"coverage: kills={c['kills']} (orphaning {c['kills_orphaning_a_job']}) takeovers={c['takeovers']} "
        f"by cause {c['takeovers_by_cause']} unexplained={c['takeovers_unexplained']}; "
        f"recovery after kill {c['recovery_after_kill_s']}")
    log(f"coverage: stale completes rejected={c['stale_completes_rejected']} {c['stale_completes_by_reason']} "
        f"lease_lost={c['lease_lost_detected']} exhausted={c['attempts_exhausted']} max_attempt={c['max_attempt']} "
        f"executions/job={c['executions_per_job']}")
    log(f"coverage: cancelled from queued={c['cancelled_from_queued']} from running={c['cancelled_from_running']} "
        f"responses={c['cancel_responses']} sent-while-running={c['cancel_sent_while_seen_running']} "
        f"lost-race={c['cancel_lost_race_to_finish']}")
    log(f"coverage: posts={c['posts']} repeats={c['repeat_posts']} bursts={c['concurrent_bursts']} "
        f"201-not-to-first={c['burst_201_not_to_first_sent']} no-201-seen={c['keys_without_any_201']} "
        f"diff-body-409={c['diff_body_409']}; long jobs {c['long_jobs']} ok-first-try {c['long_jobs_succeeded_first_attempt']}")
    log(f"final states: {c['final_states']}; client errors: {s['client_errors']}; fault actions: {s['fault_actions']}")
    if s["not_covered"]:
        log(f"NOT COVERED: {s['not_covered']}")
    log(f"VERDICT: {s['verdict']} (expected {s['expected']['verdict']}"
        f"{' on ' + ','.join(s['expected'].get('invariants', [])) if s['expected'].get('invariants') else ''})"
        f" -> {'as expected' if s['as_expected'] else 'UNEXPECTED'}")


def save(out: Path, tmp: Path, summary: dict, checker: Checker | None, client: Client | None) -> None:
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    if checker is not None:
        v = {"counts": checker.counts(),
             "samples": {k: vs[:25] for k, vs in checker.violations.items() if vs},
             "histories": checker.histories()}
        (out / "violations.json").write_text(json.dumps(v, indent=2, default=str))
    if client is not None:
        with gzip.open(out / "client-journal.jsonl.gz", "wt", encoding="utf-8") as f:
            for e in client.journal:
                f.write(json.dumps(e, default=str) + "\n")
    logs = out / "logs"
    logs.mkdir(exist_ok=True)
    for p in sorted(tmp.glob("*.log")):
        with p.open("rb") as src, gzip.open(logs / (p.name + ".gz"), "wb") as dst:
            shutil.copyfileobj(src, dst)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=sorted(SCENARIOS))
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--jobs", type=int)
    ap.add_argument("--rate", type=float)
    ap.add_argument("--label")
    ap.add_argument("--note", default="")
    ap.add_argument("--fault", action="append", default=[], help="seeded bug from app/faults.py (repeatable)")
    ap.add_argument("--expect", action="append", choices=INVARIANTS,
                    help="expect FAIL on this invariant (overrides the per-fault default; repeatable)")
    ap.add_argument("--isolation", default="read_committed", choices=["read_committed", "repeatable_read", "serializable"])
    ap.add_argument("--lease-ttl", type=float, default=5.0)
    ap.add_argument("--release-on-start", action="store_true")
    ap.add_argument("--no-sources", action="store_true", help="disable the scenario's fault sources")
    ap.add_argument("--drain-timeout", type=float, default=120.0)
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
