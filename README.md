# Soup job queue

A training-job queue on Postgres: an HTTP API, N workers, one table, one SQL statement per state transition. Built as the Soup backend take-home.

- **Design note** (written before the code): [docs/DESIGN.md](docs/DESIGN.md)
- **Report**: chaos results, silent failures, what the harness does not catch, data for the verdict, AI usage: [REPORT.md](REPORT.md)
- **Every chaos run, including the failed ones**: [results/INDEX.md](results/INDEX.md)

## Run it

```sh
docker compose up            # postgres, migrate, api on :8000, 4 workers
```

```sh
curl -s -XPOST localhost:8000/jobs -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
     -d '{"work_ms": 2000}'                               # 201 new, 200 repeat, 409 same key + other body
curl -s localhost:8000/jobs/1                              # status
curl -s 'localhost:8000/jobs?state=succeeded&limit=20'     # keyset page: {items, next_cursor}
curl -s -XPOST localhost:8000/jobs/1/cancel                # 200 cancelled, 409 already finished
```

A job's payload is `{"work_ms": int, "outcome": "ok" | "fail"}`. The work is emulated as a sleep. The side effect is a row in `effects`, written in the same statement as `running → succeeded`, so the number of effects per job can be counted.

## Prove it

| What | Command | Output |
|---|---|---|
| Exhaustive check of the protocol model (every interleaving, plus 5 buggy variants that must fail) | `python model/check.py` | [results/model-check.txt](results/model-check.txt) |
| Deterministic races R1–R7 on a real Postgres | `docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.races` | `results/races-*.txt` |
| Chaos run (fresh DB, own API + 8 workers, `kill -9` every ~2 s, duplicate POSTs, cancels) | `docker compose --profile chaos run --rm chaos canonical --seed 1` | `results/<run>/` |
| Same with a seeded bug that must be caught | `docker compose --profile chaos run --rm chaos baseline --fault NO_FENCE` | `results/<run>/` |
| Open-loop load test at 100 rps against the standard stack | `docker compose up -d && docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.loadtest` | `results/load/<run>/` |
| API smoke test | `docker compose run --rm --no-deps api python -m chaos.smoke` | stdout |

Scenarios: `baseline` (2 000 jobs, no process faults), `canonical` (the brief: 10 000 jobs, 8 workers, SIGKILL of a random worker every 1.5–2.5 s, duplicate POSTs, cancels), `extended` (canonical plus SIGSTOP zombies, lease revocation, `pg_terminate_backend` on worker and API connections, SIGKILL of the API; workers ignore lost leases so the fencing is hit every time), `soak` (100 000 jobs of 0–5 ms, faults more often).

Each run directory contains `config.json` (all parameters, git commit, the expected outcome written before the run), `harness.log`, `summary.json` (invariants, coverage counters, verdict), `violations.json` (full history of the violating jobs), the client journal and every process log. `results/INDEX.md` is regenerated from the directories after every run, so no run can drop out of it.

## Layout

```
app/schema.sql     tables, partial index, audit trigger
app/queue.py       the protocol: one function per operation, one statement per transition
app/api.py         FastAPI; each handler is one call into app.queue
app/worker.py      claim -> record execution -> work + heartbeat -> fenced complete
app/faults.py      seeded bugs, off by default (FAULT_<NAME>=1)
model/check.py     BFS over all interleavings of the abstract protocol
chaos/races.py     forced interleavings on Postgres (R1-R7)
chaos/harness.py   chaos runs: processes, fault sources, drain, results
chaos/load.py      job plan and an HTTP client that retries unknown outcomes
chaos/invariants.py I1-I10, B1 and the coverage counters
chaos/loadtest.py  open-loop load test
```

## Model ↔ code

| Model action (`model/check.py`) | Statement (`app/queue.py`) |
|---|---|
| `claim(w)` (queued, or running with an expired lease) | `claim()`: `FOR UPDATE SKIP LOCKED` CTE + `UPDATE … attempt = attempt + 1` |
| `heartbeat(w)` | `heartbeat()`: `UPDATE … WHERE id AND attempt AND state = 'running'` |
| `complete(w)` | `complete()`: `UPDATE … succeeded` + `INSERT INTO effects` in one statement, same guard |
| `fail(w)` | `fail()`: same guard |
| `cancel` | `cancel()`: `UPDATE … WHERE state IN ('queued','running')` |
| `expire` | time passing: `lease_expires_at <= now()` becomes true (or `release_slot()` after a restart) |
| `crash(w)` | the worker process dies; its connection and in-memory attempt vanish |
