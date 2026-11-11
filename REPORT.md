# Report

What was run, what it shows, and what it does not show. Every number here comes from a file under [results/](results/); [results/INDEX.md](results/INDEX.md) lists every run, including the debugging runs and any failed run.

## 1. How correctness was checked

Four independent layers, each catching what the others cannot:

| Layer | What it shows | Where |
|---|---|---|
| Exhaustive model check | The protocol is safe under **every** interleaving of claim / expire / heartbeat / complete / fail / crash / cancel (2–3 workers, up to 4 attempts, 448 states). Each of 5 buggy variants is caught, with the shortest breaking trace. | [model/check.py](model/check.py), [results/model-check.txt](results/model-check.txt) |
| Deterministic races R1–R7 | Real Postgres behaves the way the model assumes: each race is forced into one exact interleaving (the lock wait is read from `pg_stat_activity`, not guessed with a sleep). | [chaos/races.py](chaos/races.py), `results/races-*.txt` |
| Chaos runs | The assembled system (API + 8 worker processes + Postgres) under real `SIGKILL`, `SIGSTOP`, lease loss, dropped connections and API restarts, checked against 10 invariants read from the database audit trail. Seeded bugs prove the harness can see failures. | [chaos/harness.py](chaos/harness.py), `results/<run>/` |
| Load test | Latency and queue behaviour at 100 rps on the standard `docker compose up` stack. | [chaos/loadtest.py](chaos/loadtest.py), `results/load/` |

### Invariants (checked after every chaos run, from the DB and the client journal)

| | Claim |
|---|---|
| I1 | Nothing lost, nothing extra: the set of keys in the DB equals the set sent; every key was acknowledged. |
| I2 | Idempotency: one row per key; every 2xx for a key named the same id; at most one 201 per key; a different body got 409. |
| I3 | Effects: a succeeded job has exactly one effect, of its final attempt, written by that attempt's owner; any other job has none. |
| I4 | Nothing stuck: no job is queued or running after the drain. |
| I5 | Legal history: `job_events` (written by a trigger) is a chain of allowed transitions, `attempt` goes up by exactly 1 per claim, nothing after a terminal state, and the row agrees with its history. |
| I6 | The API did not lie: a cancel answered 200 ended cancelled; a 409 or a GET that showed a terminal state showed the final state. |
| I7 | Leases respected: every takeover happened after the previous lease had expired (both timestamps on the DB clock). |
| I8 | Execution witness: each `(job, attempt)` started at most once, by the attempt's owner; a succeeded job executed its final attempt. |
| I9 | Plausible outcome; a job that ran out of attempts must have every lost attempt explained by a recorded fault (kill, stop, dropped connection or revoke of its owner). |
| I10 | Pagination: a full keyset walk of `GET /jobs` (and of `?state=succeeded`) returns every id exactly once, ascending. |
| B1 | Runs without process faults only: every job executed exactly once, no takeovers. |

A run passes only if every invariant holds **and** the coverage counters its scenario must move are non-zero. A green run that never killed a worker that held a job, or never raced a cancel against a running job, is reported as `NOT COVERED`, not `PASS`. That happened to the first debugging run (300 jobs contained no job longer than the lease, so heartbeats were never exercised).

## 2. Chaos results

### The brief's scenario (`canonical`): 10 000 jobs, 8 workers, `kill -9` of a random worker every 1.5–2.5 s, duplicate POSTs and cancels

Six runs, six different seeds, all **PASS**. `RELEASE_ON_START=0` in C1–C5, so recovery relies only on lease expiry; C6 turns on the restart fast path.

| Run | Kills | …that orphaned a job | Takeovers (all explained by a kill) | Recovery after kill p50 / max | Stale completes rejected | Cancelled while running | Executions per job (count of jobs) |
|---|---|---|---|---|---|---|---|
| C1 | 135 | 76 | 76 | 4.96 s / 5.02 s | 172 | 282 | 1×9709, 2×65, 3×3 |
| C2 | 130 | 70 | 70 | 4.94 s / 5.13 s | 176 | 288 | 1×9690, 2×52, 3×4, 5×1 |
| C3 | 130 | 76 | 76 | 4.94 s / 5.03 s | 133 | 240 | 1×9681, 2×62, 3×3, 4×1, 5×1 |
| C4 | 129 | 87 | 87 | 4.93 s / 5.04 s | 154 | 280 | 1×9667, 2×65, 3×3, 4×4, 5×1 |
| C5 | 135 | 75 | 75 | 4.93 s / 5.04 s | 163 | 274 | 1×9683, 2×66, 3×2, 4×1 |
| C6 (fast path) | 131 | 71 | 71 | **0.79 s / 0.93 s** | 152 | 247 | 1×9695, 2×57, 3×4, 4×1 |

Totals over C1–C6 (60 000 jobs): 790 kills, 455 orphaned jobs, 455 takeovers, **0 takeovers without a recorded cause**, 950 stale completes rejected, 1 611 jobs cancelled while running and 1 479 while queued, 491 cancels that lost the race to a finishing job (answered 409 with the real final state), 27 226 repeated POSTs including 5 993 concurrent bursts of 3–5 identical requests (in 185 bursts the 201 went to a request other than the first one sent), 300 same-key-different-body POSTs (all 409). **Zero duplicate effects, zero lost jobs, zero stuck jobs.** One job (C3) exhausted its 5 attempts; see §5.

Jobs that never executed (`0` executions, about 250 per run) are jobs cancelled while still queued.

### Extended: every fault source at once (X1–X3, 10 000 jobs each, all PASS)

`SIGKILL` of a worker every 1.5–2.5 s, `SIGSTOP` of a worker for TTL + 1–3 s every 3–6 s (a zombie that wakes up holding a stale attempt), a revoker that moves a running job's lease into the past every 0.5–2 s, `pg_terminate_backend` on a random worker or API connection every 2–4 s, and `SIGKILL` of the API every 20–40 s. Workers run with `CHAOS_IGNORE_LEASE_LOSS=1`: they keep working after losing the lease and always try to complete, so the fence in `complete` is hit on every lost lease.

| Run | Kills / stops / revokes / conn kills / API kills | Takeovers by cause | Unexplained | Stale completes rejected (taken over / cancelled) | Exhausted (all explained) |
|---|---|---|---|---|---|
| X1 | 135 / 25 / 197 / 87 / 9 | revoke 101, kill 94, conn 24, stop 19 | 0 | 310 (112 / 198) | 9 |
| X2 | 131 / 23 / 194 / 88 / 8 | revoke 124, kill 80, conn 17, stop 11 | 0 | 338 (121 / 217) | 7 |
| X3 | 132 / 24 / 219 / 88 / 9 | revoke 131, kill 82, conn 17, stop 13 | 0 | 312 (124 / 188) | 10 |

So in each run about 120 zombies completed a job that someone else had taken over, and every one of those writes matched 0 rows. The client saw about 1 000–1 300 connection errors per run (API down) and a handful of ambiguous ones (`ReadError`, `RemoteProtocolError`, one `503`), retried all of them with the same key, and I1/I2 held: no lost key, one row per key. Attempt exhaustion is higher here (7–10 per run) because jobs lose leases to revokes and stops as well as kills; I9 matched every lost attempt of every exhausted job to a recorded fault.

### Isolation levels (canonical, 10 000 jobs)

| Run | Default isolation (confirmed in each process's start-up log) | Result |
|---|---|---|
| C1–C6 | READ COMMITTED | PASS |
| ISO1 | REPEATABLE READ | PASS (131 kills, 74 takeovers, 137 stale completes rejected) |
| ISO2 | SERIALIZABLE | PASS (132 kills, 70 takeovers, 141 stale completes rejected) |

The protocol is correct at all three levels because every transition is one statement whose condition is in its own `WHERE`; at the stricter levels a lost race surfaces as `40001`, which `_one` retries. **Gap:** the number of `40001` retries per process is counted (`queue.retry_count`) but not exported, so these runs do not show *how many* retries the stricter levels cost; R5 shows the mechanism deterministically.

### Seeded bugs: the harness sees what it claims to see

Each run enables one bug from [app/faults.py](app/faults.py) and records the expected failing invariant in `config.json` **before** it starts. Seeded-bug runs use 2 000 jobs.

| Run | Seeded bug | Scenario | Expected | Got (violations) |
|---|---|---|---|---|
| F1 | `CLAIM_READ_THEN_WRITE`: unlocked `SELECT`, then `UPDATE` by id | baseline | I7, B1 | **FAIL** I7 1 373, B1 1 055, I9 20, I5 1. I3 held: the fence in `complete` still stopped every duplicate effect |
| F2 | `NO_FENCE`: `complete` without attempt/state | extended | I3, I5, I6 | **FAIL** I3 41, I5 28, I6 37 |
| F3 | `SPLIT_EFFECT`: effect inserted before the guarded transition | extended | I3 | **FAIL** I3 80 |
| F4 | `WORKER_CLOCK`: lease compared with worker clocks, half of them +30 s | baseline | I7 | **FAIL** I7 766, I9 853, B1 189, I5 1 |
| F5 | `CREATE_CHECK_THEN_INSERT`, UNIQUE dropped | canonical | I2 | **FAIL** I2 522 |
| F6 | `NO_HEARTBEAT` | baseline | B1 | **FAIL** B1 5, I9 30 |
| F7 | `CANCEL_BLIND`: cancel without a state condition | canonical | I3, I5 | **FAIL** I3 58, I5 62, I6 44 |
| F8 | `CLAIM_READ_THEN_WRITE` + `NO_FENCE`: both lines of defence removed | baseline | I3 | first run **ABORTED** (harness error, see below); rerun **FAIL** I3 1 308, I7 718, B1 646, I6 33, I5 27, I8 3 |

All eight bugs were caught at 2 000 jobs. F1 against F8 shows the two lines of defence separately: a broken claim alone produces hundreds of double executions but no double effect; only when the fence is also gone do effects multiply.

**The aborted F8.** About 50 s in, every request from the harness timed out and the run ended in a harness error before the checks ran ([NOTES.md](results/20261111-035046-F8/NOTES.md)). It was not reproduced by the rerun. The most likely cause was found later, in the load generator (§6): the harness's HTTP client used one httpx pool of 200 connections, and under a slowdown httpcore's request-to-connection matching becomes quadratic, so the *client* pegs a CPU and times out everything it sends. This F8 started minutes after the laptop resumed from sleep, its own progress loop stalled for up to 68 s, and its errors were all client-side timeouts, which fits. The client now uses 16 small pools, and a hung API now fails I10 instead of aborting the run. All other chaos runs finished with the old client; a client meltdown can only make a run fail (timeouts, unacknowledged keys), never hide a violation, and none of them showed one.

## 3. Silent failures

Each one: the mechanism that prevents it, the deterministic proof on Postgres, the statistical evidence from chaos runs, and the seeded bug that shows the harness would have seen it.

### Double claim

- **Mechanism.** `claim` is one statement: a `FOR UPDATE SKIP LOCKED` CTE picks a row, the `UPDATE` sets `attempt = attempt + 1` under the same row lock. Two claims can never both get the same row; a row that a concurrent claim already locked is skipped, not waited for.
- **Deterministic (R1).** A holds an uncommitted claim; B's claim returns nothing in 7 ms, and still nothing after A commits (live lease). 1 000 rounds of 50 connections claiming one new job at the same instant: exactly one winner every round. The seeded `CLAIM_READ_THEN_WRITE` (unlocked `SELECT`, then `UPDATE` by id) had more than one winner in 200 of 200 rounds, up to all 50.
- **Statistical.** I7 and I8 in every chaos run: no `(job, attempt)` executed twice, no takeover of a live lease.
- **Seeded bug.** F1 (`CLAIM_READ_THEN_WRITE`): 1 373 takeovers of live leases (I7) and 1 055 jobs executed more than once (B1) in a 2 000-job run without any process fault.

### Zombie worker (comes back after its lease expired and writes a stale result)

- **Mechanism.** `complete` and `fail` are fenced: `WHERE id = $1 AND attempt = $2 AND state = 'running'`. A takeover increments `attempt`, so anything the old holder writes matches 0 rows. Safety does not depend on the lease timing at all: a late `complete` whose job nobody took over is accepted, because it is still the only grant.
- **Deterministic (R2).** A's lease expires, B takes over (attempt 2), A completes attempt 1: 0 rows, no effect; B completes: one effect. With nobody taking over, A's late complete is accepted. With the fence removed (`NO_FENCE`), both complete and the job has 2 effects.
- **Statistical.** In `extended`, workers ignore a lost lease (`CHAOS_IGNORE_LEASE_LOSS=1`) and always try to complete, while `SIGSTOP` freezes workers past their lease and a revoker moves leases into the past. X1–X3: 357 stale completes from workers whose job had been taken over, all rejected; 0 duplicate effects.
- **Seeded bug.** F2 (`NO_FENCE`): 41 jobs with a wrong effect count, 37 API lies. F3 (`SPLIT_EFFECT`, effect written in a separate statement before the guarded transition): 80 jobs with an effect that does not belong to their final state.

### Cancel / complete race

- **Mechanism.** Both are single conditional `UPDATE`s on the same row. Whichever commits first wins; the other waits on the row lock, re-checks its `WHERE` against the new row version (READ COMMITTED) and matches 0 rows. `complete` writes its effect in the same statement, so "cancelled with an effect" cannot exist. A cancel that loses returns `409` with the job's real state.
- **Deterministic (R3).** Both orders with a real lock wait: cancel first → complete returns 0 rows, 0 effects; complete first → cancel returns `conflict`/`succeeded` (API 409), 1 effect. `CANCEL_BLIND` (no state condition) acknowledges the cancel over a succeeded job, leaving a cancelled job with an effect.
- **Statistical.** C1–C6: 1 611 running jobs cancelled, 491 cancels lost to a finishing job, 950 completes rejected after a cancel; I3 and I6 hold in all of them (and in X1–X3, ISO1–ISO2).
- **Seeded bug.** F7 (`CANCEL_BLIND`): 58 cancelled jobs with an effect (I3), 62 illegal `succeeded → cancelled` transitions (I5), 44 cancels acknowledged for jobs that had already succeeded (I6).

### Duplicate on retry

- **Mechanism.** `POST /jobs` requires an `Idempotency-Key` (UNIQUE). `INSERT … ON CONFLICT DO NOTHING RETURNING`; if nothing comes back, a *separate* `SELECT` returns the existing row (a separate statement gets a fresh snapshot that includes the conflicting row). Same body → 200, different body (sha256 of canonical JSON) → 409. Every endpoint is safe to retry, so the API answers 503 "outcome unknown, retry" when it loses the database, and the client retries with the same key.
- **Deterministic (R4).** B's create waits on A's uncommitted insert of the same key, then returns A's job (`created=False`); if A rolls back, B creates it. The rejected one-statement design (`INSERT … ON CONFLICT` + `UNION SELECT`) returns **zero rows** in that race, because the whole statement uses one snapshot taken before A committed. A check-then-insert is stopped only by the UNIQUE constraint.
- **Statistical.** C1–C6: 27 226 repeated POSTs, 5 993 concurrent bursts, one row per key and at most one 201 per key (I2) in every run. In `extended` the API is killed every 20–40 s and its connections are terminated: X1–X3 retried about 3 100 failed creates with the same key and still had one row per key. Most of those failures were refused connections (the request never reached the server); only a few dozen were truly ambiguous (`ReadError`, `RemoteProtocolError`, `503`), and in 2 keys the 201 was lost and the retry got 200 with the same id. That is the path the idempotency key exists for, but it was hit rarely; R4 covers it deterministically.
- **Seeded bug.** F5 (check-then-insert, UNIQUE dropped): 522 I2 violations (several rows per key, several 201s per key, 2xx answers naming different ids).

### Isolation level

- **Mechanism.** READ COMMITTED is enough because no code path reads, decides in Python and writes back: every precondition is in the `WHERE` of the statement that writes. Each process logs `autocommit=True, transaction_isolation=read committed` at start-up and refuses to run without autocommit (`app/db.py`), so no connection holds locks across round trips.
- **Deterministic (R5).** The same complete racing a committed cancel: at READ COMMITTED a clean 0 rows; at REPEATABLE READ a `40001` serialization failure, which `_one` retries (the statement definitely did not apply) and then gets 0 rows. The difference that matters is not the level but the guard: an unguarded read-then-write at READ COMMITTED silently loses an acknowledged cancel (the job ends `succeeded`); the same read-then-write inside one REPEATABLE READ transaction is refused.
- **Statistical.** ISO1 (REPEATABLE READ) and ISO2 (SERIALIZABLE): canonical runs, both PASS. Correctness does not depend on the level; the level only changes whether a lost race is a clean 0 rows or a retried `40001`.
- **Seeded bug.** `CLAIM_READ_THEN_WRITE` is exactly the read-then-write pattern that READ COMMITTED does not protect: F1 and F8 (above).

### Clock skew

- **Mechanism.** No client timestamp ever reaches SQL. Lease expiry is set with `clock_timestamp()` and compared with `now()`, both on the database clock, so worker clocks can be arbitrarily wrong. Safety does not depend on any clock: a lease that expires too early (a DB clock jump forward, a GC pause, a frozen VM) only causes a takeover, and the fence rejects the old holder.
- **Deterministic (R6).** With a live lease, the correct claim takes nothing; the seeded `WORKER_CLOCK` (lease compared with a worker clock 30 s ahead) takes over the live lease, and the audit trail shows the takeover happened before expiry, which is what I7 checks.
- **Statistical.** The revoker in `extended` moves a running job's lease into the past every 0.5–2 s, which is what a forward jump of the DB clock would do: 610 revokes in X1–X3, 356 of them led to a takeover, 0 duplicate effects.
- **Seeded bug.** F4 (`WORKER_CLOCK`, half of the workers 30 s ahead): 766 takeovers of live leases (I7) in a run without any fault.
- **Not covered:** a *backward* jump of the DB clock (leases would live longer: slower recovery, never a double effect), and libfaketime-skewed worker processes (only the seeded bug simulates a skewed worker).

## 4. What the harness does not catch

- **Postgres itself failing.** No `kill -9` of Postgres, no fsync loss, no failover to an async replica. A failover that loses the last committed transactions could resurrect a job that already succeeded, and the effect would run again. The guarantees assume one durable primary.
- **External effects.** The effect is a row in the same database, so it commits atomically with `succeeded`. A real training job writes checkpoints to S3 or calls other services; those are at-least-once and need an idempotent receiver keyed by `job_id` (or an outbox). The harness does not test that boundary.
- **Network faults other than a dropped connection.** `pg_terminate_backend` gives a clean reset. Slow links, packet loss, half-open TCP connections (a commit that succeeds while the client hangs until a timeout) and partitions between the API and the database are not simulated.
- **Long-term behaviour.** Each run is 5 minutes on an empty database. Bloat of `jobs` from heartbeat updates, autovacuum keeping up, index growth over millions of finished jobs and unbounded growth of `job_events` / idempotency keys are not exercised (§6 has what L1 showed).
- **Scale.** At most 8 workers and about 40 jobs/s in chaos runs (limited by the emulated work, not by the queue). Contention with hundreds of workers polling the same index, and the cost of the claim scan with a deep backlog of expired leases, are not tested.
- **Timing windows narrower than the sample.** Chaos runs can only show that a race did not happen in this sample. Narrow windows are covered by the model check and R1–R7 instead, not by more random runs.
- **Bugs in the checker's own source of truth.** The invariants trust `job_events`, written by a trigger. A write path that bypassed the trigger (or a bug in it) would be invisible. The trigger skips updates that change only `lease_expires_at` (heartbeats), so heartbeats themselves are not audited.
- **Same machine, same clock.** Harness, API, workers and Postgres share one host and one kernel clock; the revoker stands in for clock jumps.
- **Worker crashes caused by the job itself** (a poison payload that kills the worker every time). `max_attempts` bounds it, but no run used such a payload.
- **One API process.** The API is stateless, so several instances should behave like one, but that was not run.

### Was 10 000 jobs enough?

For the brief's scenario, yes for what random testing can show: each canonical run produced 70–87 orphaned jobs, 240–290 cancels of running jobs (about 80 of them lost to a finishing job) and about 1 000 concurrent create bursts, and every seeded bug was caught at 2 000 jobs, most of them hundreds of times over. It is not enough to show the *absence* of rare races, and more random jobs would not change that. That is why the race-sensitive steps are proven separately by the exhaustive model check (every interleaving) and forced on Postgres by R1–R7.

## 5. Findings worth knowing

- **About 1% of long jobs fail by attempt exhaustion under the canonical kill rate, and that is correct.** A job of 6–9 s on one of 8 workers, with a kill every ~2 s, loses an attempt with probability ≈ 1 − e^(−7.5/16) ≈ 37%; losing 5 in a row is ≈ 0.7% of the long jobs (≈ 0.35 jobs per run). Observed: 1 job in 60 000 (C3), ending `failed: attempts exhausted`, with every lost attempt matched by I9 to a kill of its owner. This is the trade-off of `max_attempts`: a bound on poison jobs costs some innocent long jobs under heavy infrastructure failure. Raising it, or not counting attempts lost to infrastructure, are the options; the second needs a reliable way to tell the two apart.
- **Recovery time is the lease TTL unless the worker restarts.** p50 4.93–4.96 s with TTL 5 s; with the restart fast path (`RELEASE_ON_START`, C6) 0.79 s. The fast path is safe even when wrong, because the fence, not the lease, protects the effect.
- **`docker kill` does not trigger `restart: unless-stopped`.** Docker treats it as a manual stop. A worker killed that way stays dead, its jobs are recovered by lease expiry, and the fast path never runs. A process dying inside the container (`kill -9` of PID > 1) does trigger the restart, and the fast path recovered its job in 1.2 s. The harness kills processes directly.
- **The chaos submission rate is 40 jobs/s, not 100.** With the job mix (95% short, 5% 0.3–1.5 s, 0.5% 6–9 s) 8 workers complete about 50 jobs/s; at 80–100/s the backlog would grow without bound and the run would measure queueing, not faults. 100 rps on the API is measured separately (§6).

## 6. Data for the verdict at 100 rps

PENDING: L1.

**Verdict: TODO(human)** — SHIP or DON'T SHIP at 100 rps, and if not, what to fix first.

## 7. What surprised me and what still concerns me

TODO(human)

## 8. How AI was used

The code, the harness, the model checker and the first drafts of DESIGN.md, README.md and this report were written by an AI assistant (Claude, in Claude Code), working in this repository on my machine. I chose the stack (Python, Postgres) and the way of working, asked for the architecture to be explained to me before any code was written, and asked for the first plan to be reworked into a stronger one. The verdict (§6) and §7 are mine.

TODO(human): say what you read and checked yourself, and how (for example: which files you read line by line, which runs you re-ran, which numbers you checked against `results/`). Do not claim checks you did not do.

**How the AI's work was checked.** Not by trusting it: every claim in this report points to a file in `results/` produced by code in this repository, and the harness was itself tested by seeded bugs that must make it fail (F1–F8). Claims that were first only in the plan were then tested by experiment: the one-statement `INSERT … UNION` design really returns 0 rows under a race (R4); REPEATABLE READ really raises `40001` where READ COMMITTED re-checks the row (R5); the heartbeat really does not write audit rows (smoke test).

**Where the AI's first decision was wrong**, and what showed it:

| First decision | What showed it was wrong | Now |
|---|---|---|
| A separate reaper process returning expired jobs to `queued` | Reviewing the plan: one more process and one more transition for no gain | Expired leases are taken over inside `claim` |
| Chaos load at 80–100 jobs/s as in the brief | Arithmetic on the job mix: 8 workers finish about 50 jobs/s, so the backlog would grow without bound | 40 jobs/s; 100 rps measured separately (§6) |
| `docker kill` would trigger the container restart policy | Manual test: Docker treats it as a manual stop | The harness kills processes directly; documented in §5 |
| One httpx connection pool of 200 for the harness client | The first load test: client at 100% CPU, API idle; cProfile showed httpcore's pool matching | 16 small pools; also the likely cause of the aborted F8 |
| The first load-test numbers (p95 ≈ 11 s at 79 rps) | Same: they measured the client | Discarded as `dev-*` runs, kept in `results/load/` |
| A model-checker counterexample for the split-effect bug | Its trace was trivial, then physically impossible | Mid-operation states modelled explicitly |
| A 300-job debugging run looked green | Its coverage counters: no job longer than the lease, so heartbeats were never tested | Coverage gates every verdict (`NOT COVERED`) |

The AI also ran every command, so the raw outputs in `results/` are what the programs printed; none were edited afterwards. Failed and aborted runs are kept and listed in [results/INDEX.md](results/INDEX.md).
