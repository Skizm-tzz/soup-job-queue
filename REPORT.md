# Report

A job queue on Postgres, attacked with `kill -9`, and what I learned from it.

This is the short report. Every number in it can be recomputed from the raw run files with `python chaos/numbers.py`; its output is saved as [results/NUMBERS.txt](results/NUMBERS.txt). The long report with every detail is [REPORT-full.md](REPORT-full.md).

## Verdict: SHIP at 100 rps, with three conditions

Why ship:

- In 70 000 jobs under `kill -9` (7 runs of the brief's scenario) no job was lost, stuck or completed twice.
- At 100 rps for 10 minutes the API answered every request: 0 errors, create p99 511 ms.
- At 1 000 rps, ten times the load, it got slow, but it lost nothing.

The weak spot is delay, not loss. During a two-minute disk stall the queue grew to 3 838 jobs, and jobs started up to a minute late. For training jobs that run for minutes or hours I accept that. I would not accept a lost or doubled job, and I saw none.

Conditions, in this order:

1. **Alert on queue depth and on the age of the oldest queued job.** The backlog was invisible in API latency.
2. **Run 8 workers, not 4.** They are cheap, and 4 left too little headroom during the stall.
3. **Before real training code runs here:** take the work off the worker's event loop (otherwise the heartbeat starves), and make every external effect idempotent on `job_id`.

Not covered by this verdict: Postgres itself failing, and any hardware other than one laptop.

## The design in five lines

- Every state change is **one SQL statement with its condition in `WHERE`**. No code reads a row, decides, and writes it back.
- **Claim:** `FOR UPDATE SKIP LOCKED` picks a job and sets `attempt = attempt + 1` under the row lock.
- **Dead worker:** a 5 s lease, renewed every second. The next claim takes over a job whose lease expired.
- **Safety:** a result is accepted only `WHERE attempt = mine AND state = 'running'`, and the effect row is inserted in that same statement. A worker that comes back late holds an old `attempt`, so its write matches 0 rows.
- **Guarantee:** the work runs at-least-once (at most 5 times); the effect stored in this database is exactly-once; effects in external systems are at-least-once.

One page with the reasoning: [docs/DESIGN.md](docs/DESIGN.md).

## 1. The chaos test

The brief's scenario: 10 000 jobs, 8 workers, `kill -9` of a random worker every 1.5–2.5 s, with repeated POSTs and cancels running in parallel. Seven runs with seven seeds:

| Run | Kills | Jobs orphaned by a kill | …taken over | Stale writes rejected | Jobs with a wrong number of effects | Lost | Stuck | Result |
|---|---|---|---|---|---|---|---|---|
| C1 | 135 | 76 | 76 | 172 | 0 | 0 | 0 | PASS |
| C2 | 130 | 70 | 70 | 176 | 0 | 0 | 0 | PASS |
| C3 | 130 | 76 | 76 | 133 | 0 | 0 | 0 | PASS |
| C4 | 129 | 87 | 87 | 154 | 0 | 0 | 0 | PASS |
| C5 | 135 | 75 | 75 | 163 | 0 | 0 | 0 | PASS |
| C6 (restart fast path) | 131 | 71 | 71 | 152 | 0 | 0 | 0 | PASS |
| C7 (final code) | 131 | 87 | 87 | 147 | 0 | 0 | 0 | PASS |
| **Total, 70 000 jobs** | **921** | **542** | **542** | **1 097** | **0** | **0** | **0** | |

What else happened in these runs:

- 31 774 repeated POSTs, including 6 977 bursts of 3–5 identical concurrent requests. Always one job per key.
- 1 873 running jobs were cancelled. 560 cancels lost the race to a finishing job and got `409` with the true final state.
- A job's *work* did run more than once: in C1, 65 jobs ran twice and 3 ran three times. Its *effect* never did.
- A killed worker's job was picked up after 4.9 s (the lease), or 0.8 s with the restart fast path (C6).
- One job in 70 000 ended `failed` because all 5 of its attempts were killed. At this kill rate that is expected for long jobs, and the harness matched each lost attempt to a recorded kill.

**Harder runs.** X1–X3 add frozen workers (`SIGSTOP` past the lease), leases revoked at random, dropped DB connections and `kill -9` of the API, and tell workers to ignore a lost lease so that every zombie tries to write.

| Run | Kills / freezes / revokes / conn drops / API kills | Takeovers | Zombie writes after a takeover, all rejected | Result |
|---|---|---|---|---|
| X1 | 135 / 25 / 197 / 87 / 9 | 238 | 112 | PASS |
| X2 | 131 / 23 / 194 / 88 / 8 | 232 | 121 | PASS |
| X3 | 132 / 24 / 219 / 88 / 9 | 243 | 124 | PASS |

ISO1 and ISO2 repeat the canonical run at REPEATABLE READ and SERIALIZABLE: both PASS.

**What is checked after every run**, from the database audit trail and the client's own journal:

| | Invariant |
|---|---|
| I1 | No job lost, none extra: keys in the DB = keys sent |
| I2 | One job per idempotency key, at most one `201` |
| I3 | A succeeded job has exactly one effect; any other job has none |
| I4 | Nothing left queued or running |
| I5 | Every job's history is a chain of legal transitions |
| I6 | The API never lied: an acknowledged cancel stayed cancelled |
| I7 | No job was taken over while its lease was alive |
| I8 | No `(job, attempt)` was executed twice |
| I9 | A job that ran out of attempts lost each one to a recorded fault |
| I10 | Pagination returns every job exactly once |
| B1 | In runs without process faults, every job is executed exactly once |

A run passes only if the invariants hold **and** the run really exercised what it claims: a green run that never killed a worker holding a job counts as `NOT COVERED`, not `PASS`.

**Number of runs:** 25 chaos runs, all listed in [results/INDEX.md](results/INDEX.md): 7 canonical, 3 extended, 2 isolation, 1 baseline, 10 with a seeded bug (one aborted, §6; one run by hand, §8) and 2 debugging runs. Raw output of each is in `results/<run>/`.

## 2. Can the harness fail? Seeded bugs

A green harness proves nothing unless it can go red. Each run below switches on one deliberate bug; the expected failure was written to `config.json` before the run started.

| Run | Seeded bug | Expected to break | Violations found |
|---|---|---|---|
| F1 | Claim reads, then writes without a lock | I7 | I7 1 373, and 1 055 jobs ran twice. Effects stayed correct: the fence still held |
| F2 | Complete without the `attempt` fence | I3 | I3 41, I5 28, I6 37 |
| F3 | Effect written in a separate statement | I3 | I3 80 |
| F4 | Lease judged by a worker clock 30 s ahead | I7 | I7 766 |
| F5 | Create as check-then-insert, no UNIQUE | I2 | I2 522 |
| F6 | No heartbeat | B1 | B1 5: long jobs were taken over and ran twice |
| F7 | Cancel without a state check | I3 | I3 58, I5 62, I6 44 |
| F8 | F1 + F2 together | I3 | I3 1 308 (rerun; the first run aborted, §6) |

8 of 8 caught. F1 and F8 together show the two lines of defence: a broken claim alone causes double *executions*; only with the fence also gone do *effects* double.

## 3. Silent failures

| Failure | Why it cannot happen | Forced on real Postgres | Seen under chaos | Seeded bug caught |
|---|---|---|---|---|
| Double claim | `SKIP LOCKED`; `attempt + 1` under the row lock | R1: 1 000 rounds × 50 concurrent claims, exactly one winner each time | No `(job, attempt)` executed twice in any run | F1 |
| Zombie worker | Results are fenced by `attempt` | R2: the stale write matches 0 rows | 357 zombie writes in X1–X3, all rejected | F2, F3 |
| Cancel/complete race | Two conditional `UPDATE`s; the loser re-checks its `WHERE` | R3: both orders, with a real lock wait | 1 873 cancels of running jobs, 0 cancelled jobs with an effect | F7 |
| Duplicate on retry | `UNIQUE` key, `INSERT … ON CONFLICT`, then a separate `SELECT` | R4: second request waits, returns the same job | 31 774 repeats; API killed 26 times in X1–X3, one job per key | F5 |
| Isolation level | No read-then-write anywhere, so READ COMMITTED is enough | R5: clean 0 rows at RC, a retried `40001` at RR | ISO1, ISO2 pass | F1, F8 |
| Clock skew | Only the DB clock is used, and safety does not depend on time | R6: a skewed claim steals a live lease, the correct one does not | 610 leases revoked in X1–X3, 0 double effects | F4 |

The forced races (R1–R7) are in [chaos/races.py](chaos/races.py); transcripts are `results/races-*.txt`. Before any of this, a small model checker ([model/check.py](model/check.py)) tried **every** interleaving of the protocol (860 states) and 6 broken variants; output in [results/model-check.txt](results/model-check.txt).

Not covered: a real clock jump on the DB host (the revoker only imitates it), and workers with really skewed clocks.

## 4. What the harness does not catch

- **Postgres failing.** No kill of Postgres, no failover. Losing committed transactions in a failover could bring a finished job back.
- **External effects.** My effect is a row in the same database. A real job writes to S3 or calls an API; that is at-least-once and was not tested.
- **Network trouble other than a dropped connection:** slow links, half-open connections, partitions.
- **Work that blocks.** The emulated work is a sleep, so the heartbeat always gets to run. Real training code would block it.
- **Long-term effects:** table bloat and history that is never cleaned up.
- **Scale:** more than 8 workers, more than one API process.
- **Rare timing windows.** Random runs cannot show a race is absent. That is why the model check and the forced races exist.
- **Bugs in the audit trail itself.** The invariants trust a trigger.

**Was 10 000 jobs enough?** For random testing, yes: each run produced 70–87 orphaned jobs and hundreds of cancel races, and every seeded bug was caught at only 2 000 jobs. To *prove* a rare race is absent, no number of random jobs is enough, so those cases are covered by the model check and R1–R7.

## 5. Load

Standard `docker compose up` stack (1 API process, 4 workers), k6 with a constant arrival rate, mix 60% create / 30% get / 5% list / 5% cancel.

| Run | Target | Served | Errors | Create p50 / p99 | Jobs/s through workers | Job start delay p95 | Max queue |
|---|---|---|---|---|---|---|---|
| L1, 10 min | 100 rps | 100 rps | 0 | 8 ms / 511 ms | 60 | 59 s | 3 838 |
| L2, 2 min | 200 rps | 200 rps | 0 | 6 ms / 120 ms | 108 | 12.5 s | 1 428 |
| L3, 30 s | 1 000 rps | 291 rps | 0 | 5.5 s / 8.5 s | 59 | 81 s | 4 242 |

- **100 rps is fine for the API.** The start delay in L1 comes from one two-minute episode in which the disk slowed down and the database committed a third fewer transactions. I could not prove its cause.
- **The cost of a job is 4 disk commits.** So workers are only as fast as the disk: one flush took 9.9 ms one morning and 2.7 ms after a restart ([fsync.md](results/load/fsync.md)).
- **At 10×, the single API process breaks first.** It tops out near 290 rps. Requests wait seconds, but none fail and no invariant breaks. Fix: more API processes (it is stateless), then more workers.

**Where the design is weakest:** throughput is tied to disk commits; a dead worker's job waits up to 5 s; work can run up to 5 times; and attempts lost to infrastructure count against the limit.

## 6. What went wrong along the way

All of it is kept in `results/`.

- **One chaos run aborted** (the first F8): every request timed out. The likely cause was my own harness client, not the service ([notes](results/20261111-035046-F8/NOTES.md)). The rerun failed the way it should.
- **Two load generators were wrong.** The first measured its own CPU limit, not the service (p95 "11 s"). The patched one died of client-side timeouts in a 10-minute run. I replaced it with k6. The invalid runs are in `results/load/`, marked as such.
- **A conclusion was reversed.** "4 workers cannot keep up" was true only on a slow-disk morning.
- **A review after everything was green found real bugs:** `{"work_ms": null}` crashed a worker five times, and a `NaN` body returned 500. Both are fixed, and the final code was re-tested: smoke 24/24, races pass, canonical run C7 passes ([regression](results/)).

## 7. What surprised me and what still concerns me

What surprised me most is how often the measuring tool was wrong, not the system. The load generator saturated its own CPU twice and reported the queue as slow when it was the client. A "stuck API" in one chaos run was most likely the harness's own HTTP client. A disk that needed 10 ms per flush in the morning needed 2.7 ms after a restart, which reversed a conclusion I had almost accepted. And after every run was green, a review still found that one request with `"work_ms": null` crashed a worker five times. What still concerns me: everything was measured on one laptop; Postgres failing was never tested; the disk stall in L1 has no proven cause; and I did not write this code line by line, so my confidence rests on the evidence in `results/`, not on authorship.

## 8. How AI was used

An AI assistant (Claude, in Claude Code) wrote the code, the harness, the model checker and the drafts of the documents, and ran every command. I did not write the code.

What I did:

- I chose the stack and the way of working.
- I had the architecture planned with AI, before any code was written.
- I sent the first plan back to be made stronger. That is where the separate reaper was dropped and the model check, the forced races and the seeded bugs were added.
- I asked for this report to be rewritten: shorter, and with enough data to check it against the raw files.

What I did not do: I have not read all the code line by line. The verdict is my decision.

What I checked with my own hands: one seeded-bug run ([results/20261111-194743-my-check](results/20261111-194743-my-check/)). 2 000 jobs, 8 workers, with the `attempt` fence removed from `complete`. The harness failed, as it must: 39 cancels that the API had confirmed ended `succeeded` (I6), and 29 jobs had an illegal `cancelled → succeeded` history (I5). No job was lost or stuck (I1 and I4 held).

How I guard against trusting it blindly: the harness had to prove it can fail (§2), every number comes from a raw file (`python chaos/numbers.py`), and failed runs are kept.

Where the AI's first answer was wrong:

| First answer | What showed it was wrong | Now |
|---|---|---|
| A separate reaper process | Reviewing the plan | Takeover happens inside the claim |
| `docker kill` restarts the container | A manual test: it does not | The harness kills processes directly |
| One big HTTP connection pool in the harness | The load test: client at 100% CPU, API idle | k6 for load; small pools in the harness |
| "4 workers cannot keep up at 100 rps" | Re-measuring the disk after a restart | §5 |
| A 300-job test run looked green | Its coverage counters: heartbeats were never exercised | `NOT COVERED` verdict |
| The model's liveness check | Review: a cancel made it impossible to fail | Checked without cancel; a variant proves it can fail |
| `int(payload["work_ms"])` with no guard | Review: a `null` crashed a worker five times | Invalid jobs fail on the first attempt |
| A "one page" design note of 1 256 words that mentioned an experiment never run | Review | One page; the original kept and corrected |

## Check it yourself

```sh
python chaos/numbers.py                                             # every table above, from the raw files
docker compose up -d
docker compose run --rm --no-deps api python -m chaos.smoke         # 24 checks of the API
docker compose --profile chaos run --rm chaos canonical --seed 1    # one run of the brief's scenario, ~5 min
docker compose --profile chaos run --rm chaos baseline --fault NO_FENCE   # a seeded bug: must FAIL
docker compose exec postgres psql -U soup -d jobs \
  -c "SELECT job_id, count(*) FROM effects GROUP BY 1 HAVING count(*) > 1"  # no rows = no duplicate effects
```
