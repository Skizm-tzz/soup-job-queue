# Design note (full version)

> This is the design note as written before any service code (commit `995d9fb`), with one later correction marked below. It runs to about two and a half pages; the one-page version the brief asks for is [DESIGN.md](DESIGN.md), condensed from this text after the work was done.

A training-job queue: an HTTP API, N workers, one Postgres. The note covers the four questions from the brief: how a worker claims a job, how a dead worker is detected, what cancelling a running job does, and which guarantee the system gives.

## Core idea

You cannot tell a dead worker from a slow one. A worker that has been silent for 5 seconds may be dead, or it may wake up in a minute and keep going. So the design accepts that a job's *work* can run more than once and makes sure that its *effect* is committed at most once.

Two rules carry the design:

1. **Every state transition is a single SQL statement with its precondition in the `WHERE` clause** (compare-and-set), run in autocommit. The code never reads a row, decides something, and then writes it back. No client holds a lock across two round trips.
2. **Safety never depends on time.** The lease only decides *when a job may be taken over*. Whether a write is accepted depends only on the job's `attempt` counter (a fencing token). All timestamps come from the database clock.

## Storage: Postgres, not SQLite

The claim needs row locks plus `FOR UPDATE SKIP LOCKED`, and there are many concurrent writers in separate containers. SQLite has one write lock for the whole database and no `SKIP LOCKED`. Sharing its file between containers through a volume is also fragile. Postgres also gives READ COMMITTED with a re-check of the `WHERE` clause on the newest row version after a lock wait. Every transition below relies on that re-check.

## How a worker claims a job

One statement takes the next queued job, or takes over a `running` job whose lease has expired:

```sql
WITH c AS MATERIALIZED (
  SELECT id FROM jobs
  WHERE state IN ('queued','running')
    AND (state = 'queued' OR lease_expires_at <= now())
  ORDER BY id LIMIT 1
  FOR UPDATE SKIP LOCKED)
UPDATE jobs j SET state = 'running', attempt = j.attempt + 1,
       lease_owner = $worker, lease_expires_at = clock_timestamp() + $ttl
FROM c WHERE j.id = c.id
RETURNING j.id, j.attempt, ...;
```

- `SKIP LOCKED` means two workers can never receive the same row: a row another claim is locking right now is skipped, not waited on.
- `attempt` goes up by exactly one per claim, under the row lock. The pair `(id, attempt)` therefore names one specific grant of the job.
- A job that was taken over keeps its low `id`, so it goes to the front of the queue, not the back.

## Detecting a dead worker

While it works, a worker renews its lease every second, with a 5-second TTL:
`UPDATE jobs SET lease_expires_at = clock_timestamp() + ttl WHERE id = $1 AND attempt = $2 AND state = 'running'`.

- A dead or frozen worker stops renewing. Once the lease expires, the job matches the claim predicate again and the next free worker takes it with `attempt + 1`. There is no separate reaper process and no `running → queued` transition.
- A restarted worker uses a faster path: it immediately expires the leases still held by its own slot (the same container or slot id). A restart proves that the previous process in that slot is dead, so the job is recovered without waiting for the TTL.
- If the heartbeat updates 0 rows, the job was cancelled or taken over. The worker stops working on it and writes nothing.

## What cancelling a running job does

`UPDATE jobs SET state = 'cancelled' WHERE id = $1 AND state IN ('queued','running')`.

- The cancel takes effect immediately. It does not wait for the worker.
- The worker learns about it on its next heartbeat (at most 1 second later) and stops.
- If the worker reaches `complete` first, one of the two statements waits on the other's row lock and then re-checks its `WHERE` clause:
  - if the cancel commits first, `complete` matches 0 rows and no effect is written;
  - if `complete` commits first, the cancel matches 0 rows and the API returns `409` together with the job's real final state.
- A cancel that returned `200` is final. A cancelled job never gets an effect.

## Completion: effect and transition in one statement

```sql
WITH done AS (
  UPDATE jobs SET state = 'succeeded', finished_at = clock_timestamp()
  WHERE id = $1 AND attempt = $2 AND state = 'running' RETURNING id, attempt)
INSERT INTO effects (job_id, attempt, worker_id) SELECT id, attempt, $3 FROM done;
```

- The effect row exists if and only if the transition happened.
- A zombie worker, meaning one that froze past its lease, while someone else took over or cancelled the job, still holds an old `attempt`. Its `complete` matches 0 rows.
- A late `complete` whose lease expired but whose job *nobody took over* is accepted. It is still the only grant, so accepting it is safe.

## Guarantee

| What | Guarantee | Why |
|---|---|---|
| Running the work | at-least-once | Dead and slow cannot be told apart, so after a crash or a freeze the work may start again. Every start is recorded in `executions`. |
| Effect stored in Postgres | **exactly-once** | The effect is committed in the same statement as `running → succeeded`, and that statement is fenced by `attempt`. |
| Effect in an external system (file, S3, third-party API) | at-least-once | The external system is outside the transaction. It needs a receiver that is idempotent on `job_id`; the route to that is a transactional outbox. *(Correction: this note originally said the harness demonstrates this boundary with a file-effect experiment. That experiment was planned but never run; see REPORT.md §4.)* |

Liveness: a job cannot get stuck as long as at least one worker is alive. After `max_attempts` grants, the next claim marks the job `failed`.

## Idempotent create

`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`. If it returns nothing, a separate `SELECT` reads the existing row.

- The same key with the same body returns `200` and the existing job. The same key with a different body returns `409`.
- The `SELECT` must be a separate statement. A single CTE + `UNION` shares one snapshot and can return zero rows when two identical requests race.

## Isolation level

READ COMMITTED is enough, because every transition is one conditional `UPDATE`. After waiting on a row lock, Postgres re-evaluates the `WHERE` clause against the newest committed version of the row.

- Under REPEATABLE READ the same race raises a serialization error (`40001`) instead. That is safe but noisy; the code retries it, because the statement definitely did not apply.
- What READ COMMITTED would *not* protect is read-then-write logic in application code. The code contains none, and the harness includes a seeded bug that proves the difference.

## Rejected alternatives

- **Separate reaper process.** It adds one more moving part and one more transition. Taking over expired leases inside `claim` does the same job in the same statement.
- **A transaction held open for the whole job.** The lock would die together with the worker, which is attractive. But a frozen worker would hold the lock indefinitely, and there would be one idle-in-transaction connection per running job.
- **Batch claim.** Claiming several jobs at a time is not needed at this scale, and it makes the reasoning about leases harder.
- **`UNIQUE` on `effects`.** In production this is a good second line of defence. Here it is left out on purpose so that the harness can *count* duplicate effects instead of turning them into a constraint error.
