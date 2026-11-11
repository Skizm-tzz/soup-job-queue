# Design note (one page)

An HTTP API, N workers and one Postgres. The full version, written before the code, is [DESIGN-full.md](DESIGN-full.md).

**Core idea.** A dead worker cannot be told apart from a slow one, so the *work* may run more than once. The design guarantees that its *effect* is committed at most once. It rests on two rules:

- Every state transition is **one SQL statement in autocommit with its precondition in `WHERE`** (compare-and-set). No code reads a row, decides in Python and writes it back.
- **Safety never depends on time.** The lease only decides when a job may be taken over. Whether a write is accepted depends only on the job's `attempt` counter, which acts as a fencing token. All timestamps come from the database clock.

**Why Postgres, not SQLite.** The claim needs row locks, `FOR UPDATE SKIP LOCKED` and many concurrent writers in separate containers. SQLite has one database-wide write lock and no `SKIP LOCKED`.

**Claim.** `WITH c AS MATERIALIZED (SELECT id … WHERE state='queued' OR (state='running' AND lease_expires_at <= now()) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE jobs SET state='running', attempt=attempt+1, lease_owner=$w, lease_expires_at=clock_timestamp()+$ttl …`.

- Two workers can never get the same row: a locked row is skipped.
- `attempt` grows by one under the row lock, so `(id, attempt)` names exactly one grant of the job.

**Dead worker.** A working worker renews its lease every 1 s; the TTL is 5 s.

- A dead or frozen worker stops renewing. When its lease expires, the next claim takes the job over with `attempt+1`. There is no reaper process and no `running → queued` transition.
- A restarted worker immediately expires the leases its own slot still holds. A restart proves the previous process is dead, so the job is recovered in about 1 s instead of 5.

**Completion.** `WITH done AS (UPDATE jobs SET state='succeeded' WHERE id=$1 AND attempt=$2 AND state='running' RETURNING id) INSERT INTO effects SELECT … FROM done`. The effect exists if and only if the transition happened.

- A zombie, meaning a worker that froze past its lease while someone else took the job over, still holds the old `attempt`. Its write matches 0 rows.
- A late completion that nobody took over is accepted: it is still the only grant.

**Cancelling a running job.** `UPDATE … SET state='cancelled' WHERE id=$1 AND state IN ('queued','running')` takes effect at once. The worker sees it on its next heartbeat (0 rows) and stops. If cancel and complete race, one waits on the other's row lock and re-checks its `WHERE` (READ COMMITTED):

- if the cancel commits first, no effect is written;
- if the complete commits first, the cancel returns 409 with the real final state.

A cancel answered 200 is final.

**Guarantee.**

| | Guarantee | Why |
|---|---|---|
| Running the work | at-least-once | After a crash or freeze the work may start again (bounded by `max_attempts`). |
| Effect stored in this Postgres | **exactly-once** | It is committed in the same statement as `running → succeeded`, and that statement is fenced by `attempt`. |
| Effect in an external system | at-least-once | It is outside the transaction and needs a receiver idempotent on `job_id`, for example via an outbox. |

**Idempotent create.** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING`, then a *separate* `SELECT`. A single CTE+`UNION` shares one snapshot and can return no row in a race.

- Same body → 200.
- Different body → 409.

**Rejected.**

- **A reaper process.** It adds one more process and one more transition.
- **Holding a transaction open for the whole job.** A frozen worker would keep its lock indefinitely.
- **Batch claim.** Not needed at this scale.
- **`UNIQUE` on `effects`.** Left out so the harness can *count* duplicates instead of seeing constraint errors.
