-- Schema for the job queue. Idempotent: safe to apply on every start.
-- Every state transition is one statement in app/queue.py; this file only
-- defines the data and the audit trail the harness checks.

CREATE TABLE IF NOT EXISTS jobs (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  idempotency_key  text NOT NULL UNIQUE,
  payload          jsonb NOT NULL,
  payload_hash     text NOT NULL,           -- sha256 of canonical JSON: same key + different body -> 409
  state            text NOT NULL DEFAULT 'queued'
                   CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
  attempt          int  NOT NULL DEFAULT 0, -- fencing token: +1 on every claim, under the row lock
  max_attempts     int  NOT NULL DEFAULT 5,
  lease_owner      text,
  lease_expires_at timestamptz,             -- deliberately not indexed: heartbeats stay HOT updates
  created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  started_at       timestamptz,
  finished_at      timestamptz,
  error            text
) WITH (fillfactor = 80);                   -- free space on each page so heartbeat updates can be HOT

-- Only unfinished jobs: the claim scan does not grow with history.
CREATE INDEX IF NOT EXISTS jobs_active ON jobs (id) WHERE state IN ('queued','running');
-- For GET /jobs?state=... with keyset pagination.
CREATE INDEX IF NOT EXISTS jobs_state_id ON jobs (state, id);

-- The side effect of a job. No UNIQUE on purpose: a duplicate must be counted
-- by the harness, not turned into a constraint error. No foreign keys on the
-- auxiliary tables, so they never take locks on jobs.
CREATE TABLE IF NOT EXISTS effects (
  job_id     bigint NOT NULL,
  attempt    int    NOT NULL,
  worker_id  text   NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS effects_job ON effects (job_id);

-- Witness that work actually started (at-least-once execution counter).
CREATE TABLE IF NOT EXISTS executions (
  job_id     bigint NOT NULL,
  attempt    int    NOT NULL,
  worker_id  text   NOT NULL,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS executions_job ON executions (job_id);

-- Audit trail of every change of state, attempt or lease owner, written by a trigger.
CREATE TABLE IF NOT EXISTS job_events (
  seq                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id               bigint NOT NULL,
  old_state            text,
  new_state            text NOT NULL,
  old_attempt          int,
  new_attempt          int NOT NULL,
  old_lease_expires_at timestamptz,
  old_owner            text,
  new_owner            text,
  at                   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events (job_id, seq);

-- Diagnostics: what workers noticed and what the chaos harness did, on the DB clock.
CREATE TABLE IF NOT EXISTS diag_events (
  seq     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind    text NOT NULL,
  job_id  bigint,
  attempt int,
  actor   text,
  at      timestamptz NOT NULL DEFAULT clock_timestamp(),
  detail  jsonb
);

CREATE OR REPLACE FUNCTION log_job_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO job_events (job_id, new_state, new_attempt, new_owner)
    VALUES (NEW.id, NEW.state, NEW.attempt, NEW.lease_owner);
  ELSE
    INSERT INTO job_events (job_id, old_state, new_state, old_attempt, new_attempt,
                            old_lease_expires_at, old_owner, new_owner)
    VALUES (NEW.id, OLD.state, NEW.state, OLD.attempt, NEW.attempt,
            OLD.lease_expires_at, OLD.lease_owner, NEW.lease_owner);
  END IF;
  RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS jobs_log_insert ON jobs;
CREATE TRIGGER jobs_log_insert AFTER INSERT ON jobs
  FOR EACH ROW EXECUTE FUNCTION log_job_event();

-- Heartbeats only change lease_expires_at, so they never fire this trigger.
DROP TRIGGER IF EXISTS jobs_log_update ON jobs;
CREATE TRIGGER jobs_log_update AFTER UPDATE OF state, attempt, lease_owner ON jobs
  FOR EACH ROW
  WHEN (OLD.state IS DISTINCT FROM NEW.state
        OR OLD.attempt IS DISTINCT FROM NEW.attempt
        OR OLD.lease_owner IS DISTINCT FROM NEW.lease_owner)
  EXECUTE FUNCTION log_job_event();
