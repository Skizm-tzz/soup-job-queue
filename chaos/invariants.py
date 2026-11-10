"""Post-run checks. Everything is read back from the database (the audit trail in
job_events, effects, executions, diag_events) and the client journal; nothing is
taken from what workers say about themselves.

Invariants (a violation is a correctness failure):
  I1  nothing lost, nothing extra: DB keys == sent keys, every key acknowledged
  I2  idempotency: one row per key, every 2xx for a key names the same id, <= one 201
  I3  effects: succeeded -> exactly 1 (of the final attempt, by its owner), else 0
  I4  nothing stuck: no queued/running job after the drain
  I5  legal history: a chain of allowed transitions; the row agrees with it
  I6  the API did not lie: acked cancel -> cancelled; 409/GET-seen terminal state is final
  I7  leases respected: every takeover happened after the previous lease expired
  I8  execution witness: (job, attempt) executed at most once, by the attempt's owner;
      a succeeded job executed its final attempt
  I9  plausible outcome; every exhausted job's lost attempts are explained by a fault
  I10 pagination: a full GET /jobs walk returns every id once, ascending
  B1  (only runs without process faults) exactly one execution, no takeovers
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta

from psycopg.rows import dict_row

INVARIANTS = ("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", "B1")
TERMINAL = {"succeeded", "failed", "cancelled"}
LEGAL = {
    ("queued", "running"): 1,
    ("running", "running"): 1,  # takeover of an expired lease
    ("queued", "cancelled"): 0,
    ("running", "cancelled"): 0,
    ("running", "succeeded"): 0,
    ("running", "failed"): 0,
}
# A fault is recorded in diag_events right after it is applied, so its DB timestamp
# can trail the real moment by a little. Only used to *explain* lost attempts.
SLACK = timedelta(seconds=0.5)


async def _rows(conn, sql: str, params=None) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


def _slot(owner: str | None) -> str | None:
    return owner.split(":", 1)[0] if owner else None


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3)


class Checker:
    def __init__(self, specs, journal, walk_all, walk_succeeded, has_process_faults: bool, lease_ttl_s: float):
        self.specs = {s.key: s for s in specs}
        self.journal = journal
        self.walk_all = walk_all
        self.walk_succeeded = walk_succeeded
        self.has_process_faults = has_process_faults
        self.ttl = lease_ttl_s
        self.violations: dict[str, list[dict]] = {k: [] for k in INVARIANTS}
        self.cov: dict = {}

    def v(self, inv: str, msg: str, job: int | None = None, key: str | None = None) -> None:
        self.violations[inv].append({"job": job, "key": key, "msg": msg})

    async def load(self, conn, key_prefix: str | None = None) -> None:
        """key_prefix: check only the jobs of one run in a shared database (the load test)."""
        if key_prefix is None:
            self.jobs = await _rows(conn, "SELECT * FROM jobs ORDER BY id")
            where, p = "", None
        else:
            self.jobs = await _rows(conn, "SELECT * FROM jobs WHERE idempotency_key LIKE %s ORDER BY id", (key_prefix + "%",))
            where, p = " WHERE job_id = ANY(%s)", ([j["id"] for j in self.jobs],)
        self.events = defaultdict(list)
        for e in await _rows(conn, f"SELECT * FROM job_events{where} ORDER BY job_id, seq", p):
            self.events[e["job_id"]].append(e)
        self.effects = defaultdict(list)
        for e in await _rows(conn, f"SELECT * FROM effects{where}", p):
            self.effects[e["job_id"]].append(e)
        self.execs = defaultdict(list)
        for e in await _rows(conn, f"SELECT * FROM executions{where}", p):
            self.execs[e["job_id"]].append(e)
        self.diags = await _rows(conn, "SELECT * FROM diag_events ORDER BY seq")

    # ---------------------------------------------------------------- attempts

    def _attempts(self, job_id: int) -> list[dict]:
        """One record per claim: owner, claim time, how and when the attempt ended."""
        evs = self.events[job_id]
        out = []
        for k, e in enumerate(evs):
            if e["old_state"] in ("queued", "running") and e["new_state"] == "running":
                nxt = evs[k + 1] if k + 1 < len(evs) else None
                end_kind = None
                if nxt is not None:
                    end_kind = "takeover" if nxt["new_state"] == "running" else nxt["new_state"]
                out.append({
                    "job": job_id, "attempt": e["new_attempt"], "owner": e["new_owner"], "claim_at": e["at"],
                    "takeover": e["old_state"] == "running", "prev_lease": e["old_lease_expires_at"],
                    "end_at": nxt["at"] if nxt else None, "end_kind": end_kind,
                })
        return out

    def _explain(self, a: dict) -> dict | None:
        """The recorded fault that made this attempt lose its lease, if any."""
        slot = _slot(a["owner"])
        for d in self._faults_by_slot.get(slot, ()):
            if d["at"] > a["claim_at"] and a["end_at"] is not None and d["at"] - SLACK < a["end_at"]:
                if d["kind"] in ("kill", "stop"):
                    return d
        for d in self._conn_kills.get(a["owner"], ()):
            if d["at"] > a["claim_at"] and d["at"] - SLACK < a["end_at"]:
                return d
        for d in self._revokes.get((a["job"], a["attempt"]), ()):
            return d
        return None

    # ------------------------------------------------------------------ checks

    def run(self) -> None:
        diag_kinds = defaultdict(list)
        for d in self.diags:
            diag_kinds[d["kind"]].append(d)
        self._faults_by_slot = defaultdict(list)
        for d in diag_kinds["kill"] + diag_kinds["stop"]:
            self._faults_by_slot[d["detail"]["slot"]].append(d)
        self._conn_kills = defaultdict(list)
        for d in diag_kinds["conn_kill"]:
            app = (d["detail"] or {}).get("app", "")
            if app.startswith("worker "):
                self._conn_kills[app[len("worker "):]].append(d)
        self._revokes = defaultdict(list)
        for d in diag_kinds["revoke"]:
            self._revokes[(d["job_id"], d["attempt"])].append(d)

        by_key = defaultdict(list)
        for j in self.jobs:
            by_key[j["idempotency_key"]].append(j)
        jobs_by_id = {j["id"]: j for j in self.jobs}

        self._check_keys(by_key)
        all_attempts = []
        for j in self.jobs:
            atts = self._attempts(j["id"])
            all_attempts += atts
            self._check_job(j, atts)
        self._check_api(jobs_by_id)
        self._check_walk()
        self._coverage(all_attempts, diag_kinds, jobs_by_id)

    def _check_keys(self, by_key) -> None:
        sent = set(self.specs)
        in_db = set(by_key)
        for k in sorted(sent - in_db):
            self.v("I1", "key was sent but no job exists", key=k)
        for k in sorted(in_db - sent):
            self.v("I1", "job exists for a key that was never sent", key=k)
        creates = defaultdict(list)
        for e in self.journal:
            if e["op"] == "create" and "status" in e:
                creates[e["key"]].append(e)
        for k in sorted(sent):
            rs = creates.get(k, [])
            if not any(r["status"] in (200, 201, 409) for r in rs):
                self.v("I1", "key never acknowledged by the API", key=k)
            rows = by_key.get(k, [])
            if len(rows) > 1:
                self.v("I2", f"{len(rows)} jobs for one key: ids {[r['id'] for r in rows]}", key=k)
            ok_ids = {r["job"] for r in rs if r["status"] in (200, 201)}
            if len(ok_ids) > 1:
                self.v("I2", f"2xx responses named different ids {sorted(ok_ids)}", key=k)
            n201 = sum(1 for r in rs if r["status"] == 201)
            if n201 > 1:
                self.v("I2", f"{n201} responses with 201 for one key", key=k)
            for r in rs:
                if r["status"] == 409 and not r["diff"]:
                    self.v("I2", "409 for a repeat with the same body", key=k)
                if r["diff"] and r["status"] != 409:
                    self.v("I2", f"different body got {r['status']}, expected 409", key=k)
                if r["status"] == 409 and rows and r["job"] not in {x["id"] for x in rows}:
                    self.v("I2", "409 named a job that does not hold this key", key=k)

    def _check_job(self, j: dict, atts: list[dict]) -> None:
        jid, state = j["id"], j["state"]
        evs = self.events[jid]
        terminal = state in TERMINAL

        # I4
        if not terminal:
            self.v("I4", f"still {state} after the drain (attempt {j['attempt']})", jid)

        # I5: chain of legal transitions, and the row agrees with it
        if not evs:
            self.v("I5", "no history", jid)
        else:
            e0 = evs[0]
            if e0["old_state"] is not None or e0["new_state"] != "queued" or e0["new_attempt"] != 0:
                self.v("I5", f"history does not start with a fresh queued job: {e0['new_state']}/{e0['new_attempt']}", jid)
            for prev, e in zip(evs, evs[1:]):
                if (e["old_state"], e["old_attempt"]) != (prev["new_state"], prev["new_attempt"]):
                    self.v("I5", f"broken chain at seq {e['seq']}: {prev['new_state']}/{prev['new_attempt']} then from {e['old_state']}/{e['old_attempt']}", jid)
                step = (e["old_state"], e["new_state"])
                delta = e["new_attempt"] - (e["old_attempt"] or 0)
                if LEGAL.get(step) != delta:
                    self.v("I5", f"illegal transition {step[0]}->{step[1]} attempt {e['old_attempt']}->{e['new_attempt']} at seq {e['seq']}", jid)
            last = evs[-1]
            if (last["new_state"], last["new_attempt"]) != (state, j["attempt"]):
                self.v("I5", f"row {state}/{j['attempt']} disagrees with history {last['new_state']}/{last['new_attempt']}", jid)
        if terminal != (j["finished_at"] is not None):
            self.v("I5", f"state {state} but finished_at={j['finished_at']}", jid)
        if terminal and j["lease_owner"] is not None:
            self.v("I5", f"terminal job still has lease_owner {j['lease_owner']}", jid)
        if (state == "failed") != (j["error"] is not None):
            self.v("I5", f"state {state} with error={j['error']!r}", jid)
        if j["attempt"] > j["max_attempts"] + 1:
            self.v("I5", f"attempt {j['attempt']} > max_attempts+1", jid)

        # I3: effect iff succeeded, of the final attempt, written by that attempt's owner
        effs = self.effects.get(jid, [])
        owner = {a["attempt"]: a["owner"] for a in atts}
        if state == "succeeded":
            if len(effs) != 1:
                self.v("I3", f"succeeded with {len(effs)} effects", jid)
            for e in effs:
                if e["attempt"] != j["attempt"]:
                    self.v("I3", f"effect of attempt {e['attempt']}, job finished on attempt {j['attempt']}", jid)
                elif owner.get(e["attempt"]) != e["worker_id"]:
                    self.v("I3", f"effect by {e['worker_id']}, attempt owned by {owner.get(e['attempt'])}", jid)
        elif effs:
            self.v("I3", f"{state} job has {len(effs)} effect(s) (attempts {[e['attempt'] for e in effs]})", jid)

        # I7: a takeover only after the previous lease expired (both times on the DB clock)
        for a in atts:
            if a["takeover"] and (a["prev_lease"] is None or a["prev_lease"] > a["claim_at"]):
                ahead = (a["prev_lease"] - a["claim_at"]).total_seconds() if a["prev_lease"] else None
                self.v("I7", f"attempt {a['attempt']} took over a live lease ({ahead}s before expiry)", jid)

        # I8: execution witness
        exs = self.execs.get(jid, [])
        cnt = Counter(e["attempt"] for e in exs)
        for att, n in cnt.items():
            if n > 1:
                self.v("I8", f"attempt {att} executed {n} times", jid)
        for e in exs:
            if owner.get(e["attempt"]) != e["worker_id"]:
                self.v("I8", f"execution of attempt {e['attempt']} by {e['worker_id']}, owner {owner.get(e['attempt'])}", jid)
        if state == "succeeded" and j["attempt"] not in cnt:
            self.v("I8", f"succeeded on attempt {j['attempt']} without executing it", jid)

        # I9: plausible outcome
        outcome = (j["payload"] or {}).get("outcome", "ok")
        exhausted = state == "failed" and j["error"] == "attempts exhausted"
        if outcome == "ok" and state == "failed" and not exhausted:
            self.v("I9", f"ok job failed: {j['error']!r}", jid)
        if outcome == "fail" and state == "succeeded":
            self.v("I9", "job that reports failure succeeded", jid)
        if exhausted:
            if not self.has_process_faults:
                self.v("I9", "attempts exhausted in a run without process faults", jid)
            for a in atts:
                if a["end_kind"] == "takeover" and self._explain(a) is None:
                    self.v("I9", f"exhausted, and lost attempt {a['attempt']} ({a['owner']}) has no recorded cause", jid)

        # B1: without process faults every job runs exactly once
        if not self.has_process_faults:
            if j["attempt"] > 1 or len(exs) > 1:
                self.v("B1", f"{len(exs)} executions, attempt {j['attempt']} without any process fault", jid)

    def _check_api(self, jobs_by_id) -> None:
        for e in self.journal:
            if "status" not in e or e.get("job") is None:
                continue
            j = jobs_by_id.get(e["job"])
            if j is None:
                continue
            final = j["state"]
            if e["op"] == "cancel":
                if e["status"] == 200 and final != "cancelled":
                    self.v("I6", f"cancel acknowledged (200) but job ended {final}", j["id"])
                if e["status"] == 409 and e["state"] != final:
                    self.v("I6", f"cancel refused as {e['state']} but job ended {final}", j["id"])
            elif e["op"] == "get" and e["state"] in TERMINAL and e["state"] != final:
                self.v("I6", f"GET showed terminal {e['state']}, job ended {final}", j["id"])
            elif e["op"] == "create" and e.get("state") in TERMINAL and e["state"] != final:
                self.v("I6", f"create response showed terminal {e['state']}, job ended {final}", j["id"])

    def _check_walk(self) -> None:
        if self.walk_all is None:  # not checked in this run (shared database)
            return
        db_ids = [j["id"] for j in self.jobs]
        w = self.walk_all
        if w != sorted(w) or len(w) != len(set(w)):
            self.v("I10", f"walk not strictly ascending/unique ({len(w)} ids, {len(set(w))} distinct)")
        if set(w) != set(db_ids):
            self.v("I10", f"walk saw {len(set(w))} ids, DB has {len(db_ids)}; missing {sorted(set(db_ids) - set(w))[:10]}")
        succ = [j["id"] for j in self.jobs if j["state"] == "succeeded"]
        if self.walk_succeeded != succ:
            self.v("I10", f"state=succeeded walk returned {len(self.walk_succeeded)} ids, DB has {len(succ)}")

    # ---------------------------------------------------------------- coverage

    def _coverage(self, atts: list[dict], diag_kinds, jobs_by_id) -> None:
        c = self.cov
        kills = diag_kinds["kill"]
        c["kills"] = len(kills)
        c["stalls"] = len(diag_kinds["stop"])
        c["revokes"] = len(diag_kinds["revoke"])
        c["conn_kills"] = len(diag_kinds["conn_kill"])
        c["api_kills"] = len(diag_kinds["api_kill"])

        takeovers = [a for a in atts if a["end_kind"] == "takeover"]
        c["takeovers"] = len(takeovers)
        recovery, causes, orphaning = [], Counter(), set()
        unexplained = []
        for a in takeovers:
            d = self._explain(a)
            if d is None:
                unexplained.append(a)
                continue
            causes[d["kind"]] += 1
            if d["kind"] == "kill":
                orphaning.add(d["seq"])
                recovery.append((a["end_at"] - d["at"]).total_seconds())
        c["takeovers_by_cause"] = dict(causes)
        c["takeovers_unexplained"] = len(unexplained)
        c["takeovers_unexplained_sample"] = [
            {"job": a["job"], "attempt": a["attempt"], "owner": a["owner"],
             "held_s": round((a["end_at"] - a["claim_at"]).total_seconds(), 3)} for a in unexplained[:10]
        ]
        c["kills_orphaning_a_job"] = len(orphaning)
        c["recovery_after_kill_s"] = {"n": len(recovery), "p50": _pct(recovery, 0.5), "p95": _pct(recovery, 0.95),
                                      "max": round(max(recovery), 3) if recovery else None}

        stale = diag_kinds["stale_complete_rejected"]
        reasons = Counter()
        att_index = {(a["job"], a["attempt"]): a for a in atts}
        for d in stale:
            a = att_index.get((d["job_id"], d["attempt"]))
            reasons[a["end_kind"] if a else "unknown"] += 1
        c["stale_completes_rejected"] = len(stale)
        c["stale_completes_by_reason"] = dict(reasons)
        c["lease_lost_detected"] = len(diag_kinds["lease_lost"])
        c["attempts_exhausted"] = len(diag_kinds["attempts_exhausted"])

        cancel_from = Counter()
        for evs in self.events.values():
            for e in evs:
                if e["new_state"] == "cancelled":
                    cancel_from[e["old_state"]] += 1
        c["cancelled_from_queued"] = cancel_from["queued"]
        c["cancelled_from_running"] = cancel_from["running"]
        cancels = [e for e in self.journal if e["op"] == "cancel" and "status" in e]
        c["cancel_responses"] = dict(Counter(str(e["status"]) for e in cancels))
        wr = [e for e in cancels if e["kind"] == "when_running" and e["seen"] == "running"]
        c["cancel_sent_while_seen_running"] = len(wr)
        c["cancel_lost_race_to_finish"] = sum(1 for e in wr if e["status"] == 409)

        creates = [e for e in self.journal if e["op"] == "create" and "status" in e]
        per_key = Counter(e["key"] for e in creates)
        c["posts"] = len(creates)
        c["repeat_posts"] = sum(n - 1 for n in per_key.values())
        c["concurrent_bursts"] = sum(1 for s in self.specs.values() if s.burst > 1)
        c["burst_201_not_to_first_sent"] = sum(1 for e in creates if e["status"] == 201 and e["idx"] > 0)
        got_201 = {e["key"] for e in creates if e["status"] == 201}
        c["keys_without_any_201"] = sum(1 for k in self.specs if k not in got_201)  # first response lost, retry saw 200
        c["diff_body_409"] = sum(1 for e in creates if e["diff"] and e["status"] == 409)

        execs_per_job = Counter(len(self.execs.get(j["id"], [])) for j in self.jobs)
        c["executions_per_job"] = {str(k): v for k, v in sorted(execs_per_job.items())}
        c["max_attempt"] = max((j["attempt"] for j in self.jobs), default=0)
        c["final_states"] = dict(Counter(j["state"] for j in self.jobs))
        long_ok = [j for j in self.jobs if (j["payload"] or {}).get("work_ms", 0) > self.ttl * 1000]
        c["long_jobs"] = len(long_ok)
        c["long_jobs_succeeded_first_attempt"] = sum(1 for j in long_ok if j["state"] == "succeeded" and j["attempt"] == 1)

    def counts(self) -> dict[str, int]:
        return {k: len(v) for k, v in self.violations.items()}

    def histories(self, limit: int = 30) -> dict:
        """Full record of the first violating jobs, for violations.json."""
        jobs_by_id = {j["id"]: j for j in self.jobs}
        ids = []
        for vs in self.violations.values():
            for x in vs:
                if x["job"] is not None and x["job"] not in ids:
                    ids.append(x["job"])
        out = {}
        for jid in ids[:limit]:
            j = jobs_by_id.get(jid)
            out[str(jid)] = {
                "job": j,
                "events": self.events.get(jid, []),
                "effects": self.effects.get(jid, []),
                "executions": self.execs.get(jid, []),
                "diag": [d for d in self.diags if d["job_id"] == jid],
                "client": [e for e in self.journal if e.get("job") == jid or (j and e.get("key") == j["idempotency_key"])],
            }
        return out
