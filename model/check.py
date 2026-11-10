"""Exhaustive interleaving check of the queue protocol.

This is a small breadth-first search over every interleaving of an abstract
model of the protocol: one job, W workers, an adversarial clock that can expire
the lease at any moment, crashes, and a client that can cancel at any moment.
Each action corresponds to exactly one SQL statement in app/queue.py
(see ACTION_TO_SQL below), and each action is atomic, because each statement is.

The correct protocol must satisfy every safety property in every reachable
state, and must always be able to reach a terminal state. Every buggy variant
must be caught, and the checker prints the shortest trace that breaks it.

Usage:  python model/check.py            (exit code 0 if everything is as expected)
"""

from __future__ import annotations

import sys
from collections import deque
from dataclasses import dataclass, replace
from typing import Iterator

# Which statement in app/queue.py each model action stands for.
ACTION_TO_SQL = {
    "claim": "queue.claim()      -- UPDATE ... FOR UPDATE SKIP LOCKED, attempt+1",
    "expire": "(time passes)      -- lease_expires_at <= now()",
    "heartbeat": "queue.heartbeat()  -- UPDATE ... WHERE id, attempt, state='running'",
    "complete": "queue.complete()   -- UPDATE ... RETURNING + INSERT effects, one statement",
    "fail": "queue.fail()       -- UPDATE ... WHERE id, attempt, state='running'",
    "crash": "(kill -9)          -- worker forgets everything",
    "cancel": "queue.cancel()     -- UPDATE ... WHERE state IN ('queued','running')",
}

QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED = "Q", "R", "S", "F", "C"
TERMINAL = {SUCCEEDED, FAILED, CANCELLED}

VARIANTS = {
    "correct": "the protocol as designed",
    "no_fence": "complete() without attempt/state in WHERE",
    "cancel_blind": "cancel() without a state condition",
    "split_effect": "effect inserted by a separate statement before the transition",
    "claim_read_then_write": "claim() as unlocked SELECT, then UPDATE without a condition",
    "worker_clock": "claim() judges lease expiry by a skewed worker clock",
}


@dataclass(frozen=True)
class Worker:
    held: int | None = None  # the attempt this worker believes it owns
    seen: int | None = None  # claim_read_then_write: attempt observed by the SELECT
    effect_written: bool = False  # split_effect: first half of complete done


@dataclass(frozen=True)
class S:
    state: str = QUEUED
    attempt: int = 0
    lease_live: bool = False
    effects: int = 0
    cancel_acked: bool = False
    workers: tuple[Worker, ...] = ()


def _set_worker(s: S, i: int, w: Worker) -> S:
    ws = list(s.workers)
    ws[i] = w
    return replace(s, workers=tuple(ws))


def successors(s: S, variant: str, max_attempt: int) -> Iterator[tuple[str, S]]:
    claimable = s.state == QUEUED or (s.state == RUNNING and not s.lease_live)
    if variant == "worker_clock":
        # The worker's clock runs ahead, so it thinks a live lease has expired.
        claimable = s.state in (QUEUED, RUNNING)

    for i, w in enumerate(s.workers):
        busy = w.held is not None or w.seen is not None or w.effect_written

        # claim
        if variant == "claim_read_then_write":
            if not busy and claimable and s.attempt < max_attempt:
                yield f"read({i})", _set_worker(s, i, replace(w, seen=s.attempt))
            if w.seen is not None and w.held is None:
                # UPDATE ... WHERE id = $1 with no condition: always applies,
                # even to a job that became terminal in the meantime.
                if s.attempt < max_attempt:
                    a = s.attempt + 1
                    ns = replace(s, state=RUNNING, attempt=a, lease_live=True)
                    yield f"write_claim({i})", _set_worker(ns, i, Worker(held=a))
        elif not busy and claimable and s.attempt < max_attempt:
            a = s.attempt + 1
            ns = replace(s, state=RUNNING, attempt=a, lease_live=True)
            yield f"claim({i})", _set_worker(ns, i, Worker(held=a))

        if w.held is not None:
            mine = s.state == RUNNING and s.attempt == w.held

            # heartbeat: 0 rows means the lease is lost. The worker may ignore
            # that (CHAOS_IGNORE_LEASE_LOSS), so it keeps `held` either way; the
            # model then covers zombies that try to complete anyway.
            if mine:
                yield f"heartbeat({i})", replace(s, lease_live=True)

            # complete
            if variant == "split_effect":
                if not w.effect_written:
                    yield f"insert_effect({i})", _set_worker(
                        replace(s, effects=s.effects + 1), i, replace(w, effect_written=True)
                    )
                else:
                    ns = replace(s, state=SUCCEEDED, lease_live=False) if mine else s
                    yield f"update_succeeded({i})", _set_worker(ns, i, Worker())
            elif variant == "no_fence":
                ns = replace(s, state=SUCCEEDED, lease_live=False, effects=s.effects + 1)
                yield f"complete({i})", _set_worker(ns, i, Worker())
            else:
                ns = replace(s, state=SUCCEEDED, lease_live=False, effects=s.effects + 1) if mine else s
                yield f"complete({i})", _set_worker(ns, i, Worker())

            # fail (the job reported failure, or attempts were exhausted)
            if not w.effect_written:
                ns = replace(s, state=FAILED, lease_live=False) if mine else s
                yield f"fail({i})", _set_worker(ns, i, Worker())

        if busy:
            yield f"crash({i})", _set_worker(s, i, Worker())

    if s.state == RUNNING and s.lease_live:
        yield "expire", replace(s, lease_live=False)

    if variant == "cancel_blind":
        yield "cancel", replace(s, state=CANCELLED, lease_live=False, cancel_acked=True)
    elif s.state in (QUEUED, RUNNING):
        yield "cancel", replace(s, state=CANCELLED, lease_live=False, cancel_acked=True)


def state_violations(s: S) -> list[str]:
    v = []
    mid_operation = any(w.effect_written for w in s.workers)
    if s.effects > 1:
        v.append("more than one effect")
    elif not mid_operation and (s.effects == 1) != (s.state == SUCCEEDED):
        v.append(f"'effect exists iff succeeded' broken (state={s.state}, effects={s.effects})")
    if s.cancel_acked and s.state != CANCELLED:
        v.append("acknowledged cancel did not stick")
    if s.state == RUNNING and s.lease_live:
        owners = [w for w in s.workers if w.held == s.attempt]
        if len(owners) > 1:
            v.append("two workers hold the current attempt")
    return v


def transition_violations(a: S, b: S) -> list[str]:
    v = []
    if a.state in TERMINAL and b.state != a.state:
        v.append(f"left terminal state {a.state} -> {b.state}")
    if a.state == RUNNING and b.attempt == a.attempt + 1 and a.lease_live:
        v.append("job taken over while its lease was live")
    return v


@dataclass
class Result:
    states: int
    violation: str | None
    trace: list[str]


def check(variant: str, workers: int, max_attempt: int) -> Result:
    start = S(workers=tuple(Worker() for _ in range(workers)))
    parent: dict[S, tuple[S | None, str]] = {start: (None, "")}
    edges: dict[S, list[S]] = {}
    queue = deque([start])

    def trace_to(s: S) -> list[str]:
        out = []
        while True:
            p, lab = parent[s]
            if p is None:
                break
            out.append(lab)
            s = p
        return list(reversed(out))

    while queue:
        s = queue.popleft()
        if vs := state_violations(s):
            return Result(len(parent), vs[0], trace_to(s))
        edges[s] = []
        for label, ns in successors(s, variant, max_attempt):
            if vt := transition_violations(s, ns):
                return Result(len(parent), vt[0], trace_to(s) + [label])
            edges[s].append(ns)
            if ns not in parent:
                parent[ns] = (s, label)
                queue.append(ns)

    # Liveness: from every reachable state some terminal state must be reachable.
    # Attempts are bounded in the model; the real system enforces the bound with
    # max_attempts (the claim that exceeds it is followed by fail()).
    rev: dict[S, list[S]] = {s: [] for s in edges}
    for s, outs in edges.items():
        for ns in outs:
            rev[ns].append(s)
    frontier = deque(s for s in edges if s.state in TERMINAL)
    can_finish = set(frontier)
    while frontier:
        s = frontier.popleft()
        for p in rev[s]:
            if p not in can_finish:
                can_finish.add(p)
                frontier.append(p)
    stuck = [s for s in edges if s not in can_finish]
    if stuck:
        return Result(len(parent), f"{len(stuck)} states cannot reach a terminal state", trace_to(stuck[0]))
    return Result(len(parent), None, [])


def main() -> int:
    ok = True
    print("Model check of the queue protocol (exhaustive BFS over all interleavings)")
    print()
    print("Model action -> statement in app/queue.py:")
    for k, v in ACTION_TO_SQL.items():
        print(f"  {k:<10} {v}")
    print()
    configs = [(2, 3), (3, 3), (3, 4)]
    for variant, desc in VARIANTS.items():
        expect_pass = variant == "correct"
        print(f"{variant}: {desc}")
        for workers, max_attempt in configs:
            r = check(variant, workers, max_attempt)
            passed = r.violation is None
            as_expected = passed == expect_pass
            ok &= as_expected
            print(
                f"  [{'ok ' if as_expected else 'BAD'}] workers={workers} attempts<={max_attempt} "
                f"states={r.states:<6} {'PASS' if passed else 'VIOLATION'}"
                f" (expected {'PASS' if expect_pass else 'VIOLATION'})"
            )
            if r.violation:
                print(f"        {r.violation}")
                print(f"        shortest trace: {' -> '.join(r.trace)}")
        print()
    print("ALL AS EXPECTED" if ok else "UNEXPECTED RESULT")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
