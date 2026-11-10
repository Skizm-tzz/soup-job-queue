"""Client side of a chaos run: the job plan, an HTTP client that retries unknown
outcomes with the same request, and the journal the invariants are checked against.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx

TERMINAL = frozenset({"succeeded", "failed", "cancelled"})


@dataclass
class Spec:
    i: int
    key: str
    t: float  # send time, seconds after load start
    payload: dict
    burst: int = 1  # concurrent identical POSTs at the first send
    delayed: list[float] = field(default_factory=list)  # later identical POSTs, seconds after the first
    diff_body: bool = False  # one POST with the same key and a different body: expect 409
    cancel: str | None = None  # immediate | when_running | after_terminal
    cancel_delay: float = 0.0  # when_running: wait this long after seeing running
    cancel_twice: bool = False


def _work_ms(rng: random.Random, profile: str) -> int:
    if profile == "dense":
        return rng.randint(0, 5)
    r = rng.random()
    if r < 0.005:
        return rng.randint(6000, 9000)  # longer than the lease TTL: only these exercise heartbeats
    if r < 0.055:
        return rng.randint(300, 1500)
    return rng.randint(10, 120)


def make_plan(n: int, rate: float, seed: int, prefix: str, profile: str = "normal") -> list[Spec]:
    """Deterministic from the seed. Arrivals are Poisson with the given mean rate."""
    rng = random.Random(seed)
    t = 0.0
    specs: list[Spec] = []
    for i in range(n):
        t += rng.expovariate(rate)
        ms = _work_ms(rng, profile)
        s = Spec(i=i, key=f"{prefix}-{i}", t=t, payload={"work_ms": ms, "outcome": "fail" if rng.random() < 0.01 else "ok"})
        r = rng.random()
        if r < 0.10:
            s.burst = rng.randint(3, 5)
        elif r < 0.20:
            s.delayed = sorted(rng.uniform(1, 20) for _ in range(rng.randint(1, 2)))
        c = rng.random()
        if c < 0.03:
            s.cancel = "immediate"
        elif c < 0.05:
            s.cancel = "after_terminal"
        elif c < 0.05 + (0.40 if ms >= 300 else 0.01):
            s.cancel = "when_running"
            s.cancel_delay = rng.uniform(0, 0.3)
        s.cancel_twice = s.cancel is not None and rng.random() < 0.3
        specs.append(s)
    for s in rng.sample(specs, max(5, n // 200)):
        s.diff_body = True
    return specs


class GaveUp(Exception):
    pass


class Client:
    """Every timeout, transport error or 5xx is an unknown outcome: retry the same request."""

    def __init__(self, base_url: str, t0: float, retry_deadline_s: float = 180.0):
        self.http = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
        )
        self.t0 = t0
        self.retry_deadline_s = retry_deadline_s
        self.journal: list[dict] = []
        self.errors: Counter = Counter()
        self.gets_nonterminal = 0

    def rec(self, **e) -> None:
        e["t"] = round(time.monotonic() - self.t0, 3)
        self.journal.append(e)

    async def call(self, op: str, method: str, url: str, *, key: str | None = None, json: dict | None = None,
                   params: dict | None = None) -> httpx.Response:
        headers = {"Idempotency-Key": key} if key is not None else None
        start = time.monotonic()
        backoff = 0.05
        tries = 0
        while True:
            tries += 1
            try:
                r = await self.http.request(method, url, json=json, headers=headers, params=params)
                if r.status_code < 500:
                    return r
                err = f"http_{r.status_code}"
            except httpx.TimeoutException:
                err = "timeout"
            except httpx.TransportError as e:
                err = type(e).__name__
            self.errors[f"{op}:{err}"] += 1
            self.rec(op=op, key=key, url=url, err=err, try_=tries)
            if time.monotonic() - start > self.retry_deadline_s:
                raise GaveUp(f"{op} {url} key={key}: {err} after {tries} tries")
            await asyncio.sleep(backoff * random.uniform(0.5, 1.5))
            backoff = min(backoff * 2, 2.0)

    # ----------------------------------------------------------------- per key

    async def run_spec(self, s: Spec) -> None:
        delay = self.t0 + s.t - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            first = await asyncio.gather(*(self._post(s, s.payload, idx) for idx in range(s.burst)))
            ids = [job_id for status, job_id in first if status in (200, 201)]
            if not ids:
                return  # I1 reports the key as never acknowledged
            job_id = ids[0]
            tasks = [self._post(s, s.payload, -1 - k, after=d) for k, d in enumerate(s.delayed)]
            if s.diff_body:
                tasks.append(self._post(s, {**s.payload, "work_ms": s.payload["work_ms"] + 1}, -100, diff=True))
            if s.cancel:
                tasks.append(self._cancel_flow(s, job_id))
            await asyncio.gather(*tasks)
        except GaveUp as e:
            self.errors["gave_up"] += 1
            self.rec(op="gave_up", key=s.key, err=str(e))

    async def _post(self, s: Spec, body: dict, idx: int, after: float = 0.0, diff: bool = False) -> tuple[int, int | None]:
        if after:
            await asyncio.sleep(after)
        r = await self.call("create", "POST", "/jobs", key=s.key, json=body)
        j = r.json()
        job = j.get("job") if r.status_code == 409 else j
        job = job or {}
        self.rec(op="create", key=s.key, idx=idx, diff=diff, status=r.status_code, job=job.get("id"), state=job.get("state"))
        return r.status_code, job.get("id")

    async def get(self, job_id: int) -> dict:
        r = await self.call("get", "GET", f"/jobs/{job_id}")
        j = r.json()
        if j.get("state") in TERMINAL:
            self.rec(op="get", job=job_id, status=r.status_code, state=j["state"])
        else:
            self.gets_nonterminal += 1
        return j

    async def _poll(self, job_id: int, until: set[str], every: float, timeout: float) -> str | None:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            st = (await self.get(job_id)).get("state")
            if st in until:
                return st
            await asyncio.sleep(every)
        return None

    async def _cancel(self, job_id: int, kind: str, seen: str | None) -> None:
        r = await self.call("cancel", "POST", f"/jobs/{job_id}/cancel")
        j = r.json()
        state = j.get("state") if r.status_code == 200 else (j.get("job") or {}).get("state")
        self.rec(op="cancel", job=job_id, kind=kind, seen=seen, status=r.status_code, state=state)

    async def _cancel_flow(self, s: Spec, job_id: int) -> None:
        seen = None
        if s.cancel == "when_running":
            seen = await self._poll(job_id, {"running"} | TERMINAL, every=0.025, timeout=120)
            if seen == "running":
                await asyncio.sleep(s.cancel_delay)
        elif s.cancel == "after_terminal":
            seen = await self._poll(job_id, set(TERMINAL), every=0.25, timeout=150)
        await self._cancel(job_id, s.cancel, seen)
        if s.cancel_twice:
            await self._cancel(job_id, s.cancel + "_repeat", seen)

    # ------------------------------------------------------------- pagination

    async def walk(self, state: str | None = None, limit: int = 100) -> list[int]:
        ids: list[int] = []
        cursor = None
        while True:
            params: dict = {"limit": limit}
            if state:
                params["state"] = state
            if cursor:
                params["cursor"] = cursor
            page = (await self.call("list", "GET", "/jobs", params=params)).json()
            ids += [j["id"] for j in page["items"]]
            cursor = page["next_cursor"]
            if not cursor:
                return ids

    async def close(self) -> None:
        await self.http.aclose()
