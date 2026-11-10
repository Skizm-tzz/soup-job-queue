"""Seeded bugs for proving that the harness can see failures.

Every flag is off by default and is enabled with FAULT_<NAME>=1. With a flag on,
a chaos run is expected to FAIL on a specific invariant (see README). A run that
passes with a seeded bug means the harness is blind to that bug.
"""

from __future__ import annotations

import os

KNOWN = frozenset(
    {
        "CLAIM_READ_THEN_WRITE",  # claim: unlocked SELECT, then UPDATE without a condition
        "NO_FENCE",  # complete: no attempt/state check
        "SPLIT_EFFECT",  # complete: effect inserted by a separate statement first
        "WORKER_CLOCK",  # claim: lease expiry judged by the worker's (skewed) clock
        "CREATE_CHECK_THEN_INSERT",  # create: SELECT then INSERT (harness also drops UNIQUE)
        "NO_HEARTBEAT",  # worker never renews its lease
        "CANCEL_BLIND",  # cancel: no state condition
    }
)

ENABLED = frozenset(name for name in KNOWN if os.environ.get(f"FAULT_{name}") == "1")

_unknown = [k for k in os.environ if k.startswith("FAULT_") and k[6:] not in KNOWN]
if _unknown:
    raise RuntimeError(f"unknown fault flags: {_unknown}")


def on(name: str) -> bool:
    if name not in KNOWN:
        raise KeyError(name)
    return name in ENABLED
