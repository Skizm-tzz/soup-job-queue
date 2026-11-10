"""Runtime settings, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

ISOLATION_LEVELS = {
    "read_committed": "read committed",
    "repeatable_read": "repeatable read",
    "serializable": "serializable",
}


def _bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, "1" if default else "0").strip().lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class Settings:
    database_url: str = os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:5432/jobs")
    isolation: str = ISOLATION_LEVELS[os.environ.get("DB_ISOLATION", "read_committed")]
    statement_timeout_ms: int = int(os.environ.get("STATEMENT_TIMEOUT_MS", "10000"))

    lease_ttl_s: float = float(os.environ.get("LEASE_TTL_S", "5"))
    heartbeat_s: float = float(os.environ.get("HEARTBEAT_S", "1"))
    max_attempts: int = int(os.environ.get("MAX_ATTEMPTS", "5"))

    # Stable across restarts of the same worker (container hostname or a slot
    # number given by the chaos harness). Must not contain ':'.
    worker_slot: str = os.environ.get("WORKER_SLOT", "")
    release_on_start: bool = _bool("RELEASE_ON_START")
    # Chaos only: keep working and try to complete even after the lease is lost,
    # so that the fencing in complete() is exercised on every lost lease.
    chaos_ignore_lease_loss: bool = _bool("CHAOS_IGNORE_LEASE_LOSS")
    # Used only by the seeded bug FAULT_WORKER_CLOCK.
    clock_skew_s: float = float(os.environ.get("CLOCK_SKEW_S", "0"))

    api_pool_max: int = int(os.environ.get("API_POOL_MAX", "20"))


settings = Settings()
