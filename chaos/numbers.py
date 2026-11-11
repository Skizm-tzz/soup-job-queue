"""Recompute every headline number of REPORT.md from the raw run summaries.

Reads results/<run>/summary.json + config.json and results/load/<run>/summary.json,
prints plain tables. Standard library only:

    python chaos/numbers.py > results/NUMBERS.txt
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "results"


def load(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def runs() -> list[tuple[dict, dict]]:
    out = []
    for d in sorted(p for p in ROOT.iterdir() if p.is_dir()):
        cfg = load(d / "config.json")
        if cfg and "scenario" in cfg:
            out.append((cfg, load(d / "summary.json") or {}))
    return out


def bad(s: dict) -> str:
    v = {k: n for k, n in s.get("violations", {}).items() if n}
    return ", ".join(f"{k}={n}" for k, n in v.items()) or "none"


def main() -> None:
    rs = runs()
    done = [(c, s) for c, s in rs if s.get("completed")]
    print(f"chaos runs recorded: {len(rs)} (completed {len(done)}, aborted {len(rs) - len(done)})")
    for c, s in rs:
        if not s.get("completed"):
            print(f"  ABORTED: {c['run_id']}: {s.get('harness_error', 'no summary')}")

    print("\n== canonical (the brief's scenario), no seeded bug, read committed")
    print("label     jobs  kills orphaned takeovers unexplained stale_rejected cancel_running recovery_p50_s I3_bad_effects I1_lost I4_stuck verdict")
    tot = dict.fromkeys(("jobs", "kills", "orph", "take", "unexp", "stale", "crun", "cq", "lostrace", "repeat", "bursts", "exh"), 0)
    for c, s in done:
        if c["scenario"] != "canonical" or c["flags"] or c["isolation"] != "read_committed" or c["label"].startswith("dev"):
            continue
        k, v = s["coverage"], s["violations"]
        print(f"{c['label']:<10}{c['params']['jobs']:<6}{k['kills']:<6}{k['kills_orphaning_a_job']:<9}{k['takeovers']:<10}"
              f"{k['takeovers_unexplained']:<12}{k['stale_completes_rejected']:<15}{k['cancelled_from_running']:<15}"
              f"{k['recovery_after_kill_s']['p50']!s:<15}{v['I3']:<15}{v['I1']:<8}{v['I4']:<9}{s['verdict']}")
        for key, val in (("jobs", c["params"]["jobs"]), ("kills", k["kills"]), ("orph", k["kills_orphaning_a_job"]),
                         ("take", k["takeovers"]), ("unexp", k["takeovers_unexplained"]), ("stale", k["stale_completes_rejected"]),
                         ("crun", k["cancelled_from_running"]), ("cq", k["cancelled_from_queued"]),
                         ("lostrace", k["cancel_lost_race_to_finish"]), ("repeat", k["repeat_posts"]),
                         ("bursts", k["concurrent_bursts"]), ("exh", k["attempts_exhausted"])):
            tot[key] += val
    print(f"TOTAL   jobs={tot['jobs']} kills={tot['kills']} orphaned={tot['orph']} takeovers={tot['take']} "
          f"unexplained={tot['unexp']} stale_rejected={tot['stale']} cancelled_running={tot['crun']} cancelled_queued={tot['cq']} "
          f"cancel_lost_race={tot['lostrace']} repeat_posts={tot['repeat']} concurrent_bursts={tot['bursts']} exhausted={tot['exh']}")

    print("\n== extended and isolation runs, no seeded bug")
    for c, s in done:
        if c["flags"] or c["label"].startswith("dev") or (c["scenario"] == "canonical" and c["isolation"] == "read_committed"):
            continue
        k = s["coverage"]
        print(f"{c['label']:<6}{c['scenario']:<10}{c['isolation']:<16}jobs={c['params']['jobs']} kills={k['kills']} stops={k['stalls']} "
              f"revokes={k['revokes']} conn_kills={k['conn_kills']} api_kills={k['api_kills']} takeovers={k['takeovers']} "
              f"by_cause={k['takeovers_by_cause']} unexplained={k['takeovers_unexplained']} "
              f"stale_rejected={k['stale_completes_rejected']} {k['stale_completes_by_reason']} exhausted={k['attempts_exhausted']} "
              f"client_errors={sum(s['client_errors'].values())} violations={bad(s)} verdict={s['verdict']}")

    print("\n== seeded bugs (each run must FAIL)")
    for c, s in rs:
        if not c["flags"]:
            continue
        exp = ",".join(c["expected"].get("invariants", []))
        got = f"{s['verdict']}: {bad(s)}" if s.get("completed") else "ABORTED (harness error)"
        print(f"{c['label']:<10}{'+'.join(c['flags']):<38}{c['scenario']:<10}expected FAIL on {exp:<10} got {got}")

    print("\n== load runs with k6 (results/load)")
    for d in sorted((ROOT / "load").iterdir()):
        s = load(d / "summary.json") if d.is_dir() else None
        if not s or not s.get("k6"):
            continue
        k6, cr = s["k6"], s["k6"]["latency_ms"].get("create", {})
        st = k6["statuses"]
        errors = sum(n for name, n in st.items() if name.endswith("5xx") or name.endswith("error"))
        print(f"{d.name:<34}rps={k6['rps']} dropped={k6['dropped_iterations']} errors={errors} "
              f"create_ms p50={cr.get('p(50)')} p95={cr.get('p(95)')} p99={cr.get('p(99)')} "
              f"jobs={s['jobs']} finished={s['jobs_finished']} jobs_per_s={s['jobs_per_s_end_to_end']} "
              f"queue_wait_s={s['queue_wait_s_p50_p95_p99']} max_queued={s['max_queued']} "
              f"workers={s['config']['workers_connected']} db_invariants_failed={s['failed_invariants'] or 'none'}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
