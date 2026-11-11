# fsync latency of the Postgres data volume on this machine

`pg_test_fsync -s 2` inside the postgres container (Docker Desktop on Windows 11, WSL2, laptop SSD), 8 kB writes. Postgres uses `fdatasync`.

| When (UTC) | Host state | fdatasync | open_datasync | fsync |
|---|---|---|---|---|
| 2026-11-11 04:34 | about an hour after the laptop resumed from sleep, during the first load tests | 100.7 ops/s, **9.9 ms/op** | 90.5 ops/s, 11.0 ms | 36.6 ops/s, 27.3 ms |
| 2026-11-11 07:37 | after a full restart of the laptop | 374.3 ops/s, **2.7 ms/op** | 404.7 ops/s, 2.5 ms | 198.5 ops/s, 5.0 ms |

The first row was copied from the terminal output of that session (the command's output was not saved to a file at the time); the second is the raw output of the same command.

Why it matters: every state transition is its own autocommit, so a job costs 4 durable commits (create, claim, execution record, complete) and each worker does 3 of them in sequence. At ~10 ms per flush, 4 workers top out near the 60 jobs/s that the 100 rps mix creates, and the queue grows (`results/load/*-dev-L1-*`, `*-L1` of 04:42). At ~2.7 ms they keep up with room to spare. Group commit lets concurrent sessions share a flush, so total throughput is higher than 1/fsync, but each worker's own loop pays the full latency.
