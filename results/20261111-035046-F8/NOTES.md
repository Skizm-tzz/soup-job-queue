# F8: aborted (harness error), not a verdict

**Symptom.** From about 50 s into the run, the run's API stopped answering every request, including `GET /jobs/{id}`. The client journal has 4 834 create timeouts, 166 get, 37 cancel and 16 list timeouts, and no 5xx. Creates stopped at key 1 200 of 2 000. The workers kept going: they drained the queue (all 1 200 jobs terminal by ~110 s, last worker event at 80 s). At the end, the pagination walk gave up after 16 timeouts, the harness raised `GaveUp` and the invariant check never ran.

**What was different about this run.** The whole system was slow before the API hung:

| at ~30 s | this run | F8-rerun |
|---|---|---|
| keys sent | 904 | 1 155 |
| jobs queued | 554 | 4 |

The harness's own 10-second progress loop logged at gaps of 65, 68, 50 and 31 s, so the harness process itself was starved. This run started about 5 minutes after the laptop resumed from sleep (F4 started at 03:45 UTC after the gap following F3).

**Seeded bug visible anyway.** The kept database (`chaos_20261111_035046_f8`) has jobs with up to 5 effects each, so I3 would have failed if the check had run.

**Rerun.** [20261111-042333-F8-rerun](../20261111-042333-F8-rerun/), same seed and flags, finished normally and FAILED as expected (I3 1 308, I5 27, I6 33, I7 718, I8 3, B1 646). During it, `GET /healthz` was sampled from inside the container every ~12 s and answered 200 in 42–65 ms every time ([api-monitor.txt](../20261111-042333-F8-rerun/api-monitor.txt)).

**Cause: not determined.** The likely trigger is the host slowdown, but why the API stayed unresponsive for minutes after the queue was idle cannot be told from these logs: `api.log` has no error and there is no stack dump. It has not been reproduced. It stays an open concern in REPORT.md, because an API that hangs without logging anything would be a real production problem.

**What was changed** (commit after this note):
- `app/api.py` registers `faulthandler` on `SIGUSR1`, so a hung API's thread stacks can be dumped without stopping it.
- `chaos/harness.py`: if the API stops answering during the final walk, the harness sends `SIGUSR1` to it, records `api_unresponsive` and still runs every invariant check (the run then FAILS on I10 and I1 instead of being lost as a harness error).
