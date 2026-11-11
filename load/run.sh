#!/bin/sh
# Load test against the running standard stack (`docker compose up -d` first).
#   sh load/run.sh <label> [rate] [duration_s]
# Starts chaos.loadwatch (DB-side sampling and invariants) and k6 (load/mix.js) together.
set -eu
export MSYS_NO_PATHCONV=1  # Git Bash on Windows would rewrite /load/mix.js into a Windows path
label=${1:?label}
rate=${2:-100}
dur=${3:-600}
run="$(date -u +%Y%m%d-%H%M%S)-$label"
mkdir -p "results/load/$run"
docker compose --profile chaos run --rm --entrypoint python chaos -m chaos.loadwatch --run "$run" --duration "$dur" \
  > "results/load/$run/loadwatch.out" 2>&1 &
watch=$!
sleep 3
docker compose --profile load run --rm -e RUN="$run" -e RATE="$rate" -e DURATION="${dur}s" k6 run --quiet /load/mix.js \
  > "results/load/$run/k6.out" 2>&1 || echo "k6 exited with $?"
wait $watch || echo "loadwatch exited with $?"
grep -v Container "results/load/$run/k6.out" | tail -8
grep -E "k6 |jobs |invariants" "results/load/$run/harness.log"
