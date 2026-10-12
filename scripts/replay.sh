#!/usr/bin/env bash
# Replay any run's trail file as a readable story.
# Usage: ./scripts/replay.sh [run_id]  (default: latest run)
set -euo pipefail
TRAIL_DIR="${TRAIL_DIR:-./trail}"

RUN_ID="${1:-}"
if [ -z "$RUN_ID" ]; then
  RUN_ID=$(ls -t "$TRAIL_DIR"/*.jsonl 2>/dev/null | head -1 | xargs -n1 basename 2>/dev/null | sed 's/\.jsonl$//' || true)
  [ -z "$RUN_ID" ] && { echo "No runs yet. Start one: curl -X POST localhost:8080/runs -d '{\"topic\":\"AI startups\",\"count\":3}'"; exit 1; }
fi

FILE="$TRAIL_DIR/$RUN_ID.jsonl"
[ -f "$FILE" ] || { echo "No trail for $RUN_ID"; exit 1; }

python3 - "$FILE" << 'EOF'
import json, sys
events = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
run = events[0]["run_id"]
started = next((e for e in events if e["event"] == "started" and e["agent"] == "gateway"), None)
print(f"Run {run}" + (f" — {started['detail']}" if started else ""))
print()

NAMES = {"finder": "Finder (headlines)", "translator": "Translator", "compliance": "Compliance check", "gateway": "Gateway"}
for e in events:
    who = NAMES.get(e["agent"], e["agent"])
    ev, ms, d = e["event"], e.get("latency_ms", 0), e.get("detail", "")
    if ev == "started" and e["agent"] == "gateway":
        continue
    elif ev == "started":
        print(f"  {who} picks it up. {d}")
    elif ev == "succeeded":
        print(f"  {who} done in {ms}ms. {d}")
    elif ev == "retried":
        print(f"  {who} stumbled, trying again. {d}")
    elif ev == "degraded":
        print(f"  {who} gave up on this one (marked degraded). {d}")
    elif ev == "completed":
        print(f"\nFinished. {d}")
    else:
        print(f"  {who}: {ev}. {d}")
EOF
