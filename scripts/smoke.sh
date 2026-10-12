#!/usr/bin/env bash
# Smoke test: start a run, poll until it completes, assert the trail.
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"

echo "-> health"
curl -sf "$BASE/health" | grep -q '"ok":true' && echo "   healthy"

echo "-> start run (topic with a breach headline to prove flags work)"
RUN_JSON=$(curl -sf -X POST "$BASE/runs" -H 'Content-Type: application/json' \
  -d '{"topic":"breach","count":3,"lang":"fr"}')
echo "   $RUN_JSON"
RUN_ID=$(echo "$RUN_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])")

echo "-> poll $RUN_ID"
for i in $(seq 1 30); do
  STATUS=$(curl -sf "$BASE/runs/$RUN_ID")
  ST=$(echo "$STATUS" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
  if [ "$ST" = "completed" ]; then
    echo "   completed after ~$((i * 2))s"
    echo "$STATUS" | python3 -m json.tool | head -40
    N=$(echo "$STATUS" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['results']))")
    [ "$N" = "3" ] || { echo "FAIL: expected 3 results, got $N"; exit 1; }
    echo "SMOKE OK: 3/3 items, run $RUN_ID"
    exit 0
  fi
  sleep 2
done
echo "FAIL: run did not complete in 60s"
exit 1
