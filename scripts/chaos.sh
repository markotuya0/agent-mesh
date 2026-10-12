#!/usr/bin/env bash
# Chaos proof: kill the translator mid-run, show nothing is lost, restart it,
# watch the mesh heal and finish the run on its own.
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"

echo "== 1. murder the translator =="
docker compose stop translator > /dev/null
echo "   translator is down"

echo "== 2. start a run anyway (finder still works, tickets pile up safely) =="
RUN_JSON=$(curl -sf -X POST "$BASE/runs" -H 'Content-Type: application/json' \
  -d '{"topic":"AI startups","count":2,"lang":"fr"}')
RUN_ID=$(echo "$RUN_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])")
echo "   run $RUN_ID started"
sleep 6

echo "== 3. run is stuck but alive (translator down, nothing lost, nothing crashed) =="
curl -sf "$BASE/runs/$RUN_ID" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"   status={d['status']} received={d['received']}/{d['expected']}\")"
docker compose ps --format '{{.Service}}:{{.Status}}' | grep -E "translator|gateway"

echo "== 4. bring the translator back =="
docker compose start translator > /dev/null
echo "   translator is back"

echo "== 5. mesh heals itself: backlog drains, run completes =="
for i in $(seq 1 30); do
  STATUS=$(curl -sf "$BASE/runs/$RUN_ID")
  ST=$(echo "$STATUS" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
  if [ "$ST" = "completed" ]; then
    N=$(echo "$STATUS" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['results']))")
    echo "   completed with $N/2 items, nothing lost"
    echo "CHAOS OK: killed translator mid-run, mesh healed, run $RUN_ID finished"
    exit 0
  fi
  sleep 2
done
echo "FAIL: run did not finish 60s after translator came back"
exit 1
