# AgentMesh

Watch three AI agents pass work to each other, live. You submit a topic, the finder collects headlines, the translator translates them, compliance screens them, and every hop lands in an inspectable trail.

## What it does
- **Finder** (Python): collects headlines for your topic from a local feed
- **Translator** (Node.js): translates each headline to French, Spanish or German
- **Compliance** (Python): screens translations against a blocklist, marks pass or flag
- **Gateway** (Node.js): one REST API to start runs and watch them finish
- **Trail**: every hop logged as JSON with correlation ID, readable anytime

## Run it (2 commands, no keys, no cost)
```bash
docker compose up --build
./scripts/smoke.sh
```

Then open `http://localhost:8080` for the dashboard: start runs from the form, watch progress bars fill, click into pass/flag verdicts. Raw API still works too:
```bash
curl -X POST http://localhost:8080/runs \
  -H 'Content-Type: application/json' \
  -d '{"topic":"AI startups","count":3,"lang":"fr"}'
```

## How the mesh talks
One Redis, four streams, one envelope:

| Stream | From → To | Carries |
| ------ | --------- | ------- |
| `mesh:collect` | gateway → finder | `{run_id, topic, count}` |
| `mesh:translate` | finder → translator | `{run_id, item_id, text, target_lang}` |
| `mesh:screen` | translator → compliance | `{run_id, item_id, text, translated}` |
| `mesh:results` | compliance → gateway | `{run_id, item_id, verdict, reason}` |

Every event: `{run_id, stage, attempt, payload, ts}`. Failed hop retries twice, then the item is marked degraded and the run finishes anyway. The mesh never crashes on bad data.

## Configuration
Copy `.env.example` to `.env` to override defaults:

| Flag | What it does | Default |
| ---- | ------------ | ------- |
| `TARGET_LANG` | Translation language (`fr`, `es`, `de`) | `fr` |
| `MAX_RETRIES` | Hop retries before an item is marked degraded | `2` |
| `TRAIL_DIR` | Where run trails land (JSONL) | `./trail` |

## Design notes
- Pipeline pattern: strictly sequential stages with handoff contracts (planner scored single-agent for this scale; pipeline chosen deliberately to demonstrate reliable service-to-service handoffs)
- Queue scored 86 vs 79 for RabbitMQ on developer experience and learning curve; Redis doubles as the state store so the whole mesh is one container plus workers
- Observability follows the three pillars lightly: structured JSON logs with correlation IDs, a persisted JSONL trail, and a status endpoint. No Grafana by design: the trail file plus `GET /runs/:id` answers every demo question
- All intelligence is mocked offline (local feed, dictionary translator, blocklist). Swap any worker's `handle()` for a real model call without touching the envelope

## Tech
Python workers, Node.js gateway and translator, Redis Streams, Docker Compose. Zero external calls.
