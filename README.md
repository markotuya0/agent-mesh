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

Retell any run as a story, or prove the mesh survives a dead worker:
```bash
./scripts/replay.sh              # latest run
./scripts/replay.sh run-abc123   # a specific run
./scripts/chaos.sh               # kills translator mid-run, watches it heal
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
| `GATEWAY_PORT` | Dashboard + API port on your machine | `8080` |
| `USE_REAL_TRANSLATOR` | `false` = built-in dictionary (offline) · `true` = free MyMemory API with dictionary fallback | `false` |

## Translator modes: mock vs live

Out of the box the translator uses a small built-in word dictionary and tags output like `[FR-mock]`. It works offline and costs nothing, which is why demos and tests run on it.

Flip one flag to use real translations:

```bash
USE_REAL_TRANSLATOR=true docker compose up -d translator gateway
```

What changes: the translator calls the free MyMemory API (no key needed) and returns genuine translations, so the `-mock` tag disappears. What does not change: anything else. Same streams, same envelope, same dashboard. If the API is slow, over quota or unreachable, that single item falls back to the dictionary (8 second cap per call) and the run finishes anyway. The dashboard pill always shows which mode is live: amber `Simulated translations` or green `Live translations`. Flip back anytime:

```bash
USE_REAL_TRANSLATOR=false docker compose up -d translator gateway
```

Swapping in a different provider later (DeepL, Google, an LLM) means editing one function, `translateReal()` in `services/translator/worker.js`. The queue contract stays untouched.

## Design notes
- Pipeline pattern: strictly sequential stages with handoff contracts (planner scored single-agent for this scale; pipeline chosen deliberately to demonstrate reliable service-to-service handoffs)
- Queue scored 86 vs 79 for RabbitMQ on developer experience and learning curve; Redis doubles as the state store so the whole mesh is one container plus workers
- Observability follows the three pillars lightly: structured JSON logs with correlation IDs, a persisted JSONL trail, and a status endpoint. No Grafana by design: the trail file plus `GET /runs/:id` answers every demo question
- All intelligence is mocked offline (local feed, dictionary translator, blocklist). Swap any worker's `handle()` for a real model call without touching the envelope

## Tech
Python workers, Node.js gateway and translator, Redis Streams, Docker Compose. Offline by default, one optional free API call when you flip the flag.
