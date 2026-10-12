"""Compliance agent (Python): screens translated text against a blocklist.

Listens:  mesh:screen  (translator -> compliance)
Publishes: mesh:results (compliance -> gateway)
Verdict pass/flag per item; flags never stop the run.
"""

import json
import os
import time
import uuid

import redis

QUEUE_URL = os.environ.get("QUEUE_URL", "redis://localhost:6379")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))
TRAIL_DIR = os.environ.get("TRAIL_DIR", "./trail")
HERE = os.path.dirname(os.path.abspath(__file__))


def log(agent, run_id, event, latency_ms=0, detail=""):
    os.makedirs(TRAIL_DIR, exist_ok=True)
    with open(os.path.join(TRAIL_DIR, f"{run_id}.jsonl"), "a") as f:
        f.write(json.dumps({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "run_id": run_id, "agent": agent, "event": event,
            "latency_ms": latency_ms, "detail": detail,
        }) + "\n")
    print(json.dumps({"agent": "compliance", "run_id": run_id, "event": event, "detail": detail}), flush=True)


def load_blocklist():
    with open(os.path.join(HERE, "blocklist.txt")) as f:
        return [l.strip().lower() for l in f if l.strip() and not l.startswith("#")]


def screen(text, blocklist):
    low = text.lower()
    for term in blocklist:
        if term in low:
            return "flag", f"matched '{term}'"
    return "pass", "clean"


def handle(r, fields, blocklist):
    t0 = time.time() * 1000
    payload = json.loads(fields["payload"])
    run_id = fields["run_id"]
    log("compliance", run_id, "started", detail=f"item={payload['item_id']}")
    verdict, reason = screen(f"{payload.get('text', '')} {payload.get('translated', '')}", blocklist)
    r.xadd("mesh:results", {
        "run_id": run_id, "stage": "results", "attempt": "0",
        "payload": json.dumps({**payload, "verdict": verdict, "reason": reason}),
        "ts": str(int(time.time())),
    })
    log("compliance", run_id, "succeeded", int(time.time() * 1000 - t0),
        f"item={payload['item_id']} verdict={verdict}")


def main():
    blocklist = load_blocklist()
    r = redis.Redis.from_url(QUEUE_URL, decode_responses=True)
    try:
        r.xgroup_create("mesh:screen", "compliance", id="0", mkstream=True)
    except redis.ResponseError:
        pass  # group exists
    print(json.dumps({"agent": "compliance", "event": "listening", "stream": "mesh:screen",
                      "terms": len(blocklist)}), flush=True)
    consumer = f"compliance-{uuid.uuid4().hex[:6]}"
    while True:
        msgs = r.xreadgroup("compliance", consumer, {"mesh:screen": ">"}, count=1, block=2000)
        if not msgs:
            continue
        for _, entries in msgs:
            for msg_id, fields in entries:
                attempt = 0
                while True:
                    try:
                        handle(r, fields, blocklist)
                        r.xack("mesh:screen", "compliance", msg_id)
                        break
                    except Exception as e:  # noqa: BLE001 - degrade, never crash the mesh
                        attempt += 1
                        log("compliance", fields.get("run_id", "?"), "retried", detail=str(e))
                        if attempt > MAX_RETRIES:
                            log("compliance", fields.get("run_id", "?"), "degraded",
                                detail=f"screen failed after {attempt} attempts: {e}")
                            r.xack("mesh:screen", "compliance", msg_id)
                            break


if __name__ == "__main__":
    main()
