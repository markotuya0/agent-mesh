"""Finder agent (Python): collects mock headlines per run and fans out translate jobs.

Listens:  mesh:collect   (gateway -> finder)
Publishes: mesh:translate (finder -> translator)
Trail:    started / succeeded / degraded per job, JSONL with correlation ID.
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
    print(json.dumps({"agent": "finder", "run_id": run_id, "event": event, "detail": detail}), flush=True)


def load_feed():
    with open(os.path.join(HERE, "feed.json")) as f:
        return json.load(f)


def pick(feed, topic, count):
    want = topic.lower()
    scored = [h for h in feed if want in h.lower()] or feed
    return [scored[i % len(scored)] for i in range(count)]


def handle(r, job):
    t0 = time.time() * 1000
    run_id, topic, count = job["run_id"], job["topic"], int(job["count"])
    log("finder", run_id, "started", detail=f"topic={topic} count={count}")
    headlines = pick(load_feed(), topic, count)
    for i, h in enumerate(headlines):
        r.xadd("mesh:translate", {
            "run_id": run_id, "stage": "translate", "attempt": "0",
            "payload": json.dumps({"item_id": f"{run_id}-{i}", "text": h,
                                   "target_lang": job.get("target_lang", "fr")}),
            "ts": str(int(time.time())),
        })
    log("finder", run_id, "succeeded", int(time.time() * 1000 - t0), f"{len(headlines)} headlines")


def main():
    r = redis.Redis.from_url(QUEUE_URL, decode_responses=True)
    try:
        r.xgroup_create("mesh:collect", "finder", id="0", mkstream=True)
    except redis.ResponseError:
        pass  # group exists
    print(json.dumps({"agent": "finder", "event": "listening", "stream": "mesh:collect"}), flush=True)
    consumer = f"finder-{uuid.uuid4().hex[:6]}"
    while True:
        msgs = r.xreadgroup("finder", consumer,
                            {"mesh:collect": ">"}, count=1, block=2000)
        if not msgs:
            continue
        for _, entries in msgs:
            for msg_id, fields in entries:
                attempt = 0
                while True:
                    try:
                        handle(r, fields)
                        r.xack("mesh:collect", "finder", msg_id)
                        break
                    except Exception as e:  # noqa: BLE001 - degrade, never crash the mesh
                        attempt += 1
                        log("finder", fields.get("run_id", "?"), "retried", detail=str(e))
                        if attempt > MAX_RETRIES:
                            log("finder", fields.get("run_id", "?"), "degraded",
                                detail=f"collect failed after {attempt} attempts: {e}")
                            r.xack("mesh:collect", "finder", msg_id)
                            break


if __name__ == "__main__":
    main()
