// Gateway (Node.js): the mesh's front door. Starts runs, collects results,
// serves the trail. Plain stdlib HTTP plus the official redis client.

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const redis = require("redis");

const QUEUE_URL = process.env.QUEUE_URL || "redis://localhost:6379";
const TRAIL_DIR = process.env.TRAIL_DIR || "./trail";
const PORT = parseInt(process.env.PORT || "8080", 10);

const runs = new Map(); // run_id -> { topic, count, expected, results[], created }

function trail(runId, event, detail = "") {
  fs.mkdirSync(TRAIL_DIR, { recursive: true });
  fs.appendFileSync(
    path.join(TRAIL_DIR, `${runId}.jsonl`),
    JSON.stringify({ ts: new Date().toISOString(), run_id: runId, agent: "gateway", event, latency_ms: 0, detail }) + "\n"
  );
}

function send(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function summary(runId) {
  const run = runs.get(runId);
  if (!run) return null;
  const done = run.results.length >= run.expected;
  return {
    run_id: runId, topic: run.topic, status: done ? "completed" : "running",
    expected: run.expected, received: run.results.length,
    flagged: run.results.filter((r) => r.verdict === "flag").length,
    results: run.results,
  };
}

async function main() {
  const client = redis.createClient({ url: QUEUE_URL });
  client.on("error", (e) => console.log(JSON.stringify({ agent: "gateway", event: "redis-error", detail: String(e) })));
  await client.connect();
  try {
    await client.xGroupCreate("mesh:results", "gateway", "0", { MKSTREAM: true });
  } catch { /* group exists */ }

  // Background: collect results as compliance finishes items.
  (async () => {
    const consumer = `gateway-${Math.random().toString(16).slice(2, 8)}`;
    for (;;) {
      const res = await client.xReadGroup("gateway", consumer, { key: "mesh:results", id: ">" }, { COUNT: 10, BLOCK: 2000 });
      if (!res) continue;
      for (const { messages } of res) {
        for (const m of messages) {
          try {
            const payload = JSON.parse(m.message.payload);
            const run = runs.get(m.message.run_id);
            if (run && !run.results.some((r) => r.item_id === payload.item_id)) {
              run.results.push(payload);
              if (run.results.length >= run.expected) trail(m.message.run_id, "completed", `${run.expected} items`);
            }
          } finally {
            await client.xAck("mesh:results", "gateway", m.id);
          }
        }
      }
    }
  })();

  // Rehydrate in-memory state from trail files on boot (refresh-proof runs).
  try {
    for (const f of fs.readdirSync(TRAIL_DIR)) {
      if (!f.endsWith(".jsonl")) continue;
      const runId = f.replace(/\.jsonl$/, "");
      if (!runs.has(runId)) runs.set(runId, { topic: "?", count: 0, expected: 0, results: [], created: "" });
    }
  } catch { /* no trail yet */ }

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, "http://x");
    if (req.method === "GET" && url.pathname === "/health") return send(res, 200, { ok: true });
    if (req.method === "GET" && url.pathname === "/runs") {
      return send(res, 200, { runs: [...runs.keys()].map((id) => summary(id)).filter(Boolean) });
    }
    const match = url.pathname.match(/^\/runs\/([\w-]+)$/);
    if (req.method === "GET" && match) {
      const s = summary(match[1]);
      if (!s) {
        // Unknown to memory? Rebuild results from the trail file.
        try {
          const lines = fs.readFileSync(path.join(TRAIL_DIR, `${match[1]}.jsonl`), "utf8").split("\n").filter(Boolean);
          return send(res, 200, { run_id: match[1], status: "unknown-memory", trail: lines.map((l) => JSON.parse(l)) });
        } catch {
          return send(res, 404, { error: { code: "NOT_FOUND", message: "No such run." } });
        }
      }
      return send(res, 200, s);
    }
    if (req.method === "POST" && url.pathname === "/runs") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let data;
      try {
        data = JSON.parse(body || "{}");
      } catch {
        return send(res, 400, { error: { code: "BAD_REQUEST", message: "Body must be JSON." } });
      }
      const topic = String(data.topic || "").trim().slice(0, 100);
      const count = Math.min(10, Math.max(1, parseInt(data.count || "3", 10) || 3));
      const lang = ["fr", "es", "de"].includes(data.lang) ? data.lang : "fr";
      if (!topic) return send(res, 400, { error: { code: "BAD_REQUEST", message: "topic is required." } });
      const runId = `run-${crypto.randomBytes(4).toString("hex")}`;
      runs.set(runId, { topic, count, expected: count, results: [], created: new Date().toISOString() });
      trail(runId, "started", `topic=${topic} count=${count} lang=${lang}`);
      await client.xAdd("mesh:collect", "*", {
        run_id: runId, stage: "collect", attempt: "0",
        topic, count: String(count), target_lang: lang,
        ts: String(Math.floor(Date.now() / 1000)),
      });
      return send(res, 201, { run_id: runId, status: "running", poll: `/runs/${runId}` });
    }
    return send(res, 404, { error: { code: "NOT_FOUND", message: "Use POST /runs or GET /runs/:id." } });
  });

  server.listen(PORT, () => console.log(JSON.stringify({ agent: "gateway", event: "listening", port: PORT })));
}

main().catch((e) => { console.error(e); process.exit(1); });
