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

const DASHBOARD = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AgentMesh - live runs</title>
<style>
body{font-family:system-ui,sans-serif;max-width:760px;margin:2rem auto;padding:0 1rem;color:#1a1a1a}
.card{border:1px solid #ddd;border-radius:10px;padding:1rem;margin:0 0 1rem}
.pill{display:inline-block;border-radius:99px;padding:.1rem .6rem;font-size:.8rem}
.running{background:#eef4ff}.completed{background:#e9f7ef}.flag{background:#fdecec}
form.card{display:flex;gap:.5rem;flex-wrap:wrap;align-items:end}
label{font-size:.8rem;display:flex;flex-direction:column;gap:.2rem}
input,select,button{padding:.45rem .6rem;border-radius:6px;border:1px solid #ccc;font-size:.9rem}
button{background:#4f46e5;color:#fff;border:none;cursor:pointer}
.item{border-top:1px solid #eee;padding:.5rem 0;font-size:.9rem}
.mono{font-family:monospace;font-size:.8rem;color:#555}
</style></head><body>
<h1>AgentMesh</h1>
<p>Finder finds headlines, translator translates them, compliance screens them. Start a run:</p>
<form class="card" id="new">
<label>Topic<input name="topic" value="AI startups" required maxlength="100"></label>
<label>Count<input name="count" type="number" value="3" min="1" max="10"></label>
<label>Language<select name="lang"><option value="fr">French</option><option value="es">Spanish</option><option value="de">German</option></select></label>
<button>Run the mesh</button>
</form>
<div id="runs"></div>
<script>
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){
  const r = await fetch('/runs').then(r=>r.json());
  document.getElementById('runs').innerHTML = r.runs.slice().reverse().map(run=>{
    const pct = run.expected ? Math.round(run.received/run.expected*100) : 0;
    const items = (run.results||[]).map(x=>
      '<div class="item"><b>'+esc(x.item_id)+'</b> <span class="pill '+(x.verdict==='flag'?'flag':'completed')+'">'+esc(x.verdict)+'</span><br>'+
      esc(x.text)+'<br><span class="mono">'+esc(x.translated)+' <i>'+esc(x.reason)+'</i></span></div>').join('');
    return '<div class="card"><b>'+esc(run.topic)+'</b> <span class="pill '+run.status+'">'+esc(run.status)+'</span> '+
      '<span class="mono">'+esc(run.run_id)+' - '+run.received+'/'+run.expected+'</span>'+
      '<div style="background:#eee;border-radius:99px;height:8px;margin:.5rem 0"><div style="background:#4f46e5;height:8px;border-radius:99px;width:'+pct+'%"></div></div>'+
      items+'</div>';
  }).join('') || '<p>No runs yet.</p>';
}
document.getElementById('new').addEventListener('submit', async e=>{
  e.preventDefault();
  const f = new FormData(e.target);
  await fetch('/runs',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({topic:f.get('topic'),count:f.get('count'),lang:f.get('lang')})});
  load();
});
load(); setInterval(load, 2000);
</script></body></html>`;

function summary(runId) {  const run = runs.get(runId);
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
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type": "text/html" });
      return res.end(DASHBOARD);
    }
    return send(res, 404, { error: { code: "NOT_FOUND", message: "Use POST /runs or GET /runs/:id." } });
  });

  server.listen(PORT, () => console.log(JSON.stringify({ agent: "gateway", event: "listening", port: PORT })));
}

main().catch((e) => { console.error(e); process.exit(1); });
