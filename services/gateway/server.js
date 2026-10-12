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

const runs = new Map(); // run_id -> { n, topic, count, expected, results[], created }
let seq = 0;

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
:root{--ink:#16213a;--muted:#5b6b82;--line:#e3e8ef;--bg:#f4f6fa;--brand:#4f46e5;--green:#15803d;--greenbg:#e7f6ec;--amber:#b45309;--amberbg:#fef3e2;--red:#b91c1c;--redbg:#fdecec;--bluebg:#eaf0fe}
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;max-width:780px;margin:0 auto;padding:2rem 1rem 4rem;color:var(--ink);background:var(--bg)}
header.hero h1{margin:0;font-size:1.9rem;letter-spacing:-.02em}
header.hero p{color:var(--muted);margin:.4rem 0 1.2rem}
.card{background:#fff;border:1px solid var(--line);border-radius:14px;padding:1.1rem 1.2rem;margin:0 0 1rem;box-shadow:0 1px 2px rgba(16,24,40,.05)}
.pill{display:inline-block;border-radius:99px;padding:.15rem .65rem;font-size:.78rem;font-weight:600}
.running{background:var(--bluebg);color:#1d4ed8}.completed{background:var(--greenbg);color:var(--green)}
.pass{background:var(--greenbg);color:var(--green)}.flag{background:var(--redbg);color:var(--red)}
.pending{background:var(--amberbg);color:var(--amber)}
form.card{display:flex;gap:.6rem;flex-wrap:wrap;align-items:end}
label{font-size:.78rem;font-weight:600;color:var(--muted);display:flex;flex-direction:column;gap:.25rem}
input,select{padding:.5rem .65rem;border-radius:8px;border:1px solid var(--line);font-size:.92rem;background:#fff;color:var(--ink)}
button.run{background:var(--brand);color:#fff;border:none;cursor:pointer;padding:.55rem 1.1rem;border-radius:8px;font-size:.92rem;font-weight:600}
button.run:hover{filter:brightness(1.07)}
.runhead{display:flex;align-items:baseline;gap:.6rem;flex-wrap:wrap}
.runhead h2{margin:0;font-size:1.05rem}
.time{color:var(--muted);font-size:.8rem}
.bar{background:#e8ecf2;border-radius:99px;height:8px;margin:.6rem 0}
.bar>div{background:var(--brand);height:8px;border-radius:99px;transition:width .4s}
.item{border-top:1px solid var(--line);padding:.7rem 0;font-size:.92rem}
.item h3{margin:0 0 .3rem;font-size:.92rem;display:flex;gap:.5rem;align-items:center}
.stages{display:flex;gap:.4rem;margin:.35rem 0;font-size:.76rem;color:var(--muted)}
.stages span{background:#f1f4f9;border-radius:99px;padding:.1rem .55rem}
.stages span.ok{background:var(--greenbg);color:var(--green)}
.quote{margin:.15rem 0;color:var(--ink)}
.translated{margin:.15rem 0;color:var(--muted);font-size:.85rem}
details.raw{margin-top:.3rem;font-size:.76rem;color:var(--muted)}
details.raw summary{cursor:pointer}
footer{color:var(--muted);font-size:.8rem;text-align:center;margin-top:2rem}
</style></head><body>
<header class="hero">
<h1>AgentMesh</h1>
<p>Three agents on a factory line: one finds headlines, one translates them, one screens them. Start a run below and watch it happen.</p>
</header>
<form class="card" id="new">
<label>Topic<input name="topic" value="AI startups" required maxlength="100"></label>
<label>Count<input name="count" type="number" value="3" min="1" max="10"></label>
<label>Language<select name="lang"><option value="fr">French</option><option value="es">Spanish</option><option value="de">German</option></select></label>
<button class="run">Run the mesh</button>
</form>
<div id="runs"></div>
<footer>Finder &rarr; Translator &rarr; Compliance &middot; every hop logged, nothing lost.</footer>
<script>
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ago = iso => { const s = Math.max(1, Math.round((Date.now() - new Date(iso).getTime())/1000)); if (s < 60) return s+'s ago'; const m = Math.round(s/60); if (m < 60) return m+'m ago'; return Math.round(m/60)+'h ago'; };
async function load(){
  const r = await fetch('/runs').then(r=>r.json());
  const list = (r.runs||[]).slice().sort((a,b)=>(b.n||0)-(a.n||0));
  document.getElementById('runs').innerHTML = list.map(run=>{
    const pct = run.expected ? Math.round(run.received/run.expected*100) : 0;
    const items = (run.results||[]).map((x,i)=>
      '<div class="item"><h3>Headline '+(i+1)+' <span class="pill '+(x.verdict==='flag'?'flag':'pass')+'">'+(x.verdict==='flag'?'Needs review':'Passed')+'</span></h3>'+
      '<p class="quote">'+esc(x.text)+'</p>'+
      '<p class="translated">'+esc(x.translated)+'</p>'+
      '<div class="stages"><span class="ok">Found</span><span class="ok">Translated</span><span class="ok">Screened</span></div>'+
      '<details class="raw"><summary>Technical details</summary>id '+esc(x.item_id)+' &middot; '+esc(x.reason||'clean')+'</details></div>').join('');
    const pend = run.expected - run.received;
    const pending = pend > 0 ? '<div class="item"><h3><span class="pill pending">Working on ' + pend + ' more&hellip;</span></h3></div>' : '';
    const hist = run.historic ? '<div class="item"><span class="time">Finished before the last restart. Full story lives in its trail file.</span></div>' : '';
    return '<div class="card"><div class="runhead"><h2>Run #'+(run.n||'?')+' &middot; '+esc(run.topic)+'</h2>'+
      '<span class="pill '+run.status+'">'+(run.status==='completed'?'Finished':'Running')+'</span>'+
      '<span class="time">'+(run.created?ago(run.created):'')+' &middot; '+run.received+' of '+run.expected+' headlines</span></div>'+
      '<div class="bar"><div style="width:'+pct+'%"></div></div>'+
      items + pending + hist + '</div>';
  }).join('') || '<div class="card"><p>No runs yet. Start one above.</p></div>';
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
  if (run.historic) {
    return {
      run_id: runId, n: run.n ?? null, topic: run.topic, status: "completed",
      expected: run.expected, received: run.expected, created: null,
      flagged: 0, results: [], historic: true,
    };
  }
  const done = run.results.length >= run.expected;
  return {
    run_id: runId, n: run.n ?? null, topic: run.topic, status: done ? "completed" : "running",
    expected: run.expected, received: run.results.length, created: run.created || null,
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

  // Rehydrate the dashboard list from trail files on boot (refresh-proof runs).
  // Old runs show as finished without item detail (live results live in memory).
  try {
    const files = fs.readdirSync(TRAIL_DIR).filter((f) => f.endsWith(".jsonl")).sort();
    for (const f of files) {
      const runId = f.replace(/\.jsonl$/, "");
      if (runs.has(runId)) continue;
      let topic = "earlier run", count = 0;
      try {
        for (const line of fs.readFileSync(path.join(TRAIL_DIR, f), "utf8").split("\n")) {
          if (!line.trim()) continue;
          const e = JSON.parse(line);
          const m = e.agent === "gateway" && e.event === "started" && /topic=(.*?) count=(\d+)/.exec(e.detail || "");
          if (m) { topic = m[1]; count = parseInt(m[2], 10); break; }
        }
      } catch { /* keep defaults */ }
      runs.set(runId, { n: ++seq, topic, count, expected: count, results: [], created: "", historic: true });
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
      runs.set(runId, { n: ++seq, topic, count, expected: count, results: [], created: new Date().toISOString() });
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
