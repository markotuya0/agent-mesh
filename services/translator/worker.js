// Translator agent (Node.js): mock dictionary by default, real API on flag.
//
// Listens:  mesh:translate (finder -> translator)
// Publishes: mesh:screen   (translator -> compliance)
//
// Modes (USE_REAL_TRANSLATOR env):
//   "false" (default) — offline word dictionary + language tag. Zero keys,
//                       zero cost, works on a plane.
//   "true"            — free MyMemory translation API (no key). Any failure
//                       (rate limit, offline) falls back to the mock for that
//                       item, so the mesh never stalls on a network blip.

const redis = require("redis");
const fs = require("fs");
const https = require("https");
const path = require("path");

const QUEUE_URL = process.env.QUEUE_URL || "redis://localhost:6379";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const TRAIL_DIR = process.env.TRAIL_DIR || "./trail";
const USE_REAL = String(process.env.USE_REAL_TRANSLATOR || "false").toLowerCase() === "true";

const DICT = {
  fr: { startup: "startup", raises: "lève", "seed round": "tour d'amorçage", model: "modèle", data: "données", breach: "violation", new: "nouveau", app: "appli" },
  es: { startup: "startup", raises: "levanta", "seed round": "ronda semilla", model: "modelo", data: "datos", breach: "brecha", new: "nueva", app: "app" },
  de: { startup: "Startup", raises: "sammelt", "seed round": "Seed-Runde", model: "Modell", data: "Daten", breach: "Verletzung", new: "neu", app: "App" },
};

function translateMock(text, lang) {
  const table = DICT[lang] || DICT.fr;
  let out = text;
  for (const [en, local] of Object.entries(table)) {
    out = out.replace(new RegExp(en, "gi"), local);
  }
  return `[${lang.toUpperCase()}-mock] ${out}`;
}

function translateReal(text, lang) {
  const langpair = `en|${lang}`;
  const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text)}&langpair=${langpair}`;
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(null), 8000); // never stall the mesh
    https.get(url, (res) => {
      let raw = "";
      res.on("data", (c) => { raw += c; });
      res.on("end", () => {
        clearTimeout(timer);
        try {
          const t = JSON.parse(raw).responseData?.translatedText?.trim();
          resolve(t || null);
        } catch {
          resolve(null);
        }
      });
    }).on("error", () => { clearTimeout(timer); resolve(null); });
  });
}

async function translate(text, lang) {
  if (!USE_REAL) return translateMock(text, lang);
  const real = await translateReal(text, lang);
  if (real) return real;
  return translateMock(text, lang); // API hiccup -> mock fills in, run continues
}

function trail(runId, event, latencyMs = 0, detail = "") {
  fs.mkdirSync(TRAIL_DIR, { recursive: true });
  fs.appendFileSync(
    path.join(TRAIL_DIR, `${runId}.jsonl`),
    JSON.stringify({ ts: new Date().toISOString(), run_id: runId, agent: "translator", event, latency_ms: latencyMs, detail }) + "\n"
  );
  console.log(JSON.stringify({ agent: "translator", run_id: runId, event, detail }));
}

async function handle(client, fields) {
  const t0 = Date.now();
  const payload = JSON.parse(fields.payload);
  trail(payload.run_id || fields.run_id, "started", 0, `item=${payload.item_id}`);
  const translated = await translate(payload.text, payload.target_lang || "fr");
  await client.xAdd("mesh:screen", "*", {
    run_id: fields.run_id, stage: "screen", attempt: "0",
    payload: JSON.stringify({ ...payload, translated }),
    ts: String(Math.floor(Date.now() / 1000)),
  });
  trail(payload.run_id || fields.run_id, "succeeded", Date.now() - t0, `item=${payload.item_id}`);
}

async function main() {
  const client = redis.createClient({ url: QUEUE_URL });
  client.on("error", (e) => console.log(JSON.stringify({ agent: "translator", event: "redis-error", detail: String(e) })));
  await client.connect();
  try {
    await client.xGroupCreate("mesh:translate", "translator", "0", { MKSTREAM: true });
  } catch { /* group exists */ }
  console.log(JSON.stringify({ agent: "translator", event: "listening", stream: "mesh:translate", mode: USE_REAL ? "live-api-with-mock-fallback" : "mock" }));
  const consumer = `translator-${Math.random().toString(16).slice(2, 8)}`;
  for (;;) {
    const res = await client.xReadGroup("translator", consumer, { key: "mesh:translate", id: ">" }, { COUNT: 1, BLOCK: 2000 });
    if (!res) continue;
    for (const { messages } of res) {
      for (const m of messages) {
        let attempt = 0;
        for (;;) {
          try {
            await handle(client, m.message);
            await client.xAck("mesh:translate", "translator", m.id);
            break;
          } catch (e) {
            attempt += 1;
            trail(m.message.run_id || "?", "retried", 0, String(e && e.message || e));
            if (attempt > MAX_RETRIES) {
              trail(m.message.run_id || "?", "degraded", 0, `translate failed after ${attempt} attempts`);
              await client.xAck("mesh:translate", "translator", m.id);
              break;
            }
          }
        }
      }
    }
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
