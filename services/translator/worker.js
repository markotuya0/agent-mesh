// Translator agent (Node.js): offline mock translation per item.
//
// Listens:  mesh:translate (finder -> translator)
// Publishes: mesh:screen   (translator -> compliance)
// Honest mock: a tiny word dictionary plus a language tag, so the demo
// runs with zero keys. Swap `translate()` for a real model call later.

const redis = require("redis");
const fs = require("fs");
const path = require("path");

const QUEUE_URL = process.env.QUEUE_URL || "redis://localhost:6379";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const TRAIL_DIR = process.env.TRAIL_DIR || "./trail";

const DICT = {
  fr: { startup: "startup", raises: "lève", "seed round": "tour d'amorçage", model: "modèle", data: "données", breach: "violation", new: "nouveau", app: "appli" },
  es: { startup: "startup", raises: "levanta", "seed round": "ronda semilla", model: "modelo", data: "datos", breach: "brecha", new: "nueva", app: "app" },
  de: { startup: "Startup", raises: "sammelt", "seed round": "Seed-Runde", model: "Modell", data: "Daten", breach: "Verletzung", new: "neu", app: "App" },
};

function translate(text, lang) {
  const table = DICT[lang] || DICT.fr;
  let out = text;
  for (const [en, local] of Object.entries(table)) {
    out = out.replace(new RegExp(en, "gi"), local);
  }
  return `[${lang.toUpperCase()}] ${out}`;
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
  const translated = translate(payload.text, payload.target_lang || "fr");
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
  console.log(JSON.stringify({ agent: "translator", event: "listening", stream: "mesh:translate" }));
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
