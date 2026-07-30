#!/usr/bin/env node
/**
 * barktown — migrate diary index.json → SQLite
 *
 * Reads the existing index.json from MinIO and imports every entry into the
 * diary_entries table in the local SQLite database.  Safe to re-run: all
 * inserts use upsertDiaryEntry which is idempotent.
 *
 * Usage:
 *   node migrate-diary-to-sqlite.mjs
 *   npm run migrate-diary
 */

import { loadEnv } from "./lib/env.mjs";
loadEnv(import.meta.url);

import { buildConfig } from "./lib/config.mjs";
import { createClient, loadJson } from "./lib/minio.mjs";
import { openDb, upsertDiaryEntry } from "./lib/db.mjs";
import { log, warn } from "./lib/log.mjs";

const CFG = buildConfig();
const mc  = createClient(CFG.minio);
const db  = openDb(CFG.dbPath);

log(`Loading ${CFG.indexKey} from bucket "${CFG.bucket}"…`);
const entries = await loadJson(mc, CFG.bucket, CFG.indexKey, []);

if (!Array.isArray(entries) || entries.length === 0) {
  log("index.json is empty or missing — nothing to migrate.");
  process.exit(0);
}

log(`Found ${entries.length} entries. Importing…`);

let ok = 0;
let skipped = 0;

for (const entry of entries) {
  // Validate the minimum required fields.
  if (!entry.id || !entry.audioPath || !entry.date || !entry.datetimeLocal) {
    warn(`  skipping invalid entry (missing required fields): ${JSON.stringify(entry)}`);
    skipped++;
    continue;
  }

  // Derive `time` from datetimeLocal if not present (older index.json entries
  // may not have it).
  if (!entry.time && entry.datetimeLocal) {
    const timePart = entry.datetimeLocal.split("T")[1];
    entry.time = timePart ? timePart.slice(0, 5) : "00:00";
  }

  upsertDiaryEntry(db, entry);
  ok++;
}

const total = db.prepare("SELECT COUNT(*) AS n FROM diary_entries").get().n;
log(`Done. Imported: ${ok}  Skipped: ${skipped}  Total rows in DB: ${total}`);

db.close();
