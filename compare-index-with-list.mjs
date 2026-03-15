#!/usr/bin/env node
/**
 * barktown — compare a hand-written recording list against index.json
 *
 * Usage:  npm run compare-index-with-list -- recordings.txt
 *         node compare-index-with-list.mjs recordings.txt
 *
 * Input file format (one recording per line):
 *   YYYY-MM-DD HH-MM-SS label.ext
 *   e.g.  2021-05-25 15-44-00 bjäfsigt.aac
 *
 * Output:
 *   ✅  N  matched
 *   ❌  N  in list but MISSING from index
 *   ⚠️  N  in index but NOT in list
 */

import * as Minio from "minio";
import fs         from "fs";
import path       from "path";
import { fileURLToPath } from "url";

// ─── Load .env ────────────────────────────────────────────────────────────────

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath   = path.join(__dirname, ".env");

if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m && !(m[1] in process.env)) {
      process.env[m[1]] = m[2].replace(/^['"]|['"]$/g, "").trim();
    }
  }
}

// ─── Config ───────────────────────────────────────────────────────────────────

const CFG = {
  minio: {
    endPoint:  process.env.MINIO_ENDPOINT   ?? "localhost",
    port:      parseInt(process.env.MINIO_PORT ?? "9000", 10),
    useSSL:    (process.env.MINIO_USE_SSL   ?? "false") === "true",
    accessKey: process.env.MINIO_ACCESS_KEY ?? "minioadmin",
    secretKey: process.env.MINIO_SECRET_KEY ?? "minioadmin",
  },
  bucket:   process.env.MINIO_BUCKET ?? "barktown",
  indexKey: "index.json",
};

// ─── ANSI colours ─────────────────────────────────────────────────────────────

const GREEN  = "\x1b[32m";
const RED    = "\x1b[31m";
const YELLOW = "\x1b[33m";
const RESET  = "\x1b[0m";
const BOLD   = "\x1b[1m";

// ─── Parse list file ──────────────────────────────────────────────────────────

// Format: YYYY-MM-DD HH-MM-SS <label>.<ext>
// Also accept YYYY-MM-DD HH:MM:SS (colon variant) just in case.
const LINE_RE = /^(\d{4}-\d{2}-\d{2})\s+(\d{2})[-:](\d{2})[-:](\d{2})\s+(.+)$/;

/**
 * Parse a single list-file line.
 * @returns {{ datetimeLocal: string, label: string, originalLine: string } | null}
 */
function parseLine(line, lineNo) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) return null;

  const m = LINE_RE.exec(trimmed);
  if (!m) {
    console.warn(`  ${YELLOW}line ${lineNo}: unrecognised format, skipping: ${trimmed}${RESET}`);
    return null;
  }

  const [, datePart, hh, mm, ss, rest] = m;
  // Strip the extension from the label (optional — just for display).
  const label = rest.replace(/\.[a-zA-Z0-9]{2,5}$/, "").trim();

  return {
    datetimeLocal: `${datePart}T${hh}:${mm}:${ss}`,
    label,
    originalLine: trimmed,
  };
}

// ─── Fetch index.json ─────────────────────────────────────────────────────────

const mc = new Minio.Client(CFG.minio);

async function fetchIndex() {
  const stream = await mc.getObject(CFG.bucket, CFG.indexKey);
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const listFile = process.argv[2];
if (!listFile) {
  console.error(`Usage: node compare-index-with-list.mjs <list-file.txt>`);
  process.exit(1);
}

const absListFile = path.resolve(listFile);
if (!fs.existsSync(absListFile)) {
  console.error(`File not found: ${absListFile}`);
  process.exit(1);
}

console.log(`\nLoading list from: ${absListFile}`);

const rawLines = fs.readFileSync(absListFile, "utf8").split("\n");
const listEntries = rawLines
  .map((line, i) => parseLine(line, i + 1))
  .filter(Boolean);

console.log(`  ${listEntries.length} entries parsed from list\n`);

console.log(`Fetching index.json from bucket "${CFG.bucket}"…`);
const index = await fetchIndex();
const indexEntries = Array.isArray(index) ? index : (index.entries ?? []);
console.log(`  ${indexEntries.length} entries in index\n`);

// ─── Build lookup sets keyed by datetimeLocal ─────────────────────────────────

// Index: datetimeLocal → entry object
/** @type {Map<string, object>} */
const indexByTime = new Map();
for (const e of indexEntries) {
  if (e.datetimeLocal) indexByTime.set(e.datetimeLocal, e);
}

// List: datetimeLocal → parsed line
/** @type {Map<string, object>} */
const listByTime = new Map();
for (const e of listEntries) {
  listByTime.set(e.datetimeLocal, e);
}

// ─── Compare ──────────────────────────────────────────────────────────────────

const matched        = [];   // in both
const missingFromIdx = [];   // in list, not in index
const notInList      = [];   // in index, not in list

for (const [dt, listEntry] of listByTime) {
  if (indexByTime.has(dt)) {
    matched.push({ dt, listEntry, indexEntry: indexByTime.get(dt) });
  } else {
    missingFromIdx.push(listEntry);
  }
}

for (const [dt, indexEntry] of indexByTime) {
  if (!listByTime.has(dt)) {
    notInList.push(indexEntry);
  }
}

// Sort all output by datetimeLocal for readability.
missingFromIdx.sort((a, b) => a.datetimeLocal.localeCompare(b.datetimeLocal));
notInList.sort((a, b) => (a.datetimeLocal ?? "").localeCompare(b.datetimeLocal ?? ""));

// ─── Report ───────────────────────────────────────────────────────────────────

console.log(`${BOLD}═══ Results ═══════════════════════════════════════════${RESET}\n`);

console.log(`${GREEN}✅  ${matched.length} matched${RESET}`);

console.log(`\n${RED}❌  ${missingFromIdx.length} in list but MISSING from index:${RESET}`);
if (missingFromIdx.length === 0) {
  console.log("    (none)");
} else {
  for (const e of missingFromIdx) {
    console.log(`    ${e.datetimeLocal}  ${e.label}`);
  }
}

console.log(`\n${YELLOW}⚠️   ${notInList.length} in index but NOT in list:${RESET}`);
if (notInList.length === 0) {
  console.log("    (none)");
} else {
  for (const e of notInList) {
    const label = e.label ?? e.id ?? "(no label)";
    console.log(`    ${e.datetimeLocal}  ${label}`);
  }
}

console.log();
