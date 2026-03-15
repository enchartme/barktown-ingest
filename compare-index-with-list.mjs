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

/**
 * Each non-blank, non-comment line is treated as a filename verbatim.
 * e.g. "2025-12-11 05-32-00.m4a"  or  "2021-05-25 15-44-00 bjäfsigt.aac"
 * @returns {{ filename: string } | null}
 */
function parseLine(line, lineNo) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) return null;
  return { filename: trimmed };
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

// ─── Build lookup sets keyed by filename ─────────────────────────────────────

// Index: filename → entry object
/** @type {Map<string, object>} */
const indexByFilename = new Map();
for (const e of indexEntries) {
  if (e.filename) indexByFilename.set(e.filename, e);
}

// List: filename → parsed line
/** @type {Map<string, object>} */
const listByFilename = new Map();
for (const e of listEntries) {
  listByFilename.set(e.filename, e);
}

// ─── Compare ──────────────────────────────────────────────────────────────────

const matched        = [];   // in both
const missingFromIdx = [];   // in list, not in index
const notInList      = [];   // in index, not in list

for (const [filename, listEntry] of listByFilename) {
  if (indexByFilename.has(filename)) {
    matched.push({ filename, listEntry, indexEntry: indexByFilename.get(filename) });
  } else {
    missingFromIdx.push(listEntry);
  }
}

for (const [filename, indexEntry] of indexByFilename) {
  if (!listByFilename.has(filename)) {
    notInList.push(indexEntry);
  }
}

// Sort all output by filename for readability.
missingFromIdx.sort((a, b) => a.filename.localeCompare(b.filename));
notInList.sort((a, b) => (a.filename ?? "").localeCompare(b.filename ?? ""));

// ─── Report ───────────────────────────────────────────────────────────────────

console.log(`${BOLD}═══ Results ═══════════════════════════════════════════${RESET}\n`);

console.log(`${GREEN}✅  ${matched.length} matched${RESET}`);

console.log(`\n${RED}❌  ${missingFromIdx.length} in list but MISSING from index:${RESET}`);
if (missingFromIdx.length === 0) {
  console.log("    (none)");
} else {
  for (const e of missingFromIdx) {
    console.log(`    ${e.filename}`);
  }
}

console.log(`\n${YELLOW}⚠️   ${notInList.length} in index but NOT in list:${RESET}`);
if (notInList.length === 0) {
  console.log("    (none)");
} else {
  for (const e of notInList) {
    console.log(`    ${e.filename ?? e.id ?? "(unknown)"}`);
  }
}

console.log();
