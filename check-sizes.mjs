#!/usr/bin/env node
/**
 * barktown — file size checker
 *
 * Usage:  node check-sizes.mjs <list.txt>
 *         npm run check-sizes -- list.txt
 *
 * Input format (one file per line):
 *   <size with space-separated groups> B -- <bucket-path>
 *   e.g.  17 185 115 B -- /audio/2021/05/2021-05-25 15-44-00 bjäfsigt.aac
 *
 * Checks the size of every listed file in the MinIO bucket and reports:
 *   - exact matches
 *   - size differences bucketed by magnitude (1B, 10B, 100B … 1GB)
 *   - files missing from the bucket
 *
 * Fetches metadata only (statObject) — no files are downloaded.
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
  bucket: process.env.MINIO_BUCKET ?? "barktown",
  /** How many statObject calls to run in parallel. */
  concurrency: 32,
};

// ─── ANSI colours ─────────────────────────────────────────────────────────────

const GREEN  = "\x1b[32m";
const RED    = "\x1b[31m";
const YELLOW = "\x1b[33m";
const CYAN   = "\x1b[36m";
const RESET  = "\x1b[0m";
const BOLD   = "\x1b[1m";
const DIM    = "\x1b[2m";

// ─── Parse list file ──────────────────────────────────────────────────────────

// e.g. "     17 185 115 B -- /audio/2021/05/2021-05-25 15-44-00 bjäfsigt.aac"
const LINE_RE = /^([\d\s]+)\s+B\s+--\s+(.+)$/;

/**
 * @returns {{ listedSize: number, objectKey: string, originalLine: string } | null}
 */
function parseLine(line) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) return null;

  const m = LINE_RE.exec(trimmed);
  if (!m) return null;

  // "17 185 115" → 17185115
  const listedSize = parseInt(m[1].replace(/\s+/g, ""), 10);
  // "/audio/2022/04/file.aac" → "audio/2022/04/file.aac"  (strip leading /)
  const objectKey  = m[2].trim().replace(/^\//, "");

  return { listedSize, objectKey, originalLine: trimmed };
}

// ─── Size diff bucketing ──────────────────────────────────────────────────────

const BUCKETS = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 1_000_000_000];
const BUCKET_LABELS = [
  "exact  (0 B)",
  "≤     9 B",
  "≤    99 B",
  "≤   999 B",
  "≤  9 999 B",
  "≤ 99 999 B",
  "≤ 999 999 B",
  "≤ 999 999 999 B",
  ">  1 000 000 000 B",
];

function bucketIndex(diff) {
  if (diff === 0) return 0;
  for (let i = 1; i < BUCKETS.length; i++) {
    if (diff < BUCKETS[i]) return i;
  }
  return BUCKETS.length - 1;
}

// ─── MinIO helper ─────────────────────────────────────────────────────────────

const mc = new Minio.Client(CFG.minio);

async function statSize(objectKey) {
  try {
    const stat = await mc.statObject(CFG.bucket, objectKey);
    return stat.size;
  } catch (e) {
    if (e.code === "NotFound" || e.code === "NoSuchKey" || e.message?.includes("Not Found")) {
      return null; // missing
    }
    throw e;
  }
}

/** Run an array of async tasks with bounded concurrency. */
async function pool(tasks, concurrency) {
  const results = new Array(tasks.length);
  let next = 0;
  async function worker() {
    while (next < tasks.length) {
      const i = next++;
      results[i] = await tasks[i]();
    }
  }
  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, worker);
  await Promise.all(workers);
  return results;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const listFile = process.argv[2];
if (!listFile) {
  console.error("Usage: node check-sizes.mjs <list.txt>");
  process.exit(1);
}

const absListFile = path.resolve(listFile);
if (!fs.existsSync(absListFile)) {
  console.error(`File not found: ${absListFile}`);
  process.exit(1);
}

const rawLines = fs.readFileSync(absListFile, "utf8").split("\n");
const entries  = rawLines
  .map((line, i) => ({ parsed: parseLine(line), lineNo: i + 1 }))
  .filter(({ parsed }) => parsed !== null)
  .map(({ parsed }) => parsed);

const skipped = rawLines.filter(l => l.trim() && !parseLine(l)).length;

console.log(`\nList file  : ${absListFile}`);
console.log(`Entries    : ${entries.length}${skipped ? `  (${skipped} unrecognised lines skipped)` : ""}`);
console.log(`Bucket     : ${CFG.bucket}  (${CFG.minio.endPoint}:${CFG.minio.port})`);
console.log(`Concurrency: ${CFG.concurrency}\n`);

// ─── Run checks ───────────────────────────────────────────────────────────────

const diffBuckets  = new Array(BUCKET_LABELS.length).fill(0);
const missingFiles = [];
const diffFiles    = []; // { objectKey, listedSize, actualSize, diff }

let done = 0;
const total = entries.length;

const tasks = entries.map(entry => async () => {
  const actual = await statSize(entry.objectKey);
  done++;
  if (done % 100 === 0 || done === total) {
    process.stdout.write(`\r  Checking: ${done}/${total} …    `);
  }

  if (actual === null) {
    missingFiles.push(entry);
    return;
  }

  const diff = Math.abs(actual - entry.listedSize);
  diffBuckets[bucketIndex(diff)]++;
  if (diff > 0) {
    diffFiles.push({ objectKey: entry.objectKey, listedSize: entry.listedSize, actualSize: actual, diff });
  }
});

await pool(tasks, CFG.concurrency);
process.stdout.write("\r" + " ".repeat(40) + "\r"); // clear progress line

// ─── Report ───────────────────────────────────────────────────────────────────

const fmt = n => n.toLocaleString("sv-SE"); // Swedish locale: space as thousands sep

console.log(`${BOLD}═══ Results ═══════════════════════════════════════════${RESET}\n`);

console.log(`${BOLD}Size match distribution:${RESET}`);
for (let i = 0; i < BUCKET_LABELS.length; i++) {
  const count = diffBuckets[i];
  if (count === 0) continue;
  const colour = i === 0 ? GREEN : i <= 2 ? YELLOW : RED;
  console.log(`  ${colour}${BUCKET_LABELS[i].padEnd(24)}${RESET}  ${String(count).padStart(6)}`);
}

console.log();
console.log(`${RED}Missing from bucket    :  ${missingFiles.length}${RESET}`);
if (missingFiles.length > 0) {
  console.log(`  ${DIM}(listed below)${RESET}`);
}

const exact = diffBuckets[0];
console.log(`${GREEN}Exact matches          :  ${exact} / ${total}${RESET}`);

// ─── Detail: differing files ──────────────────────────────────────────────────

if (diffFiles.length > 0) {
  diffFiles.sort((a, b) => b.diff - a.diff);
  console.log(`\n${BOLD}${YELLOW}Files with size mismatch (${diffFiles.length}):${RESET}`);
  for (const f of diffFiles) {
    console.log(
      `  diff ${fmt(f.diff).padStart(14)} B   listed ${fmt(f.listedSize).padStart(15)} B   actual ${fmt(f.actualSize).padStart(15)} B   ${DIM}${f.objectKey}${RESET}`
    );
  }
}

// ─── Detail: missing files ────────────────────────────────────────────────────

if (missingFiles.length > 0) {
  console.log(`\n${BOLD}${RED}Missing files (${missingFiles.length}):${RESET}`);
  for (const f of missingFiles) {
    console.log(`  ${fmt(f.listedSize).padStart(15)} B   ${f.objectKey}`);
  }
}

console.log();
