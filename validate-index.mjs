#!/usr/bin/env node
/**
 * barktown — index validation script
 *
 * Usage:  npm run validate-index
 *
 * Reads .env for MinIO credentials, then:
 *   - fetches index.json
 *   - lists all objects under audio/ and waveforms/
 *   - cross-checks index vs actual files
 *   - reports duplicates (exact time and within 1 minute)
 */

import * as Minio from "minio";
import fs from "fs";
import path from "path";
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
  bucket:         process.env.MINIO_BUCKET ?? "barktown",
  audioPrefix:    "audio/",
  waveformPrefix: "waveforms/",
  indexKey:       "index.json",
};

const mc = new Minio.Client(CFG.minio);

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function listObjects(prefix) {
  return new Promise((resolve, reject) => {
    const objects = [];
    const stream  = mc.listObjectsV2(CFG.bucket, prefix, true);
    stream.on("data",  o  => objects.push(o));
    stream.on("end",   () => resolve(objects));
    stream.on("error", reject);
  });
}

async function loadIndex() {
  try {
    const stream = await mc.getObject(CFG.bucket, CFG.indexKey);
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch (e) {
    if (e.code === "NoSuchKey") return [];
    throw e;
  }
}

// ─── Output (tee: console + plain-text log) ───────────────────────────────────

const GREEN  = "\x1b[32m";
const YELLOW = "\x1b[33m";
const RED    = "\x1b[31m";
const BOLD   = "\x1b[1m";
const DIM    = "\x1b[2m";
const RESET  = "\x1b[0m";

const logLines = [];

/** Print one ANSI line to console and one plain line to the log buffer. */
function tee(ansiLine, plainLine) {
  console.log(ansiLine);
  logLines.push(plainLine ?? ansiLine);
}

const ok   = (s) => tee(`${GREEN}✓${RESET} ${s}`,  `✅ ${s}`);
const warn = (s) => tee(`${YELLOW}⚠${RESET}  ${s}`, `⚠️  ${s}`);
const bad  = (s) => tee(`${RED}✗${RESET} ${s}`,    `❌ ${s}`);
const hdr  = (s) => tee(`\n${BOLD}${s}${RESET}`,   `\n─── ${s} ───`);
const dim  = (s) => tee(`${DIM}  ${s}${RESET}`,    `   ${s}`);
const detail = (s) => {
  console.log(s);
  // Strip ANSI escape codes for the plain-text log
  logLines.push(s.replace(/\x1b\[[0-9;]*m/g, ""));
};

const __logsDir = path.join(__dirname, "logs");

function writeLog() {
  if (!fs.existsSync(__logsDir)) fs.mkdirSync(__logsDir);
  const ts      = new Date().toISOString().replace(/:/g, "-").replace("T", "_").slice(0, 19);
  const logFile = path.join(__logsDir, `validate-${ts}.txt`);
  fs.writeFileSync(logFile, logLines.join("\n") + "\n", "utf8");
  console.log(`\n${DIM}Log written → ${logFile}${RESET}`);
  return logFile;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  hdr(`Barktown index validator`);
  dim(`${CFG.minio.useSSL ? "https" : "http"}://${CFG.minio.endPoint}:${CFG.minio.port}  bucket: ${CFG.bucket}`);

  // ── Fetch data ──────────────────────────────────────────────────────────────

  process.stdout.write("\nFetching data from MinIO…");
  const [index, audioObjs, waveformObjs] = await Promise.all([
    loadIndex(),
    listObjects(CFG.audioPrefix),
    listObjects(CFG.waveformPrefix),
  ]);
  console.log(` done.\n`);
  logLines.push("");

  const audioFiles    = new Set(audioObjs.filter(o => !o.name.endsWith("/")).map(o => o.name));
  const waveformFiles = new Set(waveformObjs.filter(o => !o.name.endsWith("/")).map(o => o.name));

  dim(`index.json  : ${index.length} entries`);
  dim(`audio/      : ${audioFiles.size} files`);
  dim(`waveforms/  : ${waveformFiles.size} files`);
  // ── Index → bucket cross-check ──────────────────────────────────────────────

  hdr("Audio files");

  const audioPathsInIndex  = index.map(e => e.audioPath).filter(Boolean);
  const audioIdxSet        = new Set(audioPathsInIndex);

  const audioIdxDupes  = audioPathsInIndex.filter((v, i, a) => a.indexOf(v) !== i);
  const missingAudio   = audioPathsInIndex.filter(p => !audioFiles.has(p));
  const unindexedAudio = [...audioFiles].filter(p => !audioIdxSet.has(p));

  if (audioIdxDupes.length === 0) {
    ok(`No duplicate audioPath entries in index`);
  } else {
    bad(`${audioIdxDupes.length} audioPath(s) appear more than once in index:`);
    for (const p of new Set(audioIdxDupes)) detail(`    ${RED}${p}${RESET}`);
  }

  if (missingAudio.length === 0) {
    ok(`All indexed audio files exist in bucket`);
  } else {
    bad(`${missingAudio.length} indexed audio file(s) missing from bucket:`);
    for (const p of missingAudio) detail(`    ${RED}${p}${RESET}`);
  }

  if (unindexedAudio.length === 0) {
    ok(`No unindexed audio files in bucket`);
  } else {
    warn(`${unindexedAudio.length} audio file(s) in bucket not in index:`);
    for (const p of unindexedAudio) detail(`    ${YELLOW}${p}${RESET}`);
  }

  // ── Waveform cross-check ────────────────────────────────────────────────────

  hdr("Waveform files");

  const wavePathsInIndex = index.map(e => e.waveformPath).filter(Boolean);
  const waveIdxSet       = new Set(wavePathsInIndex);

  const waveIdxDupes   = wavePathsInIndex.filter((v, i, a) => a.indexOf(v) !== i);
  const missingWave    = wavePathsInIndex.filter(p => !waveformFiles.has(p));
  const unindexedWave  = [...waveformFiles].filter(p => !waveIdxSet.has(p));
  const missingWavePath = index.filter(e => e.kind === "audio" && !e.waveformPath);

  if (waveIdxDupes.length === 0) {
    ok(`No duplicate waveformPath entries in index`);
  } else {
    bad(`${waveIdxDupes.length} waveformPath(s) appear more than once in index:`);
    for (const p of new Set(waveIdxDupes)) detail(`    ${RED}${p}${RESET}`);
  }

  if (missingWave.length === 0) {
    ok(`All indexed waveform files exist in bucket`);
  } else {
    bad(`${missingWave.length} indexed waveform file(s) missing from bucket:`);
    for (const p of missingWave) detail(`    ${RED}${p}${RESET}`);
  }

  if (unindexedWave.length === 0) {
    ok(`No unindexed waveform files in bucket`);
  } else {
    warn(`${unindexedWave.length} waveform file(s) in bucket not in index:`);
    for (const p of unindexedWave) detail(`    ${YELLOW}${p}${RESET}`);
  }

  if (missingWavePath.length === 0) {
    ok(`All audio-kind entries have a waveformPath`);
  } else {
    warn(`${missingWavePath.length} audio-kind entry/entries with no waveformPath in index:`);
    for (const e of missingWavePath) detail(`    ${YELLOW}${e.audioPath}${RESET}`);
  }

  // ── Duplicate detection ─────────────────────────────────────────────────────

  hdr("Duplicate detection");

  const sorted = [...index].sort((a, b) =>
    (a.datetimeLocal ?? "").localeCompare(b.datetimeLocal ?? "")
  );

  const exactGroups = new Map();
  for (const e of sorted) {
    const key = e.datetimeLocal ?? "(missing)";
    if (!exactGroups.has(key)) exactGroups.set(key, []);
    exactGroups.get(key).push(e);
  }

  const exactDupes = [...exactGroups.values()].filter(g => g.length > 1);
  if (exactDupes.length === 0) {
    ok(`No entries with identical timestamps`);
  } else {
    bad(`${exactDupes.length} group(s) with identical timestamp:`);
    for (const group of exactDupes) {
      detail(`    ${RED}${group[0].datetimeLocal}${RESET}`);
      for (const e of group) detail(`      ${e.audioPath ?? e.id}`);
    }
  }

  const nearGroups = [];
  let i = 0;
  while (i < sorted.length) {
    const anchor = new Date(sorted[i].datetimeLocal ?? 0).getTime();
    let j = i + 1;
    while (
      j < sorted.length &&
      new Date(sorted[j].datetimeLocal ?? 0).getTime() - anchor < 60_000
    ) j++;
    if (j - i > 1) {
      const group = sorted.slice(i, j);
      const allSameTime = group.every(e => e.datetimeLocal === group[0].datetimeLocal);
      if (!allSameTime) nearGroups.push(group);
    }
    i = j;
  }

  if (nearGroups.length === 0) {
    ok(`No entries within 1 minute of each other (beyond exact duplicates)`);
  } else {
    warn(`${nearGroups.length} group(s) of entries recorded within 1 minute of each other:`);
    for (const group of nearGroups) {
      detail(`    ${YELLOW}${group[0].datetimeLocal} … ${group[group.length - 1].datetimeLocal}${RESET}`);
      for (const e of group) detail(`      ${e.datetimeLocal}  ${e.audioPath ?? e.id}`);
    }
  }

  // ── Summary ─────────────────────────────────────────────────────────────────

  const issues =
    audioIdxDupes.length + missingAudio.length + unindexedAudio.length +
    waveIdxDupes.length  + missingWave.length  + unindexedWave.length  +
    missingWavePath.length + exactDupes.length;

  const warnings = nearGroups.length;

  hdr("Summary");
  if (issues === 0 && warnings === 0) {
    ok(`Everything looks good — ${index.length} entries, no issues found.`);
  } else {
    if (issues > 0)   bad(`${issues} issue(s) found`);
    if (warnings > 0) warn(`${warnings} warning(s) (near-duplicate groups)`);
  }
  detail("");

  writeLog();
  process.exit(issues > 0 ? 1 : 0);
}

main().catch(e => {
  console.error(`\n${RED}Fatal:${RESET}`, e.message);
  process.exit(1);
});