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

// ─── Formatting ───────────────────────────────────────────────────────────────

const GREEN  = "\x1b[32m";
const YELLOW = "\x1b[33m";
const RED    = "\x1b[31m";
const BOLD   = "\x1b[1m";
const DIM    = "\x1b[2m";
const RESET  = "\x1b[0m";

const ok   = (s) => `${GREEN}✓${RESET} ${s}`;
const warn = (s) => `${YELLOW}⚠${RESET}  ${s}`;
const bad  = (s) => `${RED}✗${RESET} ${s}`;
const hdr  = (s) => `\n${BOLD}${s}${RESET}`;
const dim  = (s) => `${DIM}  ${s}${RESET}`;

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(hdr(`Barktown index validator`));
  console.log(dim(`${CFG.minio.useSSL ? "https" : "http"}://${CFG.minio.endPoint}:${CFG.minio.port}  bucket: ${CFG.bucket}`));

  // ── Fetch data ──────────────────────────────────────────────────────────────

  process.stdout.write("\nFetching data from MinIO…");
  const [index, audioObjs, waveformObjs] = await Promise.all([
    loadIndex(),
    listObjects(CFG.audioPrefix),
    listObjects(CFG.waveformPrefix),
  ]);
  console.log(` done.\n`);

  const audioFiles    = new Set(audioObjs.map(o => o.name));
  const waveformFiles = new Set(waveformObjs.map(o => o.name));

  console.log(dim(`index.json  : ${index.length} entries`));
  console.log(dim(`audio/      : ${audioFiles.size} files`));
  console.log(dim(`waveforms/  : ${waveformFiles.size} files`));

  // ── Index → bucket cross-check ──────────────────────────────────────────────

  console.log(hdr("Audio files"));

  const audioPathsInIndex  = index.map(e => e.audioPath).filter(Boolean);
  const audioIdxSet        = new Set(audioPathsInIndex);

  // Duplicate audioPath values within index
  const audioIdxDupes = audioPathsInIndex.filter((v, i, a) => a.indexOf(v) !== i);
  const missingAudio  = audioPathsInIndex.filter(p => !audioFiles.has(p));
  const unindexedAudio = [...audioFiles].filter(p => !audioIdxSet.has(p));

  if (audioIdxDupes.length === 0) {
    console.log(ok(`No duplicate audioPath entries in index`));
  } else {
    console.log(bad(`${audioIdxDupes.length} audioPath(s) appear more than once in index:`));
    for (const p of new Set(audioIdxDupes)) console.log(`    ${RED}${p}${RESET}`);
  }

  if (missingAudio.length === 0) {
    console.log(ok(`All indexed audio files exist in bucket`));
  } else {
    console.log(bad(`${missingAudio.length} indexed audio file(s) missing from bucket:`));
    for (const p of missingAudio) console.log(`    ${RED}${p}${RESET}`);
  }

  if (unindexedAudio.length === 0) {
    console.log(ok(`No unindexed audio files in bucket`));
  } else {
    console.log(warn(`${unindexedAudio.length} audio file(s) in bucket not in index:`));
    for (const p of unindexedAudio) console.log(`    ${YELLOW}${p}${RESET}`);
  }

  // ── Waveform cross-check ────────────────────────────────────────────────────

  console.log(hdr("Waveform files"));

  const wavePathsInIndex = index.map(e => e.waveformPath).filter(Boolean);
  const waveIdxSet       = new Set(wavePathsInIndex);

  const waveIdxDupes    = wavePathsInIndex.filter((v, i, a) => a.indexOf(v) !== i);
  const missingWave     = wavePathsInIndex.filter(p => !waveformFiles.has(p));
  const unindexedWave   = [...waveformFiles].filter(p => !waveIdxSet.has(p));

  // Entries that should have a waveform (kind=audio) but don't
  const missingWavePath = index.filter(e => e.kind === "audio" && !e.waveformPath);

  if (waveIdxDupes.length === 0) {
    console.log(ok(`No duplicate waveformPath entries in index`));
  } else {
    console.log(bad(`${waveIdxDupes.length} waveformPath(s) appear more than once in index:`));
    for (const p of new Set(waveIdxDupes)) console.log(`    ${RED}${p}${RESET}`);
  }

  if (missingWave.length === 0) {
    console.log(ok(`All indexed waveform files exist in bucket`));
  } else {
    console.log(bad(`${missingWave.length} indexed waveform file(s) missing from bucket:`));
    for (const p of missingWave) console.log(`    ${RED}${p}${RESET}`);
  }

  if (unindexedWave.length === 0) {
    console.log(ok(`No unindexed waveform files in bucket`));
  } else {
    console.log(warn(`${unindexedWave.length} waveform file(s) in bucket not in index:`));
    for (const p of unindexedWave) console.log(`    ${YELLOW}${p}${RESET}`);
  }

  if (missingWavePath.length === 0) {
    console.log(ok(`All audio-kind entries have a waveformPath`));
  } else {
    console.log(warn(`${missingWavePath.length} audio-kind entry/entries with no waveformPath in index:`));
    for (const e of missingWavePath) console.log(`    ${YELLOW}${e.audioPath}${RESET}`);
  }

  // ── Duplicate detection ─────────────────────────────────────────────────────

  console.log(hdr("Duplicate detection"));

  // Sort entries by datetimeLocal for proximity checks
  const sorted = [...index].sort((a, b) =>
    (a.datetimeLocal ?? "").localeCompare(b.datetimeLocal ?? "")
  );

  const exactGroups  = new Map(); // datetimeLocal → entries[]
  for (const e of sorted) {
    const key = e.datetimeLocal ?? "(missing)";
    if (!exactGroups.has(key)) exactGroups.set(key, []);
    exactGroups.get(key).push(e);
  }

  const exactDupes = [...exactGroups.values()].filter(g => g.length > 1);
  if (exactDupes.length === 0) {
    console.log(ok(`No entries with identical timestamps`));
  } else {
    console.log(bad(`${exactDupes.length} group(s) with identical timestamp:`));
    for (const group of exactDupes) {
      console.log(`    ${RED}${group[0].datetimeLocal}${RESET}`);
      for (const e of group) console.log(`      ${e.audioPath ?? e.id}`);
    }
  }

  // Within-1-minute groups (excluding exact duplicates already reported)
  const nearGroups = []; // groups of entries within 60 s of each other
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
      // Only report if not already an exact-duplicate group
      const allSameTime = group.every(e => e.datetimeLocal === group[0].datetimeLocal);
      if (!allSameTime) nearGroups.push(group);
    }
    i = j;
  }

  if (nearGroups.length === 0) {
    console.log(ok(`No entries within 1 minute of each other (beyond exact duplicates)`));
  } else {
    console.log(warn(`${nearGroups.length} group(s) of entries recorded within 1 minute of each other:`));
    for (const group of nearGroups) {
      console.log(`    ${YELLOW}${group[0].datetimeLocal} … ${group[group.length - 1].datetimeLocal}${RESET}`);
      for (const e of group) console.log(`      ${e.datetimeLocal}  ${e.audioPath ?? e.id}`);
    }
  }

  // ── Summary ─────────────────────────────────────────────────────────────────

  const issues =
    audioIdxDupes.length + missingAudio.length + unindexedAudio.length +
    waveIdxDupes.length  + missingWave.length  + unindexedWave.length  +
    missingWavePath.length + exactDupes.length;

  const warnings = nearGroups.length;

  console.log(hdr("Summary"));
  if (issues === 0 && warnings === 0) {
    console.log(ok(`Everything looks good — ${index.length} entries, no issues found.\n`));
  } else {
    if (issues > 0)   console.log(bad(`${issues} issue(s) found`));
    if (warnings > 0) console.log(warn(`${warnings} warning(s) (near-duplicate groups)`));
    console.log();
  }

  process.exit(issues > 0 ? 1 : 0);
}

main().catch(e => {
  console.error(`\n${RED}Fatal:${RESET}`, e.message);
  process.exit(1);
});
