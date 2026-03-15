#!/usr/bin/env node
/**
 * barktown — index rebuild script
 *
 * Usage:  npm run rebuild-index
 *
 * Takes /audio as the source of truth.
 * For each audio file:
 *   - parses the filename to extract metadata
 *   - checks for a matching waveform in /waveforms
 *   - determines kind: 'audio' if waveform exists, 'note' otherwise
 *
 * Overwrites index.json with the rebuilt index, sorted by datetimeLocal.
 *
 * NOTE: durationSec will be null for all rebuilt entries (would require
 *       downloading every file to measure).  Re-ingest to recover it.
 */

import * as Minio from "minio";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Readable } from "stream";

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

// ─── Filename pattern (must match ingest-service.mjs) ────────────────────────

const FILENAME_RE =
  /^(\d{4}-\d{2}-\d{2}) (\d{2})-(\d{2})-(\d{2})(?:\s+(\S.*?))?\.(m4a|aac)$/i;

function parseFilename(filename) {
  const match = FILENAME_RE.exec(filename);
  if (!match) return null;

  const [, datePart, hh, mm, ss, rawLabel] = match;
  const label         = rawLabel ? rawLabel.trim() : "";
  const date          = datePart;
  const time          = `${hh}:${mm}`;
  const datetimeLocal = `${date}T${hh}:${mm}:${ss}`;

  const ext  = filename.match(/\.(m4a|aac)$/i)[0];
  const stem = filename.slice(0, -ext.length);
  const id   = stem
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/, "");

  return { date, time, datetimeLocal, label, id };
}

// ─── MinIO helpers ────────────────────────────────────────────────────────────

async function listObjects(prefix) {
  return new Promise((resolve, reject) => {
    const objects = [];
    const stream  = mc.listObjectsV2(CFG.bucket, prefix, true);
    stream.on("data",  o  => objects.push(o));
    stream.on("end",   () => resolve(objects));
    stream.on("error", reject);
  });
}

async function uploadBuffer(data, objectKey, contentType = "application/json") {
  const buf    = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  const stream = Readable.from(buf);
  await mc.putObject(CFG.bucket, objectKey, stream, buf.length, { "Content-Type": contentType });
}

// ─── Formatting ───────────────────────────────────────────────────────────────

const GREEN  = "\x1b[32m";
const YELLOW = "\x1b[33m";
const RED    = "\x1b[31m";
const BOLD   = "\x1b[1m";
const DIM    = "\x1b[2m";
const RESET  = "\x1b[0m";

const log  = (...a) => console.log(...a);
const ok   = (s)   => console.log(`${GREEN}✓${RESET} ${s}`);
const warn = (s)   => console.warn(`${YELLOW}⚠${RESET}  ${s}`);
const err  = (s)   => console.error(`${RED}✗${RESET} ${s}`);
const hdr  = (s)   => console.log(`\n${BOLD}${s}${RESET}`);
const dim  = (s)   => console.log(`${DIM}  ${s}${RESET}`);

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  hdr("Barktown index rebuild");
  dim(`${CFG.minio.useSSL ? "https" : "http"}://${CFG.minio.endPoint}:${CFG.minio.port}  bucket: ${CFG.bucket}`);

  // ── List bucket ─────────────────────────────────────────────────────────────

  process.stdout.write("\nListing audio/ and waveforms/…");
  const [audioObjs, waveformObjs] = await Promise.all([
    listObjects(CFG.audioPrefix),
    listObjects(CFG.waveformPrefix),
  ]);
  console.log(" done.\n");

  const audioFiles    = audioObjs.filter(o => !o.name.endsWith("/") && o.size > 0);
  const waveformKeys  = new Set(waveformObjs.map(o => o.name));

  dim(`audio files   : ${audioFiles.length}`);
  dim(`waveform files: ${waveformKeys.size}`);

  // ── Build index entries ─────────────────────────────────────────────────────

  hdr("Processing audio files");

  const entries    = [];
  const skipped    = [];
  const usedWaveforms = new Set();

  for (const obj of audioFiles) {
    const filename = path.basename(obj.name);
    const parsed   = parseFilename(filename);

    if (!parsed) {
      warn(`Skipping unrecognised filename: ${obj.name}`);
      skipped.push(obj.name);
      continue;
    }

    const { date, time, datetimeLocal, label, id } = parsed;

    // Expected waveform path mirrors the audio path structure:
    //   audio/YYYY/MM/filename.ext  →  waveforms/YYYY/MM/id.json
    const audioDir    = path.dirname(obj.name);                          // e.g. audio/2026/01
    const relDir      = audioDir.slice(CFG.audioPrefix.length);          // e.g. 2026/01
    const waveformKey = `${CFG.waveformPrefix}${relDir}/${id}.json`;

    const hasWaveform = waveformKeys.has(waveformKey);
    const kind        = hasWaveform ? "audio" : "note";
    if (hasWaveform) usedWaveforms.add(waveformKey);

    entries.push({
      id,
      filename,
      audioPath:    obj.name,
      waveformPath: hasWaveform ? waveformKey : null,
      date,
      time,
      datetimeLocal,
      label,
      durationSec:  null,   // not available without downloading
      kind,
    });

    const kindTag = hasWaveform
      ? `${GREEN}audio${RESET}`
      : `${DIM}note${RESET}`;
    log(`  ${kindTag}  ${obj.name}`);
  }

  // ── Orphan waveforms ────────────────────────────────────────────────────────

  const orphanWaveforms = [...waveformKeys].filter(k => !usedWaveforms.has(k));
  if (orphanWaveforms.length > 0) {
    hdr("Orphan waveforms (no matching audio file)");
    for (const k of orphanWaveforms) warn(k);
  }

  // ── Sort + write ────────────────────────────────────────────────────────────

  entries.sort((a, b) => a.datetimeLocal.localeCompare(b.datetimeLocal));

  hdr("Writing index.json");
  const json = JSON.stringify(entries, null, 2) + "\n";
  await uploadBuffer(json, CFG.indexKey);

  // ── Summary ─────────────────────────────────────────────────────────────────

  hdr("Summary");
  ok(`${entries.length} entries written to index.json`);
  const audioCount = entries.filter(e => e.kind === "audio").length;
  const noteCount  = entries.filter(e => e.kind === "note").length;
  dim(`audio : ${audioCount}`);
  dim(`note  : ${noteCount}`);
  if (skipped.length > 0) {
    warn(`${skipped.length} file(s) skipped (unrecognised filename pattern):`);
    for (const s of skipped) console.log(`    ${s}`);
  }
  if (orphanWaveforms.length > 0) {
    warn(`${orphanWaveforms.length} orphan waveform file(s) not referenced by any entry`);
  }
  if (noteCount > 0) {
    dim(`note: durationSec is null for all rebuilt entries — re-ingest to recover`);
  }
  console.log();
}

main().catch(e => {
  console.error(`\n${RED}Fatal:${RESET}`, e.message);
  process.exit(1);
});
