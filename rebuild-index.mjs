#!/usr/bin/env node
/**
 * barktown — index rebuild script
 *
 * Full rebuild (default):
 *   npm run rebuild-index
 *   node rebuild-index.mjs
 *
 * Partial rebuild (upsert specific files only):
 *   npm run rebuild-index -- audio-list.txt
 *   node rebuild-index.mjs audio-list.txt
 *
 * The optional txt file should contain one MinIO audio path per line, e.g.:
 *   audio/2022/01/2022-01-03 18-15-00 barks when neighbors come home.aac
 *   audio/2022/02/2022-02-08 18-12-00 bsrk bark.aac
 *
 * Full rebuild: lists /audio/, downloads every file, overwrites index.json.
 * Partial rebuild: processes only the listed files, upserts their entries
 *   into the existing index.json (adding new ones, replacing existing ones)
 *   and leaves all other entries untouched.
 */

import * as Minio from "minio";
import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";
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
  bucket:            process.env.MINIO_BUCKET            ?? "barktown",
  audioPrefix:       "audio/",
  waveformPrefix:    "waveforms/",
  indexKey:          "index.json",
  ffprobeBin:        process.env.FFPROBE_BIN               ?? "ffprobe",
  waveformThreshSec: parseFloat(process.env.WAVEFORM_THRESHOLD_SEC ?? "5"),
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

// ─── Audio helpers ────────────────────────────────────────────────────────────

function getDuration(filePath) {
  const r = spawnSync(
    CFG.ffprobeBin,
    ["-v", "quiet", "-print_format", "json", "-show_format", filePath],
    { encoding: "utf8" }
  );
  if (r.error || r.status !== 0) return 0;
  try {
    const data = JSON.parse(r.stdout);
    return parseFloat(data.format?.duration ?? "0");
  } catch { return 0; }
}

/** Returns true if objectKey exists in the bucket. */
async function objectExists(objectKey) {
  try {
    await mc.statObject(CFG.bucket, objectKey);
    return true;
  } catch { return false; }
}

/** Load index.json from bucket. Returns [] if missing. */
async function loadIndex() {
  try {
    const stream = await mc.getObject(CFG.bucket, CFG.indexKey);
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch (e) {
    if (e.code === "NoSuchKey" || e.code === "NotFound") return [];
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

const log  = (...a) => console.log(...a);
const ok   = (s)   => console.log(`${GREEN}✓${RESET} ${s}`);
const warn = (s)   => console.warn(`${YELLOW}⚠${RESET}  ${s}`);
const err  = (s)   => console.error(`${RED}✗${RESET} ${s}`);
const hdr  = (s)   => console.log(`\n${BOLD}${s}${RESET}`);
const dim  = (s)   => console.log(`${DIM}  ${s}${RESET}`);

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const partialListFile = process.argv[2] ?? null;
  const isPartial = partialListFile !== null;

  hdr(isPartial ? "Barktown index partial rebuild" : "Barktown index rebuild");
  dim(`${CFG.minio.useSSL ? "https" : "http"}://${CFG.minio.endPoint}:${CFG.minio.port}  bucket: ${CFG.bucket}`);
  if (isPartial) dim(`List file: ${path.resolve(partialListFile)}`);

  // ── Determine which audio files to process ──────────────────────────────────

  /** @type {{ name: string, size: number }[]} */
  let audioFiles;

  if (isPartial) {
    // Parse the txt file: one audio object key per line, leading spaces stripped.
    const absPath = path.resolve(partialListFile);
    if (!fs.existsSync(absPath)) {
      err(`File not found: ${absPath}`);
      process.exit(1);
    }
    audioFiles = fs.readFileSync(absPath, "utf8")
      .split("\n")
      .map(l => l.trim())
      .filter(l => l && !l.startsWith("#"))
      .map(l => ({ name: l, size: 1 /* placeholder — will be confirmed by download */ }));
    dim(`\n${audioFiles.length} files to process from list.`);
  } else {
    process.stdout.write("\nListing audio/ and waveforms/…");
    const [audioObjs, waveformObjs] = await Promise.all([
      listObjects(CFG.audioPrefix),
      listObjects(CFG.waveformPrefix),
    ]);
    console.log(" done.\n");
    audioFiles = audioObjs.filter(o => !o.name.endsWith("/") && o.size > 0);
    // Store waveform set for full rebuild (orphan detection).
    main._waveformKeys = new Set(waveformObjs.filter(o => !o.name.endsWith("/")).map(o => o.name));
    dim(`audio files   : ${audioFiles.length}`);
    dim(`waveform files: ${main._waveformKeys.size}`);
  }

  const waveformKeys  = main._waveformKeys ?? null; // only populated in full mode

  // ── Load existing index (needed for partial; optionally for full) ────────────

  let existingEntries = [];
  if (isPartial) {
    process.stdout.write("\nLoading existing index.json…");
    existingEntries = await loadIndex();
    console.log(` ${existingEntries.length} entries.`);
  }

  // ── Build index entries ─────────────────────────────────────────────────────

  hdr("Processing audio files");
  dim(`Downloading each file to measure duration (ffprobe)…`);

  const newEntries    = [];
  const skipped       = [];
  const usedWaveforms = new Set();
  let   n             = 0;

  for (const obj of audioFiles) {
    n++;
    const filename = path.basename(obj.name);
    const parsed   = parseFilename(filename);

    if (!parsed) {
      warn(`Skipping unrecognised filename: ${obj.name}`);
      skipped.push(obj.name);
      continue;
    }

    const { date, time, datetimeLocal, label, id } = parsed;

    const audioDir    = path.dirname(obj.name);
    const relDir      = audioDir.slice(CFG.audioPrefix.length);
    const waveformKey = `${CFG.waveformPrefix}${relDir}/${id}.json`;

    // Full rebuild: check waveformKeys set.  Partial: stat the object.
    const hasWaveform = waveformKeys
      ? waveformKeys.has(waveformKey)
      : await objectExists(waveformKey);
    if (hasWaveform) usedWaveforms.add(waveformKey);

    // Download to temp dir and measure duration.
    const tmpDir   = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-rebuild-"));
    const tmpAudio = path.join(tmpDir, filename);
    try {
      await mc.fGetObject(CFG.bucket, obj.name, tmpAudio);
      const durationSec = getDuration(tmpAudio);
      const kind =
        durationSec < CFG.waveformThreshSec ? "note"
        : "audio";

      newEntries.push({
        id,
        filename,
        audioPath:    obj.name,
        waveformPath: hasWaveform ? waveformKey : null,
        date,
        time,
        datetimeLocal,
        label,
        durationSec: parseFloat(durationSec.toFixed(3)),
        kind,
      });

      const kindTag =
        kind === "audio" ? `${GREEN}audio${RESET}` :
                           `${DIM}note ${RESET}`;
      process.stdout.write(`\r  [${n}/${audioFiles.length}] ${kindTag}  ${durationSec.toFixed(1)}s  ${filename}                `);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  }
  console.log(); // newline after progress line

  // ── Orphan waveforms (full rebuild only) ────────────────────────────────────

  let orphanWaveforms = [];
  if (waveformKeys) {
    orphanWaveforms = [...waveformKeys].filter(k => !usedWaveforms.has(k));
    if (orphanWaveforms.length > 0) {
      hdr("Orphan waveforms (no matching audio file)");
      for (const k of orphanWaveforms) warn(k);
    }
  }

  // ── Merge + sort + write ────────────────────────────────────────────────────

  let finalEntries;
  if (isPartial) {
    // Upsert: replace existing entries by id, append new ones.
    const byId = new Map(existingEntries.map(e => [e.id, e]));
    let updated = 0, added = 0;
    for (const e of newEntries) {
      if (byId.has(e.id)) { updated++; } else { added++; }
      byId.set(e.id, e);
    }
    finalEntries = [...byId.values()];
    dim(`\nUpserted: ${updated} updated, ${added} added.`);
  } else {
    finalEntries = newEntries;
  }

  finalEntries.sort((a, b) => a.datetimeLocal.localeCompare(b.datetimeLocal));

  hdr("Writing index.json");
  const json = JSON.stringify(finalEntries, null, 2) + "\n";
  await uploadBuffer(json, CFG.indexKey);

  // ── Summary ─────────────────────────────────────────────────────────────────

  hdr("Summary");
  ok(`${finalEntries.length} entries in index.json`);
  const audioCount = finalEntries.filter(e => e.kind === "audio").length;
  const noteCount  = finalEntries.filter(e => e.kind === "note").length;
  dim(`audio : ${audioCount}`);
  dim(`note  : ${noteCount}`);
  if (skipped.length > 0) {
    warn(`${skipped.length} file(s) skipped (unrecognised filename pattern):`);
    for (const s of skipped) console.log(`    ${s}`);
  }
  if (orphanWaveforms.length > 0) {
    warn(`${orphanWaveforms.length} orphan waveform file(s) not referenced by any entry`);
  }
  console.log();
}

main._waveformKeys = null;

main().catch(e => {
  console.error(`\n${RED}Fatal:${RESET}`, e.message);
  process.exit(1);
});
