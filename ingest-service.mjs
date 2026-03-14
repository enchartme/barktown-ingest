#!/usr/bin/env node
/**
 * barktown — MinIO ingest service
 *
 * Watches the `<BUCKET>/new/` prefix for freshly uploaded .m4a / .aac files.
 * For each stable, correctly-named file it:
 *   1. Validates the filename pattern  YYYY-MM-DD HH-MM-SS optional comment.ext
 *   2. Downloads the file to a temp directory
 *   3. Reads duration with ffprobe
 *   4. Generates a waveform JSON with audiowaveform (skipped for very short clips)
 *   5. Uploads waveform to  <BUCKET>/waveforms/YYYY/MM/<id>.json
 *   6. Copies audio to      <BUCKET>/audio/YYYY/MM/<filename>
 *   7. Removes it from      <BUCKET>/new/<filename>
 *   8. Appends the entry to <BUCKET>/index.json
 *
 * Files whose names don't match the pattern are left in /new/ untouched.
 *
 * ─── Configuration ────────────────────────────────────────────────────────────
 *
 * All settings can be overridden with environment variables.
 *
 *  MINIO_ENDPOINT          MinIO host                  (default: localhost)
 *  MINIO_PORT              MinIO port                  (default: 9000)
 *  MINIO_USE_SSL           Use HTTPS?  true/false      (default: false)
 *  MINIO_ACCESS_KEY        Access key                  (default: minioadmin)
 *  MINIO_SECRET_KEY        Secret key                  (default: minioadmin)
 *  MINIO_BUCKET            Bucket name                 (default: barktown)
 *
 *  POLL_INTERVAL_MS        How often to scan /new/     (default: 20000)
 *  STABILITY_DELAY_MS      Idle time before processing (default: 30000)
 *
 *  FFPROBE_BIN             ffprobe binary              (default: ffprobe)
 *  AUDIOWAVEFORM_BIN       audiowaveform binary        (default: audiowaveform)
 *
 *  WAVEFORM_THRESHOLD_SEC  Min duration for waveform   (default: 5)
 *
 * ─── Running ──────────────────────────────────────────────────────────────────
 *
 *   node ingest-service.mjs
 *   npm start
 *
 * As a systemd service, copy barktown-ingest.service to /etc/systemd/system/
 * and edit the Environment lines before enabling it.
 */

import * as Minio from "minio";
import { spawnSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import { Readable } from "stream";

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
  newPrefix:         "new/",
  audioPrefix:       "audio/",
  waveformPrefix:    "waveforms/",
  indexKey:          "index.json",
  pollIntervalMs:    parseInt(process.env.POLL_INTERVAL_MS   ?? "20000", 10),
  stabilityDelayMs:  parseInt(process.env.STABILITY_DELAY_MS ?? "30000", 10),
  ffprobeBin:        process.env.FFPROBE_BIN        ?? "ffprobe",
  audiowaveformBin:  process.env.AUDIOWAVEFORM_BIN  ?? "audiowaveform",
  waveformThreshSec: parseFloat(process.env.WAVEFORM_THRESHOLD_SEC ?? "5"),
};

// ─── Filename pattern ─────────────────────────────────────────────────────────
//
//   YYYY-MM-DD HH-MM-SS optional comment.(m4a|aac)
//
//   No trailing space between end-of-comment and .ext.
//   Examples:
//     2026-01-17 15-42-00 bark bark bark shot.m4a
//     2025-12-11 05-32-00.aac

const FILENAME_RE =
  /^(\d{4}-\d{2}-\d{2}) (\d{2})-(\d{2})-(\d{2})(?:\s+(\S.*?))?\.(m4a|aac)$/i;

function parseFilename(filename) {
  const match = FILENAME_RE.exec(filename);
  if (!match) return null;

  const [, datePart, hh, mm, ss, rawLabel] = match;
  const label          = rawLabel ? rawLabel.trim() : "";
  const date           = datePart;
  const time           = `${hh}:${mm}`;
  const datetimeLocal  = `${date}T${hh}:${mm}:${ss}`;

  // Slug: "2026-01-17 15-42-00 bark shot" → "2026-01-17_15-42-00_bark_shot"
  const ext  = filename.match(/\.(m4a|aac)$/i)[0];
  const stem = filename.slice(0, -ext.length);
  const id   = stem
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/, "");

  return { date, time, datetimeLocal, label, id };
}

// ─── Logging ──────────────────────────────────────────────────────────────────

function ts()  { return new Date().toISOString(); }
function log(...a) { console.log(`[${ts()}]`, ...a); }
function warn(...a) { console.warn(`[${ts()}] WARN`, ...a); }
function err(...a) { console.error(`[${ts()}] ERROR`, ...a); }

// ─── MinIO helpers ────────────────────────────────────────────────────────────

const mc = new Minio.Client(CFG.minio);

/** List all objects under a prefix. */
async function listObjects(prefix) {
  return new Promise((resolve, reject) => {
    const objects = [];
    const stream  = mc.listObjectsV2(CFG.bucket, prefix, true);
    stream.on("data",  o  => objects.push(o));
    stream.on("end",   () => resolve(objects));
    stream.on("error", reject);
  });
}

/** Download an object to a local file path. */
async function download(objectKey, destPath) {
  await mc.fGetObject(CFG.bucket, objectKey, destPath);
}

/** Upload a local file to an object key. */
async function upload(srcPath, objectKey, contentType = "application/octet-stream") {
  await mc.fPutObject(CFG.bucket, objectKey, srcPath, { "Content-Type": contentType });
}

/** Upload a Buffer / string as an object. */
async function uploadBuffer(data, objectKey, contentType = "application/json") {
  const buf    = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  const stream = Readable.from(buf);
  await mc.putObject(CFG.bucket, objectKey, stream, buf.length, { "Content-Type": contentType });
}

/** Copy an object within the same bucket. */
async function copyObject(srcKey, destKey) {
  const conds = new Minio.CopyConditions();
  await mc.copyObject(CFG.bucket, destKey, `/${CFG.bucket}/${srcKey}`, conds);
}

/** Remove an object. */
async function removeObject(key) {
  await mc.removeObject(CFG.bucket, key);
}

/** Download index.json, parse, return array. Returns [] if not found. */
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

/** Write the entries array to index.json in the bucket. */
async function saveIndex(entries) {
  const json = JSON.stringify(entries, null, 2) + "\n";
  await uploadBuffer(json, CFG.indexKey);
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

function generateWaveform(audioPath, outPath) {
  const r = spawnSync(
    CFG.audiowaveformBin,
    ["-i", audioPath, "-o", outPath, "--pixels-per-second", "20", "--bits", "8"],
    { encoding: "utf8" }
  );
  if (r.error || r.status !== 0) {
    warn(`audiowaveform failed: ${r.stderr?.trim() || r.error?.message}`);
    return false;
  }
  return true;
}

// ─── Stability tracking ───────────────────────────────────────────────────────
//
// seenMap: objectKey → { etag, size, stableAt }
// A file is considered "stable" (upload complete) when its etag+size has not
// changed for at least STABILITY_DELAY_MS milliseconds.

const seenMap = new Map();

function updateSeen(objects) {
  const now      = Date.now();
  const liveKeys = new Set(objects.map(o => o.name));

  for (const key of seenMap.keys()) {
    if (!liveKeys.has(key)) seenMap.delete(key);
  }

  for (const obj of objects) {
    const prev    = seenMap.get(obj.name);
    const changed = !prev || prev.etag !== obj.etag || prev.size !== obj.size;
    if (changed) {
      seenMap.set(obj.name, { etag: obj.etag, size: obj.size, stableAt: now });
    }
  }
}

function stableObjects(objects) {
  const threshold = Date.now() - CFG.stabilityDelayMs;
  return objects.filter(obj => {
    const seen = seenMap.get(obj.name);
    return seen && seen.stableAt <= threshold;
  });
}

// ─── Process one file ─────────────────────────────────────────────────────────

async function processFile(obj) {
  const filename  = path.basename(obj.name);
  const objectKey = obj.name;

  log(`Processing: ${filename}`);

  const parsed = parseFilename(filename);
  if (!parsed) {
    warn(`Filename does not match pattern — leaving in /new/: "${filename}"`);
    return;
  }

  const { date, time, datetimeLocal, label, id } = parsed;
  const [yyyy, mm] = date.split("-");
  const tmpDir     = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-"));

  try {
    // Download.
    const tmpAudio = path.join(tmpDir, filename);
    await download(objectKey, tmpAudio);
    log(`  ↓ downloaded`);

    // Duration + kind.
    const durationSec = getDuration(tmpAudio);
    const kind =
      durationSec === 0             ? "empty"
      : durationSec < CFG.waveformThreshSec ? "note"
      : "audio";
    log(`  duration: ${durationSec.toFixed(2)}s  kind: ${kind}`);

    // Waveform.
    let waveformPath = null;
    if (kind === "audio") {
      const waveformFilename = `${id}.json`;
      const tmpWaveform      = path.join(tmpDir, waveformFilename);
      if (generateWaveform(tmpAudio, tmpWaveform)) {
        const waveformKey = `${CFG.waveformPrefix}${yyyy}/${mm}/${waveformFilename}`;
        await upload(tmpWaveform, waveformKey, "application/json");
        waveformPath = waveformKey;
        log(`  ↑ waveform → ${waveformKey}`);
      }
    }

    // Move audio: copy to audio/YYYY/MM/, then delete from new/.
    const audioKey = `${CFG.audioPrefix}${yyyy}/${mm}/${filename}`;
    await copyObject(objectKey, audioKey);
    await removeObject(objectKey);
    log(`  ⇒ audio   → ${audioKey}`);

    // Update index.json.
    const entry = {
      id, filename,
      audioPath: audioKey,
      waveformPath,
      date, time, datetimeLocal, label,
      durationSec: parseFloat(durationSec.toFixed(3)),
      kind,
    };

    const entries = await loadIndex();
    const idx = entries.findIndex(e => e.id === id);
    if (idx >= 0) {
      entries[idx] = entry;
    } else {
      entries.push(entry);
      entries.sort((a, b) => a.datetimeLocal.localeCompare(b.datetimeLocal));
    }
    await saveIndex(entries);
    log(`  ✓ index.json  (${entries.length} entries total)`);

    seenMap.delete(objectKey);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

// ─── Poll loop ────────────────────────────────────────────────────────────────

async function poll() {
  let objects;
  try {
    objects = await listObjects(CFG.newPrefix);
  } catch (e) {
    err(`listObjects failed: ${e.message}`);
    return;
  }

  const files = objects.filter(o => !o.name.endsWith("/") && o.size > 0);
  if (files.length === 0) return;

  updateSeen(files);

  const ready = stableObjects(files);
  if (ready.length === 0) {
    log(`${files.length} file(s) in /new/ — waiting for stability...`);
    return;
  }

  log(`${ready.length} stable file(s) ready.`);

  for (const obj of ready) {
    try {
      await processFile(obj);
    } catch (e) {
      err(`Failed to process "${obj.name}": ${e.message}`);
    }
  }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

async function main() {
  log("barktown ingest-service starting");
  log(`  MinIO  : ${CFG.minio.useSSL ? "https" : "http"}://${CFG.minio.endPoint}:${CFG.minio.port}`);
  log(`  bucket : ${CFG.bucket}`);
  log(`  poll   : every ${CFG.pollIntervalMs / 1000}s`);
  log(`  stable : after ${CFG.stabilityDelayMs / 1000}s of no change`);

  try {
    if (!(await mc.bucketExists(CFG.bucket))) {
      err(`Bucket "${CFG.bucket}" does not exist. Create it first.`);
      process.exit(1);
    }
    log(`  connected ✓`);
  } catch (e) {
    err(`Cannot connect to MinIO: ${e.message}`);
    process.exit(1);
  }

  await poll();
  setInterval(poll, CFG.pollIntervalMs);
}

main().catch(e => { err(e); process.exit(1); });
