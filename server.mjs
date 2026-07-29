#!/usr/bin/env node
/**
 * barktown-ingest — HTTP API over the training-samples database.
 *
 * Read endpoints: list/get samples and their annotations.
 * Mutating endpoints: delete a sample, rename/move it between categories,
 * and add/edit/delete fragment annotations.
 *
 * This runs as a separate process from ingest-service.mjs (which also
 * writes to the database on new uploads). SQLite's WAL mode + a busy
 * timeout (lib/db.mjs) support multiple writers/readers across processes.
 *
 * No authentication: this is intended to be reachable only over Tailscale
 * (LAN/VLAN), same trust model as barktown-goblin's own status API.
 * Training samples are ephemeral and backed up elsewhere, so open
 * read/write access on the tailnet is an accepted tradeoff for now.
 * Add auth before exposing this beyond the tailnet, or before adding
 * mutating routes for the (non-ephemeral) diary recordings corpus.
 *
 * ─── Configuration ─────────────────────────────────────────
 *
 *  DB_PATH    Local SQLite database file   (default: ./data/barktown.db)
 *  API_HOST   Interface to bind            (default: 127.0.0.1)
 *  API_PORT   Port to listen on            (default: 8090)
 *
 * ─── Running ───────────────────────────────────────────────────
 *
 *   node server.mjs
 *   npm run server
 */

import { loadEnv } from "./lib/env.mjs";
loadEnv(import.meta.url);

import Fastify from "fastify";
import cors from "@fastify/cors";
import { buildConfig } from "./lib/config.mjs";
import { createClient, copyObject, removeObject, saveJson } from "./lib/minio.mjs";
import { parseSampleFilename } from "./lib/filenames.mjs";
import {
  openDb, getSample, listSamples, listAnnotations, listAllAnnotations, exportSamplesIndexJson,
  deleteSampleRow, renameSampleTransaction,
  getAnnotation, insertAnnotation, updateAnnotation, deleteAnnotationRow,
} from "./lib/db.mjs";
import { log, warn, err } from "./lib/log.mjs";

const CFG = buildConfig();
const db = openDb(CFG.dbPath);
const mc = createClient(CFG.minio);

const app = Fastify({ logger: false });
// @fastify/cors defaults `methods` to "GET,HEAD,POST" — must list the
// mutating verbs explicitly or their preflight (OPTIONS) requests get
// rejected with "CORS Method Not Found", which browsers then report as a
// generic CORS failure on the real PATCH/DELETE request.
await app.register(cors, { origin: true, methods: ["GET", "HEAD", "PUT", "POST", "PATCH", "DELETE", "OPTIONS"] });

/** Regenerate training-samples-index.json in MinIO from the current DB contents.
 * Best-effort: the DB is the source of truth, and ingest-service.mjs regenerates
 * this file on every upload anyway, so a transient MinIO failure here shouldn't
 * fail a mutation that has already been committed to the database. */
async function refreshSamplesIndex() {
  try {
    await saveJson(mc, CFG.bucket, CFG.samplesIndexKey, exportSamplesIndexJson(db));
  } catch (e) {
    warn(`failed to refresh ${CFG.samplesIndexKey} in MinIO: ${e.message}`);
  }
}

/**
 * Validate annotation fields. Returns an error string, or null if valid.
 *
 * Annotations double as two things, distinguished by `source`:
 *  - fragment labels (source: "manual"/"model"): startSec < endSec, label is
 *    one of the fixed training-sample categories.
 *  - time-coded notes (source: "note"): a point in time (startSec === endSec
 *    is allowed), label holds the freeform note text.
 */
function validateAnnotationInput({ startSec, endSec, label }, durationSec) {
  if (typeof startSec !== "number" || !Number.isFinite(startSec) || startSec < 0) {
    return "startSec must be a non-negative number";
  }
  if (typeof endSec !== "number" || !Number.isFinite(endSec) || endSec < startSec) {
    return "endSec must be a number greater than or equal to startSec";
  }
  if (typeof durationSec === "number" && durationSec > 0 && endSec > durationSec + 0.25) {
    return `endSec (${endSec}) exceeds sample duration (${durationSec})`;
  }
  if (typeof label !== "string" || label.trim().length === 0) {
    return "label is required";
  }
  return null;
}

app.get("/health", async () => ({ ok: true }));

// ─── Training samples (read-only) ────────────────────────────────────────────

app.get("/api/samples", async (req) => {
  const label = typeof req.query.label === "string" ? req.query.label : undefined;
  return listSamples(db, { label });
});

app.get("/api/samples/:id", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }
  return sample;
});

app.get("/api/samples/:id/annotations", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }
  return listAnnotations(db, req.params.id);
});

// All annotations across all active samples, in one request — for laptop-side
// training export tools (tools/export_fragments.py in barktown-goblin) that
// need to sync the whole corpus without one request per sample.
app.get("/api/annotations", async () => {
  return listAllAnnotations(db);
});

// ─── Training samples (mutating) ─────────────────────────────────────────

// Delete a sample: removes the audio + waveform objects from MinIO, the DB
// row (annotations cascade), and regenerates training-samples-index.json.
app.delete("/api/samples/:id", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }

  try {
    await removeObject(mc, CFG.bucket, sample.audioPath);
  } catch (e) {
    warn(`delete: could not remove audio object "${sample.audioPath}": ${e.message}`);
  }
  if (sample.waveformPath) {
    try {
      await removeObject(mc, CFG.bucket, sample.waveformPath);
    } catch (e) {
      warn(`delete: could not remove waveform object "${sample.waveformPath}": ${e.message}`);
    }
  }

  deleteSampleRow(db, sample.id);
  await refreshSamplesIndex();
  log(`Deleted sample ${sample.id}`);

  return reply.code(204).send();
});

// Rename/move a sample to a different category (label). This changes the
// filename (label is embedded in it), the sample id (derived from the
// filename), and the audio/waveform object keys, moving the underlying
// MinIO objects to match.
app.patch("/api/samples/:id", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }

  const newLabel = typeof req.body?.label === "string" ? req.body.label.trim().toLowerCase() : "";
  if (!/^[a-z]+$/.test(newLabel)) {
    reply.code(400);
    return { error: "label must be one or more lowercase letters (a-z)" };
  }
  if (newLabel === sample.label) {
    return sample;
  }

  // Rebuild the filename with the new label, reusing the original timestamp.
  const [datePart, timePart] = sample.datetimeLocal.split("T");
  const newFilename = `${datePart} ${timePart.replace(/:/g, "-")} SAMPLE ${newLabel}.wav`;
  const parsedNew = parseSampleFilename(newFilename);
  if (!parsedNew) {
    reply.code(500);
    return { error: "failed to construct new filename" };
  }

  const newAudioKey = `${CFG.samplesPrefix}${newLabel}/${newFilename}`;
  const newWaveformKey = sample.waveformPath
    ? `${CFG.samplesWavePrefix}${newLabel}/${parsedNew.id}.json`
    : null;

  try {
    await copyObject(mc, CFG.bucket, sample.audioPath, newAudioKey);
    await removeObject(mc, CFG.bucket, sample.audioPath);
    if (sample.waveformPath) {
      await copyObject(mc, CFG.bucket, sample.waveformPath, newWaveformKey);
      await removeObject(mc, CFG.bucket, sample.waveformPath);
    }
  } catch (e) {
    err(`rename: MinIO move failed for ${sample.id}: ${e.message}`);
    reply.code(502);
    return { error: `failed to move objects in MinIO: ${e.message}` };
  }

  renameSampleTransaction(db, sample.id, {
    id: parsedNew.id,
    filename: newFilename,
    audioPath: newAudioKey,
    waveformPath: newWaveformKey,
    label: newLabel,
    date: parsedNew.date,
    datetimeLocal: parsedNew.datetimeLocal,
    durationSec: sample.durationSec,
  });
  await refreshSamplesIndex();
  log(`Renamed sample ${sample.id} -> ${parsedNew.id} (label: ${sample.label} -> ${newLabel})`);

  return getSample(db, parsedNew.id);
});

// ─── Annotations (mutating) ────────────────────────────────────────────────

app.post("/api/samples/:id/annotations", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }

  const { startSec, endSec, label, source } = req.body ?? {};
  const validationError = validateAnnotationInput({ startSec, endSec, label }, sample.durationSec);
  if (validationError) {
    reply.code(400);
    return { error: validationError };
  }

  const annotation = insertAnnotation(db, sample.id, {
    startSec, endSec, label: label.trim(), source: source || "manual",
  });
  reply.code(201);
  return annotation;
});

app.patch("/api/annotations/:annotationId", async (req, reply) => {
  const annotationId = Number(req.params.annotationId);
  const existing = getAnnotation(db, annotationId);
  if (!existing) {
    reply.code(404);
    return { error: "not found" };
  }

  const sample = getSample(db, existing.sampleId);
  const merged = {
    startSec: req.body?.startSec ?? existing.startSec,
    endSec: req.body?.endSec ?? existing.endSec,
    label: req.body?.label ?? existing.label,
  };
  const validationError = validateAnnotationInput(merged, sample?.durationSec);
  if (validationError) {
    reply.code(400);
    return { error: validationError };
  }

  return updateAnnotation(db, annotationId, merged);
});

app.delete("/api/annotations/:annotationId", async (req, reply) => {
  const annotationId = Number(req.params.annotationId);
  const existing = getAnnotation(db, annotationId);
  if (!existing) {
    reply.code(404);
    return { error: "not found" };
  }

  deleteAnnotationRow(db, annotationId);
  return reply.code(204).send();
});

// ─── Entry point ──────────────────────────────────────────────────────────────

const port = parseInt(process.env.API_PORT ?? "8090", 10);
const host = process.env.API_HOST ?? "127.0.0.1";

try {
  await app.listen({ port, host });
  log(`barktown-api listening on http://${host}:${port}`);
  log(`  db: ${CFG.dbPath}`);
} catch (e) {
  err(e);
  process.exit(1);
}

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, async () => {
    log(`${sig} received, shutting down...`);
    await app.close();
    db.close();
    process.exit(0);
  });
}
