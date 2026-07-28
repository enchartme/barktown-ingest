// lib/db.mjs — SQLite-backed metadata store for training samples (and,
// later, the full diary recordings corpus).
//
// This is the source of truth for sample metadata going forward. The flat
// training-samples-index.json object in MinIO is still produced from it
// (see exportSamplesIndexJson) purely for backwards compatibility with the
// existing barktown client (GoblinPiStatus.svelte), which fetches that file
// directly from the public bucket.

import Database from "better-sqlite3";
import fs from "fs";
import path from "path";

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS samples (
    id             TEXT PRIMARY KEY,
    filename       TEXT NOT NULL,
    audio_path     TEXT NOT NULL,
    waveform_path  TEXT,
    label          TEXT NOT NULL,
    date           TEXT NOT NULL,
    datetime_local TEXT NOT NULL,
    duration_sec   REAL NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'active',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_samples_label  ON samples(label);
  CREATE INDEX IF NOT EXISTS idx_samples_status ON samples(status);

  CREATE TABLE IF NOT EXISTS annotations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id   TEXT NOT NULL REFERENCES samples(id) ON DELETE CASCADE,
    start_sec   REAL NOT NULL,
    end_sec     REAL NOT NULL,
    label       TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'manual',
    created_at  TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_annotations_sample ON annotations(sample_id);
`;

/** Open (creating if necessary) the SQLite database at dbPath. */
export function openDb(dbPath) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.pragma("busy_timeout = 5000");
  db.exec(SCHEMA);
  return db;
}

/**
 * Insert or update a training sample row. `entry` uses the same field
 * names as the legacy training-samples-index.json entries (id, filename,
 * audioPath, waveformPath, label, date, datetimeLocal, durationSec).
 * Re-activates a soft-deleted row if it's re-uploaded under the same id.
 */
export function upsertSample(db, entry) {
  const now = new Date().toISOString();
  const existing = db.prepare("SELECT created_at FROM samples WHERE id = ?").get(entry.id);

  db.prepare(`
    INSERT INTO samples
      (id, filename, audio_path, waveform_path, label, date, datetime_local, duration_sec, status, created_at, updated_at)
    VALUES
      (@id, @filename, @audioPath, @waveformPath, @label, @date, @datetimeLocal, @durationSec, 'active', @createdAt, @updatedAt)
    ON CONFLICT(id) DO UPDATE SET
      filename       = excluded.filename,
      audio_path     = excluded.audio_path,
      waveform_path  = excluded.waveform_path,
      label          = excluded.label,
      date           = excluded.date,
      datetime_local = excluded.datetime_local,
      duration_sec   = excluded.duration_sec,
      status         = 'active',
      updated_at     = excluded.updated_at
  `).run({
    id: entry.id,
    filename: entry.filename,
    audioPath: entry.audioPath,
    waveformPath: entry.waveformPath ?? null,
    label: entry.label,
    date: entry.date,
    datetimeLocal: entry.datetimeLocal,
    durationSec: entry.durationSec ?? 0,
    createdAt: existing?.created_at ?? now,
    updatedAt: now,
  });
}

/** Fetch a single sample row by id (any status), or undefined. */
export function getSample(db, id) {
  return db.prepare(`
    SELECT id, filename, audio_path AS audioPath, waveform_path AS waveformPath,
           label, date, datetime_local AS datetimeLocal, duration_sec AS durationSec,
           status, created_at AS createdAt, updated_at AS updatedAt
    FROM samples WHERE id = ?
  `).get(id);
}

/** List all active samples, oldest first (matches legacy index.json order). */
export function listActiveSamples(db) {
  return db.prepare(`
    SELECT id, filename, audio_path AS audioPath, waveform_path AS waveformPath,
           label, date, datetime_local AS datetimeLocal, duration_sec AS durationSec
    FROM samples
    WHERE status = 'active'
    ORDER BY datetime_local ASC
  `).all();
}

/**
 * List active samples for the API, optionally filtered by label.
 * Includes status/timestamps, unlike the legacy-index-shaped listActiveSamples().
 */
export function listSamples(db, { label } = {}) {
  if (label) {
    return db.prepare(`
      SELECT id, filename, audio_path AS audioPath, waveform_path AS waveformPath,
             label, date, datetime_local AS datetimeLocal, duration_sec AS durationSec,
             status, created_at AS createdAt, updated_at AS updatedAt
      FROM samples
      WHERE status = 'active' AND label = ?
      ORDER BY datetime_local ASC
    `).all(label);
  }
  return db.prepare(`
    SELECT id, filename, audio_path AS audioPath, waveform_path AS waveformPath,
           label, date, datetime_local AS datetimeLocal, duration_sec AS durationSec,
           status, created_at AS createdAt, updated_at AS updatedAt
    FROM samples
    WHERE status = 'active'
    ORDER BY datetime_local ASC
  `).all();
}

/** List annotations for a sample, ordered by start time. */
export function listAnnotations(db, sampleId) {
  return db.prepare(`
    SELECT id, sample_id AS sampleId, start_sec AS startSec, end_sec AS endSec,
           label, source, created_at AS createdAt
    FROM annotations
    WHERE sample_id = ?
    ORDER BY start_sec ASC
  `).all(sampleId);
}

/**
 * List every annotation across every (active) sample, for laptop-side training
 * export tools that need to sync the whole corpus in one request instead of
 * fetching per-sample. Includes the parent sample's audioPath/durationSec so
 * callers don't need a second round trip per sample.
 */
export function listAllAnnotations(db) {
  return db.prepare(`
    SELECT a.id, a.sample_id AS sampleId, a.start_sec AS startSec, a.end_sec AS endSec,
           a.label, a.source, a.created_at AS createdAt,
           s.audio_path AS sampleAudioPath, s.duration_sec AS sampleDurationSec
    FROM annotations a
    JOIN samples s ON s.id = a.sample_id
    WHERE s.status = 'active'
    ORDER BY a.sample_id ASC, a.start_sec ASC
  `).all();
}

/**
 * Produce the legacy training-samples-index.json array (active samples only)
 * from the current DB contents, for upload to MinIO.
 */
export function exportSamplesIndexJson(db) {
  return listActiveSamples(db);
}

// ─── Mutations ───────────────────────────────────────────────────────────────────

/** Permanently delete a sample row (annotations cascade via FK). */
export function deleteSampleRow(db, id) {
  db.prepare("DELETE FROM samples WHERE id = ?").run(id);
}

/**
 * Rename a sample to a new id/filename/label/paths, atomically: insert the
 * new row, move any annotations over to the new sample id, then remove the
 * old row. Caller is responsible for moving the underlying MinIO objects
 * (audio/waveform) before calling this.
 */
export function renameSampleTransaction(db, oldId, newSample) {
  const tx = db.transaction(() => {
    const now = new Date().toISOString();
    const existing = db.prepare("SELECT created_at FROM samples WHERE id = ?").get(oldId);

    db.prepare(`
      INSERT INTO samples
        (id, filename, audio_path, waveform_path, label, date, datetime_local, duration_sec, status, created_at, updated_at)
      VALUES
        (@id, @filename, @audioPath, @waveformPath, @label, @date, @datetimeLocal, @durationSec, 'active', @createdAt, @updatedAt)
    `).run({
      id: newSample.id,
      filename: newSample.filename,
      audioPath: newSample.audioPath,
      waveformPath: newSample.waveformPath ?? null,
      label: newSample.label,
      date: newSample.date,
      datetimeLocal: newSample.datetimeLocal,
      durationSec: newSample.durationSec ?? 0,
      createdAt: existing?.created_at ?? now,
      updatedAt: now,
    });

    db.prepare("UPDATE annotations SET sample_id = ? WHERE sample_id = ?").run(newSample.id, oldId);
    db.prepare("DELETE FROM samples WHERE id = ?").run(oldId);
  });
  tx();
}

/** Fetch a single annotation row by id, or undefined. */
export function getAnnotation(db, id) {
  return db.prepare(`
    SELECT id, sample_id AS sampleId, start_sec AS startSec, end_sec AS endSec,
           label, source, created_at AS createdAt
    FROM annotations WHERE id = ?
  `).get(id);
}

/** Insert a new annotation for a sample. Returns the created row. */
export function insertAnnotation(db, sampleId, { startSec, endSec, label, source = "manual" }) {
  const now = new Date().toISOString();
  const info = db.prepare(`
    INSERT INTO annotations (sample_id, start_sec, end_sec, label, source, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(sampleId, startSec, endSec, label, source, now);
  return getAnnotation(db, info.lastInsertRowid);
}

/** Update an existing annotation. Returns the updated row. */
export function updateAnnotation(db, id, { startSec, endSec, label }) {
  const current = getAnnotation(db, id);
  if (!current) return undefined;
  db.prepare(`
    UPDATE annotations SET start_sec = ?, end_sec = ?, label = ? WHERE id = ?
  `).run(
    startSec ?? current.startSec,
    endSec ?? current.endSec,
    label ?? current.label,
    id
  );
  return getAnnotation(db, id);
}

/** Delete an annotation by id. */
export function deleteAnnotationRow(db, id) {
  db.prepare("DELETE FROM annotations WHERE id = ?").run(id);
}
