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
 * Produce the legacy training-samples-index.json array (active samples only)
 * from the current DB contents, for upload to MinIO.
 */
export function exportSamplesIndexJson(db) {
  return listActiveSamples(db);
}
