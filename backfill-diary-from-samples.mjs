#!/usr/bin/env node
// backfill-diary-from-samples.mjs
//
// One-off script: for every active training sample that has no diary_entries
// row yet, synthesise one directly from the sample's own metadata (same audio
// and waveform paths — no file copying needed).  Then set samples.diary_id
// for each matched row.
//
// The diary entry id produced by the normal ingest service for a file called
//   "2026-07-02 17-00-21 SAMPLE yap.wav"
// is the slugified stem: "2026-07-02_17-00-21_SAMPLE_yap" — which is exactly
// the sample's own id.  So diary_id = sample id for every backfilled row.
//
// Usage (dry-run):   node backfill-diary-from-samples.mjs
// Usage (apply):     node backfill-diary-from-samples.mjs --apply
//
// Safe to re-run: uses INSERT OR IGNORE so existing diary rows are untouched.

import { loadEnv } from "./lib/env.mjs";
loadEnv(import.meta.url);

import Database from "better-sqlite3";
import { getConfig }  from "./lib/config.mjs";

const apply = process.argv.includes("--apply");
const cfg   = getConfig();
const db    = new Database(cfg.dbPath);

const samples = db
  .prepare(`
    SELECT id, filename, audio_path, waveform_path, label, date,
           datetime_local, duration_sec, created_at, updated_at
    FROM   samples
    WHERE  status = 'active'
    AND    label IN ('bark', 'yap')
    AND    (diary_id IS NULL OR diary_id = '')
  `)
  .all();

console.log(`Found ${samples.length} sample(s) with no diary link.`);
if (!apply) console.log("Dry-run mode — pass --apply to commit changes.\n");

const insertDiary = db.prepare(`
  INSERT OR IGNORE INTO diary_entries
    (id, filename, audio_path, waveform_path, label, date, time,
     datetime_local, duration_sec, kind, created_at, updated_at)
  VALUES
    (@id, @filename, @audioPath, @waveformPath, @label, @date, @time,
     @datetimeLocal, @durationSec, @kind, @createdAt, @updatedAt)
`);

const setDiaryId = db.prepare(`
  UPDATE samples SET diary_id = @diaryId, updated_at = @now
  WHERE id = @id AND (diary_id IS NULL OR diary_id = '')
`);

let inserted = 0;
let linked   = 0;
let skipped  = 0;

const run = db.transaction(() => {
  for (const s of samples) {
    // Derive time (HH:MM) from datetime_local (YYYY-MM-DDTHH:MM:SS).
    const time = s.datetime_local.slice(11, 16);

    const diaryRow = {
      id:            s.id,
      filename:      s.filename,
      audioPath:     s.audio_path,
      waveformPath:  s.waveform_path ?? null,
      label:         s.label,
      date:          s.date,
      time,
      datetimeLocal: s.datetime_local,
      durationSec:   s.duration_sec,
      kind:          "audio",
      createdAt:     s.created_at,
      updatedAt:     s.updated_at,
    };

    const existing = db
      .prepare("SELECT id FROM diary_entries WHERE id = ?")
      .get(s.id);

    if (existing) {
      console.log(`  skip (diary row already exists): ${s.id}`);
      skipped++;
    } else {
      console.log(`  ${apply ? "insert" : "would insert"} diary entry: ${s.id}`);
      if (apply) {
        insertDiary.run(diaryRow);
        inserted++;
      }
    }

    if (apply) {
      setDiaryId.run({ diaryId: s.id, now: new Date().toISOString(), id: s.id });
      linked++;
    }
  }
});

if (apply) {
  run();
  console.log(`\nDone. inserted=${inserted} linked=${linked} skipped=${skipped}`);
} else {
  // Still iterate to print the plan, but outside the transaction.
  for (const s of samples) {
    const existing = db
      .prepare("SELECT id FROM diary_entries WHERE id = ?")
      .get(s.id);
    if (existing) {
      console.log(`  skip (diary row already exists): ${s.id}`);
      skipped++;
    } else {
      console.log(`  would insert diary entry: ${s.id}`);
      inserted++;
    }
  }
  console.log(`\nDry-run summary: would insert=${inserted} skipped=${skipped}`);
}
