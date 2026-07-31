import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";

import { openDb, upsertSample, insertAnnotation, getAnnotation } from "../../lib/db.mjs";
import {
  shiftNaiveTimestamp,
  buildSampleMapping,
  validateMappings,
  migrateDatabase,
} from "./training-sample-time-migration.mjs";
import { createSqliteBackup } from "../../lib/sqlite-backup.mjs";

function sample(overrides = {}) {
  return {
    id: "2026-06-07_21-54-03_SAMPLE_background",
    filename: "2026-06-07 21-54-03 SAMPLE background.wav",
    audioPath: "training-samples/background/2026-06-07 21-54-03 SAMPLE background.wav",
    waveformPath: "training-samples-waveforms/background/2026-06-07_21-54-03_SAMPLE_background.json",
    label: "background",
    date: "2026-06-07",
    datetimeLocal: "2026-06-07T21:54:03",
    durationSec: 30,
    status: "active",
    createdAt: "2026-06-07T21:54:40.000Z",
    ...overrides,
  };
}

test("adds two hours without rollover", () => {
  assert.deepEqual(shiftNaiveTimestamp("2026-06-07T12:34:56"), {
    date: "2026-06-07",
    time: "14:34:56",
    datetimeLocal: "2026-06-07T14:34:56",
  });
});

test("adds two hours with next-day rollover", () => {
  const mapping = buildSampleMapping(sample(), "test");
  assert.equal(mapping.newId, "2026-06-07_23-54-03_SAMPLE_background");
  assert.equal(mapping.newFilename, "2026-06-07 23-54-03 SAMPLE background.wav");
  assert.equal(
    mapping.newWaveformPath,
    "training-samples-waveforms/background/2026-06-07_23-54-03_SAMPLE_background.json",
  );

  const rollover = buildSampleMapping(sample({
    id: "2026-06-07_23-54-03_SAMPLE_background",
    filename: "2026-06-07 23-54-03 SAMPLE background.wav",
    audioPath: "training-samples/background/2026-06-07 23-54-03 SAMPLE background.wav",
    waveformPath: "training-samples-waveforms/background/2026-06-07_23-54-03_SAMPLE_background.json",
    datetimeLocal: "2026-06-07T23:54:03",
  }), "test");
  assert.equal(rollover.newId, "2026-06-08_01-54-03_SAMPLE_background");
  assert.equal(rollover.date, "2026-06-08");
  assert.equal(rollover.datetimeLocal, "2026-06-08T01:54:03");
});

test("rejects duplicate shifted IDs", () => {
  const first = buildSampleMapping(sample(), "test");
  assert.throws(() => validateMappings([first, { ...first, oldId: "different" }]), /target sample id collision/);
});

test("transaction changes sample IDs and retains annotation association and IDs", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-time-migration-"));
  const db = openDb(path.join(directory, "test.db"));
  try {
    const original = sample();
    upsertSample(db, original);
    const annotation = insertAnnotation(db, original.id, {
      startSec: 1.25,
      endSec: 2.5,
      label: "background",
      source: "manual",
    });
    const mapping = buildSampleMapping({
      ...original,
      status: "active",
      createdAt: original.createdAt,
    }, "test");
    validateMappings([mapping]);

    migrateDatabase(db, [mapping], "2026-07-31T12:00:00.000Z");

    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM samples WHERE id = ?").get(original.id).n, 0);
    const migrated = db.prepare("SELECT * FROM samples WHERE id = ?").get(mapping.newId);
    assert.equal(migrated.filename, mapping.newFilename);
    assert.equal(migrated.datetime_local, "2026-06-07T23:54:03");

    const migratedAnnotation = getAnnotation(db, annotation.id);
    assert.equal(migratedAnnotation.id, annotation.id);
    assert.equal(migratedAnnotation.sampleId, mapping.newId);
    assert.deepEqual(db.pragma("foreign_key_check"), []);
  } finally {
    db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("transaction handles a target ID that is another sample's source ID", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-time-chain-"));
  const db = openDb(path.join(directory, "test.db"));
  try {
    const first = sample();
    const second = sample({
      id: "2026-06-07_23-54-03_SAMPLE_background",
      filename: "2026-06-07 23-54-03 SAMPLE background.wav",
      audioPath: "training-samples/background/2026-06-07 23-54-03 SAMPLE background.wav",
      waveformPath: "training-samples-waveforms/background/2026-06-07_23-54-03_SAMPLE_background.json",
      date: "2026-06-07",
      datetimeLocal: "2026-06-07T23:54:03",
    });
    upsertSample(db, first);
    upsertSample(db, second);
    const firstAnnotation = insertAnnotation(db, first.id, {
      startSec: 1,
      endSec: 2,
      label: "background",
    });
    const secondAnnotation = insertAnnotation(db, second.id, {
      startSec: 3,
      endSec: 4,
      label: "background",
    });

    const mappings = [
      buildSampleMapping({ ...first, status: "active" }, "chain"),
      buildSampleMapping({ ...second, status: "active" }, "chain"),
    ];
    validateMappings(mappings);
    assert.equal(mappings[0].newId, second.id);

    migrateDatabase(db, mappings);

    assert.equal(getAnnotation(db, firstAnnotation.id).sampleId, mappings[0].newId);
    assert.equal(getAnnotation(db, secondAnnotation.id).sampleId, mappings[1].newId);
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM samples").get().n, 2);
    assert.deepEqual(db.pragma("foreign_key_check"), []);
  } finally {
    db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("creates an integrity-checked binary backup plus SQL and JSON exports", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-backup-"));
  const dbPath = path.join(directory, "source.db");
  const db = openDb(dbPath);
  try {
    const original = sample();
    upsertSample(db, original);
    insertAnnotation(db, original.id, {
      startSec: 1,
      endSec: 2,
      label: "background",
      source: "manual",
    });
    const result = await createSqliteBackup({
      db,
      dbPath,
      backupRoot: path.join(directory, "backups"),
      backupName: "training-samples-utc-plus-2-test",
      additionalFiles: {
        "migration-plan.json": JSON.stringify([buildSampleMapping(original, "test")], null, 2) + "\n",
        "training-samples-index.json": Buffer.from("[]\n"),
      },
      manifestMetadata: { migration: "training-samples-utc-plus-2" },
    });

    assert.ok(fs.statSync(result.dbBackupPath).size > 0);
    assert.match(fs.readFileSync(result.sqlExportPath, "utf8"), /INSERT INTO "annotations"/);
    const exported = JSON.parse(fs.readFileSync(result.jsonExportPath, "utf8"));
    assert.equal(exported.tables.samples.length, 1);
    assert.equal(exported.tables.annotations[0].sample_id, original.id);
    assert.ok(fs.existsSync(result.manifestPath));
  } finally {
    db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
