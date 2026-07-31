// Pure mapping and transactional DB helpers for the one-time migration from
// UTC-labelled training-sample timestamps to local time (UTC+2).

import path from "path";

import { parseSampleFilename } from "../../lib/filenames.mjs";

const SHIFT_HOURS = 2;

/** Shift a naive YYYY-MM-DDTHH:MM:SS value by exactly two hours. */
export function shiftNaiveTimestamp(datetimeLocal) {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/.exec(datetimeLocal);
  if (!match) throw new Error(`invalid naive timestamp: ${datetimeLocal}`);

  const [, year, month, day, hour, minute, second] = match;
  const original = new Date(Date.UTC(
    Number(year), Number(month) - 1, Number(day),
    Number(hour), Number(minute), Number(second),
  ));

  // Date normalises impossible input (for example 31 February), so verify the
  // parsed fields before using it for rollover arithmetic.
  if (
    original.getUTCFullYear() !== Number(year)
    || original.getUTCMonth() !== Number(month) - 1
    || original.getUTCDate() !== Number(day)
    || original.getUTCHours() !== Number(hour)
    || original.getUTCMinutes() !== Number(minute)
    || original.getUTCSeconds() !== Number(second)
  ) {
    throw new Error(`invalid calendar timestamp: ${datetimeLocal}`);
  }

  original.setUTCHours(original.getUTCHours() + SHIFT_HOURS);
  const shiftedDate = [
    original.getUTCFullYear().toString().padStart(4, "0"),
    (original.getUTCMonth() + 1).toString().padStart(2, "0"),
    original.getUTCDate().toString().padStart(2, "0"),
  ].join("-");
  const shiftedTime = [
    original.getUTCHours().toString().padStart(2, "0"),
    original.getUTCMinutes().toString().padStart(2, "0"),
    original.getUTCSeconds().toString().padStart(2, "0"),
  ].join(":");

  return {
    date: shiftedDate,
    time: shiftedTime,
    datetimeLocal: `${shiftedDate}T${shiftedTime}`,
  };
}

/** Build the old→new DB and object-key mapping for one sample row. */
export function buildSampleMapping(sample, migrationToken = "migration") {
  const parsed = parseSampleFilename(sample.filename);
  if (!parsed) throw new Error(`sample filename does not match the canonical pattern: ${sample.filename}`);
  if (parsed.id !== sample.id) {
    throw new Error(`sample id/filename mismatch: DB=${sample.id} filename=${parsed.id}`);
  }
  if (parsed.datetimeLocal !== sample.datetimeLocal || parsed.date !== sample.date) {
    throw new Error(`sample timestamp fields disagree for ${sample.id}`);
  }
  if (path.posix.basename(sample.audioPath) !== sample.filename) {
    throw new Error(`sample filename/audio_path mismatch for ${sample.id}`);
  }

  const shifted = shiftNaiveTimestamp(sample.datetimeLocal);
  const filename = `${shifted.date} ${shifted.time.replaceAll(":", "-")} SAMPLE ${parsed.label}.wav`;
  const parsedNew = parseSampleFilename(filename);
  if (!parsedNew) throw new Error(`failed to construct shifted filename for ${sample.id}`);

  const audioPath = path.posix.join(path.posix.dirname(sample.audioPath), filename);
  let waveformPath = null;
  if (sample.waveformPath) {
    const expectedWaveformName = `${sample.id}.json`;
    if (path.posix.basename(sample.waveformPath) !== expectedWaveformName) {
      throw new Error(`sample id/waveform_path mismatch for ${sample.id}`);
    }
    waveformPath = path.posix.join(
      path.posix.dirname(sample.waveformPath),
      `${parsedNew.id}.json`,
    );
  }

  return {
    oldId: sample.id,
    temporaryId: `__utc_plus_2__${migrationToken}__${sample.id}`,
    oldFilename: sample.filename,
    oldAudioPath: sample.audioPath,
    oldWaveformPath: sample.waveformPath,
    newId: parsedNew.id,
    newFilename: filename,
    newAudioPath: audioPath,
    newWaveformPath: waveformPath,
    label: sample.label,
    date: shifted.date,
    datetimeLocal: shifted.datetimeLocal,
    durationSec: sample.durationSec,
    status: sample.status,
    createdAt: sample.createdAt,
  };
}

/** Reject any DB ID or MinIO key collision before backup/apply. */
export function validateMappings(mappings) {
  if (mappings.length === 0) throw new Error("database contains no training samples");

  assertUnique(mappings.map((item) => item.oldId), "source sample id");
  assertUnique(mappings.map((item) => item.newId), "target sample id");
  assertUnique(mappings.map((item) => item.temporaryId), "temporary sample id");
  assertUnique(mappings.map((item) => item.oldAudioPath), "source audio key");
  assertUnique(mappings.map((item) => item.newAudioPath), "target audio key");

  const waveformMappings = mappings.filter((item) => item.oldWaveformPath);
  assertUnique(waveformMappings.map((item) => item.oldWaveformPath), "source waveform key");
  assertUnique(waveformMappings.map((item) => item.newWaveformPath), "target waveform key");

  const oldIds = new Set(mappings.map((item) => item.oldId));
  for (const item of mappings) {
    if (oldIds.has(item.temporaryId)) {
      throw new Error(`temporary sample id collides with existing id: ${item.temporaryId}`);
    }
  }
}

/**
 * Update sample primary keys and annotation foreign keys in one transaction.
 * Annotation integer IDs are deliberately preserved. The temporary-ID phase
 * makes mappings safe even when one sample's target ID is another's source ID.
 */
export function migrateDatabase(db, mappings, updatedAt = new Date().toISOString()) {
  const moveAnnotations = db.prepare("UPDATE annotations SET sample_id = ? WHERE sample_id = ?");
  const moveSampleId = db.prepare("UPDATE samples SET id = ? WHERE id = ?");
  const updateSample = db.prepare(`
    UPDATE samples SET
      id = ?, filename = ?, audio_path = ?, waveform_path = ?, date = ?,
      datetime_local = ?, updated_at = ?
    WHERE id = ?
  `);

  const transaction = db.transaction(() => {
    db.pragma("defer_foreign_keys = ON");

    for (const item of mappings) {
      const annotations = moveAnnotations.run(item.temporaryId, item.oldId);
      const sample = moveSampleId.run(item.temporaryId, item.oldId);
      if (sample.changes !== 1) throw new Error(`sample disappeared during migration: ${item.oldId}`);
      item.annotationCount = annotations.changes;
    }

    for (const item of mappings) {
      const sample = updateSample.run(
        item.newId,
        item.newFilename,
        item.newAudioPath,
        item.newWaveformPath,
        item.date,
        item.datetimeLocal,
        updatedAt,
        item.temporaryId,
      );
      if (sample.changes !== 1) throw new Error(`temporary sample missing: ${item.temporaryId}`);
      moveAnnotations.run(item.newId, item.temporaryId);
    }

    const foreignKeyErrors = db.pragma("foreign_key_check");
    if (foreignKeyErrors.length > 0) {
      throw new Error(`foreign-key check failed: ${JSON.stringify(foreignKeyErrors)}`);
    }
  });

  transaction();
}

/** Legacy index shape generated from the planned post-migration state. */
export function buildMigratedSamplesIndex(mappings) {
  return mappings
    .filter((item) => item.status === "active")
    .map((item) => ({
      id: item.newId,
      filename: item.newFilename,
      audioPath: item.newAudioPath,
      waveformPath: item.newWaveformPath,
      label: item.label,
      date: item.date,
      datetimeLocal: item.datetimeLocal,
      durationSec: item.durationSec,
    }))
    .sort((a, b) => a.datetimeLocal.localeCompare(b.datetimeLocal));
}

function assertUnique(values, description) {
  const seen = new Set();
  for (const value of values) {
    if (seen.has(value)) throw new Error(`${description} collision: ${value}`);
    seen.add(value);
  }
}
