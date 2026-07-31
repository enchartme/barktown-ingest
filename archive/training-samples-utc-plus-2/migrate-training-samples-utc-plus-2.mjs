#!/usr/bin/env node
/**
 * One-time migration: training-sample timestamps were written as UTC while
 * the diary corpus uses local time. Rename every sample and waveform +2 hours,
 * update SQLite sample IDs/fields, and preserve annotation associations.
 *
 * Default mode is a read-only dry run. `--apply` is required for mutation.
 * Both barktown-ingest and barktown-api services must be stopped for apply.
 */

import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";

import Database from "better-sqlite3";

import { loadEnv } from "../../lib/env.mjs";
loadEnv(new URL("../../package.json", import.meta.url).href);

import { buildConfig } from "../../lib/config.mjs";
import {
  createClient,
  copyObject,
  listObjects,
  objectExists,
  removeObject,
  uploadBuffer,
} from "../../lib/minio.mjs";
import {
  buildMigratedSamplesIndex,
  buildSampleMapping,
  migrateDatabase,
  validateMappings,
} from "./training-sample-time-migration.mjs";
import { createSqliteBackup } from "../../lib/sqlite-backup.mjs";
import { log, warn } from "../../lib/log.mjs";

const args = parseArgs(process.argv.slice(2));
const CFG = buildConfig();
const migrationToken = new Date().toISOString().replaceAll(/\D/g, "").slice(0, 14);

async function main() {
  const dbPath = path.resolve(CFG.dbPath);
  const backupRoot = args.backupRoot ?? path.join(path.dirname(dbPath), "backups");
  if (!fs.existsSync(dbPath)) {
    throw new Error(`SQLite database does not exist: ${dbPath}`);
  }
  assertNotPreviouslyCompleted(backupRoot);
  if (args.apply) assertServicesStopped();

  const db = new Database(dbPath, { fileMustExist: true });
  db.pragma("foreign_keys = ON");
  db.pragma("busy_timeout = 5000");

  try {
    const samples = readSamples(db);
    const mappings = samples.map((sample) => buildSampleMapping(sample, migrationToken));
    validateMappings(mappings);

    const mc = createClient(CFG.minio);
    if (!(await mc.bucketExists(CFG.bucket))) {
      throw new Error(`MinIO bucket does not exist: ${CFG.bucket}`);
    }

    const preflight = await preflightObjects(mc, mappings);
    const annotationCount = db.prepare("SELECT COUNT(*) AS n FROM annotations").get().n;
    const rolloverCount = mappings.filter((item) => item.date !== item.oldFilename.slice(0, 10)).length;

    printPlan({ dbPath, mappings, annotationCount, rolloverCount, preflight });
    if (!args.apply) {
      log("DRY RUN complete. Nothing was changed.");
      log("Stop barktown-ingest and barktown-api, then rerun with --apply.");
      return;
    }

    const oldIndexBuffer = await getObjectBuffer(mc, CFG.bucket, CFG.samplesIndexKey);
    JSON.parse(oldIndexBuffer.toString("utf8"));

    // Mandatory backups happen before the first MinIO copy or DB update.
    const backup = await createSqliteBackup({
      db,
      dbPath,
      backupRoot,
      backupName: `training-samples-utc-plus-2-${migrationToken}`,
      additionalFiles: {
        "migration-plan.json": JSON.stringify(mappings, null, 2) + "\n",
        "training-samples-index.json": oldIndexBuffer,
      },
      manifestMetadata: { migration: "training-samples-utc-plus-2" },
    });
    log(`SQLite binary backup, SQL dump, JSON export, index, and plan saved to:`);
    log(`  ${backup.backupDir}`);

    const objectMoves = buildObjectMoves(mappings, preflight.objectsByName, migrationToken);
    const newIndex = buildMigratedSamplesIndex(mappings);
    let databaseCommitted = false;

    try {
      await stageObjects(mc, objectMoves);
      await publishShiftedObjects(mc, objectMoves);
      await uploadBuffer(
        mc,
        CFG.bucket,
        JSON.stringify(newIndex, null, 2) + "\n",
        CFG.samplesIndexKey,
      );

      migrateDatabase(db, mappings);
      databaseCommitted = true;
    } catch (error) {
      if (!databaseCommitted) {
        warn(`Migration failed before DB commit; restoring original objects and index...`);
        await restorePreCommitState(mc, objectMoves, oldIndexBuffer);
      }
      throw error;
    }

    const cleanupErrors = await cleanupOldAndStagedObjects(mc, objectMoves);
    await verifyCompletedMigration(db, mc, mappings, objectMoves, annotationCount);

    const result = {
      completedAt: new Date().toISOString(),
      samplesMigrated: mappings.length,
      annotationsRetained: annotationCount,
      rolloverCount,
      cleanupErrors,
      backupDir: backup.backupDir,
    };
    fs.writeFileSync(
      path.join(backup.backupDir, "migration-result.json"),
      JSON.stringify(result, null, 2) + "\n",
      { encoding: "utf8", mode: 0o600 },
    );

    log(`Migration complete: ${mappings.length} samples, ${annotationCount} annotations.`);
    log(`Day rollovers: ${rolloverCount}`);
    if (cleanupErrors.length > 0) {
      throw new Error(`migration committed but ${cleanupErrors.length} cleanup operation(s) failed; see migration-result.json`);
    }
  } finally {
    db.close();
  }
}

function readSamples(db) {
  return db.prepare(`
    SELECT id, filename, audio_path AS audioPath, waveform_path AS waveformPath,
           label, date, datetime_local AS datetimeLocal, duration_sec AS durationSec,
           status, created_at AS createdAt, updated_at AS updatedAt
    FROM samples
    ORDER BY datetime_local ASC
  `).all();
}

async function preflightObjects(mc, mappings) {
  const [audioObjects, waveformObjects] = await Promise.all([
    listObjects(mc, CFG.bucket, CFG.samplesPrefix),
    listObjects(mc, CFG.bucket, CFG.samplesWavePrefix),
  ]);
  const isDataObject = (object) => !object.name.endsWith("/") && path.posix.basename(object.name) !== ".keep";
  const actualAudio = audioObjects.filter(isDataObject);
  const actualWaveforms = waveformObjects.filter(isDataObject);
  const actual = [...actualAudio, ...actualWaveforms];
  const objectsByName = new Map(actual.map((object) => [object.name, object]));

  const expectedSourceKeys = new Set();
  for (const item of mappings) {
    if (!item.oldAudioPath.startsWith(CFG.samplesPrefix)) {
      throw new Error(`audio path is outside ${CFG.samplesPrefix}: ${item.oldAudioPath}`);
    }
    expectedSourceKeys.add(item.oldAudioPath);
    if (item.oldWaveformPath) {
      if (!item.oldWaveformPath.startsWith(CFG.samplesWavePrefix)) {
        throw new Error(`waveform path is outside ${CFG.samplesWavePrefix}: ${item.oldWaveformPath}`);
      }
      expectedSourceKeys.add(item.oldWaveformPath);
    }
  }

  const missing = [...expectedSourceKeys].filter((key) => !objectsByName.has(key));
  const unexpected = [...objectsByName.keys()].filter((key) => !expectedSourceKeys.has(key));
  if (missing.length > 0) throw new Error(`MinIO objects missing for DB rows:\n${missing.join("\n")}`);
  if (unexpected.length > 0) {
    throw new Error(`MinIO objects are not represented by DB rows; refusing a partial migration:\n${unexpected.join("\n")}`);
  }

  const sourceKeys = expectedSourceKeys;
  for (const item of mappings) {
    for (const target of [item.newAudioPath, item.newWaveformPath].filter(Boolean)) {
      if (objectsByName.has(target) && !sourceKeys.has(target)) {
        throw new Error(`target object already exists and is not a migration source: ${target}`);
      }
    }
  }

  return { audioCount: actualAudio.length, waveformCount: actualWaveforms.length, objectsByName };
}

function printPlan({ dbPath, mappings, annotationCount, rolloverCount, preflight }) {
  log(`Training-sample UTC→local migration ${args.apply ? "APPLY" : "DRY RUN"}`);
  log(`  database   : ${dbPath}`);
  log(`  bucket     : ${CFG.bucket}`);
  log(`  samples    : ${mappings.length}`);
  log(`  annotations: ${annotationCount} (integer IDs retained; sample_id changes)`);
  log(`  audio      : ${preflight.audioCount}`);
  log(`  waveforms  : ${preflight.waveformCount}`);
  log(`  rollovers  : ${rolloverCount}`);
  const examples = mappings.length === 1 ? mappings : [mappings[0], mappings.at(-1)];
  for (const item of examples) log(`  ${item.oldFilename}  ->  ${item.newFilename}`);
}

function buildObjectMoves(mappings, objectsByName, token) {
  const moves = [];
  for (const item of mappings) {
    moves.push(makeMove("audio", item.oldAudioPath, item.newAudioPath, objectsByName, moves.length, token));
    if (item.oldWaveformPath) {
      moves.push(makeMove("waveform", item.oldWaveformPath, item.newWaveformPath, objectsByName, moves.length, token));
    }
  }
  return moves;
}

function makeMove(kind, sourceKey, targetKey, objectsByName, index, token) {
  const source = objectsByName.get(sourceKey);
  return {
    kind,
    sourceKey,
    targetKey,
    size: source.size,
    stageKey: `migration-staging/training-samples-utc-plus-2/${token}/${kind}/${String(index).padStart(6, "0")}-${path.posix.basename(sourceKey)}`,
  };
}

async function stageObjects(mc, moves) {
  log(`Staging ${moves.length} MinIO objects...`);
  for (const [index, move] of moves.entries()) {
    if (await objectExists(mc, CFG.bucket, move.stageKey)) throw new Error(`staging key exists: ${move.stageKey}`);
    await copyObject(mc, CFG.bucket, move.sourceKey, move.stageKey);
    await assertObjectSize(mc, move.stageKey, move.size);
    progress(index + 1, moves.length);
  }
}

async function publishShiftedObjects(mc, moves) {
  log(`Publishing shifted MinIO object names...`);
  for (const [index, move] of moves.entries()) {
    await copyObject(mc, CFG.bucket, move.stageKey, move.targetKey);
    await assertObjectSize(mc, move.targetKey, move.size);
    progress(index + 1, moves.length);
  }
}

async function restorePreCommitState(mc, moves, oldIndexBuffer) {
  const sourceKeys = new Set(moves.map((move) => move.sourceKey));
  for (const move of moves) {
    if (await objectExists(mc, CFG.bucket, move.stageKey)) {
      await copyObject(mc, CFG.bucket, move.stageKey, move.sourceKey);
      await assertObjectSize(mc, move.sourceKey, move.size);
    }
  }
  for (const targetKey of new Set(moves.map((move) => move.targetKey))) {
    if (!sourceKeys.has(targetKey) && await objectExists(mc, CFG.bucket, targetKey)) {
      await removeObject(mc, CFG.bucket, targetKey);
    }
  }
  await uploadBuffer(mc, CFG.bucket, oldIndexBuffer, CFG.samplesIndexKey);
}

async function cleanupOldAndStagedObjects(mc, moves) {
  const errors = [];
  const finalKeys = new Set(moves.map((move) => move.targetKey));
  const staleSources = [...new Set(moves.map((move) => move.sourceKey))]
    .filter((sourceKey) => !finalKeys.has(sourceKey));

  for (const key of staleSources) {
    try {
      await removeObject(mc, CFG.bucket, key);
    } catch (error) {
      errors.push({ operation: "remove-old", key, error: error.message });
    }
  }
  for (const move of moves) {
    try {
      await removeObject(mc, CFG.bucket, move.stageKey);
    } catch (error) {
      errors.push({ operation: "remove-stage", key: move.stageKey, error: error.message });
    }
  }
  return errors;
}

async function verifyCompletedMigration(db, mc, mappings, moves, originalAnnotationCount) {
  for (const item of mappings) {
    const row = db.prepare(`
      SELECT filename, audio_path AS audioPath, waveform_path AS waveformPath,
             date, datetime_local AS datetimeLocal
      FROM samples WHERE id = ?
    `).get(item.newId);
    if (!row || row.filename !== item.newFilename || row.audioPath !== item.newAudioPath
      || row.waveformPath !== item.newWaveformPath || row.date !== item.date
      || row.datetimeLocal !== item.datetimeLocal) {
      throw new Error(`post-migration DB verification failed: ${item.newId}`);
    }
  }
  const annotationCount = db.prepare("SELECT COUNT(*) AS n FROM annotations").get().n;
  if (annotationCount !== originalAnnotationCount) throw new Error("annotation count changed during migration");
  const foreignKeys = db.pragma("foreign_key_check");
  if (foreignKeys.length > 0) throw new Error(`foreign-key verification failed: ${JSON.stringify(foreignKeys)}`);
  for (const move of moves) await assertObjectSize(mc, move.targetKey, move.size);
}

async function assertObjectSize(mc, key, expectedSize) {
  const stat = await mc.statObject(CFG.bucket, key);
  if (stat.size !== expectedSize) {
    throw new Error(`object size mismatch for ${key}: expected ${expectedSize}, got ${stat.size}`);
  }
}

async function getObjectBuffer(mc, bucket, key) {
  const stream = await mc.getObject(bucket, key);
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  const buffer = Buffer.concat(chunks);
  if (buffer.length === 0) throw new Error(`cannot back up empty object: ${key}`);
  return buffer;
}

function assertServicesStopped() {
  for (const service of ["barktown-ingest", "barktown-api"]) {
    const result = spawnSync("systemctl", ["is-active", "--quiet", service]);
    if (result.status === 0) {
      throw new Error(`${service}.service is active; stop both Barktown services before --apply`);
    }
  }
}

function assertNotPreviouslyCompleted(backupRoot) {
  if (!fs.existsSync(backupRoot)) return;
  const completed = fs.readdirSync(backupRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith("training-samples-utc-plus-2-"))
    .map((entry) => path.join(backupRoot, entry.name, "migration-result.json"))
    .find((resultPath) => fs.existsSync(resultPath));
  if (completed) {
    throw new Error(`this one-time migration already completed; refusing to shift timestamps again (${completed})`);
  }
}

function progress(done, total) {
  if (done === total || done % 25 === 0) log(`  ${done}/${total}`);
}

function parseArgs(argv) {
  const parsed = { apply: false, backupRoot: null };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--apply") parsed.apply = true;
    else if (arg === "--backup-root") {
      parsed.backupRoot = argv[++index];
      if (!parsed.backupRoot) throw new Error("--backup-root requires a path");
    } else if (arg === "--help" || arg === "-h") {
      console.log(`Usage: node migrate-training-samples-utc-plus-2.mjs [--apply] [--backup-root DIR]\n\nWithout --apply, performs a read-only preflight/dry run.`);
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return parsed;
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exit(1);
});
