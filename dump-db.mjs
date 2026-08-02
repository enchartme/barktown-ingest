#!/usr/bin/env node
/**
 * barktown — on-demand database dump
 *
 * Creates a timestamped backup of the local SQLite database under
 * ./data/backups/<timestamp>/  containing:
 *
 *   barktown.db            — consistent binary copy (VACUUM INTO)
 *   barktown.sql           — portable SQL dump (schema + all rows)
 *   barktown.json          — all tables as JSON arrays
 *   backup-manifest.json   — sizes, sha256 checksums, row counts per table
 *
 * Usage:
 *   node dump-db.mjs
 *   npm run dump-db
 *
 * The backup directory is printed on success so you can inspect or copy it.
 * Nothing is removed from MinIO; only the local database is dumped.
 *
 * ─── Configuration ──────────────────────────────────────────────────────────
 *
 *  DB_PATH          SQLite database file   (default: ./data/barktown.db)
 *  BACKUP_ROOT      Parent directory       (default: ./data/backups)
 */

import path from "path";
import { fileURLToPath } from "url";

import { loadEnv } from "./lib/env.mjs";
loadEnv(import.meta.url);

import { buildConfig } from "./lib/config.mjs";
import { createSqliteBackup } from "./lib/sqlite-backup.mjs";
import { log, err } from "./lib/log.mjs";

const CFG  = buildConfig();
const here = path.dirname(fileURLToPath(import.meta.url));

const dbPath     = CFG.dbPath;
const backupRoot = process.env.BACKUP_ROOT ?? path.join(here, "data", "backups");

/** Format a Date as a compact local timestamp string: YYYYMMDD-HHmmss */
function formatTimestamp(d = new Date()) {
  const pad2  = (n) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}${pad2(d.getMonth() + 1)}${pad2(d.getDate())}` +
    `-${pad2(d.getHours())}${pad2(d.getMinutes())}${pad2(d.getSeconds())}`
  );
}

const backupName = `dump-${formatTimestamp()}`;
log(`Backing up ${dbPath}  →  ${backupRoot}/${backupName}`);

try {
  const result = await createSqliteBackup({ dbPath, backupRoot, backupName });

  // Print a brief summary of the manifest row counts.
  const { default: fs } = await import("fs");
  const manifest = JSON.parse(fs.readFileSync(result.manifestPath, "utf8"));

  const rowSummary = Object.entries(manifest.rowCounts)
    .map(([table, n]) => `  ${table}: ${n} row${n !== 1 ? "s" : ""}`)
    .join("\n");

  log(`Done.`);
  log(`  directory: ${result.backupDir}`);
  log(`Row counts:\n${rowSummary}`);
  log(`Files:`);
  for (const [name, info] of Object.entries(manifest.files)) {
    const kb = (info.bytes / 1024).toFixed(1);
    log(`  ${name}  ${kb} kB  sha256:${info.sha256.slice(0, 16)}…`);
  }
} catch (e) {
  err(`Backup failed: ${e.message}`);
  process.exit(1);
}
