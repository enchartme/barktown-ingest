// Consistent binary backup plus portable SQL and JSON exports.

import crypto from "crypto";
import fs from "fs";
import path from "path";

import Database from "better-sqlite3";

/**
 * Create a point-in-time backup of an open (or path-specified) SQLite database.
 *
 * @param {{
 *   db?:               import('better-sqlite3').Database;  // open connection (preferred — allows hot backup)
 *   dbPath:            string;   // path to the live database file
 *   backupRoot:        string;   // parent directory; the named sub-directory is created here
 *   backupName:        string;   // must be a single directory component, e.g. "dump-20260802-143022"
 *   additionalFiles?:  Record<string, string | Buffer>;  // extra files to write into the backup dir
 *   manifestMetadata?: Record<string, unknown>;          // merged into backup-manifest.json
 * }} opts
 *
 * @returns {Promise<{
 *   backupDir:      string;
 *   dbBackupPath:   string;
 *   sqlExportPath:  string;
 *   jsonExportPath: string;
 *   manifestPath:   string;
 * }>}
 */
export async function createSqliteBackup({
  db,
  dbPath,
  backupRoot,
  backupName,
  additionalFiles = {},
  manifestMetadata = {},
}) {
  if (!backupName || path.basename(backupName) !== backupName) {
    throw new Error(`backupName must be a single directory name: ${backupName}`);
  }

  // Allow callers to pass only dbPath (no open connection).
  // In that case we open a read-only handle just for the export; the binary
  // backup is produced from the live file via a read-only VACUUM INTO instead.
  const ownDb = !db;
  if (ownDb) {
    if (!fs.existsSync(dbPath)) throw new Error(`database file not found: ${dbPath}`);
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
  }

  const backupDir = path.resolve(backupRoot, backupName);
  if (fs.existsSync(backupDir)) throw new Error(`backup directory already exists: ${backupDir}`);
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });

  const dbBackupPath = path.join(backupDir, path.basename(dbPath));
  const sqlExportPath = path.join(backupDir, "barktown.sql");
  const jsonExportPath = path.join(backupDir, "barktown.json");
  const manifestPath = path.join(backupDir, "backup-manifest.json");

  try {
    if (ownDb) {
      // Read-only connection: use VACUUM INTO for a consistent copy without
      // needing the hot-backup API (which requires a writable connection).
      db.exec(`VACUUM INTO '${dbBackupPath.replaceAll("'", "''")}'`);
    } else {
      await db.backup(dbBackupPath);
    }


    const backupDb = new Database(dbBackupPath, { readonly: true, fileMustExist: true });
    try {
      const integrity = backupDb.pragma("integrity_check");
      if (integrity.length !== 1 || integrity[0].integrity_check !== "ok") {
        throw new Error(`SQLite backup integrity check failed: ${JSON.stringify(integrity)}`);
      }
      fs.writeFileSync(sqlExportPath, exportSql(backupDb), { encoding: "utf8", mode: 0o600 });
      const jsonData = exportJson(backupDb);
      fs.writeFileSync(jsonExportPath, JSON.stringify(jsonData, null, 2) + "\n", {
        encoding: "utf8",
        mode: 0o600,
      });

      // Row counts per table — written into the manifest for quick inspection.
      const rowCounts = Object.fromEntries(
        Object.entries(jsonData.tables).map(([name, rows]) => [name, rows.length])
      );

      const additionalPaths = [];
      for (const [filename, contents] of Object.entries(additionalFiles)) {
        if (!filename || path.basename(filename) !== filename) {
          throw new Error(`additional backup filename must be a basename: ${filename}`);
        }
        const filePath = path.join(backupDir, filename);
        fs.writeFileSync(filePath, contents, { mode: 0o600 });
        additionalPaths.push(filePath);
      }

      const files = [dbBackupPath, sqlExportPath, jsonExportPath, ...additionalPaths];
      for (const file of files) {
        if (fs.statSync(file).size === 0) throw new Error(`backup artifact is empty: ${file}`);
      }

      const manifest = {
        createdAt: new Date().toISOString(),
        sourceDatabase: path.resolve(dbPath),
        rowCounts,
        ...manifestMetadata,
        files: Object.fromEntries(files.map((file) => [
          path.basename(file),
          {
            bytes: fs.statSync(file).size,
            sha256: sha256(file),
          },
        ])),
      };
      fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n", {
        encoding: "utf8",
        mode: 0o600,
      });
    } finally {
      backupDb.close();
    }
  } finally {
    if (ownDb) db.close();
  }

  return { backupDir, dbBackupPath, sqlExportPath, jsonExportPath, manifestPath };
}

export function exportSql(db) {
  const schema = db.prepare(`
    SELECT type, name, sql
    FROM sqlite_master
    WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
    ORDER BY rowid
  `).all();
  const tables = schema.filter((row) => row.type === "table");
  const deferredSchema = schema.filter((row) => row.type !== "table");

  const lines = [
    "PRAGMA foreign_keys=OFF;",
    "BEGIN TRANSACTION;",
    ...tables.map((row) => `${row.sql};`),
  ];

  for (const table of tables) {
    const tableName = quoteIdentifier(table.name);
    const columns = db.prepare(`PRAGMA table_info(${tableName})`).all().map((column) => column.name);
    const columnList = columns.map(quoteIdentifier).join(", ");
    for (const row of db.prepare(`SELECT * FROM ${tableName}`).iterate()) {
      const values = columns.map((column) => quoteValue(row[column])).join(", ");
      lines.push(`INSERT INTO ${tableName} (${columnList}) VALUES (${values});`);
    }
  }

  lines.push(...deferredSchema.map((row) => `${row.sql};`));
  lines.push("COMMIT;", "");
  return lines.join("\n");
}

export function exportJson(db) {
  const tables = db.prepare(`
    SELECT name FROM sqlite_master
    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
    ORDER BY rowid
  `).all();
  return {
    exportedAt: new Date().toISOString(),
    userVersion: db.pragma("user_version", { simple: true }),
    tables: Object.fromEntries(tables.map(({ name }) => [
      name,
      db.prepare(`SELECT * FROM ${quoteIdentifier(name)}`).all(),
    ])),
  };
}

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function quoteValue(value) {
  if (value === null || value === undefined) return "NULL";
  if (Buffer.isBuffer(value)) return `X'${value.toString("hex")}'`;
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  return `'${String(value).replaceAll("'", "''")}'`;
}

function sha256(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}
