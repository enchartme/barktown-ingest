// Consistent binary backup plus portable SQL and JSON exports.

import crypto from "crypto";
import fs from "fs";
import path from "path";

import Database from "better-sqlite3";

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
  const backupDir = path.resolve(backupRoot, backupName);
  if (fs.existsSync(backupDir)) throw new Error(`backup directory already exists: ${backupDir}`);
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });

  const dbBackupPath = path.join(backupDir, path.basename(dbPath));
  const sqlExportPath = path.join(backupDir, "barktown.sql");
  const jsonExportPath = path.join(backupDir, "barktown.json");
  const manifestPath = path.join(backupDir, "backup-manifest.json");

  await db.backup(dbBackupPath);

  const backupDb = new Database(dbBackupPath, { readonly: true, fileMustExist: true });
  try {
    const integrity = backupDb.pragma("integrity_check");
    if (integrity.length !== 1 || integrity[0].integrity_check !== "ok") {
      throw new Error(`SQLite backup integrity check failed: ${JSON.stringify(integrity)}`);
    }
    fs.writeFileSync(sqlExportPath, exportSql(backupDb), { encoding: "utf8", mode: 0o600 });
    fs.writeFileSync(jsonExportPath, JSON.stringify(exportJson(backupDb), null, 2) + "\n", {
      encoding: "utf8",
      mode: 0o600,
    });
  } finally {
    backupDb.close();
  }

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
