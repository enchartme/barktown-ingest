// lib/env.mjs — loads a .env file (if present) into process.env without
// overriding variables already set (e.g. by systemd's EnvironmentFile).
//
// Used by CLI scripts run directly with `node script.mjs` outside systemd.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

/**
 * Load key=value pairs from a .env file located next to the given
 * import.meta.url (typically the caller's own module).
 */
export function loadEnv(callerImportMetaUrl) {
  const callerDir = path.dirname(fileURLToPath(callerImportMetaUrl));
  const envPath = path.join(callerDir, ".env");

  if (!fs.existsSync(envPath)) return;

  for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m && !(m[1] in process.env)) {
      process.env[m[1]] = m[2].replace(/^['"]|['"]$/g, "").trim();
    }
  }
}
