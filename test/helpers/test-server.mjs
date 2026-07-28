// test/helpers/test-server.mjs — spawns the real server.mjs as a child
// process against a throwaway SQLite DB, so tests exercise the actual
// Fastify app + CORS config + routes (no mocking).

import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { getFreePort } from "./free-port.mjs";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

/**
 * Start server.mjs against `dbPath`, waiting for it to answer /health.
 * Returns { baseUrl, stop }.
 */
export async function startTestServer({ dbPath, env = {} }) {
  const port = await getFreePort();
  const proc = spawn(process.execPath, [path.join(REPO_ROOT, "server.mjs")], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      DB_PATH: dbPath,
      API_HOST: "127.0.0.1",
      API_PORT: String(port),
      ...env,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderr = "";
  proc.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  const baseUrl = `http://127.0.0.1:${port}`;

  const exitedEarly = new Promise((_resolve, reject) => {
    proc.once("exit", (code) => {
      if (code !== null && code !== 0) {
        reject(new Error(`server.mjs exited early (code ${code}):\n${stderr}`));
      }
    });
  });

  await Promise.race([waitForHealth(baseUrl), exitedEarly]);

  return {
    baseUrl,
    stop: () => stopServer(proc),
  };
}

async function waitForHealth(baseUrl, { timeoutMs = 5000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${baseUrl}/health`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`server.mjs did not become healthy within ${timeoutMs}ms`);
}

function stopServer(proc) {
  return new Promise((resolve) => {
    if (proc.exitCode !== null || proc.signalCode !== null) {
      resolve();
      return;
    }
    proc.once("exit", () => resolve());
    proc.kill("SIGTERM");
  });
}
