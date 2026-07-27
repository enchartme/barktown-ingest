#!/usr/bin/env node
/**
 * barktown-ingest — read-only HTTP API over the training-samples database.
 *
 * Phase 1 of the CRUD server: list/get samples and their annotations.
 * Mutating endpoints (delete, rename/move, annotate) come next.
 *
 * This runs as a separate process from ingest-service.mjs (which owns
 * writes to the database). SQLite's WAL mode (enabled in lib/db.mjs)
 * supports one writer + multiple readers across processes safely.
 *
 * No authentication: this is intended to be reachable only over Tailscale
 * (LAN/VLAN), same trust model as barktown-goblin's own status API. Add
 * auth before exposing this beyond the tailnet, or before adding mutating
 * routes for the (non-ephemeral) diary recordings corpus.
 *
 * ─── Configuration ────────────────────────────────────────────────────────
 *
 *  DB_PATH    Local SQLite database file   (default: ./data/barktown.db)
 *  API_HOST   Interface to bind            (default: 0.0.0.0)
 *  API_PORT   Port to listen on            (default: 8090)
 *
 * ─── Running ──────────────────────────────────────────────────────────────
 *
 *   node server.mjs
 *   npm run server
 */

import { loadEnv } from "./lib/env.mjs";
loadEnv(import.meta.url);

import Fastify from "fastify";
import cors from "@fastify/cors";
import { buildConfig } from "./lib/config.mjs";
import { openDb, getSample, listSamples, listAnnotations } from "./lib/db.mjs";
import { log, err } from "./lib/log.mjs";

const CFG = buildConfig();
const db = openDb(CFG.dbPath);

const app = Fastify({ logger: false });
await app.register(cors, { origin: true });

app.get("/health", async () => ({ ok: true }));

// ─── Training samples (read-only) ────────────────────────────────────────────

app.get("/api/samples", async (req) => {
  const label = typeof req.query.label === "string" ? req.query.label : undefined;
  return listSamples(db, { label });
});

app.get("/api/samples/:id", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }
  return sample;
});

app.get("/api/samples/:id/annotations", async (req, reply) => {
  const sample = getSample(db, req.params.id);
  if (!sample || sample.status !== "active") {
    reply.code(404);
    return { error: "not found" };
  }
  return listAnnotations(db, req.params.id);
});

// ─── Entry point ──────────────────────────────────────────────────────────────

const port = parseInt(process.env.API_PORT ?? "8090", 10);
const host = process.env.API_HOST ?? "0.0.0.0";

try {
  await app.listen({ port, host });
  log(`barktown-api listening on http://${host}:${port}`);
  log(`  db: ${CFG.dbPath}`);
} catch (e) {
  err(e);
  process.exit(1);
}

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, async () => {
    log(`${sig} received, shutting down...`);
    await app.close();
    db.close();
    process.exit(0);
  });
}
