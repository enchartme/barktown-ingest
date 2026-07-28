// test/api.test.mjs — integration tests for server.mjs's HTTP API.
//
// Runs the real server.mjs as a child process against a throwaway SQLite
// DB (no mocking of Fastify, routes, or the DB layer). Only exercises
// annotation endpoints + read-only sample endpoints, which don't touch
// MinIO, so no object storage needs to be running.

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { openDb, upsertSample } from "../lib/db.mjs";
import { startTestServer } from "./helpers/test-server.mjs";

let tmpDir;
let seedDb;
let server;

before(async () => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "barktown-ingest-test-"));
  const dbPath = path.join(tmpDir, "test.db");

  seedDb = openDb(dbPath);
  upsertSample(seedDb, {
    id: "sample-001",
    filename: "2026-01-01 12-00-00 SAMPLE bark.wav",
    audioPath: "training-samples/bark/2026-01-01 12-00-00 SAMPLE bark.wav",
    waveformPath: null,
    label: "bark",
    date: "2026-01-01",
    datetimeLocal: "2026-01-01T12:00:00",
    durationSec: 2,
  });

  server = await startTestServer({ dbPath });
});

after(async () => {
  await server.stop();
  seedDb.close();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

test("GET /health reports ok", async () => {
  const res = await fetch(`${server.baseUrl}/health`);
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { ok: true });
});

test("CORS preflight allows PATCH and DELETE", async () => {
  // Regression test for the bug where @fastify/cors's default `methods`
  // ("GET,HEAD,POST") silently rejected PATCH/DELETE preflight requests,
  // which browsers reported as a generic, hard-to-diagnose CORS failure.
  const res = await fetch(`${server.baseUrl}/api/annotations`, {
    method: "OPTIONS",
    headers: {
      Origin: "http://example.test",
      "Access-Control-Request-Method": "PATCH",
    },
  });
  const allowed = res.headers.get("access-control-allow-methods") ?? "";
  assert.match(allowed, /PATCH/);
  assert.match(allowed, /DELETE/);
});

test("GET /api/samples/:id returns the seeded sample", async () => {
  const res = await fetch(`${server.baseUrl}/api/samples/sample-001`);
  assert.equal(res.status, 200);
  const sample = await res.json();
  assert.equal(sample.id, "sample-001");
  assert.equal(sample.label, "bark");
});

test("GET /api/samples/:id returns 404 for an unknown id", async () => {
  const res = await fetch(`${server.baseUrl}/api/samples/does-not-exist`);
  assert.equal(res.status, 404);
});

let annotationId;

test("POST /api/samples/:id/annotations creates a fragment", async () => {
  const res = await fetch(`${server.baseUrl}/api/samples/sample-001/annotations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ startSec: 0.5, endSec: 1.0, label: "bark", source: "manual" }),
  });
  assert.equal(res.status, 201);
  const annotation = await res.json();
  assert.equal(annotation.sampleId, "sample-001");
  assert.equal(annotation.label, "bark");
  annotationId = annotation.id;
});

test("POST /api/samples/:id/annotations rejects endSec < startSec", async () => {
  const res = await fetch(`${server.baseUrl}/api/samples/sample-001/annotations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ startSec: 1.0, endSec: 0.5, label: "bark" }),
  });
  assert.equal(res.status, 400);
});

test("GET /api/annotations aggregates across samples, joined with sample fields", async () => {
  const res = await fetch(`${server.baseUrl}/api/annotations`);
  assert.equal(res.status, 200);
  const rows = await res.json();
  const row = rows.find((r) => r.id === annotationId);
  assert.ok(row, "seeded annotation should be present");
  assert.equal(row.sampleAudioPath, "training-samples/bark/2026-01-01 12-00-00 SAMPLE bark.wav");
  assert.equal(row.sampleDurationSec, 2);
});

test("PATCH /api/annotations/:id updates the label", async () => {
  const res = await fetch(`${server.baseUrl}/api/annotations/${annotationId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label: "yap" }),
  });
  assert.equal(res.status, 200);
  const updated = await res.json();
  assert.equal(updated.label, "yap");
});

test("DELETE /api/annotations/:id removes it", async () => {
  const res = await fetch(`${server.baseUrl}/api/annotations/${annotationId}`, { method: "DELETE" });
  assert.equal(res.status, 204);

  const check = await fetch(`${server.baseUrl}/api/annotations`);
  const rows = await check.json();
  assert.ok(!rows.some((r) => r.id === annotationId));
});
