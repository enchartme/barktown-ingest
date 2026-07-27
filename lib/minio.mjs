// lib/minio.mjs — generic MinIO object helpers shared by the ingest service
// and maintenance/migration scripts. No index/domain logic here — see
// lib/db.mjs for the samples metadata store.

import * as Minio from "minio";
import { Readable } from "stream";

/** Create a MinIO client from a config object (CFG.minio). */
export function createClient(minioCfg) {
  return new Minio.Client(minioCfg);
}

/** List all objects under a prefix (recursive). */
export async function listObjects(mc, bucket, prefix) {
  return new Promise((resolve, reject) => {
    const objects = [];
    const stream = mc.listObjectsV2(bucket, prefix, true);
    stream.on("data", (o) => objects.push(o));
    stream.on("end", () => resolve(objects));
    stream.on("error", reject);
  });
}

/** Download an object to a local file path. */
export async function download(mc, bucket, objectKey, destPath) {
  await mc.fGetObject(bucket, objectKey, destPath);
}

/** Upload a local file to an object key. */
export async function upload(mc, bucket, srcPath, objectKey, contentType = "application/octet-stream") {
  await mc.fPutObject(bucket, objectKey, srcPath, { "Content-Type": contentType });
}

/** Upload a Buffer / string as an object. */
export async function uploadBuffer(mc, bucket, data, objectKey, contentType = "application/json") {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  const stream = Readable.from(buf);
  await mc.putObject(bucket, objectKey, stream, buf.length, { "Content-Type": contentType });
}

/** Copy an object within the same bucket. */
export async function copyObject(mc, bucket, srcKey, destKey) {
  const conds = new Minio.CopyConditions();
  await mc.copyObject(bucket, destKey, `/${bucket}/${srcKey}`, conds);
}

/** Remove an object. */
export async function removeObject(mc, bucket, key) {
  await mc.removeObject(bucket, key);
}

/** Returns true if objectKey exists in the bucket. */
export async function objectExists(mc, bucket, objectKey) {
  try {
    await mc.statObject(bucket, objectKey);
    return true;
  } catch {
    return false;
  }
}

/** Download and JSON-parse an object. Returns fallback if the object is missing. */
export async function loadJson(mc, bucket, objectKey, fallback = []) {
  try {
    const stream = await mc.getObject(bucket, objectKey);
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch (e) {
    if (e.code === "NoSuchKey" || e.code === "NotFound") return fallback;
    throw e;
  }
}

/** JSON-stringify and upload a value to an object key. */
export async function saveJson(mc, bucket, objectKey, value) {
  const json = JSON.stringify(value, null, 2) + "\n";
  await uploadBuffer(mc, bucket, json, objectKey);
}
