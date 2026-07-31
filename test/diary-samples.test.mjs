import { test } from "node:test";
import assert from "node:assert/strict";
import { buildDiarySampleMove, SAMPLE_LABELS } from "../lib/diary-samples.mjs";

const cfg = {
  audioPrefix: "audio/",
  archivePrefix: "uncompressed-uploads-archive/",
  samplesPrefix: "training-samples/",
};

const entry = {
  audioPath: "audio/2026/06/2026-06-07 21-54-03 barking.mp3",
  datetimeLocal: "2026-06-07T21:54:03",
};

test("buildDiarySampleMove maps an archived WAV to the selected sample label", () => {
  assert.deepEqual(buildDiarySampleMove(entry, "background", cfg), {
    label: "background",
    filename: "2026-06-07 21-54-03 SAMPLE background.wav",
    sourceKey: "uncompressed-uploads-archive/2026/06/2026-06-07 21-54-03 barking.wav",
    destinationKey: "training-samples/background/2026-06-07 21-54-03 SAMPLE background.wav",
  });
});

test("buildDiarySampleMove normalizes label case and whitespace", () => {
  assert.equal(buildDiarySampleMove(entry, "  Homestead ", cfg).label, "homestead");
});

test("the false-positive picker exposes the complete fixed label taxonomy", () => {
  assert.deepEqual(SAMPLE_LABELS, [
    "bark", "yap", "background", "wind", "homestead", "traffic", "gunshot", "wrongdog",
  ]);
});

test("buildDiarySampleMove rejects unknown labels before deriving storage keys", () => {
  assert.throws(
    () => buildDiarySampleMove(entry, "other", cfg),
    /label must be one of/,
  );
});

test("buildDiarySampleMove rejects diary audio outside the managed prefix", () => {
  assert.throws(
    () => buildDiarySampleMove({ ...entry, audioPath: "other/file.mp3" }, "wind", cfg),
    /below audio\//,
  );
});
