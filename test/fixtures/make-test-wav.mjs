// test/fixtures/make-test-wav.mjs — generates a small synthetic mono
// 16-bit PCM WAV, used as fixture audio in tests. Stands in for a real
// ~30s training sample recording without checking a binary into git.

import fs from "fs/promises";
import path from "path";

/** Build a mono 16-bit PCM WAV buffer containing a sine wave. */
export function makeSineWav({ durationSec = 2, sampleRate = 8000, freq = 440 } = {}) {
  const numSamples = Math.round(durationSec * sampleRate);
  const dataSize = numSamples * 2; // 16-bit mono
  const buffer = Buffer.alloc(44 + dataSize);

  buffer.write("RIFF", 0, "ascii");
  buffer.writeUInt32LE(36 + dataSize, 4);
  buffer.write("WAVE", 8, "ascii");
  buffer.write("fmt ", 12, "ascii");
  buffer.writeUInt32LE(16, 16); // fmt chunk size
  buffer.writeUInt16LE(1, 20); // PCM
  buffer.writeUInt16LE(1, 22); // mono
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(sampleRate * 2, 28); // byte rate
  buffer.writeUInt16LE(2, 32); // block align
  buffer.writeUInt16LE(16, 34); // bits per sample
  buffer.write("data", 36, "ascii");
  buffer.writeUInt32LE(dataSize, 40);

  for (let i = 0; i < numSamples; i++) {
    const t = i / sampleRate;
    const sample = Math.round(Math.sin(2 * Math.PI * freq * t) * 32767 * 0.5);
    buffer.writeInt16LE(sample, 44 + i * 2);
  }

  return buffer;
}

/** Write a synthetic sine WAV fixture to disk, creating parent directories. */
export async function writeTestWav(filePath, opts) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, makeSineWav(opts));
}
