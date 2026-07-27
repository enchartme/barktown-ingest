// lib/audio.mjs — ffprobe/ffmpeg/audiowaveform wrappers shared across scripts.

import { spawnSync } from "child_process";

/** Read duration (seconds) of an audio file via ffprobe. Returns 0 on failure. */
export function getDuration(ffprobeBin, filePath) {
  const r = spawnSync(
    ffprobeBin,
    ["-v", "quiet", "-print_format", "json", "-show_format", filePath],
    { encoding: "utf8" }
  );
  if (r.error || r.status !== 0) return 0;
  try {
    const data = JSON.parse(r.stdout);
    return parseFloat(data.format?.duration ?? "0");
  } catch {
    return 0;
  }
}

/**
 * Convert any audio file to WAV via ffmpeg so audiowaveform can read it.
 * Returns true on success, false on failure.
 */
export function convertToWav(inputPath, outputPath) {
  const r = spawnSync(
    "ffmpeg",
    ["-y", "-i", inputPath, "-ar", "16000", "-ac", "1", "-f", "wav", outputPath],
    { encoding: "utf8" }
  );
  if (r.error || r.status !== 0) {
    return false;
  }
  return true;
}

/** Generate a waveform peaks JSON file via audiowaveform. Returns true on success. */
export function generateWaveform(audiowaveformBin, audioPath, outPath, bits = 8) {
  const r = spawnSync(
    audiowaveformBin,
    ["-i", audioPath, "-o", outPath, "--pixels-per-second", "20", "--bits", String(bits)],
    { encoding: "utf8" }
  );
  if (r.error || r.status !== 0) {
    return false;
  }
  return true;
}
