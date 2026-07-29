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

/**
 * Convert a WAV file to MP3 with volume boost and ID3 metadata tags.
 * Returns true on success, false on failure.
 *
 * @param {string} ffmpegBin
 * @param {string} inputPath    - source .wav
 * @param {string} outputPath   - destination .mp3
 * @param {object} [opts]
 * @param {number} [opts.volumePct=200]          - output volume as % of input (200 = 2×)
 * @param {number} [opts.bitrate=128]            - MP3 bitrate in kbps
 * @param {object} [opts.metadata={}]
 * @param {string} [opts.metadata.title]
 * @param {string} [opts.metadata.date]          - YYYY-MM-DD
 * @param {string} [opts.metadata.datetime]      - ISO local datetime
 * @param {string} [opts.metadata.location]      - free-form location / coordinates
 * @param {string} [opts.metadata.originalFilename]
 */
export function convertWavToMp3(ffmpegBin, inputPath, outputPath, opts = {}) {
  const { volumePct = 200, bitrate = 128, metadata = {} } = opts;
  const volumeFilter = (volumePct / 100).toFixed(6);

  const metaArgs = [];
  if (metadata.title) {
    metaArgs.push("-metadata", `title=${metadata.title}`);
  }
  if (metadata.date) {
    metaArgs.push("-metadata", `date=${metadata.date}`);
  }
  // Build a human-readable comment with provenance.
  const commentParts = [];
  if (metadata.datetime)         commentParts.push(`Recorded: ${metadata.datetime}`);
  if (metadata.location)         commentParts.push(`Location: ${metadata.location}`);
  if (metadata.originalFilename) commentParts.push(`Source: ${metadata.originalFilename}`);
  if (commentParts.length > 0) {
    metaArgs.push("-metadata", `comment=${commentParts.join("  ")}`);
  }
  if (metadata.originalFilename) {
    metaArgs.push("-metadata", `TXXX:original_filename=${metadata.originalFilename}`);
  }
  metaArgs.push("-metadata", "encoded_by=barktown-ingest");

  const r = spawnSync(
    ffmpegBin,
    [
      "-y", "-i", inputPath,
      "-af", `volume=${volumeFilter}`,
      "-codec:a", "libmp3lame",
      "-b:a", `${bitrate}k`,
      "-id3v2_version", "3",
      ...metaArgs,
      outputPath,
    ],
    { encoding: "utf8" }
  );
  return !(r.error || r.status !== 0);
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
