// lib/config.mjs — shared configuration for ingest-service, migration and
// maintenance scripts.
//
// Call buildConfig() *after* loadEnv() (see lib/env.mjs) so CLI scripts that
// load a .env file pick up overrides; under systemd, EnvironmentFile already
// populates process.env before the process starts, so buildConfig() alone
// is sufficient there.

export function buildConfig() {
  return {
    minio: {
      endPoint: process.env.MINIO_ENDPOINT ?? "localhost",
      port: parseInt(process.env.MINIO_PORT ?? "9000", 10),
      useSSL: (process.env.MINIO_USE_SSL ?? "false") === "true",
      accessKey: process.env.MINIO_ACCESS_KEY ?? "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY ?? "minioadmin",
    },
    bucket: process.env.MINIO_BUCKET ?? "barktown",

    newPrefix: "upload-here/",
    audioPrefix: "audio/",
    waveformPrefix: "waveforms/",
    indexKey: "index.json",

    samplesPrefix: "training-samples/",
    samplesWavePrefix: "training-samples-waveforms/",
    samplesIndexKey: "training-samples-index.json",

    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS ?? "20000", 10),
    stabilityDelayMs: parseInt(process.env.STABILITY_DELAY_MS ?? "30000", 10),

    ffprobeBin: process.env.FFPROBE_BIN ?? "ffprobe",
    audiowaveformBin: process.env.AUDIOWAVEFORM_BIN ?? "audiowaveform",
    waveformThreshSec: parseFloat(process.env.WAVEFORM_THRESHOLD_SEC ?? "5"),

    // Local SQLite database — metadata store for training samples (and,
    // later, the full recordings corpus). Not uploaded to MinIO.
    dbPath: process.env.DB_PATH ?? "./data/barktown.db",
  };
}
