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
    archivePrefix: "uncompressed-uploads-archive/",
    audioPrefix: "audio/",
    waveformPrefix: "waveforms/",
    indexKey: "index.json",

    samplesPrefix: "training-samples/",
    samplesWavePrefix: "training-samples-waveforms/",
    samplesIndexKey: "training-samples-index.json",

    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS ?? "20000", 10),
    stabilityDelayMs: parseInt(process.env.STABILITY_DELAY_MS ?? "30000", 10),

    ffprobeBin: process.env.FFPROBE_BIN ?? "ffprobe",
    ffmpegBin: process.env.FFMPEG_BIN ?? "ffmpeg",
    audiowaveformBin: process.env.AUDIOWAVEFORM_BIN ?? "audiowaveform",
    waveformThreshSec: parseFloat(process.env.WAVEFORM_THRESHOLD_SEC ?? "5"),

    // WAV-to-MP3 pre-processing (applied to .wav files landing in upload-here/).
    // wavVolumeBoostPct: 200 = 2× amplitude (6 dB louder).  Set to 100 to skip.
    wavVolumeBoostPct: parseInt(process.env.WAV_VOLUME_BOOST_PCT ?? "200", 10),
    wavMp3Bitrate: parseInt(process.env.WAV_MP3_BITRATE ?? "128", 10),
    // Optional free-form location string embedded in the MP3 comment tag.
    // e.g. "52.3676° N, 4.9041° E" or "Backyard, Amsterdam"
    recordingLocation: process.env.RECORDING_LOCATION ?? "",

    // Local SQLite database — metadata store for training samples (and,
    // later, the full recordings corpus). Not uploaded to MinIO.
    dbPath: process.env.DB_PATH ?? "./data/barktown.db",
  };
}
