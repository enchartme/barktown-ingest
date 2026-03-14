# barktown-ingest

Pi-side ingest service for **barktown**. Watches the `upload-here/` prefix in a MinIO bucket, validates filenames, generates waveform data, organises audio into `audio/YYYY/MM/` and keeps `index.json` up to date.

---

## How it works

1. You upload `.m4a` or `.aac` files to `<bucket>/upload-here/`
2. The service polls that prefix every 20 s
3. Once a file's size and ETag have been stable for 30 s (upload complete), it is processed:
   - Filename is validated against `YYYY-MM-DD HH-MM-SS optional comment.ext`
   - Invalid names are left in `upload-here/` untouched — rename and re-upload
   - `ffprobe` reads the duration
   - `audiowaveform` generates peak data (skipped for clips < 5 s)
   - Waveform JSON is uploaded to `waveforms/YYYY/MM/<id>.json`
   - Audio is copied to `audio/YYYY/MM/<filename>` then removed from `upload-here/`
   - `index.json` is updated (appended + sorted)

---

## Prerequisites

```bash
# ffmpeg / ffprobe
sudo apt update && sudo apt install ffmpeg

# audiowaveform — grab the pre-built arm64 .deb from GitHub releases:
VER=1.10.1
wget https://github.com/bbc/audiowaveform/releases/download/${VER}/audiowaveform_${VER}-1-12_arm64.deb
sudo apt install ./audiowaveform_${VER}-1-12_arm64.deb

# Node.js >= 18 (already installed)
```

---

## Install

```bash
git clone <this-repo> ~/barktown-ingest
cd ~/barktown-ingest
npm install
```

---

## Configuration

All settings are environment variables:

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `localhost` | MinIO host |
| `MINIO_PORT` | `9000` | MinIO port |
| `MINIO_USE_SSL` | `false` | Use HTTPS |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key |
| `MINIO_BUCKET` | `barktown` | Bucket name |
| `POLL_INTERVAL_MS` | `20000` | How often to scan `upload-here/` (ms) |
| `STABILITY_DELAY_MS` | `30000` | Idle time before processing a file (ms) |
| `FFPROBE_BIN` | `ffprobe` | Path to ffprobe binary |
| `AUDIOWAVEFORM_BIN` | `audiowaveform` | Path to audiowaveform binary |
| `WAVEFORM_THRESHOLD_SEC` | `5` | Min duration to generate a waveform |

---

## Running manually

```bash
MINIO_ACCESS_KEY=yourkey MINIO_SECRET_KEY=yoursecret node ingest-service.mjs
```

---

## Running as a systemd service

Edit `barktown-ingest.service` — set the `MINIO_*` credentials and adjust `WorkingDirectory` if needed. Then:

```bash
sudo cp barktown-ingest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now barktown-ingest
journalctl -u barktown-ingest -f    # follow logs
```

---

## MinIO bucket setup

> On this Pi `minio-client` is the CLI (not `mc`, which is Midnight Commander).

Create the bucket and the `upload-here/` prefix marker:

```bash
# Configure alias (once)
minio-client alias set local http://localhost:9000 minioadmin minioadmin

# Create bucket
minio-client mb local/barktown

# Create the upload-here/ prefix (upload an empty marker object)
echo "" | minio-client pipe local/barktown/upload-here/.keep

# Make the bucket publicly readable (so the SvelteKit app can fetch assets)
minio-client anonymous set download local/barktown
```

To manually inspect or remove a stuck file:

```bash
minio-client ls local/barktown/upload-here/
minio-client rm local/barktown/upload-here/"2026-01-17 15-42-00 bad name.m4a"
```

---

## Filename pattern

```
YYYY-MM-DD HH-MM-SS optional comment.m4a
YYYY-MM-DD HH-MM-SS optional comment.aac
```

- No trailing space before `.ext`
- Files not matching this pattern stay in `upload-here/` untouched

Examples of valid names:
```
2026-01-17 15-42-00 bark bark bark shot.m4a
2025-12-11 05-32-00.aac
2026-02-07 17-25-00 barks and yaps.m4a
```
