import path from "path";
import { SAMPLE_LABELS } from "./sample-labels.mjs";

const SAMPLE_LABEL_SET = new Set(SAMPLE_LABELS);

/**
 * Build the MinIO keys used when a diary recording is turned back into a
 * training sample. The archived source mirrors the diary audio path, except
 * that it lives below uncompressed-uploads-archive/ and is the original WAV.
 */
export function buildDiarySampleMove(entry, requestedLabel, cfg) {
  const label = typeof requestedLabel === "string"
    ? requestedLabel.trim().toLowerCase()
    : "";
  if (!SAMPLE_LABEL_SET.has(label)) {
    throw new Error(`label must be one of: ${SAMPLE_LABELS.join(", ")}`);
  }

  const audioPath = typeof entry?.audioPath === "string" ? entry.audioPath : "";
  if (!audioPath.startsWith(cfg.audioPrefix)) {
    throw new Error(`diary audio path must be below ${cfg.audioPrefix}`);
  }

  const relativeAudioPath = audioPath.slice(cfg.audioPrefix.length);
  if (!relativeAudioPath || path.posix.basename(relativeAudioPath) === relativeAudioPath) {
    throw new Error("diary audio path must include year/month directories");
  }
  const sourceRelativePath = relativeAudioPath.replace(/\.[^.\/]+$/, ".wav");
  if (sourceRelativePath === relativeAudioPath) {
    throw new Error("diary audio path must have a file extension");
  }

  const datetimeMatch = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})$/.exec(entry?.datetimeLocal ?? "");
  if (!datetimeMatch) {
    throw new Error("diary entry must have a full local timestamp");
  }
  const [, date, hh, mm, ss] = datetimeMatch;
  const filename = `${date} ${hh}-${mm}-${ss} SAMPLE ${label}.wav`;

  return {
    label,
    filename,
    sourceKey: `${cfg.archivePrefix}${sourceRelativePath}`,
    destinationKey: `${cfg.samplesPrefix}${label}/${filename}`,
  };
}
