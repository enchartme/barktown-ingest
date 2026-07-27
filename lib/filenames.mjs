// lib/filenames.mjs — filename parsing/validation shared by the ingest
// service and maintenance scripts. Keep the two patterns in sync with the
// README's documented conventions.

// Diary recordings:  YYYY-MM-DD HH-MM-SS optional comment.(m4a|aac)
const FILENAME_RE =
  /^(\d{4}-\d{2}-\d{2}) (\d{2})-(\d{2})-(\d{2})(?:\s+(\S.*?))?\.(m4a|aac)$/i;

/** Slugify a filename stem into a stable, URL/filesystem-safe id. */
function slugify(stem) {
  return stem
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/, "");
}

/** Parse a diary recording filename. Returns null if it doesn't match. */
export function parseFilename(filename) {
  const match = FILENAME_RE.exec(filename);
  if (!match) return null;

  const [, datePart, hh, mm, ss, rawLabel] = match;
  const label = rawLabel ? rawLabel.trim() : "";
  const date = datePart;
  const time = `${hh}:${mm}`;
  const datetimeLocal = `${date}T${hh}:${mm}:${ss}`;

  const ext = filename.match(/\.(m4a|aac)$/i)[0];
  const stem = filename.slice(0, -ext.length);
  const id = slugify(stem);

  return { date, time, datetimeLocal, label, id };
}

// Training samples (uploaded by barktown-goblin):
//   YYYY-MM-DD HH-MM-SS SAMPLE <label>.wav
const SAMPLE_FILENAME_RE =
  /^(\d{4}-\d{2}-\d{2}) (\d{2})-(\d{2})-(\d{2}) SAMPLE ([a-z]+)\.wav$/i;

/** Parse a training-sample filename. Returns null if it doesn't match. */
export function parseSampleFilename(filename) {
  const match = SAMPLE_FILENAME_RE.exec(filename);
  if (!match) return null;
  const [, datePart, hh, mm, ss, label] = match;
  const datetimeLocal = `${datePart}T${hh}:${mm}:${ss}`;
  const stem = filename.slice(0, -".wav".length);
  const id = slugify(stem);
  return { date: datePart, datetimeLocal, label: label.toLowerCase(), id };
}
