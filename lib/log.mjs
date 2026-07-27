// lib/log.mjs — shared timestamped console logging helpers.

export function ts() {
  return new Date().toISOString();
}

export function log(...a) {
  console.log(`[${ts()}]`, ...a);
}

export function warn(...a) {
  console.warn(`[${ts()}] WARN`, ...a);
}

export function err(...a) {
  console.error(`[${ts()}] ERROR`, ...a);
}
