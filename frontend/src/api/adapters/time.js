// time.js — Adapter Note §13 (ISO ↔ JS Date + port-local HH:MM helpers)
// DST-aware via date-fns-tz. Pure functions; no globals; no caching.

import { formatInTimeZone } from 'date-fns-tz';

export function parseIso(iso) {
  if (typeof iso !== 'string' || iso.length === 0) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  return d;
}

export function isoToDateOrNull(iso) {
  return parseIso(iso);
}

export function localShort(iso, timezone) {
  const d = parseIso(iso);
  if (!d) return null;
  try {
    return formatInTimeZone(d, timezone || 'UTC', 'HH:mm');
  } catch {
    // Invalid timezone or formatting error — return UTC short as fallback
    try {
      return formatInTimeZone(d, 'UTC', 'HH:mm');
    } catch {
      return null;
    }
  }
}

export function localFull(iso, timezone) {
  const d = parseIso(iso);
  if (!d) return null;
  try {
    return formatInTimeZone(d, timezone || 'UTC', 'EEE dd MMM HH:mm');
  } catch {
    try {
      return formatInTimeZone(d, 'UTC', 'EEE dd MMM HH:mm');
    } catch {
      return null;
    }
  }
}

export function nowDate() {
  return new Date();
}
