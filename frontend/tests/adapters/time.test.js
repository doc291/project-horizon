import { describe, it, expect } from 'vitest';
import { parseIso, isoToDateOrNull, localShort, localFull, nowDate } from '../../src/api/adapters/time.js';

describe('time.js — ISO parsing', () => {
  it('parses a valid UTC ISO', () => {
    const d = parseIso('2026-05-20T04:00:00+00:00');
    expect(d).toBeInstanceOf(Date);
    expect(d.getUTCFullYear()).toBe(2026);
    expect(d.getUTCMonth()).toBe(4);
    expect(d.getUTCDate()).toBe(20);
  });
  it('returns null for non-string / empty / bad input', () => {
    expect(parseIso(null)).toBe(null);
    expect(parseIso(undefined)).toBe(null);
    expect(parseIso('')).toBe(null);
    expect(parseIso('not-a-date')).toBe(null);
    expect(parseIso(123)).toBe(null);
  });
});

describe('time.js — localShort port-local formatting', () => {
  it('formats Brisbane time correctly (UTC+10, no DST)', () => {
    // 04:00 UTC = 14:00 in Brisbane (AEST = UTC+10, year-round)
    expect(localShort('2026-05-20T04:00:00+00:00', 'Australia/Brisbane')).toBe('14:00');
  });
  it('formats Melbourne time correctly (DST-aware)', () => {
    // 04:00 UTC in May = AEST (UTC+10) for Melbourne = 14:00
    expect(localShort('2026-05-20T04:00:00+00:00', 'Australia/Melbourne')).toBe('14:00');
    // 04:00 UTC in January = AEDT (UTC+11) for Melbourne = 15:00
    expect(localShort('2026-01-20T04:00:00+00:00', 'Australia/Melbourne')).toBe('15:00');
  });
  it('returns null for invalid inputs', () => {
    expect(localShort(null, 'Australia/Brisbane')).toBe(null);
    expect(localShort('invalid', 'Australia/Brisbane')).toBe(null);
  });
  it('falls back to UTC if timezone invalid', () => {
    const result = localShort('2026-05-20T04:00:00+00:00', 'Mars/Phobos');
    expect(result).toBe('04:00');
  });
});

describe('time.js — localFull', () => {
  it('produces day-month-time string in port-local time', () => {
    const s = localFull('2026-05-20T04:00:00+00:00', 'Australia/Brisbane');
    expect(s).toMatch(/^[A-Z][a-z]{2} \d{2} [A-Z][a-z]{2} \d{2}:\d{2}$/);
    expect(s).toContain('14:00'); // Brisbane local
  });
});

describe('time.js — nowDate', () => {
  it('returns a Date approximately equal to now', () => {
    const before = Date.now();
    const d = nowDate();
    const after = Date.now();
    expect(d).toBeInstanceOf(Date);
    expect(d.getTime()).toBeGreaterThanOrEqual(before);
    expect(d.getTime()).toBeLessThanOrEqual(after);
  });
});
