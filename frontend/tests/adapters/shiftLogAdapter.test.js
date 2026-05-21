// shiftLogAdapter.test.js — Shift Log derivation.
//
// Asserts:
//   - rows are derived ONLY from explicit timestamped facts already in
//     ViewSummary (arrivals, departures, detected conflicts)
//   - no synthetic operator-action events are produced
//   - rows sort most-recent-first
//   - the derivation source label is always 'derived'
//   - empty / null / malformed input → empty rows, no throw
//   - missingDomains continues to declare shiftLog + auditLog as absent

import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { adaptShiftLog } from '../../src/api/adapters/shiftLogAdapter.js';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

describe('shiftLogAdapter — derived event log (M2 Plan §7.2)', () => {
  it('returns the expected top-level shape', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    expect(sl).toHaveProperty('rows');
    expect(sl).toHaveProperty('missingDomains');
    expect(Array.isArray(sl.rows)).toBe(true);
    expect(Array.isArray(sl.missingDomains)).toBe(true);
  });

  it('every row carries the expected fields', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    expect(sl.rows.length).toBeGreaterThan(0);
    for (const r of sl.rows) {
      expect(r).toHaveProperty('id');
      expect(r).toHaveProperty('timestampIso');
      expect(r).toHaveProperty('type');
      expect(r).toHaveProperty('severity');
      expect(r).toHaveProperty('vesselId');
      expect(r).toHaveProperty('vesselName');
      expect(r).toHaveProperty('berth');
      expect(r).toHaveProperty('description');
      expect(r).toHaveProperty('source');
      expect(r).toHaveProperty('dataSource');
      expect(typeof r.timestampIso).toBe('string');
      expect(r.source).toBe('derived');
    }
  });

  it('event types are limited to the three derived categories', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    for (const r of sl.rows) {
      expect(['arrival', 'departure', 'conflict_detected']).toContain(r.type);
    }
  });

  it('does NOT produce any operator-action / audit-style events', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    for (const r of sl.rows) {
      // No operator-action event types.
      expect(r.type).not.toMatch(/ack|acknowledg|defer|apply|override|escalat|commit/i);
      // No `actor` field (we never invent operator identity).
      expect(r).not.toHaveProperty('actor');
    }
  });

  it('arrival events match vessels with ata present', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    const arrivals = sl.rows.filter(r => r.type === 'arrival');
    const vesselsWithAta = view.vessels.filter(v => v.ataIso);
    expect(arrivals.length).toBe(vesselsWithAta.length);
  });

  it('departure events match vessels with atd present', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    const departures = sl.rows.filter(r => r.type === 'departure');
    const vesselsWithAtd = view.vessels.filter(v => v.atdIso);
    expect(departures.length).toBe(vesselsWithAtd.length);
  });

  it('conflict_detected events match conflicts with conflictTime present', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    const detected = sl.rows.filter(r => r.type === 'conflict_detected');
    const conflictsWithTs = view.conflicts.filter(c => c.conflictTimeIso);
    expect(detected.length).toBe(conflictsWithTs.length);
  });

  it('rows are sorted most-recent-first', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    const timestamps = sl.rows.map(r => Date.parse(r.timestampIso));
    for (let i = 0; i < timestamps.length - 1; i++) {
      expect(timestamps[i]).toBeGreaterThanOrEqual(timestamps[i + 1]);
    }
  });

  it('continues to declare shiftLog + auditLog as missing domains', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    expect(sl.missingDomains).toContain('shiftLog');
    expect(sl.missingDomains).toContain('auditLog');
  });

  it('quiet fixture: rows present (arrivals + any conflicts), no synthesised activity', () => {
    const view = adaptSummary(brisbaneQuiet);
    const sl = adaptShiftLog(view);
    expect(Array.isArray(sl.rows)).toBe(true);
    // brisbane-quiet has 0 conflicts — all rows here are arrivals/departures only.
    for (const r of sl.rows) {
      expect(['arrival', 'departure']).toContain(r.type);
    }
  });

  it('null-fields fixture: defensive — never throws', () => {
    const view = adaptSummary(nullFields);
    expect(() => adaptShiftLog(view)).not.toThrow();
    const sl = adaptShiftLog(view);
    expect(Array.isArray(sl.rows)).toBe(true);
  });

  it('malformed fixture: defensive — never throws', () => {
    const view = adaptSummary(malformed);
    expect(() => adaptShiftLog(view)).not.toThrow();
    const sl = adaptShiftLog(view);
    expect(Array.isArray(sl.rows)).toBe(true);
  });

  it('null input → empty rows, no throw', () => {
    expect(adaptShiftLog(null).rows).toEqual([]);
    expect(adaptShiftLog(undefined).rows).toEqual([]);
    expect(adaptShiftLog({}).rows).toEqual([]);
  });

  it('source label is ALWAYS "derived"', () => {
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    expect(sl.rows.length).toBeGreaterThan(0);
    for (const r of sl.rows) {
      expect(r.source).toBe('derived');
    }
  });

  it('does NOT produce events for future scheduled ETA/ETD (only past observations)', () => {
    // brisbane-busy has many scheduled / confirmed vessels with future eta/etd.
    // None of those should produce a row.
    const view = adaptSummary(brisbaneBusy);
    const sl = adaptShiftLog(view);
    // No arrival event should reference a vessel that doesn't have ata.
    const arrivals = sl.rows.filter(r => r.type === 'arrival');
    for (const a of arrivals) {
      const v = view.vessels.find(x => x.vesselId === a.vesselId);
      expect(v).toBeTruthy();
      expect(v.ataIso).toBeTruthy();
    }
  });

  it('purity: invoking twice on the same input is deep-equal', () => {
    const view = adaptSummary(brisbaneBusy);
    const a = adaptShiftLog(view);
    const b = adaptShiftLog(view);
    expect(a).toEqual(b);
  });
});
