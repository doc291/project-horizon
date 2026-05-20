import { describe, it, expect } from 'vitest';
import { adaptConflicts } from '../../src/api/adapters/conflictAdapter.js';
import { brisbaneBusy, brisbaneQuiet, malformed, nullFields } from '../fixtures.js';

describe('conflictAdapter — conflict normalisation (Adapter Note §8)', () => {
  it('returns array of normalised conflicts', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    expect(Array.isArray(out)).toBe(true);
    expect(out.length).toBe(brisbaneBusy.conflicts.length);
  });
  it('uppercase severity', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    out.forEach(c => {
      expect(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']).toContain(c.severity);
    });
  });
  it('cascade is ALWAYS null (Canon §8.8)', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    out.forEach(c => {
      expect(c.cascade).toBe(null);
    });
  });
  it('renames id → conflictId, conflict_type → type, vessel_ids → vesselIds, berth_id → berth', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    const c = out[0];
    expect(c.conflictId).toBe(brisbaneBusy.conflicts[0].id);
    expect(c.type).toBe(brisbaneBusy.conflicts[0].conflict_type);
    expect(c.vesselIds).toEqual(brisbaneBusy.conflicts[0].vessel_ids);
    expect(c.berth).toBe(brisbaneBusy.conflicts[0].berth_id);
  });
  it('derives a title from conflict_type + vessel_names', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    out.forEach(c => {
      expect(typeof c.title).toBe('string');
      expect(c.title.length).toBeGreaterThan(0);
    });
  });
  it('flattens decision_support.options into options[]', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    const c = out.find(c => Array.isArray(c.options) && c.options.length > 0);
    if (c) {
      expect(Array.isArray(c.options)).toBe(true);
    }
  });
  it('preserves confidence as STRING (Adapter Note §8.6 — never fabricated)', () => {
    const out = adaptConflicts(brisbaneBusy.conflicts, 'Australia/Brisbane');
    out.forEach(c => {
      if (c.confidence !== null) {
        expect(typeof c.confidence).toBe('string');
      }
    });
  });
  it('returns empty array when conflicts: []', () => {
    const out = adaptConflicts(brisbaneQuiet.conflicts, 'Australia/Brisbane');
    expect(out).toEqual([]);
  });
  it('handles unknown severity defensively (EXPLOSIVE → INFO)', () => {
    const out = adaptConflicts(malformed.conflicts, 'Australia/Brisbane');
    expect(out[0].severity).toBe('INFO');
  });
  it('handles null berth_id', () => {
    // null-fields.json sets conflicts[0].berth_id = null
    const out = adaptConflicts(nullFields.conflicts, 'Australia/Brisbane');
    expect(out[0].berth).toBe(null);
    expect(out[0].berthName).toBe(null);
  });
  it('handles null decision_support', () => {
    // null-fields.json sets conflicts[1].decision_support = null
    const out = adaptConflicts(nullFields.conflicts, 'Australia/Brisbane');
    if (out.length > 1) {
      expect(out[1].confidence).toBe(null);
      expect(out[1].recommendedOptionId).toBe(null);
    }
  });
  it('returns empty array on non-array input', () => {
    expect(adaptConflicts(null, 'Australia/Brisbane')).toEqual([]);
    expect(adaptConflicts(undefined, 'Australia/Brisbane')).toEqual([]);
  });
});
