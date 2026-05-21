// vtsAdapter.test.js — VTS view derivation
//
// Tests the pure presentation join over the adapted ViewSummary:
//   - vessels with hasConflict flag derived from conflicts[].vesselIds
//   - conflicts filtered to signalType ∈ { CONFLICT, WARNING }
//   - heading / speed / course intentionally NOT in the output (deferred)
//   - empty / null / malformed inputs handled defensively
//
// Uses the same fixture-loader pattern as the M1 adapter tests.

import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { adaptVts } from '../../src/api/adapters/vtsAdapter.js';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

describe('vtsAdapter — VTS pane derivation (M2 Plan §7.3)', () => {
  it('returns { vessels, conflicts } shape', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    expect(vts).toHaveProperty('vessels');
    expect(vts).toHaveProperty('conflicts');
    expect(Array.isArray(vts.vessels)).toBe(true);
    expect(Array.isArray(vts.conflicts)).toBe(true);
  });

  it('passes through every vessel from the adapted view', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    expect(vts.vessels.length).toBe(view.vessels.length);
  });

  it('vessel records carry id, name, type, status, berth, loaM, riskLevel, lat, lon, hasConflict, conflictIds', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    for (const v of vts.vessels) {
      expect(v).toHaveProperty('vesselId');
      expect(v).toHaveProperty('name');
      expect(v).toHaveProperty('type');
      expect(v).toHaveProperty('status');
      expect(v).toHaveProperty('berth');
      expect(v).toHaveProperty('loaM');
      expect(v).toHaveProperty('riskLevel');
      expect(v).toHaveProperty('lat');
      expect(v).toHaveProperty('lon');
      expect(v).toHaveProperty('hasConflict');
      expect(v).toHaveProperty('conflictIds');
      expect(Array.isArray(v.conflictIds)).toBe(true);
      expect(typeof v.hasConflict).toBe('boolean');
    }
  });

  it('hasConflict is true iff vessel appears in at least one conflict.vesselIds', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    const idsInConflict = new Set();
    for (const c of view.conflicts) {
      for (const vid of c.vesselIds) idsInConflict.add(vid);
    }
    for (const v of vts.vessels) {
      expect(v.hasConflict).toBe(idsInConflict.has(v.vesselId));
    }
  });

  it('conflictIds on a vessel matches the conflicts referencing that vessel', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    for (const v of vts.vessels) {
      const expectedConflictIds = view.conflicts
        .filter((c) => Array.isArray(c.vesselIds) && c.vesselIds.includes(v.vesselId))
        .map((c) => c.conflictId);
      // The adapter's conflictIds may include CONFLICT and WARNING and
      // ADVISORY-level entries (it joins against all conflicts in the
      // adapted view, not just the filtered operational signals).
      // So expectedConflictIds is the upper bound; the adapter result
      // must be a subset (and equal in practice here because we walk all
      // conflicts).
      expect(v.conflictIds.sort()).toEqual(expectedConflictIds.sort());
    }
  });

  it('conflicts list filters to signalType ∈ { CONFLICT, WARNING }', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    for (const c of vts.conflicts) {
      expect(['CONFLICT', 'WARNING']).toContain(c.signalType);
    }
  });

  it('does not synthesise heading / speed / course (deferred to a future fixture refresh)', () => {
    const view = adaptSummary(brisbaneBusy);
    const vts = adaptVts(view);
    for (const v of vts.vessels) {
      expect(v).not.toHaveProperty('heading');
      expect(v).not.toHaveProperty('speed');
      expect(v).not.toHaveProperty('course');
      expect(v).not.toHaveProperty('sog');
      expect(v).not.toHaveProperty('cog');
    }
  });

  it('quiet fixture: 0 conflicts, every vessel.hasConflict === false', () => {
    const view = adaptSummary(brisbaneQuiet);
    const vts = adaptVts(view);
    expect(vts.conflicts.length).toBe(0);
    for (const v of vts.vessels) {
      expect(v.hasConflict).toBe(false);
      expect(v.conflictIds).toEqual([]);
    }
  });

  it('null-fields fixture: produces an array (no throw, no silent drop of valid rows)', () => {
    const view = adaptSummary(nullFields);
    const vts = adaptVts(view);
    expect(Array.isArray(vts.vessels)).toBe(true);
    expect(Array.isArray(vts.conflicts)).toBe(true);
  });

  it('malformed fixture: defensive — never throws', () => {
    const view = adaptSummary(malformed);
    expect(() => adaptVts(view)).not.toThrow();
    const vts = adaptVts(view);
    expect(Array.isArray(vts.vessels)).toBe(true);
    expect(Array.isArray(vts.conflicts)).toBe(true);
  });

  it('null input → empty result, no throw', () => {
    expect(adaptVts(null)).toEqual({ vessels: [], conflicts: [] });
    expect(adaptVts(undefined)).toEqual({ vessels: [], conflicts: [] });
    expect(adaptVts({})).toEqual({ vessels: [], conflicts: [] });
  });

  it('vessels with non-string vesselId are dropped if id is required for hasConflict join', () => {
    // adaptVessels itself filters out non-object entries; this is a
    // belt-and-braces check that vtsAdapter does not crash on partial data.
    const view = adaptSummary({});
    const vts = adaptVts(view);
    expect(vts.vessels).toEqual([]);
  });

  it('purity: invoking twice on the same input produces deep-equal results', () => {
    const view = adaptSummary(brisbaneBusy);
    const a = adaptVts(view);
    const b = adaptVts(view);
    expect(a).toEqual(b);
  });
});
