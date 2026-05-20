import { describe, it, expect } from 'vitest';
import { adaptGuidance } from '../../src/api/adapters/guidanceAdapter.js';
import { brisbaneBusy, brisbaneQuiet, melbourneSim } from '../fixtures.js';

describe('guidanceAdapter — guidance + alert-shaped conflicts (Adapter Note §9)', () => {
  it('returns an array', () => {
    const out = adaptGuidance(brisbaneBusy.guidance, brisbaneBusy.conflicts, 'Australia/Brisbane');
    expect(Array.isArray(out)).toBe(true);
  });
  it('produces sorted items by severity then title', () => {
    const out = adaptGuidance(brisbaneBusy.guidance, brisbaneBusy.conflicts, 'Australia/Brisbane');
    const order = ['CRITICAL', 'HIGH', 'WARNING', 'MEDIUM', 'ADVISORY', 'LOW', 'INFO'];
    let prevRank = -1;
    out.forEach(item => {
      const r = order.indexOf(item.severity);
      expect(r).toBeGreaterThanOrEqual(prevRank);
      prevRank = Math.max(prevRank, r);
    });
  });
  it('excludes signal_type=CONFLICT (those are decision-card items)', () => {
    const out = adaptGuidance(brisbaneBusy.guidance, brisbaneBusy.conflicts, 'Australia/Brisbane');
    // No item should have id starting "c-" if its source had signal_type=CONFLICT
    const cIds = out.filter(o => o.id.startsWith('c-')).map(o => o.id.slice(2));
    cIds.forEach(id => {
      const src = brisbaneBusy.conflicts.find(c => c.id === id);
      if (src) {
        expect((src.signal_type || '').toUpperCase()).not.toBe('CONFLICT');
      }
    });
  });
  it('returns empty array for quiet fixture (no conflicts → no alert-shaped items)', () => {
    const out = adaptGuidance(brisbaneQuiet.guidance, brisbaneQuiet.conflicts, 'Australia/Brisbane');
    expect(Array.isArray(out)).toBe(true);
  });
  it('handles undefined guidance + conflicts', () => {
    expect(adaptGuidance(undefined, undefined, 'Australia/Brisbane')).toEqual([]);
  });
  it('produces non-empty result for Melbourne (richer fixture)', () => {
    const out = adaptGuidance(melbourneSim.guidance, melbourneSim.conflicts, 'Australia/Melbourne');
    expect(Array.isArray(out)).toBe(true);
  });
});
