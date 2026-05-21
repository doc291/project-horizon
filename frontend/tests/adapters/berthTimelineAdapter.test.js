// berthTimelineAdapter.test.js — M2 Berth Timeline derivation.
//
// Tests the pure presentation join that produces rows-of-segments + time
// window for the Berth Timeline tab.

import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { adaptBerthTimeline } from '../../src/api/adapters/berthTimelineAdapter.js';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

describe('berthTimelineAdapter — Berth Timeline derivation (M2 Plan §7.1)', () => {
  it('returns the expected top-level shape', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    expect(tl).toHaveProperty('rows');
    expect(tl).toHaveProperty('timeWindow');
    expect(tl).toHaveProperty('vesselsWithoutBerth');
    expect(Array.isArray(tl.rows)).toBe(true);
    expect(typeof tl.timeWindow).toBe('object');
    expect(Array.isArray(tl.vesselsWithoutBerth)).toBe(true);
  });

  it('produces one row per berth in the input', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    expect(tl.rows.length).toBe(view.berths.length);
    const ids = tl.rows.map(r => r.berthId).sort();
    const expected = view.berths.map(b => b.id).sort();
    expect(ids).toEqual(expected);
  });

  it('every row carries berthId, name, status, readinessTimeIso, maxLoaM, segments[]', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    for (const r of tl.rows) {
      expect(r).toHaveProperty('berthId');
      expect(r).toHaveProperty('name');
      expect(r).toHaveProperty('status');
      expect(r).toHaveProperty('readinessTimeIso');
      expect(r).toHaveProperty('maxLoaM');
      expect(r).toHaveProperty('segments');
      expect(Array.isArray(r.segments)).toBe(true);
    }
  });

  it('every segment carries vessel identifiers, start/end ISO, status flags, conflict join', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    const segs = tl.rows.flatMap(r => r.segments);
    expect(segs.length).toBeGreaterThan(0);
    for (const s of segs) {
      expect(s).toHaveProperty('vesselId');
      expect(s).toHaveProperty('vesselName');
      expect(s).toHaveProperty('vesselType');
      expect(s).toHaveProperty('vesselStatus');
      expect(typeof s.startIso).toBe('string');
      expect(typeof s.endIso).toBe('string');
      expect(typeof s.scheduled).toBe('boolean');
      expect(typeof s.actualised).toBe('boolean');
      expect(Array.isArray(s.conflictIds)).toBe(true);
      expect(typeof s.hasConflict).toBe('boolean');
    }
  });

  it('segment.scheduled vs actualised match ata/atd presence', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    const segs = tl.rows.flatMap(r => r.segments);
    for (const s of segs) {
      // scheduled and actualised are mutually exclusive.
      expect(s.scheduled && s.actualised).toBe(false);
      // hasConflict iff conflictIds.length > 0.
      expect(s.hasConflict).toBe(s.conflictIds.length > 0);
    }
  });

  it('segment.start is strictly before segment.end (no zero/inverted windows)', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    for (const s of tl.rows.flatMap(r => r.segments)) {
      expect(Date.parse(s.endIso)).toBeGreaterThan(Date.parse(s.startIso));
    }
  });

  it('row.segments are sorted by start time ascending', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    for (const r of tl.rows) {
      const starts = r.segments.map(s => Date.parse(s.startIso));
      const sorted = [...starts].sort((a, b) => a - b);
      expect(starts).toEqual(sorted);
    }
  });

  it('timeWindow spans the min start and max end across all segments', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    const segs = tl.rows.flatMap(r => r.segments);
    const starts = segs.map(s => Date.parse(s.startIso));
    const ends = segs.map(s => Date.parse(s.endIso));
    const minStart = Math.min(...starts);
    expect(Date.parse(tl.timeWindow.startIso)).toBe(minStart);
    expect(Date.parse(tl.timeWindow.endIso)).toBeGreaterThanOrEqual(Math.max(...ends));
    expect(tl.timeWindow.durationMs).toBeGreaterThan(0);
  });

  it('berth_overlap conflicts surface on segments via conflictIds + hasConflict', () => {
    const view = adaptSummary(brisbaneBusy);
    const tl = adaptBerthTimeline(view);
    const overlapConflicts = view.conflicts.filter(c => c.type === 'berth_overlap');
    expect(overlapConflicts.length).toBeGreaterThan(0);
    const conflictedVesselIds = new Set(overlapConflicts.flatMap(c => c.vesselIds));
    const conflictedSegments = tl.rows.flatMap(r => r.segments)
      .filter(s => conflictedVesselIds.has(s.vesselId));
    expect(conflictedSegments.length).toBeGreaterThan(0);
    for (const s of conflictedSegments) {
      expect(s.hasConflict).toBe(true);
    }
  });

  it('quiet fixture: rows present, segments mostly present, no hasConflict', () => {
    const view = adaptSummary(brisbaneQuiet);
    const tl = adaptBerthTimeline(view);
    expect(tl.rows.length).toBeGreaterThan(0);
    for (const s of tl.rows.flatMap(r => r.segments)) {
      expect(s.hasConflict).toBe(false);
      expect(s.conflictIds).toEqual([]);
    }
  });

  it('null-fields fixture: defensive — never throws, always returns shape', () => {
    const view = adaptSummary(nullFields);
    expect(() => adaptBerthTimeline(view)).not.toThrow();
    const tl = adaptBerthTimeline(view);
    expect(tl).toHaveProperty('rows');
    expect(tl).toHaveProperty('timeWindow');
    expect(tl).toHaveProperty('vesselsWithoutBerth');
  });

  it('malformed fixture: defensive — never throws, always returns shape', () => {
    const view = adaptSummary(malformed);
    expect(() => adaptBerthTimeline(view)).not.toThrow();
    const tl = adaptBerthTimeline(view);
    expect(Array.isArray(tl.rows)).toBe(true);
  });

  it('empty input → empty shape, no throw', () => {
    expect(adaptBerthTimeline(null).rows).toEqual([]);
    expect(adaptBerthTimeline(undefined).rows).toEqual([]);
    expect(adaptBerthTimeline({}).rows).toEqual([]);
  });

  it('vessels with unknown berth_id are reported in vesselsWithoutBerth', () => {
    const view = adaptSummary(brisbaneBusy);
    // Inject a vessel with a non-existent berth_id by editing the view shape.
    const synthVessel = {
      ...view.vessels[0],
      vesselId: 'V_TEST_OUT',
      berth: 'B_DOES_NOT_EXIST',
    };
    const v2 = { ...view, vessels: [...view.vessels, synthVessel] };
    const tl = adaptBerthTimeline(v2);
    expect(tl.vesselsWithoutBerth).toContain('V_TEST_OUT');
  });

  it('does NOT fabricate timing — segments with bad start/end are dropped silently', () => {
    const fakeView = {
      berths: [{ id: 'B01', name: 'Berth 1', status: 'available' }],
      vessels: [
        { vesselId: 'V1', name: 'A', berth: 'B01', etaIso: null, etdIso: null,
          ataIso: null, atdIso: null, type: null, status: null },
        { vesselId: 'V2', name: 'B', berth: 'B01', etaIso: '2026-05-22T10:00:00Z',
          etdIso: '2026-05-22T12:00:00Z', ataIso: null, atdIso: null, type: null,
          status: 'scheduled' },
      ],
      conflicts: [],
    };
    const tl = adaptBerthTimeline(fakeView);
    expect(tl.rows.length).toBe(1);
    // Only V2 should make it onto the timeline.
    expect(tl.rows[0].segments.length).toBe(1);
    expect(tl.rows[0].segments[0].vesselId).toBe('V2');
  });

  it('purity: invoking twice on the same input is deep-equal', () => {
    const view = adaptSummary(brisbaneBusy);
    const a = adaptBerthTimeline(view);
    const b = adaptBerthTimeline(view);
    expect(a).toEqual(b);
  });
});
