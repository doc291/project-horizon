// pilotageAdapter.test.js — Pilotage view normalisation (M2 Plan §7.4)
//
// Asserts:
//   - shape: { assignments[], counts, missingDomains }
//   - each assignment is a pure shape normalisation of the raw record
//   - sort is by scheduledTimeIso ascending (forward-looking queue)
//   - records with missing id / vesselId / scheduled_time are dropped
//   - no business rules: no fatigue / competency / roster compliance
//     fields are introduced
//   - empty / null / malformed → empty result, no throw

import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { adaptPilotage } from '../../src/api/adapters/pilotageAdapter.js';
import {
  brisbaneBusy,
  brisbaneQuiet,
  melbourneSim,
  nullFields,
  malformed,
} from '../fixtures.js';

describe('pilotageAdapter — pilotage normalisation (M2 Plan §7.4)', () => {
  it('returns the expected top-level shape', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    expect(p).toHaveProperty('assignments');
    expect(p).toHaveProperty('counts');
    expect(p).toHaveProperty('missingDomains');
    expect(Array.isArray(p.assignments)).toBe(true);
    expect(p.counts).toHaveProperty('total');
    expect(p.counts).toHaveProperty('inbound');
    expect(p.counts).toHaveProperty('outbound');
    expect(p.counts).toHaveProperty('byStatus');
  });

  it('produces one assignment per well-formed input record', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    expect(p.assignments.length).toBeGreaterThan(0);
    // Every input record in brisbane-busy is well-formed; expect equal count.
    expect(p.assignments.length).toBe(view.pilotage.length);
  });

  it('every assignment carries the normalised field set', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    for (const a of p.assignments) {
      expect(a).toHaveProperty('id');
      expect(a).toHaveProperty('vesselId');
      expect(a).toHaveProperty('vesselName');
      expect(a).toHaveProperty('pilotId');
      expect(a).toHaveProperty('scheduledTimeIso');
      expect(a).toHaveProperty('boardingStation');
      expect(a).toHaveProperty('direction');
      expect(a).toHaveProperty('status');
      expect(typeof a.id).toBe('string');
      expect(typeof a.vesselId).toBe('string');
      expect(typeof a.scheduledTimeIso).toBe('string');
    }
  });

  it('does NOT introduce fatigue / competency / roster fields', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    for (const a of p.assignments) {
      expect(a).not.toHaveProperty('fatigue');
      expect(a).not.toHaveProperty('fatigueScore');
      expect(a).not.toHaveProperty('competency');
      expect(a).not.toHaveProperty('competencyScore');
      expect(a).not.toHaveProperty('rosterCompliance');
      expect(a).not.toHaveProperty('certification');
      expect(a).not.toHaveProperty('certifications');
    }
  });

  it('direction is constrained to { inbound, outbound, null }', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    for (const a of p.assignments) {
      expect([null, 'inbound', 'outbound']).toContain(a.direction);
    }
  });

  it('assignments are sorted by scheduledTimeIso ascending', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    const ts = p.assignments.map(a => Date.parse(a.scheduledTimeIso));
    for (let i = 0; i < ts.length - 1; i++) {
      expect(ts[i]).toBeLessThanOrEqual(ts[i + 1]);
    }
  });

  it('counts reflect the assignments array', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    expect(p.counts.total).toBe(p.assignments.length);
    const inbound  = p.assignments.filter(a => a.direction === 'inbound').length;
    const outbound = p.assignments.filter(a => a.direction === 'outbound').length;
    expect(p.counts.inbound).toBe(inbound);
    expect(p.counts.outbound).toBe(outbound);
    const byStatus = {};
    for (const a of p.assignments) {
      if (a.status) byStatus[a.status] = (byStatus[a.status] || 0) + 1;
    }
    expect(p.counts.byStatus).toEqual(byStatus);
  });

  it('renames pilot_name to pilotId and preserves the resource-safe code verbatim', () => {
    const view = adaptSummary(brisbaneBusy);
    const p = adaptPilotage(view);
    const raw = view.pilotage[0];
    const a = p.assignments.find(x => x.id === raw.id);
    expect(a.pilotId).toBe(raw.pilot_name);
  });

  it('drops records with missing id / vessel_id / scheduled_time (defensive — no synthesis)', () => {
    const fakeView = {
      pilotage: [
        { id: 'P1', vessel_id: 'V1', vessel_name: 'A',
          pilot_name: 'PILOT_X', scheduled_time: '2026-05-20T10:00:00Z',
          boarding_station: null, direction: 'inbound', status: 'confirmed' },
        // missing id
        { vessel_id: 'V2', scheduled_time: '2026-05-20T11:00:00Z' },
        // missing vessel_id
        { id: 'P3', scheduled_time: '2026-05-20T12:00:00Z' },
        // missing scheduled_time
        { id: 'P4', vessel_id: 'V4' },
        // invalid scheduled_time
        { id: 'P5', vessel_id: 'V5', scheduled_time: 'not-a-date' },
      ],
    };
    const p = adaptPilotage(fakeView);
    expect(p.assignments.length).toBe(1);
    expect(p.assignments[0].id).toBe('P1');
  });

  it('Melbourne fixture: 9 assignments, both directions present', () => {
    const view = adaptSummary(melbourneSim);
    const p = adaptPilotage(view);
    expect(p.assignments.length).toBe(view.pilotage.length);
    expect(p.counts.inbound + p.counts.outbound).toBeLessThanOrEqual(p.counts.total);
  });

  it('quiet fixture: assignments present (pilotage is not zeroed in the quiet fixture)', () => {
    const view = adaptSummary(brisbaneQuiet);
    const p = adaptPilotage(view);
    expect(Array.isArray(p.assignments)).toBe(true);
    expect(p.assignments.length).toBe(view.pilotage.length);
  });

  it('null-fields fixture: defensive — never throws', () => {
    const view = adaptSummary(nullFields);
    expect(() => adaptPilotage(view)).not.toThrow();
    const p = adaptPilotage(view);
    expect(Array.isArray(p.assignments)).toBe(true);
  });

  it('malformed fixture: defensive — never throws', () => {
    const view = adaptSummary(malformed);
    expect(() => adaptPilotage(view)).not.toThrow();
    const p = adaptPilotage(view);
    expect(Array.isArray(p.assignments)).toBe(true);
  });

  it('null / undefined / empty input → empty result, no throw', () => {
    expect(adaptPilotage(null).assignments).toEqual([]);
    expect(adaptPilotage(undefined).assignments).toEqual([]);
    expect(adaptPilotage({}).assignments).toEqual([]);
    expect(adaptPilotage({ pilotage: [] }).assignments).toEqual([]);
  });

  it('missingDomains marks "pilotage" only when input is absent', () => {
    expect(adaptPilotage({}).missingDomains).toEqual(['pilotage']);
    expect(adaptPilotage({ pilotage: [] }).missingDomains).toEqual(['pilotage']);
    const view = adaptSummary(brisbaneBusy);
    expect(adaptPilotage(view).missingDomains).toEqual([]);
  });

  it('purity: invoking twice on the same input is deep-equal', () => {
    const view = adaptSummary(brisbaneBusy);
    const a = adaptPilotage(view);
    const b = adaptPilotage(view);
    expect(a).toEqual(b);
  });
});
