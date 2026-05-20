import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { brisbaneBusy, brisbaneQuiet, melbourneSim, nullFields, malformed } from '../fixtures.js';

const REQUIRED_KEYS = [
  'timestamp', 'generatedAtIso', 'portId', 'portName', 'portTimezone',
  'portStatus', 'conditions', 'vessels', 'berths', 'conflicts', 'guidance',
  'pilotage', 'towage', 'dashboardMetrics', 'etdRisk', 'liveness',
  'dataSource', 'dataSourceLabel', 'availablePorts', 'missingDomains',
];

describe('summaryAdapter — top-level ViewSummary (Adapter Note §5)', () => {
  it.each([
    ['brisbaneBusy', brisbaneBusy],
    ['brisbaneQuiet', brisbaneQuiet],
    ['melbourneSim', melbourneSim],
    ['nullFields', nullFields],
    ['malformed', malformed],
  ])('produces complete ViewSummary shape for %s fixture', (name, raw) => {
    const view = adaptSummary(raw);
    REQUIRED_KEYS.forEach(key => {
      expect(view).toHaveProperty(key);
    });
  });

  it('extracts port identity correctly (Brisbane)', () => {
    const v = adaptSummary(brisbaneBusy);
    expect(v.portName).toBe('Port of Brisbane');
    expect(v.portId).toBe('BRISBANE');
    expect(v.portTimezone).toMatch(/Brisbane|Sydney|Australia/);
  });

  it('extracts port identity correctly (Melbourne)', () => {
    const v = adaptSummary(melbourneSim);
    expect(v.portName).toBe('Port of Melbourne');
    expect(v.portId).toBe('MELBOURNE');
  });

  it('missingDomains always includes shiftLog + auditLog + cascade', () => {
    const v = adaptSummary(brisbaneBusy);
    expect(v.missingDomains).toContain('shiftLog');
    expect(v.missingDomains).toContain('auditLog');
    expect(v.missingDomains).toContain('cascade');
  });

  it('liveness flags sourced from port_profile', () => {
    const v = adaptSummary(brisbaneBusy);
    expect(typeof v.liveness.vessel).toBe('boolean');
    expect(typeof v.liveness.weather).toBe('boolean');
    expect(typeof v.liveness.tide).toBe('boolean');
    expect(typeof v.liveness.isLive).toBe('boolean');
  });

  it('availablePorts is an array', () => {
    const v = adaptSummary(brisbaneBusy);
    expect(Array.isArray(v.availablePorts)).toBe(true);
  });

  // Empty-state test (Adapter Note §15.7) — adapter always returns parseable ViewSummary
  it('returns a complete ViewSummary for empty input {}', () => {
    const v = adaptSummary({});
    REQUIRED_KEYS.forEach(key => expect(v).toHaveProperty(key));
    expect(v.vessels).toEqual([]);
    expect(v.conflicts).toEqual([]);
    expect(v.guidance).toEqual([]);
    expect(v.etdRisk).toEqual([]);
    expect(v.berths).toEqual([]);
    expect(v.pilotage).toEqual([]);
    expect(v.towage).toEqual([]);
  });

  it('returns a complete ViewSummary for null input', () => {
    const v = adaptSummary(null);
    REQUIRED_KEYS.forEach(key => expect(v).toHaveProperty(key));
  });

  it('returns a complete ViewSummary for undefined input', () => {
    const v = adaptSummary(undefined);
    REQUIRED_KEYS.forEach(key => expect(v).toHaveProperty(key));
  });

  it('returns a complete ViewSummary for malformed input', () => {
    const v = adaptSummary(malformed);
    REQUIRED_KEYS.forEach(key => expect(v).toHaveProperty(key));
    // port_profile was deleted → portTimezone defaults to Brisbane
    expect(v.portTimezone).toBe('Australia/Brisbane');
  });

  // Snapshot tests — one per fixture (Adapter Note §16.3 #1)
  it('matches snapshot for brisbaneBusy', () => {
    const v = adaptSummary(brisbaneBusy);
    // Snapshot only stable subset (omit Date objects which serialise unpredictably)
    expect({
      portId: v.portId, portName: v.portName, portTimezone: v.portTimezone,
      missingDomains: v.missingDomains,
      vesselCount: v.vessels.length,
      conflictCount: v.conflicts.length,
      etdRiskCount: v.etdRisk.length,
      liveness: v.liveness,
      dashboardMetricsKeys: Object.keys(v.dashboardMetrics).sort(),
      conditionsKeys: Object.keys(v.conditions).sort(),
    }).toMatchSnapshot();
  });

  it('matches snapshot for brisbaneQuiet', () => {
    const v = adaptSummary(brisbaneQuiet);
    expect({
      portId: v.portId,
      vesselCount: v.vessels.length,
      conflictCount: v.conflicts.length,
      activeConflicts: v.dashboardMetrics.activeConflicts,
      criticalConflicts: v.dashboardMetrics.criticalConflicts,
    }).toMatchSnapshot();
  });

  it('matches snapshot for melbourneSim', () => {
    const v = adaptSummary(melbourneSim);
    expect({
      portId: v.portId, portName: v.portName, portTimezone: v.portTimezone,
      vesselCount: v.vessels.length,
      conflictCount: v.conflicts.length,
    }).toMatchSnapshot();
  });

  // Round-trip test (Adapter Note §16.3 #5)
  it('survives JSON round-trip', () => {
    const v1 = adaptSummary(brisbaneBusy);
    // Dates serialise to ISO strings; re-parsing produces equivalent values
    const cloned = JSON.parse(JSON.stringify(v1));
    expect(cloned.portId).toBe(v1.portId);
    expect(cloned.portName).toBe(v1.portName);
    expect(cloned.vessels.length).toBe(v1.vessels.length);
    expect(cloned.conflicts.length).toBe(v1.conflicts.length);
    expect(cloned.missingDomains).toEqual(v1.missingDomains);
  });
});
