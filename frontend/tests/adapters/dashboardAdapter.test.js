import { describe, it, expect } from 'vitest';
import { adaptDashboard } from '../../src/api/adapters/dashboardAdapter.js';
import { brisbaneBusy, brisbaneQuiet, melbourneSim, malformed } from '../fixtures.js';

describe('dashboardAdapter — dashboard + port_status merge (Adapter Note §10)', () => {
  it('extracts core KPI fields from Brisbane busy', () => {
    const d = adaptDashboard(brisbaneBusy.dashboard, brisbaneBusy.port_status);
    expect(typeof d.berthUtilisationPct).toBe('number');
    expect(typeof d.onTimeDeparturePct).toBe('number');
    expect(typeof d.vesselsInPort).toBe('number');
    expect(typeof d.activeConflicts).toBe('number');
  });
  it('renames snake_case → camelCase', () => {
    const d = adaptDashboard(brisbaneBusy.dashboard, brisbaneBusy.port_status);
    expect(d.berthUtilisationPct).toBe(brisbaneBusy.dashboard.berth_utilisation_pct);
    expect(d.forecastUtilisation48h).toBe(brisbaneBusy.dashboard.forecast_utilisation_48h);
    expect(d.onTimeDeparturePct).toBe(brisbaneBusy.dashboard.on_time_departure_pct);
    expect(d.pilotOps12h).toBe(brisbaneBusy.dashboard.pilot_ops_12h);
    expect(d.tugOps12h).toBe(brisbaneBusy.dashboard.tug_ops_12h);
    expect(d.vesselsAtRisk).toBe(brisbaneBusy.dashboard.vessels_at_risk);
  });
  it('zero counts on quiet fixture', () => {
    const d = adaptDashboard(brisbaneQuiet.dashboard, brisbaneQuiet.port_status);
    expect(d.activeConflicts).toBe(0);
    expect(d.criticalConflicts).toBe(0);
    expect(d.vesselsAtRisk).toBe(0);
  });
  it('handles missing input', () => {
    const d = adaptDashboard(undefined, undefined);
    expect(d.berthUtilisationPct).toBe(null);
    expect(d.activeConflicts).toBe(0);
    expect(d.criticalConflicts).toBe(0);
    expect(d.vesselsAtRisk).toBe(0);
  });
  it('passes out-of-range value through (UI clamps for display)', () => {
    // malformed.json sets berth_utilisation_pct = 200
    const d = adaptDashboard(malformed.dashboard, malformed.port_status);
    expect(d.berthUtilisationPct).toBe(200);
  });
  it('handles Melbourne fixture (different vessel counts)', () => {
    const d = adaptDashboard(melbourneSim.dashboard, melbourneSim.port_status);
    expect(typeof d.vesselsInPort).toBe('number');
  });
});
