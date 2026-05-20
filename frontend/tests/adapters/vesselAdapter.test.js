import { describe, it, expect } from 'vitest';
import { adaptVessels } from '../../src/api/adapters/vesselAdapter.js';
import { brisbaneBusy, nullFields, malformed } from '../fixtures.js';

describe('vesselAdapter — vessel normalisation + risk join (Adapter Note §7)', () => {
  it('returns array of normalised vessels', () => {
    const out = adaptVessels(brisbaneBusy.vessels, brisbaneBusy.etd_risk, 'Australia/Brisbane');
    expect(Array.isArray(out)).toBe(true);
    expect(out.length).toBe(brisbaneBusy.vessels.length);
  });
  it('renames id → vesselId, vessel_type → type, draught → draftM, cargo_type → cargo, berth_id → berth', () => {
    const out = adaptVessels(brisbaneBusy.vessels, brisbaneBusy.etd_risk, 'Australia/Brisbane');
    const v = out[0];
    expect(v.vesselId).toBe(brisbaneBusy.vessels[0].id);
    expect(v.type).toBe(brisbaneBusy.vessels[0].vessel_type);
    expect(v.draftM).toBe(brisbaneBusy.vessels[0].draught);
    expect(v.cargo).toBe(brisbaneBusy.vessels[0].cargo_type);
    expect(v.berth).toBe(brisbaneBusy.vessels[0].berth_id);
  });
  it('joins risk data from etd_risk by vesselId', () => {
    const out = adaptVessels(brisbaneBusy.vessels, brisbaneBusy.etd_risk, 'Australia/Brisbane');
    const v = out.find(v => brisbaneBusy.etd_risk.some(r => r.vessel_id === v.vesselId));
    expect(v).toBeTruthy();
    expect(typeof v.riskScore).toBe('number');
    expect(['critical', 'high', 'medium', 'low']).toContain(v.riskLevel);
    expect(Array.isArray(v.riskFactors)).toBe(true);
  });
  it('defaults risk to 0/low/[] for vessels without etd_risk entry', () => {
    // null-fields fixture has etd_risk truncated by 2 — exercises the join-miss path
    const out = adaptVessels(nullFields.vessels, nullFields.etd_risk, 'Australia/Brisbane');
    const unjoined = out.filter(v => !nullFields.etd_risk.some(r => r.vessel_id === v.vesselId));
    expect(unjoined.length).toBeGreaterThan(0);
    unjoined.forEach(v => {
      expect(v.riskScore).toBe(0);
      expect(v.riskLevel).toBe('low');
      expect(v.riskFactors).toEqual([]);
    });
  });
  it('preserves null eta/etd/ata/atd', () => {
    const out = adaptVessels(nullFields.vessels, nullFields.etd_risk, 'Australia/Brisbane');
    // vessels[0].etd was set to null in null-fields.json
    expect(out[0].etdIso).toBe(null);
    expect(out[0].etd).toBe(null);
    expect(out[0].etdLocalShort).toBe(null);
  });
  it('handles malformed loa (string) defensively', () => {
    const out = adaptVessels(malformed.vessels, malformed.etd_risk, 'Australia/Brisbane');
    // vessels[0].loa was "big" in malformed.json
    expect(out[0].loaM).toBe(null);
  });
  it('handles missing source field as Simulation', () => {
    const out = adaptVessels(nullFields.vessels, nullFields.etd_risk, 'Australia/Brisbane');
    // vessels[3].source = null and vessels[4].source removed
    expect(out[3].source).toBe('Simulation');
    expect(out[4].source).toBe('Simulation');
  });
  it('returns empty array on non-array input', () => {
    expect(adaptVessels(null, [], 'Australia/Brisbane')).toEqual([]);
    expect(adaptVessels(undefined, [], 'Australia/Brisbane')).toEqual([]);
    expect(adaptVessels('not-an-array', [], 'Australia/Brisbane')).toEqual([]);
  });
  it('produces etaLocalShort in HH:mm port-local format when eta present', () => {
    const out = adaptVessels(brisbaneBusy.vessels, brisbaneBusy.etd_risk, 'Australia/Brisbane');
    const v = out.find(v => v.etaIso);
    if (v) {
      expect(v.etaLocalShort).toMatch(/^\d{2}:\d{2}$/);
      expect(v.eta).toBeInstanceOf(Date);
    }
  });
});
