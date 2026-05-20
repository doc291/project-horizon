import { describe, it, expect } from 'vitest';
import { adaptEtdRisk } from '../../src/api/adapters/etdRiskAdapter.js';
import { brisbaneBusy, malformed } from '../fixtures.js';

describe('etdRiskAdapter — etd_risk normalisation (Adapter Note §11)', () => {
  it('returns array of normalised entries', () => {
    const out = adaptEtdRisk(brisbaneBusy.etd_risk);
    expect(Array.isArray(out)).toBe(true);
    expect(out.length).toBe(brisbaneBusy.etd_risk.length);
  });
  it('renames vessel_id → vesselId, vessel_name → vesselName, risk_score → riskScore', () => {
    const out = adaptEtdRisk(brisbaneBusy.etd_risk);
    const r = out[0];
    expect(r.vesselId).toBe(brisbaneBusy.etd_risk[0].vessel_id);
    expect(r.vesselName).toBe(brisbaneBusy.etd_risk[0].vessel_name);
    expect(r.riskScore).toBe(brisbaneBusy.etd_risk[0].risk_score);
  });
  it('delta is ALWAYS null (Adapter Note §11.2 — never synthesised)', () => {
    const out = adaptEtdRisk(brisbaneBusy.etd_risk);
    out.forEach(r => {
      expect(r.delta).toBe(null);
    });
  });
  it('exposes first risk factor as reasonPrimary', () => {
    const out = adaptEtdRisk(brisbaneBusy.etd_risk);
    const r = out.find(r => r.riskFactors.length > 0);
    if (r) {
      expect(r.reasonPrimary).toBe(r.riskFactors[0]);
    }
  });
  it('handles malformed etd_risk (object instead of array) gracefully', () => {
    // malformed.json sets etd_risk = {}
    const out = adaptEtdRisk(malformed.etd_risk);
    expect(out).toEqual([]);
  });
  it('preserves riskLevel from closed set', () => {
    const out = adaptEtdRisk(brisbaneBusy.etd_risk);
    out.forEach(r => {
      expect(['critical', 'high', 'medium', 'low']).toContain(r.riskLevel);
    });
  });
  it('returns empty array on null', () => {
    expect(adaptEtdRisk(null)).toEqual([]);
    expect(adaptEtdRisk(undefined)).toEqual([]);
  });
});
