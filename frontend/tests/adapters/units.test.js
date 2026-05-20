import { describe, it, expect } from 'vitest';
import { nmToKm, kmToNm, NM_TO_KM } from '../../src/api/adapters/units.js';

describe('units.js — nm ↔ km conversion (Adapter Note §13.4)', () => {
  it('converts whole nautical miles correctly', () => {
    expect(nmToKm(1)).toBeCloseTo(1.852, 3);
    expect(nmToKm(10)).toBeCloseTo(18.52, 2);
    expect(nmToKm(6.5)).toBeCloseTo(12.0, 1);
  });
  it('uses 1.852 conversion factor', () => {
    expect(NM_TO_KM).toBe(1.852);
  });
  it('returns null for non-finite input', () => {
    expect(nmToKm(null)).toBe(null);
    expect(nmToKm(undefined)).toBe(null);
    expect(nmToKm('foggy')).toBe(null);
    expect(nmToKm(NaN)).toBe(null);
    expect(nmToKm(Infinity)).toBe(null);
  });
  it('round-trips approximately', () => {
    const km = nmToKm(7.2);
    const nm = kmToNm(km);
    expect(nm).toBeCloseTo(7.2, 1);
  });
});
