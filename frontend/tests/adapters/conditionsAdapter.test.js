import { describe, it, expect } from 'vitest';
import { adaptConditions } from '../../src/api/adapters/conditionsAdapter.js';
import { brisbaneBusy, melbourneSim, malformed } from '../fixtures.js';

describe('conditionsAdapter — merge weather + tides + ukc (Adapter Note §6)', () => {
  it('extracts weather fields from Brisbane fixture', () => {
    const c = adaptConditions(brisbaneBusy.weather, brisbaneBusy.tides, brisbaneBusy.ukc, 'Australia/Brisbane');
    expect(c.rating).toMatch(/^(EXCELLENT|GOOD|MODERATE|POOR|UNKNOWN)$/);
    expect(typeof c.windSpeedKts).toBe('number');
    expect(typeof c.swellHeightM).toBe('number');
    expect(typeof c.visibilityNm).toBe('number');
    expect(typeof c.pressureHpa).toBe('number');
  });
  it('produces visibilityKm from visibilityNm (1.852 conversion)', () => {
    const c = adaptConditions(brisbaneBusy.weather, brisbaneBusy.tides, brisbaneBusy.ukc, 'Australia/Brisbane');
    if (c.visibilityNm !== null) {
      expect(c.visibilityKm).toBeCloseTo(c.visibilityNm * 1.852, 1);
    }
  });
  it('extracts tides from Brisbane fixture', () => {
    const c = adaptConditions(brisbaneBusy.weather, brisbaneBusy.tides, brisbaneBusy.ukc, 'Australia/Brisbane');
    expect(typeof c.tideHeightM).toBe('number');
    expect(c.tideState).toMatch(/Rising|Falling/);
    expect(c.tideNextLabel).toMatch(/HW|LW/);
    expect(c.tideNextTime).toMatch(/^\d{2}:\d{2}$/);
  });
  it('extracts ukc from Brisbane fixture', () => {
    const c = adaptConditions(brisbaneBusy.weather, brisbaneBusy.tides, brisbaneBusy.ukc, 'Australia/Brisbane');
    expect(typeof c.ukcM).toBe('number');
    expect(typeof c.ukcStatus).toBe('string');
  });
  it('handles malformed weather visibility (string instead of number)', () => {
    const c = adaptConditions(malformed.weather, malformed.tides, malformed.ukc, 'Australia/Brisbane');
    expect(c.visibilityNm).toBe(null);
    expect(c.visibilityKm).toBe(null);
  });
  it('handles entirely missing input', () => {
    const c = adaptConditions(undefined, undefined, undefined, 'Australia/Brisbane');
    expect(c.rating).toBe('UNKNOWN');
    expect(c.windSpeedKts).toBe(null);
    expect(c.visibilityNm).toBe(null);
    expect(c.tideHeightM).toBe(null);
    expect(c.ukcM).toBe(null);
  });
  it('handles Melbourne fixture (different port)', () => {
    const c = adaptConditions(melbourneSim.weather, melbourneSim.tides, melbourneSim.ukc, 'Australia/Melbourne');
    expect(typeof c.windSpeedKts).toBe('number');
    expect(typeof c.swellHeightM).toBe('number');
  });
});
