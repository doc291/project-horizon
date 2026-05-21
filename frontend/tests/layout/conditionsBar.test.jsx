// conditionsBar.test.jsx — Conditions strip rebalance invariants.
//
// Asserts the M2 visual-remediation slice has reorganised the
// conditions strip into three flex regions (anchor pill / centred
// tile group / symmetric spacer) while preserving the information
// hierarchy.
//
// No data binding is changed — the existing six environmental
// tiles (WIND / SWELL / VIS / PRESSURE / TIDE / UKC) remain. Only
// the visual rhythm changes.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { ConditionsBar } from '../../src/layout/ConditionsBar.jsx';

function render(node) {
  return renderToStaticMarkup(node);
}

const goodConditions = {
  rating: 'GOOD',
  windSpeedKts: 12,
  windDirLabel: 'SE',
  windBeaufort: 4,
  swellHeightM: 0.8,
  swellPeriodS: 7,
  swellDirLabel: 'E',
  visibilityNm: 10,
  visibilityKm: 18.5,
  pressureHpa: 1013,
  tideHeightM: 2.1,
  tideState: 'rising',
  tideNextLabel: 'HIGH',
  tideNextTime: '14:30',
  ukcM: 1.6,
  ukcStatus: 'good',
};

describe('ConditionsBar — three-region rebalance (M2 visual remediation)', () => {
  it('renders without throwing', () => {
    expect(() => render(<ConditionsBar conditions={goodConditions} />)).not.toThrow();
  });

  it('renders the anchor pill region (with the rating pill anchored left)', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    expect(html).toMatch(/<div[^>]*class="hz-conditions-anchor"/);
    expect(html).toMatch(/GOOD/);
  });

  it('renders the centred tiles region with all six environmental tiles', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    expect(html).toMatch(/<div[^>]*class="hz-conditions-tiles"/);
    expect(html).toMatch(/WIND/);
    expect(html).toMatch(/SWELL/);
    expect(html).toMatch(/VIS/);
    expect(html).toMatch(/PRESSURE/);
    expect(html).toMatch(/TIDE/);
    expect(html).toMatch(/UKC/);
  });

  it('renders the symmetric right spacer region', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    expect(html).toMatch(/<div[^>]*class="hz-conditions-spacer"[^>]*aria-hidden="true"/);
  });

  it('preserves the information hierarchy (no new metric introduced)', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    // Count of tile labels in the rendered markup — should be exactly 6.
    const labelCount = (
      html.match(/class="hz-cond-label"/g) || []
    ).length;
    expect(labelCount).toBe(6);
  });

  it('renders nothing when conditions are absent', () => {
    expect(render(<ConditionsBar conditions={null} />)).toBe('');
    expect(render(<ConditionsBar conditions={undefined} />)).toBe('');
  });

  it('falls back gracefully on unknown rating', () => {
    const html = render(
      <ConditionsBar conditions={{ ...goodConditions, rating: undefined }} />
    );
    expect(html).toMatch(/UNKNOWN/);
  });

  it('does NOT add operator-action affordances or filters', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/<select\b/);
  });

  it('does NOT introduce a port selector', () => {
    const html = render(<ConditionsBar conditions={goodConditions} />);
    expect(html).not.toMatch(/Select port|Switch port|Change port/i);
    expect(html).not.toMatch(/Brisbane|Melbourne|Geelong|Darwin/);
  });
});
