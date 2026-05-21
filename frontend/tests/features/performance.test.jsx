// performance.test.jsx — Performance placeholder tab smoke tests.
//
// The Performance tab is a parked surface in M2 — no adapter, no data
// binding, no analytics, no charts, no export. These tests pin the
// placeholder invariants.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { PerformanceTab } from '../../src/features/performance/PerformanceTab.jsx';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('PerformanceTab — placeholder (M2 §8.5)', () => {
  it('renders without throwing (no props required)', () => {
    expect(() => render(<PerformanceTab />)).not.toThrow();
  });

  it('does not consume any data prop (placeholder semantics)', () => {
    // Passing or omitting `data` must produce identical markup — the
    // placeholder has no data binding.
    const a = render(<PerformanceTab />);
    const b = render(<PerformanceTab data={{ portStatus: {}, vessels: [] }} />);
    expect(a).toBe(b);
  });

  it('shows the DEFERRED label', () => {
    const html = render(<PerformanceTab />);
    expect(html).toMatch(/PERFORMANCE · DEFERRED/);
  });

  it('shows the PLACEHOLDER · NO ANALYTICS footer', () => {
    const html = render(<PerformanceTab />);
    expect(html).toMatch(/PLACEHOLDER · NO ANALYTICS/);
    expect(html).toMatch(/no charts/);
    expect(html).toMatch(/no calculations/);
    expect(html).toMatch(/no export/);
  });

  it('says explicitly that Performance is deferred and parked for M2', () => {
    const html = render(<PerformanceTab />);
    expect(html).toMatch(/Performance analytics will be introduced in a later milestone/);
    expect(html).toMatch(/parked for M2/);
    expect(html).toMatch(/No live performance calculations are active in M2/);
  });

  it('lists future-scope items as plain text bullets (not active features)', () => {
    const html = render(<PerformanceTab />);
    expect(html).toMatch(/Future scope may include/);
    // The six suggested items.
    expect(html).toMatch(/Throughput/);
    expect(html).toMatch(/Berth utilisation/);
    expect(html).toMatch(/Decision outcomes/);
    expect(html).toMatch(/Delay trends/);
    expect(html).toMatch(/Resource utilisation/);
    expect(html).toMatch(/Operational reliability/);
    expect(html).toMatch(/Not authorised scope/);
  });

  it('does NOT render any chart / canvas / svg analytics primitive', () => {
    const html = render(<PerformanceTab />);
    expect(html).not.toMatch(/<canvas\b/);
    expect(html).not.toMatch(/<svg\b/);
    expect(html).not.toMatch(/recharts|chart\.js|d3/i);
  });

  it('does NOT render any data table', () => {
    const html = render(<PerformanceTab />);
    expect(html).not.toMatch(/<table\b/);
    expect(html).not.toMatch(/<th\b/);
    expect(html).not.toMatch(/<tbody\b/);
  });

  it('does NOT render operator-action affordances or export controls', () => {
    const html = render(<PerformanceTab />);
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);  // word-boundary; "deferred" is allowed
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
    expect(html).not.toMatch(/\bExport\b/);
    expect(html).not.toMatch(/\bDownload\b/);
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/<select\b/);
    expect(html).not.toMatch(/<a[^>]*download/);
  });

  it('does NOT introduce Smart Ocean X framing or Beta 10 references', () => {
    const html = render(<PerformanceTab />);
    expect(html).not.toMatch(/Smart Ocean X/i);
    expect(html).not.toMatch(/Beta 10/i);
  });
});
