// centerPanel.test.jsx — M2 CenterPanel + renderActiveTab.
//
// Verifies:
//   - default render shows the Dashboard tab (M1-shipped default)
//   - renderActiveTab() switches between the six Canon §1.1.3 tab
//     components based on the activeTab key
//   - the nav strip is rendered inside the centre panel
//   - no port selector / port switcher appears

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { CenterPanel, renderActiveTab } from '../../src/layout/CenterPanel.jsx';
import { brisbaneBusy, brisbaneQuiet, nullFields, malformed } from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('CenterPanel — composes nav + active-tab body (M2 final slice)', () => {
  const data = adaptSummary(brisbaneBusy);

  it('renders without throwing', () => {
    expect(() => render(<CenterPanel data={data} />)).not.toThrow();
  });

  it('default render shows the Dashboard tab (M1-shipped default)', () => {
    const html = render(<CenterPanel data={data} />);
    // Dashboard's heading is "Port overview" (see DashboardTab.jsx).
    expect(html).toContain('Port overview');
    // The nav reports dashboard as active.
    expect(html).toMatch(/data-active-tab="dashboard"/);
  });

  it('renders the centre-panel nav strip inside the centre panel', () => {
    const html = render(<CenterPanel data={data} />);
    expect(html).toMatch(/<nav[^>]*class="hz-center-nav"/);
    expect(html).toMatch(/role="tablist"/);
    expect(html).toMatch(/aria-label="Centre panel view"/);
  });

  it('wraps the active tab in a tabpanel element keyed to the active tab', () => {
    const html = render(<CenterPanel data={data} />);
    expect(html).toMatch(/role="tabpanel"/);
    expect(html).toMatch(/id="hz-center-panel-dashboard"/);
  });

  it('renders without throwing across all fixtures (quiet / null-fields / malformed)', () => {
    for (const fix of [brisbaneQuiet, nullFields, malformed]) {
      const d = adaptSummary(fix);
      expect(() => render(<CenterPanel data={d} />)).not.toThrow();
    }
  });

  it('does NOT render a port selector / port switcher anywhere in the centre panel', () => {
    const html = render(<CenterPanel data={data} />);
    expect(html).not.toMatch(/Select port|Switch port|Change port/i);
    // No native port-selection controls.
    expect(html).not.toMatch(/<select\b[^>]*port/i);
  });
});

describe('renderActiveTab — pure tab-selection helper (M2 final slice)', () => {
  const data = adaptSummary(brisbaneBusy);

  function uniqueLabel(activeTab) {
    return render(renderActiveTab(activeTab, data));
  }

  it('dashboard → DashboardTab ("Port overview")', () => {
    expect(uniqueLabel('dashboard')).toContain('Port overview');
  });

  it('berth-timeline → BerthTimelineTab ("Berth timeline" or "BERTH TIMELINE")', () => {
    const html = uniqueLabel('berth-timeline');
    expect(html).toMatch(/Berth timeline/);
    expect(html).toMatch(/BERTH TIMELINE · READ-ONLY/);
  });

  it('shift-log → ShiftLogTab', () => {
    const html = uniqueLabel('shift-log');
    expect(html).toMatch(/SHIFT LOG · READ-ONLY/);
  });

  it('vts → VtsTab', () => {
    const html = uniqueLabel('vts');
    expect(html).toMatch(/VTS · READ-ONLY/);
  });

  it('pilotage → PilotageTab', () => {
    const html = uniqueLabel('pilotage');
    expect(html).toMatch(/PILOTAGE · READ-ONLY/);
  });

  it('performance → PerformanceTab placeholder', () => {
    const html = uniqueLabel('performance');
    expect(html).toMatch(/PERFORMANCE · DEFERRED/);
    expect(html).toMatch(/PLACEHOLDER · NO ANALYTICS/);
  });

  it('unknown / null / undefined activeTab → falls back to Dashboard', () => {
    expect(render(renderActiveTab(undefined, data))).toContain('Port overview');
    expect(render(renderActiveTab(null,      data))).toContain('Port overview');
    expect(render(renderActiveTab('xyz',     data))).toContain('Port overview');
  });
});
