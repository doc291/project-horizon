// shiftLog.test.jsx — Shift Log component smoke tests.
//
// Uses react-dom/server.renderToStaticMarkup (no new dependency).

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { ShiftLogTab } from '../../src/features/shift-log/ShiftLogTab.jsx';
import { ShiftLogRow } from '../../src/features/shift-log/ShiftLogRow.jsx';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('ShiftLogTab — read-only Shift Log (M2 §8.2)', () => {
  it('renders without throwing on brisbane-busy', () => {
    const data = adaptSummary(brisbaneBusy);
    expect(() => render(<ShiftLogTab data={data} />)).not.toThrow();
  });

  it('shows the read-only label', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    expect(html).toMatch(/SHIFT LOG · READ-ONLY/);
  });

  it('shows the derived-not-audit-ledger disclosure', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    expect(html).toMatch(/DERIVED · NOT AUDIT LEDGER/);
    expect(html).toMatch(/Not a substitute for the\s+official audit ledger/);
  });

  it('renders a table with the expected columns when rows exist', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    expect(html).toMatch(/<table[^>]*role="table"/);
    expect(html).toContain('Time (UTC)');
    expect(html).toContain('Type');
    expect(html).toContain('Severity');
    expect(html).toContain('Vessel / context');
    expect(html).toContain('Description');
  });

  it('does NOT contain operator-action affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
    expect(html).not.toMatch(/<select\b/);
  });

  it('does NOT render filter UI affordances (no filters in M2)', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    // The product copy says "No filters in M2." (a disclosure), but no
    // actual filter input / dropdown / button must appear. The broader
    // operator-action test above already asserts no <input> / <select> /
    // <form>; this test pins the specific filter case via labelling.
    expect(html).not.toMatch(/Filter by/i);
    expect(html).not.toMatch(/Search by/i);
    expect(html).not.toMatch(/<input[^>]*type="search"/);
    expect(html).not.toMatch(/<input[^>]*placeholder="[^"]*filter/i);
  });

  it('does NOT render export / download affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<ShiftLogTab data={data} />);
    expect(html).not.toMatch(/\bExport\b/);
    expect(html).not.toMatch(/\bDownload\b/);
    expect(html).not.toMatch(/<a[^>]*download/);
  });

  it('renders an explicit empty state when there are no events', () => {
    const data = adaptSummary({});
    const html = render(<ShiftLogTab data={data} />);
    expect(html).toMatch(/NO EVENTS/);
    expect(html).toMatch(/No events for current shift/);
  });

  it('renders without throwing on quiet fixture', () => {
    const data = adaptSummary(brisbaneQuiet);
    expect(() => render(<ShiftLogTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on null-fields fixture', () => {
    const data = adaptSummary(nullFields);
    expect(() => render(<ShiftLogTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on malformed fixture', () => {
    const data = adaptSummary(malformed);
    expect(() => render(<ShiftLogTab data={data} />)).not.toThrow();
  });
});

describe('ShiftLogRow — single event row (M2 §8.2)', () => {
  const baseEvent = {
    id: 'arr-V001',
    timestampIso: '2026-05-20T06:00:00Z',
    type: 'arrival',
    severity: 'INFO',
    vesselId: 'V001',
    vesselName: 'ALPHA',
    berth: 'B01',
    description: 'ALPHA arrived at B01.',
    source: 'derived',
    dataSource: 'simulated',
  };

  it('renders the UTC-formatted time, type pill, vessel and description', () => {
    const html = render(
      <table><tbody><ShiftLogRow event={baseEvent} /></tbody></table>
    );
    expect(html).toMatch(/UTC/);
    expect(html).toContain('Arrival');
    expect(html).toContain('ALPHA');
    expect(html).toContain('B01');
    expect(html).toContain('arrived');
  });

  it('shows "—" severity for INFO events; shows a pill for higher severities', () => {
    const info = render(
      <table><tbody><ShiftLogRow event={baseEvent} /></tbody></table>
    );
    expect(info).toMatch(/<span[^>]*hz-sl-cell-muted[^>]*>—<\/span>/);

    const high = render(
      <table><tbody>
        <ShiftLogRow event={{ ...baseEvent, type: 'conflict_detected', severity: 'HIGH', description: 'Berth overlap' }} />
      </tbody></table>
    );
    expect(high).toContain('HIGH');
  });

  it('falls back to "—" for missing vessel/description without throwing', () => {
    const html = render(
      <table><tbody>
        <ShiftLogRow event={{ ...baseEvent, vesselId: null, vesselName: null, berth: null, description: null }} />
      </tbody></table>
    );
    expect(html).toContain('—');
  });

  it('does NOT render interactive elements per row', () => {
    const html = render(
      <table><tbody><ShiftLogRow event={baseEvent} /></tbody></table>
    );
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/onclick=/i);
    expect(html).not.toMatch(/<a\b/);
  });
});
