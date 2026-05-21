// vts.test.jsx — M2 VTS tab component smoke tests.
//
// Uses react-dom/server.renderToStaticMarkup to exercise the component
// tree in Node without any DOM dependency (matches the M1 vite.config.js
// `environment: 'node'` setting; no new npm dependency added).
//
// The vtsAdapter is unit-tested separately in tests/adapters/vtsAdapter.test.js
// for derivation correctness. These tests confirm the components render
// without throwing and surface the expected human-readable copy.

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { VtsTab } from '../../src/features/vts/VtsTab.jsx';
import { VesselListPane } from '../../src/features/vts/VesselListPane.jsx';
import { ConflictsList } from '../../src/features/vts/ConflictsList.jsx';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('VtsTab — read-only VTS tab (M2 §8.3)', () => {
  it('renders without throwing on brisbane-busy', () => {
    const data = adaptSummary(brisbaneBusy);
    expect(() => render(<VtsTab data={data} />)).not.toThrow();
  });

  it('shows the read-only label', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<VtsTab data={data} />);
    expect(html).toMatch(/VTS · READ-ONLY/);
  });

  it('does NOT contain operator-action affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<VtsTab data={data} />);
    // Canon §10 anti-patterns and M2 Implementation Plan §10.3 (no DSW),
    // §12.5–§12.7 (no ACK / COMMIT / DEFER / OVERRIDE / ESCALATE / audit).
    // Use word boundaries + the uppercase token names from M2 §12 to avoid
    // matching legitimate copy like "deferred to a future fixture refresh".
    expect(html).not.toMatch(/\bACK\b/);
    expect(html).not.toMatch(/\bCOMMIT\b/);
    expect(html).not.toMatch(/\bDEFER\b/);
    expect(html).not.toMatch(/\bOVERRIDE\b/);
    expect(html).not.toMatch(/\bESCALATE\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/<form\b/);
  });

  it('surfaces the fixture-data footer disclosure', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<VtsTab data={data} />);
    expect(html).toMatch(/FIXTURE DATA/);
    expect(html).toMatch(/not a VTS replacement/);
  });

  it('renders without throwing on null-fields fixture', () => {
    const data = adaptSummary(nullFields);
    expect(() => render(<VtsTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on malformed fixture', () => {
    const data = adaptSummary(malformed);
    expect(() => render(<VtsTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on empty data', () => {
    const data = adaptSummary({});
    expect(() => render(<VtsTab data={data} />)).not.toThrow();
  });
});

describe('VesselListPane — vessels table (M2 §8.3)', () => {
  it('renders an empty state when there are no vessels', () => {
    const html = render(<VesselListPane vessels={[]} />);
    expect(html).toMatch(/NO VESSELS IN VIEW/);
    expect(html).not.toMatch(/<table\b/);
  });

  it('renders a table row per vessel', () => {
    const data = adaptSummary(brisbaneBusy);
    // Use the adapter's vessel output directly (already shape-correct).
    const v = data.vessels.map((vv) => ({
      vesselId: vv.vesselId,
      name: vv.name,
      type: vv.type,
      status: vv.status,
      berth: vv.berth,
      loaM: vv.loaM,
      riskLevel: vv.riskLevel,
      lat: vv.lat,
      lon: vv.lon,
      hasConflict: false,
      conflictIds: [],
    }));
    const html = render(<VesselListPane vessels={v} />);
    expect(html).toMatch(/<table[^>]*role="table"/);
    // first vessel name appears in the markup
    if (v[0]) expect(html).toContain(v[0].name);
  });

  it('surfaces hasConflict pill on conflicted vessels', () => {
    const html = render(
      <VesselListPane
        vessels={[
          {
            vesselId: 'V001',
            name: 'TEST VESSEL',
            type: 'Container',
            status: 'berthed',
            berth: 'B01',
            loaM: 200,
            riskLevel: 'medium',
            lat: -27.4,
            lon: 153.1,
            hasConflict: true,
            conflictIds: ['c1'],
          },
        ]}
      />
    );
    expect(html).toMatch(/YES/);
  });

  it('falls back to "—" for missing fields without throwing', () => {
    const html = render(
      <VesselListPane
        vessels={[
          {
            vesselId: 'X1',
            name: null,
            type: null,
            status: null,
            berth: null,
            loaM: null,
            riskLevel: null,
            lat: null,
            lon: null,
            hasConflict: false,
            conflictIds: [],
          },
        ]}
      />
    );
    expect(html).toMatch(/—/);
  });
});

describe('ConflictsList — filtered conflicts pane (M2 §8.3)', () => {
  it('renders an empty state when there are no conflicts', () => {
    const html = render(<ConflictsList conflicts={[]} />);
    expect(html).toMatch(/NO ACTIVE CONFLICTS/);
    expect(html).not.toMatch(/<ul\b/);
  });

  it('renders a row per conflict with severity + signal pills and title', () => {
    const html = render(
      <ConflictsList
        conflicts={[
          {
            conflictId: 'c1',
            type: 'berth_overlap',
            severity: 'CRITICAL',
            signalType: 'CONFLICT',
            title: 'Berth Overlap: ALPHA vs BETA',
            vesselIds: ['V1', 'V2'],
            vesselNames: ['ALPHA', 'BETA'],
            berth: 'B04',
            dataSource: 'simulated',
          },
        ]}
      />
    );
    expect(html).toMatch(/<ul[^>]*role="list"/);
    expect(html).toContain('Berth Overlap: ALPHA vs BETA');
    expect(html).toMatch(/CRITICAL/);
    expect(html).toMatch(/CONFLICT/);
    expect(html).toMatch(/ALPHA/);
    expect(html).toMatch(/B04/);
  });

  it('does NOT render operator-action affordances', () => {
    const html = render(
      <ConflictsList
        conflicts={[
          {
            conflictId: 'c1',
            type: 'berth_overlap',
            severity: 'HIGH',
            signalType: 'WARNING',
            title: 'Test',
            vesselIds: [],
            vesselNames: [],
            berth: null,
            dataSource: 'simulated',
          },
        ]}
      />
    );
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/\bAcknowledge\b/);
    expect(html).not.toMatch(/\b(ACK|COMMIT|DEFER|OVERRIDE|ESCALATE)\b/);
  });
});
