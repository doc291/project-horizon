// berthTimeline.test.jsx — M2 Berth Timeline component smoke tests.
//
// Uses react-dom/server.renderToStaticMarkup (no new dependency).

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { BerthTimelineTab } from '../../src/features/berth-timeline/BerthTimelineTab.jsx';
import { BerthRow } from '../../src/features/berth-timeline/BerthRow.jsx';
import { VesselSegment } from '../../src/features/berth-timeline/VesselSegment.jsx';
import { TimeAxis } from '../../src/features/berth-timeline/TimeAxis.jsx';
import {
  brisbaneBusy,
  brisbaneQuiet,
  nullFields,
  malformed,
} from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('BerthTimelineTab — read-only Berth Timeline (M2 §8.1)', () => {
  it('renders without throwing on brisbane-busy', () => {
    const data = adaptSummary(brisbaneBusy);
    expect(() => render(<BerthTimelineTab data={data} />)).not.toThrow();
  });

  it('shows the read-only label', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<BerthTimelineTab data={data} />);
    expect(html).toMatch(/BERTH TIMELINE · READ-ONLY/);
  });

  it('does NOT contain operator-action affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<BerthTimelineTab data={data} />);
    // Use word boundaries + uppercase tokens from M2 §12 to avoid
    // matching legitimate copy.
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

  it('renders one row per berth in the input', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<BerthTimelineTab data={data} />);
    for (const b of data.berths) {
      // Each berth row has its id rendered as the label.
      expect(html).toContain(b.id);
    }
  });

  it('shows the fixture-data footer disclosure', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<BerthTimelineTab data={data} />);
    expect(html).toMatch(/FIXTURE DATA/);
    expect(html).toMatch(/no rescheduling/);
  });

  it('renders without throwing on quiet fixture', () => {
    const data = adaptSummary(brisbaneQuiet);
    expect(() => render(<BerthTimelineTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on null-fields fixture', () => {
    const data = adaptSummary(nullFields);
    expect(() => render(<BerthTimelineTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on malformed fixture', () => {
    const data = adaptSummary(malformed);
    expect(() => render(<BerthTimelineTab data={data} />)).not.toThrow();
  });

  it('renders an empty / no-window state on empty data', () => {
    const data = adaptSummary({});
    const html = render(<BerthTimelineTab data={data} />);
    expect(html).toMatch(/NO SCHEDULED MOVEMENTS/);
  });
});

describe('BerthRow — single berth lane (M2 §8.1)', () => {
  it('renders the berth id, name, and status', () => {
    const row = {
      berthId: 'B01',
      name: 'DP World Berth 5',
      status: 'occupied',
      readinessTimeIso: '2026-05-20T14:04:22Z',
      maxLoaM: 350,
      segments: [],
    };
    const html = render(
      <BerthRow
        row={row}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-22T00:00:00Z"
      />
    );
    expect(html).toContain('B01');
    expect(html).toContain('DP World Berth 5');
    expect(html).toMatch(/Occupied/);
  });

  it('shows an empty notice when the lane has no segments', () => {
    const row = {
      berthId: 'B99',
      name: 'Empty Lane',
      status: 'available',
      readinessTimeIso: null,
      maxLoaM: null,
      segments: [],
    };
    const html = render(
      <BerthRow
        row={row}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-22T00:00:00Z"
      />
    );
    expect(html).toMatch(/No scheduled occupancy in this window/);
  });

  it('renders a vessel segment when present', () => {
    const row = {
      berthId: 'B01',
      name: 'B01',
      status: 'occupied',
      readinessTimeIso: null,
      maxLoaM: 350,
      segments: [
        {
          vesselId: 'V001',
          vesselName: 'TEST VESSEL',
          vesselType: 'Container',
          vesselStatus: 'berthed',
          startIso: '2026-05-20T06:00:00Z',
          endIso: '2026-05-20T18:00:00Z',
          scheduled: false,
          actualised: false,
          conflictIds: [],
          hasConflict: false,
        },
      ],
    };
    const html = render(
      <BerthRow
        row={row}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-22T00:00:00Z"
      />
    );
    expect(html).toContain('TEST VESSEL');
  });
});

describe('VesselSegment — single occupancy block (M2 §8.1)', () => {
  const baseSegment = {
    vesselId: 'V001',
    vesselName: 'ALPHA',
    vesselType: 'Container',
    vesselStatus: 'berthed',
    startIso: '2026-05-20T06:00:00Z',
    endIso: '2026-05-20T18:00:00Z',
    scheduled: false,
    actualised: false,
    conflictIds: [],
    hasConflict: false,
  };

  it('renders a positioned segment with the vessel name', () => {
    const html = render(
      <VesselSegment
        segment={baseSegment}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-21T00:00:00Z"
      />
    );
    expect(html).toContain('ALPHA');
    // Position style is in pct units. Should contain `left:` and `width:`.
    expect(html).toMatch(/left:\s*\d/);
    expect(html).toMatch(/width:\s*\d/);
  });

  it('shows the conflict indicator when hasConflict is true', () => {
    const html = render(
      <VesselSegment
        segment={{ ...baseSegment, hasConflict: true, conflictIds: ['c1', 'c2'] }}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-21T00:00:00Z"
      />
    );
    expect(html).toMatch(/aria-label="has conflict"/);
    expect(html).toMatch(/hz-bt-segment-conflict/);
  });

  it('returns null on missing window or bad timing', () => {
    expect(
      render(<VesselSegment segment={baseSegment} windowStartIso={null} windowEndIso={null} />)
    ).toBe('');
    expect(
      render(
        <VesselSegment
          segment={{ ...baseSegment, startIso: 'not-a-date', endIso: 'also-not' }}
          windowStartIso="2026-05-20T00:00:00Z"
          windowEndIso="2026-05-21T00:00:00Z"
        />
      )
    ).toBe('');
  });

  it('does NOT render interactive elements', () => {
    const html = render(
      <VesselSegment
        segment={baseSegment}
        windowStartIso="2026-05-20T00:00:00Z"
        windowEndIso="2026-05-21T00:00:00Z"
      />
    );
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/onclick=/i);
    expect(html).not.toMatch(/draggable=/i);
  });
});

describe('TimeAxis — read-only time axis (M2 §8.1)', () => {
  it('renders nothing on missing window', () => {
    expect(render(<TimeAxis timeWindow={null} />)).toBe('');
    expect(
      render(<TimeAxis timeWindow={{ startIso: null, endIso: null, durationMs: 0 }} />)
    ).toBe('');
  });

  it('renders a sequence of tick labels for a multi-hour window', () => {
    const html = render(
      <TimeAxis
        timeWindow={{
          startIso: '2026-05-20T00:00:00Z',
          endIso: '2026-05-20T12:00:00Z',
          durationMs: 12 * 3600 * 1000,
        }}
      />
    );
    // Expect at least one HH:00 tick label.
    expect(html).toMatch(/\d\d:00/);
    expect(html).toMatch(/aria-label="Timeline hours \(UTC\)"/);
  });
});
