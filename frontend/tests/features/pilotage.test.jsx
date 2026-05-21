// pilotage.test.jsx — Pilotage tab component smoke tests.
//
// Uses react-dom/server.renderToStaticMarkup (no new dependency).

import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, it, expect } from 'vitest';
import { adaptSummary } from '../../src/api/adapters/summaryAdapter.js';
import { PilotageTab } from '../../src/features/pilotage/PilotageTab.jsx';
import { PilotageAssignmentRow } from '../../src/features/pilotage/PilotageAssignmentRow.jsx';
import {
  brisbaneBusy,
  brisbaneQuiet,
  melbourneSim,
  nullFields,
  malformed,
} from '../fixtures.js';

function render(node) {
  return renderToStaticMarkup(node);
}

describe('PilotageTab — read-only Pilotage (M2 §8.4)', () => {
  it('renders without throwing on brisbane-busy', () => {
    const data = adaptSummary(brisbaneBusy);
    expect(() => render(<PilotageTab data={data} />)).not.toThrow();
  });

  it('shows the read-only label', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).toMatch(/PILOTAGE · READ-ONLY/);
  });

  it('shows the SIMULATED / NOT A PILOTAGE OPERATING SYSTEM disclosure', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).toMatch(/SIMULATED · NOT A PILOTAGE OPERATING SYSTEM/);
    expect(html).toMatch(/No dispatch authority/);
    expect(html).toMatch(/No fatigue or competency validation/);
  });

  it('renders the table with the expected columns when assignments exist', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).toMatch(/<table[^>]*role="table"/);
    expect(html).toContain('Scheduled (UTC)');
    expect(html).toContain('Vessel');
    expect(html).toContain('Direction');
    expect(html).toContain('Pilot ID');
    expect(html).toContain('Boarding station');
    expect(html).toContain('Status');
  });

  it('shows total / inbound / outbound counts', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).toContain('Total');
    expect(html).toContain('Inbound');
    expect(html).toContain('Outbound');
  });

  it('does NOT contain operator-action affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
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

  it('does NOT render dispatch / assign / reassign / cancel UI', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).not.toMatch(/\bDispatch\b/);
    expect(html).not.toMatch(/\bAssign\b/);
    expect(html).not.toMatch(/\bReassign\b/);
    expect(html).not.toMatch(/\bCancel\b/);
  });

  it('does NOT render fatigue / competency / certification / Kyber fields', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    // The disclosure copy legitimately includes the words "fatigue" and
    // "competency" in negative form ("No fatigue or competency validation").
    // The test pins the absence of actual UI fields / columns / pills,
    // not the absence of the word in disclosure copy.
    expect(html).not.toMatch(/<th[^>]*>\s*Fatigue/i);
    expect(html).not.toMatch(/<th[^>]*>\s*Competenc/i);
    expect(html).not.toMatch(/<th[^>]*>\s*Certification/i);
    expect(html).not.toMatch(/<th[^>]*>\s*Roster/i);
    expect(html).not.toMatch(/Fatigue score/i);
    expect(html).not.toMatch(/Competency score/i);
    expect(html).not.toMatch(/Roster compliance/i);
    // Kyber must not appear anywhere — there is no legitimate copy
    // mention in M2 since the Kyber boundary is in force.
    expect(html).not.toMatch(/Kyber/i);
  });

  it('does NOT render export / download affordances', () => {
    const data = adaptSummary(brisbaneBusy);
    const html = render(<PilotageTab data={data} />);
    expect(html).not.toMatch(/\bExport\b/);
    expect(html).not.toMatch(/\bDownload\b/);
    expect(html).not.toMatch(/<a[^>]*download/);
  });

  it('renders the empty state when there are no assignments', () => {
    const data = adaptSummary({});
    const html = render(<PilotageTab data={data} />);
    expect(html).toMatch(/NO ASSIGNMENTS/);
    expect(html).toMatch(/No pilotage assignments/);
  });

  it('renders without throwing on quiet fixture', () => {
    const data = adaptSummary(brisbaneQuiet);
    expect(() => render(<PilotageTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on Melbourne fixture', () => {
    const data = adaptSummary(melbourneSim);
    expect(() => render(<PilotageTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on null-fields fixture', () => {
    const data = adaptSummary(nullFields);
    expect(() => render(<PilotageTab data={data} />)).not.toThrow();
  });

  it('renders without throwing on malformed fixture', () => {
    const data = adaptSummary(malformed);
    expect(() => render(<PilotageTab data={data} />)).not.toThrow();
  });
});

describe('PilotageAssignmentRow — single assignment row (M2 §8.4)', () => {
  const baseAssignment = {
    id: 'PIL-V001-IN',
    vesselId: 'V001',
    vesselName: 'ALPHA',
    pilotId: 'PILOT_BNE_PSP_03',
    scheduledTimeIso: '2026-05-20T11:34:22Z',
    boardingStation: 'Brisbane Bar Pilot Station',
    direction: 'inbound',
    status: 'confirmed',
  };

  it('renders the time, vessel, direction, pilot id, boarding station, and status', () => {
    const html = render(
      <table><tbody><PilotageAssignmentRow assignment={baseAssignment} /></tbody></table>
    );
    expect(html).toMatch(/UTC/);
    expect(html).toContain('ALPHA');
    expect(html).toContain('Inbound');
    expect(html).toContain('PILOT_BNE_PSP_03');
    expect(html).toContain('Brisbane Bar Pilot Station');
    expect(html).toContain('Confirmed');
  });

  it('exposes the pilot identifier inside <code> (resource-safe code presentation)', () => {
    const html = render(
      <table><tbody><PilotageAssignmentRow assignment={baseAssignment} /></tbody></table>
    );
    expect(html).toMatch(/<code[^>]*>PILOT_BNE_PSP_03<\/code>/);
  });

  it('falls back to "—" for missing fields without throwing', () => {
    const html = render(
      <table><tbody>
        <PilotageAssignmentRow
          assignment={{
            id: 'PIL-X',
            vesselId: 'X',
            vesselName: null,
            pilotId: null,
            scheduledTimeIso: '2026-05-20T11:34:22Z',
            boardingStation: null,
            direction: null,
            status: null,
          }}
        />
      </tbody></table>
    );
    expect(html).toContain('—');
  });

  it('does NOT render any interactive elements per row', () => {
    const html = render(
      <table><tbody><PilotageAssignmentRow assignment={baseAssignment} /></tbody></table>
    );
    expect(html).not.toMatch(/<button\b/);
    expect(html).not.toMatch(/<input\b/);
    expect(html).not.toMatch(/onclick=/i);
    expect(html).not.toMatch(/<a\b/);
  });
});
