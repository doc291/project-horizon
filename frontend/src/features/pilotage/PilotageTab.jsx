import React, { useMemo } from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';
import { adaptPilotage } from '../../api/adapters/pilotageAdapter.js';
import { PilotageAssignmentRow } from './PilotageAssignmentRow.jsx';

// PilotageTab — M2 read-only Pilotage centre-spine tab.
// (M2 Implementation Plan §7.4 + §8.4.)
//
// Renders a chronological list of pilotage assignments from the
// captured /api/summary fixtures. The V1 pilotage data is SIMULATED
// or DERIVED — not authoritative operational pilotage data — and
// the tab makes this explicit in copy and disclosure. This is a
// read-only coordination view, NOT a pilotage operating system.
//
// Explicitly NOT implemented (per the user's authorisation):
//   - Pilot dispatch
//   - Assignment editing or reassignment
//   - Fatigue scoring
//   - Competency / certification validation
//   - Roster compliance
//   - Kyber integration
//   - Any write path
//
// Honours M2 Alignment Review (PR #60) guidance:
//   §3.1 No dashboard-first creep — neutral table styling
//   §3.2 No demo-style interaction — no modals, no animations
//   §3.3 No frontend-heavy logic — derivation in adapter; the
//        adapter contains zero business rules
//   §3.4 Display vs operational state separation — no display state
//        in adapter
//   §3.5 Role-agnostic components — no role naming baked in
//   §3.6 Function over polish — minimal CSS
//   §3.7 No Beta 10 patterns as precedent — V1 React/Vite stack only
//   §3.8 No fixture-coupling — defensive across all 4 fixtures
//
// Kyber boundary: HONOURED — no Kyber-related fields, no operational
// command capability, no authoritative-pilotage-system semantics.

export function PilotageTab({ data }) {
  const pl = useMemo(() => adaptPilotage(data), [data]);
  const assignments = Array.isArray(pl.assignments) ? pl.assignments : [];
  const isMissing = (pl.missingDomains || []).includes('pilotage');

  return (
    <div className="hz-pl-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">Pilotage</h2>
          <p className="hz-dashboard-sub">
            Read-only coordination view · simulated / derived · M2 · times in UTC
          </p>
        </div>
        <Pill tone="info" variant="outline">PILOTAGE · READ-ONLY</Pill>
      </div>

      <Card>
        <h3 className="hz-section-title">Assignments</h3>
        <p className="hz-section-sub">
          Coordination view of pilotage assignments derived from the
          captured summary. Pilot identifiers shown are resource-safe
          codes from the port profile. Not an authoritative pilotage
          system. No dispatch, no assignment editing, no fatigue or
          competency validation in M2.
        </p>
        {isMissing || assignments.length === 0 ? (
          <div className="hz-pl-empty">
            <Pill tone="info" variant="outline">NO ASSIGNMENTS</Pill>
            <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
              No pilotage assignments for the current view.
            </span>
          </div>
        ) : (
          <>
            <div className="hz-pl-counts">
              <span className="hz-pl-counts-item">
                <span className="hz-pl-counts-label">Total</span>
                <span className="hz-pl-counts-value">{pl.counts.total}</span>
              </span>
              <span className="hz-pl-counts-item">
                <span className="hz-pl-counts-label">Inbound</span>
                <span className="hz-pl-counts-value">{pl.counts.inbound}</span>
              </span>
              <span className="hz-pl-counts-item">
                <span className="hz-pl-counts-label">Outbound</span>
                <span className="hz-pl-counts-value">{pl.counts.outbound}</span>
              </span>
            </div>
            <table className="hz-pl-table" role="table" aria-label="Pilotage assignments">
              <thead>
                <tr>
                  <th scope="col">Scheduled (UTC)</th>
                  <th scope="col">Vessel</th>
                  <th scope="col">Direction</th>
                  <th scope="col">Pilot ID</th>
                  <th scope="col">Boarding station</th>
                  <th scope="col">Status</th>
                </tr>
              </thead>
              <tbody>
                {assignments.map((a) => (
                  <PilotageAssignmentRow key={a.id} assignment={a} />
                ))}
              </tbody>
            </table>
          </>
        )}
      </Card>

      <div className="hz-dashboard-footer">
        <Pill tone="warning" variant="outline">SIMULATED · NOT A PILOTAGE OPERATING SYSTEM</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          Source: derived from captured <code>/api/summary</code> snapshot.
          Not live pilotage roster. No fatigue or competency validation.
          No dispatch authority.
        </span>
      </div>
    </div>
  );
}
