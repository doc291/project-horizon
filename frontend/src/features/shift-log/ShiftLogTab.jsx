import React, { useMemo } from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';
import { adaptShiftLog } from '../../api/adapters/shiftLogAdapter.js';
import { ShiftLogRow } from './ShiftLogRow.jsx';

// ShiftLogTab — M2 read-only Shift Log centre-spine tab.
// (M2 Implementation Plan §7.2 + §8.2.)
//
// Renders a chronological list of *derived* operational events:
// arrivals, departures, and detected conflicts. No filters in M2
// (Plan §14.5 default). No interaction beyond visual hover. No
// operator-action affordances. No acknowledgement, no editing, no
// export. The "derived" disclosure is prominent because the V1
// captured /api/summary fixtures do not carry a real audit ledger
// or events stream; the rows below are observations of timestamped
// facts already present in the data.
//
// Honours M2 Alignment Review (PR #60) guidance:
//   §3.1 No dashboard-first creep — neutral table styling
//   §3.2 No demo-style interaction — no modals, no animations
//   §3.3 No frontend-heavy logic — derivation in adapter
//   §3.4 Display vs operational state separation — no display state in adapter
//   §3.5 Role-agnostic components — no role naming
//   §3.6 Function over polish — minimal CSS
//   §3.7 No Beta 10 patterns as precedent — V1 React/Vite stack only
//   §3.8 No fixture-coupling — empty state handled defensively

export function ShiftLogTab({ data }) {
  const sl = useMemo(() => adaptShiftLog(data), [data]);
  const rows = Array.isArray(sl.rows) ? sl.rows : [];

  return (
    <div className="hz-sl-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">Shift log</h2>
          <p className="hz-dashboard-sub">
            Read-only event log · derived from current summary ·
            adapter-fed · M2 · times in UTC
          </p>
        </div>
        <Pill tone="info" variant="outline">SHIFT LOG · READ-ONLY</Pill>
      </div>

      <Card>
        <h3 className="hz-section-title">Recent events</h3>
        <p className="hz-section-sub">
          Derived from the current summary. Captured fixtures do not
          include a real audit ledger; events here are observations of
          timestamped facts already in the data (arrivals, departures,
          detected conflicts). Operator actions are not represented.
          No filters in M2.
        </p>
        {rows.length === 0 ? (
          <div className="hz-sl-empty">
            <Pill tone="info" variant="outline">NO EVENTS</Pill>
            <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
              No events for current shift.
            </span>
          </div>
        ) : (
          <table className="hz-sl-table" role="table" aria-label="Derived shift log events">
            <thead>
              <tr>
                <th scope="col">Time (UTC)</th>
                <th scope="col">Type</th>
                <th scope="col">Severity</th>
                <th scope="col">Vessel / context</th>
                <th scope="col">Description</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((e) => (
                <ShiftLogRow key={e.id} event={e} />
              ))}
            </tbody>
          </table>
        )}
      </Card>

      <div className="hz-dashboard-footer">
        <Pill tone="warning" variant="outline">DERIVED · NOT AUDIT LEDGER</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          Source: derived from captured <code>/api/summary</code> snapshot.
          Not a replay of operator actions. Not a substitute for the
          official audit ledger.
        </span>
      </div>
    </div>
  );
}
