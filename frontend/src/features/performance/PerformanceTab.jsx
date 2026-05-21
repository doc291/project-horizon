import React from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';

// PerformanceTab — M2 Performance placeholder (M2 Implementation Plan
// §8.5 — placeholder only).
//
// This is a parked surface. Performance analytics are deferred to a
// later milestone. No adapter, no data binding, no calculations, no
// charts, no export. The tab exists in the codebase so the centre-
// spine tab set is structurally complete (Canon §1.1.3 lists six
// tabs: Dashboard, Berth Timeline, Shift Log, VTS, Pilotage,
// Performance), but it carries no analytics in M2.
//
// Honours M2 Alignment Review (PR #60) guidance:
//   §3.1 No dashboard-first creep — minimal placeholder
//   §3.2 No demo-style interaction — static text only
//   §3.3 No frontend-heavy logic — none
//   §3.4 Display vs operational state separation — no operational
//        state involved (placeholder)
//   §3.5 Role-agnostic — no role naming
//   §3.6 Function over polish — minimal CSS
//   §3.7 No Beta 10 patterns as precedent — V1 React/Vite stack only
//   §3.8 No fixture-coupling — does not consume `data` at all

// Future-scope items — high-level only, not commitments. These
// describe the kind of work Performance might eventually carry; they
// are not authorised M2/M3 scope and are not promises.
const FUTURE_SCOPE_ITEMS = [
  'Throughput (vessels, movements, berth-occupancy hours)',
  'Berth utilisation trends',
  'Decision outcomes (acknowledgements, deferrals, applied alternatives)',
  'Delay trends and root-cause classification',
  'Resource utilisation (pilots, tugs, mooring gangs, terminals)',
  'Operational reliability and incident summary',
];

export function PerformanceTab(/* data deliberately unused */) {
  return (
    <div className="hz-pf-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">Performance</h2>
          <p className="hz-dashboard-sub">
            Placeholder · deferred to a later milestone · M2
          </p>
        </div>
        <Pill tone="muted" variant="outline">PERFORMANCE · DEFERRED</Pill>
      </div>

      <Card>
        <h3 className="hz-section-title">This view is intentionally parked for M2</h3>
        <p className="hz-section-sub">
          Performance analytics will be introduced in a later milestone.
          No live performance calculations are active in M2.
        </p>
        <div className="hz-pf-future">
          <div className="hz-pf-future-label">
            Future scope may include
          </div>
          <ul className="hz-pf-future-list" role="list">
            {FUTURE_SCOPE_ITEMS.map((item) => (
              <li key={item} className="hz-pf-future-item">{item}</li>
            ))}
          </ul>
          <p className="hz-pf-future-note">
            High-level intent only. Not authorised scope. Each item
            requires its own scope proposal before any implementation.
          </p>
        </div>
      </Card>

      <div className="hz-dashboard-footer">
        <Pill tone="muted" variant="outline">PLACEHOLDER · NO ANALYTICS</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          No data binding · no charts · no calculations · no export ·
          parked surface for centre-spine tab-set completeness only
        </span>
      </div>
    </div>
  );
}
