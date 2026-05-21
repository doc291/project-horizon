import React, { useMemo } from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';
import { adaptVts } from '../../api/adapters/vtsAdapter.js';
import { VesselListPane } from './VesselListPane.jsx';
import { ConflictsList } from './ConflictsList.jsx';

// VtsTab — M2 read-only VTS centre-spine tab (M2 Implementation Plan §8.3).
//
// Renders a read-only vessel list joined with conflict membership, alongside
// a filtered conflicts pane (CONFLICT and WARNING signal types only). The
// tab consumes the same adapted ViewSummary that the Dashboard tab uses;
// the vtsAdapter is a pure presentation join on top.
//
// Honours the M2 Alignment Review (PR #60) guidance:
//   - No map. No editing. No vessel commanding. (§3.2 demo-style
//     patterns; Canon §10 anti-patterns.)
//   - No business logic in the component — derivation is in vtsAdapter.
//     (§3.3 frontend-heavy logic.)
//   - No role-coupling baked into the component. (§3.5 insufficient
//     role separation.)
//   - heading / speed / course intentionally not shown — deferred to a
//     future fixture refresh. (§3.8 fixture-coupling avoidance.)
//   - Read-only labelling visible. (M2 Implementation Plan §10.1.)

export function VtsTab({ data }) {
  // adaptVts is a pure function; memo to avoid recomputing on every render
  // even though current cost is small.
  const vts = useMemo(() => adaptVts(data), [data]);

  return (
    <div className="hz-vts-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">VTS — vessels in view</h2>
          <p className="hz-dashboard-sub">
            Read-only vessel view · adapter-fed · M2
          </p>
        </div>
        <Pill tone="info" variant="outline">VTS · READ-ONLY</Pill>
      </div>

      <Card>
        <h3 className="hz-section-title">Vessels</h3>
        <p className="hz-section-sub">
          Vessels currently in scope for this port. Heading and speed are
          deferred to a future fixture refresh.
        </p>
        <VesselListPane vessels={vts.vessels} />
      </Card>

      <Card>
        <h3 className="hz-section-title">Conflicts and warnings</h3>
        <p className="hz-section-sub">
          Filtered to <code>CONFLICT</code> and <code>WARNING</code> signal
          types. Advisory and weather items remain on the Dashboard right
          rail.
        </p>
        <ConflictsList conflicts={vts.conflicts} />
      </Card>

      <div className="hz-dashboard-footer">
        <Pill tone="warning" variant="outline">FIXTURE DATA</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          Source: captured <code>/api/summary</code> snapshot · not live VTS ·
          decision support only · not a VTS replacement
        </span>
      </div>
    </div>
  );
}
