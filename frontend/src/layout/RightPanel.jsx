import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';

// RightPanel — Canon §1.1.4 + §4.6. The active-focus rail.
// In M0: a single placeholder card. No vessel roster, no Audit Log tab,
// no decision card. The Audit Log surface depends on a future
// GET /api/audit endpoint that does not exist today
// (HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md §10).

export function RightPanel() {
  return (
    <aside className="hz-panel hz-panel-right">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="info" variant="outline">M0 PLACEHOLDER</Pill>
          <h3>Vessel roster &amp; audit log</h3>
          <p>
            Arrive in later milestones. Vessel roster (read-only) is
            scoped to M1+; the right-rail Audit Log tab depends on a
            future <code>GET /api/audit</code> endpoint that does not
            exist today, per
            <code>HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md</code>
            §10.
          </p>
        </div>
      </Card>
    </aside>
  );
}
