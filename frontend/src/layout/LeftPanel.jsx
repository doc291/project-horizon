import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';

// LeftPanel — Canon §1.1.2 + §4.4.
// M1: DesignVerificationSwatch removed (per M1 Scope Proposal §5 item 10).
// Alert list + Active Decision card remain M2+ scope; placeholder
// stays in M1 to preserve the canonical shell layout.

export function LeftPanel() {
  return (
    <aside className="hz-panel hz-panel-left">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="info" variant="outline">M2+ PLACEHOLDER</Pill>
          <h3>Operator alerts &amp; decision card</h3>
          <p>
            Arrive in later milestones. Real alert list and the
            Active Decision card are deferred to M2+ per
            <code>HORIZON_V1_EXECUTION_PLAN_v0.1.md</code> §11.
          </p>
          <p style={{ marginTop: 'var(--s-3)' }}>
            M1 exercises the adapter against captured fixtures
            (Brisbane / Melbourne snapshots). The conflict /
            guidance data flows through the adapter and is
            available in <code>summary.conflicts</code> and
            <code>summary.guidance</code> for downstream surfaces
            that arrive in M2+.
          </p>
        </div>
      </Card>
    </aside>
  );
}
