import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';

// RightPanel — Canon §1.1.4 + §4.6.
// M2 visual remediation: this is the ACTION AND COORDINATION rail
// per Operational UX Direction PR #58 §3.1 — long-term home for
// active conflicts, recommendations, ETD risks, acknowledgements,
// sequencing issues, coordination tasks, and timeline-compression
// warnings.
//
// M2 ships the placeholder only. The real decision-card list and
// the operator-action lifecycle (ACK / DEFER / APPLY / OVERRIDE /
// ESCALATE) are M3+ work under separate scope — they require
// server-authoritative auth, RBAC and immutable audit
// emission, none of which exist in M2 (per Operational Platform
// Workflows §5.6, §5.7, §6.1 and §6.2).
//
// Read-only placeholder only. No write actions. No buttons. No
// port selector.

export function RightPanel() {
  return (
    <aside className="hz-panel hz-panel-right">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="warning" variant="outline">ACTION &amp; COORDINATION</Pill>
          <h3>Operator alerts &amp; decision card</h3>
          <p>
            The right rail is the long-term action and coordination
            surface (per Operational UX Direction §3.1). Active
            conflicts, recommendations, ETD risks, acknowledgements,
            sequencing decisions and coordination tasks will land
            here in later milestones.
          </p>
          <p style={{ marginTop: 'var(--s-3)' }}>
            M2 exercises the adapter against captured fixtures
            (Brisbane / Melbourne / Geelong / Darwin snapshots).
            Conflict and guidance data already flows through the
            adapter and is available in <code>summary.conflicts</code>
            and <code>summary.guidance</code> for the decision-card
            list that arrives in M3+.
          </p>
          <p style={{ marginTop: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
            <strong>Not yet implemented:</strong> ACK / DEFER /
            APPLY / OVERRIDE / ESCALATE operator actions. These
            require server-authoritative auth, RBAC and audit
            emission — all M3+ scope.
          </p>
        </div>
      </Card>
    </aside>
  );
}
