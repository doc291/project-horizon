import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';

// LeftPanel — Canon §1.1.2 + §4.4.
// M2 visual remediation: the operator alerts / decision card
// placeholder MOVED to the right rail (per Operational UX Direction
// PR #58 §3.1 — right rail is the long-term coordination/action
// surface). The left rail now carries supporting context only —
// lower-priority, non-action content.
//
// No write actions. No operator-action affordances. No port
// selector. M3+ will populate this rail with role-specific
// supporting context per UX Direction §3.4 and Platform Workflows
// §3 (e.g. shift handover briefing, port-context summary, watch
// log). For M2 the rail carries a calm placeholder.

export function LeftPanel() {
  return (
    <aside className="hz-panel hz-panel-left">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="muted" variant="outline">SUPPORTING CONTEXT</Pill>
          <h3>Supporting context</h3>
          <p>
            The left rail will carry lower-priority operational
            context in later milestones — e.g. shift briefing,
            port-context summary, watch log.
          </p>
          <p style={{ marginTop: 'var(--s-3)' }}>
            Action and coordination items live on the right rail
            (per Operational UX Direction §3.1). The centre spine
            is the operator's primary working surface.
          </p>
        </div>
      </Card>
    </aside>
  );
}
