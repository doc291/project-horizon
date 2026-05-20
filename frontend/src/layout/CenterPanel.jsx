import React from 'react';
import { Card } from '../components/Card.jsx';
import { Pill } from '../components/Pill.jsx';

// CenterPanel — Canon §1.1.3 + §4.5. The centre operational spine.
// In M0: a single placeholder card. No tabs, no Dashboard, no Berth
// Timeline, no Shift Log, no VTS, no Pilotage, no Performance.
// All six tab surfaces are deferred to M2+ per HORIZON_V1_EXECUTION_PLAN
// _v0.1.md §11.

export function CenterPanel() {
  return (
    <main className="hz-panel hz-panel-center">
      <Card>
        <div className="hz-placeholder">
          <Pill tone="info" variant="outline">M0 PLACEHOLDER</Pill>
          <h3>Centre operational spine — tabs</h3>
          <p>
            Arrive in later milestones. The six centre-tab surfaces
            (Dashboard, Berth Timeline, Shift Log, VTS Map, Pilotage,
            Performance) are deferred to M2+ per
            <code>HORIZON_V1_EXECUTION_PLAN_v0.1.md</code> §11.
          </p>
          <p style={{ marginTop: 'var(--s-3)' }}>
            M0 proves only the shell and design tokens; no operational
            content is implemented. The shell layout, conditions ribbon,
            and Horizon Dark visual identity are the M0 acceptance
            surface.
          </p>
        </div>
      </Card>
    </main>
  );
}
