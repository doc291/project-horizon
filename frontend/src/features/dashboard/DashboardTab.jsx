import React from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';
import { KpiTileGroup } from './KpiTileGroup.jsx';
import { EtdRiskTable } from './EtdRiskTable.jsx';
import { UtilisationSummary } from './UtilisationSummary.jsx';

// DashboardTab — partial Dashboard for M1 (Canon §4.5 + M1 Plan §10.4).
// Compositions of KPI tiles, ETD risk table, and utilisation summary.
// Full berth heatmap + other 5 tabs deferred to M2+.

export function DashboardTab({ data }) {
  return (
    <div className="hz-dashboard-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">Port overview</h2>
          <p className="hz-dashboard-sub">
            Read-only fixture-backed view · adapter-fed · M1
          </p>
        </div>
        <Pill tone="info" variant="outline">DASHBOARD · READ-ONLY</Pill>
      </div>

      <Card>
        <KpiTileGroup data={data} />
      </Card>

      <Card>
        <h3 className="hz-section-title">ETD risk</h3>
        <p className="hz-section-sub">
          Vessels at risk of late departure (top 5 by risk score).
        </p>
        <EtdRiskTable etdRisk={data.etdRisk} />
      </Card>

      <Card>
        <UtilisationSummary data={data} />
      </Card>

      <div className="hz-dashboard-footer">
        <Pill tone="warning" variant="outline">FIXTURE DATA</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          Source: captured /api/summary snapshot · not live backend ·
          last update from <code>summary.generatedAtIso</code>
        </span>
      </div>
    </div>
  );
}
