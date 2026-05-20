import React from 'react';
import { KpiTile } from './KpiTile.jsx';

// KpiTileGroup — three categorical rows (Operations, Performance, Safety)
// bound to summary.dashboardMetrics + summary.portStatus.
// Per M1 Implementation Plan §10.4.

export function KpiTileGroup({ data }) {
  const d = data.dashboardMetrics;
  const ps = data.portStatus;

  return (
    <div className="hz-kpi-groups">
      <div className="hz-kpi-row">
        <h4 className="hz-kpi-row-label">Operations</h4>
        <div className="hz-kpi-tiles">
          <KpiTile label="Vessels in port" value={fmt(ps.vesselsInPort)} sub={`${fmt(ps.berthsOccupied, '—')}/${fmt(ps.berthsTotal, '—')} berths occupied`} />
          <KpiTile label="Expected 24h" value={fmt(ps.vesselsExpected24h)} sub={`Departing: ${fmt(ps.vesselsDeparting24h)}`} />
          <KpiTile
            label="Active conflicts"
            value={fmt(d.activeConflicts, 0)}
            sub={`Critical: ${fmt(d.criticalConflicts, 0)}`}
            tone={d.criticalConflicts > 0 ? 'critical' : null}
          />
        </div>
      </div>

      <div className="hz-kpi-row">
        <h4 className="hz-kpi-row-label">Performance</h4>
        <div className="hz-kpi-tiles">
          <KpiTile label="Avg dwell" value={d.avgDwellHours != null ? `${d.avgDwellHours.toFixed(1)}h` : '—'} sub="14-day rolling" />
          <KpiTile label="Berth utilisation" value={d.berthUtilisationPct != null ? `${d.berthUtilisationPct}%` : '—'} sub={d.forecastUtilisation48h != null ? `48h forecast ${d.forecastUtilisation48h}%` : null} />
          <KpiTile label="On-time departure" value={d.onTimeDeparturePct != null ? `${d.onTimeDeparturePct}%` : '—'} sub="7-day rolling" />
        </div>
      </div>

      <div className="hz-kpi-row">
        <h4 className="hz-kpi-row-label">Safety</h4>
        <div className="hz-kpi-tiles">
          <KpiTile
            label="Vessels at risk"
            value={fmt(d.vesselsAtRisk, 0)}
            tone={d.vesselsAtRisk > 0 ? 'warning' : null}
          />
          <KpiTile label="Pilot ops 12h" value={fmt(d.pilotOps12h, 0)} />
          <KpiTile label="Tug ops 12h" value={fmt(d.tugOps12h, 0)} />
        </div>
      </div>
    </div>
  );
}

function fmt(value, fallback = '—') {
  return value === null || value === undefined ? fallback : value;
}
