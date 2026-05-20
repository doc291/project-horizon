import React from 'react';

// UtilisationSummary — single-line berth utilisation summary.
// Full heatmap deferred to M2 per M1 Scope Proposal §19.6 + resolved decision #6.

export function UtilisationSummary({ data }) {
  const d = data.dashboardMetrics;
  const ps = data.portStatus;

  const currentPct = d.berthUtilisationPct;
  const forecastPct = d.forecastUtilisation48h;
  const occupied = ps.berthsOccupied;
  const total = ps.berthsTotal;

  return (
    <div className="hz-utilisation-summary">
      <span className="hz-utilisation-label">Berth utilisation</span>
      <span className="hz-utilisation-value">
        {currentPct != null ? `${currentPct}%` : '—'}
        {occupied != null && total != null ? ` (${occupied}/${total} berths)` : ''}
      </span>
      {forecastPct != null && (
        <span className="hz-utilisation-forecast">
          · next 48h forecast {forecastPct}%
        </span>
      )}
      <span className="hz-utilisation-note"> · full heatmap arrives in M2</span>
    </div>
  );
}
