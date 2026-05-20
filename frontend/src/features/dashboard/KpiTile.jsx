import React from 'react';

// KpiTile — single KPI cell. M1 read-only; no trend indicators
// (M1 Implementation Plan §10.3 + Adapter Note §10.3).

export function KpiTile({ label, value, sub, tone }) {
  const displayValue = value === null || value === undefined ? '—' : value;
  return (
    <div className={`hz-kpi-tile ${tone ? `hz-kpi-tile-${tone}` : ''}`}>
      <div className="hz-kpi-label">{label}</div>
      <div className="hz-kpi-value">{displayValue}</div>
      {sub ? <div className="hz-kpi-sub">{sub}</div> : null}
    </div>
  );
}
