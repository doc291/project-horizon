import React from 'react';
import { Pill } from '../../components/Pill.jsx';
import { RiskBar } from './RiskBar.jsx';

// EtdRiskTable — top-5 ETD risk entries from adapter output.
// Read-only; no interactivity. Per M1 Implementation Plan §10.4.

const LEVEL_TONE = {
  critical: 'critical',
  high: 'warning',
  medium: 'info',
  low: 'success',
};

export function EtdRiskTable({ etdRisk }) {
  const top5 = Array.isArray(etdRisk) ? etdRisk.slice(0, 5) : [];
  if (top5.length === 0) {
    return (
      <div className="hz-etd-empty">
        <Pill tone="success" variant="outline">NO ETD RISK</Pill>
        <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
          No vessels currently flagged at risk.
        </span>
      </div>
    );
  }

  return (
    <table className="hz-etd-table">
      <thead>
        <tr>
          <th>Vessel</th>
          <th>Level</th>
          <th>Primary factor</th>
          <th>Risk</th>
        </tr>
      </thead>
      <tbody>
        {top5.map((r) => (
          <tr key={r.vesselId}>
            <td>{r.vesselName || r.vesselId}</td>
            <td>
              <Pill tone={LEVEL_TONE[r.riskLevel] || 'info'}>
                {String(r.riskLevel || 'low').toUpperCase()}
              </Pill>
            </td>
            <td className="hz-etd-factor">{r.reasonPrimary || '—'}</td>
            <td>
              <RiskBar score={r.riskScore} level={r.riskLevel} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
