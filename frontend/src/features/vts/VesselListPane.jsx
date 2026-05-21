import React from 'react';
import { Pill } from '../../components/Pill.jsx';

// VesselListPane — VTS read-only vessel list.
// Renders the adapted VTS vessels list as a flat read-only table:
// name, type, status, berth (if any), LOA, hasConflict flag.
// No map. No editing. No commanding. Per M2 Implementation Plan §8.3
// and the Canon §10 anti-pattern boundary (read-only VTS only).

const RISK_TONE = {
  critical: 'critical',
  high: 'warning',
  medium: 'info',
  low: 'success',
};

const STATUS_LABEL = {
  berthed: 'Berthed',
  arrived: 'Arrived',
  confirmed: 'Confirmed',
  scheduled: 'Scheduled',
  at_risk: 'At risk',
  departed: 'Departed',
};

function fmtLoa(m) {
  if (typeof m !== 'number' || !isFinite(m)) return '—';
  return `${m.toFixed(0)} m`;
}

function statusLabel(s) {
  if (typeof s !== 'string') return '—';
  return STATUS_LABEL[s] || s.charAt(0).toUpperCase() + s.slice(1);
}

export function VesselListPane({ vessels }) {
  const rows = Array.isArray(vessels) ? vessels : [];

  if (rows.length === 0) {
    return (
      <div className="hz-vts-empty">
        <Pill tone="info" variant="outline">NO VESSELS IN VIEW</Pill>
        <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
          No vessels currently in scope for this port.
        </span>
      </div>
    );
  }

  return (
    <table className="hz-vts-table" role="table" aria-label="Vessels in view">
      <thead>
        <tr>
          <th scope="col">Vessel</th>
          <th scope="col">Type</th>
          <th scope="col">Status</th>
          <th scope="col">Berth</th>
          <th scope="col">LOA</th>
          <th scope="col">Risk</th>
          <th scope="col">Conflict</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((v) => (
          <tr key={v.vesselId || `${v.name}-${v.berth}`}>
            <td>{v.name || v.vesselId || '—'}</td>
            <td className="hz-vts-cell-muted">{v.type || '—'}</td>
            <td>{statusLabel(v.status)}</td>
            <td className="hz-vts-cell-muted">{v.berth || '—'}</td>
            <td className="hz-vts-cell-num">{fmtLoa(v.loaM)}</td>
            <td>
              <Pill tone={RISK_TONE[v.riskLevel] || 'info'}>
                {String(v.riskLevel || 'low').toUpperCase()}
              </Pill>
            </td>
            <td>
              {v.hasConflict ? (
                <Pill tone="warning">YES</Pill>
              ) : (
                <span className="hz-vts-cell-muted">—</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
