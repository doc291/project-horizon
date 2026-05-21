import React from 'react';
import { Pill } from '../../components/Pill.jsx';

// ConflictsList — VTS read-only conflicts pane.
// Renders the filtered conflicts list (CONFLICT and WARNING signal types
// only — ADVISORY / WEATHER cards remain on the Dashboard right rail).
// No interaction. No acknowledge. No defer. No commit. Per M2
// Implementation Plan §8.3 and §10.1 read-only labelling.

const SEVERITY_TONE = {
  CRITICAL: 'critical',
  HIGH: 'warning',
  MEDIUM: 'info',
  LOW: 'success',
  INFO: 'muted',
};

const SIGNAL_TONE = {
  CONFLICT: 'critical',
  WARNING: 'warning',
};

export function ConflictsList({ conflicts }) {
  const rows = Array.isArray(conflicts) ? conflicts : [];

  if (rows.length === 0) {
    return (
      <div className="hz-vts-empty">
        <Pill tone="success" variant="outline">NO ACTIVE CONFLICTS</Pill>
        <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
          No CONFLICT or WARNING items in the current port view.
        </span>
      </div>
    );
  }

  return (
    <ul className="hz-vts-conflicts" role="list">
      {rows.map((c) => (
        <li key={c.conflictId} className="hz-vts-conflicts-row">
          <div className="hz-vts-conflicts-row-head">
            <Pill tone={SIGNAL_TONE[c.signalType] || 'info'}>{c.signalType || 'INFO'}</Pill>
            <Pill tone={SEVERITY_TONE[c.severity] || 'info'} variant="outline">
              {c.severity || 'INFO'}
            </Pill>
            <span className="hz-vts-conflicts-row-title">
              {c.title || c.type || 'Conflict'}
            </span>
          </div>
          {Array.isArray(c.vesselNames) && c.vesselNames.length > 0 && (
            <div className="hz-vts-conflicts-row-meta">
              <span className="hz-vts-cell-muted">Vessels:</span>{' '}
              {c.vesselNames.join(' · ')}
            </div>
          )}
          {c.berth && (
            <div className="hz-vts-conflicts-row-meta">
              <span className="hz-vts-cell-muted">Berth:</span> {c.berth}
            </div>
          )}
        </li>
      ))}
    </ul>
  );
}
