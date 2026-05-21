import React from 'react';
import { Pill } from '../../components/Pill.jsx';

// ShiftLogRow — single derived event row. Read-only. No actions.
// M2 Implementation Plan §8.2.

const TYPE_LABEL = {
  arrival:           'Arrival',
  departure:         'Departure',
  conflict_detected: 'Conflict detected',
};

const TYPE_TONE = {
  arrival:           'success',
  departure:         'info',
  conflict_detected: 'warning',
};

const SEVERITY_TONE = {
  CRITICAL: 'critical',
  HIGH:     'warning',
  MEDIUM:   'info',
  LOW:      'success',
  INFO:     'muted',
};

function pad2(n) {
  return n < 10 ? '0' + n : '' + n;
}

function fmtUtcShort(iso) {
  if (typeof iso !== 'string' || !iso) return '—';
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return iso;
  const day = pad2(d.getUTCDate());
  const mon = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getUTCMonth()];
  const hh  = pad2(d.getUTCHours());
  const mm  = pad2(d.getUTCMinutes());
  return `${day} ${mon} ${hh}:${mm} UTC`;
}

export function ShiftLogRow({ event }) {
  if (!event) return null;
  return (
    <tr>
      <td className="hz-sl-cell-time">{fmtUtcShort(event.timestampIso)}</td>
      <td>
        <Pill tone={TYPE_TONE[event.type] || 'info'} variant="outline">
          {TYPE_LABEL[event.type] || event.type}
        </Pill>
      </td>
      <td>
        {event.severity && event.severity !== 'INFO' ? (
          <Pill tone={SEVERITY_TONE[event.severity] || 'info'}>
            {event.severity}
          </Pill>
        ) : (
          <span className="hz-sl-cell-muted">—</span>
        )}
      </td>
      <td className="hz-sl-cell-vessel">
        {event.vesselName || event.vesselId || '—'}
        {event.berth ? <span className="hz-sl-cell-muted"> · {event.berth}</span> : null}
      </td>
      <td className="hz-sl-cell-desc">{event.description || '—'}</td>
    </tr>
  );
}
