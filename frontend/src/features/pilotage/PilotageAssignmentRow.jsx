import React from 'react';
import { Pill } from '../../components/Pill.jsx';

// PilotageAssignmentRow — single pilotage assignment row. Read-only.
// No buttons, no links, no operator-action affordances. M2
// Implementation Plan §8.4.

const DIRECTION_TONE = {
  inbound:  'info',
  outbound: 'advisory',
};

const DIRECTION_LABEL = {
  inbound:  'Inbound',
  outbound: 'Outbound',
};

const STATUS_TONE = {
  confirmed:   'success',
  scheduled:   'info',
  in_progress: 'warning',
  completed:   'muted',
  cancelled:   'muted',
};

function statusLabel(s) {
  if (typeof s !== 'string') return '—';
  return s.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

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

export function PilotageAssignmentRow({ assignment }) {
  if (!assignment) return null;
  return (
    <tr>
      <td className="hz-pl-cell-time">{fmtUtcShort(assignment.scheduledTimeIso)}</td>
      <td className="hz-pl-cell-vessel">{assignment.vesselName || assignment.vesselId || '—'}</td>
      <td>
        {assignment.direction ? (
          <Pill tone={DIRECTION_TONE[assignment.direction] || 'info'} variant="outline">
            {DIRECTION_LABEL[assignment.direction]}
          </Pill>
        ) : (
          <span className="hz-pl-cell-muted">—</span>
        )}
      </td>
      <td>
        <code className="hz-pl-cell-pilot">{assignment.pilotId || '—'}</code>
      </td>
      <td className="hz-pl-cell-station">{assignment.boardingStation || '—'}</td>
      <td>
        {assignment.status ? (
          <Pill tone={STATUS_TONE[assignment.status] || 'info'}>
            {statusLabel(assignment.status)}
          </Pill>
        ) : (
          <span className="hz-pl-cell-muted">—</span>
        )}
      </td>
    </tr>
  );
}
