import React from 'react';
import { Pill } from '../../components/Pill.jsx';
import { VesselSegment } from './VesselSegment.jsx';

// BerthRow — a single berth lane with a label cell and an occupancy lane.
// The lane positions vessel segments by percentage of the timeline's
// time window. Read-only. M2 Implementation Plan §8.1.

const STATUS_TONE = {
  available:   'success',
  occupied:    'info',
  reserved:    'advisory',
  maintenance: 'muted',
};

function statusLabel(s) {
  if (typeof s !== 'string') return 'Unknown';
  return s.charAt(0).toUpperCase() + s.slice(1);
}

export function BerthRow({ row, windowStartIso, windowEndIso }) {
  if (!row) return null;
  const segments = Array.isArray(row.segments) ? row.segments : [];

  return (
    <div className="hz-bt-row" role="row">
      <div className="hz-bt-row-label" role="rowheader">
        <div className="hz-bt-row-label-id">{row.berthId}</div>
        <div className="hz-bt-row-label-name">{row.name || row.berthId}</div>
        <div className="hz-bt-row-label-status">
          <Pill tone={STATUS_TONE[row.status] || 'info'} variant="outline">
            {statusLabel(row.status)}
          </Pill>
        </div>
      </div>
      <div className="hz-bt-row-lane" role="cell">
        {segments.length === 0 && (
          <div className="hz-bt-row-empty">
            <span>No scheduled occupancy in this window.</span>
          </div>
        )}
        {segments.map((s) => (
          <VesselSegment
            key={`${s.vesselId}-${s.startIso}`}
            segment={s}
            windowStartIso={windowStartIso}
            windowEndIso={windowEndIso}
          />
        ))}
      </div>
    </div>
  );
}
