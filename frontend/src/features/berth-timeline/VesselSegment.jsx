import React from 'react';

// VesselSegment — a single vessel occupancy block on a berth lane.
// Positioned absolutely within the lane container by left% / width%
// derived from the row's time window. Read-only. No drag, no resize,
// no click handler beyond visual focus-on-hover via CSS. M2
// Implementation Plan §8.1, Canon §10 anti-pattern boundary
// (no timeline editing).

function pctRange(startMs, endMs, segStartMs, segEndMs) {
  const span = endMs - startMs;
  if (span <= 0) return { leftPct: 0, widthPct: 0 };
  const leftPct = Math.max(0, ((segStartMs - startMs) / span) * 100);
  const rightPct = Math.min(100, ((segEndMs - startMs) / span) * 100);
  return { leftPct, widthPct: Math.max(0, rightPct - leftPct) };
}

function segmentClass(segment) {
  const classes = ['hz-bt-segment'];
  if (segment.actualised)     classes.push('hz-bt-segment-actualised');
  else if (segment.scheduled) classes.push('hz-bt-segment-scheduled');
  else                        classes.push('hz-bt-segment-active');
  if (segment.hasConflict)    classes.push('hz-bt-segment-conflict');
  if (segment.vesselStatus === 'at_risk') classes.push('hz-bt-segment-at-risk');
  return classes.join(' ');
}

export function VesselSegment({ segment, windowStartIso, windowEndIso }) {
  if (!segment || !windowStartIso || !windowEndIso) return null;
  const windowStartMs = Date.parse(windowStartIso);
  const windowEndMs   = Date.parse(windowEndIso);
  const segStartMs    = Date.parse(segment.startIso);
  const segEndMs      = Date.parse(segment.endIso);
  if (!Number.isFinite(windowStartMs) || !Number.isFinite(windowEndMs)
   || !Number.isFinite(segStartMs)    || !Number.isFinite(segEndMs)) {
    return null;
  }

  const { leftPct, widthPct } = pctRange(windowStartMs, windowEndMs, segStartMs, segEndMs);
  if (widthPct <= 0) return null;

  const title = [
    segment.vesselName || segment.vesselId,
    segment.vesselStatus ? `(${segment.vesselStatus})` : null,
    `${segment.startIso} → ${segment.endIso}`,
    segment.hasConflict ? `conflicts: ${segment.conflictIds.join(', ')}` : null,
  ].filter(Boolean).join(' · ');

  return (
    <div
      className={segmentClass(segment)}
      style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
      title={title}
      aria-label={title}
    >
      <span className="hz-bt-segment-label">
        {segment.vesselName || segment.vesselId}
      </span>
      {segment.hasConflict && (
        <span className="hz-bt-segment-conflict-dot" aria-label="has conflict">!</span>
      )}
    </div>
  );
}
