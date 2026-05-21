import React from 'react';

// TimeAxis — read-only hour-by-hour axis spanning the timeline's time window.
// Emits absolute-positioned tick labels along a CSS-grid track; the parent
// row provides the track. No interaction, no zoom, no scroll. M2
// Implementation Plan §8.1.

const HOUR_MS = 60 * 60 * 1000;

function pad2(n) {
  return n < 10 ? '0' + n : '' + n;
}

function fmtTick(date) {
  // UTC formatting, no port-local time. Local-time formatting is a future
  // concern (timezone-aware UI is part of M3+ when the role-mode system
  // arrives). For M2 the axis is UTC and labelled as such in the tab copy.
  const h = pad2(date.getUTCHours());
  if (date.getUTCHours() === 0) {
    const day = pad2(date.getUTCDate());
    return `${day} 00:00`;
  }
  return `${h}:00`;
}

function chooseStepMs(durationMs) {
  // Aim for ~12 tick labels in the visible window. Snap to common
  // intervals for readability.
  const targetCount = 12;
  const rawStep = durationMs / targetCount;
  const candidates = [
    HOUR_MS,
    2 * HOUR_MS,
    3 * HOUR_MS,
    4 * HOUR_MS,
    6 * HOUR_MS,
    8 * HOUR_MS,
    12 * HOUR_MS,
    24 * HOUR_MS,
  ];
  for (const c of candidates) {
    if (c >= rawStep) return c;
  }
  return 24 * HOUR_MS;
}

export function TimeAxis({ timeWindow }) {
  if (!timeWindow || !timeWindow.startIso || !timeWindow.endIso) {
    return null;
  }
  const startMs = Date.parse(timeWindow.startIso);
  const endMs   = Date.parse(timeWindow.endIso);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return null;
  }
  const durationMs = endMs - startMs;
  const stepMs = chooseStepMs(durationMs);

  // First tick at the next ceil-of-stepMs after startMs.
  const firstTickMs = Math.ceil(startMs / stepMs) * stepMs;
  const ticks = [];
  for (let t = firstTickMs; t <= endMs; t += stepMs) {
    const pct = ((t - startMs) / durationMs) * 100;
    ticks.push({ ms: t, pct });
  }

  return (
    <div className="hz-bt-axis" aria-label="Timeline hours (UTC)">
      {ticks.map(({ ms, pct }) => (
        <div
          key={ms}
          className="hz-bt-axis-tick"
          style={{ left: `${pct}%` }}
        >
          <span className="hz-bt-axis-tick-label">{fmtTick(new Date(ms))}</span>
        </div>
      ))}
    </div>
  );
}
