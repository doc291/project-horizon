import React, { useMemo } from 'react';
import { Card } from '../../components/Card.jsx';
import { Pill } from '../../components/Pill.jsx';
import { adaptBerthTimeline } from '../../api/adapters/berthTimelineAdapter.js';
import { TimeAxis } from './TimeAxis.jsx';
import { BerthRow } from './BerthRow.jsx';

// BerthTimelineTab — M2 read-only Berth Timeline tab.
//
// Renders one row per berth with vessel occupancy as time-positioned
// segments. No interaction beyond visual hover (tooltip via the
// browser-native `title` attribute). No drag, no resize, no edit, no
// rescheduling. No charting library — CSS-grid layout + absolute-
// positioned segments. Honours M2 Implementation Plan §8.1 and Canon
// §10 anti-patterns (no timeline editing, no scenario builder).
//
// Honours M2 Alignment Review (PR #60) guidance:
//   §3.1 No dashboard-first creep — focuses on operational legibility
//   §3.2 No demo-style interaction — title-tooltip only; no modals
//   §3.3 No frontend-heavy logic — derivation in adapter
//   §3.4 Display vs operational state separation — no display state in
//        adapter; component owns visual positioning only
//   §3.5 Role-agnostic components — no role naming baked in
//   §3.6 Function over polish — minimal CSS, no animations
//   §3.7 No Beta 10 patterns as precedent — React/Vite stack only
//   §3.8 No fixture-coupling — components parameterise on shape

export function BerthTimelineTab({ data }) {
  const tl = useMemo(() => adaptBerthTimeline(data), [data]);

  const hasWindow = tl.timeWindow && tl.timeWindow.startIso && tl.timeWindow.endIso;

  return (
    <div className="hz-bt-tab">
      <div className="hz-dashboard-head">
        <div>
          <h2 className="hz-dashboard-title">Berth timeline</h2>
          <p className="hz-dashboard-sub">
            Read-only occupancy view · adapter-fed · M2 · times in UTC
          </p>
        </div>
        <Pill tone="info" variant="outline">BERTH TIMELINE · READ-ONLY</Pill>
      </div>

      {!hasWindow && (
        <Card>
          <div className="hz-bt-empty">
            <Pill tone="info" variant="outline">NO SCHEDULED MOVEMENTS</Pill>
            <span style={{ marginLeft: 'var(--s-3)', color: 'var(--text-muted)' }}>
              No vessel windows available for this port.
            </span>
          </div>
        </Card>
      )}

      {hasWindow && (
        <Card>
          <div className="hz-bt-grid" role="table" aria-label="Berth occupancy timeline">
            <div className="hz-bt-axis-row">
              <div className="hz-bt-axis-spacer" aria-hidden="true" />
              <div className="hz-bt-axis-track">
                <TimeAxis timeWindow={tl.timeWindow} />
              </div>
            </div>
            {tl.rows.map((row) => (
              <BerthRow
                key={row.berthId}
                row={row}
                windowStartIso={tl.timeWindow.startIso}
                windowEndIso={tl.timeWindow.endIso}
              />
            ))}
          </div>
        </Card>
      )}

      {Array.isArray(tl.vesselsWithoutBerth) && tl.vesselsWithoutBerth.length > 0 && (
        <Card>
          <h3 className="hz-section-title">Vessels without a berth lane</h3>
          <p className="hz-section-sub">
            These vessels reference a berth not present in the current
            port's berth list. Shown for awareness only.
          </p>
          <ul className="hz-bt-orphans">
            {tl.vesselsWithoutBerth.map((vid) => (
              <li key={vid}><code>{vid}</code></li>
            ))}
          </ul>
        </Card>
      )}

      <div className="hz-dashboard-footer">
        <Pill tone="warning" variant="outline">FIXTURE DATA</Pill>
        <span style={{ marginLeft: 'var(--s-3)', fontSize: 11, color: 'var(--text-muted)' }}>
          Source: captured <code>/api/summary</code> snapshot · read-only ·
          no rescheduling · no editing
        </span>
      </div>
    </div>
  );
}
