import React from 'react';
import { HorizonHeader } from './layout/HorizonHeader.jsx';
import { ConditionsBar } from './layout/ConditionsBar.jsx';
import { LeftPanel } from './layout/LeftPanel.jsx';
import { CenterPanel } from './layout/CenterPanel.jsx';
import { RightPanel } from './layout/RightPanel.jsx';
import { EmptyState } from './components/EmptyState.jsx';
import { useSummary } from './api/horizon.js';

// App — Horizon V1 M1 shell.
// Four-region layout per Canon §1.1. Data now flows through the
// adapter via useSummary() — no SAMPLE static import.
//
// Per HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md §10:
//   - HorizonHeader + ConditionsBar adapter-backed
//   - CenterPanel renders partial Dashboard tab
//   - LeftPanel + RightPanel remain M0 placeholders
//   - DemoBanner remains (fixture-backed, not live backend)

export default function App() {
  const { data, isLoading, error, lastUpdated, isStale } = useSummary();

  // Loading + error states use EmptyState component to preserve shell
  if (!data && (isLoading || error)) {
    return (
      <div className="hz-app">
        <div className="hz-demo-banner">
          Horizon V1 — sandbox · demo data · not for operational use
        </div>
        <EmptyState isLoading={isLoading} error={error} />
      </div>
    );
  }

  // Defensive: if data is somehow still null with no loading/error, render empty shell
  if (!data) {
    return (
      <div className="hz-app">
        <div className="hz-demo-banner">
          Horizon V1 — sandbox · demo data · not for operational use
        </div>
        <EmptyState />
      </div>
    );
  }

  return (
    <div className="hz-app">
      <div className="hz-demo-banner">
        Horizon V1 — sandbox · demo data · not for operational use
      </div>
      <HorizonHeader summary={data} lastUpdated={lastUpdated} isStale={isStale} />
      <ConditionsBar conditions={data.conditions} timezone={data.portTimezone} />
      {isStale && (
        <div className="hz-stale-banner">
          Connection lost — showing last good values from {lastUpdated?.toLocaleTimeString?.()}
        </div>
      )}
      <div className="hz-shell">
        <LeftPanel />
        <CenterPanel data={data} />
        <RightPanel />
      </div>
    </div>
  );
}
