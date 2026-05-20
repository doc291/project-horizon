import React from 'react';
import { DashboardTab } from '../features/dashboard/DashboardTab.jsx';

// CenterPanel — Canon §1.1.3 + §4.5.
// M1: renders partial Dashboard tab consuming adapter output.
// Other 5 tabs (Berth Timeline, Shift Log, VTS, Pilotage, Performance)
// remain deferred to M2+ per Execution Plan §11.

export function CenterPanel({ data }) {
  return (
    <main className="hz-panel hz-panel-center">
      <DashboardTab data={data} />
    </main>
  );
}
