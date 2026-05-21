import React, { useState } from 'react';
import { DashboardTab } from '../features/dashboard/DashboardTab.jsx';
import { BerthTimelineTab } from '../features/berth-timeline/BerthTimelineTab.jsx';
import { ShiftLogTab } from '../features/shift-log/ShiftLogTab.jsx';
import { VtsTab } from '../features/vts/VtsTab.jsx';
import { PilotageTab } from '../features/pilotage/PilotageTab.jsx';
import { PerformanceTab } from '../features/performance/PerformanceTab.jsx';
import { CenterPanelNav } from './CenterPanelNav.jsx';

// CenterPanel — Canon §1.1.3 + §4.5.
// M2 final slice: in-memory Centre Panel Navigation switches between
// the six Canon §1.1.3 centre-spine views. Default view is Dashboard
// (the M1-shipped default, preserved here so the operator-first
// posture of UX Direction §3.2 is the M3+ concern, not M2's).
//
// This is CENTRE-PANEL view navigation only — NOT port switching.
// The port context is read-only; port access is governed by login /
// user permissions / role-based access in the future platform model.

export function renderActiveTab(activeTab, data) {
  switch (activeTab) {
    case 'berth-timeline': return <BerthTimelineTab data={data} />;
    case 'shift-log':      return <ShiftLogTab data={data} />;
    case 'vts':            return <VtsTab data={data} />;
    case 'pilotage':       return <PilotageTab data={data} />;
    case 'performance':    return <PerformanceTab />;
    case 'dashboard':
    default:               return <DashboardTab data={data} />;
  }
}

export function CenterPanel({ data }) {
  const [activeTab, setActiveTab] = useState('dashboard');

  return (
    <main className="hz-panel hz-panel-center">
      <CenterPanelNav activeTab={activeTab} onChange={setActiveTab} />
      <section
        className="hz-center-body"
        role="tabpanel"
        id={`hz-center-panel-${activeTab}`}
        aria-label="Centre panel content"
      >
        {renderActiveTab(activeTab, data)}
      </section>
    </main>
  );
}
