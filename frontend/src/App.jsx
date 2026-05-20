import React from 'react';
import { HorizonHeader } from './layout/HorizonHeader.jsx';
import { ConditionsBar } from './layout/ConditionsBar.jsx';
import { LeftPanel } from './layout/LeftPanel.jsx';
import { CenterPanel } from './layout/CenterPanel.jsx';
import { RightPanel } from './layout/RightPanel.jsx';
import { SAMPLE } from './data/sample.js';

// App — Horizon V1 M0 shell.
// Four-region layout per Canon §1.1:
//   • DEMO banner (M0 only)
//   • Top operational ribbon (HorizonHeader)
//   • Persistent conditions ribbon (ConditionsBar)
//   • 3-column shell (Left | Centre | Right panels)
//
// All data comes from frontend/src/data/sample.js (static fixture).
// No backend calls anywhere in this file or its children.

export default function App() {
  return (
    <div className="hz-app">
      <div className="hz-demo-banner">
        Horizon V1 — sandbox · demo data · not for operational use
      </div>
      <HorizonHeader summary={SAMPLE} />
      <ConditionsBar conditions={SAMPLE.conditions} />
      <div className="hz-shell">
        <LeftPanel />
        <CenterPanel />
        <RightPanel />
      </div>
    </div>
  );
}
