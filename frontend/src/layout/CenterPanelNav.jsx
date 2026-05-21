import React from 'react';

// CenterPanelNav — Centre Panel Navigation (M2 final slice).
//
// In-memory, presentational tab strip for switching between the six
// Canon §1.1.3 centre-spine views: Dashboard, Berth Timeline, Shift
// Log, VTS, Pilotage, Performance.
//
// Critical clarification (per the Centre Panel Navigation correction):
//   This is CENTRE-PANEL view navigation only. It is NOT port
//   switching. There is NO port selector, NO port switcher, NO cross-
//   port UI behaviour. The port context is read-only and comes from
//   the running summary/fixture/runtime context. Port access is
//   controlled by login / user permissions / role-based access in the
//   future platform model (M3+).
//
// Controlled component contract:
//   { activeTab: string, onChange: (tabKey: string) => void }
// Renders six tabs as <button type="button" role="tab" ...>. The
// parent (CenterPanel) owns the activeTab state.
//
// Accessibility:
//   - role="tablist" on the wrapping nav
//   - role="tab" + aria-selected on each button
//   - The activeTab key appears in the data-active-tab attribute on
//     the wrapping nav for testability without DOM interaction.

export const CENTRE_PANEL_TABS = Object.freeze([
  { key: 'dashboard',      label: 'Dashboard' },
  { key: 'berth-timeline', label: 'Berth Timeline' },
  { key: 'shift-log',      label: 'Shift Log' },
  { key: 'vts',            label: 'VTS' },
  { key: 'pilotage',       label: 'Pilotage' },
  { key: 'performance',    label: 'Performance' },
]);

export function CenterPanelNav({ activeTab, onChange }) {
  const safeActive = CENTRE_PANEL_TABS.some(t => t.key === activeTab)
    ? activeTab
    : 'dashboard';

  function handle(tabKey) {
    if (typeof onChange === 'function' && tabKey !== safeActive) {
      onChange(tabKey);
    }
  }

  return (
    <nav
      className="hz-center-nav"
      role="tablist"
      aria-label="Centre panel view"
      data-active-tab={safeActive}
    >
      {CENTRE_PANEL_TABS.map((tab) => {
        const isActive = tab.key === safeActive;
        const classes = ['hz-center-nav-tab'];
        if (isActive) classes.push('hz-center-nav-tab-active');
        return (
          <button
            key={tab.key}
            type="button"
            role="tab"
            aria-selected={isActive}
            aria-controls={`hz-center-panel-${tab.key}`}
            className={classes.join(' ')}
            onClick={() => handle(tab.key)}
          >
            {tab.label}
          </button>
        );
      })}
    </nav>
  );
}
