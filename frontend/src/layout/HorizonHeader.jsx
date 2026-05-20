import React, { useEffect, useState } from 'react';
import { Icon } from '../components/Icon.jsx';
import { Pill } from '../components/Pill.jsx';
import { Dot } from '../components/Dot.jsx';

// HorizonHeader — Canon §1.1.1 + §4.2. Sticky 72px top ribbon.
// Identity + 4 stat tiles + role pill + data-source pill + clock + co-brand.
// All data from sample.js; clock is real (local timekeeping, not Beta 10 data).

function useClock() {
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  return now;
}

function fmtClock(d) {
  return d.toLocaleTimeString('en-AU', {
    hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}

function fmtDate(d) {
  return d.toLocaleDateString('en-AU', {
    weekday: 'short', day: '2-digit', month: 'short',
  });
}

export function HorizonHeader({ summary }) {
  const now = useClock();
  const { portName, portStatus, conditions, dashboardMetrics } = summary;
  const movements6h = dashboardMetrics.pilotOps12h + dashboardMetrics.tugOps12h;

  return (
    <header className="hz-header">
      <div className="hz-header-brand">
        <Icon name="anchor" size={22} style={{ color: 'var(--teal)' }} />
        <span className="hz-brand-wordmark">HORIZON</span>
        <span className="hz-brand-port">{portName}</span>
      </div>

      <div className="hz-header-stats">
        <Stat label="Vessels in port" value={portStatus.vesselsInPort} />
        <Stat label="Movements 6h" value={movements6h} />
        <Stat
          label="Conflicts"
          value={portStatus.criticalConflicts}
          pulse={portStatus.criticalConflicts > 0}
        />
        <Stat label="Conditions" value={conditions.rating} />
      </div>

      <div className="hz-header-right">
        <Pill tone="info" variant="outline">VTSO</Pill>
        <Pill tone="warning">DEMO · SIMULATION</Pill>
        <div className="hz-clock">
          <div className="hz-clock-time">{fmtClock(now)}</div>
          <div className="hz-clock-date">{fmtDate(now)}</div>
        </div>
        <div className="hz-cobrand">AMS Group</div>
      </div>
    </header>
  );
}

function Stat({ label, value, pulse }) {
  return (
    <div className="hz-stat">
      <div className="hz-stat-label">{label}</div>
      <div className="hz-stat-value">
        {pulse ? <Dot tone="critical" pulse /> : null}
        {value}
      </div>
    </div>
  );
}
