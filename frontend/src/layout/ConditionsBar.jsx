import React from 'react';
import { Icon } from '../components/Icon.jsx';
import { Pill } from '../components/Pill.jsx';

// ConditionsBar — Canon §1.3 + §4.3. Persistent 64px ribbon below header.
// Surfaces weather/tide/UKC tiles; every tile carries an explicit (demo)
// marker per Plan v0.2 §9.

const RATING_TONE = {
  EXCELLENT: 'success',
  GOOD: 'success',
  MODERATE: 'warning',
  POOR: 'critical',
};

export function ConditionsBar({ conditions: c }) {
  return (
    <div className="hz-conditions">
      <Pill tone={RATING_TONE[c.rating] || 'info'}>{c.rating}</Pill>

      <ConditionsTile
        icon="wind"
        label="WIND"
        value={`${c.windSpeedKts} kts`}
        sub={`${c.windDirLabel} · BFT ${c.windBeaufort}`}
      />
      <ConditionsTile
        icon="waves"
        label="SWELL"
        value={`${c.swellHeightM} m`}
        sub={`${c.swellPeriodS} s · ${c.swellDirLabel}`}
      />
      <ConditionsTile
        icon="eye"
        label="VIS"
        value={`${c.visibilityNm} nm`}
        sub={`(${c.visibilityKm.toFixed(1)} km)`}
      />
      <ConditionsTile
        icon="gauge"
        label="PRESSURE"
        value={`${c.pressureHpa} hPa`}
      />
      <ConditionsTile
        icon="waves"
        label="TIDE"
        value={`${c.tideHeightM} m ${c.tideState}`}
        sub={`Next ${c.tideNextLabel} ${c.tideNextTime}`}
      />
      <ConditionsTile
        icon="alert"
        label="UKC"
        value={`${c.ukcM} m`}
        sub={`(${c.ukcStatus})`}
      />
    </div>
  );
}

function ConditionsTile({ icon, label, value, sub }) {
  return (
    <div className="hz-cond-tile">
      <Icon name={icon} size={16} />
      <div className="hz-cond-text">
        <div className="hz-cond-label">
          {label}<span className="hz-cond-demo">(demo)</span>
        </div>
        <div className="hz-cond-value">{value}</div>
        {sub ? <div className="hz-cond-sub">{sub}</div> : null}
      </div>
    </div>
  );
}
