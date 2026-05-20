import React from 'react';
import { Icon } from '../components/Icon.jsx';
import { Pill } from '../components/Pill.jsx';

// ConditionsBar — Canon §1.3 + §4.3. Persistent 64px ribbon below header.
// M1: bound to adapter-produced `conditions` block (per Adapter Note §6).

const RATING_TONE = {
  EXCELLENT: 'success',
  GOOD: 'success',
  MODERATE: 'warning',
  POOR: 'critical',
  UNKNOWN: 'info',
};

function fmt(value, digits = 1) {
  if (value === null || value === undefined) return '—';
  if (typeof value !== 'number') return String(value);
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(digits);
}

export function ConditionsBar({ conditions: c }) {
  if (!c) return null;
  return (
    <div className="hz-conditions">
      <Pill tone={RATING_TONE[c.rating] || 'info'}>{c.rating || 'UNKNOWN'}</Pill>

      <ConditionsTile
        icon="wind"
        label="WIND"
        value={`${fmt(c.windSpeedKts, 0)} kts`}
        sub={[c.windDirLabel, c.windBeaufort != null ? `BFT ${c.windBeaufort}` : null].filter(Boolean).join(' · ') || null}
      />
      <ConditionsTile
        icon="waves"
        label="SWELL"
        value={`${fmt(c.swellHeightM)} m`}
        sub={[c.swellPeriodS != null ? `${c.swellPeriodS} s` : null, c.swellDirLabel].filter(Boolean).join(' · ') || null}
      />
      <ConditionsTile
        icon="eye"
        label="VIS"
        value={c.visibilityNm != null ? `${fmt(c.visibilityNm)} nm` : '—'}
        sub={c.visibilityKm != null ? `(${fmt(c.visibilityKm)} km)` : null}
      />
      <ConditionsTile
        icon="gauge"
        label="PRESSURE"
        value={c.pressureHpa != null ? `${c.pressureHpa} hPa` : '—'}
      />
      <ConditionsTile
        icon="waves"
        label="TIDE"
        value={c.tideHeightM != null ? `${fmt(c.tideHeightM)} m ${c.tideState || ''}`.trim() : '—'}
        sub={c.tideNextLabel && c.tideNextTime ? `Next ${c.tideNextLabel} ${c.tideNextTime}` : null}
      />
      <ConditionsTile
        icon="alert"
        label="UKC"
        value={c.ukcM != null ? `${fmt(c.ukcM)} m` : '—'}
        sub={c.ukcStatus ? `(${c.ukcStatus})` : null}
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
