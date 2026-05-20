import React from 'react';

// RiskBar — inline 0–100 progress bar coloured by riskLevel.
// Per M1 Implementation Plan §11 — small component, no new primitives.

const TONE_VAR = {
  critical: 'var(--critical)',
  high: 'var(--warning)',
  medium: 'var(--teal)',
  low: 'var(--success)',
};

export function RiskBar({ score, level }) {
  const value = typeof score === 'number' ? Math.max(0, Math.min(100, score)) : 0;
  const colour = TONE_VAR[level] || TONE_VAR.low;
  return (
    <div className="hz-risk-bar" aria-label={`Risk ${value} of 100, ${level || 'unknown'} level`}>
      <div
        className="hz-risk-bar-fill"
        style={{ width: `${value}%`, background: colour }}
      />
      <span className="hz-risk-bar-value">{value}</span>
    </div>
  );
}
