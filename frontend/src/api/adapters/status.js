// status.js — Adapter Note §14 (severity / status / source mappings)
// Closed-set maps. Unknown values fall back to safe defaults; never throws.

const SEVERITY_MAP = {
  critical: 'CRITICAL',
  high: 'HIGH',
  medium: 'MEDIUM',
  low: 'LOW',
};

const RATING_MAP = {
  Excellent: 'EXCELLENT',
  Good: 'GOOD',
  Moderate: 'MODERATE',
  Poor: 'POOR',
};

const SOURCE_MAP = {
  ais: 'AIS',
  mst: 'MST',
  qships: 'QShips',
  live: 'Live',
  simulation: 'Simulation',
  sim: 'Simulation',
  mock: 'Simulation',
  bom: 'BOM',
};

export function mapSeverity(raw) {
  if (typeof raw !== 'string') return 'INFO';
  return SEVERITY_MAP[raw.toLowerCase()] || 'INFO';
}

export function mapConditionsRating(raw) {
  if (typeof raw !== 'string') return 'UNKNOWN';
  return RATING_MAP[raw] || RATING_MAP[raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase()] || 'UNKNOWN';
}

export function mapSource(raw) {
  if (raw == null || raw === '') return 'Simulation';
  if (typeof raw !== 'string') return 'Simulation';
  return SOURCE_MAP[raw.toLowerCase()] || 'Simulation';
}

export function mapRiskLevel(raw) {
  if (typeof raw !== 'string') return 'low';
  const v = raw.toLowerCase();
  if (['critical', 'high', 'medium', 'low'].includes(v)) return v;
  return 'low';
}
