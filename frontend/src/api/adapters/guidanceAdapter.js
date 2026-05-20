// guidanceAdapter.js — Adapter Note §9 (guidance + alert-shaped conflicts)
// Combines raw guidance[] items with alert-shaped conflicts (signal_type
// ∈ WARNING / ADVISORY / WEATHER) into a unified, sorted alert list.

import { mapSeverity } from './status.js';
import { isoToDateOrNull, localShort } from './time.js';

function safeString(value) {
  return typeof value === 'string' ? value : null;
}

const SEVERITY_ORDER = { CRITICAL: 0, HIGH: 1, WARNING: 1, MEDIUM: 2, ADVISORY: 2, LOW: 3, INFO: 4 };

function severityRank(s) {
  if (typeof s !== 'string') return 99;
  return SEVERITY_ORDER[s.toUpperCase()] ?? 99;
}

function mapGuidanceCategory(rawCategory, conflictType) {
  if (typeof rawCategory === 'string' && rawCategory.length) return rawCategory.toUpperCase();
  if (typeof conflictType === 'string') {
    if (conflictType.startsWith('berth')) return 'BERTH';
    if (conflictType.startsWith('pilotage') || conflictType.startsWith('tug')) return 'NAVIGATION';
    if (conflictType.startsWith('weather') || conflictType.startsWith('wind') ||
        conflictType.startsWith('swell') || conflictType.startsWith('vis')) return 'WEATHER';
  }
  return 'INFO';
}

export function adaptGuidance(rawGuidance, rawConflicts, portTimezone) {
  const items = [];

  // 1. Raw guidance items
  if (Array.isArray(rawGuidance)) {
    for (const g of rawGuidance) {
      if (!g || typeof g !== 'object') continue;
      items.push({
        id: `g-${safeString(g.id) || items.length}`,
        category: mapGuidanceCategory(g.category, null),
        severity: typeof g.severity === 'string' ? g.severity.toUpperCase() : 'INFO',
        title: safeString(g.title),
        detail: safeString(g.detail),
        deadline: safeString(g.deadline),
        timestampShort: safeString(g.ts),
      });
    }
  }

  // 2. Alert-shaped conflicts (signal_type ∈ WARNING / ADVISORY / WEATHER)
  if (Array.isArray(rawConflicts)) {
    for (const c of rawConflicts) {
      if (!c || typeof c !== 'object') continue;
      const sig = typeof c.signal_type === 'string' ? c.signal_type.toUpperCase() : '';
      if (sig === 'CONFLICT') continue; // these are decision-card items, not alerts
      if (!['WARNING', 'ADVISORY', 'WEATHER', 'INFO'].includes(sig)) continue;

      items.push({
        id: `c-${safeString(c.id) || items.length}`,
        category: mapGuidanceCategory(null, c.conflict_type),
        severity: mapSeverity(c.severity),
        title: safeString(c.description) ? safeString(c.description).split('.')[0] : safeString(c.conflict_type),
        detail: safeString(c.description),
        deadline: c.decision_support?.decision_deadline
          ? localShort(c.decision_support.decision_deadline, portTimezone)
          : null,
        timestampShort: c.conflict_time ? localShort(c.conflict_time, portTimezone) : null,
      });
    }
  }

  // 3. Sort: severity ascending, then by title for stability
  items.sort((a, b) => {
    const r = severityRank(a.severity) - severityRank(b.severity);
    if (r !== 0) return r;
    return (a.title || '').localeCompare(b.title || '');
  });

  return items;
}
