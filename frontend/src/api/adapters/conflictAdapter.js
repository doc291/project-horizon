// conflictAdapter.js — Adapter Note §8 (conflict normalisation; cascade: null)
// Severity uppercased; cascade NEVER synthesised; options flattened from decision_support.

import { mapSeverity } from './status.js';
import { isoToDateOrNull, localShort } from './time.js';

function safeNumber(value, fallback = null) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

function safeString(value) {
  return typeof value === 'string' ? value : null;
}

function humaniseType(t) {
  if (typeof t !== 'string' || !t.length) return 'Conflict';
  return t.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

function deriveTitle(conflictType, vesselNames) {
  const base = humaniseType(conflictType);
  if (Array.isArray(vesselNames) && vesselNames.length) {
    return `${base}: ${vesselNames.join(' vs ')}`;
  }
  return base;
}

export function adaptConflicts(rawConflicts, portTimezone) {
  if (!Array.isArray(rawConflicts)) return [];

  return rawConflicts.map((c) => {
    if (!c || typeof c !== 'object') return null;

    const decisionDeadlineIso = safeString(c.decision_support?.decision_deadline);
    const conflictTimeIso = safeString(c.conflict_time);

    const ds = c.decision_support && typeof c.decision_support === 'object' ? c.decision_support : null;
    const seqAlts = Array.isArray(c.sequencing_alternatives) ? c.sequencing_alternatives : [];

    return {
      conflictId: safeString(c.id),
      type: safeString(c.conflict_type),
      signalType: safeString(c.signal_type),
      severity: mapSeverity(c.severity),
      title: deriveTitle(c.conflict_type, c.vessel_names),
      description: safeString(c.description),
      vesselIds: Array.isArray(c.vessel_ids) ? c.vessel_ids : [],
      vesselNames: Array.isArray(c.vessel_names) ? c.vessel_names : [],
      berth: safeString(c.berth_id),
      berthName: safeString(c.berth_name),
      conflictTimeIso,
      conflictTime: isoToDateOrNull(conflictTimeIso),
      conflictTimeShort: conflictTimeIso ? localShort(conflictTimeIso, portTimezone) : null,
      decisionDeadlineIso,
      decisionDeadline: isoToDateOrNull(decisionDeadlineIso),
      decisionDeadlineShort: decisionDeadlineIso ? localShort(decisionDeadlineIso, portTimezone) : null,
      recommendedOptionId: ds ? safeString(ds.recommended_option_id) : null,
      recommendedReasoning: ds ? safeString(ds.recommended_reasoning) : null,
      confidence: ds ? safeString(ds.confidence) : null,
      options: seqAlts, // flat array of {id, label, desc/strategy, risk, ...} per server.py
      resolutionGuidance: Array.isArray(c.resolution_options) ? c.resolution_options : [],
      overallSafetyLabel: safeString(c.safety_score),
      dataSource: safeString(c.data_source),
      cascade: null, // Canon §8.8 — never synthesised
    };
  }).filter(Boolean);
}
