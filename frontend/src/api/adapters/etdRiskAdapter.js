// etdRiskAdapter.js — Adapter Note §11 (etd_risk array normalisation)
// delta is NEVER synthesised (Adapter Note §11.2).

import { mapRiskLevel } from './status.js';

function safeNumber(value, fallback = null) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

function safeString(value) {
  return typeof value === 'string' ? value : null;
}

export function adaptEtdRisk(rawEtdRisk) {
  if (!Array.isArray(rawEtdRisk)) return [];

  return rawEtdRisk.map((r) => {
    if (!r || typeof r !== 'object') return null;
    const factors = Array.isArray(r.risk_factors) ? r.risk_factors : [];
    return {
      vesselId: safeString(r.vessel_id),
      vesselName: safeString(r.vessel_name),
      riskScore: safeNumber(r.risk_score, 0),
      riskLevel: mapRiskLevel(r.risk_level),
      riskFactors: factors,
      reasonPrimary: factors[0] || null,
      delta: null, // Adapter Note §11.2 — never synthesised
    };
  }).filter(Boolean);
}
