// vesselAdapter.js — Adapter Note §7 (vessel normalisation + risk join)
// Joins etd_risk onto each vessel by vessel_id; missing match → default risk.

import { mapSource, mapRiskLevel } from './status.js';
import { isoToDateOrNull, localShort } from './time.js';

function safeNumber(value, fallback = null) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

function safeString(value) {
  return typeof value === 'string' ? value : null;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

export function adaptVessels(rawVessels, rawEtdRisk, portTimezone) {
  if (!Array.isArray(rawVessels)) return [];

  const riskByVesselId = {};
  if (Array.isArray(rawEtdRisk)) {
    for (const r of rawEtdRisk) {
      if (r && typeof r === 'object' && typeof r.vessel_id === 'string') {
        riskByVesselId[r.vessel_id] = r;
      }
    }
  }

  return rawVessels.map((v) => {
    if (!v || typeof v !== 'object') return null;
    const risk = riskByVesselId[v.id] || null;

    const etaIso = safeString(v.eta);
    const etdIso = safeString(v.etd);
    const ataIso = safeString(v.ata);
    const atdIso = safeString(v.atd);

    return {
      vesselId: safeString(v.id),
      name: safeString(v.name),
      imo: safeString(v.imo),
      type: safeString(v.vessel_type),
      flag: safeString(v.flag),
      status: safeString(v.status),
      source: mapSource(v.source),
      etaIso,
      eta: isoToDateOrNull(etaIso),
      etaLocalShort: etaIso ? localShort(etaIso, portTimezone) : null,
      etdIso,
      etd: isoToDateOrNull(etdIso),
      etdLocalShort: etdIso ? localShort(etdIso, portTimezone) : null,
      ataIso,
      atdIso,
      berth: safeString(v.berth_id),
      loaM: safeNumber(v.loa),
      draftM: safeNumber(v.draught),
      cargo: safeString(v.cargo_type),
      pilotageRequired: typeof v.pilotage_required === 'boolean' ? v.pilotage_required : null,
      towageRequired: typeof v.towage_required === 'boolean' ? v.towage_required : null,
      agent: safeString(v.agent),
      notes: v.notes === undefined ? null : v.notes,
      lat: safeNumber(v.lat),
      lon: safeNumber(v.lon),
      // Risk join
      riskScore: risk ? safeNumber(risk.risk_score, 0) : 0,
      riskLevel: risk ? mapRiskLevel(risk.risk_level) : 'low',
      riskFactors: risk ? safeArray(risk.risk_factors) : [],
    };
  }).filter(Boolean);
}
