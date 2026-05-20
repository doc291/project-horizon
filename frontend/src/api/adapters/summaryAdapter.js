// summaryAdapter.js — Adapter Note §5 (top-level mapping)
// Combines the per-domain adapters into a single ViewSummary per §2.2.
// Pure function. No state. Defensive against missing input.

import { adaptConditions } from './conditionsAdapter.js';
import { adaptVessels } from './vesselAdapter.js';
import { adaptConflicts } from './conflictAdapter.js';
import { adaptGuidance } from './guidanceAdapter.js';
import { adaptDashboard } from './dashboardAdapter.js';
import { adaptEtdRisk } from './etdRiskAdapter.js';
import { mapSource } from './status.js';
import { isoToDateOrNull } from './time.js';

const EMPTY = {};

function safeObject(v) {
  return v && typeof v === 'object' && !Array.isArray(v) ? v : EMPTY;
}

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function safeString(v, fallback = null) {
  return typeof v === 'string' ? v : fallback;
}

export function adaptSummary(rawSummary) {
  // Per Adapter Note §15.7 — adapter always returns a parseable ViewSummary,
  // even from empty / null / malformed input.
  const raw = safeObject(rawSummary);

  const portProfile = safeObject(raw.port_profile);
  const portTimezone = safeString(portProfile.timezone, 'Australia/Brisbane');
  const portId = safeString(portProfile.id, 'BRISBANE');
  const portName = safeString(raw.port_name, 'Unknown Port');

  const missingDomains = [];
  // shiftLog has no backend source today (Adapter Note §12.2)
  missingDomains.push('shiftLog');
  // auditLog has no backend source today
  missingDomains.push('auditLog');
  // cascade is never synthesised (Adapter Note §8.8)
  missingDomains.push('cascade');

  // Liveness flags — sourced from port_profile when present
  const liveness = {
    vessel: portProfile.using_live_vessel_data === true,
    weather: portProfile.using_live_weather_data === true,
    tide: portProfile.using_live_tidal_data === true,
    isLive: false,
  };
  liveness.isLive = liveness.vessel; // primary "live" signal per Adapter Note §14.4 boolean

  // Detect partial response (a sentinel for the UI to surface "stale" state)
  const isPartial = Object.keys(raw).length === 0;
  if (isPartial) {
    missingDomains.push('all');
  }

  const generatedAtIso = safeString(raw.generated_at);

  return {
    timestamp: isoToDateOrNull(generatedAtIso),
    generatedAtIso,
    portId,
    portName,
    portTimezone,
    portStatus: {
      berthsOccupied: safeObject(raw.port_status).berths_occupied ?? null,
      berthsAvailable: safeObject(raw.port_status).berths_available ?? null,
      berthsTotal: safeObject(raw.port_status).berths_total ?? null,
      vesselsInPort: safeObject(raw.port_status).vessels_in_port ?? null,
      vesselsExpected24h: safeObject(raw.port_status).vessels_expected_24h ?? null,
      vesselsDeparting24h: safeObject(raw.port_status).vessels_departing_24h ?? null,
      activeConflicts: safeObject(raw.port_status).active_conflicts ?? 0,
      criticalConflicts: safeObject(raw.port_status).critical_conflicts ?? 0,
      pilotsAvailable: safeObject(raw.port_status).pilots_available ?? null,
      tugsAvailable: safeObject(raw.port_status).tugs_available ?? null,
    },
    conditions: adaptConditions(raw.weather, raw.tides, raw.ukc, portTimezone),
    vessels: adaptVessels(raw.vessels, raw.etd_risk, portTimezone),
    berths: safeArray(raw.berths),
    conflicts: adaptConflicts(raw.conflicts, portTimezone),
    guidance: adaptGuidance(raw.guidance, raw.conflicts, portTimezone),
    pilotage: safeArray(raw.pilotage),
    towage: safeArray(raw.towage),
    dashboardMetrics: adaptDashboard(raw.dashboard, raw.port_status),
    etdRisk: adaptEtdRisk(raw.etd_risk),
    liveness,
    dataSource: mapSource(raw.data_source),
    dataSourceLabel: safeString(raw.data_source_label, 'Demo · Fixture'),
    availablePorts: safeArray(portProfile.available_ports),
    missingDomains,
  };
}
