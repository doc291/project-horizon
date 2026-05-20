// dashboardAdapter.js — Adapter Note §10 (dashboard + port_status → dashboardMetrics)
// Merges two raw blocks; renames snake_case to camelCase.

function safeNumber(value, fallback = null) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

export function adaptDashboard(rawDashboard, rawPortStatus) {
  const d = rawDashboard && typeof rawDashboard === 'object' ? rawDashboard : {};
  const p = rawPortStatus && typeof rawPortStatus === 'object' ? rawPortStatus : {};

  return {
    // Operations
    berthUtilisationPct: safeNumber(d.berth_utilisation_pct),
    forecastUtilisation48h: safeNumber(d.forecast_utilisation_48h),
    vesselsInPort: safeNumber(p.vessels_in_port ?? d.vessels_in_port),
    vesselsExpected24h: safeNumber(p.vessels_expected_24h ?? d.vessels_expected_24h),
    vesselsDeparting24h: safeNumber(p.vessels_departing_24h),
    activeConflicts: safeNumber(p.active_conflicts ?? d.active_conflicts, 0),
    criticalConflicts: safeNumber(p.critical_conflicts ?? d.critical_conflicts, 0),
    // Performance
    onTimeDeparturePct: safeNumber(d.on_time_departure_pct),
    avgDwellHours: safeNumber(d.avg_dwell_hours),
    pilotOps12h: safeNumber(d.pilot_ops_12h, 0),
    tugOps12h: safeNumber(d.tug_ops_12h, 0),
    // Safety
    vesselsAtRisk: safeNumber(d.vessels_at_risk, 0),
    // Resources
    pilotsAvailable: safeNumber(p.pilots_available),
    tugsAvailable: safeNumber(p.tugs_available),
    berthsOccupied: safeNumber(p.berths_occupied),
    berthsAvailable: safeNumber(p.berths_available),
    berthsTotal: safeNumber(p.berths_total),
  };
}
