// pilotageAdapter.js — M2 Implementation Plan §7.4 (Pilotage derivation).
//
// Pure presentation normalisation over the already-adapted ViewSummary.
// Takes the raw pilotage array (passed through unmodified by summaryAdapter)
// and produces a typed, sorted, read-only view of pilotage assignments.
//
// Critical framing (per the user's authorisation):
//   The V1 pilotage data is SIMULATED or DERIVED, not authoritative
//   operational pilotage data. The adapter normalises the existing
//   records — it does NOT invent records, does NOT validate fatigue or
//   competency, does NOT compute roster compliance, and does NOT cross
//   the Kyber boundary. Pilot identifiers are resource-safe codes
//   already present in the fixture (e.g. "PILOT_BNE_PSP_03"); they are
//   passed through verbatim.
//
// Contract:
//   Input:   ViewSummary (the output of summaryAdapter)
//   Reads:   pilotage[] — array of raw records of shape
//            { id, vessel_id, vessel_name, pilot_name, scheduled_time,
//              boarding_station, direction, status }
//   Output:  {
//              assignments: [{
//                id, vesselId, vesselName, pilotId, scheduledTimeIso,
//                boardingStation, direction, status,
//              }],
//              counts: { total, inbound, outbound, byStatus: { <status>: n } },
//              missingDomains: ['pilotage' | ...] when input is missing,
//            }
//   Null defence: empty / null / malformed input → empty arrays, no throw.
//
// Design decisions:
//   - The adapter contains zero business rules. It is a pure shape
//     normalisation + sort.
//   - Records with no scheduled_time, no vessel_id, or no id are dropped
//     (defensive — we do not synthesise identifiers or timestamps).
//   - Sort: by scheduled time ascending (earliest first) so the tab
//     reads as a forward-looking queue. Stable tiebreaker on id.
//   - Pilot identifier is exposed as `pilotId` (the existing fixture
//     field is `pilot_name`, but the value is a code, not a real
//     person's name — `pilotId` is a more honest field name in the
//     view-model).
//   - The view-model does NOT carry fatigue, competency, certification,
//     or roster-compliance fields. Those would require an authoritative
//     pilotage system that V1 does not have.

const KNOWN_DIRECTIONS = new Set(['inbound', 'outbound']);

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseIsoMs(iso) {
  if (typeof iso !== 'string' || !iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function safeString(value) {
  return typeof value === 'string' ? value : null;
}

function normaliseRecord(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const id        = safeString(raw.id);
  const vesselId  = safeString(raw.vessel_id);
  const scheduled = safeString(raw.scheduled_time);
  if (!id || !vesselId || !scheduled || parseIsoMs(scheduled) === null) {
    return null;
  }
  const direction = safeString(raw.direction);
  return {
    id,
    vesselId,
    vesselName: safeString(raw.vessel_name),
    pilotId: safeString(raw.pilot_name),  // resource-safe code, not a real name
    scheduledTimeIso: scheduled,
    boardingStation: safeString(raw.boarding_station),
    direction: KNOWN_DIRECTIONS.has(direction) ? direction : null,
    status: safeString(raw.status),
  };
}

export function adaptPilotage(viewSummary) {
  const empty = {
    assignments: [],
    counts: { total: 0, inbound: 0, outbound: 0, byStatus: {} },
    missingDomains: ['pilotage'],
  };

  if (!viewSummary || typeof viewSummary !== 'object') return empty;

  const raw = safeArray(viewSummary.pilotage);
  if (raw.length === 0) return empty;

  const assignments = raw.map(normaliseRecord).filter(Boolean);

  // Sort by scheduled time ascending; stable tiebreaker on id.
  assignments.sort((a, b) => {
    const ta = parseIsoMs(a.scheduledTimeIso) || 0;
    const tb = parseIsoMs(b.scheduledTimeIso) || 0;
    if (ta !== tb) return ta - tb;
    return a.id.localeCompare(b.id);
  });

  // Counts
  const counts = {
    total: assignments.length,
    inbound: 0,
    outbound: 0,
    byStatus: {},
  };
  for (const a of assignments) {
    if (a.direction === 'inbound')  counts.inbound  += 1;
    if (a.direction === 'outbound') counts.outbound += 1;
    if (a.status) {
      counts.byStatus[a.status] = (counts.byStatus[a.status] || 0) + 1;
    }
  }

  return {
    assignments,
    counts,
    missingDomains: [],
  };
}
