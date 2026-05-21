// berthTimelineAdapter.js — M2 Implementation Plan §7.1 (Berth Timeline derivation)
//
// Pure presentation join over the already-adapted ViewSummary. Builds the
// read-only Berth Timeline view: one row per berth, vessel occupancy as
// time segments, conflict membership joined per segment.
//
// Contract per the M2 Implementation Plan §7.1:
//   Input:   ViewSummary (the output of summaryAdapter)
//   Reads:   berths[] (raw passthrough on the view), vessels[] (adapted),
//            conflicts[] (adapted)
//   Output:  {
//              rows: [{
//                berthId, name, status, readinessTimeIso, maxLoaM,
//                segments: [{
//                  vesselId, vesselName, vesselType, vesselStatus,
//                  startIso, endIso, scheduled, actualised,
//                  conflictIds, hasConflict,
//                }]
//              }],
//              timeWindow: { startIso, endIso, durationMs },
//              vesselsWithoutBerth: [vesselId, ...],
//            }
//   Null defence: empty input → { rows: [], timeWindow: { startIso: null,
//                                  endIso: null, durationMs: 0 },
//                                  vesselsWithoutBerth: [] }.
//
// Design decisions:
//   - Each segment is derived from a vessel's eta/etd/ata/atd. The
//     start/end are chosen as follows:
//       start = ata (if present) else eta
//       end   = atd (if present) else etd
//     A segment is included only if BOTH a start and an end resolve to a
//     valid ISO timestamp. Segments with unresolvable timing are dropped
//     (no fabrication, no synthesis) and the dropped vesselId is recorded
//     in vesselsWithoutBerth or skipped entirely.
//   - `actualised` is true if BOTH ata and atd are present (the vessel has
//     completed the call). `scheduled` is true if NEITHER ata nor atd is
//     present (the vessel has not yet arrived). Mid-call (berthed) is
//     represented by `actualised=false, scheduled=false`.
//   - Conflict join: each segment lists the conflictIds of conflicts whose
//     vesselIds include this segment's vesselId. `hasConflict` is true iff
//     conflictIds is non-empty.
//   - The time window is the [min start, max end] across all segments,
//     clamped to a minimum of 1 hour to avoid zero-width windows on
//     sparse data.
//   - Vessels with a berth_id that does not exist in the berths list are
//     dropped from rows (their berth has no lane) and surfaced in
//     vesselsWithoutBerth so the UI can show a notice.
//   - No business logic. No reordering. No recommendation. No COLREGS
//     classification. Pure data shape transformation.

const MIN_WINDOW_MS = 60 * 60 * 1000; // 1 hour

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseIsoMs(iso) {
  if (typeof iso !== 'string' || !iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function pickStartEnd(vessel) {
  // M1 vesselAdapter exposes etaIso, etdIso, ataIso, atdIso.
  const startIso = vessel.ataIso || vessel.etaIso || null;
  const endIso   = vessel.atdIso || vessel.etdIso || null;
  return { startIso, endIso };
}

export function adaptBerthTimeline(viewSummary) {
  const empty = {
    rows: [],
    timeWindow: { startIso: null, endIso: null, durationMs: 0 },
    vesselsWithoutBerth: [],
  };

  if (!viewSummary || typeof viewSummary !== 'object') return empty;

  const rawBerths    = safeArray(viewSummary.berths);
  const rawVessels   = safeArray(viewSummary.vessels);
  const rawConflicts = safeArray(viewSummary.conflicts);

  if (rawBerths.length === 0 && rawVessels.length === 0) return empty;

  // Index conflicts by vesselId for the join.
  const conflictsByVesselId = new Map();
  for (const c of rawConflicts) {
    if (!c || typeof c !== 'object') continue;
    const cid = c.conflictId;
    if (typeof cid !== 'string' || !cid) continue;
    for (const vid of safeArray(c.vesselIds)) {
      if (typeof vid !== 'string' || !vid) continue;
      const existing = conflictsByVesselId.get(vid);
      if (existing) existing.push(cid);
      else conflictsByVesselId.set(vid, [cid]);
    }
  }

  // Build the row scaffold from berths first so empty berths still appear.
  const rowByBerthId = new Map();
  for (const b of rawBerths) {
    if (!b || typeof b !== 'object') continue;
    const id = typeof b.id === 'string' ? b.id : null;
    if (!id) continue;
    rowByBerthId.set(id, {
      berthId: id,
      name: typeof b.name === 'string' ? b.name : id,
      status: typeof b.status === 'string' ? b.status : null,
      readinessTimeIso: typeof b.readiness_time === 'string' ? b.readiness_time : null,
      maxLoaM: typeof b.max_loa === 'number' && isFinite(b.max_loa) ? b.max_loa : null,
      segments: [],
    });
  }

  // Walk vessels, append segments to the matching berth row.
  const vesselsWithoutBerth = [];
  let minStartMs = Infinity;
  let maxEndMs   = -Infinity;

  for (const v of rawVessels) {
    if (!v || typeof v !== 'object') continue;
    const vesselId = typeof v.vesselId === 'string' ? v.vesselId : null;
    if (!vesselId) continue;

    const berthId = typeof v.berth === 'string' ? v.berth : null;
    if (!berthId) {
      vesselsWithoutBerth.push(vesselId);
      continue;
    }
    const row = rowByBerthId.get(berthId);
    if (!row) {
      vesselsWithoutBerth.push(vesselId);
      continue;
    }

    const { startIso, endIso } = pickStartEnd(v);
    const startMs = parseIsoMs(startIso);
    const endMs   = parseIsoMs(endIso);
    if (startMs === null || endMs === null || endMs <= startMs) {
      // Dropped: insufficient or inconsistent timing data.
      // Not surfaced as vesselsWithoutBerth (the vessel is correctly
      // assigned to a berth; its timing is just unusable for the lane).
      continue;
    }

    if (startMs < minStartMs) minStartMs = startMs;
    if (endMs   > maxEndMs)   maxEndMs   = endMs;

    const hasAta = typeof v.ataIso === 'string' && v.ataIso.length > 0;
    const hasAtd = typeof v.atdIso === 'string' && v.atdIso.length > 0;
    const conflictIds = conflictsByVesselId.get(vesselId) || [];

    row.segments.push({
      vesselId,
      vesselName: typeof v.name === 'string' ? v.name : null,
      vesselType: typeof v.type === 'string' ? v.type : null,
      vesselStatus: typeof v.status === 'string' ? v.status : null,
      startIso,
      endIso,
      scheduled: !hasAta && !hasAtd,
      actualised: hasAta && hasAtd,
      conflictIds,
      hasConflict: conflictIds.length > 0,
    });
  }

  // Sort each row's segments by start time so the timeline renders left-to-right.
  for (const row of rowByBerthId.values()) {
    row.segments.sort((a, b) => parseIsoMs(a.startIso) - parseIsoMs(b.startIso));
  }

  // Final time window.
  let startIso = null;
  let endIso = null;
  let durationMs = 0;
  if (minStartMs !== Infinity && maxEndMs !== -Infinity) {
    if (maxEndMs - minStartMs < MIN_WINDOW_MS) {
      maxEndMs = minStartMs + MIN_WINDOW_MS;
    }
    startIso = new Date(minStartMs).toISOString();
    endIso   = new Date(maxEndMs).toISOString();
    durationMs = maxEndMs - minStartMs;
  }

  return {
    rows: Array.from(rowByBerthId.values()),
    timeWindow: { startIso, endIso, durationMs },
    vesselsWithoutBerth,
  };
}
