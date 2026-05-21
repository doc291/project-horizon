// vtsAdapter.js — M2 Implementation Plan §7.3 (VTS view derivation)
//
// Pure presentation join over the already-adapted ViewSummary. Builds the
// read-only VTS view: a vessel list joined with conflict membership, plus
// a parallel conflicts list filtered to the operational signal types (CONFLICT
// and WARNING).
//
// Contract per the M2 Implementation Plan §7.3:
//   Input:   ViewSummary (the output of summaryAdapter)
//   Reads:   vessels[], conflicts[]
//   Output:  {
//              vessels: [{ vesselId, name, type, status, berth, loaM,
//                           riskLevel, lat, lon, hasConflict, conflictIds }],
//              conflicts: [{ conflictId, type, severity, signalType, title,
//                             vesselIds, vesselNames, berth, dataSource }],
//            }
//   Null defence: empty input → { vessels: [], conflicts: [] }.
//
// Design decisions:
//   - heading / speed / course are NOT exposed by this adapter. The captured
//     /api/summary fixtures (PR #49) do not carry these fields, and the
//     adapter must not synthesise them (M2 Implementation Plan §7 "no
//     summaryAdapter shape change"; M2 Alignment Review PR #60 §3.3 "no
//     frontend-heavy logic"; Plan §18.2 stop condition for missing fixture
//     fields). These are a future fixture-refresh concern, not an M2
//     concern.
//   - hasConflict is derived by intersecting each vessel's id against the
//     vesselIds array on every conflict (small N; no indexing required).
//   - The conflicts list is filtered to signalType in { 'CONFLICT', 'WARNING' }
//     so the VTS pane shows operationally relevant items only. ADVISORY and
//     WEATHER cards remain on the Dashboard right rail unchanged.
//   - Pure; deterministic; no Date.now(); no I/O.

const OPERATIONAL_SIGNALS = new Set(['CONFLICT', 'WARNING']);

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

export function adaptVts(viewSummary) {
  if (!viewSummary || typeof viewSummary !== 'object') {
    return { vessels: [], conflicts: [] };
  }

  const rawVessels   = safeArray(viewSummary.vessels);
  const rawConflicts = safeArray(viewSummary.conflicts);

  // Build vesselId → [conflictId, ...] map from the conflicts list.
  const conflictsByVesselId = new Map();
  for (const c of rawConflicts) {
    if (!c || typeof c !== 'object') continue;
    const cid = c.conflictId;
    if (typeof cid !== 'string' || !cid) continue;
    for (const vid of safeArray(c.vesselIds)) {
      if (typeof vid !== 'string' || !vid) continue;
      const existing = conflictsByVesselId.get(vid);
      if (existing) {
        existing.push(cid);
      } else {
        conflictsByVesselId.set(vid, [cid]);
      }
    }
  }

  const vessels = rawVessels.map((v) => {
    if (!v || typeof v !== 'object') return null;
    const vesselId = typeof v.vesselId === 'string' ? v.vesselId : null;
    const conflictIds = vesselId ? (conflictsByVesselId.get(vesselId) || []) : [];
    return {
      vesselId,
      name: typeof v.name === 'string' ? v.name : null,
      type: typeof v.type === 'string' ? v.type : null,
      status: typeof v.status === 'string' ? v.status : null,
      berth: typeof v.berth === 'string' ? v.berth : null,
      loaM: typeof v.loaM === 'number' && isFinite(v.loaM) ? v.loaM : null,
      riskLevel: typeof v.riskLevel === 'string' ? v.riskLevel : 'low',
      lat: typeof v.lat === 'number' && isFinite(v.lat) ? v.lat : null,
      lon: typeof v.lon === 'number' && isFinite(v.lon) ? v.lon : null,
      hasConflict: conflictIds.length > 0,
      conflictIds,
    };
  }).filter(Boolean);

  const conflicts = rawConflicts.map((c) => {
    if (!c || typeof c !== 'object') return null;
    return {
      conflictId: typeof c.conflictId === 'string' ? c.conflictId : null,
      type: typeof c.type === 'string' ? c.type : null,
      severity: typeof c.severity === 'string' ? c.severity : 'INFO',
      signalType: typeof c.signalType === 'string' ? c.signalType : null,
      title: typeof c.title === 'string' ? c.title : null,
      vesselIds: safeArray(c.vesselIds),
      vesselNames: safeArray(c.vesselNames),
      berth: typeof c.berth === 'string' ? c.berth : null,
      dataSource: typeof c.dataSource === 'string' ? c.dataSource : null,
    };
  }).filter((c) => c && OPERATIONAL_SIGNALS.has(c.signalType));

  return { vessels, conflicts };
}
