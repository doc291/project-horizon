// shiftLogAdapter.js — M2 Implementation Plan §7.2 (Shift Log derivation)
//
// Pure presentation derivation from the already-adapted ViewSummary. The
// captured /api/summary fixtures do not carry an `events[]` array (see
// summaryAdapter's missingDomains entry for 'shiftLog'). This adapter
// therefore derives a Shift Log view from explicit, timestamped, already-
// present facts in ViewSummary — it does NOT invent events, and does NOT
// pretend to be an audit ledger.
//
// Sources (every event is a presentation of a fact that already exists in
// the source data — nothing is fabricated):
//   1. Vessel arrivals — for each vessel with ataIso, emit:
//        { type: 'arrival',  timestamp: ataIso }
//   2. Vessel departures — for each vessel with atdIso, emit:
//        { type: 'departure', timestamp: atdIso }
//   3. Detected conflicts — for each conflict with conflictTimeIso, emit:
//        { type: 'conflict_detected', timestamp: conflictTimeIso }
//
// NOT derived (deliberately, per design intent):
//   - Operator actions (ACK / DEFER / APPLY / OVERRIDE / ESCALATE) — there
//     is no audit ledger feeding the V1 fixture-fed pipeline, and we do
//     not synthesise operator identity or operator-action events.
//   - "Shift start" / "shift end" markers — not in the data.
//   - "Conflict resolved" / "ETA recovered" markers — not in the data.
//   - Predicted future ETA/ETD as "events" — those are scheduling, not
//     observed events.
//
// Output:
//   {
//     rows: [{
//       id, timestampIso, type, severity, vesselId, vesselName,
//       berth, description, source, dataSource,
//     }],
//     missingDomains: ['shiftLog' | 'auditLog' | ...],
//   }
//
// `rows` are sorted most-recent-first (standard log convention).
// `source` is always 'derived' so the UI can be explicit that this is an
// observational view, not an audit-ledger replay.

const TYPE_RANK = {
  conflict_detected: 0,
  arrival: 1,
  departure: 2,
};

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseIsoMs(iso) {
  if (typeof iso !== 'string' || !iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function arrivalEvent(v) {
  const tsIso = typeof v.ataIso === 'string' && v.ataIso ? v.ataIso : null;
  if (!tsIso) return null;
  return {
    id: `arr-${v.vesselId}`,
    timestampIso: tsIso,
    type: 'arrival',
    severity: 'INFO',
    vesselId: typeof v.vesselId === 'string' ? v.vesselId : null,
    vesselName: typeof v.name === 'string' ? v.name : null,
    berth: typeof v.berth === 'string' ? v.berth : null,
    description: `${v.name || v.vesselId || 'Vessel'} arrived${v.berth ? ` at ${v.berth}` : ''}.`,
    source: 'derived',
    dataSource: 'simulated',
  };
}

function departureEvent(v) {
  const tsIso = typeof v.atdIso === 'string' && v.atdIso ? v.atdIso : null;
  if (!tsIso) return null;
  return {
    id: `dep-${v.vesselId}`,
    timestampIso: tsIso,
    type: 'departure',
    severity: 'INFO',
    vesselId: typeof v.vesselId === 'string' ? v.vesselId : null,
    vesselName: typeof v.name === 'string' ? v.name : null,
    berth: typeof v.berth === 'string' ? v.berth : null,
    description: `${v.name || v.vesselId || 'Vessel'} departed${v.berth ? ` ${v.berth}` : ''}.`,
    source: 'derived',
    dataSource: 'simulated',
  };
}

function conflictEvent(c) {
  const tsIso = typeof c.conflictTimeIso === 'string' && c.conflictTimeIso
    ? c.conflictTimeIso
    : null;
  if (!tsIso) return null;
  const names = safeArray(c.vesselNames);
  const namesStr = names.length ? names.join(' / ') : '';
  const title = typeof c.title === 'string' ? c.title : (c.type || 'Conflict');
  return {
    id: `cnf-${c.conflictId}`,
    timestampIso: tsIso,
    type: 'conflict_detected',
    severity: typeof c.severity === 'string' ? c.severity : 'INFO',
    vesselId: names.length === 1 && safeArray(c.vesselIds)[0]
      ? safeArray(c.vesselIds)[0]
      : null,
    vesselName: namesStr || null,
    berth: typeof c.berth === 'string' ? c.berth : null,
    description: title,
    source: 'derived',
    dataSource: typeof c.dataSource === 'string' ? c.dataSource : 'simulated',
  };
}

export function adaptShiftLog(viewSummary) {
  const empty = { rows: [], missingDomains: ['shiftLog', 'auditLog'] };
  if (!viewSummary || typeof viewSummary !== 'object') return empty;

  const rawVessels   = safeArray(viewSummary.vessels);
  const rawConflicts = safeArray(viewSummary.conflicts);

  const events = [];
  for (const v of rawVessels) {
    if (!v || typeof v !== 'object') continue;
    const a = arrivalEvent(v);   if (a) events.push(a);
    const d = departureEvent(v); if (d) events.push(d);
  }
  for (const c of rawConflicts) {
    if (!c || typeof c !== 'object') continue;
    const e = conflictEvent(c);
    if (e) events.push(e);
  }

  // Sort by timestamp descending (most recent first). Stable tiebreaker
  // on the type rank, then on the id.
  events.sort((a, b) => {
    const tb = parseIsoMs(b.timestampIso) || 0;
    const ta = parseIsoMs(a.timestampIso) || 0;
    if (tb !== ta) return tb - ta;
    const ra = TYPE_RANK[a.type] ?? 99;
    const rb = TYPE_RANK[b.type] ?? 99;
    if (ra !== rb) return ra - rb;
    return a.id.localeCompare(b.id);
  });

  return {
    rows: events,
    // We continue to surface that shiftLog and auditLog are missing
    // domains — the rows here are derived presentation, not the real
    // shift log or audit ledger. The UI must say so.
    missingDomains: ['shiftLog', 'auditLog'],
  };
}
