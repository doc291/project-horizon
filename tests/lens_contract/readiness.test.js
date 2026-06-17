// Port Readiness Scorecard — Phase 1 acceptance harness.
//
// Verifies the deterministic, conservative, honest readiness scorecard built
// from /api/summary data already available in Beta 10 / Beta 12. It string-
// extracts the PURE assessment block from index.html (between PRS-PURE-BEGIN
// and PRS-PURE-END) and exercises it against synthetic summaries.
//
// Proves (per the Phase-1 brief):
//   1. Missing terminal data creates visible uncertainty / qualifier.
//   2. Simulated towage/pilotage data is labelled and never shown as confirmed
//      live readiness.
//   3. A berth conflict drives Berth Readiness to AT RISK or NOT READY.
//   4. UKC/tide risk drives Navigation Readiness to AT RISK or NOT READY.
//   5. Missing AIS / no ETA degrades Navigation Readiness to UNCERTAIN.
//   6. Composite state follows the conservative rule (incl. the structural
//      terminal exception).
//
// TEST-ONLY. Reads index.html read-only; never modifies any runtime file.
// Exits non-zero on failure so run_all.js gates on it.
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..', '..');
const html = fs.readFileSync(path.join(ROOT, 'index.html'), 'utf-8');

// ── Extract the PURE readiness block and compile it standalone ──────────────
function extractPure() {
  const b = html.indexOf('// PRS-PURE-BEGIN');
  const e = html.indexOf('// PRS-PURE-END');
  if (b < 0 || e < 0) throw new Error('PRS-PURE markers not found in index.html');
  return html.slice(b, e);
}
function compile() {
  const src = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    ${extractPure()}
    return { PRS, buildVesselReadiness, renderReadinessCardHTML, prsRelevantVessels,
             _prsAssessNavigation, _prsAssessService, _prsAssessBerth, _prsCompose,
             prsPictureVessels, prsSortByUrgency, prsSummaryCounts, prsPrimaryReason };
  `;
  return new Function(src)();
}
const ctx = compile();
const S = ctx.PRS;

// ── Phase A — compile PRS-PURE + LENS-RDX helpers + both lens blocks together,
//    exactly as they coexist in the IIFE, to test lens readiness consumption
//    and the Tab↔lens divergence guarantee end-to-end. ─────────────────────────
function slice(startMark, endMark) {
  const b = html.indexOf(startMark), e = html.indexOf(endMark, b + 1);
  if (b < 0 || e < 0) throw new Error(`markers not found: ${startMark} … ${endMark}`);
  return html.slice(b, e);
}
function compileLens() {
  const PURE = extractPure();
  const LENSRDX = slice('// LENS-RDX-BEGIN', '// LENS-RDX-END');
  const TOW_DATA = slice('  let _prevConflictIds = null;',
    '  // ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer');
  const PIL = slice('// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer',
    '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.');
  const TOW_REND = slice('  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.',
    '  function onData(d){');
  // S1.8 — the Pilotage/Towage lens readiness block now surfaces the canonical
  // Rationale, so the lens test context must include the signal/Rationale block
  // (proven disjoint from PURE in compileSignal).
  const RDX = slice('function _prsTopStateClass(st){', '// RDX-SIGNAL-END');
  const src = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    function fmtTsRel(ts){ return fmtTs(ts); }
    function fmtTimeRel(iso){ try{ return new Date(iso).toISOString().slice(11,16)+" UTC"; }catch(e){ return String(iso); } }
    ${PURE}
    ${RDX}
    ${TOW_DATA}
    ${PIL}
    ${TOW_REND}
    ${LENSRDX}
    return { buildVesselReadiness, _lensReadinessMap, _lensReadinessBlock,
             buildTowageShift, renderTowageShift, buildPilotageWatch, renderPilotageWatch };
  `;
  return new Function(src)();
}
const lens = compileLens();

// ── Compile PRS-PURE engine + the top-level canonical Signal Object builders
//    (Readiness Tab integration), to test mapping/render and divergence. ───────
function compileSignal() {
  const PURE = extractPure();
  const RDX = slice('function _prsTopStateClass(st){', '// RDX-SIGNAL-END');
  const src = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTimeRel(iso){ try{ return new Date(iso).toISOString().slice(11,16)+" UTC"; }catch(e){ return String(iso); } }
    ${PURE}
    ${RDX}
    return { buildVesselReadiness, _prsCapabilityNote, _rdxToSignal,
             _rdxSignalObjectHTML, _rdxPostureHTML, _rdxStateSlug };
  `;
  return new Function(src)();
}
const sigc = compileSignal();

// ── Tiny assertion harness ──────────────────────────────────────────────────
let PASS = 0, FAIL = 0;
function check(id, cond, detail) {
  if (cond) { PASS++; console.log(`  [PASS] ${id} — ${detail}`); }
  else      { FAIL++; console.log(`  [FAIL] ${id} — ${detail}`); }
}

// ── Fixture builders (synthetic /api/summary slices) ────────────────────────
const FUTURE = '2099-01-01T06:00:00Z';   // always future; absolute value irrelevant to logic
// ETA relative to NOW, for the operational-relevance window (calibration).
function inHours(h){ return new Date(Date.now() + h*3600000).toISOString(); }
function baseSummary(over) {
  return Object.assign({
    beta11: { enabled: true },
    port_profile: { using_live_vessel_data: true },
    weather: { conditions: 'Good', source: 'live' },
    arrival_ukc: { status: 'good', critical_vessel: null, all: [] },
    conflicts: [],
    berths: [{ id: 'B01', name: 'Webb Dock 1 East' }],
    vessels: []
  }, over || {});
}
function vessel(over) {
  return Object.assign({
    id: 'V1', name: 'TEST VESSEL', source: 'ais', status: 'confirmed',
    berth_id: 'B01', eta: FUTURE, draught: 11.0,
    pilotage_required: false, towage_required: false
  }, over || {});
}

console.log('=== Port Readiness Scorecard — Phase 1 acceptance ===\n');

// 1. Missing terminal data → berth UNCERTAIN(structural) + visible qualifier;
//    composite READY-with-qualifier when nav+service READY.
{
  const v = vessel({ pilotage_required: false, towage_required: false });
  const card = ctx.buildVesselReadiness(v, baseSummary({ vessels: [v] }));
  const berthUncertain = card.berth.state === S.UNCERTAIN && card.berth.kind === 'structural_terminal';
  const reasonVisible = /schedule|terminal/i.test(JSON.stringify(card.berth));
  const qualifier = card.composite.state === S.READY && /unconfirmed/i.test(card.composite.qualifier || '');
  check('PRS-1', berthUncertain && reasonVisible && qualifier,
    `berth UNCERTAIN(structural)=${berthUncertain}, schedule/terminal reason visible=${reasonVisible}, composite READY+qualifier=${qualifier}`);
}

// 2. Simulated pilotage/towage labelled, never confirmed-live readiness.
{
  const v = vessel({ pilotage_required: true, towage_required: true });
  const svc = ctx._prsAssessService(v, baseSummary({ vessels: [v] }));
  const pil = svc.subs.find(s => s.key === 'pilotage');
  const tow = svc.subs.find(s => s.key === 'towage');
  const labelled = pil.simulated === true && tow.simulated === true
    && /simulated/i.test(pil.provenance) && /simulated/i.test(tow.provenance);
  const notLiveReady = pil.state !== S.READY && tow.state !== S.READY && svc.state === S.UNCERTAIN;
  check('PRS-2', labelled && notLiveReady,
    `pilotage+towage labelled simulated=${labelled}, service UNCERTAIN not READY=${notLiveReady}`);
}

// 3. Berth conflict WITHIN 12h → Berth Readiness NOT READY (blocker) / AT RISK.
{
  const v = vessel({ id: 'V7', name: 'OVERLAP VESSEL', eta: inHours(6) });
  const critical = ctx._prsAssessBerth(v, baseSummary({
    conflicts: [{ conflict_type: 'berth_overlap', severity: 'critical', vessel_ids: ['V7'],
                  description: 'Berth occupied on arrival.', data_source: 'simulated' }]
  }));
  const high = ctx._prsAssessBerth(v, baseSummary({
    conflicts: [{ conflict_type: 'berth_overlap', severity: 'high', vessel_ids: ['V7'],
                  description: 'Overlap within clearance.', data_source: 'simulated' }]
  }));
  check('PRS-3', critical.state === S.NOT_READY && high.state === S.AT_RISK,
    `<=12h critical overlap → NOT READY (${critical.state}); high overlap → AT RISK (${high.state})`);
}

// 4. UKC/tide risk → Navigation Readiness AT RISK or NOT READY.
{
  const v = vessel({ id: 'V3', name: 'TIDE RUNNER' });
  const neg = ctx._prsAssessNavigation(v, baseSummary({
    arrival_ukc: { status: 'critical', critical_vessel: 'TIDE RUNNER',
                   all: [{ vessel_id: 'V3', ukc_m: -0.2 }] }
  }));
  const tight = ctx._prsAssessNavigation(v, baseSummary({
    arrival_ukc: { status: 'warning', critical_vessel: 'TIDE RUNNER',
                   all: [{ vessel_id: 'V3', ukc_m: 0.4 }] }
  }));
  check('PRS-4', neg.state === S.NOT_READY && tight.state === S.AT_RISK,
    `negative UKC → NOT READY (${neg.state}); tight UKC → AT RISK (${tight.state})`);
}

// 5. Missing AIS / no ETA → Navigation UNCERTAIN.
{
  const v = vessel({ source: 'sim', eta: null });
  const nav = ctx._prsAssessNavigation(v, baseSummary({ port_profile: { using_live_vessel_data: false } }));
  check('PRS-5', nav.state === S.UNCERTAIN && nav.kind === 'missing_eta',
    `no ETA + simulated source → navigation UNCERTAIN(missing_eta) (${nav.state}/${nav.kind})`);
}

// 6. Composite follows the CALIBRATED conservative rule.
//    (Updated per the calibration sprint: structural absence no longer forces
//    AT RISK; degrading uncertainty — stale / missing-ETA — does.)
{
  const R = { state: S.READY }, A = { state: S.AT_RISK }, N = { state: S.NOT_READY };
  const Ustruct = { state: S.UNCERTAIN, kind: 'structural_terminal' };
  const Ufeed   = { state: S.UNCERTAIN, kind: 'missing_feed' };
  const Ustale  = { state: S.UNCERTAIN, kind: 'stale' };
  const Ueta    = { state: S.UNCERTAIN, kind: 'missing_eta' };
  // any NOT READY wins
  const c1 = ctx._prsCompose(R, R, N).state === S.NOT_READY;
  // any AT RISK (no NOT READY) wins
  const c2 = ctx._prsCompose(R, A, Ustruct).state === S.AT_RISK;
  // structural service-feed absence + all assessable READY → READY + qualifier
  const sx = ctx._prsCompose(R, Ufeed, R);
  const c3 = sx.state === S.READY && /service readiness unconfirmed/i.test(sx.qualifier || '');
  // structural terminal berth + nav+svc READY → READY + qualifier
  const ex = ctx._prsCompose(R, R, Ustruct);
  const c4 = ex.state === S.READY && /berth readiness unconfirmed/i.test(ex.qualifier || '');
  // all READY → READY
  const c5 = ctx._prsCompose(R, R, R).state === S.READY;
  // DEGRADING uncertainty: stale connected data → AT RISK; missing ETA → AT RISK
  const c6 = ctx._prsCompose(R, Ustale, R).state === S.AT_RISK;
  const c7 = ctx._prsCompose(Ueta, R, R).state === S.AT_RISK;
  check('PRS-6', c1 && c2 && c3 && c4 && c5 && c6 && c7,
    `NOT READY wins=${c1}; AT RISK wins=${c2}; structural-feed→READY+qual=${c3}; structural-terminal→READY+qual=${c4}; all READY→READY=${c5}; stale→AT RISK=${c6}; missing-ETA→AT RISK=${c7}`);
}

// 7. Renderer never emits clearance/authorisation language; always advisory.
{
  const v = vessel({ pilotage_required: true, towage_required: true });
  const out = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(v, baseSummary({ vessels: [v] })));
  const banned = /\b(cleared|authoris|authoriz|approved|proceed)\b/i.test(out);
  const advisory = /advisory only/i.test(out) && /not a clearance/i.test(out);
  check('PRS-7', !banned && advisory,
    `no clearance/authorise/approve/proceed wording=${!banned}; advisory disclaimer present=${advisory}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1B — Readiness tab (forward picture, summary counts, sort, detail, labels)
// ─────────────────────────────────────────────────────────────────────────────

// 8. Forward picture selection: approaching + berthed-with-ETD + conflict-flagged;
//    excludes berthed-without-departure.
{
  const approaching = vessel({ id:'A1', name:'APPROACHER', status:'confirmed', berth_id:'B01', eta:FUTURE });
  const departing   = vessel({ id:'D1', name:'DEPARTER',   status:'berthed',   berth_id:'B01', eta:null, etd:FUTURE });
  const flagged     = vessel({ id:'C1', name:'CONFLICTED', status:'berthed',   berth_id:'B01', eta:null, etd:null });
  const excluded    = vessel({ id:'X1', name:'STATIONARY', status:'berthed',   berth_id:'B01', eta:null, etd:null });
  const sum = baseSummary({
    vessels:[approaching, departing, flagged, excluded],
    conflicts:[{ conflict_type:'berth_overlap', severity:'high', vessel_ids:['C1'], description:'overlap', data_source:'simulated' }]
  });
  const ids = ctx.prsPictureVessels(sum).map(v => v.id);
  check('PRS-8', ids.indexOf('A1')>=0 && ids.indexOf('D1')>=0 && ids.indexOf('C1')>=0 && ids.indexOf('X1')<0,
    `picture includes approaching+departing+flagged, excludes idle-berthed (${ids.join(',')})`);
}

// 9. Summary counts are correct.
{
  const cards = [
    { composite:{ state:S.READY } }, { composite:{ state:S.READY } },
    { composite:{ state:S.AT_RISK } }, { composite:{ state:S.NOT_READY } },
    { composite:{ state:S.UNCERTAIN } }
  ];
  const c = ctx.prsSummaryCounts(cards);
  check('PRS-9', c.total===5 && c['READY']===2 && c['AT RISK']===1 && c['NOT READY']===1 && c['UNCERTAIN']===1,
    `counts total=${c.total} R=${c['READY']} A=${c['AT RISK']} N=${c['NOT READY']} U=${c['UNCERTAIN']}`);
}

// 10. Sort by urgency: NOT READY > AT RISK > UNCERTAIN > READY.
{
  const mk = (st,eta) => ({ composite:{ state:st }, vessel:{ eta:eta } });
  const sorted = ctx.prsSortByUrgency([
    mk(S.READY,'2099-01-01T01:00:00Z'),
    mk(S.UNCERTAIN,'2099-01-01T01:00:00Z'),
    mk(S.NOT_READY,'2099-01-01T01:00:00Z'),
    mk(S.AT_RISK,'2099-01-01T01:00:00Z')
  ]).map(x => x.composite.state);
  check('PRS-10', JSON.stringify(sorted)===JSON.stringify([S.NOT_READY,S.AT_RISK,S.UNCERTAIN,S.READY]),
    `urgency order = ${sorted.join(' > ')}`);
}

// 11. Primary reason reflects the worst (composite) component.
{
  const v = vessel({ id:'V3', name:'TIDE RUNNER' });
  const card = ctx.buildVesselReadiness(v, baseSummary({
    arrival_ukc:{ status:'critical', critical_vessel:'TIDE RUNNER', all:[{vessel_id:'V3', ukc_m:-0.3}] }, vessels:[v]
  }));
  const reason = ctx.prsPrimaryReason(card);
  check('PRS-11', card.composite.state===S.NOT_READY && /UKC/i.test(reason),
    `NOT READY primary reason surfaces UKC cause: "${reason}"`);
}

// 12. Expanding a vessel reveals the three components; simulated/assumed/unavailable
//     labels remain visible in the detail card.
{
  const v = vessel({ id:'V9', name:'FULL SERVICE', pilotage_required:true, towage_required:true });
  const out = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] })));
  const comps = /Navigation Readiness/.test(out) && /Service Readiness/.test(out) && /Berth Readiness/.test(out);
  const labels = /simulated/i.test(out) && /(unconfirmed|schedule)/i.test(out) && /not available/i.test(out);
  check('PRS-12', comps && labels,
    `detail shows 3 components=${comps}; simulated/unconfirmed/unavailable labels visible=${labels}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Calibration Sprint — operational credibility
// ─────────────────────────────────────────────────────────────────────────────

// 13. Structurally-absent service (and terminal) data does NOT force composite
//     AT RISK when the assessable component (Navigation) is READY.
{
  const v = vessel({ id:'RT1', name:'ROUTINE', eta:inHours(6), pilotage_required:true, towage_required:true });
  const card = ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] }));
  const ok = card.composite.state === S.READY
    && /service readiness unconfirmed/i.test(card.composite.qualifier||'')
    && /berth readiness unconfirmed/i.test(card.composite.qualifier||'')
    && card.service.state === S.UNCERTAIN && card.berth.state === S.UNCERTAIN;
  check('PRS-13', ok,
    `routine vessel (services simulated, no terminal feed) → composite ${card.composite.state} + qualifier "${card.composite.qualifier}"; service/berth still UNCERTAIN`);
}

// 14. Berth conflict in the 12–24h forward window → AT RISK (not NOT READY).
{
  const v = vessel({ id:'V7', name:'FWD VESSEL', eta:inHours(18) });
  const berth = ctx._prsAssessBerth(v, baseSummary({
    conflicts:[{ conflict_type:'berth_overlap', severity:'critical', vessel_ids:['V7'], description:'overlap', data_source:'simulated' }]
  }));
  check('PRS-14', berth.state === S.AT_RISK, `12–24h berth conflict → AT RISK (${berth.state})`);
}

// 15. Berth conflict beyond 24h → does NOT drive NOT READY (informational only).
{
  const v = vessel({ id:'V7', name:'FARFUTURE', eta:inHours(30) });
  const sum = baseSummary({
    vessels:[v],
    conflicts:[{ conflict_type:'berth_overlap', severity:'critical', vessel_ids:['V7'], description:'overlap', data_source:'simulated' }]
  });
  const berth = ctx._prsAssessBerth(v, sum);
  const card = ctx.buildVesselReadiness(v, sum);
  check('PRS-15', berth.state !== S.NOT_READY && card.composite.state !== S.NOT_READY,
    `>24h berth conflict → berth ${berth.state} (not NOT READY), composite ${card.composite.state} (not NOT READY)`);
}

// 16. Bridge restriction beyond 24h → informational only (Navigation not elevated).
{
  const v = vessel({ id:'BR1', name:'BRIDGE FAR', eta:inHours(30) });
  const nav = ctx._prsAssessNavigation(v, baseSummary({
    conflicts:[{ conflict_type:'bridge_restriction', severity:'critical', vessel_ids:['BR1'], description:'Bolte air draught', data_source:'simulated' }]
  }));
  const noBridgeSub = !(nav.subs||[]).some(s => s.key === 'bridge');
  const noted = (nav.notes||[]).some(n => /bridge/i.test(n));
  check('PRS-16', nav.state === S.READY && noBridgeSub && noted,
    `>24h bridge → nav ${nav.state}, no bridge sub=${noBridgeSub}, informational note=${noted}`);
}

// 17. Bridge restriction within 24h → AT RISK, never NOT READY.
{
  const v = vessel({ id:'BR2', name:'BRIDGE SOON', eta:inHours(6) });
  const nav = ctx._prsAssessNavigation(v, baseSummary({
    conflicts:[{ conflict_type:'bridge_restriction', severity:'critical', vessel_ids:['BR2'], description:'Bolte air draught', data_source:'simulated' }]
  }));
  const bridgeSub = (nav.subs||[]).find(s => s.key === 'bridge');
  check('PRS-17', nav.state === S.AT_RISK && bridgeSub && bridgeSub.state === S.AT_RISK,
    `<=24h bridge → nav ${nav.state}, bridge sub ${bridgeSub && bridgeSub.state} (never NOT READY)`);
}

// 18. UKC physically insufficient still drives composite NOT READY.
{
  const v = vessel({ id:'V3', name:'TIDE RUNNER', eta:inHours(6) });
  const card = ctx.buildVesselReadiness(v, baseSummary({
    vessels:[v],
    arrival_ukc:{ status:'critical', critical_vessel:'TIDE RUNNER', all:[{ vessel_id:'V3', ukc_m:-0.3 }] }
  }));
  check('PRS-18', card.navigation.state === S.NOT_READY && card.composite.state === S.NOT_READY,
    `negative UKC → navigation ${card.navigation.state}, composite ${card.composite.state}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Stakeholder Contribution Sprint — capability notes (expanded scorecard only)
// ─────────────────────────────────────────────────────────────────────────────
const SVC_NOTE = /whether tug capacity covers this vessel/i;
const BERTH_NOTE = /terminal completion forecasts/i;
const NAV_NOTE = /Confidence improves with verified draft/i;

// 19. Service UNCERTAIN → service capability note in the expanded scorecard.
{
  const v = vessel({ id:'S1', name:'SERVICE V', eta:inHours(6), pilotage_required:true, towage_required:true });
  const out = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] })));
  check('PRS-19', SVC_NOTE.test(out), `service capability note present in expanded card=${SVC_NOTE.test(out)}`);
}

// 20. Berth UNCERTAIN (schedule-based) → berth capability note.
{
  const v = vessel({ id:'B1', name:'BERTH V', eta:inHours(6), pilotage_required:false, towage_required:false });
  const out = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] })));
  check('PRS-20', BERTH_NOTE.test(out), `berth capability note present in expanded card=${BERTH_NOTE.test(out)}`);
}

// 21. Navigation note appears only where confidence is partial/modelled.
{
  // partial: MST source (draft modelled)
  const vp = vessel({ id:'N1', name:'MST V', source:'mst', eta:inHours(6), pilotage_required:false, towage_required:false });
  const partialOut = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(vp, baseSummary({ vessels:[vp] })));
  // clean: live AIS + live weather
  const vc = vessel({ id:'N2', name:'AIS V', source:'ais', eta:inHours(6), pilotage_required:false, towage_required:false });
  const cleanOut = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(vc, baseSummary({ vessels:[vc], weather:{conditions:'Good',source:'live'} })));
  check('PRS-21', NAV_NOTE.test(partialOut) && !NAV_NOTE.test(cleanOut),
    `nav note on partial(MST)=${NAV_NOTE.test(partialOut)}; absent on clean live AIS=${!NAV_NOTE.test(cleanOut)}`);
}

// 22. No blame language anywhere in the expanded scorecard.
{
  const v = vessel({ id:'S2', name:'BLAME CHK', eta:inHours(6), pilotage_required:true, towage_required:true });
  const out = ctx.renderReadinessCardHTML(ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] })));
  const blame = /(missing\s+provider|not\s+connected|failed\s+to\s+share|has\s+not\s+shared|provider\s+has\s+not)/i.test(out);
  check('PRS-22', !blame, `no blame language in expanded card=${!blame}`);
}

// 23. Capability notes are EXPANDED-only — present in the scorecard, absent from
//     the collapsed-row reason source (prsPrimaryReason).
{
  const v = vessel({ id:'S3', name:'SCOPE CHK', eta:inHours(6), pilotage_required:true, towage_required:true });
  const card = ctx.buildVesselReadiness(v, baseSummary({ vessels:[v] }));
  const expanded = ctx.renderReadinessCardHTML(card);
  const rowReason = ctx.prsPrimaryReason(card);
  check('PRS-23', SVC_NOTE.test(expanded) && !SVC_NOTE.test(rowReason),
    `capability note in expanded=${SVC_NOTE.test(expanded)}; absent from collapsed-row reason=${!SVC_NOTE.test(rowReason)}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase A — Readiness integration into Towage & Pilotage lenses
// ─────────────────────────────────────────────────────────────────────────────
// One summary feeding both lenses + the Tab. Three vessels with distinct states:
//   ALPHA  — routine (services simulated, no terminal feed) → READY + qualifier
//   BRAVO  — berth_not_ready within 12h → NOT READY
//   CHARLIE— bridge_restriction within 24h → AT RISK
function phaseASummary(){
  const eta = h => new Date(Date.now() + h*3600000).toISOString();
  const sched = h => new Date(Date.now() + h*3600000).toISOString();
  const V = (id,name,o) => Object.assign({ id, name, source:'ais', status:'confirmed',
    draught:10, pilotage_required:true, towage_required:true }, o);
  return {
    beta11:{enabled:true}, port_profile:{using_live_vessel_data:true},
    weather:{conditions:'Good', source:'live'},
    arrival_ukc:{ status:'good', critical_vessel:null, all:[] },
    tides:{ next_event_type:'HW', next_event_time:eta(5) },
    berths:[{id:'B01',name:'Berth 1'},{id:'B02',name:'Berth 2',readiness_time:eta(13)},{id:'B03',name:'Berth 3'}],
    vessels:[
      V('VA','ALPHA',{ berth_id:'B01', eta:eta(6) }),
      V('VB','BRAVO',{ berth_id:'B02', eta:eta(5) }),
      V('VC','CHARLIE',{ berth_id:'B03', eta:eta(6) })
    ],
    conflicts:[
      { conflict_type:'berth_not_ready', severity:'high', vessel_ids:['VB'], description:'Berth 2 not ready before arrival.', data_source:'simulated' },
      { conflict_type:'bridge_restriction', severity:'high', vessel_ids:['VC'], description:'Bolte air draught', data_source:'simulated' }
    ],
    pilotage:[
      { id:'PIL-VA', vessel_id:'VA', vessel_name:'ALPHA',  scheduled_time:sched(5), direction:'inbound', status:'confirmed', boarding_station:'PBG' },
      { id:'PIL-VB', vessel_id:'VB', vessel_name:'BRAVO',  scheduled_time:sched(4), direction:'inbound', status:'confirmed', boarding_station:'PBG' },
      { id:'PIL-VC', vessel_id:'VC', vessel_name:'CHARLIE',scheduled_time:sched(5), direction:'inbound', status:'confirmed', boarding_station:'PBG' }
    ],
    towage:[
      { id:'TOW-VA', vessel_id:'VA', vessel_name:'ALPHA',  scheduled_time:sched(5), direction:'arrival', status:'confirmed', tugs_assigned:[] },
      { id:'TOW-VB', vessel_id:'VB', vessel_name:'BRAVO',  scheduled_time:sched(4), direction:'arrival', status:'confirmed', tugs_assigned:[] },
      { id:'TOW-VC', vessel_id:'VC', vessel_name:'CHARLIE',scheduled_time:sched(5), direction:'arrival', status:'confirmed', tugs_assigned:[] }
    ],
    port_tugs:[{name:'SVR Apex',bollard_pull_t:70}]
  };
}
{
  const d = phaseASummary();
  const rdx = lens._lensReadinessMap(d);
  const towHTML = lens.renderTowageShift(lens.buildTowageShift(d), rdx);
  const pilHTML = lens.renderPilotageWatch(lens.buildPilotageWatch(d), rdx);
  const tabState = id => lens.buildVesselReadiness(d.vessels.find(v=>v.id===id), d).composite.state;

  // 24. Towage jobs display the canonical readiness state (badge in lens-rdx block).
  const towHasBadge = /class="lens-rdx"/.test(towHTML) && /Port Readiness/.test(towHTML);
  check('PRS-24', towHasBadge, `towage jobs render canonical readiness block=${towHasBadge}`);

  // 25. Pilotage transits display the canonical readiness state.
  const pilHasBadge = /class="lens-rdx"/.test(pilHTML) && /Port Readiness/.test(pilHTML);
  check('PRS-25', pilHasBadge, `pilotage transits render canonical readiness block=${pilHasBadge}`);

  // 26. Component strips render (Nav/Service/Berth via shared Tab classes).
  const strip = html => /class="rdx-strip"/.test(html) && /Navigation/.test(html) && /Service/.test(html) && /Berth/.test(html);
  check('PRS-26', strip(towHTML) && strip(pilHTML), `component strips render in both lenses (tow=${strip(towHTML)}, pil=${strip(pilHTML)})`);

  // 27. Readiness summary calculates correctly (counts per item == tally).
  const sumOk = /Readiness:\s*1 NOT READY · 1 AT RISK · 1 READY/.test(towHTML) &&
                /Readiness:\s*1 NOT READY · 1 AT RISK · 1 READY/.test(pilHTML);
  check('PRS-27', sumOk, `headline readiness summary correct in both lenses=${sumOk}`);

  // 28. Capability notes appear in the lens where a component is UNCERTAIN.
  const cap = /With pilotage and towage data/.test(towHTML) && /terminal completion forecasts/.test(towHTML);
  check('PRS-28', cap, `capability notes present inside lens readiness blocks=${cap}`);

  // 29. DIVERGENCE GUARANTEE — for every vessel, the readiness block the lens
  //     renders is byte-identical to the canonical card, and equals the Tab state.
  let diverged = [];
  ['VA','VB','VC'].forEach(id => {
    const canonical = lens._lensReadinessBlock(rdx[id]);
    const inTow = towHTML.includes(canonical);
    const inPil = pilHTML.includes(canonical);
    const stateMatches = rdx[id].composite.state === tabState(id);
    if(!(inTow && inPil && stateMatches)) diverged.push(`${id}(tow=${inTow},pil=${inPil},state=${stateMatches})`);
  });
  check('PRS-29', diverged.length === 0,
    `Tab/Towage/Pilotage readiness identical for every vessel${diverged.length?': DIVERGENCE '+diverged.join(','):''}`);

  // 30. Existing lens behaviour intact — with NO readiness map, the lenses render
  //     exactly as before (no readiness block, no summary line).
  const towPlain = lens.renderTowageShift(lens.buildTowageShift(d));
  const pilPlain = lens.renderPilotageWatch(lens.buildPilotageWatch(d));
  const additiveOnly = !/lens-rdx/.test(towPlain) && !/lens-rdx/.test(pilPlain)
    && !/Readiness:/.test(towPlain) && !/Readiness:/.test(pilPlain);
  check('PRS-30', additiveOnly, `readiness is additive-only (absent when no map supplied)=${additiveOnly}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Canonical Signal Object integration (Readiness Tab)
// ─────────────────────────────────────────────────────────────────────────────
{
  const d = phaseASummary();   // ALPHA→READY, BRAVO→NOT READY, CHARLIE→AT RISK
  const note = sigc._prsCapabilityNote;
  const cardOf = id => sigc.buildVesselReadiness(d.vessels.find(v=>v.id===id), d);
  const sigOf  = id => sigc._rdxToSignal(cardOf(id), d, note);
  const slug = st => st==='READY'?'ready':st==='AT RISK'?'at-risk':st==='NOT READY'?'not-ready':'uncertain';

  // 31. Readiness state preserved — object state == canonical engine state.
  const stateOk = ['VA','VB','VC'].every(id => sigOf(id).state === slug(cardOf(id).composite.state));
  check('PRS-31', stateOk, `signal-object composite state == engine state for every vessel=${stateOk}`);

  // 32. Component states preserved (Navigation/Service/Berth).
  const comp = sigOf('VA').components;
  const card = cardOf('VA');
  const compOk = comp[0].state===slug(card.navigation.state)
    && comp[1].state===slug(card.service.state) && comp[2].state===slug(card.berth.state);
  check('PRS-32', compOk, `object component states == engine component states=${compOk}`);

  // 33. Confidence mapping deterministic (not recomputed): structural service feed → none;
  //     schedule-based berth → available; live navigation → live.
  const s = sigOf('VA');
  const confOk = s.components[1].conf==='none' && s.components[2].conf==='available' && s.components[0].conf==='live';
  check('PRS-33', confOk, `confidence dots: Nav=${s.components[0].conf} Svc=${s.components[1].conf} Berth=${s.components[2].conf}`);

  // 34. Contribution visibility maintained — gap names contributors; capability notes survive in evidence.
  const sig = sigOf('VA');
  const html = sigc._rdxSignalObjectHTML(sig, true);  // expanded
  const gapOk = /Terminal/.test(sig.gap||'') && /Towage \+ Pilotage/.test(sig.gap||'');
  const noteOk = /With pilotage and towage data/.test(html) && /terminal completion forecasts/.test(html);
  check('PRS-34', gapOk && noteOk, `gap names contributors=${gapOk}; capability notes preserved in evidence=${noteOk}`);

  // 35. Canonical hierarchy respected: state → reason → components → evidence.
  const open = sigc._rdxSignalObjectHTML(sigOf('VB'), true);
  const iState=open.indexOf('sig-state'), iReason=open.indexOf('sig-reason'),
        iComp=open.indexOf('sig-comps'), iEvi=open.indexOf('sig-evidence');
  const hierarchyOk = iState>=0 && iState<iReason && iReason<iComp && iComp<iEvi
    && /NOT READY/.test(open);
  check('PRS-35', hierarchyOk, `state→reason→components→evidence order respected & state word dominant=${hierarchyOk}`);

  // 36. No divergence — object state derives only from the engine composite.
  const noDiverge = ['VA','VB','VC'].every(id => {
    const objWord = sigc._rdxStateSlug(cardOf(id).composite.state);
    return sigOf(id).state === objWord;
  });
  check('PRS-36', noDiverge, `object never diverges from engine composite=${noDiverge}`);

  // 37. Posture object: counts + dominant phrasing (derived from same Signals).
  const cards = ['VB','VC','VA'].map(cardOf);
  const pst = sigc._rdxPostureHTML(cards);
  const postureOk = /pst-bar/.test(pst) && /Not ready/.test(pst) && /At risk/.test(pst) && /Ready/.test(pst)
    && /not ready/.test(pst) && /Berth availability/.test(pst);
  check('PRS-37', postureOk, `posture renders proportional bar + dominant phrase/constraint=${postureOk}`);

  // ── Signal Density Architecture ────────────────────────────────────────────
  const compact = sigc._rdxSignalObjectHTML(sigOf('VA'), false);  // default tier
  const full    = sigc._rdxSignalObjectHTML(sigOf('VA'), true);   // expanded tier

  // 38. Compact density + GREEN SUPPRESSION (S1.4): a non-READY signal carries the
  //     full field set (reason, strip, summary dot); a READY signal collapses to
  //     state+object only; time-to-ready shown where available.
  const cIsCompact = /class="sig sig-compact/.test(compact);
  const compactVB = sigc._rdxSignalObjectHTML(sigOf('VB'), false);  // NOT READY
  const nrFields = /sigc-state/.test(compactVB) && /sigc-vessel/.test(compactVB) && /sigc-berth/.test(compactVB)
    && /sigc-reason/.test(compactVB) && /sigc-strip/.test(compactVB) && /sig-sumconf/.test(compactVB);
  const greenCollapsed = !/sigc-reason/.test(compact) && !/sigc-strip/.test(compact) && !/sig-sumconf/.test(compact);
  const changeWhenAvail = /sigc-change/.test(compactVB);
  check('PRS-38', cIsCompact && nrFields && greenCollapsed && changeWhenAvail,
    `non-green field set=${nrFields}; green collapsed=${greenCollapsed}; time-to-ready=${changeWhenAvail}`);

  // 39. Compact hides foot, evidence, capability notes, per-component confidence.
  const cHides = !/sig-foot/.test(compact) && !/sig-evidence/.test(compact)
    && !/sig-evi-note/.test(compact) && !/sig-conf /.test(compact) && !/sig-conf"/.test(compact);
  check('PRS-39', cHides, `Compact defers foot/evidence/capability/per-component confidence=${cHides}`);

  // 40. Expansion renders FULL Signal Object with all detail.
  const fFull = !/sig-compact/.test(full) && /sig-foot/.test(full) && /sig-evidence/.test(full)
    && /sig-evi-note/.test(full) && /sig-conf/.test(full);
  check('PRS-40', fFull, `expansion = Full Signal (foot + evidence + capability + per-component confidence)=${fFull}`);

  // 41. State identical Compact vs Full always; component-state parity holds for a
  //     non-green signal. A READY signal's compact omits the strip by suppression,
  //     so component parity is asserted on the NOT-READY fixture (VB).
  function _rdxWord(s){return s==='ready'?'READY':s==='at-risk'?'AT RISK':s==='not-ready'?'NOT READY':'UNCERTAIN';}
  const sgNR = sigOf('VB');
  const compactNR = compactVB;
  const fullNR    = sigc._rdxSignalObjectHTML(sgNR, true);
  const wNR=_rdxWord(sgNR.state);
  const sameState = compactNR.indexOf(wNR)>=0 && fullNR.indexOf(wNR)>=0;
  const sameComps = sgNR.components.every(c=>{ const cw=_rdxWord(c.state); return compactNR.indexOf(cw)>=0 && fullNR.indexOf(cw)>=0; });
  const greenStateParity = compact.indexOf('READY')>=0 && full.indexOf('READY')>=0;
  check('PRS-41', sameState && sameComps && greenStateParity,
    `non-green state+components identical=${sameState&&sameComps}; green state parity=${greenStateParity}`);

  // ── S1 — Rationale object + explainability contract (Slice 1) ───────────────
  // 42. Every signal carries a Rationale with the full field set incl. change_narrative stub.
  const RAT_KEYS=['state','posture','trust_gate','driver','evidence','consequence','capability_gap','change_narrative'];
  const ratOk = ['VA','VB','VC'].every(id=>{ const r=sigOf(id).rationale;
    return r && RAT_KEYS.every(k=>k in r) && r.change_narrative && ('present' in r.change_narrative)
      && r.driver && ('component' in r.driver) && Array.isArray(r.evidence); });
  check('PRS-42', ratOk, `Rationale (state/posture/trust_gate/driver/evidence/consequence/capability_gap/change_narrative) on every signal=${ratOk}`);

  // 43. No advisory-prohibited command language in any rendered signal (compact or full).
  const PROHIB=/\b(proceed|approved|cleared|confirmed by horizon)\b|you (?:must|should)/i;
  const allRender=['VA','VB','VC'].map(id=>sigc._rdxSignalObjectHTML(sigOf(id),false)+' '+sigc._rdxSignalObjectHTML(sigOf(id),true)).join(' ');
  const noProhib=!PROHIB.test(allRender);
  check('PRS-43', noProhib, `no prohibited advisory copy in rendered signals=${noProhib}`);

  // 44. Posture bounded by load-bearing provenance: directive_advisory ⇒ trust Strong.
  const postureBoundedOk = ['VA','VB','VC'].every(id=>{ const r=sigOf(id).rationale;
    return r.posture!=='directive_advisory' || r.trust_gate==='Strong'; });
  check('PRS-44', postureBoundedOk, `posture strength bounded by load-bearing trust (directive ⇒ Strong)=${postureBoundedOk}`);

  // 45. Consequence is conditional ("if conditions persist"), never operator-blame.
  const consOk = ['VB','VC'].every(id=>{ const c=sigOf(id).rationale.consequence;
    return c && /if conditions persist/i.test(c.text) && !/if you do not act|you must|you should/i.test(c.text); });
  check('PRS-45', consOk, `consequence conditional, no operator-blame voice=${consOk}`);

  // 46. Green signal has null consequence; non-green has consequence text.
  const greenCons = (sigOf('VA').rationale.consequence===null)
    && !!(sigOf('VB').rationale.consequence && sigOf('VB').rationale.consequence.text);
  check('PRS-46', greenCons, `consequence null on READY, present on non-READY=${greenCons}`);

  // 47. S1.8 — the Pilotage/Towage lens readiness block surfaces the Rationale:
  //     load-bearing trust gate + conditional consequence (advisory) on a non-green vessel.
  const vbVessel = (d.vessels||[]).find(v=>v.id==='VB');
  const lensBlock = lens._lensReadinessBlock(lens.buildVesselReadiness(vbVessel, d));
  const lensOk = /Trust:/.test(lensBlock) && /if conditions persist/i.test(lensBlock) && /What changed/.test(lensBlock);
  check('PRS-47', lensOk, `Pilotage/Towage readiness block surfaces trust gate + conditional consequence + change stub=${lensOk}`);
}

// ── Report ──────────────────────────────────────────────────────────────────
console.log(`\n${'='.repeat(60)}`);
console.log(`Port Readiness Phase 1: ${PASS} pass / ${FAIL} fail`);
console.log('='.repeat(60));
process.exit(FAIL === 0 ? 0 : 1);
