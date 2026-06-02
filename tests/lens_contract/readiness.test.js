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
             _prsAssessNavigation, _prsAssessService, _prsAssessBerth, _prsCompose };
  `;
  return new Function(src)();
}
const ctx = compile();
const S = ctx.PRS;

// ── Tiny assertion harness ──────────────────────────────────────────────────
let PASS = 0, FAIL = 0;
function check(id, cond, detail) {
  if (cond) { PASS++; console.log(`  [PASS] ${id} — ${detail}`); }
  else      { FAIL++; console.log(`  [FAIL] ${id} — ${detail}`); }
}

// ── Fixture builders (synthetic /api/summary slices) ────────────────────────
const FUTURE = '2099-01-01T06:00:00Z';   // always future; absolute value irrelevant to logic
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

// 3. Berth conflict → Berth Readiness AT RISK or NOT READY.
{
  const v = vessel({ id: 'V7', name: 'OVERLAP VESSEL' });
  const critical = ctx._prsAssessBerth(v, baseSummary({
    conflicts: [{ conflict_type: 'berth_overlap', severity: 'critical', vessel_ids: ['V7'],
                  description: 'Berth occupied on arrival.', data_source: 'simulated' }]
  }));
  const high = ctx._prsAssessBerth(v, baseSummary({
    conflicts: [{ conflict_type: 'berth_overlap', severity: 'high', vessel_ids: ['V7'],
                  description: 'Overlap within clearance.', data_source: 'simulated' }]
  }));
  check('PRS-3', critical.state === S.NOT_READY && high.state === S.AT_RISK,
    `critical overlap → NOT READY (${critical.state}); high overlap → AT RISK (${high.state})`);
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

// 6. Composite follows the conservative rule.
{
  const R = { state: S.READY }, A = { state: S.AT_RISK }, N = { state: S.NOT_READY };
  const Ustruct = { state: S.UNCERTAIN, kind: 'structural_terminal' };
  const Ufeed = { state: S.UNCERTAIN, kind: 'missing_feed' };
  // any NOT READY wins
  const c1 = ctx._prsCompose(R, R, N).state === S.NOT_READY;
  // any AT RISK (no NOT READY) wins
  const c2 = ctx._prsCompose(R, A, Ustruct).state === S.AT_RISK;
  // service missing_feed uncertainty (not terminal) → AT RISK, not the exception
  const c3 = ctx._prsCompose(R, Ufeed, R).state === S.AT_RISK;
  // exception: only uncertainty is structural terminal berth, nav+svc READY → READY+qualifier
  const ex = ctx._prsCompose(R, R, Ustruct);
  const c4 = ex.state === S.READY && /unconfirmed/i.test(ex.qualifier || '');
  // all READY → READY
  const c5 = ctx._prsCompose(R, R, R).state === S.READY;
  check('PRS-6', c1 && c2 && c3 && c4 && c5,
    `NOT READY wins=${c1}; AT RISK wins=${c2}; missing_feed→AT RISK=${c3}; structural-terminal exception→READY+qualifier=${c4}; all READY→READY=${c5}`);
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

// ── Report ──────────────────────────────────────────────────────────────────
console.log(`\n${'='.repeat(60)}`);
console.log(`Port Readiness Phase 1: ${PASS} pass / ${FAIL} fail`);
console.log('='.repeat(60));
process.exit(FAIL === 0 ? 0 : 1);
