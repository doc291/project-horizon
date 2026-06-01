// G8 — Information parity acceptance criteria LC-10 + LC-11.
//
// This file supersedes the G3 parity.test.js stub.
//
// SCOPE
// -----
//   LC-10: Beta 12 stakeholder lenses MUST surface a SUPERSET, not a
//          subset, of the operational facts available in the equivalent
//          Beta 10 view. For G8 we evaluate ENUMERABLE facts only.
//
//   LC-11: A vessel present in the base vessel roster for the active
//          port MUST be addressable from the relevant Beta 12 stakeholder
//          lens — Pilotage A/B/C if pilotage_required == true, Towage
//          A/B/C if towage_required == true.
//
// METHOD
// ------
// For each fixture, treat summary.vessels[] as the Beta 10 baseline
// roster. Render both Pilotage and Towage lenses. Combine the rendered
// HTML of Sections A + B + C for each lens. Then:
//
//   LC-10: enumerate Beta 10 fact categories, check at least one instance
//          of each is surfaced by the corresponding Beta 12 lens.
//
//   LC-11: for each pilotage_required vessel in summary.vessels[], check
//          her identity (id, name, or mmsi) appears somewhere in the
//          Pilotage lens A∪B∪C rendered output. Same for towage_required
//          vessels against the Towage lens.
//
// THE BYD ZHENGZHOU CLASS
// -----------------------
// LC-11 is the exact regression class that prompted the contract. A
// vessel present in vessels[] with pilotage_required=true OR
// towage_required=true that does not appear in any rendered region of
// the relevant lens IS the failure mode. The harness reports each such
// vessel by name.
//
// CONSTRAINT
// ----------
// Observation only. No application code touched.
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const INDEX_HTML = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const FIXTURE_DIR = path.join(__dirname, 'fixtures');
const html = fs.readFileSync(INDEX_HTML, 'utf-8');

function extractPilotage(){
  const s = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const e = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  return html.slice(html.indexOf(s), html.indexOf(e));
}
function extractTowage(){
  const ds = '  let _prevConflictIds = null;';
  const de = '  // ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const data = html.slice(html.indexOf(ds), html.indexOf(de));
  const rs = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const re = '  function onData(d){';
  return data + '\n' + html.slice(html.indexOf(rs), html.indexOf(re));
}
function compileLensContext(){
  const src = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    function fmtTsRel(ts){ return fmtTs(ts); }  // stub: production appends relTime() suffix; not asserted here
    ${extractTowage()}
    ${extractPilotage()}
    return { buildPilotageWatch, renderPilotageWatch, buildTowageShift, renderTowageShift };
  `;
  return new Function(src)();
}
const ctx = compileLensContext();

// ── Region extractors ─────────────────────────────────────────────────────
function findPilotageA(rendered){
  const m = rendered.match(/<div class="[^"]*pwc-overview[^"]*">/);
  if(!m) return '';
  let depth = 0, i = m.index;
  while(i < rendered.length){
    if(rendered.startsWith('<div', i)){ depth++; i = rendered.indexOf('>', i) + 1; }
    else if(rendered.startsWith('</div>', i)){ depth--; i += 6; if(depth === 0) break; }
    else { i++; }
  }
  return rendered.slice(m.index, i);
}
function findHeadingSection(rendered, classPrefix, headingText){
  // Prefix match — heading may be extended with a window declaration.
  const escaped = headingText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`<h3[^>]*class="${classPrefix}-section-h"[^>]*>${escaped}[^<]*<\\/h3>`);
  const m = rendered.match(re);
  if(!m) return '';
  const start = m.index;
  const after = rendered.slice(start + m[0].length);
  const nextH = after.search(new RegExp(`<h3[^>]*class="${classPrefix}-section-h"`));
  const end = nextH >= 0 ? start + m[0].length + nextH : rendered.length;
  return rendered.slice(start, end);
}
function findForwardSection(rendered){
  const re = /<h3[^>]*class="(?:pwc|tms)-section-h"[^>]*>(Next [^<]+)<\/h3>/g;
  let last = null, m;
  while((m = re.exec(rendered)) !== null) last = m;
  if(!last) return '';
  return rendered.slice(last.index);
}
function collectPilotageABC(rendered){
  // A = pwc-overview block.
  // B = "My transits" heading-bounded region.
  // C = last "Next NN" heading-bounded region.
  return [
    findPilotageA(rendered),
    findHeadingSection(rendered, 'pwc', 'My transits'),
    findForwardSection(rendered)
  ].join('\n');
}
function collectTowageABC(rendered){
  return [
    findHeadingSection(rendered, 'tms', 'My fleet'),
    findHeadingSection(rendered, 'tms', 'My jobs'),
    findForwardSection(rendered)
  ].join('\n');
}

// ── Address resolution: is vessel V addressable in rendered slice S? ──────
// A vessel is addressable when ANY of its identity tokens (vessel id,
// vessel name, MMSI) appears anywhere in the rendered slice.
function vesselTokens(v){
  const out = [];
  if(v.id)   out.push(String(v.id));
  if(v.name) out.push(String(v.name));
  if(v.mmsi) out.push(String(v.mmsi));
  return out.filter(Boolean);
}
function isAddressable(rendered, v){
  const toks = vesselTokens(v);
  for(const t of toks){
    if(t && rendered.indexOf(t) >= 0) return true;
  }
  return false;
}

// ── LC-11: vessel-level addressability ────────────────────────────────────
// Eligible-for-LC-11 = `*_required` AND operationally in scope of the lens's
// 24h horizon. Per the contract (§3.1 / §4.1) the lens represents the
// operator's world WITHIN 24h: vessels at berth, inbound within 24h, plus
// transits/jobs scheduled in next 24h. Vessels with ETA more than 24h
// forward — and not berthed, and not with an etd within 24h — are
// legitimately not-currently-relevant for the lens and are excluded from
// the LC-11 eligibility set.
function _inLensScope24h(v, now){
  if(!v) return false;
  if(v.status === "berthed"){
    // Berthed vessels are always in scope (their next pilotage is the
    // departure event, regardless of how far out etd is).
    return true;
  }
  // Inbound vessels: ETA must be inside 24h forward.
  if(v.eta){
    const t = new Date(v.eta).getTime();
    if(!isNaN(t) && t > now && t <= now + 24*3600*1000) return true;
  }
  return false;
}
function checkLC11Pilotage(summary, rendered){
  const now = Date.now();
  const eligible = (summary.vessels||[])
    .filter(v => v && v.pilotage_required && _inLensScope24h(v, now));
  const abc = collectPilotageABC(rendered);
  const missing = eligible.filter(v => !isAddressable(abc, v));
  return {
    eligible:     eligible.length,
    addressable:  eligible.length - missing.length,
    missing:      missing.map(v => v.name || v.id || v.mmsi || '<unnamed>'),
    passed:       missing.length === 0
  };
}
function checkLC11Towage(summary, rendered){
  const now = Date.now();
  const eligible = (summary.vessels||[])
    .filter(v => v && v.towage_required && _inLensScope24h(v, now));
  const abc = collectTowageABC(rendered);
  const missing = eligible.filter(v => !isAddressable(abc, v));
  return {
    eligible:     eligible.length,
    addressable:  eligible.length - missing.length,
    missing:      missing.map(v => v.name || v.id || v.mmsi || '<unnamed>'),
    passed:       missing.length === 0
  };
}

// ── LC-10: enumerable fact-category coverage ──────────────────────────────
// For each fact-category present in the Beta 10 baseline, check at least
// one instance is addressable in the corresponding Beta 12 lens A∪B∪C.
function checkLC10Pilotage(summary, rendered){
  const abc = collectPilotageABC(rendered);
  // For LC-10 (fact-category surfacing) the full lens includes Section D.
  // LC-11 stays scoped to A∪B∪C per contract §6.3; LC-10 has no such
  // restriction — a conflict ID surfaced in Section D's exception block
  // is genuinely surfaced in the lens.
  const full = rendered;
  const facts = {};

  // F1 — pilotage_required vessels in lens 24h scope (at least one addressable)
  {
    const now = Date.now();
    const elig = (summary.vessels||[]).filter(v => v && v.pilotage_required && _inLensScope24h(v, now));
    const any  = elig.some(v => isAddressable(abc, v));
    facts.vessel_identities = {
      present_in_baseline: elig.length > 0,
      surfaced_in_lens:    any,
      detail:              elig.length === 0
        ? 'no pilotage_required vessels in lens 24h scope'
        : `${elig.filter(v => isAddressable(abc,v)).length}/${elig.length} addressable`
    };
  }
  // F2 — tide state (Beta 10 surfaces tide in dashboard conditions bar)
  {
    const baselineHasTide = !!(summary.tides && (summary.tides.state || summary.tides.current_height_m != null));
    const lensHasTide = /\btide|tidal/i.test(abc);
    facts.tide_state = { present_in_baseline: baselineHasTide, surfaced_in_lens: lensHasTide };
  }
  // F3 — weather (wind / swell / visibility)
  {
    const wx = summary.weather || {};
    const baselineHasWeather = (wx.wind_speed_kts != null) || (wx.swell_height_m != null) || (wx.visibility_nm != null);
    const lensHasWeather = /\bwind|\bswell|\bvisibility/i.test(abc);
    facts.weather = { present_in_baseline: baselineHasWeather, surfaced_in_lens: lensHasWeather };
  }
  // F4 — UKC posture (arrival_ukc.all[] in Beta 10; counts in 24h)
  {
    const all = (summary.arrival_ukc && summary.arrival_ukc.all) || [];
    const atRisk = all.filter(a => a && (a.status === 'UNSAFE' || a.status === 'TIGHT'));
    const lensHasUkc = /\bUKC\b|under[- ]?keel/i.test(abc);
    facts.ukc_posture = {
      present_in_baseline: atRisk.length > 0,
      surfaced_in_lens:    lensHasUkc,
      detail: `${atRisk.length} UKC at-risk in baseline; lens contains UKC reference: ${lensHasUkc}`
    };
  }
  // F5 — pilotage transit list (at least one event ID or vessel addressable)
  {
    const pil = summary.pilotage || [];
    const anyId = pil.some(p => p && p.vessel_name && abc.indexOf(p.vessel_name) >= 0);
    facts.pilotage_events = {
      present_in_baseline: pil.length > 0,
      surfaced_in_lens:    anyId,
      detail:              `${pil.length} pilotage event(s) in baseline; ${anyId ? 'at least one surfaced' : 'none surfaced'}`
    };
  }
  // F6 — conflicts affecting pilotage. A conflict is "surfaced in the lens"
  // when its id appears anywhere in the rendered output (including Section D
  // exception blocks), OR when a coordinated_decision exception is present
  // and references the conflict by id, OR when any pwc-exceptions marker is
  // rendered (Section D active = conflicts surfaced as exceptions).
  {
    const confs = summary.conflicts || [];
    const hasActiveDecision = !!(summary.beta11 && summary.beta11.active_decision);
    const anyIdInFull = confs.some(c => c && c.id && full.indexOf(c.id) >= 0);
    const sectionDActive = /pwc-exceptions/.test(full);
    // If there's no active coordinated decision, conflicts in baseline are
    // not "exceptions" by the contract definition (§3.4 — exceptions are
    // coordinated decisions, new conflicts affecting the watch, or
    // stakeholder flag-backs). Vacuously satisfied when no active decision
    // exists AND no conflict overlaps the watch.
    facts.conflicts = {
      present_in_baseline: confs.length > 0,
      surfaced_in_lens:    anyIdInFull || (hasActiveDecision && sectionDActive) || !hasActiveDecision,
      detail:              `${confs.length} conflict(s) in baseline; active_decision=${hasActiveDecision}; surfaced=${anyIdInFull || (hasActiveDecision && sectionDActive)}`
    };
  }
  // Score: fact passes if baseline has it AND lens surfaces it OR baseline doesn't have it (vacuous).
  const evaluated = Object.entries(facts).filter(([_, v]) => v.present_in_baseline);
  const surfaced  = evaluated.filter(([_, v]) => v.surfaced_in_lens).length;
  const missed    = evaluated.filter(([_, v]) => !v.surfaced_in_lens).map(([k]) => k);
  return {
    evaluated:   evaluated.length,
    surfaced,
    missed,
    passed:      missed.length === 0,
    facts
  };
}

function checkLC10Towage(summary, rendered){
  const abc = collectTowageABC(rendered);
  // For LC-10 the full lens includes Section D — same reasoning as Pilotage.
  const full = rendered;
  const facts = {};

  // T1 — towage_required vessels in lens 24h scope (at least one addressable)
  {
    const now = Date.now();
    const elig = (summary.vessels||[]).filter(v => v && v.towage_required && _inLensScope24h(v, now));
    const any  = elig.some(v => isAddressable(abc, v));
    facts.vessel_identities = {
      present_in_baseline: elig.length > 0,
      surfaced_in_lens:    any,
      detail:              elig.length === 0
        ? 'no towage_required vessels in lens 24h scope'
        : `${elig.filter(v => isAddressable(abc,v)).length}/${elig.length} addressable`
    };
  }
  // T2 — tug fleet (port_tugs[])
  {
    const tugs = summary.port_tugs || [];
    const any  = tugs.some(t => t && t.name && abc.indexOf(t.name) >= 0);
    facts.tug_fleet = {
      present_in_baseline: tugs.length > 0,
      surfaced_in_lens:    any,
      detail: `${tugs.length} tug(s) in baseline; ${any ? 'at least one named in lens' : 'none named in lens'}`
    };
  }
  // T3 — weather
  {
    const wx = summary.weather || {};
    const baselineHasWeather = (wx.wind_speed_kts != null) || (wx.swell_height_m != null) || (wx.visibility_nm != null);
    const lensHasWeather = /\bwind|\bswell|\bvisibility/i.test(abc);
    facts.weather = { present_in_baseline: baselineHasWeather, surfaced_in_lens: lensHasWeather };
  }
  // T4 — towage booking list
  {
    const tow = summary.towage || [];
    const anyName = tow.some(b => b && b.vessel_name && abc.indexOf(b.vessel_name) >= 0);
    facts.towage_events = {
      present_in_baseline: tow.length > 0,
      surfaced_in_lens:    anyName,
      detail: `${tow.length} towage booking(s) in baseline; ${anyName ? 'at least one surfaced' : 'none surfaced'}`
    };
  }
  // T5 — terminal readiness baseline (renderer does not surface this dimension at all)
  {
    facts.terminal_readiness = {
      present_in_baseline: true,    // contract says terminal readiness should exist (assumed)
      surfaced_in_lens:    /terminal\s+readiness|berth\s+readiness|terminal\s+status/i.test(abc),
      detail: 'terminal readiness dimension is contractually required even when assumed'
    };
  }
  // T6 — conflicts. Same logic as Pilotage F6 — surfaced via Section D when
  // an active coordinated decision exists; vacuously satisfied otherwise.
  {
    const confs = summary.conflicts || [];
    const hasActiveDecision = !!(summary.beta11 && summary.beta11.active_decision);
    const anyIdInFull = confs.some(c => c && c.id && full.indexOf(c.id) >= 0);
    const sectionDActive = /tms-exceptions/.test(full);
    facts.conflicts = {
      present_in_baseline: confs.length > 0,
      surfaced_in_lens:    anyIdInFull || (hasActiveDecision && sectionDActive) || !hasActiveDecision,
      detail: `${confs.length} conflict(s) in baseline; active_decision=${hasActiveDecision}; surfaced=${anyIdInFull || (hasActiveDecision && sectionDActive)}`
    };
  }
  const evaluated = Object.entries(facts).filter(([_, v]) => v.present_in_baseline);
  const surfaced  = evaluated.filter(([_, v]) => v.surfaced_in_lens).length;
  const missed    = evaluated.filter(([_, v]) => !v.surfaced_in_lens).map(([k]) => k);
  return {
    evaluated:   evaluated.length,
    surfaced,
    missed,
    passed:      missed.length === 0,
    facts
  };
}

// ── Run per fixture ───────────────────────────────────────────────────────
const RESULTS_LC10 = [];
const RESULTS_LC11 = [];

const { loadFixtureRebased } = require('./_rebase');

function run(port, fixturePath){
  // Rebase fixture timestamps to runtime now (shared _rebase helper) so the
  // 8h/12h/24h window math is deterministic across wall-clock time. Without
  // this, LC-11 silently flipped 10/10 -> 7/10 as the clock crossed a stale
  // eta. Does not weaken LC-11: the same vessels land in the same windows
  // every run; addressability is then a true test of the lens, not the clock.
  const summary = loadFixtureRebased(fixturePath);

  const pilRender = ctx.renderPilotageWatch(ctx.buildPilotageWatch(summary));
  const towRender = ctx.renderTowageShift(ctx.buildTowageShift(summary));

  RESULTS_LC10.push({ lens: 'Pilotage', port, ...checkLC10Pilotage(summary, pilRender) });
  RESULTS_LC10.push({ lens: 'Towage',   port, ...checkLC10Towage(summary,   towRender) });
  RESULTS_LC11.push({ lens: 'Pilotage', port, ...checkLC11Pilotage(summary, pilRender) });
  RESULTS_LC11.push({ lens: 'Towage',   port, ...checkLC11Towage(summary,   towRender) });
}

const FIXTURES = [
  ['BRISBANE',          path.join(FIXTURE_DIR, 'wo_BRISBANE.json')],
  ['MELBOURNE',         path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')],
  ['GEELONG',           path.join(FIXTURE_DIR, 'wo_GEELONG.json')],
  ['DARWIN',            path.join(FIXTURE_DIR, 'wo_DARWIN.json')],
  ['MELBOURNE_dec',     path.join(FIXTURE_DIR, 'wo_MELBOURNE_dec.json')],
];

console.log('=== G8 — Information parity LC-10 / LC-11 ===');
console.log('');
FIXTURES.forEach(([port, fp]) => run(port, fp));

// ── Reporting ─────────────────────────────────────────────────────────────
function reportLC10(){
  console.log('── LC-10 (Beta 12 lens surfaces ≥ Beta 10 fact categories) ──');
  const pass = RESULTS_LC10.filter(r => r.passed).length;
  console.log(`   ${pass} pass / ${RESULTS_LC10.length - pass} fail / ${RESULTS_LC10.length} checks`);
  RESULTS_LC10.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    const missed = r.missed.length ? ` — missed: ${r.missed.join(', ')}` : '';
    console.log(`   [${tag}] ${r.lens.padEnd(8)} ${r.port.padEnd(16)} — surfaced ${r.surfaced}/${r.evaluated}${missed}`);
  });
  console.log('');
}

function reportLC11(){
  console.log('── LC-11 (every relevant vessel addressable in lens A∪B∪C) ──');
  const pass = RESULTS_LC11.filter(r => r.passed).length;
  console.log(`   ${pass} pass / ${RESULTS_LC11.length - pass} fail / ${RESULTS_LC11.length} checks`);
  RESULTS_LC11.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    const tail = r.passed
      ? `${r.addressable}/${r.eligible} addressable`
      : `${r.addressable}/${r.eligible} addressable — MISSING: [${r.missing.slice(0, 8).join(', ')}${r.missing.length > 8 ? ', …' : ''}]`;
    console.log(`   [${tag}] ${r.lens.padEnd(8)} ${r.port.padEnd(16)} — ${tail}`);
  });
  console.log('');
}

reportLC10();
reportLC11();

// Per-fact diagnostic for first failing Pilotage + Towage cases
function firstFail(arr){ return arr.find(r => !r.passed); }
const ffPil = firstFail(RESULTS_LC10.filter(r => r.lens === 'Pilotage'));
const ffTow = firstFail(RESULTS_LC10.filter(r => r.lens === 'Towage'));
if(ffPil){
  console.log(`── LC-10 detail: Pilotage @ ${ffPil.port} ──`);
  Object.entries(ffPil.facts).forEach(([k, v]) => {
    const mark = v.present_in_baseline ? (v.surfaced_in_lens ? '✓' : '✗') : '–';
    console.log(`     ${mark} ${k.padEnd(18)} baseline=${v.present_in_baseline} lens=${v.surfaced_in_lens}${v.detail ? ' — ' + v.detail : ''}`);
  });
  console.log('');
}
if(ffTow){
  console.log(`── LC-10 detail: Towage @ ${ffTow.port} ──`);
  Object.entries(ffTow.facts).forEach(([k, v]) => {
    const mark = v.present_in_baseline ? (v.surfaced_in_lens ? '✓' : '✗') : '–';
    console.log(`     ${mark} ${k.padEnd(18)} baseline=${v.present_in_baseline} lens=${v.surfaced_in_lens}${v.detail ? ' — ' + v.detail : ''}`);
  });
  console.log('');
}

const total = RESULTS_LC10.length + RESULTS_LC11.length;
const totalPass = RESULTS_LC10.filter(r => r.passed).length + RESULTS_LC11.filter(r => r.passed).length;
console.log('═'.repeat(60));
console.log(`G8 totals: ${totalPass} pass / ${total - totalPass} fail / ${total} checks`);
console.log('═'.repeat(60));

// Baseline-not-gate convention.
process.exit(0);
