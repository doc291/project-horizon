// G4 — Structural acceptance criteria LC-1, LC-4, LC-5.
//
// SCOPE
// -----
//   LC-1: each stakeholder lens MUST render four distinct sections
//         (A Operational Context, B Current Watch/Shift, C Forward Pressure,
//         D Exceptions) as separately identifiable regions in the DOM.
//   LC-4: Section B MUST declare its watch/shift window in the rendered
//         output (e.g., "Next 8 hours").
//   LC-5: Section C MUST declare its forward-pressure window in the
//         rendered output as a TWO-BOUND range strictly later than B's
//         window-end (e.g., "Next 8–24 hours").
//
// The current renderers do not emit formal <section data-lens-section="A">
// markers. Per Tony's G4 instruction ("detect sections using the best
// available current render markers"), this harness uses content heuristics
// (CSS class names and section heading text). When the heuristic cannot
// resolve a section, that is REPORTED AS A GOVERNANCE FAILURE — not a
// reason to modify the renderer.
//
// EXPECTED FAILURES (documented in advance):
//   - LC-4 Pilotage: Section B header is "My transits" with no window
//     declaration. The "Next 8 hours" string is in the rendered output
//     but it is the header of Section C (forward), not Section B.
//   - LC-4 Towage: same shape. Section B is "My jobs", no window.
//   - LC-5 Pilotage: Section C header is "Next 8 hours" — a single
//     duration, not a two-bound forward range strictly later than B's
//     window-end.
//   - LC-5 Towage: Section C header is "Next 12 hours" — same shape.
//   - LC-1 Section D: D renders only when exceptions exist. For plain
//     port fixtures (no active decision), D is legitimately absent. The
//     <port>_MELBOURNE_dec.json fixtures exercise the D-present path.
//
// CONSTRAINT
// ----------
// This harness does not modify index.html, server.py, or any module under
// the source tree outside tests/lens_contract/. It only observes.
//
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const INDEX_HTML = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const FIXTURE_DIR = path.join(__dirname, 'fixtures');

const html = fs.readFileSync(INDEX_HTML, 'utf-8');

// ── Extract the Pilotage data + renderer block ────────────────────────────
function extractPilotage(){
  const startMarker = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const endMarker   = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const si = html.indexOf(startMarker), ei = html.indexOf(endMarker, si);
  if(si < 0 || ei < 0) throw new Error('Pilotage marker extraction failed');
  return html.slice(si, ei);
}

// ── Extract the Towage data + renderer blocks ─────────────────────────────
// Layout in index.html (Beta 12 IIFE):
//   "T1: pure data transform..."          ← Towage DATA layer
//   "Pilotage Window Confidence..."       ← Pilotage DATA + RENDERER
//   "renderTowageShift(shift) -> HTML..." ← Towage RENDERER (after Pilotage)
//   function onData(d){...}               ← end of IIFE body
//
// So Towage is split: data BEFORE Pilotage, renderer AFTER.
function extractTowage(){
  // Towage DATA layer + module-level helpers (_prevConflictIds, _shiftHash,
  // _assignTugStatus, _SHIFT_SEV_RANK, _maxSeverity) live just above the T1
  // marker. We start from those helpers so buildTowageShift compiles
  // standalone.
  const dataStart = '  let _prevConflictIds = null;';
  const dataEnd   = '  // ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const dsi = html.indexOf(dataStart), dei = html.indexOf(dataEnd, dsi);
  if(dsi < 0 || dei < 0) throw new Error('Towage data marker extraction failed');
  const dataBlock = html.slice(dsi, dei);

  // Towage RENDERER (the "renderTowageShift -> HTML string." header → onData)
  const rendStart = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const rendEnd   = '  function onData(d){';
  const rsi = html.indexOf(rendStart), rei = html.indexOf(rendEnd, rsi);
  if(rsi < 0 || rei < 0) throw new Error('Towage renderer marker extraction failed');
  const rendBlock = html.slice(rsi, rei);

  // The Towage data block already includes _SHIFT_SEV_RANK / _maxSeverity /
  // _shiftHash / _assignTugStatus (now that we extend the start range).
  // We do NOT add header helpers here — re-declaration would shadow them.
  return dataBlock + '\n' + rendBlock;
}

// Compile both extracted blocks into a single context exposing build/render.
function compileLensContext(){
  const pilotageBlock = extractPilotage();
  const towageBlock   = extractTowage();
  const harnessSrc = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    ${towageBlock}
    ${pilotageBlock}
    return {
      buildPilotageWatch, renderPilotageWatch,
      buildTowageShift,   renderTowageShift
    };
  `;
  return new Function(harnessSrc)();
}

const ctx = compileLensContext();

// ── Section extractors ────────────────────────────────────────────────────
// Best-available section detection — heuristic, content-based.
// Pilotage:
//   A (Operational Context): div.pwc-overview (class "pwc-section pwc-overview")
//   B (Current Watch):       h3 "My transits" + following section content
//   C (Forward Pressure):    h3 "Next NN hours" + following section content
//   D (Exceptions):          div.pwc-exceptions (only when exceptions present)
// Towage:
//   A (Operational Context): h3 "My fleet" + following section content
//   B (Current Shift):       h3 "My jobs" + following section content
//   C (Forward Pressure):    h3 "Next NN hours" + following section content
//   D (Exceptions):          div.tms-exceptions (only when exceptions present)

function findSectionByHeading(rendered, headingText){
  // Returns the substring of the lens render between this heading's <h3>
  // open and the next <h3> open OR the end of the lens. Matches the
  // heading text as a PREFIX so headings extended with window
  // declarations (e.g., "My transits — next 8 hours") still resolve
  // to the same logical section.
  const escapedHeading = headingText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`<h3[^>]*>${escapedHeading}[^<]*<\\/h3>`);
  const m = rendered.match(re);
  if(!m) return null;
  const start = m.index;
  const after = rendered.slice(start + m[0].length);
  const nextHeading = after.search(/<h3[^>]*class="(?:pwc|tms)-section-h"/);
  const end = nextHeading >= 0 ? start + m[0].length + nextHeading : rendered.length;
  return rendered.slice(start, end);
}

function findOverviewSection(rendered){
  // Pilotage Section A: matches class "pwc-overview"; not delimited by h3.
  // Range: from opening <div class="...pwc-overview..."> to its matching </div>.
  const openRe = /<div class="[^"]*pwc-overview[^"]*">/;
  const m = rendered.match(openRe);
  if(!m) return null;
  // Find the matching close — track <div> depth from the opening tag.
  let depth = 0;
  let i = m.index;
  const len = rendered.length;
  while(i < len){
    if(rendered.startsWith('<div', i)){ depth++; i = rendered.indexOf('>', i) + 1; }
    else if(rendered.startsWith('</div>', i)){ depth--; i += 6; if(depth === 0) break; }
    else { i++; }
  }
  return rendered.slice(m.index, i);
}

function findExceptionsSection(rendered, classPrefix){
  // classPrefix is "pwc" or "tms"
  const openRe = new RegExp(`<div class="${classPrefix}-exceptions">`);
  const m = rendered.match(openRe);
  if(!m) return null;
  let depth = 0;
  let i = m.index;
  while(i < rendered.length){
    if(rendered.startsWith('<div', i)){ depth++; i = rendered.indexOf('>', i) + 1; }
    else if(rendered.startsWith('</div>', i)){ depth--; i += 6; if(depth === 0) break; }
    else { i++; }
  }
  return rendered.slice(m.index, i);
}

function findForwardSection(rendered){
  // Forward section = first h3 matching "Next NN hours" pattern in the lens.
  // (Pilotage and Towage both use this label — the criterion is to detect
  // the LATER section after Section B.)
  const re = /<h3[^>]*class="(?:pwc|tms)-section-h"[^>]*>(Next [^<]+)<\/h3>/g;
  let lastMatch = null;
  let m;
  while((m = re.exec(rendered)) !== null){ lastMatch = m; }
  if(!lastMatch) return null;
  const start = lastMatch.index;
  return rendered.slice(start);
}

// ── Section-presence detectors per lens ───────────────────────────────────
function detectPilotageSections(rendered, fixtureHasExceptions){
  return {
    A: findOverviewSection(rendered),
    B: findSectionByHeading(rendered, 'My transits'),
    C: findForwardSection(rendered),
    D: findExceptionsSection(rendered, 'pwc'),
    fixtureHasExceptions
  };
}

function detectTowageSections(rendered, fixtureHasExceptions){
  return {
    A: findSectionByHeading(rendered, 'My fleet'),
    B: findSectionByHeading(rendered, 'My jobs'),
    C: findForwardSection(rendered),
    D: findExceptionsSection(rendered, 'tms'),
    fixtureHasExceptions
  };
}

// ── Window-declaration detectors ──────────────────────────────────────────
const SINGLE_WINDOW_RE  = /Next\s+\d+\s+hours?\b/i;
const TWO_BOUND_WINDOW_RE = /Next\s+\d+\s*[-–—–—]\s*\d+\s+hours?\b|\bin\s+\d+\s*to\s*\d+\s+hours?\b/i;

function hasSingleWindow(section){
  if(!section) return false;
  return SINGLE_WINDOW_RE.test(section);
}
function hasTwoBoundWindow(section){
  if(!section) return false;
  return TWO_BOUND_WINDOW_RE.test(section);
}

// ── Criterion checks per fixture ──────────────────────────────────────────
const RESULTS = [];
function record(lens, port, criterion, passed, detail){
  RESULTS.push({ lens, port, criterion, passed, detail });
}

function checkLens(lensName, sections, port){
  // ── LC-1: all four sections detectable ────────────────────────────────
  const aOK = !!sections.A;
  const bOK = !!sections.B;
  const cOK = !!sections.C;
  // D is conditional on the fixture: a fixture without exceptions
  // legitimately renders no D section. Only fail when the fixture HAS
  // exceptions but no D was rendered.
  const dExpected = sections.fixtureHasExceptions;
  const dOK = dExpected ? !!sections.D : true;
  const lc1 = aOK && bOK && cOK && dOK;
  const lc1Detail = `A=${aOK?'✓':'✗'} B=${bOK?'✓':'✗'} C=${cOK?'✓':'✗'} D=${dOK?'✓':'✗'}${dExpected?'(expected)':'(n/a no exceptions)'}`;
  record(lensName, port, 'LC-1', lc1, lc1Detail);

  // ── LC-4: Section B declares its watch/shift window ───────────────────
  // The window declaration must appear INSIDE Section B's range, not in
  // the lens at large.
  const lc4 = hasSingleWindow(sections.B);
  let lc4Detail;
  if(!sections.B) lc4Detail = 'Section B not detected — cannot check window declaration';
  else if(lc4)    lc4Detail = `window phrase found in Section B`;
  else            lc4Detail = `Section B has no "Next NN hours" / equivalent. Heading is content-only (e.g., "My transits" / "My jobs").`;
  record(lensName, port, 'LC-4', lc4, lc4Detail);

  // ── LC-5: Section C declares a TWO-BOUND forward window ───────────────
  const hasAny = hasSingleWindow(sections.C);
  const hasTwoBound = hasTwoBoundWindow(sections.C);
  const lc5 = hasTwoBound;
  let lc5Detail;
  if(!sections.C) lc5Detail = 'Section C not detected — cannot check window declaration';
  else if(hasTwoBound) lc5Detail = 'two-bound forward window phrase found in Section C';
  else if(hasAny)      lc5Detail = `Section C declares a single duration only (not a two-bound forward range). Current heading mislabels forward window as same-as-watch.`;
  else                 lc5Detail = 'Section C has no window phrase at all';
  record(lensName, port, 'LC-5', lc5, lc5Detail);
}

function runFixture(port, fixturePath){
  const summary = require('./_rebase').loadFixtureRebased(fixturePath); // rebase stale fixture timestamps to runtime now (deterministic windows)
  const hasExceptions = !!(summary.beta11 && summary.beta11.active_decision);

  const watch = ctx.buildPilotageWatch(summary);
  const pilotageRender = ctx.renderPilotageWatch(watch);
  const pilSections = detectPilotageSections(pilotageRender, hasExceptions);
  checkLens('Pilotage', pilSections, port);

  const shift = ctx.buildTowageShift(summary);
  const towageRender = ctx.renderTowageShift(shift);
  const towSections = detectTowageSections(towageRender, hasExceptions);
  checkLens('Towage', towSections, port);
}

const FIXTURES = [
  ['BRISBANE',           path.join(FIXTURE_DIR, 'wo_BRISBANE.json')],
  ['MELBOURNE',          path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')],
  ['GEELONG',            path.join(FIXTURE_DIR, 'wo_GEELONG.json')],
  ['DARWIN',             path.join(FIXTURE_DIR, 'wo_DARWIN.json')],
  ['MELBOURNE_dec',      path.join(FIXTURE_DIR, 'wo_MELBOURNE_dec.json')]
];

console.log('=== G4 — Structural criteria LC-1 / LC-4 / LC-5 ===');
console.log('');
FIXTURES.forEach(([port, fp]) => runFixture(port, fp));

// ── Reporting ─────────────────────────────────────────────────────────────
function summariseBy(criterion){
  const rows = RESULTS.filter(r => r.criterion === criterion);
  const pass = rows.filter(r => r.passed).length;
  const fail = rows.length - pass;
  return { rows, pass, fail };
}

function report(criterion){
  const { rows, pass, fail } = summariseBy(criterion);
  console.log(`── ${criterion} ${'─'.repeat(56 - criterion.length)}`);
  console.log(`   ${pass} pass / ${fail} fail / ${rows.length} total`);
  rows.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    console.log(`   [${tag}] ${r.lens.padEnd(8)} ${r.port.padEnd(16)} — ${r.detail}`);
  });
  console.log('');
}

report('LC-1');
report('LC-4');
report('LC-5');

const totalPass = RESULTS.filter(r => r.passed).length;
const totalFail = RESULTS.length - totalPass;
console.log('═'.repeat(60));
console.log(`G4 totals: ${totalPass} pass / ${totalFail} fail / ${RESULTS.length} checks`);
console.log('═'.repeat(60));

// EXIT CONVENTION
// ---------------
// G4 is a governance baseline. Failures here are EXPECTED and REPORTED.
// We exit 0 so run_all.js reports the test as RAN; the lens-contract
// status is what matters and is printed above. Future G-tasks may
// promote failures to non-zero exits once remediation plans land.
process.exit(0);
