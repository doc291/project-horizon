// G5 — Mutation acceptance criterion LC-2.
//
// SCOPE
// -----
//   LC-2: Section A (Operational Context) MUST NOT be derived solely from
//         Section B's filtered watch/shift set. Shrinking B to zero MUST
//         NOT shrink, empty, or materially change Section A — provided
//         vessels[] for the active port has operational activity.
//
// MUTATION STRATEGY
// -----------------
// The contract example mutation is "shrink B's window to zero". An
// equivalent and renderer-agnostic mutation is to set the upstream feeds
// that B filters to empty arrays. Specifically:
//
//   summary.pilotage = []   →  Section B (Pilotage transits) renders empty
//   summary.towage   = []   →  Section B (Towage jobs) renders empty
//
// Section A SHOULD continue to render persistent operational context that
// is independent of these feeds. Any change in Section A indicates Section
// A is reading from feeds it should not be reading from, OR from filtered
// derivatives of those feeds. Either way: LC-2 fails.
//
// EXPECTED FAILURES (documented in advance, per the governance memo)
// ------------------------------------------------------------------
//   Pilotage Section A is the Watch Overview composed of five rows:
//     _pwcOvDemand        — reads watch.transits (filtered)        ← derives from B
//     _pwcOvWindow        — reads watch.transits flag_reasons      ← derives from B
//     _pwcOvShortNotice   — reads watch.transits flag_reasons      ← derives from B
//     _pwcOvEnvironment   — reads watch.forward.environment         (independent)
//     _pwcOvRoster        — reads watch.pilots_available            (independent)
//   3 of 5 rows are filtered derivatives. Section A is expected to shrink
//   under mutation. LC-2 expected to FAIL.
//
//   Towage Section A is the fleet list rendered by _tmsRenderFleet:
//     Each tug entry includes "${t.bookings_in_window} jobs this shift" —
//     bookings_in_window IS counted INSIDE the filtered window.
//   The fleet count itself is from port_tugs[] (independent of B). The
//   per-tug bookings count is a filtered derivative. Section A's textual
//   content is expected to change. LC-2 expected to FAIL.
//
// CONSTRAINT
// ----------
// This harness mutates fixture data IN MEMORY only. It does not modify
// any fixture file, any harness file, index.html, server.py, or any
// renderer / backend code.
//
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const INDEX_HTML = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const FIXTURE_DIR = path.join(__dirname, 'fixtures');
const html = fs.readFileSync(INDEX_HTML, 'utf-8');

// ── Extractors (mirror those in lc_structural.test.js so this file is
//    self-contained — no shared module yet at G5). ───────────────────────
function extractPilotage(){
  const startMarker = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const endMarker   = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const si = html.indexOf(startMarker), ei = html.indexOf(endMarker, si);
  if(si < 0 || ei < 0) throw new Error('Pilotage marker extraction failed');
  return html.slice(si, ei);
}
function extractTowage(){
  const dataStart = '  let _prevConflictIds = null;';
  const dataEnd   = '  // ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const dsi = html.indexOf(dataStart), dei = html.indexOf(dataEnd, dsi);
  if(dsi < 0 || dei < 0) throw new Error('Towage data marker extraction failed');
  const dataBlock = html.slice(dsi, dei);
  const rendStart = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const rendEnd   = '  function onData(d){';
  const rsi = html.indexOf(rendStart), rei = html.indexOf(rendEnd, rsi);
  if(rsi < 0 || rei < 0) throw new Error('Towage renderer marker extraction failed');
  const rendBlock = html.slice(rsi, rei);
  return dataBlock + '\n' + rendBlock;
}

function compileLensContext(){
  const harnessSrc = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    ${extractTowage()}
    ${extractPilotage()}
    return {
      buildPilotageWatch, renderPilotageWatch,
      buildTowageShift,   renderTowageShift
    };
  `;
  return new Function(harnessSrc)();
}
const ctx = compileLensContext();

// ── Section A extractors (heuristic — same as lc_structural) ──────────────
function findPilotageA(rendered){
  const openRe = /<div class="[^"]*pwc-overview[^"]*">/;
  const m = rendered.match(openRe);
  if(!m) return null;
  let depth = 0, i = m.index;
  while(i < rendered.length){
    if(rendered.startsWith('<div', i)){ depth++; i = rendered.indexOf('>', i) + 1; }
    else if(rendered.startsWith('</div>', i)){ depth--; i += 6; if(depth === 0) break; }
    else { i++; }
  }
  return rendered.slice(m.index, i);
}
function findTowageA(rendered){
  // Section A heading: "My fleet" (prefix match — heading may be extended
  // with a window declaration). Range = heading through to next h3 (B).
  const re = /<h3[^>]*class="tms-section-h"[^>]*>My fleet[^<]*<\/h3>/;
  const m = rendered.match(re);
  if(!m) return null;
  const start = m.index;
  const after = rendered.slice(start + m[0].length);
  const nextH = after.search(/<h3[^>]*class="tms-section-h"/);
  const end = nextH >= 0 ? start + m[0].length + nextH : rendered.length;
  return rendered.slice(start, end);
}

// ── Mutation: drop Section B's underlying feeds ───────────────────────────
function mutateSummary(summary){
  // Deep copy by JSON round-trip — summary is plain JSON
  const m = JSON.parse(JSON.stringify(summary));
  m.pilotage = [];
  m.towage   = [];
  return m;
}

// ── Diagnostic helpers ────────────────────────────────────────────────────
function countRowClasses(section, classRegex){
  if(!section) return 0;
  const re = new RegExp(classRegex, 'g');
  let n = 0;
  while(re.exec(section) !== null) n++;
  return n;
}

function strip(s){ return s ? s.replace(/\s+/g, ' ').trim() : ''; }

function diffReport(before, after, label){
  const b = before || '';
  const a = after || '';
  return {
    label,
    beforeChars: b.length,
    afterChars:  a.length,
    deltaChars:  a.length - b.length,
    identical:   b === a,
  };
}

// ── Run per fixture / per lens ────────────────────────────────────────────
const RESULTS = [];
function record(lens, port, passed, detail, diag){
  RESULTS.push({ lens, port, passed, detail, diag });
}

function checkPilotage(port, summary){
  const watchN = ctx.buildPilotageWatch(summary);
  const renderN = ctx.renderPilotageWatch(watchN);
  const sectAN  = findPilotageA(renderN);

  const mutated = mutateSummary(summary);
  const watchM  = ctx.buildPilotageWatch(mutated);
  const renderM = ctx.renderPilotageWatch(watchM);
  const sectAM  = findPilotageA(renderM);

  const rowsBefore = countRowClasses(sectAN, '<div class="pwc-ov-');
  const rowsAfter  = countRowClasses(sectAM, '<div class="pwc-ov-');
  const diag = diffReport(sectAN, sectAM, 'Pilotage Section A');
  diag.rowsBefore = rowsBefore;
  diag.rowsAfter  = rowsAfter;
  diag.rowDelta   = rowsAfter - rowsBefore;

  // Material change criteria:
  //   1. row count shrinks (a Layer 1 row disappeared) → FAIL
  //   2. content not byte-identical AND char delta > 5% → FAIL
  const rowShrink = rowsAfter < rowsBefore;
  const charPct = diag.beforeChars > 0
                ? Math.abs(diag.deltaChars) / diag.beforeChars
                : 0;
  const charChange = !diag.identical && charPct > 0.05;
  const passed = !rowShrink && !charChange && (diag.identical || charPct === 0);

  let detail;
  if(passed) detail = `Section A unchanged under mutation (rows ${rowsBefore}, chars ${diag.beforeChars})`;
  else if(rowShrink) detail = `Section A LOST ${rowsBefore - rowsAfter} row(s) under mutation (${rowsBefore} → ${rowsAfter}); content derived from filtered Section B`;
  else if(charChange) detail = `Section A content materially changed (${diag.beforeChars}→${diag.afterChars} chars, Δ=${diag.deltaChars}); some content is filtered`;
  else detail = `Section A content differs but within tolerance (${diag.beforeChars}→${diag.afterChars})`;

  record('Pilotage', port, passed, detail, diag);
}

function checkTowage(port, summary){
  const shiftN  = ctx.buildTowageShift(summary);
  const renderN = ctx.renderTowageShift(shiftN);
  const sectAN  = findTowageA(renderN);

  const mutated = mutateSummary(summary);
  const shiftM  = ctx.buildTowageShift(mutated);
  const renderM = ctx.renderTowageShift(shiftM);
  const sectAM  = findTowageA(renderM);

  // Tug row count should NOT change (fleet is from port_tugs, not filter).
  const tugRowsBefore = countRowClasses(sectAN, '<div class="tms-tug ');
  const tugRowsAfter  = countRowClasses(sectAM, '<div class="tms-tug ');

  // Per-tug bookings_in_window text: sum of "N jobs this shift" numbers.
  function sumBookings(section){
    if(!section) return 0;
    const re = /(\d+)\s+job(?:s)?\s+this\s+shift/g;
    let total = 0, m;
    while((m = re.exec(section)) !== null) total += parseInt(m[1], 10);
    return total;
  }
  const sumBefore = sumBookings(sectAN);
  const sumAfter  = sumBookings(sectAM);

  const diag = diffReport(sectAN, sectAM, 'Towage Section A');
  diag.tugRowsBefore = tugRowsBefore;
  diag.tugRowsAfter  = tugRowsAfter;
  diag.bookingsSumBefore = sumBefore;
  diag.bookingsSumAfter  = sumAfter;
  diag.bookingsDelta = sumAfter - sumBefore;

  const tugRowShrink = tugRowsAfter < tugRowsBefore;
  const bookingsChanged = sumBefore !== sumAfter;
  const charPct = diag.beforeChars > 0
                ? Math.abs(diag.deltaChars) / diag.beforeChars
                : 0;
  const charChange = !diag.identical && charPct > 0.02;

  const passed = !tugRowShrink && !bookingsChanged && !charChange;

  let detail;
  if(passed) detail = `Section A unchanged under mutation (${tugRowsBefore} tugs, bookings sum ${sumBefore})`;
  else if(tugRowShrink) detail = `Tug count LOST ${tugRowsBefore - tugRowsAfter} row(s) under mutation`;
  else if(bookingsChanged) detail = `Per-tug bookings count changed (sum ${sumBefore} → ${sumAfter}); fleet row text reads filtered B`;
  else if(charChange) detail = `Section A content materially changed (${diag.beforeChars}→${diag.afterChars} chars)`;
  else detail = `Section A near-identical but not byte-identical`;

  record('Towage', port, passed, detail, diag);
}

const FIXTURES = [
  ['BRISBANE',          path.join(FIXTURE_DIR, 'wo_BRISBANE.json')],
  ['MELBOURNE',         path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')],
  ['GEELONG',           path.join(FIXTURE_DIR, 'wo_GEELONG.json')],
  ['DARWIN',            path.join(FIXTURE_DIR, 'wo_DARWIN.json')],
  ['MELBOURNE_dec',     path.join(FIXTURE_DIR, 'wo_MELBOURNE_dec.json')],
];

console.log('=== G5 — Mutation criterion LC-2 (Section A independence) ===');
console.log('Mutation applied: summary.pilotage = []; summary.towage = []');
console.log('');

FIXTURES.forEach(([port, fp]) => {
  const summary = require('./_rebase').loadFixtureRebased(fp); // rebase stale fixture timestamps to runtime now (deterministic windows)
  checkPilotage(port, summary);
  checkTowage(port, summary);
});

// ── Reporting ─────────────────────────────────────────────────────────────
function report(lens){
  const rows = RESULTS.filter(r => r.lens === lens);
  const pass = rows.filter(r => r.passed).length;
  const fail = rows.length - pass;
  console.log(`── ${lens} ${'─'.repeat(54 - lens.length)}`);
  console.log(`   ${pass} pass / ${fail} fail / ${rows.length} fixtures`);
  rows.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    console.log(`   [${tag}] ${r.port.padEnd(16)} — ${r.detail}`);
    if(!r.passed){
      const d = r.diag;
      if(lens === 'Pilotage'){
        console.log(`          rows ${d.rowsBefore}→${d.rowsAfter} (Δ ${d.rowDelta}); chars ${d.beforeChars}→${d.afterChars} (Δ ${d.deltaChars})`);
      } else {
        console.log(`          tugs ${d.tugRowsBefore}→${d.tugRowsAfter}; bookings sum ${d.bookingsSumBefore}→${d.bookingsSumAfter} (Δ ${d.bookingsDelta}); chars Δ ${d.deltaChars}`);
      }
    }
  });
  console.log('');
}

report('Pilotage');
report('Towage');

const totalPass = RESULTS.filter(r => r.passed).length;
const totalFail = RESULTS.length - totalPass;
console.log('═'.repeat(60));
console.log(`LC-2 totals: ${totalPass} pass / ${totalFail} fail / ${RESULTS.length} checks`);
console.log('═'.repeat(60));

// Same exit convention as G4: governance baseline, not gate. Failures are
// reported, not raised. Exit 0 so run_all.js shows the test as RAN.
process.exit(0);
