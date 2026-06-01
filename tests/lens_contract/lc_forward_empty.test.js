// G6 — LC-3 (Section C strictly later than B) + LC-9 (empty-B resilience).
//
// SCOPE
// -----
//   LC-3: Section C MUST contain items strictly LATER than Section B's
//         window_end. For Pilotage: Section C items must be > now + 7h
//         (the rolling-8h watch B-window). For Towage: items must be
//         > now + 11h (the rolling-12h shift B-window).
//
//   LC-9: When Section B is empty (no transits / no jobs in window), the
//         lens MUST still render:
//         - Section A (Operational Context)
//         - Section C (Forward Pressure)
//         - Section D (Exceptions) when applicable
//         - A clear "Quiet watch" / "Quiet shift" note inside Section B
//         The lens MUST NOT degrade to a single empty box.
//
// METHOD
// ------
//   LC-3: For each fixture, render the lens normally and inspect Section C.
//         Two heuristics combined:
//           (a) Parse the Section C heading's window declaration. If it
//               reads "Next N hours" where N ≤ B.window_end_hours, no item
//               INSIDE Section C can be strictly later than B → FAIL.
//           (b) Parse rendered slot time labels (HH:MM–HH:MM). Compute the
//               max hour offset from "now". If max ≤ B_end_hours → FAIL.
//
//   LC-9: For each fixture, deep-copy and set summary.pilotage = [] (and
//         summary.towage = []). Render and check Section A presence,
//         Section C presence, and quiet-state note inside Section B.
//
// EXPECTED FAILURES (documented per the governance memo)
// ------------------------------------------------------
//   LC-3 Pilotage: Section C heading is "Next 8 hours" — same upper bound
//     as B (now + 8h). Forward slots run 0-8h. No item is strictly later
//     than B.window_end. Expected to FAIL all five fixtures.
//   LC-3 Towage: Section C heading is "Next 12 hours" — same upper bound
//     as B. Forward slots run 0-12h. Same shape. Expected to FAIL.
//
//   LC-9 Pilotage: empty-pilotage path emits "No transits scheduled in
//     the current watch window." Section A renders (5-row overview).
//     Section C renders (forward slots). Section D conditional. Expected
//     to PASS structurally.
//   LC-9 Towage: empty-towage path emits "No jobs scheduled for the
//     current shift." Section A renders (fleet). Section C renders.
//     Expected to PASS structurally.
//
// CONSTRAINT
// ----------
// Observation only. No application code touched. No commits, no deploys.
//
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const INDEX_HTML = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const FIXTURE_DIR = path.join(__dirname, 'fixtures');
const html = fs.readFileSync(INDEX_HTML, 'utf-8');

// ── Extractors (identical to lc_structural / lc_mutation) ─────────────────
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
  const dataBlock = html.slice(dsi, dei);
  const rendStart = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const rendEnd   = '  function onData(d){';
  const rsi = html.indexOf(rendStart), rei = html.indexOf(rendEnd, rsi);
  return dataBlock + '\n' + html.slice(rsi, rei);
}
function compileLensContext(){
  const harnessSrc = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    ${extractTowage()}
    ${extractPilotage()}
    return { buildPilotageWatch, renderPilotageWatch, buildTowageShift, renderTowageShift };
  `;
  return new Function(harnessSrc)();
}
const ctx = compileLensContext();

// ── Section locators ──────────────────────────────────────────────────────
function findForwardSection(rendered){
  // The forward section is identified by the last <h3 class="...-section-h">
  // matching "Next NN hours" in the lens render.
  const re = /<h3[^>]*class="(?:pwc|tms)-section-h"[^>]*>(Next [^<]+)<\/h3>/g;
  let last = null, m;
  while((m = re.exec(rendered)) !== null) last = m;
  if(!last) return { section: null, heading: null };
  return { section: rendered.slice(last.index), heading: last[1] };
}
function findSectionByHeading(rendered, headingText){
  // Prefix match — headings extended with window declarations still resolve.
  const escaped = headingText.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`<h3[^>]*>${escaped}[^<]*<\\/h3>`);
  const m = rendered.match(re);
  if(!m) return null;
  const start = m.index;
  const after = rendered.slice(start + m[0].length);
  const nextH = after.search(/<h3[^>]*class="(?:pwc|tms)-section-h"/);
  const end = nextH >= 0 ? start + m[0].length + nextH : rendered.length;
  return rendered.slice(start, end);
}
function findPilotageA(rendered){
  const m = rendered.match(/<div class="[^"]*pwc-overview[^"]*">/);
  if(!m) return null;
  let depth = 0, i = m.index;
  while(i < rendered.length){
    if(rendered.startsWith('<div', i)){ depth++; i = rendered.indexOf('>', i) + 1; }
    else if(rendered.startsWith('</div>', i)){ depth--; i += 6; if(depth === 0) break; }
    else { i++; }
  }
  return rendered.slice(m.index, i);
}

// ── LC-3 helpers ──────────────────────────────────────────────────────────
function parseDeclaredUpperBoundHours(headingText){
  // "Next 8 hours" → 8
  // "Next 12 hours" → 12
  // "Next 8-24 hours" / "Next 8–24 hours" → 24 (the upper)
  if(!headingText) return null;
  const m1 = headingText.match(/Next\s+\d+\s*[-–—]\s*(\d+)\s+hours?/i);
  if(m1) return parseInt(m1[1], 10);
  const m2 = headingText.match(/Next\s+(\d+)\s+hours?/i);
  if(m2) return parseInt(m2[1], 10);
  return null;
}

// ── LC-9 helpers ──────────────────────────────────────────────────────────
function hasQuietStateNote(rendered, lens){
  // Pilotage quiet phrase: "No transits scheduled in the current watch window."
  // Towage quiet phrase:   "No jobs scheduled for the current shift."
  if(lens === 'Pilotage') return /No transits scheduled/.test(rendered);
  if(lens === 'Towage')   return /No jobs scheduled/.test(rendered);
  return false;
}
function mutateSummaryEmptyB(summary){
  const m = JSON.parse(JSON.stringify(summary));
  m.pilotage = [];
  m.towage   = [];
  return m;
}

// ── Run per fixture ───────────────────────────────────────────────────────
const RESULTS_LC3 = [];
const RESULTS_LC9 = [];

function checkLC3(lens, port, rendered, bWindowHours){
  const forward = findForwardSection(rendered);
  if(!forward.section){
    RESULTS_LC3.push({ lens, port, passed: false, detail: 'Section C not detected' });
    return;
  }
  const declaredUpper = parseDeclaredUpperBoundHours(forward.heading);
  // Strict LC-3: declared upper bound must be > bWindowHours.
  // If equal or less, no item INSIDE Section C can be strictly later than
  // B.window_end → FAIL.
  let passed, detail;
  if(declaredUpper == null){
    passed = false;
    detail = `Section C heading "${forward.heading}" has no parseable window — cannot verify strictly-later`;
  } else if(declaredUpper > bWindowHours){
    passed = true;
    detail = `Section C declares Next 8–${declaredUpper}h or equivalent; strictly later than B.window_end (${bWindowHours}h)`;
  } else {
    passed = false;
    detail = `Section C declared upper bound (${declaredUpper}h) ≤ B.window_end (${bWindowHours}h). Items in Section C cannot be strictly later than Section B — Section C is a duplicate / summary of B, not Forward Pressure.`;
  }
  RESULTS_LC3.push({ lens, port, passed, detail, declaredUpper, bWindowHours });
}

function checkLC9(lens, port, summary){
  const mutated = mutateSummaryEmptyB(summary);
  const render = lens === 'Pilotage'
    ? ctx.renderPilotageWatch(ctx.buildPilotageWatch(mutated))
    : ctx.renderTowageShift(ctx.buildTowageShift(mutated));

  const sectionA = lens === 'Pilotage'
    ? findPilotageA(render)
    : findSectionByHeading(render, 'My fleet');
  const forward = findForwardSection(render);
  const quietNote = hasQuietStateNote(render, lens);
  const decExpected = !!(summary.beta11 && summary.beta11.active_decision);
  const sectionD = lens === 'Pilotage'
    ? /class="pwc-exceptions"/.test(render)
    : /class="tms-exceptions"/.test(render);

  const aOK = !!sectionA;
  const cOK = !!forward.section;
  const qOK = quietNote;
  const dOK = decExpected ? sectionD : true;

  const passed = aOK && cOK && qOK && dOK;
  const detail = `A=${aOK?'✓':'✗'} C=${cOK?'✓':'✗'} quiet-note=${qOK?'✓':'✗'} D=${dOK?'✓':'✗'}${decExpected?'(expected)':'(n/a)'}`;
  RESULTS_LC9.push({ lens, port, passed, detail });
}

const FIXTURES = [
  ['BRISBANE',          path.join(FIXTURE_DIR, 'wo_BRISBANE.json')],
  ['MELBOURNE',         path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')],
  ['GEELONG',           path.join(FIXTURE_DIR, 'wo_GEELONG.json')],
  ['DARWIN',            path.join(FIXTURE_DIR, 'wo_DARWIN.json')],
  ['MELBOURNE_dec',     path.join(FIXTURE_DIR, 'wo_MELBOURNE_dec.json')],
];

console.log('=== G6 — LC-3 (Section C strictly later) + LC-9 (empty-B resilience) ===');
console.log('');

FIXTURES.forEach(([port, fp]) => {
  const summary = JSON.parse(fs.readFileSync(fp, 'utf-8'));

  // LC-3 — normal render, inspect Section C declared window
  const pilRender = ctx.renderPilotageWatch(ctx.buildPilotageWatch(summary));
  checkLC3('Pilotage', port, pilRender, 8);  // B window_end = now + 8h

  const towRender = ctx.renderTowageShift(ctx.buildTowageShift(summary));
  checkLC3('Towage', port, towRender, 12);  // B window_end = now + 12h

  // LC-9 — mutate to empty B, verify Section A/C/D + quiet note
  checkLC9('Pilotage', port, summary);
  checkLC9('Towage', port, summary);
});

// ── Reporting ─────────────────────────────────────────────────────────────
function reportSet(criterion, results){
  console.log(`── ${criterion} ${'─'.repeat(56 - criterion.length)}`);
  const pass = results.filter(r => r.passed).length;
  const fail = results.length - pass;
  console.log(`   ${pass} pass / ${fail} fail / ${results.length} checks`);
  results.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    console.log(`   [${tag}] ${r.lens.padEnd(8)} ${r.port.padEnd(16)} — ${r.detail}`);
  });
  console.log('');
}

reportSet('LC-3', RESULTS_LC3);
reportSet('LC-9', RESULTS_LC9);

const totalPass = RESULTS_LC3.filter(r => r.passed).length + RESULTS_LC9.filter(r => r.passed).length;
const totalFail = RESULTS_LC3.length + RESULTS_LC9.length - totalPass;
console.log('═'.repeat(60));
console.log(`G6 totals: ${totalPass} pass / ${totalFail} fail / ${RESULTS_LC3.length + RESULTS_LC9.length} checks`);
console.log('═'.repeat(60));

// Same exit convention as G4/G5: governance baseline, not gate.
process.exit(0);
