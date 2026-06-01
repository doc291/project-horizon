// G7 — Representational adequacy criteria LC-6 / LC-7 / LC-8.
//
// SCOPE
// -----
//   LC-6: If the fixture has operational activity, Section A MUST reference
//         at least THREE distinct operational dimensions defined in
//         docs/governance/LENS-CONTRACT.md §3.1 (Pilotage) and §4.1 (Towage).
//
//   LC-7: Pilotage Section A MUST include:
//          (a) count of vessels at berth requiring outbound pilotage
//          (b) count of inbound vessels within 24h requiring pilotage
//
//   LC-8: Towage Section A MUST include:
//          (a) tug count
//          (b) count of vessels at berth requiring outbound towage
//          (c) count of inbound vessels within 24h requiring towage
//
// METHOD
// ------
// Render the current Pilotage / Towage lens for each fixture. Extract
// Section A using the best-available markers (same as G4-G6 harnesses).
// Probe Section A's rendered HTML for each dimension defined in the
// contract. Dimension presence is detected by content patterns the
// current renderer emits (e.g. the "Roster reference: N pilots" string
// for the Pilotage roster dimension). When the current UI does not
// expose any marker for a dimension at all, that is recorded as a
// governance failure — not as a reason to modify the renderer.
//
// "Operational activity present" means the underlying vessels[] for the
// fixture's port has at least one vessel. The harness counts the fixture's
// vessels[] to derive operational-activity status per fixture.
//
// EXPECTED FAILURES (documented per the governance memo §4.1-4.2)
// ---------------------------------------------------------------
//   Pilotage Section A currently surfaces:
//     ✓ pilot roster reference (the "N pilots on duty" row)
//     ⚠ tidal envelope (the environment row contains a tide-state line)
//     ⚠ UKC posture (the window-pressure row mentions UKC when present)
//     ✗ count of vessels at berth requiring pilotage  ← LC-7(a) MISSING
//     ✗ count of inbound 24h requiring pilotage       ← LC-7(b) MISSING
//   The demand headline counts ONLY in-watch transits, not the broader
//   vessel population (LC-2/G5 finding restated).
//
//   Towage Section A currently surfaces:
//     ✓ tug fleet status                              ← LC-8(a) present
//     ✗ vessels at berth requiring outbound towage    ← LC-8(b) MISSING
//     ✗ inbound 24h requiring towage                  ← LC-8(c) MISSING
//     ✗ terminal readiness baseline                   ← MISSING
//     ✗ weather envelope                              ← MISSING
//   The lens reads only port_tugs[] and the filtered bookings count per tug.
//
// CONSTRAINT
// ----------
// Observation only. No application code touched.
//
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
  const rend = html.slice(html.indexOf(rs), html.indexOf(re));
  return data + '\n' + rend;
}
function compileLensContext(){
  const src = `
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
    ${extractTowage()}
    ${extractPilotage()}
    return { buildPilotageWatch, renderPilotageWatch, buildTowageShift, renderTowageShift };
  `;
  return new Function(src)();
}
const ctx = compileLensContext();

// ── Section A extractors (same as G4-G6) ──────────────────────────────────
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
function findTowageA(rendered){
  // Prefix match — heading may be extended with a window declaration.
  const m = rendered.match(/<h3[^>]*class="tms-section-h"[^>]*>My fleet[^<]*<\/h3>/);
  if(!m) return null;
  const start = m.index;
  const after = rendered.slice(start + m[0].length);
  const next = after.search(/<h3[^>]*class="tms-section-h"/);
  const end = next >= 0 ? start + m[0].length + next : rendered.length;
  return rendered.slice(start, end);
}

// ── Dimension detectors per lens ──────────────────────────────────────────
// Each detector returns { present: boolean, evidence: string }
function probePilotageDimensions(sectionA){
  if(!sectionA) {
    return {
      vesselsAtBerth:  { present: false, evidence: 'Section A not detected' },
      inbound24h:      { present: false, evidence: 'Section A not detected' },
      pilotRoster:     { present: false, evidence: 'Section A not detected' },
      tidalEnvelope:   { present: false, evidence: 'Section A not detected' },
      ukcPosture:      { present: false, evidence: 'Section A not detected' },
    };
  }
  // Strip HTML to text for content searches.
  const text = sectionA.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

  // Pilot roster reference — Roster reference: N pilots on duty.
  const rosterRe = /Roster reference:[^.]+/i;
  const rosterMatch = text.match(rosterRe);

  // Tidal envelope — environment row contains tide state.
  const tideRe = /\btide\b|tidal/i;
  const tideMatch = text.match(tideRe);

  // UKC posture — window-pressure row mentions UKC when present.
  const ukcRe = /\bUKC\b|under[- ]?keel/i;
  const ukcMatch = text.match(ukcRe);

  // Count of vessels at berth requiring outbound pilotage — would read as
  // "N vessels at berth", "N outbound", "N awaiting departure", or similar.
  // Currently NOT rendered.
  const berthRe = /(\d+)\s+vessel(?:s)?\s+at\s+berth|(\d+)\s+(?:berthed|outbound)\s+pilotage|(\d+)\s+awaiting\s+depart/i;
  const berthMatch = text.match(berthRe);

  // Count of inbound vessels within 24h requiring pilotage.
  const inboundRe = /(\d+)\s+inbound(?:\s+24h?)?|(\d+)\s+vessels?\s+inbound\s+within\s+24|(\d+)\s+arriv\w+\s+24/i;
  const inboundMatch = text.match(inboundRe);

  return {
    vesselsAtBerth: { present: !!berthMatch,    evidence: berthMatch  ? berthMatch[0]  : 'no count for vessels at berth requiring outbound pilotage' },
    inbound24h:     { present: !!inboundMatch,  evidence: inboundMatch? inboundMatch[0]: 'no count for inbound vessels within 24h requiring pilotage' },
    pilotRoster:    { present: !!rosterMatch,   evidence: rosterMatch ? rosterMatch[0] : 'no pilot roster reference' },
    tidalEnvelope:  { present: !!tideMatch,     evidence: tideMatch   ? `text contains "${tideMatch[0]}"` : 'no tidal envelope reference' },
    ukcPosture:     { present: !!ukcMatch,      evidence: ukcMatch    ? `text contains "${ukcMatch[0]}"`  : 'no UKC posture reference' },
  };
}

function probeTowageDimensions(sectionA){
  if(!sectionA){
    return {
      tugFleet:          { present: false, evidence: 'Section A not detected' },
      vesselsAtBerth:    { present: false, evidence: 'Section A not detected' },
      inbound24h:        { present: false, evidence: 'Section A not detected' },
      terminalReadiness: { present: false, evidence: 'Section A not detected' },
      weatherEnvelope:   { present: false, evidence: 'Section A not detected' },
    };
  }
  const text = sectionA.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

  // Tug fleet status — counted by tms-tug entries inside Section A.
  const tugRe = /<div class="tms-tug /g;
  let tugCount = 0;
  while(tugRe.exec(sectionA) !== null) tugCount++;

  // Vessels at berth requiring outbound towage.
  const berthRe = /(\d+)\s+vessel(?:s)?\s+at\s+berth|(\d+)\s+(?:berthed|outbound)\s+towage|(\d+)\s+awaiting\s+depart/i;
  const berthMatch = text.match(berthRe);

  // Inbound 24h requiring towage.
  const inboundRe = /(\d+)\s+inbound(?:\s+24h?)?|(\d+)\s+vessels?\s+inbound\s+within\s+24|(\d+)\s+arriv\w+\s+24/i;
  const inboundMatch = text.match(inboundRe);

  // Terminal readiness baseline.
  const terminalRe = /terminal\s+readiness|berth\s+readiness|terminal\s+status/i;
  const terminalMatch = text.match(terminalRe);

  // Weather envelope.
  const weatherRe = /\bwind\b|\bswell\b|\bweather\b|\bvisibility\b/i;
  const weatherMatch = text.match(weatherRe);

  return {
    tugFleet:          { present: tugCount > 0,  evidence: tugCount > 0 ? `${tugCount} tug row(s) rendered` : 'no tug fleet rendered' },
    vesselsAtBerth:    { present: !!berthMatch,  evidence: berthMatch   ? berthMatch[0]    : 'no count for vessels at berth requiring outbound towage' },
    inbound24h:        { present: !!inboundMatch,evidence: inboundMatch ? inboundMatch[0]  : 'no count for inbound vessels within 24h requiring towage' },
    terminalReadiness: { present: !!terminalMatch, evidence: terminalMatch ? terminalMatch[0] : 'no terminal readiness baseline' },
    weatherEnvelope:   { present: !!weatherMatch,  evidence: weatherMatch  ? `text contains "${weatherMatch[0]}"` : 'no weather envelope reference' },
  };
}

// ── Run per fixture ───────────────────────────────────────────────────────
const RESULTS_LC6 = [];
const RESULTS_LC7 = [];
const RESULTS_LC8 = [];

function runFixture(port, fixturePath){
  const summary = require('./_rebase').loadFixtureRebased(fixturePath); // rebase stale fixture timestamps to runtime now (deterministic windows)
  const hasActivity = Array.isArray(summary.vessels) && summary.vessels.length > 0;

  // PILOTAGE
  const watch = ctx.buildPilotageWatch(summary);
  const pilRender = ctx.renderPilotageWatch(watch);
  const pilA = findPilotageA(pilRender);
  const pilDims = probePilotageDimensions(pilA);
  const pilDimCount = Object.values(pilDims).filter(d => d.present).length;

  // LC-6 Pilotage — ≥ 3 dimensions when activity present
  {
    const passed = !hasActivity || pilDimCount >= 3;
    const dimList = Object.entries(pilDims).filter(([_, v]) => v.present).map(([k]) => k).join(', ') || '(none)';
    RESULTS_LC6.push({
      lens: 'Pilotage', port, passed,
      detail: hasActivity
        ? `${pilDimCount} dimension(s) present: ${dimList}`
        : `no operational activity in fixture — LC-6 vacuously passes`
    });
  }

  // LC-7 — both Pilotage counts must be present
  {
    const a = pilDims.vesselsAtBerth.present;
    const b = pilDims.inbound24h.present;
    const passed = a && b;
    RESULTS_LC7.push({
      port, passed,
      detail: `vessels-at-berth=${a?'✓':'✗'} inbound-24h=${b?'✓':'✗'} — ${a&&b ? 'both counts present' : 'missing required counts: ' + (!a?'vessels-at-berth ':'') + (!b?'inbound-24h':'')}`,
      pilDims
    });
  }

  // TOWAGE
  const shift = ctx.buildTowageShift(summary);
  const towRender = ctx.renderTowageShift(shift);
  const towA = findTowageA(towRender);
  const towDims = probeTowageDimensions(towA);
  const towDimCount = Object.values(towDims).filter(d => d.present).length;

  // LC-6 Towage
  {
    const passed = !hasActivity || towDimCount >= 3;
    const dimList = Object.entries(towDims).filter(([_, v]) => v.present).map(([k]) => k).join(', ') || '(none)';
    RESULTS_LC6.push({
      lens: 'Towage', port, passed,
      detail: hasActivity
        ? `${towDimCount} dimension(s) present: ${dimList}`
        : `no operational activity in fixture — LC-6 vacuously passes`
    });
  }

  // LC-8 — all three Towage counts must be present
  {
    const a = towDims.tugFleet.present;
    const b = towDims.vesselsAtBerth.present;
    const c = towDims.inbound24h.present;
    const passed = a && b && c;
    const missing = [!a && 'tug-count', !b && 'vessels-at-berth', !c && 'inbound-24h'].filter(Boolean);
    RESULTS_LC8.push({
      port, passed,
      detail: `tugs=${a?'✓':'✗'} vessels-at-berth=${b?'✓':'✗'} inbound-24h=${c?'✓':'✗'} — ${a&&b&&c ? 'all three present' : 'missing: ' + missing.join(', ')}`,
      towDims
    });
  }
}

const FIXTURES = [
  ['BRISBANE',          path.join(FIXTURE_DIR, 'wo_BRISBANE.json')],
  ['MELBOURNE',         path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')],
  ['GEELONG',           path.join(FIXTURE_DIR, 'wo_GEELONG.json')],
  ['DARWIN',            path.join(FIXTURE_DIR, 'wo_DARWIN.json')],
  ['MELBOURNE_dec',     path.join(FIXTURE_DIR, 'wo_MELBOURNE_dec.json')],
];

console.log('=== G7 — Representational adequacy LC-6 / LC-7 / LC-8 ===');
console.log('');
FIXTURES.forEach(([port, fp]) => runFixture(port, fp));

// ── Reporting ─────────────────────────────────────────────────────────────
function report(label, results){
  console.log(`── ${label} ${'─'.repeat(56 - label.length)}`);
  const pass = results.filter(r => r.passed).length;
  const fail = results.length - pass;
  console.log(`   ${pass} pass / ${fail} fail / ${results.length} checks`);
  results.forEach(r => {
    const tag = r.passed ? 'PASS' : 'FAIL';
    const head = r.lens ? `${r.lens.padEnd(8)} ${r.port.padEnd(16)}` : r.port.padEnd(25);
    console.log(`   [${tag}] ${head} — ${r.detail}`);
  });
  console.log('');
}

report('LC-6 (≥3 dimensions in Section A)', RESULTS_LC6);
report('LC-7 (Pilotage required counts)',    RESULTS_LC7);
report('LC-8 (Towage required counts)',      RESULTS_LC8);

// Per-dimension diagnostic block (first fixture only, for clarity).
console.log('── Per-dimension probe (MELBOURNE fixture, both lenses) ─────');
const sampleSummary = require('./_rebase').loadFixtureRebased(path.join(FIXTURE_DIR, 'wo_MELBOURNE.json')); // rebase stale fixture timestamps
const samplePilA = findPilotageA(ctx.renderPilotageWatch(ctx.buildPilotageWatch(sampleSummary)));
const sampleTowA = findTowageA(ctx.renderTowageShift(ctx.buildTowageShift(sampleSummary)));
const sP = probePilotageDimensions(samplePilA);
const sT = probeTowageDimensions(sampleTowA);
console.log('   Pilotage dimensions:');
Object.entries(sP).forEach(([k, v]) => {
  console.log(`     ${v.present ? '✓' : '✗'} ${k.padEnd(18)} — ${v.evidence}`);
});
console.log('   Towage dimensions:');
Object.entries(sT).forEach(([k, v]) => {
  console.log(`     ${v.present ? '✓' : '✗'} ${k.padEnd(18)} — ${v.evidence}`);
});
console.log('');

const total = RESULTS_LC6.length + RESULTS_LC7.length + RESULTS_LC8.length;
const totalPass = RESULTS_LC6.filter(r => r.passed).length
                + RESULTS_LC7.filter(r => r.passed).length
                + RESULTS_LC8.filter(r => r.passed).length;
console.log('═'.repeat(60));
console.log(`G7 totals: ${totalPass} pass / ${total - totalPass} fail / ${total} checks`);
console.log('═'.repeat(60));

// Same exit convention as G4/G5/G6.
process.exit(0);
