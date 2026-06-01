// P5-P7 acceptance harness — render PILOTAGE watch for all four ports and
// the Melbourne+decision state; assert against doctrine constraints.
const fs = require('fs');
const path = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const html = fs.readFileSync(path, 'utf-8');

// Extract the whole Pilotage data layer + renderer block from the IIFE.
// Boundary markers: from the Pilotage data-layer header through to (not
// including) the Towage renderer header.
const startMarker = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
const endMarker = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
const si = html.indexOf(startMarker), ei = html.indexOf(endMarker, si);
if (si < 0 || ei < 0) { console.error('Marker extraction failed'); process.exit(1); }
const block = html.slice(si, ei);

// We need _maxSeverity / _SHIFT_SEV_RANK from the Towage data layer; pull them too.
const towStart = '// T1: pure data transform — fleet + bookings in window.';
const towEnd   = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
const tsi = html.indexOf(towStart), tei = html.indexOf(towEnd, tsi);
const towBlock = html.slice(tsi, tei);
// Extract only the _SHIFT_SEV_RANK + _maxSeverity definitions (no buildTowageShift).
// They sit just above buildTowageShift in the file; we can take the block before
// 'function buildTowageShift'.
const sharedHelpers = `
  const _SHIFT_SEV_RANK = { advisory: 1, warning: 2, critical: 3 };
  function _maxSeverity(a, b){
    if(!a) return b;
    if(!b) return a;
    return (_SHIFT_SEV_RANK[a] || 0) >= (_SHIFT_SEV_RANK[b] || 0) ? a : b;
  }
`;

const harnessSrc = `
  function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
  function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
${sharedHelpers}
${block}
  return { buildPilotageWatch, renderPilotageWatch };
`;
const ctx = new Function(harnessSrc)();

const RESULTS = [];
function check(criterion, label, passed, detail){ RESULTS.push({criterion, label, passed, detail}); }
function load(p){ return JSON.parse(fs.readFileSync(p, 'utf-8')); }
function rend(p){ return ctx.renderPilotageWatch(ctx.buildPilotageWatch(load(p))); }

const allFixtures = [
  ['BRISBANE',          '/tmp/p567_BRISBANE.json'],
  ['MELBOURNE',         '/tmp/p567_MELBOURNE.json'],
  ['GEELONG',           '/tmp/p567_GEELONG.json'],
  ['DARWIN',            '/tmp/p567_DARWIN.json'],
  ['MELBOURNE_dec',     '/tmp/p567_MELBOURNE_dec.json'],
];

// CRITERION 1 — All four ports render cleanly
console.log('=== CRITERION 1: All four ports + decision state render ===');
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  const ok = out.indexOf('Pilots available this watch') >= 0
          && out.indexOf('My transits') >= 0
          && out.indexOf('Next 8 hours') >= 0;
  check(1, `${label}: pwc-watch render`, ok, `length=${out.length} chars`);
});

// CRITERION 2 — Synthetic pilot IDs absent from rendered output
console.log('\n=== CRITERION 2: No synthetic pilot IDs in rendered output ===');
const synthPattern = /PILOT_[A-Z]{3}_PSP/g;
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  const m = out.match(synthPattern) || [];
  check(2, `${label}: no PILOT_XXX_PSP IDs`, m.length === 0, `matches=${m.length}`);
});

// CRITERION 3 — No Horizon-as-actor language in rendered output
console.log('\n=== CRITERION 3: No Horizon-as-actor language ===');
const prohibited = ['Morning Brief','Horizon disruption signal','Recommended review','What changed','Commercial consequence','detected by Horizon','Horizon is watching','Horizon is flagging'];
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  prohibited.forEach(s => {
    check(3, `${label}: "${s}" absent`, out.indexOf(s) < 0, '');
  });
});

// CRITERION 4 — No commercial figures in rendered output
console.log('\n=== CRITERION 4: No commercial figures ===');
const commercial = ['A$650,000','A$25,000/hr','A$650000','total_impact_aud','cost_per_hour_aud','650,000'];
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  commercial.forEach(s => check(4, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// CRITERION 5 — No qualifications / fatigue / rest / chain language
console.log('\n=== CRITERION 5: No fabricated qualification/fatigue language ===');
const fabrications = ['Class 1','Class 2','qualified','rated for','endorsed','fatigue','rested','duty hours','mandatory rest','dependency chain','cascades to','blocks the next'];
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  fabrications.forEach(s => check(5, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// CRITERION 6 — Pilots available is COUNT only (no enumeration)
console.log('\n=== CRITERION 6: Layer 1 is count only ===');
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  const hasCount = /class="pwc-pilots-count">\d+/.test(out) || /class="pwc-pilots-count">—/.test(out);
  const hasNoteAboutRoster = out.indexOf('sit in your roster system') >= 0;
  check(6, `${label}: pilots-count card present`, hasCount, '');
  check(6, `${label}: companion-note about roster present`, hasNoteAboutRoster, '');
});

// CRITERION 7 — Stage 2 as Layer 5 exception (Melbourne+decision)
console.log('\n=== CRITERION 7: Stage 2 as Layer 5 exception ===');
const melDec = rend('/tmp/p567_MELBOURNE_dec.json');
const c7 = {
  hasExceptionBlock:        melDec.indexOf('pwc-ex-coordinated') >= 0,
  hasCoordinatedLabel:      melDec.indexOf('Coordinated action required') >= 0,
  hasActionLabel:           melDec.indexOf('Amend pilot boarding time to the new tidal window') >= 0,
  hasAckBtn:                melDec.indexOf('id="pwc-ack-btn"') >= 0,
  hasFlagBtn:               melDec.indexOf('id="pwc-flag-btn"') >= 0,
  excAboveCount:            melDec.indexOf('pwc-ex-coordinated') < melDec.indexOf('Pilots available this watch'),
};
Object.entries(c7).forEach(([k, v]) => check(7, k, v, ''));

// CRITERION 8 — UKC signal renders correctly for Geelong (real ukc_unsafe data)
console.log('\n=== CRITERION 8: UKC signal renders (Geelong has live ukc_unsafe) ===');
const gee = rend('/tmp/p567_GEELONG.json');
const hasUnsafeFlag = gee.indexOf('UKC at predicted arrival') >= 0;
const hasWindowSnapshot = gee.indexOf('pwc-transit-window') >= 0;
check(8, 'Geelong: UKC unsafe message present', hasUnsafeFlag, '');
check(8, 'Geelong: window snapshot rendered for transits with arrival_ukc data', hasWindowSnapshot, '');

// CRITERION 9 — Darwin empty-state (Darwin has 7 transits — should render normally)
// (Towage had Darwin=0 jobs which exercised the empty path; for Pilotage,
//  Darwin has 7 transits so we exercise the populated path instead.)
console.log('\n=== CRITERION 9: Darwin renders all sections ===');
const dar = rend('/tmp/p567_DARWIN.json');
check(9, 'Darwin: My transits populated', dar.indexOf('class="pwc-transits-list"') >= 0, '');
check(9, 'Darwin: forward slots rendered', dar.indexOf('class="pwc-slot"') >= 0, '');
check(9, 'Darwin: no JS error (output > 1KB)', dar.length > 1000, `length=${dar.length}`);

// CRITERION 10 — Per-transit flag inline (no standalone at-risk section)
console.log('\n=== CRITERION 10: Confidence inline on transits, not standalone ===');
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  const noStandalone = out.indexOf('At-risk transits') < 0 && out.indexOf('At risk transits') < 0;
  check(10, `${label}: no standalone at-risk section`, noStandalone, '');
});
const mel = rend('/tmp/p567_MELBOURNE.json');
const inlineFlags = (mel.match(/class="pwc-transit-flagged/g) || []).length;
const flagPills   = (mel.match(/class="pwc-transit-flag pwc-flag-pill-/g) || []).length;
check(10, `Melbourne: inline flagged transits`, inlineFlags > 0, `count=${inlineFlags}`);
check(10, `Melbourne: inline flag-severity pills`, flagPills > 0, `count=${flagPills}`);

// CRITERION 11 — No "replaces the roster" framing
console.log('\n=== CRITERION 11: Horizon positioned as companion, not replacement ===');
const replaceFraming = ['replaces your roster','replacement for your roster','instead of your roster'];
allFixtures.forEach(([label, path]) => {
  const out = rend(path);
  replaceFraming.forEach(s => check(11, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// Final tally
console.log('\n=== ACCEPTANCE TOTALS ===');
const grouped = {};
RESULTS.forEach(r => {
  if(!grouped[r.criterion]) grouped[r.criterion] = [];
  grouped[r.criterion].push(r);
});
let totalPass = 0, totalFail = 0;
Object.keys(grouped).sort((a,b)=>+a-+b).forEach(k => {
  const list = grouped[k];
  const pass = list.filter(r => r.passed).length;
  const fail = list.length - pass;
  totalPass += pass; totalFail += fail;
  console.log(`Criterion ${k}: ${pass}/${list.length} pass${fail?' — FAILURES:':''}`);
  list.forEach(r => { if(!r.passed) console.log(`  FAIL [${r.label}] — ${r.detail}`); });
});
console.log(`\nTOTAL: ${totalPass} pass / ${totalFail} fail / ${totalPass+totalFail} checks`);
process.exit(totalFail > 0 ? 1 : 0);
