// WO1-WO4 acceptance harness — verifies the Pilotage Watch Overview against
// the 15 governing constraints + posture-change report per row.
const fs = require('fs');
const path = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html';
const html = fs.readFileSync(path, 'utf-8');

// Extract Pilotage data + renderer block (markers unchanged from WO0).
const startMarker = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
const endMarker = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
const si = html.indexOf(startMarker), ei = html.indexOf(endMarker, si);
if (si < 0 || ei < 0) { console.error('Marker extraction failed'); process.exit(1); }
const block = html.slice(si, ei);

const harnessSrc = `
  function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
  function fmtTs(ts){ try{ return new Date(ts).toISOString().slice(11,16); }catch(e){ return String(ts); } }
  const _SHIFT_SEV_RANK = { advisory: 1, warning: 2, critical: 3 };
  function _maxSeverity(a, b){ if(!a) return b; if(!b) return a; return (_SHIFT_SEV_RANK[a] || 0) >= (_SHIFT_SEV_RANK[b] || 0) ? a : b; }
${block}
  return { buildPilotageWatch, renderPilotageWatch };
`;
const ctx = new Function(harnessSrc)();

const RESULTS = [];
function check(criterion, label, passed, detail){ RESULTS.push({criterion, label, passed, detail}); }
function load(p){ return JSON.parse(fs.readFileSync(p, 'utf-8')); }
function rend(p){ return ctx.renderPilotageWatch(ctx.buildPilotageWatch(load(p))); }
function build(p){ return ctx.buildPilotageWatch(load(p)); }

const F = [
  ['BRISBANE',          '/tmp/wo_BRISBANE.json'],
  ['MELBOURNE',         '/tmp/wo_MELBOURNE.json'],
  ['GEELONG',           '/tmp/wo_GEELONG.json'],
  ['DARWIN',            '/tmp/wo_DARWIN.json'],
  ['MELBOURNE_dec',     '/tmp/wo_MELBOURNE_dec.json'],
];

// --- Acceptance checks ---

// 1. Old pilot card classes absent from source and rendered output
F.forEach(([label, path]) => {
  const out = rend(path);
  ['pwc-pilots-card','pwc-pilots-count','pwc-pilots-label','pwc-pilots-note'].forEach(c => {
    check(1, `${label}: old class "${c}" absent from render`, out.indexOf(c) < 0, '');
  });
});
['pwc-pilots-card','pwc-pilots-count','pwc-pilots-label','pwc-pilots-note'].forEach(c => {
  check(1, `Source: old class "${c}" absent`, html.indexOf(c) < 0, '');
});

// 2. Watch Overview present
F.forEach(([label, path]) => {
  const out = rend(path);
  check(2, `${label}: pwc-overview present`, out.indexOf('pwc-overview') >= 0, '');
});

// 3. Demand headline well-formed
F.forEach(([label, path]) => {
  const out = rend(path);
  const hasCount = /\d+ transits? this watch/.test(out);
  const hasSplit = /\d+ inbound · \d+ outbound/.test(out);
  const hasNext  = /(next|now): [^<]+ at \d{2}:\d{2}/.test(out);
  check(3, `${label}: "{N} transit(s) this watch"`, hasCount, '');
  check(3, `${label}: "{I} inbound · {O} outbound"`, hasSplit, '');
  check(3, `${label}: "next:|now: <vessel> at HH:MM"`, hasNext, '');
});

// 4. Window pressure conditional behaviour
F.forEach(([label, path]) => {
  const w = build(path);
  const hasUkcFlagged = (w.transits || []).some(t => (t.flag_reasons||[]).some(r => r.source === 'ukc_tight' || r.source === 'ukc_unsafe'));
  const out = rend(path);
  const hasUkcRow = /pwc-ov-pressure[^"]*">\d+ tight UKC margin/.test(out);
  if (hasUkcFlagged) {
    check(4, `${label}: has UKC flag → window-pressure row PRESENT`, hasUkcRow, '');
  } else {
    check(4, `${label}: no UKC flag → window-pressure row HIDDEN`, !hasUkcRow, '');
  }
});

// 5. Short-notice conditional behaviour
F.forEach(([label, path]) => {
  const w = build(path);
  const hasShort = (w.transits || []).some(t => (t.flag_reasons||[]).some(r => r.source === 'short_notice'));
  const out = rend(path);
  const hasShortRow = /pwc-ov-pressure[^"]*">\d+ short-notice request/.test(out);
  if (hasShort) {
    check(5, `${label}: has short-notice → short-notice row PRESENT`, hasShortRow, '');
  } else {
    check(5, `${label}: no short-notice → short-notice row HIDDEN`, !hasShortRow, '');
  }
});

// 6. Companion-system note present in all fixtures
F.forEach(([label, path]) => {
  const out = rend(path);
  const hasNote = out.indexOf('Roster reference:') >= 0;
  const hasWording = out.indexOf('The roster holds allocation, qualifications and fatigue') >= 0
                  || out.indexOf('pilot duty status lives in your roster') >= 0;
  check(6, `${label}: "Roster reference:" present`, hasNote, '');
  check(6, `${label}: companion-system wording present`, hasWording, '');
});

// 7. Synthetic pilot IDs absent
F.forEach(([label, path]) => {
  const out = rend(path);
  const m = out.match(/PILOT_[A-Z]{3}_PSP/g) || [];
  check(7, `${label}: no synthetic pilot IDs`, m.length === 0, `count=${m.length}`);
});

// 8. The word "fatigue" appears ONLY inside the companion-system note
F.forEach(([label, path]) => {
  const out = rend(path);
  // Strip the companion-note phrase and check no other "fatigue"
  const stripped = out.replace(/The roster holds allocation, qualifications and fatigue\./g, '');
  const stripped2 = stripped.replace(/pilot duty status lives in your roster\./g, '');
  const elsewhere = stripped2.indexOf('fatigue') >= 0;
  check(8, `${label}: "fatigue" only inside companion note`, !elsewhere, '');
});

// 9. No Horizon-as-actor language
const prohibited = ['Morning Brief','Horizon disruption signal','Recommended review','What changed','detected by Horizon','Horizon is watching','Horizon is flagging','Horizon says'];
F.forEach(([label, path]) => {
  const out = rend(path);
  prohibited.forEach(s => check(9, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// 10. No commercial figures
const commercial = ['A$650,000','A$25,000/hr','A$650000','total_impact_aud','cost_per_hour_aud','650,000','Commercial consequence'];
F.forEach(([label, path]) => {
  const out = rend(path);
  commercial.forEach(s => check(10, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// 11. No recommendation language
const recWords = ['Recommended','recommend','recommendation','we suggest','our recommendation','suggested action'];
F.forEach(([label, path]) => {
  const out = rend(path);
  recWords.forEach(s => check(11, `${label}: "${s}" absent`, out.indexOf(s) < 0, ''));
});

// 12. Layer 5 exception renders above Watch Overview (Melbourne+decision)
const melDec = rend('/tmp/wo_MELBOURNE_dec.json');
const exIdx = melDec.indexOf('pwc-ex-coordinated');
const ovIdx = melDec.indexOf('pwc-overview');
check(12, 'Melbourne+dec: coordinated_decision exception PRESENT', exIdx >= 0, '');
check(12, 'Melbourne+dec: exception ABOVE Watch Overview', exIdx >= 0 && exIdx < ovIdx, `ex@${exIdx} ov@${ovIdx}`);
check(12, 'Melbourne+dec: pwc-ack-btn / pwc-flag-btn still present',
  melDec.indexOf('id="pwc-ack-btn"') >= 0 && melDec.indexOf('id="pwc-flag-btn"') >= 0, '');

// 13. Towage My Shift unchanged — source-level invariant
const tmsFns = ['function renderTowageShift','function buildTowageShift','function _tmsRenderFleet','function _tmsRenderJobs','function _tmsRenderShiftLoad'];
tmsFns.forEach(fn => check(13, `Source: ${fn} present`, html.indexOf(fn) >= 0, ''));
const tmsClasses = ['.tms-shift','.tms-tug','.tms-job','.tms-slot'];
tmsClasses.forEach(c => check(13, `Source: ${c} present`, html.indexOf(c) >= 0, ''));

// 14. Terminal / Assurance / VTSO unchanged — source-level invariant
const otherFns = ['function assuranceBody','function renderVtsoPanel','function b11DecisionContext','function b11CoordStatus','function b11RoleGuidance'];
otherFns.forEach(fn => check(14, `Source: ${fn} present`, html.indexOf(fn) >= 0, ''));
// Confirm TERMINAL branch in b11RoleGuidance still present
check(14, 'Source: b11RoleGuidance TERMINAL branch present', /if\(role==="TERMINAL"\)/.test(html), '');

// --- Posture-change report per row ---
// For each row, describe how the scheduler's posture would change if it changed.
const POSTURE = {};
F.forEach(([label, path]) => {
  const w = build(path);
  const total = (w.forward && w.forward.transit_count != null) ? w.forward.transit_count : (w.transits || []).length;
  const inbound = (w.transits || []).filter(t => t.direction === 'inbound').length;
  const outbound = (w.transits || []).filter(t => t.direction === 'outbound').length;
  const ukcAffected = (w.transits || []).filter(t => (t.flag_reasons||[]).some(r => r.source === 'ukc_tight' || r.source === 'ukc_unsafe'));
  const shortAffected = (w.transits || []).filter(t => (t.flag_reasons||[]).some(r => r.source === 'short_notice'));
  const breach = (w.forward && w.forward.environment && w.forward.environment.weather && w.forward.environment.weather.threshold_breach) ? true : false;
  POSTURE[label] = {
    total, inbound, outbound,
    ukc_affected: ukcAffected.length,
    ukc_worst_vessel: ukcAffected.length > 0 ?
      ukcAffected.reduce((m, t) => (m === null || (t.window && t.window.ukc_m < m.window.ukc_m)) ? t : m, null).vessel_name : null,
    short_affected: shortAffected.length,
    pilots: w.pilots_available,
    breach: breach,
    tide_state: w.forward && w.forward.environment && w.forward.environment.tide ? w.forward.environment.tide.state : null,
    conditions: w.forward && w.forward.environment && w.forward.environment.weather ? w.forward.environment.weather.conditions : null,
  };
});

// --- Output ---
console.log('=== WO ACCEPTANCE RESULTS ===\n');
const grouped = {};
RESULTS.forEach(r => { if(!grouped[r.criterion]) grouped[r.criterion] = []; grouped[r.criterion].push(r); });
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

console.log('\n=== PER-FIXTURE POSTURE-CHANGE EVIDENCE ===');
Object.entries(POSTURE).forEach(([label, p]) => {
  console.log(`\n--- ${label} ---`);
  console.log(`  Row 1 — Demand: ${p.total} transits, ${p.inbound} in / ${p.outbound} out`);
  console.log(`  Row 2 — Window pressure: ${p.ukc_affected > 0 ? p.ukc_affected + " affected (worst: " + p.ukc_worst_vessel + ")" : "HIDDEN (no UKC flags)"}`);
  console.log(`  Row 3 — Short-notice:    ${p.short_affected > 0 ? p.short_affected + " affected" : "HIDDEN"}`);
  console.log(`  Row 4 — Environment: tide=${p.tide_state}  conditions=${p.conditions}  breach=${p.breach}`);
  console.log(`  Row 5 — Roster reference: ${p.pilots != null ? p.pilots + ' pilots' : 'null (fallback wording)'}`);
});

console.log('\n=== SAMPLE RENDERS ===');
['MELBOURNE','GEELONG'].forEach(label => {
  const out = rend(`/tmp/wo_${label}.json`);
  // Extract the Watch Overview section only for readability
  const ovStart = out.indexOf('pwc-overview');
  const ovEndContext = out.indexOf('pwc-section', ovStart + 1); // next section starts here
  const slice = out.slice(out.lastIndexOf('<div', ovStart), ovEndContext);
  console.log(`\n--- ${label} Watch Overview HTML ---`);
  console.log(slice.replace(/></g, '>\n<'));
});

process.exit(totalFail > 0 ? 1 : 0);
