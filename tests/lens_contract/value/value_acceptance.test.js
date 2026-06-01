// LV0 — Value-acceptance harness.
//
// PURPOSE
// -------
// The LC-1..LC-11 contract harness measures STRUCTURE (four sections present,
// Section A independent of B, Section C strictly later, vessels addressable).
// A lens can pass all of that and still be operationally thin — a list of rows
// with no lead, no will-it-hold verdict, no derived pressure, no clash
// detection. That thinness is the regression documented in the Lens
// Regression Investigation.
//
// This harness measures VALUE. Each check looks for a specific operational
// MARKER that a later LV phase will introduce:
//
//   VAL-P1  pwc-lead          next/at-risk transit floated to a lead block   (LV1)
//   VAL-P2  pwc-verdict       per-transit will-it-hold verdict line          (LV1)
//   VAL-P3  pwc-lead salience at-risk transit is the lead (pinned)           (LV1)
//   VAL-P4  pwc-pressure      Section C derived forward-pressure statements  (LV2)
//   VAL-P5  pwc-shape         Section A watch-shape headline                 (LV1)
//   VAL-T1  tms-lead          next job lead block incl. tug availability     (LV3)
//   VAL-T2  tms-clash         capacity-clash present on clash fixture only   (LV3)
//   VAL-T3  tms-tug-return    down-tug rendered with return/impact           (LV3)
//   VAL-T4  tms-fwd-demand    Section C demand-vs-capacity forward curve      (LV4)
//   VAL-T5  tms-shape         Section A shift-shape headline                 (LV3)
//   VAL-X1  (manual)          stakeholder read-aloud signoff                 (LV5)
//   VAL-X2  (manual)          distinctness-from-dashboard written assertion  (LV5)
//
// At LV0 every automated check is expected to FAIL RED — the markers do not
// exist yet. That RED baseline is the objective measure of the regression.
// Each later phase turns its slice GREEN. The marker class names below are
// the contract between this harness and the LV phases: each phase MUST emit
// its named marker.
//
// EXIT CONVENTION
// ---------------
// Exit 0 (baseline mode), consistent with the lc_*.test.js harnesses, so the
// suite stays green while the value findings are reported inline. This harness
// is run as a SIBLING at LV0 (not wired into run_all.js) per the LV0 brief.
// A future phase may flip it to a hard gate once all checks are GREEN.
//
// TIMESTAMP REBASE
// ----------------
// Fixtures author timestamps relative to their `generated_at` anchor. The
// build functions use Date.now() for window math. Before building, this
// harness shifts every ISO-8601 timestamp in the fixture by
// (now - generated_at) so engineered conditions land inside the live
// 8h watch / 12h shift / 24h forward windows regardless of run time.
//
// TEST-ONLY. Reads index.html read-only; never modifies any runtime file.
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..', '..', '..');
const INDEX_HTML = path.join(ROOT, 'index.html');
const FIXDIR = path.join(ROOT, 'tests', 'lens_contract', 'fixtures', 'value');
const html = fs.readFileSync(INDEX_HTML, 'utf-8');

// ── Extract build/render blocks (same slicing as lc_*.test.js) ─────────────
function extractPilotage() {
  const s = '// ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const e = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  return html.slice(html.indexOf(s), html.indexOf(e));
}
function extractTowage() {
  const ds = '  let _prevConflictIds = null;';
  const de = '  // ════════════════════════════════════════════════════════════════════════\n  // Pilotage Window Confidence — data layer';
  const data = html.slice(html.indexOf(ds), html.indexOf(de));
  const rs = '  // ════════════════════════════════════════════════════════════════════════\n  // renderTowageShift(shift) -> HTML string.';
  const re = '  function onData(d){';
  return data + '\n' + html.slice(html.indexOf(rs), html.indexOf(re));
}
function compile() {
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
const ctx = compile();

// ── Runtime timestamp rebase ───────────────────────────────────────────────
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;
function rebase(obj, deltaMs) {
  if (typeof obj === 'string') {
    if (ISO_RE.test(obj)) {
      const shifted = new Date(new Date(obj).getTime() + deltaMs);
      return shifted.toISOString().replace(/\.\d{3}Z$/, 'Z');
    }
    return obj;
  }
  if (Array.isArray(obj)) return obj.map(x => rebase(x, deltaMs));
  if (obj && typeof obj === 'object') {
    const out = {};
    for (const k of Object.keys(obj)) out[k] = rebase(obj[k], deltaMs);
    return out;
  }
  return obj;
}
function loadFixture(name) {
  const raw = JSON.parse(fs.readFileSync(path.join(FIXDIR, name), 'utf-8'));
  const anchor = new Date(raw.generated_at).getTime();
  const delta = Date.now() - anchor;
  return rebase(raw, delta);
}

// ── Check harness ──────────────────────────────────────────────────────────
const RESULTS = [];
function record(id, phase, passed, detail) {
  RESULTS.push({ id, phase, passed, detail });
}
function pilotage(name) {
  const fx = loadFixture(name);
  return ctx.renderPilotageWatch(ctx.buildPilotageWatch(fx));
}
function towage(name) {
  const fx = loadFixture(name);
  return ctx.renderTowageShift(ctx.buildTowageShift(fx));
}
function safe(fn) {
  try { return { ok: true, out: fn() }; }
  catch (e) { return { ok: false, err: String(e && e.message || e) }; }
}

console.log('=== LV0 — Value-acceptance baseline ===');
console.log('Each automated check seeks an operational MARKER a later LV phase introduces.');
console.log('RED here is expected and is the objective measure of the regression.\n');

// ── Pilotage value checks ──────────────────────────────────────────────────
{
  const r = safe(() => pilotage('pil_tide_closing.json'));
  if (!r.ok) { record('VAL-P1', 'LV1', false, `render error: ${r.err}`); }
  else record('VAL-P1', 'LV1', /class="pwc-lead/.test(r.out),
    'next/at-risk transit floated to a lead block (marker .pwc-lead)');

  const r2 = safe(() => pilotage('pil_tide_closing.json'));
  record('VAL-P2', 'LV1', r2.ok && /class="pwc-verdict/.test(r2.out),
    'per-transit will-it-hold verdict line (marker .pwc-verdict)');

  // VAL-P3 salience: the at-risk vessel ("TIDE RUNNER", the only TIGHT transit)
  // should be marked as the lead. Pinned-first proven by lead block carrying it.
  const r3 = safe(() => pilotage('pil_tide_closing.json'));
  const p3 = r3.ok && /class="pwc-lead[^"]*"[^>]*>[\s\S]*?TIDE RUNNER/.test(r3.out);
  record('VAL-P3', 'LV1', p3,
    'at-risk transit is pinned as the lead (TIDE RUNNER inside .pwc-lead)');

  // VAL-P4 forward pressure derived: pil_tide_closing (tide-window) and
  // pil_cluster (stacking) must each yield a .pwc-pressure statement.
  const a = safe(() => pilotage('pil_tide_closing.json'));
  const b = safe(() => pilotage('pil_cluster.json'));
  const p4 = a.ok && b.ok && /class="pwc-pressure/.test(a.out) && /class="pwc-pressure/.test(b.out);
  record('VAL-P4', 'LV2', p4,
    'Section C derived forward-pressure statements (marker .pwc-pressure) on tide-closing AND cluster fixtures');

  const r5 = safe(() => pilotage('pil_cluster.json'));
  record('VAL-P5', 'LV1', r5.ok && /class="pwc-shape/.test(r5.out),
    'Section A watch-shape headline (marker .pwc-shape)');
}

// ── Towage value checks ────────────────────────────────────────────────────
{
  const r1 = safe(() => towage('tow_clash.json'));
  record('VAL-T1', 'LV3', r1.ok && /class="tms-lead/.test(r1.out),
    'next job lead block incl. tug availability (marker .tms-lead)');

  // VAL-T2 clash: present in tow_clash, ABSENT in tow_noclash.
  const clash = safe(() => towage('tow_clash.json'));
  const noclash = safe(() => towage('tow_noclash.json'));
  const presentInClash = clash.ok && /class="tms-clash/.test(clash.out);
  const absentInNoClash = noclash.ok && !/class="tms-clash/.test(noclash.out);
  record('VAL-T2', 'LV3', presentInClash && absentInNoClash,
    `capacity-clash present on clash fixture (${presentInClash}) AND absent on no-clash fixture (${absentInNoClash})`);

  const r3 = safe(() => towage('tow_downtug.json'));
  record('VAL-T3', 'LV3', r3.ok && /class="tms-tug-return/.test(r3.out),
    'down-tug rendered with return/impact (marker .tms-tug-return)');

  const r4 = safe(() => towage('tow_demand_peak.json'));
  record('VAL-T4', 'LV4', r4.ok && /class="tms-fwd-demand/.test(r4.out),
    'Section C demand-vs-capacity forward curve (marker .tms-fwd-demand)');

  const r5 = safe(() => towage('tow_clash.json'));
  record('VAL-T5', 'LV3', r5.ok && /class="tms-shape/.test(r5.out),
    'Section A shift-shape headline (marker .tms-shape)');
}

// ── Report ─────────────────────────────────────────────────────────────────
function line(r) {
  const tag = r.passed ? 'GREEN' : 'RED  ';
  console.log(`  [${tag}] ${r.id.padEnd(7)} (${r.phase}) — ${r.detail}`);
}
console.log('── Pilotage ─────────────────────────────────────────────────');
RESULTS.filter(r => r.id.startsWith('VAL-P')).forEach(line);
console.log('');
console.log('── Towage ───────────────────────────────────────────────────');
RESULTS.filter(r => r.id.startsWith('VAL-T')).forEach(line);
console.log('');
console.log('── Manual gates (not automated) ─────────────────────────────');
console.log('  [MANUAL] VAL-X1 (LV5) — stakeholder read-aloud: a Melbourne pilot and a tug');
console.log('           master answer the four 30-second questions using only the lens.');
console.log('           Signoff -> docs/governance/lens-signoffs/. PENDING.');
console.log('  [MANUAL] VAL-X2 (LV5) — distinctness: written assertion that each lens surfaces');
console.log('           >=2 operational facts the VTSO dashboard does NOT surface');
console.log('           stakeholder-side. PENDING.');
console.log('');

const green = RESULTS.filter(r => r.passed).length;
const red = RESULTS.length - green;
console.log('═'.repeat(62));
console.log(`LV0 baseline: ${green} GREEN / ${red} RED / ${RESULTS.length} automated checks`);
console.log(`Expected at LV0: 0 GREEN / ${RESULTS.length} RED (markers not yet implemented).`);
console.log('Manual gates VAL-X1, VAL-X2: PENDING (LV5).');
console.log('═'.repeat(62));

// Baseline mode — exit 0 so the suite stays green; findings reported inline.
process.exit(0);
