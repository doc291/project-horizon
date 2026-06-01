// G9 — Threshold scanner LC-12 + LC-13.
//
// This file supersedes the G3 threshold_scan.test.js stub.
//
// SCOPE
// -----
//   LC-12: No hard-coded numeric threshold may gate stakeholder visibility
//          unless it is sourced from port_profile or explicitly listed in
//          docs/governance/APPROVED-THRESHOLDS.md with justification.
//
//   LC-13: Every approved threshold must have boundary-precision test
//          fixtures (value − ε, exact, value + ε) with documented
//          expected outcomes.
//
// METHOD
// ------
// Read each in-scope source file and run a regex scan for numeric
// comparison patterns against identifiers that participate in stakeholder
// visibility paths (loa, beam, draught, sog, wind, swell, visibility,
// hrs_to_eta, ukc_m, etc.) plus window-arithmetic patterns
// (now + N * 3600 * 1000 in JS).
//
// Each hit is classified:
//   APPROVED        — listed in APPROVED-THRESHOLDS.md allowlist
//   GOVERNANCE FAIL — hard-coded, gates visibility, not allowlisted
//   INFORMATIONAL   — numeric threshold but NOT a visibility gate
//
// LC-12 fails when any hit is GOVERNANCE FAIL.
// LC-13 fails when any APPROVED threshold lacks boundary fixtures under
// tests/lens_contract/thresholds/.
//
// CONSTRAINT
// ----------
// Read-only scan. Files are opened with fs.readFileSync. Nothing is
// written to any source under server.py, mst_scraper.py, index.html,
// aisstream_scraper.py, weather.py, or port_profiles.py.
// ──────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');

const ROOT = '/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon';

// ── In-scope source files ─────────────────────────────────────────────────
const SOURCES = [
  'server.py',
  'mst_scraper.py',
  'aisstream_scraper.py',
  'weather.py',
  'port_profiles.py',
  'index.html'
];

// ── Allowlist — synced manually with APPROVED-THRESHOLDS.md ──────────────
// Each entry is keyed by "file:line" (or "<MIGRATED:id>" for thresholds
// that have been removed from code entirely) and carries its allowlist
// status. Contract v1.1 allowlist: A1 (named-constant approval, fixtures
// pending) and B1 (migrated to port_profile data, fixtures present).
const ALLOWLIST = {
  'aisstream_scraper.py:153': {
    pattern: 'sog < IN_PORT_SOG_KTS',
    id: 'A1',
    status: 'APPROVED',
    note: 'Named constant IN_PORT_SOG_KTS (1.0 kt). AIS-domain stationary classification.',
    boundary_fixture: null  // LC-13: pending
  },
  // B1 has no file:line entry because the threshold no longer exists in
  // code — migrated to port_profile.towage_rule data at v1.1. Listed here
  // so LC-13 can validate its boundary-fixture coverage.
  '<MIGRATED:B1>': {
    pattern: 'representative towage-demand model (formerly loa > 200)',
    id: 'B1',
    status: 'APPROVED-MIGRATED',
    note: 'Threshold removed from code. Demand computed via mst_scraper._n_tugs_for(rule, vessel) from port_profile.towage_rule. Boundary fixtures: B1_pre/at/post_threshold.json.',
    boundary_fixture: 'B1_*'
  }
};

// ── Visibility-gate identifier set (used to classify hits) ────────────────
// Identifiers that participate in stakeholder-visibility paths. A numeric
// comparison against any of these is at minimum suspicious and must be
// either allowlisted or justified.
const VISIBILITY_IDS = [
  'loa', 'beam', 'draught', 'sog',
  'wind', 'swell', 'visibility', 'vis',
  'wind_speed_kts', 'swell_height_m',
  'hrs_to_eta', 'ukc_m', 'min_ukc_m', 'available_depth_m', 'vessel_draught_m',
  'eff_swell', 'eff_wind',
  'eta', 'etd', 'scheduled_time',
  'towage_required', 'pilotage_required'
];

// ── Scan patterns ─────────────────────────────────────────────────────────
// Python / JS comparison: <ident> <op> <number>
const COMPARE_RE = new RegExp(
  '(\\b(?:' + VISIBILITY_IDS.join('|') + ')\\b)\\s*([<>]=?|==|!=)\\s*([-+]?\\d+(?:\\.\\d+)?)',
  'g'
);
// JS window arithmetic — now + N * 3600 * 1000
const WINDOW_RE = /now\s*([+\-])\s*(\d+)\s*\*\s*3600\s*\*\s*1000/g;
// Skip patterns — comments, doc strings, schemas
function isComment(line){
  return /^\s*#/.test(line) || /^\s*\/\//.test(line) || /^\s*\*/.test(line);
}

// ── Run the scan ──────────────────────────────────────────────────────────
const HITS = [];

function classify(file, line, identifier, op, value, srcLine){
  // Lines that are documentation / comments → INFORMATIONAL
  if(isComment(srcLine)) return { category: 'INFORMATIONAL', reason: 'in comment' };

  // Known-non-visibility paths: fuel estimate, decision deadline display, etc.
  // Detect by line-CONTENT rather than file:line — line numbers shift as
  // refactors land. The LOA-tiered fuel estimate (APPROVED-THRESHOLDS.md
  // entry B8) is identified by the `base_fuel = ...` assignment pattern
  // regardless of which line it currently occupies.
  if(file === 'server.py' && /base_fuel\s*=/.test(srcLine)){
    return { category: 'INFORMATIONAL', reason: 'LOA-tiered fuel-cost estimate display; does not gate stakeholder visibility (APPROVED-THRESHOLDS.md B8)' };
  }

  // Allowlist hits
  const key = `${file}:${line}`;
  if(ALLOWLIST[key]){
    return { category: 'APPROVED', reason: ALLOWLIST[key].note, allowlistId: ALLOWLIST[key].id };
  }

  // Everything else gating visibility → GOVERNANCE FAIL
  return { category: 'GOVERNANCE_FAIL', reason: 'hard-coded numeric threshold against a visibility-gating identifier; not in APPROVED-THRESHOLDS.md' };
}

function classifyWindow(file, line, sign, hours, srcLine){
  if(isComment(srcLine)) return { category: 'INFORMATIONAL', reason: 'in comment' };
  // Window arithmetic in renderers gates Section B/C inclusion → governance.
  // Currently no allowlist entry for window arithmetic at G9 v1.0.
  return {
    category: 'GOVERNANCE_FAIL',
    reason: `window arithmetic (${sign}${hours}h) inside renderer / data layer; magic number embedded in code rather than referenced from a named operational constant`
  };
}

function scanFile(rel){
  const abs = path.join(ROOT, rel);
  const text = fs.readFileSync(abs, 'utf-8');
  const lines = text.split('\n');

  for(let i = 0; i < lines.length; i++){
    const line = lines[i];
    const lineNo = i + 1;
    // Identifier comparison hits
    COMPARE_RE.lastIndex = 0;
    let m;
    while((m = COMPARE_RE.exec(line)) !== null){
      const [match, ident, op, val] = m;
      const verdict = classify(rel, lineNo, ident, op, val, line);
      HITS.push({
        file: rel,
        line: lineNo,
        match: match.trim(),
        kind: 'compare',
        identifier: ident,
        op,
        value: parseFloat(val),
        srcLine: line.trim().slice(0, 100),
        ...verdict
      });
    }
    // Window arithmetic hits (JS only)
    if(rel.endsWith('.html')){
      WINDOW_RE.lastIndex = 0;
      let w;
      while((w = WINDOW_RE.exec(line)) !== null){
        const [match, sign, hours] = w;
        const verdict = classifyWindow(rel, lineNo, sign, hours, line);
        HITS.push({
          file: rel,
          line: lineNo,
          match: match.trim(),
          kind: 'window',
          identifier: 'now',
          op: sign,
          value: parseInt(hours, 10),
          srcLine: line.trim().slice(0, 100),
          ...verdict
        });
      }
    }
  }
}

console.log('=== G9 — Threshold scan LC-12 / LC-13 ===');
console.log('');
console.log(`Scanning ${SOURCES.length} source files for hard-coded numeric thresholds...`);
console.log('');

SOURCES.forEach(scanFile);

// ── LC-12 report ──────────────────────────────────────────────────────────
const fails  = HITS.filter(h => h.category === 'GOVERNANCE_FAIL');
const approved = HITS.filter(h => h.category === 'APPROVED');
const informational = HITS.filter(h => h.category === 'INFORMATIONAL');

console.log('── LC-12 (hard-coded thresholds gating stakeholder visibility) ──');
console.log(`   ${fails.length === 0 ? 'PASS' : 'FAIL'}: ${fails.length} governance failure(s) detected`);
console.log(`   ${approved.length} approved threshold(s) on the allowlist`);
console.log(`   ${informational.length} informational hit(s) (not visibility gates)`);
console.log('');

if(fails.length > 0){
  console.log('   GOVERNANCE FAILURES:');
  fails.forEach(h => {
    console.log(`     ${h.file}:${h.line}  ${h.kind === 'window' ? `now${h.op}${h.value}h window` : `${h.identifier} ${h.op} ${h.value}`}`);
    console.log(`        ${h.reason}`);
    console.log(`        SRC: ${h.srcLine}`);
  });
  console.log('');
}

if(approved.length > 0){
  console.log('   APPROVED:');
  approved.forEach(h => {
    console.log(`     ${h.file}:${h.line}  ${h.identifier} ${h.op} ${h.value}  [${h.allowlistId || '—'}]`);
    console.log(`        ${h.reason}`);
  });
  console.log('');
}

if(informational.length > 0){
  console.log('   INFORMATIONAL:');
  informational.forEach(h => {
    console.log(`     ${h.file}:${h.line}  ${h.identifier} ${h.op} ${h.value}`);
    console.log(`        ${h.reason}`);
  });
  console.log('');
}

// ── LC-13 report (boundary-fixture coverage of approved thresholds) ──────
console.log('── LC-13 (approved thresholds must have boundary fixtures) ──');
const thresholdsDir = path.join(__dirname, 'thresholds');
const hasFixtureDir = fs.existsSync(thresholdsDir);
const fixtureFiles = hasFixtureDir
  ? fs.readdirSync(thresholdsDir).filter(f => f.endsWith('.json'))
  : [];

const approvedNeedingFixtures = Object.entries(ALLOWLIST).map(([key, info]) => {
  // A fixture is considered to exist if a file named <allowlistId>_*.json
  // exists in the thresholds directory.
  const required = new RegExp(`^${info.id}_`, 'i');
  const present = fixtureFiles.filter(f => required.test(f));
  return {
    key,
    id: info.id,
    boundaryFixturesFound: present,
    passed: present.length >= 3  // at least 3 boundary fixtures expected
  };
});

const lc13Pass = approvedNeedingFixtures.filter(r => r.passed).length;
const lc13Fail = approvedNeedingFixtures.length - lc13Pass;
console.log(`   ${lc13Fail === 0 ? 'PASS' : 'FAIL'}: ${lc13Pass} / ${approvedNeedingFixtures.length} approved threshold(s) have boundary fixtures`);

if(!hasFixtureDir){
  console.log(`   (tests/lens_contract/thresholds/ directory does not exist — no boundary fixtures created yet)`);
}

approvedNeedingFixtures.forEach(r => {
  const tag = r.passed ? 'PASS' : 'FAIL';
  console.log(`   [${tag}] ${r.id} (${r.key}) — fixtures found: ${r.boundaryFixturesFound.length} (expected ≥ 3)`);
});

// ── Boundary-fixture INVENTORY + validation (R4.2) ────────────────────────
// Pending-allowlist thresholds may carry boundary fixtures before the
// threshold migrates to the Approved Allowlist. This block inventories
// every fixture under tests/lens_contract/thresholds/ and validates B1
// fixtures (representative towage-demand model) by invoking the Python
// helper `_n_tugs_for` against the Melbourne towage_rule. A failure here
// indicates the helper produces a result inconsistent with the fixture's
// documented `expected.n_tugs`.
//
// B1 fixtures are NOT yet on the Approved Allowlist (that promotion
// happens at R4.6); the inventory check is informational at R4.2 and
// does not affect LC-12 / LC-13 pass/fail totals reported above.
console.log('');
console.log('── Boundary-fixture inventory + B1 validation (R4.2) ──');
const allFixtureFiles = hasFixtureDir
  ? fs.readdirSync(thresholdsDir).filter(f => f.endsWith('.json')).sort()
  : [];
const b1Files = allFixtureFiles.filter(f => /^B1_/.test(f));
console.log(`   Inventory: ${allFixtureFiles.length} fixture file(s) under thresholds/`);
allFixtureFiles.forEach(f => console.log(`     - ${f}`));

const { spawnSync } = require('child_process');
const PROJECT_ROOT = path.resolve(__dirname, '..', '..');
function runHelperOnFixture(fixturePath){
  const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));
  const vessel = fixture.vessel || {};
  const pyScript = `
import sys, json
sys.path.insert(0, ${JSON.stringify(PROJECT_ROOT)})
from mst_scraper import _n_tugs_for
from port_profiles import PORT_PROFILES
rule = PORT_PROFILES["MELBOURNE"]["towage_rule"]
vessel = json.loads(${JSON.stringify(JSON.stringify(vessel))})
result = _n_tugs_for(rule, vessel)
print(json.dumps({"n_tugs": result}))
`;
  const out = spawnSync('python3', ['-c', pyScript], { encoding: 'utf-8' });
  if(out.status !== 0){
    return { error: out.stderr || 'python3 exited non-zero', fixture };
  }
  try {
    const parsed = JSON.parse((out.stdout || '').trim());
    return { actual: parsed.n_tugs, expected: (fixture.expected || {}).n_tugs, fixture };
  } catch(e){
    return { error: `parse error: ${e.message}; raw=${out.stdout}`, fixture };
  }
}

if(b1Files.length === 0){
  console.log('   B1 validation: (no B1_*.json fixtures present)');
} else {
  console.log(`   B1 validation: ${b1Files.length} fixture(s) — Melbourne representative towage_rule`);
  let b1Pass = 0, b1Fail = 0;
  b1Files.forEach(f => {
    const result = runHelperOnFixture(path.join(thresholdsDir, f));
    if(result.error){
      console.log(`     [FAIL] ${f.padEnd(28)} — error: ${result.error.split('\n')[0]}`);
      b1Fail++;
    } else if(result.actual === result.expected){
      console.log(`     [PASS] ${f.padEnd(28)} — helper produced n_tugs=${result.actual} (expected ${result.expected})`);
      b1Pass++;
    } else {
      console.log(`     [FAIL] ${f.padEnd(28)} — helper produced n_tugs=${result.actual}, expected ${result.expected}`);
      b1Fail++;
    }
  });
  console.log(`   B1 summary: ${b1Pass} pass / ${b1Fail} fail / ${b1Files.length} B1 fixtures`);
  console.log('   NOTE: B1 (loa > 200) is NOT yet on the Approved Allowlist. Validation here');
  console.log('         is informational at R4.2 and does not change the LC-12/LC-13 totals.');
  console.log('         B1 promotion to the Allowlist happens at R4.6 after helper wiring.');
}

// ── Explicit canary check: loa > 200 ──────────────────────────────────────
console.log('');
console.log('── Canary case: `loa > 200` (towage_required) ──');
const loaHits = HITS.filter(h => h.identifier === 'loa' && h.op === '>' && h.value === 200);
if(loaHits.length === 0){
  console.log('   NOT DETECTED — scanner missed the canary case (regex / file-set bug)');
} else {
  loaHits.forEach(h => {
    console.log(`   ${h.category === 'GOVERNANCE_FAIL' ? '❌' : '✓ '} ${h.file}:${h.line}  ${h.match}`);
    console.log(`        category: ${h.category}`);
    console.log(`        reason:   ${h.reason}`);
  });
}
console.log('');

const totalFails = fails.length + lc13Fail;
const totalChecks = HITS.length + approvedNeedingFixtures.length;
const totalPass   = totalChecks - totalFails;
console.log('═'.repeat(60));
console.log(`G9 totals: ${totalPass} pass / ${totalFails} fail / ${totalChecks} checks`);
console.log('═'.repeat(60));

// Baseline-not-gate convention.
process.exit(0);
