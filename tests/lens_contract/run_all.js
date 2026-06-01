// Lens-contract test runner.
//
// Spawns each *.test.js in turn, aggregates results, and exits non-zero if
// any test fails.
//
// G3 RESPONSIBILITY (this file): orchestration only. No assertions. No
// behavioural changes to the migrated harnesses. The migrated harnesses use
// the same /tmp/<prefix>_<port>.json fixture paths they used in their
// original /tmp location; this runner pre-populates /tmp from the
// source-controlled fixtures/ directory so the harness logic remains
// byte-identical to the /tmp originals.
//
// Future runners (G4+) may replace the /tmp shim with in-process fixture
// loading once the harnesses themselves are updated.

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const HERE = __dirname;
const FIXTURES_DIR = path.join(HERE, 'fixtures');
const TMP = '/tmp';

// ── Step 1: replicate fixtures into /tmp so byte-identical legacy harnesses
//            can read their hard-coded paths without modification. ──────────
function syncFixtures() {
  const files = fs.readdirSync(FIXTURES_DIR).filter(f => f.endsWith('.json'));
  let copied = 0;
  for (const f of files) {
    const src = path.join(FIXTURES_DIR, f);
    const dst = path.join(TMP, f);
    fs.copyFileSync(src, dst);
    copied++;
  }
  return copied;
}

// ── Step 2: discover *.test.js files in this directory (non-recursive). ────
function discoverTests() {
  return fs.readdirSync(HERE)
    .filter(f => f.endsWith('.test.js'))
    .sort()
    .map(f => path.join(HERE, f));
}

// ── Step 3: run each test, capture exit code + first 40 output lines. ──────
function runTest(file) {
  const start = Date.now();
  const result = spawnSync(process.execPath, [file], { encoding: 'utf-8' });
  const ms = Date.now() - start;
  return {
    file:    path.basename(file),
    code:    result.status,
    ms,
    stdout:  result.stdout || '',
    stderr:  result.stderr || '',
  };
}

// ── Main ───────────────────────────────────────────────────────────────────
(function main() {
  console.log('Lens-contract harness runner');
  console.log('─'.repeat(60));

  const copied = syncFixtures();
  console.log(`Replicated ${copied} fixture file(s) into ${TMP}/`);
  console.log('');

  const tests = discoverTests();
  if (tests.length === 0) {
    console.log('No *.test.js files found in', HERE);
    process.exit(1);
  }

  const results = tests.map(runTest);

  console.log('');
  console.log('─'.repeat(60));
  console.log('Summary');
  console.log('─'.repeat(60));

  let pass = 0, fail = 0;
  for (const r of results) {
    const status = r.code === 0 ? 'PASS' : `FAIL (exit ${r.code})`;
    console.log(`  ${r.file.padEnd(36)} ${status.padEnd(16)} ${r.ms}ms`);
    if (r.code === 0) pass++; else fail++;
  }

  console.log('');
  console.log(`Total: ${pass} pass / ${fail} fail / ${results.length} files`);

  if (fail > 0) {
    console.log('');
    console.log('Failure output:');
    for (const r of results.filter(x => x.code !== 0)) {
      console.log(`─── ${r.file} ───`);
      console.log(r.stdout.split('\n').slice(-30).join('\n'));
      if (r.stderr) {
        console.log('--- stderr ---');
        console.log(r.stderr.split('\n').slice(-20).join('\n'));
      }
    }
  }

  process.exit(fail > 0 ? 1 : 0);
})();
