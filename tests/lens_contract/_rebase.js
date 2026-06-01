// Shared fixture timestamp rebase for the lens-contract structural harnesses.
//
// The captured fixtures (wo_*.json, p567_*.json, dc_*.json) carry STALE
// absolute timestamps from capture time. The lens build functions
// (buildPilotageWatch / buildTowageShift) use Date.now() for their window
// math (8h watch, 12h shift, 24h forward, 24h operational context). When a
// harness runs at an arbitrary wall-clock time, whether a fixture's transit /
// job / eta lands inside a given window depends on the gap between the
// fixture's absolute timestamps and "now" — making acceptance non-deterministic
// (e.g. LC-11 silently flipped from 10/10 to 7/10 as the wall clock crossed a
// stale eta, with NO code change).
//
// This helper anchors every ISO-8601 timestamp in a fixture to runtime `now`
// by shifting it by (now - fixture.generated_at). All relative spacing is
// preserved exactly, so the engineered/captured operational structure lands in
// the live windows regardless of when the test runs. This is the SAME pattern
// already used by tests/lens_contract/value/value_acceptance.test.js.
//
// It does NOT weaken any criterion: it makes the SCOPE the harness evaluates
// deterministic (the same vessels land in the same windows every run), so the
// addressability / independence / forward-window assertions test the lens
// logic rather than the wall clock. Vessel statuses, flags, and counts are
// untouched.
//
// TEST-ONLY. Never imported by application code.

const fs = require('fs');

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

// Read a fixture from disk and rebase its timestamps to runtime now.
// If the fixture has no parseable `generated_at` anchor, it is returned
// unchanged (no-op) so callers are always safe.
function loadFixtureRebased(absPath) {
  const raw = JSON.parse(fs.readFileSync(absPath, 'utf-8'));
  if (!raw || typeof raw.generated_at !== 'string') return raw;
  const anchor = new Date(raw.generated_at).getTime();
  if (isNaN(anchor)) return raw;
  return rebase(raw, Date.now() - anchor);
}

module.exports = { rebase, loadFixtureRebased };
