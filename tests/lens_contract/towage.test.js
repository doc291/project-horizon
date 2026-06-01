// Towage lens acceptance harness.
//
// MIGRATION NOTE (G3, 2026-06-01):
// There was no source-controlled Towage acceptance harness at the time of
// the lens-contract repo migration. The original T1-T13 implementation work
// used inline acceptance scripts that were not preserved in /tmp at the time
// of the G3 inventory. This file is therefore a structural placeholder.
//
// STATUS: stub — no assertions yet.
// SCOPE BEGINS AT: G4 (LC-1, LC-4, LC-5 — structural)
//                  G5 (LC-2 — mutation test against Towage data layer)
//                  G6 (LC-3, LC-9 — Section C boundary + empty-Section-B)
//                  G7 (LC-6, LC-8 — Towage Section A representational adequacy)
//
// This stub is intentionally a passing no-op so that run_all.js produces a
// consistent run report. It does not assert correctness of the Towage lens.

console.log('=== Towage lens harness ===');
console.log('STUB — no assertions yet. Implementation begins at G4.');
console.log('See docs/governance/LENS-CONTRACT.md §4 (Towage Lens Contract)');
console.log('See docs/governance/LENS-CONTRACT.md §6 (LC-1..LC-14 criteria)');
process.exit(0);
