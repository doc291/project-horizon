# Lens-Contract Acceptance Harness

This directory holds the source-controlled acceptance harness for Horizon's
stakeholder lenses. It implements (or will implement) acceptance criteria
**LC-1 through LC-14** from
[`docs/governance/LENS-CONTRACT.md`](../../docs/governance/LENS-CONTRACT.md).

## Purpose

Before the G3 migration, the lens harnesses lived in `/tmp/` and were lost on
any reboot. They were not source-controlled, not auditable, and not
reproducible. This directory exists to:

1. Bring the harnesses under version control.
2. Make harness runs reproducible (`node tests/lens_contract/run_all.js`).
3. Enforce the binding contract in
   [`docs/governance/LENS-CONTRACT.md`](../../docs/governance/LENS-CONTRACT.md).

## Current scope (G3 — migration only)

This commit performs **pure migration**. The harness logic that was in `/tmp/`
has been moved here **byte-identically**. No new assertions, no behavioural
changes, no interpretation of the lens-contract criteria.

| File | Source | Status |
|---|---|---|
| `legacy/pilotage_wo.test.js` | `/tmp/wo_acceptance.js` | ✓ byte-identical migration. Relocated from top level at remediation step R1 because its Criterion 3 asserts against the pre-R1 Watch Overview demand-headline text patterns (`N transit(s) this watch`, `I inbound · O outbound`, `next:/now:`), which R1 replaced with the contract-mandated operational-context rows. Other criteria still meaningful (synthetic ID scan, commercial-figure scan, fabricated-qualification scan) and runnable manually: `node tests/lens_contract/legacy/pilotage_wo.test.js`. |
| `legacy/pilotage_p567.test.js` | `/tmp/p567_acceptance.js` | ✓ byte-identical migration (older Pilotage harness, preserved for audit) |
| `data_confidence.test.js` | `/tmp/dc_harness.js` | ✓ byte-identical migration |
| `towage.test.js` | (no source) | stub — no migration source available |
| `parity.test.js` | (new) | stub — implementation begins at G8 |
| `threshold_scan.test.js` | (new) | stub — implementation begins at G9 |
| `run_all.js` | (new) | orchestration only — no assertions |

## Lens-contract criteria status

**None of the LC-1..LC-14 criteria are implemented yet.** Migration only. The
migrated harnesses check the constraints they were written to check —
including some that overlap with the contract — but they were not authored
against the contract and are not the contract's enforcement mechanism.

Criterion implementation begins at **G4** and is delivered task-by-task per
the governance implementation plan:

| Stage | Criteria | Notes |
|---|---|---|
| G4 | LC-1, LC-4, LC-5 | Structural — section markers + window declarations |
| G5 | LC-2 | Mutation test — Section A not derived from Section B |
| G6 | LC-3, LC-9 | Section C strictly later than B; empty-B resilience |
| G7 | LC-6, LC-7, LC-8 | Representational adequacy (Section A floor) |
| G8 | LC-10, LC-11 | Information parity vs Beta 10 |
| G9 | LC-12, LC-13 | Threshold scanner + boundary precision |
| G10 | LC-14 | Manual stakeholder read-aloud signoff process |

Each stage is gated on explicit approval from the contract owner per
[`docs/governance/LENS-CONTRACT.md`](../../docs/governance/LENS-CONTRACT.md) §1.4.

## How to run

```bash
node tests/lens_contract/run_all.js
```

Exit code:
- `0` — every `*.test.js` passed.
- `1` — at least one test failed.

The runner:
1. Replicates the source-controlled fixtures from `fixtures/` into `/tmp/` so
   that the byte-identical migrated harnesses (which still hard-code `/tmp/`
   paths) can run without modification.
2. Spawns each `*.test.js` in this directory in turn (alphabetical order).
3. Aggregates exit codes and prints a single summary.
4. Exits non-zero if any test failed.

Legacy harnesses under `legacy/` are **not** run by the default runner. They
are preserved for migration audit only. To run a legacy harness directly:

```bash
node tests/lens_contract/legacy/pilotage_p567.test.js
```

## Fixtures

See [`fixtures/README.md`](fixtures/README.md) for fixture sources, refresh
process, and governance review requirements.

## Adding a new lens

A new stakeholder lens does not begin with code. It begins with a Lens
Proposal completed against the template in
[`docs/governance/LENS-CONTRACT.md`](../../docs/governance/LENS-CONTRACT.md)
§7, committed under `docs/governance/lens-proposals/`, and approved by the
contract owner. Only then does a new `<role>.test.js` file get added to this
directory, alongside the fixtures the proposal specifies.

## Why /tmp paths persist in the migrated harnesses

The G3 migration was deliberately scoped to "no behavioural changes". The
existing `/tmp/<prefix>_<port>.json` paths inside the harnesses were not
modified. The runner (`run_all.js`) compensates by replicating fixtures from
the source-controlled `fixtures/` directory into `/tmp/` before each run.

Future stages (G4+) will update each harness to read directly from
`tests/lens_contract/fixtures/` and the `/tmp` shim will be removed. Until
then, the shim is documented and intentional.

## Ownership

Harness ownership follows lens ownership. The contract owner
(Tony Trajceski) is the final authority on what passes and what fails. No
criterion may be waived without an explicit written approval recorded against
the relevant commit.
