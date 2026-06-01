# Value-Acceptance Harness (LV0)

This directory holds the **value-acceptance** layer of the lens-contract test
suite. It is distinct from, and complementary to, the structural contract
harness (`tests/lens_contract/lc_*.test.js`, criteria LC-1..LC-13).

## Why this exists

The structural harness measures whether the four lens sections are *present
and correctly bounded*. A lens can pass all of LC-1..LC-11 and still be
operationally thin — a list of rows with no lead item, no will-it-hold
verdict, no derived forward pressure, and no capacity-clash detection. That
thinness is the regression documented in the Lens Regression Investigation.

The value harness measures whether the lens carries **operational decision
value** — the things a pilot or tug master would actually use in the first 30
seconds. It is adversarial to thinness: a structurally-compliant but thin lens
fails here.

## Files

| File | Role |
|---|---|
| `value_acceptance.test.js` | The harness. Extracts `buildPilotageWatch` / `renderPilotageWatch` / `buildTowageShift` / `renderTowageShift` from `index.html`, rebases fixture timestamps to runtime `now`, and runs VAL-P1..VAL-T5 against engineered fixtures. |
| `make_fixtures.py` | Generator for the six engineered fixtures under `../fixtures/value/`. Re-run to regenerate. Derives each fixture from `wo_MELBOURNE.json` by targeted mutation so every fixture is a structurally complete /api/summary snapshot. |
| `README.md` | This file. |

## Engineered fixtures (`../fixtures/value/`)

| Fixture | Engineers | Targets |
|---|---|---|
| `pil_tide_closing.json` | in-watch TIGHT-UKC transit + closing tide window | VAL-P1, P2, P3, P4, P5 |
| `pil_cluster.json` | 3 boardings stacked in a 45-min forward window (+9h) | VAL-P1, P4, P5 |
| `tow_clash.json` | two in-shift jobs share SVR Apex at overlapping times | VAL-T1, T2(+), T5 |
| `tow_noclash.json` | same jobs/times but different tugs (no clash) | VAL-T2(−) |
| `tow_downtug.json` | 5-tug fleet (one MAINTENANCE) + 1 job | VAL-T3, T5 |
| `tow_demand_peak.json` | 6 jobs at +14h each needing 2 tugs; fleet=5 | VAL-T4 |

## Timestamp rebase

Fixtures author timestamps relative to their `generated_at` anchor. The build
functions use `Date.now()` for window math. Before building, the harness shifts
every ISO-8601 timestamp by `(now − generated_at)` so the engineered conditions
land inside the live 8h watch / 12h shift / 24h forward windows regardless of
when the test runs.

## Automated checks → LV phase that turns each GREEN

| Check | Marker the phase must emit | Turns GREEN at |
|---|---|---|
| VAL-P1 | `.pwc-lead` (next/at-risk transit lead block) | LV1 |
| VAL-P2 | `.pwc-verdict` (per-transit will-it-hold line) | LV1 |
| VAL-P3 | at-risk transit pinned inside `.pwc-lead` | LV1 |
| VAL-P4 | `.pwc-pressure` (derived forward-pressure statements) | LV2 |
| VAL-P5 | `.pwc-shape` (Section A watch-shape headline) | LV1 |
| VAL-T1 | `.tms-lead` (next job lead block incl. tug availability) | LV3 |
| VAL-T2 | `.tms-clash` present on clash fixture, absent on no-clash | LV3 |
| VAL-T3 | `.tms-tug-return` (down-tug return/impact) | LV3 |
| VAL-T4 | `.tms-fwd-demand` (demand-vs-capacity forward curve) | LV4 |
| VAL-T5 | `.tms-shape` (Section A shift-shape headline) | LV3 |

The marker class names are the **contract between this harness and the LV
phases**: each phase MUST emit its named marker. If a phase implements the
feature under a different class name, update both the renderer and this harness
together (and note it in the LENS-CONTRACT changelog).

## Manual gates (not automated)

These are the ultimate measures of value; the automated checks above are
proxies for them.

### VAL-X1 — Stakeholder read-aloud (LV5)

A Melbourne pilot and a Melbourne tug master each cold-read the relevant lens
for 30 seconds and verbally answer the four questions:

1. What is happening right now?
2. What do I need to do?
3. What is coming next?
4. What just changed?

Every answer must be sourceable from the lens. Record the session as a signoff
under `docs/governance/lens-signoffs/YYYY-MM-DD-<role>-MELBOURNE.md`.
**Status: PENDING (LV5).**

### VAL-X2 — Distinctness from the VTSO dashboard (LV5)

A written, reviewed assertion that each lens surfaces at least two operational
facts the VTSO dashboard does NOT surface stakeholder-side. Current intended
distinct facts:

- Pilotage: transit-specific tide-window-closing pressure; watch-shape headline.
- Towage: per-tug capacity-clash; fleet demand-vs-capacity forward curve.

If a lens shows only dashboard-equivalent facts, it fails its reason to exist.
**Status: PENDING (LV5).**

## Running

```bash
# Structural contract (unchanged; must stay green)
node tests/lens_contract/run_all.js

# Value baseline (LV0: expected all-RED; later phases turn slices GREEN)
node tests/lens_contract/value/value_acceptance.test.js
```

The value harness is run as a **sibling** at LV0 — deliberately NOT wired into
`run_all.js` yet, so the structural suite stays green while the value baseline
is RED. Wiring into `run_all.js` (as a hard gate) is deferred until all VAL
checks are GREEN (post-LV5).

## Exit convention

`value_acceptance.test.js` exits 0 in baseline mode (findings reported inline),
consistent with the `lc_*.test.js` harnesses. It does not gate the suite at
LV0. A future phase may flip it to a hard gate once every check is GREEN.
