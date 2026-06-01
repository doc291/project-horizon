# VAL-X2 — Distinctness From the VTSO Dashboard

> Manual value gate. A stakeholder lens exists to give a pilot or tug master
> something the general VTSO coordination dashboard does **not** surface for
> their role. If a lens shows only what the dashboard already shows, it has no
> reason to exist — it is a filtered dashboard, which is exactly the regression
> the Lens Contract was written to prevent.

VAL-X2 implements the distinctness requirement from the Lens Regression
Investigation and `tests/lens_contract/value/README.md`.

## The test

For each stakeholder lens, assert in writing that it surfaces **at least two
operational facts that are not directly visible in the VTSO dashboard for that
stakeholder.** "Directly visible" means a pilot/tug master could read it off the
dashboard without doing their own derivation. A fact the dashboard contains only
as raw inputs the operator must combine themselves does **not** count as
"directly visible" — surfacing the derived consequence is exactly the lens's job.

VAL-X2 PASSES for a lens when ≥2 distinct facts are documented and confirmed by
the reviewer. It is a written, reviewed assertion — not automated — because
"directly visible on the dashboard" is a judgement a domain reviewer makes.

## Pilotage lens — distinct facts

| # | Fact the lens surfaces | Where in the lens | Dashboard equivalent? |
|---|---|---|---|
| 1 | **Transit-specific will-it-hold verdict** — per transit, e.g. "UKC tight (0.4m) — watch the window" | Section B, `.pwc-verdict` on each transit + lead block | No. The dashboard shows vessels, berths, and conflict cards, but not a per-transit UKC/tide hold verdict scoped to the pilot's boarding decision. |
| 2 | **Tide-window / forward pressure** — derived constraints forming over the next watch, e.g. "MV X arrives with UKC margin 0.4m in 6h", "3 pilotage movements 09:00–09:30" | Section C, `.pwc-pressure` | No. The dashboard surfaces tide and UKC as raw conditions/feeds; it does not derive *which* upcoming transit is pressured *when*, nor flag pilotage-movement clustering. |
| 3 (supporting) | **Watch-shape headline** — "Busy watch — 6 transits, 2 tide-constrained, peak 14:00–16:00" | Lens top, `.pwc-shape` | No. The dashboard has no watch-scoped load characterisation. |

**Pilotage VAL-X2: ≥2 distinct facts → satisfied** (verdict + tide-window pressure; watch-shape is a third).

## Towage lens — distinct facts

| # | Fact the lens surfaces | Where in the lens | Dashboard equivalent? |
|---|---|---|---|
| 1 | **Per-tug capacity clash** — "SVR Apex assigned to two jobs within 60 minutes — capacity clash" | Section B, `.tms-clash` | No. The dashboard shows towage events but does not cross-check a single tug's assignments for overlap. This is the headline new intelligence the dashboard never computes stakeholder-side. |
| 2 | **Forward demand-vs-capacity** — "Forward peak 18:00 — 6 tug assignments, 4 available; shortfall risk by 2 tugs" | Section C, `.tms-fwd-demand` | No. The dashboard shows neither aggregate forward tug demand nor its comparison against available fleet. |
| 3 (supporting) | **Fleet-readiness impact** — down-tug shown with its impact ("Unavailable in current simulated fleet state") + shift-shape headline | Section A, `.tms-tug-return` / `.tms-shape` | Partially. The dashboard may show tug status, but not the shift-scoped readiness characterisation tied to the tug master's shift load. |

**Towage VAL-X2: ≥2 distinct facts → satisfied** (capacity clash + demand-vs-capacity; fleet-readiness is a third).

## Reviewer assertion

| Lens | ≥2 distinct facts documented | Reviewer | Date | Verdict |
|---|---|---|---|---|
| Pilotage | Yes (verdict, tide-window pressure, watch-shape) | _pending_ | _pending_ | PASS / FAIL |
| Towage | Yes (capacity clash, demand-vs-capacity, fleet readiness) | _pending_ | _pending_ | PASS / FAIL |

The distinct-fact mapping above is **documented and self-evident from the
implemented markers** (LV1–LV4). Final VAL-X2 sign-off requires a reviewer to
confirm — against the live VTSO dashboard — that these facts are not directly
visible there for the stakeholder. Until that confirmation, VAL-X2 is
**documented but reviewer-pending**, consistent with VAL-X1.

## What would FAIL VAL-X2

- A lens whose only content is a filtered subset of dashboard vessel/berth/
  conflict data with no derived verdict, pressure, clash, or demand signal.
- A lens where the "distinct" facts are actually rendered identically on the
  dashboard for that role.
- A lens that surfaces a derived fact the dashboard already derives and
  displays prominently to that stakeholder.

## Cross-reference

- `tests/lens_contract/value/value_acceptance.test.js` — automated VAL-P/VAL-T markers backing facts above.
- `docs/governance/lens-signoffs/TEMPLATE.md` — VAL-X1 read-aloud sign-off.
- `docs/governance/LENS-CONTRACT.md` §6 — the binding criteria.
