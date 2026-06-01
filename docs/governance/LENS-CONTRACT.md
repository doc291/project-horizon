---
version: 1.1
effective_date: 2026-06-01
owner: Tony Trajceski
status: ratified
supersedes: 1.0 (additive amendment — B1 representative towage-demand model migrated; see LENS-CONTRACT-CHANGELOG.md v1.1)
---

# Horizon Stakeholder Lens Contract

> The doctrinal source of truth for every Horizon stakeholder lens.
> No stakeholder-lens work proceeds without satisfying this contract.

---

## 1. Preamble

### 1.1 Purpose

This document defines the structure, content floors, and acceptance criteria that
every Horizon stakeholder lens MUST satisfy before it is implemented, reviewed,
demoed, or shipped.

It exists because the first Beta 12 stakeholder lenses (Pilotage Watch Overview
and Towage My Shift) shipped with a doctrinal regression: the operator's
"World" was silently collapsed onto the operator's "Watch", and the lens — while
internally consistent — no longer represented the stakeholder's operational
reality. Vessels demonstrably in port did not appear anywhere in the lens. The
lens passed every test it was given because the tests verified what the lens
did, not what the stakeholder needed.

This contract closes that gap by:

1. Naming the four sections every lens MUST contain.
2. Specifying what each section is allowed to be derived from.
3. Defining acceptance criteria that fail when a section is missing, collapsed,
   or derived from a narrower section.
4. Forcing hidden visibility gates (such as hard-coded operational thresholds)
   into explicit, port-configurable, reviewed allowlists.

### 1.2 Scope

This contract governs **every Horizon UI surface that represents a single
stakeholder's operational world**:

| In scope | Out of scope |
|---|---|
| Pilotage lens | The Beta 10 VTSO coordination dashboard (already broadly compliant — VTSO retained context because the Beta 10 dashboard was not replaced) |
| Towage lens | Port Brief PDF |
| Future lenses (Terminal, Customs, Marine Pilot Office, Vessel Agent, etc.) | Health / diagnostic endpoints |
| Any future replacement of a stakeholder-scoped Beta 10 panel | API responses (`/api/summary`, `/api/health-data`) |

When a future stakeholder lens is added to Horizon, it is added under this
contract. There is no exception path.

### 1.3 Who must read this

**Mandatory reading, in order, before any stakeholder-lens work:**

1. This document, in full, including the criteria in §6.
2. The current entry in `docs/governance/LENS-CONTRACT-CHANGELOG.md` (if
   present) — to surface any amendments since version 1.0.
3. The "How to add a new stakeholder lens" template in §7.

Every agent or contributor who modifies a stakeholder lens renderer, its data
layer, its filters, or its acceptance harness MUST declare compliance in the
commit message in the form:

```
lens-contract: LC-1✓ LC-2✓ LC-3✓ LC-4✓ LC-5✓ LC-6✓ LC-7✓ LC-8✓ LC-9✓
                LC-10✓ LC-11✓ LC-12✓ LC-13✓ LC-14<status>
```

`LC-14` may be marked `manual-pending` for preview deployments. It MUST be
explicitly signed off (per §6.5) before a lens is described as demo-ready or
production-ready.

### 1.4 Authority

This contract is owned by **Tony Trajceski (Horizon division founder)**. It is
amended by versioned changelog entries; no silent edits. Disputes about contract
interpretation are resolved by the owner.

---

## 2. The common A / B / C / D structure

Every stakeholder lens MUST render four sections, in this order, as separately
identifiable regions of the lens:

```
┌───────────────────────────────────────────────────────────────────┐
│  D. Exceptions  — interrupts above all other layers when present  │
├───────────────────────────────────────────────────────────────────┤
│  A. Operational Context                                           │
│     The stakeholder's persistent operational surface, independent │
│     of any time window. Answers: "What is my world right now?"    │
├───────────────────────────────────────────────────────────────────┤
│  B. Current Watch / Shift                                         │
│     Time-bounded events the stakeholder must act on now.          │
│     Answers: "What do I need to do in this watch/shift?"          │
├───────────────────────────────────────────────────────────────────┤
│  C. Forward Pressure                                              │
│     Items beyond the watch/shift window but already shaping it.   │
│     Answers: "What is coming?"                                    │
└───────────────────────────────────────────────────────────────────┘
```

### 2.1 Properties each section MUST have

| Property | Section A | Section B | Section C | Section D |
|---|---|---|---|---|
| Time-bounded | No — persistent | Yes — natural operational unit (e.g., watch, shift) | Yes — strictly LATER than B's window-end | No — surfaces when present |
| Mandatory | Yes | Yes | Yes (may render empty with explicit note) | Conditional (renders when exceptions exist) |
| May be derived from another section | No — derivation from B is the regression this contract exists to prevent | May read from A but defines its own filter | Must reference items strictly later than B.winEnd | May reference items from B/A by ID |
| Visible window declared in render | N/A | Yes (e.g., "Next 8 hours") | Yes (e.g., "Next 8–24 hours") | N/A |

### 2.2 What "persistent" means for Section A

Section A is the answer to *"if you woke up at this terminal cold, what would
you need to know about the operation before you started reading your watch?"*
It is the operator's awareness backdrop. It changes slowly. It is independent
of whether any particular event is scheduled in the next hour, the next watch,
or the next day.

A Section A that goes empty when the watch is empty is a Section A that has
collapsed onto Section B. That is the failure mode this contract prohibits.

### 2.3 What "strictly later" means for Section C

Section C exists to surface what is *coming* but *not yet actionable*. It
extends the operator's awareness past the watch boundary without diluting the
watch. Every item rendered in C must have a `scheduled_time`, `eta`, or `etd`
strictly greater than Section B's `window_end`. Items inside Section B's window
belong in B, not C.

### 2.4 Empty-state behaviour

| Section | Empty-state requirement |
|---|---|
| A | Cannot legitimately be empty when `vessels[]` for the active port is non-empty. If A is empty in that condition, the lens fails LC-2 and LC-6. |
| B | May legitimately be empty (a quiet watch). MUST render an explicit "Quiet watch — no transits scheduled" / "Quiet shift — no jobs scheduled" note. MUST still render A, C, D fully. |
| C | May legitimately be empty. MUST render "Next watch quiet" / "Next shift quiet" note rather than collapse to nothing. |
| D | Renders when exceptions exist; absent otherwise. |

---

## 3. Pilotage Lens Contract

### 3.1 Section A — Operational Context (persistent, no time window)

The pilot's world **before** she opens her watch.

| Element | Definition | Source field |
|---|---|---|
| Vessels at berth requiring outbound pilotage | All vessels where `status == "berthed"` AND `pilotage_required == true` | `vessels[]` |
| Vessels inbound 24 h requiring inbound pilotage | All vessels where `status in ("expected","confirmed")` AND `eta ≤ now + 24h` AND `pilotage_required == true` | `vessels[]` |
| Pilot roster reference | Count + companion note that allocation, qualifications, and fatigue live in the pilotage system | `port_profile.pilots` |
| Today's tidal envelope | Highest / lowest astronomical tide, current trend, next slack | `tides` |
| UKC posture | Count of vessels with UKC at risk in the next 24 h | `arrival_ukc.all[]` |

**Representational floor (LC-6 / LC-7):** if `len(vessels) ≥ 3` for the active
port, Section A MUST reference at least three of the elements above as
distinct, separately-identifiable counts or notes.

### 3.2 Section B — Current Watch

**Window:** rolling 8 hours: `[now − 1h, now + 7h]`. This is the pilot's
natural watch horizon and is the correct boundary for Section B alone.

| Element | Definition |
|---|---|
| My transits | `pilotage[]` filtered to `scheduled_time` in window |
| Per-transit confidence flag | Inline pill (Layer 3) — UKC, weather, decision-required |
| Window snapshot per transit | UKC envelope at predicted arrival, when present |

### 3.3 Section C — Forward Pressure

**Window:** `[now + 7h, now + 24h]`. Items here are STRICTLY later than Section
B's `window_end`.

| Element | Definition |
|---|---|
| Transits forming next watch | `pilotage[]` with `scheduled_time` in 8–24 h |
| Vessels approaching the tide-window edge | `arrival_ukc.all[]` where `hrs_to_eta` in 8–24 h AND status approaching unsafe |
| Weather change crossing a pilotage threshold | `weather` forecast crossing wind / visibility limit within 24 h |
| Decisions forming around a forward transit | `beta11.scenarios` propagating into the next watch |

### 3.4 Section D — Exceptions

| Element | Definition |
|---|---|
| Coordinated decisions requiring pilot acknowledgement | `beta11.active_decision` where stakeholder = PILOTAGE |
| New inbound vessel conflicts affecting pilotage | `conflicts[]` diff vs previous poll, `signal_type == "CONFLICT"` |
| UKC suddenly unsafe inside the watch | `arrival_ukc.all[]` where status just transitioned to UNSAFE for an in-watch transit |
| Stakeholder flag-back from another role | `beta11.active_decision.required_stakeholders[].flag_reason` |

---

## 4. Towage Lens Contract

### 4.1 Section A — Operational Context (persistent, no time window)

The tug master's world **before** she opens her shift.

| Element | Definition | Source field |
|---|---|---|
| Tug fleet status | All tugs with operational_status, bollard pull, bookings_in_window | `port_tugs[]` |
| Vessels at berth requiring outbound towage | All `status == "berthed"` AND `towage_required == true` | `vessels[]` |
| Vessels inbound 24 h requiring inbound towage | All `status in ("expected","confirmed")` AND `eta ≤ now + 24h` AND `towage_required == true` | `vessels[]` |
| Terminal readiness baseline | Per-berth readiness signal | (currently `assumed`) |
| Weather envelope | Wind / swell / visibility against tug limits | `weather` |

**Representational floor (LC-6 / LC-8):** if `len(vessels) ≥ 3` for the active
port, Section A MUST reference `port_tugs` count plus at least two further
elements above.

### 4.2 Section B — Current Shift

**Window:** rolling 12 hours: `[now − 1h, now + 11h]`. This is the natural tug
shift and is the correct boundary for Section B alone.

| Element | Definition |
|---|---|
| My jobs | `towage[]` filtered to `scheduled_time` in window |
| Per-job confidence flag | Inline pill (Layer 3) — vessel late, weather breach, decision-required |
| Tug assignments per job | From `tugs_assigned` |
| Shift load | Distribution of jobs across the shift |

### 4.3 Section C — Forward Pressure

**Window:** `[now + 11h, now + 24h]`. Items STRICTLY later than Section B's
`window_end`.

| Element | Definition |
|---|---|
| Jobs forming next shift | `towage[]` with `scheduled_time` in 12–24 h |
| Vessels likely to slip into shift | Vessels with `eta` / `etd` within 1–2 h of the window boundary |
| Tug operational changes coming | Scheduled tug maintenance / handover |
| Weather change crossing a tug limit | Forecast threshold crossings in 12–24 h |

### 4.4 Section D — Exceptions

| Element | Definition |
|---|---|
| Coordinated decisions requiring tug-master acknowledgement | `beta11.active_decision` where stakeholder = TOWAGE |
| New bookings inside the shift | `towage[]` diff vs previous poll |
| Tug operational change inside the shift | `port_tugs[].operational_status` transition |
| Stakeholder flag-back from another role | `beta11.active_decision.required_stakeholders[].flag_reason` |

---

## 5. Threshold-Defensibility Doctrine

### 5.1 The principle

**No stakeholder visibility may be gated by a hidden, hard-coded numeric
threshold.** Visibility gates are doctrine, not magic numbers; they must live
in port configuration, be reviewed by operators, and have boundary-precision
test fixtures.

### 5.2 The exemplar: `loa > 200` is not acceptable

The current Beta 12 implementation gates whether a vessel is towage-eligible on
a single binary condition:

```
towage_required = loa > 200
```

This rule fails the doctrine in six specific ways:

| Failure | Detail |
|---|---|
| **Single-dimension threshold** | Real towage need depends on **LOA, beam, draught, deadweight, vessel type, berth, wind, current** — not LOA alone. Car carriers and box-ships have high windage; LNGCs have stringent rules at any LOA. |
| **Hard-coded, not port-configurable** | Each port's pilots and harbour master publish a towage matrix. Melbourne, Brisbane, Geelong, Darwin each have different thresholds. A global `>200` ignores this. |
| **No tiering** | Real rules tier tug count: 0 / 1 / 2 / 3 / 4 tugs depending on LOA band, not a binary include/exclude. |
| **Source-precision sensitive** | AIS-reported LOA is `A + B` (forward + aft dimensions) reported in whole metres. A vessel of true LOA 199.99 m may be reported as 199 m, 199.9 m, or 200 m depending on the transponder. A threshold exactly at 200 m turns AIS rounding into a business-rule flip. |
| **No vessel-type override** | A 180 m car carrier with high windage absolutely needs tugs. A 220 m self-propelled barge in calm conditions may not. LOA alone misses this. |
| **The canary case: BYD ZHENGZHOU** | Her real LOA is ~199.9 m. Under `loa > 200` she is silently excluded from the towage data path entirely. AIS rounding alone decides whether she ever appears in a tug master's world. |

### 5.3 What a defensible rule looks like

A defensible visibility rule has three properties:

1. **Per-port, configurable** — sourced from `port_profile`, not from a code
   constant.
2. **Multi-dimensional** — LOA band, beam, vessel type, and optionally berth /
   weather modify the result.
3. **Tiered output** — returns `n_tugs` (0, 1, 2, 3, …). The boolean
   `towage_required = n_tugs >= 1` is a derived property, not the rule itself.

A minimal defensible v1 (illustrative; the actual rule is ratified by port
operators, not by Horizon):

```yaml
port_profile.towage_rule:
  bands:
    - { loa_max: 100, n_tugs: 0 }
    - { loa_max: 180, n_tugs: 1 }
    - { loa_max: 220, n_tugs: 2 }
    - { loa_max: 280, n_tugs: 3 }
    - { loa_max: 999, n_tugs: 4 }
  vessel_type_floor:
    "Car carrier": 2   # high-windage; minimum 2 tugs regardless of LOA band
    "LNG carrier": 3
    "Tanker":      2
```

Under such a rule, BYD ZHENGZHOU (199.9 m car carrier) lands in the 180–220 m
band (2 tugs) or is promoted to floor-of-2 by the car-carrier rule. Either way,
`towage_required = true`, and the rest of this contract decides her placement.

### 5.4 The allowlist requirement

Every numeric threshold used in a stakeholder visibility path MUST be:

1. Listed in `docs/governance/APPROVED-THRESHOLDS.md` with file:line, value,
   port_profile source key, justification, and date of operator review.
2. Boundary-tested at `value − epsilon` and `value + epsilon` with documented
   expected outcomes in `tests/lens_contract/thresholds/`.
3. Reviewed annually or whenever a port operator amends their towage / pilotage
   / UKC / wind matrix.

Any numeric threshold detected in code that is NOT on the allowlist fails LC-12
and blocks the change.

---

## 6. Acceptance Criteria

These criteria are binding. A change touching a stakeholder lens MUST be
verified against every applicable criterion before merge or deploy. Failures
must be either fixed or explicitly waived in writing (per §1.4 — the owner is
the only authority that may waive a criterion).

### 6.1 Structural criteria

| ID | Criterion | Failure mode prevented |
|---|---|---|
| **LC-1** | The lens MUST render Sections A, B, C, and D as separately identifiable regions in the DOM (e.g., distinct `<section data-lens-section="A">` markers). | Section collapse / window applied to the entire lens. |
| **LC-2** | Section A MUST NOT be derived solely from Section B's filtered set. Mutation test: shrink B's window to zero — Section A MUST still render with non-zero content if `vessels[]` for the active port is non-empty. | The Beta 12 Pilotage / Towage regression that prompted this contract. |
| **LC-3** | Every item rendered in Section C MUST have `scheduled_time` (or equivalent time field) strictly LATER than Section B's `window_end`. | Forward Pressure duplicating Section B. |
| **LC-4** | Section B's window MUST be declared in the rendered output (e.g., "Next 8 hours"). | Implicit windows the operator cannot see. |
| **LC-5** | Section C's window MUST be declared in the rendered output (e.g., "Next 8–24 hours"). | Implicit windows the operator cannot see. |

### 6.2 Representational adequacy criteria

| ID | Criterion | Failure mode prevented |
|---|---|---|
| **LC-6** | If `len(vessels) ≥ 3` for the active port, Section A MUST reference at least three distinct operational dimensions named in the relevant lens contract. | One-number Section A (e.g., "Pilots available: 4" with nothing else). |
| **LC-7** | Pilotage Section A MUST include the count of `vessels[]` where `status == "berthed"` AND `pilotage_required`, and the count where `eta ≤ now + 24h` AND `pilotage_required`. | "Pilots available" being the sole Section A element. |
| **LC-8** | Towage Section A MUST include `port_tugs` count, count of `vessels[]` where `status == "berthed"` AND `towage_required`, and count where `eta ≤ now + 24h` AND `towage_required`. | Tug-only Section A with no vessel context. |
| **LC-9** | Empty-Section-B state MUST still render Sections A, C, D AND an explicit "Quiet watch / shift" note. Never a single empty box. | Lens degrading to a blank page when the watch is quiet. |

### 6.3 Information parity criteria

| ID | Criterion | Failure mode prevented |
|---|---|---|
| **LC-10** | For every stakeholder, the operational facts surfaced in the corresponding Beta 10 view MUST be matched or superseded by the Beta 12 lens. The lens MUST surface a superset, not a subset. | Net information loss when replacing Beta 10 surfaces with Beta 12 lenses. |
| **LC-11** | A vessel present in the Beta 10 vessel roster for the active port MUST be addressable from the Beta 12 stakeholder lens (Section A ∪ B ∪ C), or be explicitly justified as not-relevant-to-that-stakeholder. | The BYD-ZHENGZHOU class of regression — a vessel demonstrably in port but invisible to the stakeholder. |

### 6.4 Threshold defensibility criteria

| ID | Criterion | Failure mode prevented |
|---|---|---|
| **LC-12** | No hard-coded numeric threshold may gate stakeholder visibility. All such thresholds MUST be sourced from `port_profile` and be tiered, not binary. Every threshold in the visibility path MUST be listed in `docs/governance/APPROVED-THRESHOLDS.md`. | Port-blind, single-dimension gates like `loa > 200`. |
| **LC-13** | Boundary-precision test: every threshold MUST be tested at `value − epsilon` and `value + epsilon`. The expected outcome MUST be documented and visible in the lens (e.g., "1 tug" vs "2 tugs"), not a binary include / exclude. | A 199.9 vs 200.0 reading silently flipping inclusion. |

### 6.5 Stakeholder read-aloud criterion (manual gate)

| ID | Criterion | Failure mode prevented |
|---|---|---|
| **LC-14** | A domain practitioner (pilot, tug master, terminal operator, as appropriate to the lens) cold-reads the lens for 30 seconds and verbally answers four questions: *"What is my world right now? What do I need to do? What is coming? What just changed?"* Each answer MUST be sourceable from the lens. Sign-off is recorded under `docs/governance/lens-signoffs/YYYY-MM-DD-<role>-<port>.md`. | Doctrine drift — a lens that passes every automated check but fails the only check that matters. |

LC-14 is **blocking for production / demo status** and **non-blocking for
preview deploys**. A preview may ship marked `LC-14: manual-pending`. A demo
may not.

---

## 7. How to add a new stakeholder lens

Every new lens MUST be authored by completing this template **before any
renderer code is written**. The completed template is committed under
`docs/governance/lens-proposals/YYYY-MM-DD-<role>.md` and approved by the
contract owner.

### 7.1 Lens Proposal Template

```markdown
---
lens_name: <e.g., Terminal Operations>
stakeholder: <e.g., TERMINAL>
author: <name>
date: <YYYY-MM-DD>
contract_version: 1.0
---

# <Stakeholder> Lens Proposal

## 1. Stakeholder identity

- Role:
- Operational unit (watch / shift / window / continuous):
- Decision authority:
- Existing system of record (what Horizon must NOT duplicate):
- Existing Beta 10 surface (if any) that this lens replaces or augments:

## 2. Section A — Operational Context

For each element below: name, definition, source field, representational-floor
contribution.

| Element | Definition | Source field |
|---|---|---|
|         |            |              |

Representational floor: if `len(vessels) ≥ 3`, Section A references AT LEAST
the following <N> elements: ...

## 3. Section B — Current <Watch / Shift / Window>

- Window definition (size + units):
- Why this window? (citation to operational practice):
- Per-item content:
- Confidence rules:

## 4. Section C — Forward Pressure

- Window definition (strictly LATER than B's window_end):
- Per-item content:
- Empty-state note text:

## 5. Section D — Exceptions

- Interrupt sources:
- Acknowledgement / flag-back affordances:
- Placement within the lens:

## 6. Threshold registry

List every numeric threshold this lens depends on:

| Threshold | Value | Source (port_profile key) | Justification | Operator-reviewed? |
|---|---|---|---|---|
|           |       |                            |               |                    |

## 7. Acceptance fixtures

- Fixture file paths (under `tests/lens_contract/fixtures/`):
- Boundary fixtures for each threshold (under `tests/lens_contract/thresholds/`):
- Decision-active fixture (Section D exercise):
- Empty-B fixture (LC-9 exercise):

## 8. LC-14 signoff plan

- Practitioner identity (role, port):
- Signoff target date:
- Read-aloud script: include the four canonical questions and the lens regions
  the practitioner is expected to use to answer each one.
```

### 7.2 Lifecycle

| Phase | Required artefact | Required approval |
|---|---|---|
| Proposal | Lens Proposal Template completed | Contract owner |
| Data layer | `build<Role><Window>(summary)` function + acceptance fixtures committed | Contract owner |
| Renderer | `render<Role><Window>(state)` function + LC-1 through LC-13 passing | Contract owner |
| Manual gate | LC-14 signoff stored under `docs/governance/lens-signoffs/` | Practitioner + contract owner |
| Production | All criteria passing + signoff complete | Contract owner |

No phase begins until the previous phase is approved. No phase is skipped.

---

## 8. Amendments

This document is amended by appending a dated entry to
`docs/governance/LENS-CONTRACT-CHANGELOG.md`. Amendments must:

1. Include a version bump (`1.0 → 1.1` for additions, `1.0 → 2.0` for breaking
   changes).
2. Update the `version:` and `effective_date:` fields in this file's header.
3. Be authored or approved by the contract owner.

Silent edits to this document — even typographical — are prohibited. Every diff
gets a changelog entry.

---

*End of contract.*
