# Horizon V1 — Recommendation Lifecycle Reconciliation (v0.1)

**Status:** Reconciliation note — documentation only
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review), future M1/M2 implementers
**Authoritative inputs (merged on `main @ c7827bb`):**
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43) — §4 (7-state lifecycle)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31) — §7 (11-stage lifecycle)
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise implementation.** It reconciles
two existing models so future M1 / M2 work begins from a single,
unambiguous lifecycle map. No code is written. No `frontend/`
directory is created. No `server.py` change is proposed.

---

## 1. Executive Summary

The V1 planning corpus contains **two recommendation-lifecycle
models**, written for different audiences:

- **Component & Interaction Canon §4** defines a **7-state**
  operational lifecycle (RECOMMENDED → ACKNOWLEDGED → COMMITTED /
  DEFERRED / OVERRIDDEN / ESCALATED → EXPIRED, plus INACTION as a
  reviewer-only overlay). The Canon is UI-facing.
- **Operational Workflow Model §7** defines an **11-stage**
  detailed workflow (detected → generated → presented →
  acknowledged → accepted/resolved → deferred → overridden →
  escalated → expired → closed → reviewed). The Workflow Model is
  audit-and-process-facing.

The two are **not contradictory** — the Workflow Model is finer-
grained because audit semantics need to distinguish system steps
(detected vs generated vs presented) that the UI collapses into a
single "RECOMMENDED" state. But the corpus has not yet had **one
explicit mapping** between them. This document is that mapping.

**Reconciliation outcome:** the Canon's 7 states are the **UI
state vocabulary**; the Workflow Model's 11 stages are the
**audit / workflow vocabulary**; one Canon state maps to one or
more Workflow stages; one Workflow stage maps to exactly one
Canon state (or to `n/a` for review-only steps).

**Reconciled rule (governing future M1 / M2 implementation):**

> Components import the **Canon 7-state lifecycle**.
> Audit events emit against the **Workflow 11-stage lifecycle**.
> The reconciliation table in §4 is the only sanctioned bridge
> between the two. No new state may be added to either model
> without amending both.

**This document does not authorise implementation.** M0 remains
on hold pending Railway recovery and an explicit M0 authorisation
message. This reconciliation is design groundwork that lets M1
begin with one lifecycle truth instead of two.

---

## 2. Source Models

### 2.1 Canon §4 lifecycle (7 states + 1 reviewer designation)

UI-facing. Encodes operator-visible state vocabulary. Closed
state set; transitions enforced server-side; INACTION is a
reviewer-applied designation atop EXPIRED.

| # | State | Authored by | Glyph | Colour |
|---|---|---|---|---|
| 1 | RECOMMENDED | System | ◆ | Teal |
| 2 | ACKNOWLEDGED | Operator | ● | Green |
| 3 | COMMITTED | Operator | ■ | Green (binding) |
| 4 | DEFERRED | Operator | ⏳ amber square | Amber |
| 5 | OVERRIDDEN | Operator | ◇ hollow diamond | Purple |
| 6 | ESCALATED | Operator | ⇡ stacked diamond | Blue |
| 7 | EXPIRED | System (auto at deadline) | ○ | Muted |
| 8\* | INACTION | Reviewer (post-hoc) | ✕ | Critical |

\* Reviewer designation only; never operator-authored.

### 2.2 Workflow Model §7 lifecycle (11 stages)

Audit-and-process-facing. Encodes every workflow moment that
needs distinct audit semantics, ownership transfer, or actor
authority.

| # | Stage | Owner | Actor | Audit event |
|---|---|---|---|---|
| 7.1 | detected | system | none | `CONFLICT_DETECTED` (Phase 0.7a) |
| 7.2 | generated | system | none | `RECOMMENDATION_GENERATED` (Phase 0.7b) |
| 7.3 | presented | system → VTSO | VTSO / HM / SS | `RECOMMENDATION_PRESENTED` (Phase 0.7c) |
| 7.4 | acknowledged | VTSO | VTSO / HM | `OPERATOR_ACKNOWLEDGED` (V1 new) |
| 7.5 | accepted / resolved | VTSO / HM | VTSO / HM | `OPERATOR_ACTED` with `action_type='conflict_resolve'` (Phase 0.8a base, V1.1 extends) |
| 7.6 | deferred | HM (V1.0) | HM | `OPERATOR_DEFERRED` (Phase 0 reserved, V1.2) |
| 7.7 | overridden | HM | HM | `OPERATOR_OVERRODE` (Phase 0 reserved, V1.2) |
| 7.8 | escalated | source → target | VTSO / SS | `OPERATOR_ESCALATED` (V1 new) |
| 7.9 | expired / deadline passed | named owner at expiry | none | `DEADLINE_PASSED` (Phase 0 reserved, V1.x) |
| 7.10 | closed | resolver / overrider / HM | VTSO / HM | implied by `OPERATOR_ACTED`; `RECOMMENDATION_CLOSED` proposed |
| 7.11 | reviewed | n/a (read action) | SS / HM / Executive | `AUDIT_READ` (V1 new) |

### 2.3 Where they differ in granularity

| Granularity gap | Canon view | Workflow view |
|---|---|---|
| **System pipeline** (detected → generated → presented) | Single RECOMMENDED state | Three distinct stages with three audit events |
| **Resolution closure** (accepted vs closed) | Single COMMITTED state | accepted/resolved + closed (two stages, one implied by the other's audit event) |
| **Reviewer activity** | INACTION designation only | reviewed stage with its own audit event |
| **Detection without recommendation** | Not modelled (Canon starts at RECOMMENDED) | detected is a discrete first stage even without a recommendation emerging |

The Workflow Model intentionally separates system-side steps
that the operator never sees; the Canon collapses them because
the operator UI shows them as a single "incoming recommendation"
moment. Both are correct for their audience.

---

## 3. Canonical Lifecycle State Set

For all V1 implementation work, the operational state set is:

1. **RECOMMENDED** — system has detected a condition, generated
   a recommendation, and presented it to an operator surface.
   Operator has not yet engaged.
2. **ACKNOWLEDGED** — operator has clicked ACK. The
   recommendation is on the operator's plate; no commitment yet.
3. **COMMITTED** — operator has chosen and bound a resolution
   (the recommended option or, via OVERRIDDEN path, a different
   one). Terminal.
4. **DEFERRED** — operator has parked the recommendation with a
   reason and (optionally) a defer-until. Will re-present at
   defer-until.
5. **OVERRIDDEN** — operator has committed an alternative to the
   recommended option. Terminal. (Per Canon §10.2, override
   authority is server-enforced.)
6. **ESCALATED** — operator has routed the recommendation to a
   higher authority. The original chain forks; the receiver
   begins a new ACKNOWLEDGED → COMMITTED sub-chain.
7. **EXPIRED** — `decision_deadline` (from the Phase 0.7b §1.4.1
   pinned snapshot) has passed without ACK / DEFER / OVERRIDDEN /
   ESCALATED. System-emitted.
8. **INACTION** (reviewer designation only) — an EXPIRED record
   that a reviewer subsequently flags as having had operational
   consequence. Adds a reviewer annotation atop the existing
   EXPIRED record; does not create a new lifecycle state at
   operational time.

This is the **closed set** for V1.0 + V1.x + V1.6. Adding a new
state requires amending **both** the Canon §4 and this
reconciliation note.

---

## 4. Workflow Stage Mapping Table

Every Workflow Model §7 stage maps to exactly one Canon state
(or `n/a` for read-only). The reverse mapping (Canon → Workflow)
may be one-to-many because the Canon collapses system stages.

| Workflow stage | Canonical state | Actor | UI surface | Audit implication | Implementation milestone |
|---|---|---|---|---|---|
| 7.1 detected | RECOMMENDED | system | none operator-facing | `CONFLICT_DETECTED` emitted | M1 (read existing Phase 0.7a emit) |
| 7.2 generated | RECOMMENDED | system | none operator-facing | `RECOMMENDATION_GENERATED` with §1.4.1 snapshot | M1 (read existing Phase 0.7b emit) |
| 7.3 presented | RECOMMENDED | system → VTSO | left rail alert list, right rail decision card | `RECOMMENDATION_PRESENTED` emitted on first authenticated surface render | M1 (read existing Phase 0.7c emit) |
| 7.4 acknowledged | ACKNOWLEDGED | VTSO / HM | decision card → click ACK | `OPERATOR_ACKNOWLEDGED` (V1 new event type — emit in M1) | **M1** (introduces the new event type and write path) |
| 7.5 accepted / resolved | COMMITTED | VTSO / HM | DSW step 4 (Commit decision) or right-rail card Commit button | `OPERATOR_ACTED` with `action_type='conflict_resolve'` | M2 / M7 per Execution Plan (commit-to-backend path) |
| 7.6 deferred | DEFERRED | HM (V1.0) | DSW step 4 → Defer affordance with mandatory reason | `OPERATOR_DEFERRED` (Phase 0 reserved; V1.2 emits) | M2+ (deferred per Phase 0 reservation) |
| 7.7 overridden | OVERRIDDEN | HM only | DSW step 3 → select non-recommended option + DSW step 4 → Commit | `OPERATOR_OVERRODE` (Phase 0 reserved; V1.2 emits) | M2+ (deferred per Phase 0 reservation) |
| 7.8 escalated | ESCALATED | VTSO / SS | dedicated "Escalate" affordance on decision card | `OPERATOR_ESCALATED` (V1 new event type) | **M1 (event type defined) → M2+ (escalation UI)** |
| 7.9 expired / deadline passed | EXPIRED | system (recorded) | timeline (replay only); shift log row | `DEADLINE_PASSED` (Phase 0 reserved; V1.x emits — requires V1's pinned-deadline lifecycle per Phase 0.8b deferral note) | V1.x (Phase 0.8b deferred) |
| 7.10 closed | (terminal flag on COMMITTED / OVERRIDDEN / EXPIRED) | resolver | implicit | implied by closing event (`OPERATOR_ACTED` / `OPERATOR_OVERRODE` / `DEADLINE_PASSED`); a separate `RECOMMENDATION_CLOSED` event is proposed but not authorised | M2+ (decision on `RECOMMENDATION_CLOSED` deferred) |
| 7.11 reviewed | (no state change) | reviewer | Replay workspace, audit log right-panel tab | `AUDIT_READ` (V1 new event type) | M8+ (audit retrieval / Replay surfaces) |

**Reverse mapping (Canon → Workflow stages):**

| Canon state | Workflow stages that map to it |
|---|---|
| RECOMMENDED | 7.1 detected + 7.2 generated + 7.3 presented |
| ACKNOWLEDGED | 7.4 acknowledged |
| COMMITTED | 7.5 accepted/resolved + 7.10 closed (when closed via resolution) |
| DEFERRED | 7.6 deferred |
| OVERRIDDEN | 7.7 overridden + 7.10 closed (when closed via override) |
| ESCALATED | 7.8 escalated |
| EXPIRED | 7.9 expired + 7.10 closed (when closed via expiry) |
| INACTION | (no Workflow stage — reviewer annotation only; uses `AUDIT_READ` for the annotating session and a proposed `INACTION_DESIGNATED` event for the designation) |
| (no Canon equivalent) | 7.11 reviewed (read-only, no state change) |

---

## 5. State vs Stage Distinction

### 5.1 State

A **state** is the current operational lifecycle status of a
single recommendation, as visible to UI components.

- A recommendation is **always in exactly one Canon state** at
  any given moment.
- State transitions are the events the UI most cares about: a
  state change triggers visual changes, badge updates, sort
  order changes, glyph changes on the timeline.
- State is what `ViewSummary` (per Adapter Note §2.2) exposes
  to components.

### 5.2 Stage

A **stage** is a workflow / process moment that may or may not
correspond to a state change.

- Multiple stages may map to a single state. The three system
  stages (detected, generated, presented) all map to
  RECOMMENDED.
- A stage may produce an audit event without changing state.
  Example: the `RECOMMENDATION_PRESENTED` event fires when a
  recommendation first reaches an authenticated surface — the
  recommendation remains in RECOMMENDED state, but a Phase 0.7c
  audit row is written.
- A stage may exist that has no state at all. Example: `7.11
  reviewed` is a read action against historical records; no
  state mutates.

### 5.3 One state → multiple stages

The RECOMMENDED state covers three system stages that the UI
collapses:

```
detected     ─┐
generated    ─┼─→  RECOMMENDED  (one Canon state)
presented    ─┘
```

Three audit events fire; one UI badge appears.

### 5.4 One stage → not always a state change

The `reviewed` stage (7.11) writes an `AUDIT_READ` event but
**does not** change the underlying recommendation's state. The
operator does not see "the recommendation is now reviewed"; the
reviewer sees the review action in the audit chain.

Similarly, the `closed` stage (7.10) typically produces an audit
event but no separate state — the closure is conveyed via the
terminal state (COMMITTED, OVERRIDDEN, EXPIRED) and the closing
event's `action_type`.

### 5.5 Why this matters

If a future implementer conflates state with stage, they may try
to emit a state-change event for every stage (creating a
"PRESENTED" Canon state, for example). The reconciled rule
prevents this: Canon states are operator-visible; Workflow
stages are audit-visible; they overlap but are not the same.

---

## 6. Valid Transitions

### 6.1 Canon state transitions (Canon §4.1 reproduced + clarified)

```
RECOMMENDED ──▶ ACKNOWLEDGED ──▶ COMMITTED              (happy path)
            ──▶ ACKNOWLEDGED ──▶ DEFERRED ──▶ ACKNOWLEDGED ──▶ COMMITTED
            ──▶ ACKNOWLEDGED ──▶ OVERRIDDEN              (committed with alternative)
            ──▶ ACKNOWLEDGED ──▶ ESCALATED               (new sub-chain under receiver)
            ──▶ EXPIRED                                   (no operator engagement)
            ──▶ ACKNOWLEDGED ──▶ EXPIRED                  (engaged but no decision)

EXPIRED     ──▶ INACTION                                  (reviewer designation; new audit row, no state mutation on the original)
```

### 6.2 Invalid transitions (server-side rejection required)

- RECOMMENDED → COMMITTED (no ACK first) — the canonical path
  requires explicit acknowledgement before commitment, with the
  one exception that low-severity recommendations **MAY** ACK +
  COMMIT in a single click from the right-rail card (Canon §7.2);
  the server still records both events.
- COMMITTED → any other state — COMMITTED is terminal.
- OVERRIDDEN → any other state — terminal.
- EXPIRED → ACKNOWLEDGED / COMMITTED / DEFERRED / OVERRIDDEN /
  ESCALATED — EXPIRED is terminal at operational time; only
  reviewer can designate INACTION over it.
- DEFERRED → COMMITTED directly (without re-ACK at defer-until)
  — per Canon §4.1 the recommendation re-presents and the
  operator re-acknowledges before committing.
- Any operator-authored transition without an authenticated
  session — Canon §10.2 forbids frontend-only RBAC; the server
  is the gate.
- Out-of-order writes (Workflow stage 7.4 before 7.3, etc.) —
  Workflow Model §7 implies server-side ordering enforcement.

### 6.3 Escalation produces a sub-chain

ESCALATED is **not** a terminal state in the same sense as
COMMITTED / OVERRIDDEN / EXPIRED. The original recommendation
enters a "handed off" state, and a **new** RECOMMENDED → ...
chain begins under the receiver. The two chains are linked by
the `OPERATOR_ESCALATED` event's `original_recommendation_id`
and `target_recommendation_id` fields (proposed).

This means the original recommendation's terminal state is
recorded as ESCALATED; the receiver's resulting chain has its
own lifecycle.

---

## 7. Actor Authority Mapping

Per Permission Model §6 + Workflow Model §5 (Operational
Authority Flow) + Canon §2 + Canon §10.2 (server-side
enforcement):

| Verb | VTSO | Shift Supervisor | Harbour Master | Port Executive | Stakeholder | Marine Infra | Reviewer (post-hoc) |
|---|---|---|---|---|---|---|---|
| ACK | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | n/a |
| COMMIT (accept/resolve) | ✅ | ✅ | ✅ | ❌ (read-only on Decisions per Canon §6.2) | ❌ | ❌ | n/a |
| DEFER | ❌ (V1.0) | ❌ (V1.0) | ✅ V1.0 | ❌ | ❌ | ❌ | n/a |
| OVERRIDE | ❌ | ❌ | ✅ HM only | ❌ | ❌ | ❌ | n/a |
| ESCALATE | ✅ (→ SS / HM) | ✅ (→ HM) | ❌ (HM is the top of port escalation; external escalation is V1.x) | ❌ | ❌ | ❌ | n/a |
| Reviewer INACTION designation | ❌ | ✅ (post-shift review queue) | ✅ (incident replay) | ❌ (Executive read-only) | ❌ | ❌ | ✅ |
| Read audit / replay | ✅ (own actions in shift log) | ✅ (shift's actions in review queue) | ✅ (full audit) | ✅ (aggregated only — Executive mode) | ❌ (out of scope) | ❌ (scope-limited) | ✅ |

V1.x **MAY** extend DEFER authority to VTSO (per Workflow Model
§7.6 open question); the V1.0 default is HM-only.

The authority check is **server-side** per Canon §10.2. The UI
displays affordances per the role pill (a hint), but the gate is
the API layer.

---

## 8. Audit Event Mapping

### 8.1 Phase 0 already-emissible events

These events are live in Beta 10 backend (production emission is
no-op while Stage E-prod is paused, but the code paths exist).

| Lifecycle stage | Audit event | Phase | Notes |
|---|---|---|---|
| 7.1 detected | `CONFLICT_DETECTED` | Phase 0.7a | Emits on every detected conflict |
| 7.2 generated | `RECOMMENDATION_GENERATED` | Phase 0.7b | Carries §1.4.1 decision-time snapshot (cascade, options, deadline, model confidence) |
| 7.3 presented | `RECOMMENDATION_PRESENTED` | Phase 0.7c | Scoped to surface + actor_handle |
| 7.5 (subset) accept | `OPERATOR_ACTED` with `action_type` ∈ `{whatif_apply, whatif_clear, send_brief}` | Phase 0.8a | Currently covers what-if + brief actions; **NOT yet** `conflict_resolve` |

### 8.2 Phase 0 reserved (deferred) events

Reserved in the closed-set CHECK constraint but not yet emitted.

| Lifecycle stage | Audit event | Status |
|---|---|---|
| 7.6 deferred | `OPERATOR_DEFERRED` | Reserved; V1.2 emits |
| 7.7 overridden | `OPERATOR_OVERRODE` | Reserved; V1.2 emits |
| 7.9 expired | `DEADLINE_PASSED` | Reserved; V1.x emits (requires pinned-deadline lifecycle work — Phase 0.8b was deferred) |
| (session terminus) | `SESSION_ENDED_WITHOUT_ACTION` | Reserved; V1.x emits |

### 8.3 V1 candidate events (new types proposed)

Not yet defined in the Phase 0 closed-set. Adding any of these
requires a CHECK-constraint expansion and a separate authorised
backend change.

| Lifecycle stage | Audit event | Required by |
|---|---|---|
| 7.4 acknowledged | `OPERATOR_ACKNOWLEDGED` | M1 (ACK write path) |
| 7.5 (extended) conflict resolve | `OPERATOR_ACTED` with `action_type='conflict_resolve'` | M7 (apply-decision endpoint) |
| 7.8 escalated | `OPERATOR_ESCALATED` | M2+ (escalation UI + write) |
| 7.10 closed (optional separate event) | `RECOMMENDATION_CLOSED` | M2+ (decision deferred) |
| 7.11 reviewed | `AUDIT_READ` | M8+ (audit retrieval) |
| INACTION designation | `INACTION_DESIGNATED` | V1.6 (reviewer surface) |
| Mode entry (Canon §6.3) | `REGULATOR_MODE_ENTERED` | V1.6 (Replay) |
| Shift handover | `SHIFT_OPENED` / `SHIFT_CLOSED` / `HANDOVER_CREATED` / `HANDOVER_ACCEPTED` | M2+ / V1.x |
| Incident replay | `INCIDENT_OPENED` / `INCIDENT_REPLAYED` | M8 / V1.6 |

Closed-set CHECK-constraint additions **MUST** be reviewed as
Phase-0-style schema changes (alembic migration, regression-gate
update, authorised by Tony before merge). They are out of scope
of this reconciliation note and out of scope of M1.

### 8.4 Event types that should NOT exist yet

These are tempting but premature. Any future PR proposing them
should be rejected unless a specific operational requirement is
documented:

| Tempting event | Why deferred |
|---|---|
| `RECOMMENDATION_OBSOLETED` | Workflow Model §7.2 open question — current Phase 0.7b answer is "different rec → different recommendation_id → re-emit" rather than an obsoletion event |
| `DEADLINE_WARNING` (n minutes before expiry) | Notification surface, not a lifecycle event; should live in a notifications system, not the audit chain |
| `OPERATOR_VIEWED` (any session render) | Too noisy; `RECOMMENDATION_PRESENTED` already captures first-render |
| `RECOMMENDATION_REOPENED` | COMMITTED / OVERRIDDEN are terminal per §6.2; reopening would mutate a closed audit record |
| `CONSEQUENCE_OBSERVED` (outcome of a committed decision) | Worthwhile in V1.6+ for closed-loop analytics, but not a recommendation-lifecycle event — separate concern |

---

## 9. UI Mapping

Per Canon §3 (closed colour set, closed glyph alphabet) + Canon
§4 (state-to-glyph mapping):

| Canon state | Pill label | Glyph | Colour token | Operational surface | Replay surface |
|---|---|---|---|---|---|
| RECOMMENDED | `RECOMMENDED` (outline teal) | ◆ filled diamond | `--info` (teal) | Left rail alert list + Right rail decision card | System swimlane glyph |
| ACKNOWLEDGED | `ACK` (tinted green) | ● filled circle | `--success` | Right rail decision card with ACK badge | Operator swimlane glyph |
| COMMITTED | `COMMITTED` (tinted green, filled) | ■ filled square | `--success` | Right rail card (terminal); shift log row | Operator swimlane terminal glyph |
| DEFERRED | `DEFERRED` (tinted amber) | ⏳ amber square (filled) | `--warning` | Right rail card with defer-until visible; re-presents at defer-until | Operator swimlane glyph + amber connector to re-present |
| OVERRIDDEN | `OVERRIDE` (tinted purple) | ◇ hollow diamond | `--purple` (`#A78BFA`) | Right rail card with OVERRIDE badge; decision sheet shows both rejected + chosen | Operator swimlane hollow diamond + thread back to System swimlane |
| ESCALATED | `ESCALATED` (tinted blue) | ⇡ stacked diamond | `--blue` (`#60A5FA`) | Right rail card with both originator + receiver badges | Operator swimlane stacked-diamond glyph + fork to receiver's lane |
| EXPIRED | `EXPIRED` (outline muted) | ○ empty circle | `--text-muted` | Not on operational surfaces (Replay-visible only); shift log row | Greyed glyph on owner's swimlane |
| INACTION | (reviewer-only label `INACTION`) | ✕ filled cross | `--critical` | **Never appears in operational mode** (Canon §3.5) | Reviewer swimlane annotation overlay |

Per Canon §3.5 reviewer-only semantics: INACTION pills, audit-
hash chips, and chain-verification status **MUST NOT** appear in
operational mode.

---

## 10. M0 Boundary

Per M0 Scope Proposal §10 + §14 + §15 + Canon §7.4 + §11.5 + §11.1:

### 10.1 What M0 may display

- Static / demo lifecycle states only, rendered against
  `frontend/src/data/sample.js` (shaped as `ViewSummary` per
  Adapter Note §2.2)
- All seven Canon states visible on the demo (one recommendation
  per state in the sample, so the visual treatment of each glyph
  / pill / colour is verifiable)
- DSW five-step flow navigation as a client-only state machine
  (no backend writes)
- **A "DEMO" pill** (per Canon §7.4) on every recommendation
  card / DSW step that is reading from `sample.js`

### 10.2 What M0 MUST NOT do

- **No real lifecycle writes** — no `OPERATOR_ACKNOWLEDGED`,
  `OPERATOR_ACTED`, `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`,
  `OPERATOR_ESCALATED` events emitted; no HTTP calls to any
  backend
- **No audit writes** — the audit module is not imported into
  M0 frontend code (M0 frontend has no Python at all)
- **No backend state machine** — the seven-state machine lives
  in the backend, not the frontend; M0 frontend renders state
  values but does not transition them
- **No real action semantics** — clicking ACK does not record
  ACK; clicking COMMIT does not commit; clicking DEFER does not
  defer; clicking OVERRIDE does not override; clicking ESCALATE
  does not escalate
- **No false-state UI** — no "Audit trail preserved" text on
  the DSW step 5; no "Decision applied to live schedule"; M0
  honest framing per UX/UI Handoff Validation §9.4

### 10.3 Demo labels required

- "DEMO" pill near the decision ID in every DSW header (Canon §7.4)
- "DEMO" overlay on the right-rail decision card if the source
  data is `sample.js`
- "DEMO" or "SIMULATED" indicator on the conditions ribbon if
  the conditions block is static
- "(M0 placeholder)" text in the right-rail Audit Log tab
- A persistent banner along the top edge of the V1 sandbox URL:
  "Horizon V1 — sandbox · demo data · not for operational use"

### 10.4 What M0 audit log displays

Nothing operational. The Audit Log right-panel tab displays the
literal string "(M0 placeholder)" or shows a static fixture of
session-only client-side events labelled clearly as such. No
chain hash, no chain-verification status, no per-event audit
indicator in M0.

---

## 11. M1 / M2 Implications

### 11.1 M1 — read-only API integration

M1 introduces:

- The adapter layer per Adapter Design Note (consumes Phase 0
  `RECOMMENDATION_GENERATED` + `RECOMMENDATION_PRESENTED` events
  via `/api/summary`)
- Read of system-side audit events from `/api/summary` (already
  present in the response)
- **`OPERATOR_ACKNOWLEDGED` write path** — first new lifecycle
  audit event V1 introduces (requires backend addition to
  Phase 0 CHECK constraint + a new endpoint `POST /api/ack` or
  similar — out of scope of M1 unless explicitly extended)
- ACK is the only write authorised in M1 (per M0 Scope Proposal
  §6.3 "M1 must not introduce write actions" — see open question
  §12.1 below)

### 11.2 M2 (or later) — action / audit lifecycle becomes real

M2+ introduces:

- COMMIT path (`OPERATOR_ACTED` with `action_type='conflict_resolve'`)
- DEFER path (`OPERATOR_DEFERRED` — Phase 0 reserved type)
- OVERRIDE path (`OPERATOR_OVERRODE` — Phase 0 reserved type)
- ESCALATE path (`OPERATOR_ESCALATED` — new event type, requires
  CHECK-constraint extension)
- Apply-decision endpoint design (new — not in Beta 10)
- Authority enforcement at the API layer (per Canon §10.2)

### 11.3 Replay implementation (V1.6 / M8+)

Replay introduces:

- `AUDIT_READ` event type
- INACTION designation surface (reviewer-only)
- Replay workspace per Canon §8
- Three-mode system (Operational / Executive / Regulator) with
  mode-switch audit event for Regulator mode entry
- Chain-verification UI (Canon §10.3 — three states: verified /
  partial / broken)
- `INCIDENT_OPENED` / `INCIDENT_REPLAYED` event types

### 11.4 EXPIRED stage requires Phase 0.8b reactivation

`DEADLINE_PASSED` is Phase 0 reserved but was **deferred** in
Step 0.8b (per session history). Activating it in V1.x requires:

- Server-side pinned-deadline lifecycle (a recommendation's
  `decision_deadline` from the §1.4.1 snapshot must drive an
  expiry tick — not yet implemented)
- A separate authorised Phase 0.8b-like task
- Updates to the Workflow Model §7.9 open question (what happens
  when a deadline passes — auto-escalate? severity uprank?
  auto-close?)

EXPIRED is therefore a **V1.x state**, not M1 or M2. M1 may
display EXPIRED in the demo via `sample.js`, but real EXPIRED
emission requires V1.x backend work.

---

## 12. Open Questions

### 12.1 ACK as a write in M1?

M0 Scope Proposal §10 says "M0 makes ZERO HTTP calls". M1 read-
only API integration is a clear next step. But where does the
first **write** belong — M1 (extending M0's read-only posture
with one write, ACK) or M2 (preserving M1 as strictly read-only)?

**Proposed default:** M1 stays read-only. ACK write path is M1.5
or M2 — separately authorised. The benefit: M1's adapter +
fetch wrapper + polling hook can ship and stabilise without
backend write coupling.

### 12.2 Closed event — separate or implied?

Workflow Model §7.10 proposes a separate `RECOMMENDATION_CLOSED`
event for clarity. Phase 0.8a model is "closure is implied by
the closing action's `action_type`". Canon §4 has no terminal
"closed" state.

**Proposed default:** stick with the Phase 0.8a model. Closure
is implied by the closing event's `action_type`. No separate
event type. Revisit only if V1.6 Replay needs a unified "this is
the closure moment" timestamp not already implied.

### 12.3 INACTION designation event type

Canon §4 row 8 defines INACTION as a reviewer overlay. No event
type yet exists for the designation itself. A `INACTION_DESIGNATED`
event (linking the EXPIRED record + the reviewer + the rationale
+ a harm reference) is proposed.

**Proposed default:** define `INACTION_DESIGNATED` as a V1.6
event type. Adding it to the Phase 0 CHECK constraint is part
of the V1.6 Replay workstream.

### 12.4 Auto-actions on EXPIRED?

Workflow Model §7.9 asks: "What's the operational consequence of
a passed deadline besides the audit row? Does the recommendation
get auto-escalated? Does its severity uprank? Does the system
auto-close it?"

**Proposed default for V1.0:** no auto-action. EXPIRED is a
terminal state; the recommendation drops out of active queues
and appears in the shift log + replay. The reviewer may
designate INACTION post-hoc, but the system does nothing
automatically. Revisit in V1.x if operational testing surfaces
specific cases where auto-escalation would prevent harm.

### 12.5 Multi-step escalation chain modelling

Workflow Model §7.8 asks: when VTSO → SS → HM, is this one
escalation that transitions target, or two chained escalations?
Canon §6.3 reconciliation says: each escalation forks a new
chain.

**Proposed default:** each escalation event is a new
`OPERATOR_ESCALATED` with its own `original_recommendation_id`
and `target_recommendation_id`. A VTSO → SS → HM chain produces
two `OPERATOR_ESCALATED` events and three linked recommendation
sub-chains. Audit-trail clarity wins.

### 12.6 Defer authority — VTSO or HM-only?

Workflow Model §7.6 lists HM (V1.0); VTSO (V1.x, possibly).
Canon §2.3 does not specify authority.

**Proposed default:** HM-only in V1.0 (Workflow Model wins).
V1.x decision based on operational feedback.

### 12.7 Override leaves original as OVERRIDDEN or closed?

Workflow Model §7.7 open question. Canon §6.1 implies original
recommendation goes to OVERRIDDEN (terminal); HM's chosen
alternative is conveyed via the `OPERATOR_OVERRODE` event's
payload (rejected recommendation + chosen alternative).

**Proposed default:** the original recommendation enters
OVERRIDDEN terminal state. The HM's alternative is recorded in
the same `OPERATOR_OVERRODE` event's payload, not as a new
recommendation. The new chosen option becomes operationally
binding through the override event itself.

---

## 13. Recommendations

### 13.1 Adopt the reconciliation table as authoritative

The mapping in §4 is the single sanctioned bridge between Canon
§4 and Workflow Model §7. Any future PR — M1, M2, V1.x — that
touches lifecycle handling **MUST** cite this section and honour
the mapping.

### 13.2 Canon = UI state; Workflow = audit / process

Implementation rule (per §1 Executive Summary): components
import the Canon 7-state lifecycle; backend / audit emits
against the Workflow 11-stage lifecycle. Adapter Design Note §5
already establishes the adapter as the single seam — this
reconciliation extends that seam to lifecycle vocabulary.

### 13.3 Adding a new state requires amending both models

New lifecycle states (e.g. a future `PENDING_AUTHORITY` state if
multi-step authority workflows surface) **MUST** be added to
**both** the Canon and the Workflow Model, with this
reconciliation note amended to reflect the new mapping. Silent
drift between the two models is the failure mode this document
exists to prevent.

### 13.4 M0 honours the closed state set

M0's `sample.js` displays only Canon states. The seven states
are rendered; INACTION is rendered only on the demo's reviewer-
mode surface (if M0 ships one — likely it does not in the
minimal scope). No state outside the closed set appears in M0.

### 13.5 ACK as the M1.5 boundary

The reconciliation flags ACK as the first write that V1
introduces. Whether ACK ships in M1 (extending M1's read-only
posture) or M1.5 / M2 (preserving M1 strictly read-only) is an
open M1 authorisation decision. Default per §12.1: M1.5 or M2.

### 13.6 Defer Phase 0.8b reactivation until V1.x

EXPIRED state requires the pinned-deadline lifecycle work that
was deferred in Step 0.8b. M1 displays EXPIRED via `sample.js`
mock; real EXPIRED emission requires V1.x backend work with its
own Tony authorisation.

### 13.7 Event-type additions require schema review

`OPERATOR_ACKNOWLEDGED`, `OPERATOR_ESCALATED`, `AUDIT_READ`,
`INACTION_DESIGNATED`, `INCIDENT_OPENED`, `INCIDENT_REPLAYED`
are all V1 candidate event types not yet in the Phase 0 CHECK
constraint. Each addition is a backend schema change requiring
its own Phase-0-style alembic migration + regression-gate
update + explicit Tony authorisation. **None** of these
additions are authorised by this reconciliation note.

### 13.8 Reconciliation is documentation, not authorisation

This note resolves a planning ambiguity (two lifecycle models →
one reconciled mapping). It does not authorise M0 implementation,
does not provision Railway, does not change any runtime file,
does not modify any audit code, and does not unblock anything
that was previously blocked. Its value accrues at M1 / M2 /
V1.6, when implementers reach for "which lifecycle?" and find
the answer here.

---

## End of reconciliation note

**Status:** v0.1 reconciliation — documentation only
**Implementation status:** None
**Next action:** ChatGPT engineering review; then Tony's
decision on merging. Once merged, the document is the canonical
bridge between Canon §4 and Workflow Model §7 for all future
V1 implementation work.

The fourteenth (post-merge: fifteenth) V1 planning document.
Railway remains in outage; M0 implementation remains paused.
