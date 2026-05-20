# Horizon V1 — Foundation Review Pack (v0.1)

**Status:** Stakeholder review pack — V1 foundation summary
**Document version:** 0.1
**Date:** 2026-05-15
**Audience:** AMS leadership, Horizon product/engineering, operational
maritime stakeholders, future Claude Design / CX handoff
**Foundation documents (all on `main`):**
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30, `4c4940f`)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31, `6d09b3d`)
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` (PR #32, `3e747dc`)
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md` (PR #33, `406de24`)
- `HORIZON_V1_IMPLEMENTATION_STRATEGY_v0.1.md` (PR #34, `e2284d4`)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

The Horizon V1 foundation — 8,228 lines across five v0.1 design
documents — is now on `main` and ready for stakeholder review. This
document is the **concise review pack**: a synthesis that allows
reviewers to assess the foundation without reading 8,228 lines
linearly.

### What the V1 foundation now defines

| Document | Question | Lines |
|---|---|---|
| Permission Model | WHO uses Horizon and WHAT they can do | 1,041 |
| Workflow Model | HOW Horizon is operationally used | 1,654 |
| Screen Architecture | WHERE workflows live in the application | 1,782 |
| Information Architecture | WHAT information flows through Horizon | 1,934 |
| Implementation Strategy | HOW V1 should be engineered | 1,817 |
| **Total** | — | **8,228** |

Together they define a complete V1 design: the role catalogue, the
operational lifecycle, the screen surfaces, the information flows,
and the engineering execution strategy that takes Horizon from a
Beta 10 demo to a production operating system for port operations.

### Why this matters

- **Commercial readiness.** Horizon V1 is the production-ready
  successor to Beta 10. The foundation defines what "production-
  ready" means in concrete operational terms.
- **Operational defensibility.** V1 introduces structured audit,
  role-based access, replay, and escalation — the substrate of
  regulatory and insurer review.
- **Engineering predictability.** With the foundation locked, V1
  implementation work proceeds against a stable target, not a
  shifting one.
- **Risk discipline.** Beta 10 stays protected throughout V1 work;
  the Phase 0 regression gate prevents drift.
- **Strategic clarity.** Each major architectural decision is
  documented, traceable, and reviewable rather than evolving by
  accretion.

### What is not yet authorised

This review pack does **NOT** authorise:

- V1 implementation (no V1 code, schema, API, or UI)
- Stage E-prod activation (production audit emission remains paused)
- Changes to Beta 10 (the demo baseline is protected)
- Changes to PRs #27, #28, #29 (preview-deploy DO-NOT-MERGE
  artefacts; closed without merge when V1 ships)
- Customer V1 deployments (the foundation must be accepted first)

The foundation is **input to the review process**, not output of one.
Implementation authorisation is a separate decision that follows
review acceptance.

---

## 2. V1 Foundation Overview

Each foundation document carries a sharp purpose. None duplicates the
others; each consumes the prior documents as authoritative for their
domain.

### Permission Model — WHO

Defines six V1 roles (VTSO, Harbour Master, Shift Supervisor, Port
Executive, Port Stakeholders, Marine Infrastructure), a 25-permission
catalogue, a 17-scope-rule system, and a build-priority ordering
(VTSO first; Marine Infrastructure as a permission, not a standalone
role, in V1.0). Establishes role / permission / scope as distinct
primitives; rejects the conflation that creates most RBAC drift.

**Key contribution:** the foundational contract for authority.

### Operational Workflow Model — HOW

Defines the temporal / coordination state machines for V1: shift
lifecycle (8 substates), recommendation lifecycle (11 stages),
escalation paths (4 canonical), conflict ownership transfers, defer
/ override workflows, executive review cadences, incident replay
flow, and stakeholder coordination. Maps each workflow moment to an
audit event.

**Key contribution:** time as a first-class operational concept;
single-ownership invariant.

### Screen Architecture — WHERE

Defines the structural projection of workflows into UI: 7
application domains, persistent left rail + top bar, role-specific
consoles (VTSO operational, SS handover, HM authority, Executive
dashboard, Stakeholder mobile, Replay & Incident Review), device
strategy (desktop / tablet / mobile per role), notification model.
Explicitly NOT visual design — structure only.

**Key contribution:** the spatial / navigational projection of the
authority and workflow contracts.

### Information Architecture — WHAT

Defines the substance Horizon operates on: 17 information domains,
per-domain source-of-truth model, operational data flow (10
stages), real-time vs historical separation, 17 × 6 role-scoped data
projection matrix, sensitive-information boundaries, integration
boundaries (including the Kyber boundary), audit information model,
replay information requirements, analytics metrics, AI consumption
boundaries, data quality and confidence handling.

**Key contribution:** explicit source attribution; explicit AI
prerequisites; de-identified resource model.

### Implementation Strategy — HOW TO BUILD

Defines the engineering execution: Beta 10 preservation strategy, 5
core architectural primitives, RBAC migration path, recommendation
lifecycle engine strategy, replay & audit evolution, API & service
boundary, integration strategy, frontend evolution, deployment
strategy, multi-tenant strategy, infrastructure & observability,
Claude Code governance rules, 10 specific rewrite-vs-preserve
decisions, 7-phase build sequence (V1.0 through V1.6), 20 open
questions for V1.0 architecture review.

**Key contribution:** the engineering-execution contract; explicit
Claude Code governance.

---

## 3. Key Strategic Decisions

Across the five documents, the foundation surfaces eight strategic
decisions that frame everything else.

### 3.1 Beta 10 remains protected

`phase-0-complete @ 4ad4aae` is the immutable Beta 10 baseline.
`tests/test_beta10_regression.py` is the active CI gate (46/46
passing). Beta 10 receives bug fixes only; no feature evolution.
V1 ships in parallel, not on top.

### 3.2 V1 is separate from Beta 10

Different branches (`v1/*` for V1; `main` continues for Beta 10),
different deploy targets (separate Railway projects), different
authentication model, different frontend. A V1 commit cannot break
Beta 10; a Beta 10 fix cannot destabilise V1.

### 3.3 RBAC is required before meaningful production audit

The Phase 0 deferred event types (`DEADLINE_PASSED`,
`SESSION_ENDED_WITHOUT_ACTION`, `OPERATOR_DEFERRED`,
`OPERATOR_OVERRODE`) require V1's per-user identity to be
operationally meaningful. Persisting audit emission with Beta-10-
shaped actor attribution (every action attributed to the shared
`O-1` handle) would record evidentiary data of limited value.

### 3.4 Server-side filtering is mandatory

Permissions are enforced at the API layer, not in the frontend.
Hidden destinations don't appear greyed-out — they don't appear at
all. A role that doesn't have visibility of a domain doesn't
receive the data in the response payload.

### 3.5 Operational ownership is exclusive

A conflict, recommendation, or escalation has exactly one owner at
any moment. Ownership transfers are explicit and auditable. There
is no "joint ownership" or "shared ownership" — only delegation,
escalation, or handover, each producing an audit event.

### 3.6 Replay becomes operational evidence

The audit ledger is the operationally-authoritative substrate for
post-hoc reconstruction. Every decision must be reconstructible.
Replay surfaces are designed to mirror live operational views (the
principle of cognitive continuity between live and replay).

### 3.7 Kyber remains separate

Horizon V1 does not depend on Kyber code. Future integration
(V1.5+) is API/event-based with a de-identified / capability-based
resource model. The boundary is non-negotiable and enforced at the
engineering layer.

### 3.8 Stage E-prod remains paused

Production audit emission stays paused until V1's user/role
identity is on every audit row. The trade-off is commercial
demonstration vs evidentiary cleanliness; both paths are
defensible; the operator decides when and which.

---

## 4. Operational Model Summary

Six V1 roles, ordered by build priority, with one-paragraph operational
purpose each.

### VTSO (Build Priority 1)

The always-on operational user. Real-time vessel management,
conflict resolution, what-if exploration, weather response. Closest
to Beta 10's current operator. Full operational density across
vessels / berths / conflicts / recommendations / pilotage / towage.
Cannot approve port closure, override recommendations, or sign off
Port Briefs (those are HM authority).

### Harbour Master (Build Priority 2)

The authority figure. Full visibility with drill-down on demand.
Approves port closures, overrides recommendations, signs off Port
Briefs, reviews the full audit trail. All authority actions
require structured reason codes and secondary confirmation. The
Harbour Master is the terminal authority within Horizon's
operational scope.

### Shift Supervisor / Watch Lead (Build Priority 3)

The supervisory layer. Manages shift handovers, escalates to the
Harbour Master, reviews VTSO actions. Distinct from VTSO in that
the SS is action-low and oversight-high. Full visibility within
their shift's scope; handover history within their own scope.
Critical missing role from Beta 10 — without it there is no
structured handover or supervisory review.

### Port Executive (Build Priority 4)

The strategic oversight role. Weekly / monthly cadence — not
always-on. Sees aggregated metrics, trend charts, Port Brief, and
incident summaries. **Cannot take operational actions.** Reads
emit `AUDIT_READ` events so the ledger captures executive
oversight.

### Port Stakeholders (Build Priority 5)

Pilots, towage, mooring crews, terminals, shipping lines. Mobile-
first; narrow scope (own assignments only). Two operational verbs:
confirm assignment, report delay/issue. For pilots specifically,
Kyber owns detailed rostering; Horizon's stakeholder role is the
broader port context feed.

### Marine Infrastructure (Build Priority 6)

Manages berth closures, maintenance windows, infrastructure
outages. **Recommended as a permission within Harbour Master / SS
roles in V1.0**, becoming a standalone role only when workflow
complexity justifies it.

---

## 5. Workflow Summary

Six workflow surfaces define how operations actually run.

### Shift Lifecycle

A shift opens (SS-initiated or first-VTSO-login fallback), runs
steady-state, may include mid-shift delegation, approaches end-
of-shift preparation, ends with handover creation, handover
acceptance, and formal shift close. Each transition emits an
audit event (`SHIFT_OPENED`, `HANDOVER_CREATED`,
`HANDOVER_ACCEPTED`, `SHIFT_CLOSED`).

### Recommendation Lifecycle

A recommendation moves through 11 stages: detected → generated →
presented → acknowledged → resolved (or deferred / overridden /
escalated) → expired / closed → reviewed. Each stage has an
owner, allowed actors, expected time behaviour, audit event, and
notification behaviour. The Phase 0.7b decision-time snapshot
captures the engine's view at generation; the full lifecycle
extends through V1.2 (defer/override) and V1.3 (escalation).

### Conflict Ownership

A conflict is owned by exactly one actor at any moment: system at
detection, VTSO at presentation, SS during escalation, HM during
authority decision, system again at closure. Ownership transfers
are explicit; multi-watch ports use scope-based assignment; quiet-
period conflicts remain owned by their assigned VTSO until acted
on or aged out.

### Escalation

Four canonical paths: VTSO → SS (primary), SS → HM (authority),
direct VTSO → HM (emergency bypass), multi-step (V → SS → HM).
All escalations carry structured reason codes (closed catalogue,
9 codes in V1.0 baseline). Bypass escalations are flagged in
audit. Every escalation has a defined closure path.

### Handover

A 9-substage workflow: pre-handover review → outgoing handover
creation → pending-item inventory snapshot → escalation queue
transfer → incoming read and acceptance → clarification (if
needed) → formal ownership transfer → handover audit trail
captured. The handover is the highest-frequency audit-significant
event in a port's operational lifecycle.

### Override / Defer

HM-only in V1.0. Both require mandatory reason codes from closed
catalogues (6 codes each for defer and override in V1.0). Override
supersedes the original recommendation permanently; defer
re-presents at `defer_until` or emits `DEADLINE_PASSED` if
unrevisited. Override audit captures both the chosen alternative
and the original engine recommendation.

### Replay

Post-hoc reconstruction of operational decisions from the audit
ledger. The replay surface mirrors the live VTSO console for the
selected timeline position. Replay outputs: timeline view,
authority chain visualisation, decision-snapshot viewer, ownership
transfer history, chain integrity verification.

---

## 6. Screen / UX Summary

Six primary operational surfaces.

### VTSO Operational Console

Four-region desktop layout: Live Vessel Map (always visible),
Active Movement Timeline (collapsible), Coordination Stack
(conflict + recommendation queues, always visible),
Operational Context Strip (weather / tides / pilotage / towage,
collapsible). Action affordances on each conflict detail.
Pending-action tile always visible. Designed for sustained
attention.

### Shift Supervisor Console

Three-region desktop layout: Watch Overview (active VTSOs, their
workload), Coordination Stack (read access + review filters),
Supervisory Workspace (handover composer, escalation queue, VTSO
action review). Map prominence reduced vs VTSO; focus is on
oversight, not action.

### Harbour Master Console

Three-region desktop layout: Authority Decision Queue,
Operational Summary (compact), Audit / Replay Access (one click
away). All authority actions require structured reason codes and
secondary confirmation. The override approval surface displays the
original recommendation, alternatives, decision snapshot, and
HM's chosen alternative side-by-side.

### Executive Dashboard

Single-page-scrollable with five sections: Headline KPIs, Trend
Surfaces, Incident Summaries, Operational Risk Indicators, Replay
& Drill-In Surface. Mobile-friendly primary form; desktop
secondary. Executive does NOT see real-time conflict detail or
free-text operational notes.

### Stakeholder Mobile Feed

Single-screen mobile-first surface for pilots / towage / mooring /
terminals / shipping lines. Assignment feed; confirm CTA; report
delay form; report issue form. Pilotage detail is de-identified
(capability descriptor, not named individual). Kyber boundary
preserved (V1 stakeholder role is the broader port context;
Kyber owns rostering detail).

### Replay & Incident Review

The most cognitively demanding desktop-only surface. Replay
timeline at top; reconstructed operational console below;
side panels for recommendation chains, escalation chains, actor /
ownership history, audit chain integrity. Incident package
export (JSON + PDF + hash proof).

---

## 7. Information Architecture Summary

The substrate the other documents act on.

### Data Domains

17 distinct domains: vessels, berths, movements, conflicts,
recommendations, actions, escalations, shifts, handovers, weather/
tides, pilotage, towage, stakeholder assignments, incidents, audit
events, executive metrics, admin configuration. Each has a single
domain owner responsible for semantic correctness.

### Source-of-Truth Model

Every domain has named authoritative source(s), explicit fallback
paths, and confidence handling. Vessel data: AISStream primary →
MST cache → QShips → simulation. Tides: BOM primary → cosine
fallback. Source attribution preserved on every record so
downstream consumers see "where this data came from."

### Role-Scoped Projection

17 domains × 6 roles matrix. Every cell defines visibility level:
Full / Filtered / Aggregated / De-identified / None. The same
underlying operational state produces different projections per
role. Filtering happens at the API layer, server-side, before the
response leaves the backend.

### Sensitive Information Boundaries

Ten categories codified: pilot identity (de-identified to capability
descriptor), operational notes (scoped to audit subject), commercial
impact (aggregated for executive), security-sensitive incidents
(clearance-restricted), internal executive analysis (HR boundary),
audit detail (role-scoped depth), stakeholder commercial commitments
(outside Horizon scope), maintenance constraints (vendor identity
restricted), personal data (minimised throughout).

### Audit and Replay Information Model

Append-only ledger per Phase 0.5b (hash-chained per-tenant). Every
event payload carries `user_id`, `role`, `scope`, `session_id`
(V1 extension; no schema migration required). Replay reconstructs
operational state from `RECOMMENDATION_GENERATED` decision-time
snapshots plus surrounding audit events. Chain integrity is the
verification mechanism (`verify_chain`).

### AI Governance Boundary

AI consumption is governed by principle 8: AI can consume
information only after source, scope, and lineage are clear. V1
establishes the substrate; AI features are V1.x+ deferred. AI must
not consume: personal identity, sensitive notes, unscoped
stakeholder data, raw audit payloads beyond permission boundary,
cross-tenant data, security-sensitive incident detail, commercial
detail beyond aggregated form, internal executive analysis.

---

## 8. Implementation Strategy Summary

How to engineer V1.

### V1 Target Architecture

External sources → normalisation layer → operational state model →
decision engine (conflicts + recommendations) → role-scoped API →
clients (web + mobile + integration) → audit capture → replay /
analytics. Modular monolith for V1.0; microservices deferred to
V2+.

### Five Core Architectural Primitives

1. Tenant-scoped everything
2. Session-carries-identity (user / tenant / port / role / scope /
   session / shift on every request)
3. Server-side authorisation (frontend reflects backend decisions)
4. Append-only audit (no row mutation; chain integrity)
5. Single operational ownership (exclusive; auditable transfers)

### Build Sequencing — Seven Phases

| Phase | Deliverable |
|---|---|
| V1.0 | Foundation: RBAC schema, role-scoped `/api/v1/summary`, login flow, VTSO console foundational layout |
| V1.1 | VTSO action surface: acknowledge, resolve, run-what-if, escalate (creation only) |
| V1.2 | HM authority surface: override, defer, port closure, sign-off; deadline scanner; Phase 0 deferred events become live |
| V1.3 | SS console: handover composer, acceptance, escalation queue, VTSO action review |
| V1.4 | Executive dashboard: KPIs, trends, incident summaries |
| V1.5 | Stakeholder mobile surface; Kyber integration boundary contract |
| V1.6 | Replay & Incident Review; incident package export |

### Environment Strategy

Separate Railway projects for Beta 10 (`horizon-prod`, unchanged)
and V1 (`horizon-v1-prod` + `horizon-v1-preview`). Branch-to-
environment mapping enforced. Promotion gates at every merge. No
direct-to-production deploys, ever.

### Claude Code Governance

Default posture: explicit-authorisation-required. Specific lists in
Implementation Strategy §16 define what Claude may propose without
authorisation, what requires authorisation, what is never
implemented without review. Branch naming conventions, PR scope
rules, merge approval rules all codified.

### No-Direct-to-Production Rule

All V1 changes flow through preview environments with explicit
acceptance gates before production. The Phase 1.2 audit activation
pattern (Stages B → C → D → Drill before E) generalises to V1
work.

---

## 9. Major Open Questions for Stakeholder Review

The five foundation documents collectively surface ~80 open
questions. The most consequential for stakeholder review fall into
seven groups.

### 9.1 Operational Authority

- **Single supervisor per shift vs co-supervision** for larger
  ports? (Workflow §17.1)
- **Can a VTSO acknowledge another VTSO's pending item** in a busy
  watch? (Workflow §17.2)
- **Authority delegation** — when an HM delegates to SS as Acting
  HM, is that transient role assumption or a separate role grant?
  (Workflow §17.11)
- **Smaller-port role consolidation** — should one user hold both
  HM and SS roles? (Permission Model §13.3)

### 9.2 Shift & Handover

- **Canonical shift boundary** — wall-clock, login, or explicit
  start-shift action? (Workflow §17.3)
- **Mid-shift delegation** — V1.0 feature, V1.x phase-in, or
  omitted entirely? (Workflow §17.17)
- **Handover concurrency** — what if outgoing SS hasn't finished
  writing when incoming SS arrives? (Workflow §17.9)
- **Handover edit-after-write** — can the outgoing SS edit after
  submitting but before acceptance? (Workflow §9.9)

### 9.3 Escalation, Defer, Override

- **Override permanence** — does an override require eventual
  review / closure? (Workflow §17.5)
- **Deferral revisit cadence** — system-prompted at `defer_until`,
  operator-prompted before then? (Workflow §17.4)
- **High-severity acknowledgement timeout** — what's the threshold
  beyond which the system escalates automatically? (Workflow
  §17.10)
- **Escalation event naming** — `OPERATOR_ESCALATED` (verb-named)
  vs `ESCALATION_CREATED` (artefact-named)? (Workflow §17.14)
- **Cross-port escalation** — when multi-port tenants exist, can
  HM escalate to tenant-level authority? (Workflow §17.6)

### 9.4 Stakeholder Access

- **Stakeholder authentication model** — Horizon-direct, federated
  with Kyber, port-issued, customer-managed? (Permission Model
  §13.5; Workflow §17.15)
- **Notification transport** — SSE / WebSocket / push / SMS /
  email? (Screen Architecture §12.10; Workflow §17.15)
- **Stakeholder push without Horizon authentication** —
  acceptable for low-stakes assignments? (Workflow §17.8)
- **Stakeholder visibility of conflict reasons** — operational
  transparency vs commercial sensitivity? (Information Arch §18.6)

### 9.5 Data Ownership & Multi-Tenant

- **Authoritative source for berth schedules** — QShips primary,
  or operator-entered primary with QShips verifying? (Information
  Arch §18.1)
- **AIS vs port-system reconciliation** — who wins when sources
  disagree? (Information Arch §18.3)
- **Multi-tenant in one Postgres vs one Postgres per tenant**
  from V1.0? (Implementation Strategy §20.10)
- **Customer-specific deployment pattern** — one Railway project
  per customer? (Implementation Strategy §20.7)

### 9.6 Audit & Replay

- **Replay scope** — full audit window, per-incident bounded slice,
  or both? (Workflow §17.7)
- **Replay separate permissions from live operations**? (Screen
  Architecture §17.10)
- **Retention requirements by information type** — different
  retention per domain? (Information Arch §18.8)
- **Audit payload redaction** — what regulators see vs other
  consumers? (Information Arch §18.7)
- **`AUDIT_READ` granularity** — per query, per page-view, per
  record? (Workflow §17.16)

### 9.7 AI / Future Intelligence

- **AI governance boundaries** — who governs training data scope,
  output authority, audit posture? (Information Arch §18.10)
- **AI advisory output authority** — never delegable to AI, but
  what's the appropriate human review cadence? (Information Arch
  §15.3)
- **AI integration architectural slot** — where do AI features
  appear in V1 architecture so they don't need retrofitting?
  (Implementation Strategy §17.5; Screen Arch §17.9)

---

## 10. Proposed Review Process

The foundation reaches v1.0 (acceptance) through a structured
review cycle. Each step has a defined audience and a defined
output.

### 10.1 Internal AMS Review

**Audience:** AMS leadership, Tony, ChatGPT.
**Output:** Acceptance of the five v0.1 documents OR specific
revision requests for v0.2.
**Focus:** strategic posture, commercial alignment, governance
structure, Beta 10 protection.
**Timeline:** weeks 1-2 of the review cycle.

### 10.2 Operational Stakeholder Review

**Audience:** at least one practising Harbour Master, one VTSO,
one Shift Supervisor (sourced from Brisbane / Melbourne / Geelong
or AMS-network ports).
**Output:** validation of role profiles, lifecycle stages,
escalation paths, screen layout assumptions; structural objections
where operational reality contradicts the model.
**Focus:** does this match how a real port operates? Are roles
defined the way operations actually work? Are workflows realistic?
**Timeline:** weeks 3-5 of the review cycle.

### 10.3 Architecture / Engineering Review

**Audience:** AMS engineering team + (optionally) external
architecture advisors.
**Output:** acceptance OR specific revision requests for
Implementation Strategy v0.2; framework / stack decisions resolved.
**Focus:** technical feasibility, V1.0 architecture decisions
(framework, frontend, deployment, observability), build sequencing
realism.
**Timeline:** weeks 4-6 of the review cycle (overlapping with
operational review).

### 10.4 Claude Design / CX Review

**Audience:** Claude Design / external CX advisors.
**Output:** acceptance of the Screen Architecture as design input;
identification of visual / interaction design decisions to be
made downstream.
**Focus:** does the screen architecture support good design
outcomes? Are the device strategy decisions sound? Are the
interaction primitives sufficient?
**Timeline:** weeks 5-7 of the review cycle.

### 10.5 Final v0.2 Update Cycle

**Audience:** all of the above; consolidated revisions.
**Output:** v1.0 of each foundation document (acceptance versions).
**Focus:** incorporate review feedback; resolve open questions
prioritised for V1.0 lock; document deferrals for V1.x.
**Timeline:** weeks 7-9 of the review cycle.

### 10.6 What Each Review Does NOT Decide

- **Internal AMS review** does not decide framework / stack
  questions (architecture review owns those)
- **Operational stakeholder review** does not decide engineering
  approach (its concern is whether the model matches reality)
- **Architecture review** does not decide operational role
  semantics (stakeholder review owns those)
- **Claude Design review** does not decide architecture — Design
  consumes the architecture, doesn't author it

Cross-talk between reviews is expected and welcome. Conflicts
between reviews are resolved by the operator (Tony) with explicit
documented rationale.

---

## 11. Recommended Next Actions

Concise list of what should happen next.

### 11.1 Schedule the four reviews (§10)

Each review needs a defined start date, defined audience, defined
output document. The cycle is ~8-9 weeks if reviews run mostly in
parallel; longer if sequential.

### 11.2 Distribute the foundation to Claude Design / CX

Once the architecture review accepts the foundation, hand off
all five documents to Claude Design (or whichever CX team is
engaged) as authoritative input. Visual design begins against
v1.0 of the foundation, not v0.1.

### 11.3 Create the V1.0 implementation backlog AFTER review acceptance

The V1.0 backlog is derived from the foundation. It is **not**
created in parallel with review. Creating implementation backlog
against unaccepted designs is design debt. Wait.

### 11.4 Keep Stage E-prod paused unless separately authorised

The recommendation across all five foundation documents is
consistent: Stage E-prod activation is a separate operational
decision, made after V1 RBAC is shipping. Until then, persistent
audit in production carries Beta-10-shaped attribution. The
trade-off is commercial demonstration vs evidentiary cleanliness;
both paths are defensible.

### 11.5 Keep Beta 10 protected

`phase-0-complete @ 4ad4aae` remains the immutable baseline.
`tests/test_beta10_regression.py` remains the active CI gate.
Beta 10 production continues serving demos and customer
presentations throughout the V1 development period.

### 11.6 During the review cycle, do NOT begin V1 code

The Implementation Strategy §16 governance rule applies: explicit
authorisation is required for any V1 implementation work. Review
acceptance is the trigger; until then, no V1 branches beyond
`docs/*`.

### 11.7 Document review feedback as it arrives

Each review produces feedback. Capture it in v0.2 candidate
revisions per document. Do not silently incorporate; surface every
change with reviewer attribution and rationale.

---

## 12. Non-Goals

This review pack explicitly does NOT:

- **Start V1 implementation.** No code, no schema, no migrations,
  no API endpoints, no frontend work. The foundation must be
  reviewed and accepted first.

- **Activate production audit.** Stage E-prod remains paused.
  Persistent audit is gated behind V1's per-user identity being
  on every audit row (currently Phase 0 helpers emit with
  Beta-10-shaped `O-1` actor handle).

- **Change Beta 10.** The Beta 10 production deployment, the
  marketing site, the regression gate baseline, and the
  `phase-0-complete` tag all stay untouched throughout the review
  period.

- **Merge or close DO-NOT-MERGE PRs #27, #28, #29.** Those PRs
  are preview-only Phase 1.2 deploy artefacts and remain open as
  forensic reference. They will be closed-without-merge after
  V1.0 ships and the Phase 1.2 sequence is operationally
  superseded.

- **Authorise specific customer engagements against V1.** Customer
  V1 commitments require an accepted foundation, an operational
  V1.0 deployment, and a per-customer onboarding plan. None of
  those exist yet.

- **Decide between Stage E-prod activation paths (A vs B).** The
  Information Architecture §9.4 documents both; the decision
  belongs to operations / commercial, not to this document.

- **Replace or supersede the five foundation documents.** This
  review pack is a synthesis; the five documents remain the
  authoritative source. Where this pack says "the Permission
  Model specifies X," the Permission Model itself wins on
  detail.

---

**End of v0.1 Review Pack.**

The V1 foundation is ready for stakeholder review. No further
foundation work is authorised until reviews have produced explicit
acceptance (or specific revisions) per §10. The next concrete step
is **scheduling the four review tracks** with their defined
audiences and outputs.
