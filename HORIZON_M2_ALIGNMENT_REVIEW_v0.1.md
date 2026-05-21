# Horizon — M2 Alignment Review (v0.1)

**Document status:** Draft for review — **alignment review only**
**Document type:** Structured governance / architecture alignment review
**Target stream:** V1 sandbox / future architecture only
  **— Beta 10 is excluded by the Immutability Rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 763be55`
  (post-PR #59 — Operational Platform Workflows)
**Date:** 2026-05-21
**Scope of authority:** Reviews the existing M2 direction (PR #53
  Scope Proposal + PR #54 Implementation Plan) against the newly
  merged platform-governance and workflow documents. **Review only**
  — does **not** modify M2 scope, does **not** redesign M2, does
  **not** authorise any implementation, does **not** modify any
  previously merged document, does **not** touch Beta 10 in any way.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59, `763be55`)
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58, `cc2f2ea`)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57, `dbc7ae8`)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54, `d42b9e8`)
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53, `8c73c55`)
- `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md` (PR #52, `dde3a84`)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51, `b0d9ff2`)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- Beta 10 Immutability Rule (state-alignment update, recorded
  in-session)

---

## 1. Executive summary

**Is M2 still directionally correct?** Yes.

**Is M2 still safe to proceed?** Yes — under the existing
governance pattern (Phase 0 authorisation → Phase 1 local
implementation → Phase 2 sandbox verification → Phase 3 merge),
with the small set of implementation-period guidance items
captured in §3 below.

**Does M2 align with the new operational platform direction?**
**Yes, with one minor caveat.** M2 was scoped before the
Operational UX Direction (PR #58) and the Operational Platform
Workflows (PR #59) were merged. Re-reading M2 with both documents
in hand:

- M2's read-only / fixture-fed posture is **structurally
  compatible** with the new direction. No M2 acceptance criterion
  contradicts §3 of the UX Direction (right rail = action surface;
  centre = VTS / spatial; KPIs = supporting; role-modes
  anticipated) or §5 of the Platform Workflows (API-first;
  server-side authorisation; immutable audit; state separation;
  testable services).
- M2 explicitly **defers** everything the new direction
  anticipates for M3+ — full role-mode system, write paths,
  auth enforcement, audit emission, live integrations.
- M2's deny-list (§4 / §12 of the M2 Implementation Plan) already
  excludes the items the new direction expects to live in M3+.

The caveat is **not a scope change**. It is a small set of
**implementation-period drift risks** that the M2 team should
hold in mind as Phase 1 proceeds (when authorised) — captured in
§3 of this review. None of these requires modifying the M2
Implementation Plan; they are notes for the implementer.

**Recommended posture (full rationale in §7): Proceed with
guidance notes.** No re-sequencing, no scope split, no
modification to PR #53 or PR #54.

**M2 implementation start remains gated** on a separate explicit
"Authorised: begin Horizon V1 M2 implementation" message from
Tony, per the M2 Implementation Plan §19.1 (Phase 0). This
review does not authorise it.

---

## 2. What M2 already gets right

The M2 Scope Proposal (PR #53) and Implementation Plan (PR #54)
were drafted before the Operational UX Direction (PR #58) and
the Operational Platform Workflows (PR #59), but they align
strongly with both documents. Specifically:

### 2.1 Operational direction

M2 extends Horizon's centre-spine tab set from the partial
Dashboard (M1) to Berth Timeline, Shift Log, VTS, Pilotage,
and Performance (placeholder). Every one of those tabs is an
**operational** surface — not a marketing surface, not a
financial surface, not a generic widget-grid surface. The
choice of tabs reflects what coordination users (VTSO, Harbour
Master, Shift Supervisor) actually do.

### 2.2 Coordination-first concepts

The VTS, Pilotage, and Shift Log tabs are explicitly
coordination-class. Berth Timeline is the spatial / scheduling
view of the operator's working window. The Decisions panel
(right side) is preserved from M1 and continues to hold
CONFLICT-class decision cards (now demonstrably non-empty for
all four pilot ports after PR #55 / PR #56).

This is structurally aligned with UX Direction §3.1 (decision
cards → right rail) and §3.2 (VTS / spatial → centre).

### 2.3 VTS / spatial awareness foundations

M2 introduces a dedicated VTS tab in the centre spine. The
M2 Implementation Plan §8.3 explicitly notes the VTS tab is
read-only (vessel list + conflicts pane), no map editing, no
DSW. This is exactly the "future default centre-spine for
coordination users" surface the UX Direction §3.2 anticipates.
M2 lays the foundation; M3+ takes it to the default-tab
posture.

### 2.4 Separation from Beta 10

M2 is, by construction, separated from Beta 10:
- M2 ships against `/fixtures/*.json`, not Beta 10's live data
- `VITE_API_BASE` defaults to `/fixtures`
- No `server.py` change
- No root `railway.toml` change
- Phase 2 bundle inspection confirms only `/fixtures` fetch
  targets in the bundle (M2 Implementation Plan §15.2)

This honours the Beta 10 Immutability Rule **by construction**,
not by convention. The Rule was declared after M2 was drafted,
but M2's scope was already compatible with it.

### 2.5 Workflow alignment

M2's tab set aligns to the workflow profiles in Operational
Platform Workflows §3:

| M2 tab | Workflow profile primary use |
|---|---|
| Dashboard (M1) | Executive View, secondary for Coordination View (UX Direction §3.4) |
| Berth Timeline | Terminal Operator (§3.6); Coordination View (§3.1) supporting |
| Shift Log | Harbour Master (§3.2); Coordination View (§3.1) supporting |
| VTS | VTSO (§3.1, primary) |
| Pilotage | Pilotage Coordinator (§3.4), Towage Coordinator (§3.5) supporting |
| Performance placeholder | Executive View (§3.3) future |

M2 does not implement role-mode switching, but it builds the
surfaces that role-mode-aware composition will later select
from. This is the right sequencing.

### 2.6 Architectural trajectory

M2 honours every Platform Workflows §5 principle that is
in-scope for a read-only / fixture-fed milestone:

| Workflows §5 principle | M2 alignment |
|---|---|
| §5.1 API-first | M2 builds against the existing `ViewSummary` shape, not against a hardcoded UI assumption |
| §5.2 No business logic in frontend | M2 explicitly does not modify `detect_conflicts` or `_build_decision_support`; adapters are pure presentation transforms |
| §5.3 Frontend consumes APIs | M2 frontend consumes `/fixtures/*.json` via the M1 `horizon.js` fetch wrapper — the same code path that will consume the live API later |
| §5.4 Typed domain models | M2 honours the Adapter Design Note §2.2 contract (20-key `ViewSummary`) — typed-in-spirit even before formal typing arrives |
| §5.5 Event-driven server-side | Out of M2 scope (correctly); does not preclude in M3+ |
| §5.6 Server-side authorisation | Out of M2 scope (correctly — M2 has no write paths and no auth); does not preclude in M3+ |
| §5.7 Immutable audit | Out of M2 scope (correctly — M2 has no operator actions to audit); does not preclude in M3+ |
| §5.8 Separation of operational / decision / display state | Adapters are pure presentation transforms; the underlying fixture data is the operational + decision state; this separation is structurally honoured |
| §5.9 Testable services | M2 plans test coverage parity with M1 (Vitest for adapters and helpers); regression gate locked at 46/46 |
| §5.10 Integration-ready data boundaries | The adapter pattern itself is the integration boundary; M2 does not embed any new integration |

### 2.7 Role-awareness

M2 does not implement modes, but its component composition is
role-agnostic: KPI tiles, ETD risk tables, conflict lists,
vessel lists, pilotage tables. None of these is hard-bound to a
single role. This means a later mode system can compose them
without rewriting them, which is what UX Direction §6.4
(mode-blind component design) requires.

### 2.8 Future platform compatibility

M2's deliverables map cleanly to the Platform Workflows §8 data
model:

| M2 surface | Workflows §8 entities consumed |
|---|---|
| Berth Timeline | `Berth`, `PortCall`, `Movement`, `Vessel`, `Constraint` |
| Shift Log | `AuditEvent` (derived from fixture events; not the live audit ledger), `OperationalEvent` (informal) |
| VTS | `Vessel`, `Conflict`, `Constraint` |
| Pilotage | `Resource` (pilot), `Movement`, `Vessel` |
| Performance placeholder | (none — placeholder) |

When the live API arrives in M3+, the same UI components consume
the same entity shapes from the API instead of from fixtures.
The adapter layer is the seam.

---

## 3. Areas where M2 could accidentally drift

These are **drift risks for the implementation period**, not
structural problems with the M2 plan. None of these requires
modifying PR #53 or PR #54. They are notes for the implementer.

### 3.1 Dashboard-first behaviour creep

**Risk:** during implementation, the team polishes the
Dashboard tab (which is the M1-shipped default) more than the
new VTS / Pilotage / Berth Timeline / Shift Log tabs, because
Dashboard is already the most familiar.

**Why it matters:** UX Direction §3.2 explicitly anticipates
VTS as the *future* default for coordination users. If M2 ships
VTS as a thin afterthought, M3 has to re-implement it.

**Mitigation (no scope change):** during Phase 1 implementation,
allocate visual / interaction polish proportional to the
expected *future* prominence of each tab — not proportional to
how much code already exists. The VTS tab in particular should
be implemented with the M3+ default-tab posture in mind.

### 3.2 Demo-style interaction patterns

**Risk:** during implementation, components adopt Beta-10-era
interaction patterns (modal dialogs, full-page transitions,
demo-style animations) that are visually impressive in a demo
but increase friction in an operational context.

**Why it matters:** M2 sits on the platform-trajectory side of
the Beta 10 Immutability Rule. Beta 10 patterns optimise for
demo legibility; the platform optimises for operator
efficiency.

**Mitigation (no scope change):** keep interactions minimal —
read-only highlight on hover, no modals, no full-page
transitions for tab switching, no demo-style animations.
Canon §3 visual semantics + §10 anti-patterns already require
this; Phase 1 review should re-check.

### 3.3 Frontend-heavy logic

**Risk:** during implementation, adapter logic creeps from
"pure presentation transform" toward "small business rule"
(e.g. deriving a synthetic conflict in the adapter to handle
a fixture-shape gap, or computing severity in the adapter
because the fixture lacks it).

**Why it matters:** Platform Workflows §5.2 — no business
logic trapped in the frontend. The adapter is the seam to
the future live API; once it carries business logic, that
logic has to be relocated server-side later, which is a
re-architecture, not a refactor.

**Mitigation (no scope change):** if an adapter would have to
compute a business rule to populate a tab, **stop and report**
(M2 Implementation Plan §18.11). The right answer is to update
the fixture shape (Option 4b capture if necessary, with
separate authorisation) or to defer the field, not to fake it
client-side.

### 3.4 Weak separation between display state and operational state

**Risk:** during implementation, the tab switcher (M2
Implementation Plan §8.6, in-memory state) leaks into
state shape used by adapters or hooks (e.g. a tab-specific
filter that the adapter consumes), blurring the distinction.

**Why it matters:** Platform Workflows §5.8 requires display
state (client-owned) and operational state (server-owned) to
be cleanly separated. Mixing them now means a later mode
system has to untangle them.

**Mitigation (no scope change):** the tab switcher's state
must live in the layout / page layer, not be passed into
adapters. Adapters consume `ViewSummary`; they do not consume
"which tab is active". This is already implicit in M2
Implementation Plan §9.3 but bears re-confirmation in Phase 1
code review.

### 3.5 Insufficient role separation

**Risk:** during implementation, components hard-code
single-role assumptions (e.g. "this KPI tile is for
executives") in markup or copy, baking role coupling into the
component layer.

**Why it matters:** Platform Workflows §3 anticipates 8 user
profiles; UX Direction §6.4 explicitly warns against
mode-blind component design.

**Mitigation (no scope change):** components remain
role-agnostic. Mode-specific composition lives in the page /
layout layer in M3+. Phase 1 review should flag any component
that names a role in its own implementation.

### 3.6 Over-indexing on visual polish before workflow maturity

**Risk:** during implementation, polish-heavy work
(animations, micro-interactions, refined typography) drains
time that would otherwise close the open questions in M2
Implementation Plan §14 (fixture-shape coverage for Pilotage,
Berth Timeline window, etc.).

**Why it matters:** M2's value is operational legibility, not
visual sophistication. Polish is M2.5 / M3 work.

**Mitigation (no scope change):** Phase 1 should prioritise
function over polish; visual refinement is a post-merge or
later-milestone concern. M2 acceptance criteria (A1–A13) do
not require polish; they require correctness and coverage.

### 3.7 Introducing Beta 10 assumptions into V1

**Risk:** during implementation, an engineer reaches for a
Beta 10 implementation pattern (e.g. the in-page client-side
filter at `server.py:3542`, or the inline JS handlers in the
Beta 10 page bundle) and replicates it in M2 because "it works
in Beta 10".

**Why it matters:** Platform Workflows §10.3 — Beta 10 is not
used as architecture precedent where it conflicts with secure
platform design. Some Beta 10 patterns (HTML-in-Python,
inline JS, no client framework) are explicitly *not* the
platform direction.

**Mitigation (no scope change):** Phase 1 PR review should
treat "this is how Beta 10 does it" as a flag for re-think,
not a justification. M2 uses the V1 React / Vite frontend; it
does not replicate Beta 10's page-bundle architecture.

### 3.8 Accidental coupling to simulated / demo-era patterns

**Risk:** during implementation, the team writes code that
*assumes* fixture data (e.g. hardcoded vessel names from the
M1 fixtures, fixture-specific severity counts in tests,
fixture-specific identifiers in component logic).

**Why it matters:** when the live API arrives in M3+, the
fixture-specific assumptions break. The seam that should hide
the fixture-vs-live difference (the adapter) instead leaks
fixture identity throughout the codebase.

**Mitigation (no scope change):** components and tests should
parameterise on shape, not on specific fixture values
(except for snapshot-style tests that explicitly assert
fixture content). The M2 deny-list (§12 of Implementation
Plan) already covers most of this; Phase 1 code review should
re-check on fixture-coupling risk specifically.

---

## 4. What MUST remain out of M2 scope

The following items are **explicitly out of M2** and must
**not** drift in during Phase 1 implementation. Each belongs
to a later platform-maturity stage and would require its own
scope proposal + implementation plan + explicit Tony
authorisation.

| # | Item | Belongs to |
|---|---|---|
| 4.1 | **Full RBAC implementation** (server-enforced role / permission / port scoping per Platform Workflows §6.2) | M3 or M4 — security / platform foundation milestone |
| 4.2 | **Production-grade auth stack** (IdP integration, SSO, MFA, session lifecycle, token rotation per Workflows §6.1, §6.6) | M3 or M4 — security foundation |
| 4.3 | **Full audit framework** (append-only hash-chained ledger emitting from V1, per Workflows §5.7) | M3 or M4 — audit foundation |
| 4.4 | **Production infrastructure** (deploy platform, region, secrets, KMS, observability per Workflows §6.10 / open questions §11.2.2) | M4 or M5 — production-readiness |
| 4.5 | **Native mobile** (iOS, Android, App Store distribution, MDM) | Future — likely M5+ |
| 4.6 | **Pilot Proximity Companion App implementation** (per PR #57) | Future — dedicated milestone downstream of V1 |
| 4.7 | **Replay engine** (Workflows §4.12 reconstructable per-timestamp state) | M3 or M4 — audit / replay foundation |
| 4.8 | **Complete API stabilisation** (versioning policy, deprecation, OpenAPI / IDL surface per Workflows §5.4 + §7.1) | M4 — API foundation |
| 4.9 | **ML / AI orchestration** (anomaly detection, recommendation ranking via ML, predictive routing) | Future — explicit AMSG strategic decision required |
| 4.10 | **Advanced workflow automation** (auto-dispatch, auto-acknowledge, auto-escalate) | **Never automated end-to-end** per Workflows §3 (must-never-be-automated); selective automation only with explicit policy |
| 4.11 | **Operational write-back integration** (Horizon updating PMS, statutory reports, billing) | M4 or M5 — integration fabric milestone |
| 4.12 | **Production multi-tenancy** (multi-port-authority customer hosting, per-tenant isolation, per-tenant audit chain genesis at production scale) | M4 or M5 — multi-tenant platform milestone |

Confirmation: **none of these are in M2's current scope, and
none should drift in.** The M2 Implementation Plan §12
deny-list already excludes them. This review re-affirms.

---

## 5. What should likely move to M3+

Items below are reasonable next-milestone candidates after M2
closes. They are **not authorised** here; they are recorded so
the M3+ scope-proposal process has a starting list.

5.1 **Operational modes** — Executive / Coordination / VTSO /
Pilotage / Incident / Replay-Audit (UX Direction §3.4;
Workflows §3). Mode-switching UX (Workflows open questions
§11.4.2) needs design before implementation.

5.2 **Full VTS-first coordination layout** — VTS becomes the
default centre-spine tab for coordination users (UX Direction
§3.2). Includes spatial conflict overlays, predicted paths,
tide / channel overlays.

5.3 **Audit / Replay UX** — the Replay / Audit Mode user
profile (Workflows §3.8) and the swimlane timeline (Canon §8).
Requires the replay-snapshot stream (§5.7 below).

5.4 **Notification orchestration** — outbound dispatch to pilot
dispatcher, tug operator, terminal operator, vessel agent
through configured channels (Workflows §4.10). Requires
integration fabric.

5.5 **Advanced decision workflows** — ACK / DEFER / APPLY /
REJECT / ESCALATE operator actions (Workflows §4.8), each
audited, server-authorised, and producing downstream
notification. The decision card lifecycle becomes a true
state machine.

5.6 **Secure integration fabric** — typed, audited, scoped
integration boundary for external systems (AIS, BoM, PMS,
pilot organisation, tug operator, terminal operator). Per
Workflows §5.10 and §7's integrations / ingest domain.

5.7 **Event architecture** — internal event stream for
operational events (Workflows §5.5). Substrate choice
(Workflows open questions §11.2.3) needs decision.

5.8 **Production security posture** — IdP, RBAC enforcement,
session / token, secrets management, pre-deploy pen-test
(Workflows §6).

5.9 **Deployment orchestration** — production deploy platform
choice (Workflows open questions §11.2.2), CI / CD pipeline,
blue / green or canary rollout, rollback playbook.

5.10 **Enterprise observability** — structured logging, metrics,
tracing, alerting, audit-export workflow (Workflows open
questions §11.5.3), uptime SLOs.

### 5.1.x Suggested M3 / M4 / M5 sequencing (recommendation only)

Not authorised here. Recorded for future scope-proposal use.

| Stage | Likely focus |
|---|---|
| **M3** | Operational modes + full VTS centre layout + lightweight read-only audit / replay surface (still mostly fixture-fed; introduce server-side auth shim as an opt-in) |
| **M4** | Decision-action write paths (ACK / DEFER / APPLY / REJECT / ESCALATE) + full audit framework + RBAC + initial live integration (one feed, one port) |
| **M5** | Production multi-tenancy + integration fabric + notification orchestration + enterprise observability + pre-deploy pen-test |

Each requires its own scope proposal, plan, and authorisation.

---

## 6. Beta 10 governance check

Explicit confirmation of the Beta 10 Immutability Rule
relative to M2:

6.1 **M2 must not backport into Beta 10.** Even when an M2
component would visually or behaviourally improve Beta 10, the
backport is forbidden. Beta 10 is a preserved commercial
trust surface. Confirmed by:
- M2 Implementation Plan §4.5 explicitly lists `server.py`,
  root `railway.toml`, and audit helpers as "files explicitly
  NOT touched in M2".
- M2 ships against `/fixtures`, not against Beta 10's live data.
- M2 deploys to `horizon-v1-sandbox`, not to
  `Project-Horizon / production`.
- The Beta 10 regression gate (`tests/test_beta10_regression.py`)
  remains 46/46 throughout M2.

6.2 **M2 must not chase visual parity with Beta 10.** Beta 10's
UI is optimised for demo legibility. M2's UI is optimised for
operational ergonomics. Where the two diverge, M2 follows
operational ergonomics per UX Direction §3 and Workflows §3.
Confirmed by:
- UX Direction §3 explicitly re-prioritises information
  hierarchy away from a dashboard-first / demo-friendly
  posture.
- M2 Implementation Plan §10 read-only safeguards favour
  defensive copy and explicit empty / loading / stale states
  over demo polish.

6.3 **M2 should optimise for long-term platform architecture.**
Per the post-demo state-alignment update: "V1 can optimise for
operational ergonomics and long-term architecture rather than
demo preservation." Confirmed by:
- M2's adapter pattern is the seam to the future live API
  (Workflows §5.10 integration-ready boundaries).
- M2's component composition is role-agnostic so the M3+ mode
  system can compose without rewriting (§2.7 above).
- M2 fixtures are reused unchanged; no fixture refresh
  introduces a Beta 10 dependency.

6.4 **Beta 10 remains protected commercial surface only.**
M2 introduces no path — code, configuration, deployment, or
operational — by which a V1 change reaches Beta 10. Confirmed
by the Phase 2 bundle-inspection requirement (M2
Implementation Plan §15.2) which would fail any production
URL in the bundle.

6.5 **No "while we are here" fixes.** Confirmed: M2 acceptance
criteria A1–A13 are tightly scoped. The Implementation Plan
§18 stop conditions explicitly trigger on diff scope drift
beyond the frontend-only boundary.

6.6 **Burden of proof:** *"Why MUST this exist in Beta 10?"*
For M2, the answer is "nothing." No M2 deliverable needs to
exist in Beta 10. Confirmed.

---

## 7. Recommended posture

### Recommendation: **Proceed with guidance notes.**

(Not "Proceed unchanged" — because the guidance notes in §3 are
worth holding in mind during Phase 1.)
(Not "Split scope" — because nothing in M2 needs to be removed.)
(Not "Re-sequence components" — because the existing sequencing
in M2 Implementation Plan §19 is correct.)

### Rationale

M2 was scoped before the Operational UX Direction (PR #58) and
the Operational Platform Workflows (PR #59) were merged. A
plausible concern is that M2 might have to be re-scoped or
re-sequenced in light of those documents. **It does not.** The
analysis in §2 above shows that M2 aligns structurally with both
documents — its scope is compatible, its deny-list anticipates
the items deferred to M3+, and its architectural patterns
honour the platform principles in Workflows §5.

What the new documents *do* surface is a small set of
implementation-period drift risks (§3 above): things the M2
implementer should hold in mind as Phase 1 proceeds. None of
these is structural; none requires modifying PR #53 or PR #54;
none requires re-opening M2's acceptance criteria. They are
guidance notes the implementer (Claude, or a future
implementation team) should re-read at Phase 1 kick-off.

### Concrete next steps under this recommendation

1. **No modification to PR #53 (M2 Scope Proposal) or PR #54
   (M2 Implementation Plan).** Both remain valid as written.
2. **No new M2 acceptance criteria.** A1–A13 stand.
3. **No new M2 deny-list items.** The existing §12 deny-list
   already excludes the §4 items above.
4. **At Phase 0 (the M2 implementation start authorisation), Tony's
   authorisation message should explicitly cite this review** so
   the implementer enters Phase 1 with §3 guidance in mind.
5. **At Phase 1 PR review, the §3 guidance notes are part of
   the review checklist** (alongside the existing M2
   Implementation Plan validation gates).
6. **At M2 close (post-Phase 4 Beta 10 visual check), the M2
   Retrospective should explicitly assess M2's actual outcome
   against the §3 drift risks** — did Dashboard-first creep
   happen? Did frontend-heavy logic appear? Etc.
7. **M3 scope proposal must cite this review** in addition to
   the UX Direction and Platform Workflows documents.

### What this recommendation explicitly does NOT do

- It does **not** authorise M2 implementation start. That
  remains gated on a separate explicit Tony authorisation
  (M2 Implementation Plan §19.1, Phase 0).
- It does **not** modify M2 scope or plan.
- It does **not** add work to M2.
- It does **not** create new M2 deliverables.
- It does **not** weaken any M2 stop condition.
- It does **not** touch Beta 10.
- It does **not** authorise any M3+ scope.

---

## 8. Open questions

Recorded for Tony / maritime SME / engineering review. **None
of these blocks M2.** They are inputs to the M3 scope-proposal
process when that begins.

### 8.1 M2-specific (implementation-period only)

8.1.1 At Phase 1 kick-off, should §3 of this review be linked
in the M2 Implementation Plan §15 validation gates as an
explicit pre-merge checklist item? (Default proposal: yes —
include as a Phase 1 review reference, not as new gate
criteria.)

8.1.2 Should the M2 Performance tab placeholder copy explicitly
reference the planned M3+ scope, or remain generic
("Deferred to a future milestone")? (Default proposal: generic
— specific milestone-naming risks coupling the placeholder to
M3 scope that does not yet exist.)

### 8.2 M3 sequencing

8.2.1 Should M3 be a single milestone or split into M3a (modes
+ VTS layout) and M3b (audit / replay surface)? Depends on
team capacity and on the answer to 8.2.2.

8.2.2 Is the default mode per role (UX Direction open question
§7.1) decided in M3 scope, or earlier in a dedicated mode-policy
document?

8.2.3 Does M3 introduce server-side auth as a real surface, or
keep it as a fixture-fed mock until M4? (Workflows §5.6 says
server-side, but the M3 increment may justify a phased
introduction.)

### 8.3 Architecture

8.3.1 What is the V1 production deployment platform (Workflows
open questions §11.2.2)? Railway, AWS, Azure, GCP, or AMSG-owned?
This affects M4 / M5 scope but should be decided before M3
acceptance to avoid platform-coupling debt.

8.3.2 What is the storage choice for the audit ledger and the
operational store (Workflows open questions §11.2.1)? Relational
+ append-only is the strong default but multi-store is feasible.

8.3.3 What is the event-stream substrate (Workflows open questions
§11.2.3)? In-process async is sufficient for M3; durable substrate
(Kafka, NATS, Redis Streams) probably needed by M4.

### 8.4 Workflow

8.4.1 Does the §3 (Workflows) workflow set need maritime SME
validation **before** M3 scope, or can M3 proceed with the
workflows-as-drafted and the SME validation happen in parallel?

8.4.2 Which port is the first live-client target (Workflows open
questions §11.1.1)? Brisbane / Darwin / other? Affects M4 / M5
acceptance criteria and integration priorities.

### 8.5 Governance

8.5.1 At what cadence is the Beta 10 Immutability Rule
re-affirmed (Workflows §10.1, §10.2, §10.3, §10.14, §10.15)?
Per scope proposal? Per implementation plan? Per retrospective?
All three? (Default proposal: cite in every scope proposal and
every implementation plan; re-affirm in every retrospective.)

8.5.2 Who, in addition to Tony, is authorised to declare the
"critical demo-blocking defect" exception to the Immutability
Rule? Currently the exception is Tony-only; M3+ may need a
clear succession / coverage policy.

8.5.3 How is the Independence Reset framing audited across the
M3+ document stream? Per-document grep at PR review time, or a
documented review checklist?

### 8.6 Maritime SME

8.6.1 Are the eight workflow profiles in Workflows §3 complete
for the first live-client port, or is a port-specific role
missing (e.g. Bunker Coordinator, Customs Liaison)?

8.6.2 Are the 13 API domains in Workflows §7 the right
decomposition, or do real port operations cluster them
differently?

8.6.3 Does the M2 read-only posture meet first-live-client
expectations for a useful operational surface, or is some
write capability needed earlier than M4? (If yes, that
becomes a structural M2.5 / M3 scope question.)

---

## End of M2 Alignment Review v0.1

**Status: alignment review only. Not authorised for
implementation. Does not modify M2.**

Confirmed by this document:
- **M2 is directionally correct.** Scope and plan (PR #53,
  PR #54) remain valid.
- **M2 aligns with the newly merged platform direction.** The
  Operational UX Direction (PR #58) and the Operational
  Platform Workflows (PR #59) do not require any change to
  M2's scope, plan, or acceptance criteria.
- **A small set of implementation-period guidance notes
  applies** (§3). These are not new scope; they are notes for
  the implementer at Phase 1.
- **Items belonging to later platform-maturity stages remain
  excluded** (§4). The existing M2 deny-list already covers
  them; this review re-affirms.
- **The Beta 10 Immutability Rule is honoured by M2 by
  construction** (§6). M2 introduces no path by which a V1
  change reaches Beta 10.
- **The recommended posture is "Proceed with guidance notes"**
  (§7) — no modification to PR #53 or PR #54.
- **M2 implementation start remains gated** on a separate
  explicit Tony authorisation per M2 Implementation Plan
  §19.1 (Phase 0).
- **No code, fixture, Railway, env-var, or infrastructure
  change** is authorised by this document.
- **Beta 10 is not touched** by this document or by the review
  process that produced it.
- **The Independence / Architecture Reset (PR #51) framing is
  preserved** — no Smart Ocean X dependency framing.

**Next action:** Tony's review. Optional ChatGPT engineering
review. If approved and merged, this review becomes the
governance reference cited at M2 Phase 0 (implementation start
authorisation) and at M3 scope-proposal time.
