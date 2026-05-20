# Horizon V1 — M2 Scope Proposal (v0.1)

**Document status:** Draft for review
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT
**Execution agent:** Claude
**Effective baseline:** `origin/main @ dde3a84`
  (Merge #52 — M1 Retrospective, on top of Merge #51 — Horizon
  Independence / Architecture Reset, on top of M1 merge `e90eb56`)
**Date:** 2026-05-20
**Scope of authority:** Proposes the candidate scope for Milestone
  M2 of the Horizon V1 programme. This document is **a proposal**,
  not an authorisation. It does **not** authorise M2 implementation,
  does **not** change any code, fixtures, or infrastructure, and
  does **not** modify any previously merged document.

**Cited governing documents (all on `main`):**
- `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md` (PR #52, `dde3a84`)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51, `b0d9ff2`)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` (PR #48)
- `HORIZON_V1_EXECUTION_PLAN_v0.1.md` (PR #36)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md`
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md`

---

## 1. Executive summary

Milestone M2 is proposed as the natural continuation of the
fixture-fed, read-only V1 frontend programme that closed
successfully through M0 and M1. M2 expands the operational
surfaces visible to the user: the four remaining centre-spine
tabs (Berth Timeline, Shift Log, VTS, Pilotage) and a deliberate
decision on Performance (limited or fully deferred).

M2 introduces **no new backend dependency, no live Beta 10
calls, no auth, no writes, no audit emission, no Stage E-prod
activity, and no database dependency**. The fixture-fed pipeline
proven in M1 remains the data backbone.

This document is a **scope proposal only**. It does not authorise
implementation. M2 implementation requires a separate M2
Implementation Plan PR and Tony's explicit authorisation,
following the same governance pattern that closed M0 and M1
cleanly. The Smart Ocean X architectural dependency is
**formally closed** per the Independence / Architecture Reset
(PR #51, `b0d9ff2`) and remains so under this proposal.

---

## 2. M2 objective

The objective of M2 is to **expand the read-only operational
surface area** of the Horizon V1 frontend from the partial
Dashboard tab delivered in M1 to the remaining four
canonical centre-spine tabs defined in Canon §1.1.3 — and to
make a deliberate decision on the Performance tab.

M2 is **not** about:
- introducing live data,
- introducing write paths,
- introducing user authentication or RBAC,
- introducing audit emission,
- introducing any database dependency,
- changing Beta 10 production behaviour,
- modifying `server.py`, root `railway.toml`, or the audit ledger,
- touching Stage E-prod or `preview-audit-activation`,
- changing the Independence Reset framing.

M2 is about **operational legibility**: giving operators visual
access to the full set of centre-spine tabs against captured /
fixture-backed operational data, so that subsequent milestones can
exercise lifecycle, mode-switch, and Replay flows on a fully
populated UI.

---

## 3. Recommended M2 scope

The recommendation is to deliver **four read-only tabs** in M2,
with **Performance limited / partially deferred to M2.5 or M3**.

### 3.1 Recommended in-scope (M2)

| # | Tab | Canon ref | Adapter coverage | Scope summary |
|---|---|---|---|---|
| 1 | **Berth Timeline** | §1.1.3, §1.2, §3.x | `berthAdapter` (new) on top of existing `summaryAdapter` | Berth-row-as-Gantt, vessel blocks as time segments. Read-only. No drag, no edit, no scenario builder. Stale-data banner reused. |
| 2 | **Shift Log** | §1.1.3, §11 (recommended impl order, "Shift Log table reads real events") | `shiftLogAdapter` (new) consuming captured events list from existing fixtures | Read-only table of events for the current shift. No write, no acknowledgement, no comment. |
| 3 | **VTS summary** | §1.1.3, §9 ("Map-based VTS interaction beyond read-only" is explicitly forbidden mobile, kept read-only desktop here) | `vtsAdapter` (new) consuming vessel and conflict slices already in fixtures | Read-only VTS pane: vessel list, headings, speeds, conflict flags. No map editing. No vessel "follow" beyond visual highlight. |
| 4 | **Pilotage summary** | §1.1.3 | `pilotageAdapter` (new) consuming pilotage-relevant fields in existing fixtures | Read-only pilotage queue / assignments view. No assignment edit, no dispatch action. |

All four tabs are **read-only** by construction. None introduces
a write path, an audit emission, a backend call, or any
authentication concern. All four reuse the M1 polling hook
(`useSummary`), stale-data semantics, and visual semantics.

### 3.2 Recommended partial / deferred (M2 boundary call)

| Tab | Recommendation | Rationale |
|---|---|---|
| **Performance** | **Limited or deferred** | The Performance tab is the most data-intensive and the most likely to surface fixture-shape gaps and Canon §3 visual-semantics edge cases. Recommendation: deliver a **placeholder Performance tab** with a stub header and an explicit "deferred" notice in M2, and full Performance content in a subsequent M2.5 / M3 milestone. The M2 Implementation Plan should confirm which of these two postures to take. |

### 3.3 Out-of-scope by construction (not in M2 at all)

- Replay swimlane timeline (Canon §8) — deferred
- DSW interaction model (Canon §7) — deferred
- Operator action endpoints (ACK / COMMIT / DEFER / OVERRIDE / ESCALATE) — deferred and not part of any V1.x read-only scope
- Mode-switch beyond visual scaffolding — deferred
- Mobile views — deferred
- Sub-tabs / drill-downs within a tab — deferred
- Any backend or `server.py` changes — explicitly out of scope
- Any Stage E-prod / preview-audit-activation work — explicitly out of scope

---

## 4. Out-of-scope items

The following are **explicitly out of scope for M2**, listed here
so that the M2 Implementation Plan can reference this list
verbatim and the execution agent has an unambiguous deny-list.

4.1 Live Beta 10 backend calls of any kind.
4.2 Production `/api/*` calls of any kind from the V1 frontend.
4.3 Auth enforcement (login gate, session, RBAC, role checks).
4.4 Server-side RBAC, ACL, or permission evaluation.
4.5 ACK writes (any acknowledgement of any conflict, recommendation, or event).
4.6 COMMIT, DEFER, OVERRIDE, ESCALATE — any operator action endpoint.
4.7 Audit emission of any kind (no audit ledger writes, no audit reads needing a backend).
4.8 Stage E-prod activity (Stage E-prod remains paused).
4.9 Database dependency of any kind (no Postgres connection, no preview-audit-activation Postgres touch).
4.10 `server.py` changes (any change, in any direction).
4.11 Root `railway.toml` changes — root config is Beta 10's and must remain immutable in M2.
4.12 Production state changes of any kind (no `set_port` toggles against Beta 10, no production data refresh, no production cache warm-up).
4.13 New external services (no new Open-Meteo style live dependency added by M2; existing Beta 10 Open-Meteo behaviour is pre-existing and untouched).
4.14 New backend endpoints in any form.
4.15 Smart Ocean X framing in any new document or comment.

If any item in §4 is required, M2 stops and a separate
authorisation is requested.

---

## 5. Why M2 remains read-only

The decision to keep M2 read-only is **deliberate and load-bearing**.

5.1 **Beta 10 isolation.** M1 proved that a fixture-fed pipeline
can drive a useful operational surface without touching
production. Maintaining the same posture in M2 preserves the
guarantee that V1 cannot disturb Beta 10 by construction.

5.2 **Operator action paths must come later, deliberately.** ACK,
COMMIT, DEFER, OVERRIDE, ESCALATE are safety-critical decisions
in real port operations (Canon §0 preamble). They require
authentication, RBAC, audit emission, and post-action lifecycle
guarantees. None of these are appropriate to bolt onto M2 — they
need their own scoping milestone and independent authorisation.

5.3 **Audit ledger must remain undisturbed.** The append-only
hash-chained audit ledger (per Phase 0.7) was sealed prior to V1
work. Introducing audit emission from V1 requires a separate,
explicit decision about per-tenant genesis, signing keys, and
hash-chain interaction. M2 does not attempt this.

5.4 **Fixture-fed delivery has been proven cheap and fast.** M1
showed that fixture-fed components can be delivered, tested, and
deployed in a single milestone with very tight diffs. There is
no programmatic gain to be had by introducing live reads into
M2.

5.5 **Independence Reset framing is preserved.** Read-only +
fixture-fed leaves all backend / substrate questions open for
later, deliberate decisions — which is exactly what the
Independence Reset (PR #51, §11 M2+ implications) recommends.

5.6 **M1 Retrospective §13.4 explicitly recommends no live read
for M2** unless called out and authorised separately. This
proposal honours that recommendation.

---

## 6. Data / fixture implications

### 6.1 Existing fixtures
The five fixture files committed in PR #49 (`brisbane-busy.json`,
`brisbane-quiet.json`, `melbourne-sim.json`, `null-fields.json`,
`malformed.json`) form the M2 data substrate. Each is checked
into `frontend/public/fixtures/` and served as a static asset by
the same path used in M1.

### 6.2 Fixture-shape coverage check (M2 prerequisite)

Before M2 implementation begins, the M2 Implementation Plan must
include a **fixture-shape coverage check** that confirms each of
the four new tabs has sufficient data in the existing fixtures:

| Tab | Required fields (from `ViewSummary`) | Coverage assessment (proposal) |
|---|---|---|
| Berth Timeline | `berths[]`, `vessels[]` with assigned berths, `time_window`, `arrivals[]`, `departures[]` | Expected present; confirm in plan |
| Shift Log | `events[]` (or equivalent) with timestamps and actor + type | Expected present; confirm in plan; if gaps exist, prefer derived enrichment in adapter over new fixture capture |
| VTS summary | `vessels[]` with positions / headings / speeds; `conflicts[]` already in fixtures | Confirmed present (M1 uses conflicts) |
| Pilotage summary | `pilotage[]` or equivalent (assignments / queue) | Confirm in plan; if absent, decide whether to derive from `vessels[]` + arrivals, or capture additional fixture |

Important: if a fixture shape gap is found, **the default is
adapter-level derivation**, not new fixture capture. New fixture
capture is allowed only if derivation cannot reasonably produce
the required field, and must use Option 4b (local server, no
production touch) per M1 Retrospective §7.

### 6.3 Open-Meteo provenance caveat carried forward
Existing fixtures contain live-sourced Open-Meteo weather values
captured at fixture creation time (M1 Retrospective §8). This
provenance is unchanged by M2. M2 introduces no new live external
data path.

### 6.4 Fixture diff tooling (recommended)
M1 Retrospective §13.5 recommended a small fixture diff tool. M2
should either:
(a) implement it as part of M2 (low overhead), or
(b) defer it explicitly with a stated rationale.
The M2 Implementation Plan should pick one.

---

## 7. UI / component implications

### 7.1 New components (per tab)
Each new tab introduces a top-level component plus a small set of
sub-components, mirroring the M1 Dashboard structure under
`frontend/src/features/`.

| Tab | Proposed feature directory | Notable sub-components |
|---|---|---|
| Berth Timeline | `frontend/src/features/berth-timeline/` | `BerthTimelineTab.jsx`, `BerthRow.jsx`, `VesselSegment.jsx`, `TimeAxis.jsx` |
| Shift Log | `frontend/src/features/shift-log/` | `ShiftLogTab.jsx`, `ShiftLogRow.jsx`, `ShiftLogFilters.jsx` (read-only) |
| VTS | `frontend/src/features/vts/` | `VtsTab.jsx`, `VesselListPane.jsx`, `ConflictsList.jsx` |
| Pilotage | `frontend/src/features/pilotage/` | `PilotageTab.jsx`, `PilotageAssignmentRow.jsx` |
| Performance (placeholder) | `frontend/src/features/performance/` | `PerformanceTabPlaceholder.jsx` (or full component if M2 elects full delivery) |

### 7.2 Reused infrastructure
All tabs reuse:
- `useSummary` polling hook (no changes)
- Adapter pipeline (extended with new tab-specific adapters)
- Stale-data banner
- Persistent DEMO banner (M0)
- Top operational ribbon, conditions ribbon (M0)
- Three-column shell

### 7.3 Tab switcher / centre spine
Canon §1.1.3 requires the centre spine to host the canonical tab
set. M1 left this as the Dashboard tab only. M2 must introduce a
**tab switcher** in the centre spine. The tab switcher itself is
a small UI primitive; it has no backend dependency and emits no
events beyond local state changes.

### 7.4 No mode switch
M2 does **not** introduce mode switching (Canon §6) beyond the
existing operational-mode default. Replay mode (Canon §8) remains
deferred.

### 7.5 No DSW
M2 does **not** introduce the Decision Support Window (Canon §7).
DSW requires an operator action path and is therefore not
appropriate to M2.

### 7.6 No mobile
Mobile views remain deferred (Canon §9).

### 7.7 Accessibility / contrast
M2 must respect Canon §3 visual semantics (status colours,
contrast, density). The M2 Implementation Plan should include an
accessibility / contrast spot-check, even though no new colour
tokens are introduced.

---

## 8. Canon alignment

The proposed M2 scope is aligned with `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md`
as follows.

| Canon section | Alignment in M2 |
|---|---|
| §1.1 Four-region shell | Unchanged from M0/M1; reused |
| §1.1.3 Centre spine tabs | M2 delivers Berth Timeline, Shift Log, VTS, Pilotage (and Performance placeholder or full) — the remaining canonical tabs |
| §1.2 Region widths (360 \| 1fr \| 380) | Unchanged |
| §1.3 Conditions ribbon | Unchanged from M0; reused |
| §2 Interaction patterns | Read-only interactions only; no operator action patterns introduced |
| §3 Visual semantics | Reused; no new colour tokens proposed |
| §4 Seven-state lifecycle | Read-only display of lifecycle states allowed; lifecycle transitions deferred |
| §5 Panel behaviour | Left and right rails unchanged behavioural model; centre spine extended |
| §6 Mode system | Operational mode only; Replay deferred |
| §7 DSW | Deferred |
| §8 Replay | Deferred |
| §9 Mobile | Deferred |
| §10 Anti-patterns | Honoured — no map-based VTS interaction beyond read-only; no Berth Timeline editing; no scenario builders |
| §11 Recommended implementation order | M2 corresponds to the centre-spine completion step in the recommended order |

M2 explicitly honours Canon §10 anti-patterns. Specifically:
- VTS map interaction beyond read-only is **forbidden** by Canon
  §9 (mobile) and held read-only on desktop per this proposal.
- Berth Timeline editing and scenario builders are **forbidden**
  by Canon §9 (mobile) and held read-only on desktop per this
  proposal.

---

## 9. Architecture implications

### 9.1 No backend changes
M2 introduces **no backend changes**. `server.py` is untouched.
The audit ledger is untouched. The Beta 10 deployment is
untouched. The `horizon-prod`, Stage E-prod, and preview Postgres
environments are untouched.

### 9.2 Frontend architecture
M2 extends the existing `frontend/src/features/` layout with new
feature directories per tab. The adapter layer
(`frontend/src/api/adapters/`) gains tab-specific adapters
consuming the same `ViewSummary` shape and producing tab-shaped
view models. No change is needed to `summaryAdapter` itself.

### 9.3 Polling and stale-data model
Unchanged from M1. All tabs subscribe to the same `useSummary`
output. Tab switching does not retrigger polling.

### 9.4 Routing
M2 may introduce client-side routing for tab selection (e.g. URL
hash, query parameter, or in-memory state). The Implementation
Plan should decide whether routing is part of M2 or kept as
in-memory state until later. Recommendation: in-memory state
only, unless a clear reason emerges.

### 9.5 No new external dependencies
No new npm packages are anticipated. The M2 Implementation Plan
should confirm this and explicitly list any new dependency, with
justification, if one becomes necessary.

### 9.6 Independence Reset alignment
All M2 architecture is AMSG-owned and AMSG-authored, per the
Independence Reset (§5 Architecture implications, §13 Forbidden
framing, §14 Recommended wording). No Smart Ocean X framing
appears in any new code or document.

---

## 10. Railway / sandbox implications

### 10.1 No Railway config changes
M2 reuses the existing `/frontend/railway.json` config and the
existing `horizon-v1-sandbox` Railway service. **No Railway config
changes are proposed.** Root `railway.toml` remains immutable
(Beta 10's config).

### 10.2 Source branch
The M2 Implementation Plan should propose a new source branch
(`feat/v1-m2` or similar) and a Tony-side switch of the
`horizon-v1-sandbox` source branch at Phase 2. The execution
agent does **not** perform Railway dashboard changes.

### 10.3 Two-phase deploy pattern
Phase 1: local validation (Vitest, lint, build). Phase 2:
Tony-side source-branch switch + Claude read-only sandbox
verification (bundle hash match, fetch-target sanity check, no
production URLs in bundle, `instanceStatus: RUNNING`).

### 10.4 Sandbox cost
The `horizon-v1-sandbox` continues to be Tony-controlled. M2 does
not propose continuous deployment; deploys are explicit per
phase.

### 10.5 Config Path setting
The lesson from M1 Retrospective §9 holds: service-level Config
Path setting in the Railway dashboard takes precedence and is
already pointed at `/frontend/railway.json`. M2 must not require
this to change.

---

## 11. Beta 10 protection approach

M2 protects Beta 10 by **construction**, not by convention.

11.1 **No live Beta 10 calls.** `VITE_API_BASE` continues to
default to `/fixtures`. No production URL is embedded in the M2
bundle.

11.2 **No `server.py` changes.** Beta 10 backend code is untouched.

11.3 **No `set_port` mutation.** Any new fixture (if required) is
captured via Option 4b (local server.py, no production touch).

11.4 **No Stage E-prod activity.** Stage E-prod remains paused.

11.5 **No preview Postgres touch.** Preview-audit-activation
Postgres remains untouched.

11.6 **No root `railway.toml` changes.** Beta 10 deployment
config remains untouched.

11.7 **Tony performs Beta 10 visual check post-M2 merge.** Same
pattern as M1 (Retrospective §2). The visual check covers `/`,
`/login`, port switcher across all four ports, and demo
behaviour invariance.

11.8 **Regression gate `tests/test_beta10_regression.py` remains
46/46.** Any failure blocks merge. If M2 introduces new
fixture-shape derivations or adapters, these must not regress the
existing test set.

11.9 **Bundle inspection at Phase 2.** Sandbox bundle must show
only `/fixtures/*` as a fetch target. Any unexpected URL in the
bundle blocks M2 close.

---

## 12. Smart Ocean X independence confirmation

This proposal confirms the following, in alignment with
`HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md`:

12.1 The Smart Ocean X architectural dependency is **formally
closed** and remains so under this proposal.

12.2 No M2 document, comment, or code identifier may reference
Smart Ocean X as a current dependency, substrate, or partner.

12.3 M2 uses neutral framing per Independence Reset §13 and §14:
"Horizon" / "AMSG Horizon" / "Horizon by AMS Group" only.

12.4 Historical / legal references to Smart Ocean X (e.g. the
preserved agreement) are not touched, not rewritten, and not
referenced as architectural inputs.

12.5 Any drift back to Smart Ocean X framing in M2 documents,
PRs, or comments is a governance breach and must be raised and
corrected immediately.

12.6 The Independence Reset baseline (`b0d9ff2`) and the M1
Retrospective baseline (`dde3a84`) are both upstream of any M2
work. M2 cannot exist without acknowledging them.

---

## 13. Risks and mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 13.1 | Fixture shape gaps for new tabs | Medium | Medium | Fixture-shape coverage check in M2 Implementation Plan §6.2; adapter-level derivation preferred over new fixture capture |
| 13.2 | UI complexity (Berth Timeline Gantt) drags milestone | Medium | Medium | Limit M2 Berth Timeline to read-only static Gantt; no drag, no zoom, no edit |
| 13.3 | Performance tab attempts to grow M2 scope | Medium | Medium | Default Performance to placeholder; explicit decision in M2 Implementation Plan |
| 13.4 | New dependency creep (charting libraries, map libraries) | Low–Medium | Medium | Plan must explicitly list any new dependency and justify it; default is none |
| 13.5 | Mode switch / Replay creeps into M2 | Low | Medium | Out-of-scope list (§4) is closed; tab switcher is not a mode switcher |
| 13.6 | Operator action endpoints requested mid-M2 (ACK / COMMIT etc.) | Low | High | Out-of-scope list (§4); requires separate authorisation |
| 13.7 | Auth or RBAC requested mid-M2 | Low | High | Out-of-scope list (§4); requires separate authorisation |
| 13.8 | Live Beta 10 read requested mid-M2 | Low | High | Out-of-scope list (§4); requires separate authorisation |
| 13.9 | Railway config drift | Low | High | Tony-side control; execution agent does not touch Railway settings; root `railway.toml` immutable |
| 13.10 | Smart Ocean X framing reappears in new documents | Low | Medium | Independence Reset §13 / §14 framing required; ChatGPT review gate |
| 13.11 | Audit emission requested mid-M2 | Low | High | Out-of-scope list (§4); requires separate authorisation |
| 13.12 | Database dependency requested mid-M2 | Low | High | Out-of-scope list (§4); requires separate authorisation |
| 13.13 | Beta 10 regression on M2 deploy | Very low | Very high | Beta 10 untouched by construction; Tony visual check post-merge |
| 13.14 | Tab switcher introduces routing complexity | Medium | Low | Default to in-memory state; URL routing deferred unless explicitly authorised |
| 13.15 | Performance tab partial delivery confuses operators | Low | Low | Placeholder copy explicit ("Performance — deferred to M2.5 / M3"); Canon §11 anti-pattern check |

No risk in this table is currently high-likelihood and
high-impact. The governance pattern from M1 is the primary
control.

---

## 14. Open questions

The M2 Implementation Plan PR must resolve each of these before
implementation begins.

14.1 **Performance tab posture**: placeholder only, partial, or
full? Default proposal: placeholder.

14.2 **Pilotage fixture-shape**: does the existing
`ViewSummary` carry enough fields, or is adapter-level derivation
required? If neither suffices, is Option 4b refresh appropriate?

14.3 **Tab switcher routing**: in-memory state vs. URL hash vs.
query parameter. Default proposal: in-memory state.

14.4 **Fixture diff tooling**: in M2 or deferred? Default
proposal: defer with explicit rationale, unless a fixture refresh
is required.

14.5 **Shift Log filtering**: which filters are part of M2 (time
window, actor, event type)? Default proposal: read-only display
with **no** filters in M2; filters arrive later.

14.6 **VTS pane density**: how many vessels are typically
present, and does the VTS pane need pagination or virtualisation
in M2? Default proposal: no virtualisation in M2 (current fixture
vessel counts are small).

14.7 **Berth Timeline time window**: fixed 24-hour shift window,
or operator-configurable? Default proposal: fixed shift window
based on `time_window` from `ViewSummary`.

14.8 **Component-level test coverage target**: Vitest expectation
for new tabs. Default proposal: parity with M1 adapter / hook
coverage; component snapshot tests not required.

14.9 **Bundle size budget**: should M2 introduce an explicit
budget? Default proposal: yes — record current M1 bundle size and
target M2 within +30% as a soft guard.

14.10 **Sandbox naming**: does M2 deploy to the same
`horizon-v1-sandbox` service or a new sandbox service? Default
proposal: same service (Tony-side branch switch).

---

## 15. Acceptance criteria

The proposed acceptance criteria below are **proposed**, not
authorised. They are anchored to the M1 acceptance criteria shape
(M1 Retrospective §3) and the V1 Execution Plan §11.

The M2 Implementation Plan must adopt, refine, or reject each
item explicitly.

A1. M2 fixtures: either reused from M1 unchanged, or new captures
via Option 4b only. No production touch.

A2. Adapter coverage: tab-specific adapters implemented per
Adapter Design Note §2.2 contract; no `summaryAdapter` shape
change without explicit Plan-level note.

A3. Polling: `useSummary` reused unchanged.

A4. Tabs delivered: Berth Timeline, Shift Log, VTS, Pilotage —
all read-only — and Performance (placeholder or full per the
Plan's resolved decision).

A5. Tab switcher in the centre spine, in-memory state (unless
the Plan resolves to a different posture).

A6. Unit tests added (Vitest) covering new adapters and helpers;
target test count and shape recorded in the Plan.

A7. Regression gate `tests/test_beta10_regression.py` remains
green at 46/46 throughout (pre-merge and post-merge).

A8. Railway sandbox `horizon-v1-sandbox` serves the M2 build
from the new `feat/v1-m2` source branch (Tony-side switch).

A9. Bundle hash matches local build; no production URLs in
bundle; only `/fixtures` as fetch target.

A10. PR merged to `main` with single, tightly scoped diff.

A11. Tony's Beta 10 post-M2 visual check passes (`/`, `/login`,
port switcher across BNE / MEL / GEX / DAR, no errors, demo
behaviour unchanged).

A12. Smart Ocean X independence confirmed in the M2 retrospective.

A13. No new auth, no writes, no audit emission, no Stage E-prod,
no database dependency, no `server.py` change, no root
`railway.toml` change introduced.

---

## 16. Recommendations

16.1 **Authorise an M2 Implementation Plan PR** (single
markdown file at root), branched off `origin/main @ dde3a84`,
adopting the recommended scope in §3 with explicit decisions on
the open questions in §14.

16.2 **Maintain the M1 governance pattern.** Explicit Tony
authorisation per task; ChatGPT pre-merge review gate; strict
deny-list per authorisation; stop-for-review markers honoured.

16.3 **Keep M2 read-only and fixture-fed by default.** Any
deviation must be raised in the Plan and authorised separately.

16.4 **Preserve Beta 10 protection by construction**, not by
convention (§11 of this proposal).

16.5 **Honour the Independence Reset framing in every M2
artefact**, code, document, and comment.

16.6 **Default Performance to placeholder** unless the Plan
resolves otherwise.

16.7 **Default fixture handling to adapter-level derivation**.
Avoid new fixture capture unless derivation cannot meet
requirements; if capture is needed, use Option 4b only.

16.8 **Record a bundle-size soft budget** as an M2 hygiene
control.

16.9 **Do not progress automatically from this proposal to M2
implementation.** A separate explicit "Authorised: draft M2
Implementation Plan" message from Tony is required.

16.10 **At M2 close, produce an M2 Retrospective** mirroring the
M1 Retrospective shape (Retrospective §15.8 of M1).

---

## End of M2 scope proposal

This document is descriptive, advisory, and proposes scope only.

Confirmed by this document:
- M2 is **not** authorised by this proposal.
- Any M2 implementation requires a separate, explicitly
  authorised **M2 Implementation Plan** PR and Tony
  authorisation.
- M2 is proposed to remain **fixture-backed and read-only**.
- M2 is proposed to introduce **no live Beta 10 calls, no
  production `/api/*` calls, no auth, no RBAC, no ACK / COMMIT /
  DEFER / OVERRIDE / ESCALATE writes, no audit emission, no
  Stage E-prod activity, no database dependency, no `server.py`
  changes, no root `railway.toml` changes, and no production
  state changes**.
- The Smart Ocean X architectural dependency remains **formally
  closed** under the Independence / Architecture Reset
  (PR #51, `b0d9ff2`).
- The M1 Retrospective (PR #52, `dde3a84`) governs the
  governance pattern this proposal aligns to.

**Next action:** ChatGPT engineering review of this proposal,
then Tony's review. If approved, Tony authorises a separate **M2
Implementation Plan** PR as the next governance step. M2
implementation does **not** begin until that Plan is also
authorised.
