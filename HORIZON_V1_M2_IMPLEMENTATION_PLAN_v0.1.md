# Horizon V1 — M2 Implementation Plan (v0.1)

**Document status:** Draft for review
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 8c73c55`
  (Merge #53 — M2 Scope Proposal, on top of Merge #52 — M1
  Retrospective, on top of Merge #51 — Horizon Independence /
  Architecture Reset, on top of M1 merge `e90eb56`)
**Date:** 2026-05-20
**Scope of authority:** Defines the concrete implementation plan
  for Milestone M2 of the Horizon V1 programme. This is **a plan**,
  not an authorisation to write code. It does **not** authorise M2
  implementation, does **not** change any code, fixtures, or
  infrastructure, and does **not** modify any previously merged
  document.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53, `8c73c55`)
- `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md` (PR #52, `dde3a84`)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51, `b0d9ff2`)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` (PR #48)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- `HORIZON_V1_EXECUTION_PLAN_v0.1.md` (PR #36)

---

## 1. Executive summary

M2 implements four additional read-only centre-spine tabs (Berth
Timeline, Shift Log, VTS summary, Pilotage summary) and a
deliberately limited Performance surface, on top of the existing
M1 fixture-fed pipeline. The implementation honours all
constraints recorded in the M2 Scope Proposal: no live Beta 10
calls, no production `/api/*` calls, no auth, no RBAC, no writes,
no audit emission, no Stage E-prod activity, no database
dependency, no `server.py` changes, no root `railway.toml`
changes, no production state changes, and no Smart Ocean X
framing.

The plan resolves the open questions from M2 Scope Proposal §14
with explicit default choices. Performance is delivered as a
**placeholder tab** in M2 with a clear "deferred" notice. The
tab switcher uses **in-memory state**, not URL routing. Fixture
needs are met by **adapter-level derivation** wherever possible;
no production capture is authorised by this plan.

This plan is **descriptive and prescriptive**, not an
authorisation. M2 implementation requires a **separate explicit
Tony authorisation** after this plan is reviewed and merged.
Until that authorisation is received, no M2 code is written, no
branch is created, and no Railway service is touched. The Smart
Ocean X architectural dependency remains **formally closed**
under the Independence / Architecture Reset (PR #51, `b0d9ff2`).

---

## 2. Resolved M2 scope

The Scope Proposal listed candidate scope. This plan **resolves**
each item explicitly. All five tabs below are read-only.

| # | Tab | Posture in M2 | Resolution rationale |
|---|---|---|---|
| 1 | **Berth Timeline** | **In scope — read-only static Gantt** | Highest operator value among new tabs; fixture data supports it via derivation |
| 2 | **Shift Log** | **In scope — read-only table** | Adapter-derivable from existing fixture events; no filters in M2 |
| 3 | **VTS summary** | **In scope — read-only vessel + conflict pane** | Direct reuse of vessel and conflict slices already adapted in M1 |
| 4 | **Pilotage summary** | **In scope — read-only assignments view** | Derivation-first; new capture only if derivation insufficient |
| 5 | **Performance** | **Placeholder only** | M2 Scope Proposal §3.2 default posture; full delivery deferred to M2.5 / M3 |

Resolved decisions on M2 Scope Proposal §14 open questions:

| § | Open question | Resolved decision |
|---|---|---|
| 14.1 | Performance posture | **Placeholder only.** Stub header + explicit "deferred to M2.5 / M3" notice. No data binding. |
| 14.2 | Pilotage fixture shape | **Adapter-level derivation first.** New Option 4b capture only if derivation cannot meet requirements; ChatGPT/Tony decision required before any capture. |
| 14.3 | Tab switcher routing | **In-memory state only.** No URL hash, no query parameter. URL routing deferred. |
| 14.4 | Fixture diff tooling | **Deferred** to M2.5 / M3. No new tooling in M2; rationale: no new fixtures planned. |
| 14.5 | Shift Log filtering | **No filters in M2.** Read-only display only. |
| 14.6 | VTS pane density | **No virtualisation in M2.** Current fixture vessel counts are small. |
| 14.7 | Berth Timeline time window | **Fixed shift window** from `ViewSummary.time_window`. Operator-configurable windows deferred. |
| 14.8 | Test coverage target | **Parity with M1 adapter / hook test coverage.** Component snapshot tests not required. |
| 14.9 | Bundle size budget | **Recorded as soft guard: current M1 bundle + 30 %.** Hard fail not introduced; reported in Phase 2 verification. |
| 14.10 | Sandbox naming | **Same `horizon-v1-sandbox` service.** Tony-side branch switch from `feat/v1-m1` to `feat/v1-m2`. |

Acceptance criteria from M2 Scope Proposal §15 (A1 – A13) are
adopted unchanged and recorded in §16 of this plan.

---

## 3. Intended branch name

**Proposed branch:** `feat/v1-m2`

**Branched off:** `origin/main @ 8c73c55` (or later, at the time
of authorisation; see §19 Phase sequencing).

The branch is **not** created by this plan. It is created **only
after** Tony's explicit "Authorised: begin Horizon V1 M2
implementation" message. Until that message, no branch exists,
no commits exist, no PRs exist.

---

## 4. File / folder scaffold

The scaffold below is **planned**, not created. Paths follow the
M1 pattern (`frontend/src/features/<feature>/`).

### 4.1 New feature directories (under `frontend/src/features/`)

```
frontend/src/features/
├── dashboard/                   ← existing (M1)
├── berth-timeline/              ← NEW (M2)
│   ├── BerthTimelineTab.jsx
│   ├── BerthRow.jsx
│   ├── VesselSegment.jsx
│   ├── TimeAxis.jsx
│   └── index.js
├── shift-log/                   ← NEW (M2)
│   ├── ShiftLogTab.jsx
│   ├── ShiftLogRow.jsx
│   └── index.js
├── vts/                         ← NEW (M2)
│   ├── VtsTab.jsx
│   ├── VesselListPane.jsx
│   ├── ConflictsList.jsx
│   └── index.js
├── pilotage/                    ← NEW (M2)
│   ├── PilotageTab.jsx
│   ├── PilotageAssignmentRow.jsx
│   └── index.js
└── performance/                 ← NEW (M2, placeholder)
    ├── PerformanceTabPlaceholder.jsx
    └── index.js
```

### 4.2 New adapter modules (under `frontend/src/api/adapters/`)

```
frontend/src/api/adapters/
├── conditionsAdapter.js         ← existing (M1)
├── conflictAdapter.js           ← existing (M1)
├── dashboardAdapter.js          ← existing (M1)
├── etdRiskAdapter.js            ← existing (M1)
├── guidanceAdapter.js           ← existing (M1)
├── status.js                    ← existing (M1)
├── summaryAdapter.js            ← existing (M1) — no shape change
├── time.js                      ← existing (M1)
├── units.js                     ← existing (M1)
├── vesselAdapter.js             ← existing (M1)
├── berthTimelineAdapter.js      ← NEW (M2)
├── shiftLogAdapter.js           ← NEW (M2)
├── vtsAdapter.js                ← NEW (M2)
└── pilotageAdapter.js           ← NEW (M2)
```

### 4.3 New / extended layout files

```
frontend/src/layout/
├── CenterPanel.jsx              ← MODIFIED — add tab switcher + tab routing
└── TabSwitcher.jsx              ← NEW (M2) — in-memory state, no URL routing
```

### 4.4 New test files (under `frontend/src/__tests__/` or co-located)

One test file per new adapter and one per new tab, following the
M1 layout. Vitest is reused; no new test framework.

```
frontend/src/__tests__/
├── adapters/
│   ├── berthTimelineAdapter.test.js     ← NEW (M2)
│   ├── shiftLogAdapter.test.js          ← NEW (M2)
│   ├── vtsAdapter.test.js               ← NEW (M2)
│   └── pilotageAdapter.test.js          ← NEW (M2)
└── features/
    ├── berthTimeline.test.jsx           ← NEW (M2)
    ├── shiftLog.test.jsx                ← NEW (M2)
    ├── vts.test.jsx                     ← NEW (M2)
    └── pilotage.test.jsx                ← NEW (M2)
```

### 4.5 Files explicitly NOT touched in M2

- `server.py` (root) — untouched
- `railway.toml` (root) — untouched (Beta 10 config, immutable)
- `frontend/railway.json` — untouched (M0/M1 config, working)
- `frontend/public/fixtures/*.json` — untouched (M1 fixtures reused unchanged)
- `frontend/src/api/horizon.js` — untouched (M1 fetch wrapper)
- `frontend/src/hooks/useSummary.js` — untouched (M1 hook reused)
- `frontend/src/api/adapters/summaryAdapter.js` — untouched (shape preserved)
- `tests/test_beta10_regression.py` — untouched (regression gate)
- Audit helpers, port profiles, scrapers, BoM/tides/weather/MST/AIS modules — untouched
- Any preview-audit-activation file — untouched
- Any Stage E-prod file — untouched

---

## 5. Dependency changes, if any

**Default:** no new npm dependencies in M2.

The M2 work uses only:
- React 18 (existing)
- Vite 5 (existing)
- date-fns + date-fns-tz (existing)
- Vitest (existing)

The Berth Timeline is the only tab that could conceivably pull in
a charting / Gantt library. **This is explicitly avoided.** The
Berth Timeline renders as a CSS-grid Gantt with `position:
absolute` time segments; no charting library is added.

If during implementation a dependency need is discovered, the
work **stops** and a separate explicit Tony authorisation is
requested with rationale.

---

## 6. Data and fixture strategy

### 6.1 Fixture set
The five fixtures from PR #49 (`brisbane-busy.json`,
`brisbane-quiet.json`, `melbourne-sim.json`, `null-fields.json`,
`malformed.json`) are reused **unchanged**.

### 6.2 No new fixture capture
M2 does **not** capture new fixtures by default. New capture is
allowed **only** if adapter-level derivation cannot meet
requirements for one of the four tabs, and only via Option 4b
(local `server.py`, no production touch) per M1 Retrospective §7.
Any new capture requires a separate explicit Tony authorisation.

### 6.3 Provenance carried forward
Existing fixtures contain live-sourced Open-Meteo weather values
captured at fixture time. This is pre-existing, documented in M1
Retrospective §8 and the fixtures README. M2 introduces no new
live external data path.

### 6.4 Fixture-shape coverage check
Before implementation of each tab, the adapter author confirms
that the required `ViewSummary` fields are present (or derivable)
in all four operational fixtures (`brisbane-busy.json`,
`brisbane-quiet.json`, `melbourne-sim.json`, `null-fields.json`).
The `malformed.json` fixture remains the defence test for
adapter resilience.

### 6.5 No fixture diff tooling in M2
Per §2 / §14.4: deferred. Rationale recorded: no new fixtures
planned in M2, so the diff tooling has no immediate utility.

---

## 7. Adapter derivation plan

Each new adapter follows the M1 pattern: a pure function that
takes `ViewSummary` (the M1 `summaryAdapter` output) and returns
a tab-specific view model. Adapters are pure, deterministic, and
unit-tested with Vitest.

### 7.1 `berthTimelineAdapter.js`
- **Input:** `ViewSummary` (existing M1 shape)
- **Reads:** `berths[]`, `vessels[]`, `arrivals[]`, `departures[]`, `time_window`
- **Output:** `{ rows: [{ berth_id, label, segments: [{ vessel_id, start, end, status }] }], time_window: { start, end } }`
- **Derivation:** For each berth, walk arrivals/departures and assigned vessels, produce non-overlapping time segments. Where data is sparse, missing fields render as gaps (no fabrication).
- **Null defence:** Inputs `null` / `undefined` → empty rows.

### 7.2 `shiftLogAdapter.js`
- **Input:** `ViewSummary`
- **Reads:** `events[]` (or equivalent; if absent, derive from arrivals/departures/conflicts)
- **Output:** `{ rows: [{ id, timestamp, type, actor, description }] }`
- **Derivation:** Prefer an `events[]` field if present. If absent in fixtures, derive a synthetic event log from arrivals/departures + conflict transitions (clearly typed as derived).
- **Null defence:** Empty input → empty rows; UI shows "No events for current shift."

### 7.3 `vtsAdapter.js`
- **Input:** `ViewSummary`
- **Reads:** `vessels[]`, `conflicts[]`
- **Output:** `{ vessels: [{ id, name, heading, speed, status, has_conflict }], conflicts: [{ id, vessels, severity }] }`
- **Derivation:** Trivial mapping; `has_conflict` derived by cross-referencing each vessel's id against `conflicts[].vessels`.
- **Null defence:** Empty input → empty pane with "No vessels in view" notice.

### 7.4 `pilotageAdapter.js`
- **Input:** `ViewSummary`
- **Reads:** `pilotage[]` if present; otherwise derive from `vessels[]` + `arrivals[]` (vessels requiring pilots based on profile flags).
- **Output:** `{ assignments: [{ vessel_id, pilot, eta, status }] }`
- **Derivation:** If `pilotage[]` absent and derivation produces sparse data, render the empty / sparse state honestly. No fabrication.
- **Null defence:** Empty input → empty assignments list with "No pilotage assignments" notice.
- **Decision gate:** If derivation produces results that do not match operator expectations, **stop** and request new-fixture authorisation (per §6.2).

### 7.5 No `summaryAdapter.js` shape change
The 20-key `ViewSummary` contract from the Adapter Design Note
§2.2 is **not** modified by M2. Any change to `summaryAdapter`
shape is out of scope and requires a separate Plan amendment.

---

## 8. Tab implementation plan

Each tab follows the M1 Dashboard pattern: a top-level
`<TabName>Tab.jsx` consuming the appropriate adapter output
through `useSummary`, with sub-components for visual elements.

### 8.1 Berth Timeline tab
- Render a CSS-grid Gantt: rows = berths, columns = time slots
  derived from `time_window`.
- Vessel segments rendered as positioned blocks (`position:
  absolute`) within rows.
- Status colours come from existing M1 status tokens (Canon §3).
- Read-only: no click handlers beyond highlighting; no drag, no
  resize, no edit.
- Stale-data banner reused from M1.
- "No data" state explicit (empty Gantt rows + notice).

### 8.2 Shift Log tab
- Render a vertical table of events ordered most-recent-first.
- Columns: timestamp (with timezone), type, actor, description.
- Read-only: no row click actions, no comment input, no
  acknowledgement.
- "No events" state explicit.

### 8.3 VTS tab
- Two-pane layout: vessel list on left, conflicts list on right
  (or top/bottom — to be confirmed during component layout pass).
- Read-only: vessel rows highlight on hover; clicking a vessel
  produces only a visual focus state, no navigation, no DSW.
- No map. Map-based VTS interaction beyond read-only is forbidden
  by Canon §10 anti-patterns.

### 8.4 Pilotage tab
- Single-table assignments view.
- Columns: vessel, pilot, ETA, status.
- Read-only: no assignment edit, no dispatch, no cancel.
- "No assignments" state explicit.

### 8.5 Performance tab (placeholder)
- Renders only a stub header and an explicit notice:
  *"Performance — deferred to M2.5 / M3. Read-only metrics will
  appear here in a future milestone."*
- No data binding. No adapter wired in.
- Listed in tab switcher for layout completeness and Canon
  §1.1.3 alignment.

### 8.6 Tab switcher
- New component `frontend/src/layout/TabSwitcher.jsx`.
- In-memory state (React `useState`).
- Default tab on load: Dashboard (M1 default preserved).
- Tab list: Dashboard · Berth Timeline · Shift Log · VTS ·
  Pilotage · Performance.
- Tab switching does not retrigger polling; `useSummary` remains
  shared and single-flight.
- No URL hash / query parameter.

---

## 9. Component implementation plan

### 9.1 React patterns
- Function components only; no class components.
- Hooks for state; no Redux, no MobX, no third-party state lib.
- Props are plain JS objects; no PropTypes or TypeScript
  introduction (matching M1).
- Components are pure where possible; side effects only in
  hooks.

### 9.2 Styling
- Reuse existing CSS layer (`frontend/src/styles/layout.css` and
  tokens). New tab-specific CSS may extend the existing
  stylesheet; no new CSS framework introduced.
- Status colours and density per Canon §3.

### 9.3 Time and units
- All time formatting goes through `frontend/src/api/adapters/time.js`
  (date-fns-tz). No raw `new Date()` rendering.
- Unit conversion (nm ↔ km) goes through `units.js`.

### 9.4 Polling
- All tabs subscribe to the same `useSummary` output. Tab switch
  does not re-fetch.
- Stale-data banner is shared across tabs (top of centre spine).

### 9.5 Loading and empty states
- Each tab renders an explicit empty / loading / stale state.
- No tab silently renders a half-populated table.

### 9.6 Defensive rendering
- Adapter outputs may be empty arrays under null-fields /
  malformed fixtures. Each component handles empty rows / empty
  panes with an explicit notice.

---

## 10. UI labelling and read-only safeguards

### 10.1 Read-only labelling
- The persistent DEMO banner from M0 is preserved.
- Each new tab inherits the existing stale-data banner.
- No "Acknowledge", "Commit", "Defer", "Override", "Escalate"
  buttons appear anywhere in M2 code. Their absence is enforced
  by code review.

### 10.2 Defensive copy
- Pilotage tab text: "Read-only summary."
- Shift Log tab text: "Read-only event log."
- VTS tab text: "Read-only vessel view."
- Berth Timeline tab text: "Read-only schedule view."
- Performance tab text (placeholder): "Performance — deferred to
  M2.5 / M3."

### 10.3 No DSW
The Decision Support Window (Canon §7) is **not** implemented in
M2. Any component named or referenced as DSW is a code-review
fail.

### 10.4 No mode switch
Operational mode only. Replay mode (Canon §8) is **not**
implemented in M2.

### 10.5 No mobile entry points
Mobile-specific routes / components are **not** implemented in
M2 (Canon §9 — mobile-specific items remain deferred).

---

## 11. What must remain mocked / static

The following items are intentionally mocked, derived, or static
in M2 and must not be wired to any live source:

| Item | Posture in M2 | Source |
|---|---|---|
| `/api/summary` payload | Fixture (`frontend/public/fixtures/*.json`) | Static JSON |
| Weather conditions | Embedded in fixtures at capture time | Static (Open-Meteo provenance disclosed) |
| Tides, swell | Embedded in fixtures at capture time | Static |
| Berth assignments / Gantt segments | Derived from fixtures | Static |
| Shift log events | Derived from fixtures (if `events[]` absent) | Static (synthetic, typed as derived) |
| VTS vessel positions | From fixtures | Static |
| Conflicts | From fixtures | Static |
| Pilotage assignments | Derived or from fixtures | Static |
| Performance metrics | **Not rendered** | Placeholder only |
| Audit ledger | **Not read, not written** | n/a |
| Auth / session | **Not implemented in M2** | n/a |
| Operator actions | **Not implemented in M2** | n/a |
| Real-time updates beyond polling | **Not implemented in M2** | n/a |
| Map tiles for VTS | **Not implemented in M2** | n/a |

---

## 12. What must not be implemented

This list is the **deny-list** for M2 implementation. If any item
appears in a PR diff during M2, the PR is rejected.

12.1 Live Beta 10 backend calls of any kind.
12.2 Production `/api/*` calls of any kind.
12.3 Auth (login gate, session check, RBAC).
12.4 Server-side RBAC.
12.5 ACK writes (acknowledgement of conflicts / recommendations /
events).
12.6 COMMIT, DEFER, OVERRIDE, ESCALATE — any operator action
endpoint.
12.7 Audit emission of any kind.
12.8 Stage E-prod activity (Stage E-prod remains paused).
12.9 Database dependency (no Postgres connection; no
preview-audit-activation Postgres touch).
12.10 `server.py` changes (any direction).
12.11 Root `railway.toml` changes (Beta 10 config, immutable).
12.12 Production state changes (no `set_port` against Beta 10,
no production data refresh).
12.13 New external services / live external data dependencies
introduced from V1.
12.14 New backend endpoints.
12.15 Smart Ocean X framing in any new document, comment, or
identifier.
12.16 Charting / Gantt libraries (Berth Timeline is CSS-grid).
12.17 URL-based tab routing (in-memory only).
12.18 DSW component (Canon §7) — deferred.
12.19 Replay mode (Canon §8) — deferred.
12.20 Mobile-specific routes (Canon §9) — deferred.
12.21 Performance tab full content (placeholder only in M2).

---

## 13. Beta 10 protection approach

M2 protects Beta 10 by **construction**:

13.1 No live Beta 10 calls. `VITE_API_BASE` defaults to
`/fixtures`. Phase 2 bundle inspection must confirm no Beta 10
URL is embedded.

13.2 No `server.py` changes.

13.3 No `set_port` mutation against Beta 10. New fixtures, if
required, are captured via Option 4b only.

13.4 No Stage E-prod activity.

13.5 No preview Postgres touch.

13.6 No root `railway.toml` changes.

13.7 Tony performs Beta 10 visual check post-M2 merge (same
pattern as M1 Retrospective §2): `/`, `/login`, port switcher
across BNE / MEL / GEX / DAR, demo behaviour invariance.

13.8 Regression gate `tests/test_beta10_regression.py` remains
46/46 throughout (Phase 1 and Phase 2). A failure blocks merge.

13.9 Bundle inspection at Phase 2: only `/fixtures/*` fetch
targets allowed.

---

## 14. Railway sandbox deployment handling

### 14.1 Same sandbox service
`horizon-v1-sandbox` is reused. No new Railway project, no new
service, no new environment created in M2.

### 14.2 Tony-side source-branch switch
At Phase 2, Tony switches the `horizon-v1-sandbox` source branch
from `feat/v1-m1` to `feat/v1-m2`. The execution agent does
**not** perform this action.

### 14.3 No Railway config changes
`/frontend/railway.json` continues to govern. Service-level
Config Path remains `/frontend/railway.json`. No changes
required.

### 14.4 Read-only verification by the execution agent
After Tony switches the source branch and Railway redeploys, the
execution agent performs **read-only** verification: deploy
status, instance status, bundle hash match, fetch-target sanity
check, no production URLs in bundle, regression gate green
locally on the same commit.

### 14.5 No autoscale, no continuous deploy
Each phase deploy is explicit. No background redeploys.

### 14.6 No preview environment
M2 does not require a preview environment. The single sandbox
service is sufficient.

---

## 15. Validation gates

### 15.1 Phase 1 (local)
- Vitest: all new and existing tests pass.
- Eslint (if configured): clean.
- `npm run build`: succeeds without warnings.
- `tests/test_beta10_regression.py`: 46/46 PASS.
- Manual local smoke: load each tab against `brisbane-busy`,
  `brisbane-quiet`, `melbourne-sim`, `null-fields`, `malformed`.
  Each tab must render an explicit state under each fixture
  (data, empty, or stale).

### 15.2 Phase 2 (sandbox)
- Tony switches `horizon-v1-sandbox` source branch to `feat/v1-m2`.
- Execution agent verifies (read-only):
  - `instanceStatus: RUNNING`
  - Deploy `commitHash` matches local HEAD.
  - Bundle hash matches local `npm run build` output.
  - Only `/fixtures/*` fetch targets in bundle.
  - No production URLs (no Beta 10 hostname) in bundle.
  - Regression gate 46/46 locally on the same commit.

### 15.3 Pre-merge
- Single tightly scoped diff (frontend + adapters + tests only).
- No diff to `server.py`, root `railway.toml`, audit modules,
  scrapers, weather/tides/BoM/MST/AIS modules, or fixtures.
- `gh pr view` shows CLEAN / MERGEABLE.
- ChatGPT pre-merge review complete.
- Tony's merge authorisation message received.

### 15.4 Post-merge
- `origin/main` advances to the merge SHA.
- Regression gate 46/46.
- No Railway config changes in diff.
- Tony's Beta 10 visual check (post-merge or scheduled).
- PRs #27 / #28 / #29 untouched.

---

## 16. Acceptance criteria

Adopted unchanged from M2 Scope Proposal §15.

A1. M2 fixtures: reused from M1 unchanged. No production touch.

A2. Adapter coverage: tab-specific adapters implemented per
Adapter Design Note §2.2 contract; no `summaryAdapter` shape
change.

A3. Polling: `useSummary` reused unchanged.

A4. Tabs delivered: Berth Timeline, Shift Log, VTS, Pilotage —
all read-only — and Performance placeholder.

A5. Tab switcher in the centre spine, in-memory state.

A6. Unit tests added (Vitest) covering new adapters and helpers;
parity with M1 coverage style.

A7. Regression gate `tests/test_beta10_regression.py` remains
green at 46/46 throughout.

A8. Railway sandbox `horizon-v1-sandbox` serves M2 build from
`feat/v1-m2` (Tony-side switch).

A9. Bundle hash matches local build; no production URLs in
bundle; only `/fixtures` as fetch target.

A10. PR merged to `main` with single, tightly scoped diff.

A11. Tony's Beta 10 post-M2 visual check passes.

A12. Smart Ocean X independence confirmed in the M2
retrospective.

A13. No new auth, no writes, no audit emission, no Stage E-prod,
no database dependency, no `server.py` change, no root
`railway.toml` change introduced.

---

## 17. Rollback plan

### 17.1 Pre-merge rollback (during implementation)
- All M2 work occurs on `feat/v1-m2`. To roll back any work in
  progress, the branch can be force-reset locally by Claude on
  explicit Tony authorisation, or deleted entirely.
- No production state is touched at any point; no rollback of
  production is required.

### 17.2 Post-merge rollback
- If a critical defect is discovered after PR merge to `main`,
  the rollback is a normal git revert of the merge commit.
- Because M2 introduces no backend, no database migration, no
  audit emission, and no production state change, the revert is
  purely a code-level operation — no data rollback is required.
- The `horizon-v1-sandbox` would redeploy from the reverted
  `main` automatically (if Tony switches it back) or remain on
  the prior `feat/v1-m2` build pending Tony's source-branch
  decision.

### 17.3 Sandbox rollback
- If sandbox bundle inspection at Phase 2 fails (e.g. a
  production URL appears in the bundle), Phase 2 stops
  immediately. Tony switches `horizon-v1-sandbox` source branch
  back to `feat/v1-m1` to restore M1 operational state. Claude
  diagnoses, fixes locally on `feat/v1-m2`, and re-requests
  Phase 2 verification.

### 17.4 Beta 10 rollback
- Beta 10 is not touched by M2 implementation by construction.
- If a post-merge Beta 10 visual check by Tony surfaces an
  unexpected regression (extremely unlikely, given no backend
  change), Claude diagnoses **read-only** and proposes a
  fix-forward PR; revert is also available.

### 17.5 Phase-0 baseline
- `phase-0-complete @ 4ad4aae` remains immutable through any
  rollback. The tag is not touched.

---

## 18. Stop conditions

M2 implementation **stops immediately** and requests a separate
Tony authorisation if any of the following occurs:

18.1 An item from §12 (deny-list) is required to satisfy any
acceptance criterion.

18.2 An adapter cannot derive required data, and a new fixture
capture is therefore needed.

18.3 The Beta 10 regression gate
(`tests/test_beta10_regression.py`) drops below 46/46.

18.4 A bundle inspection at Phase 2 reveals a production URL or
non-`/fixtures` fetch target.

18.5 A new external dependency is needed (npm package or
external service).

18.6 A Canon §10 anti-pattern (e.g. map-based VTS interaction
beyond read-only, Berth Timeline editing, scenario builders) is
introduced or required.

18.7 The Independence Reset framing (§13 / §14) is breached in
any artefact.

18.8 Railway behaviour deviates from the M1-known-good
configuration in a way that requires `railway.json` or root
`railway.toml` changes.

18.9 Any Stage E-prod / preview-audit-activation /
horizon-prod / Beta 10 state needs to change to satisfy M2.

18.10 A PR diff exceeds the tightly scoped frontend-only
boundary (e.g. drifts into `server.py`, audit modules, scrapers,
or fixtures).

18.11 ChatGPT review surfaces a material finding that requires
re-planning rather than mid-PR amendment.

When a stop condition fires, Claude posts a stop notice with the
specific condition number from this list and waits for explicit
Tony direction.

---

## 19. Phase 1 / Phase 2 sequencing

The two-phase deploy pattern from M0 and M1 is reused.

### 19.1 Phase 0 — Authorisation gate (pre-code)
Tony posts: "Authorised: begin Horizon V1 M2 implementation,
Phase 1 local only." Until this message arrives, no branch
exists, no code is written, no tests are written.

### 19.2 Phase 1 — Local implementation
- Claude creates `feat/v1-m2` off the latest `origin/main`.
- Implements adapters → components → tab switcher in that order.
- Writes Vitest tests per §4.4.
- Runs §15.1 local validation.
- Opens a single PR titled `feat(v1-m2): read-only tabs (Berth
  Timeline, Shift Log, VTS, Pilotage) + Performance placeholder`
  against `main`.
- Stops for ChatGPT review and Tony's "Phase 2 authorised"
  message. Phase 2 is **not** initiated by Claude unilaterally.

### 19.3 Phase 2 — Sandbox verification
- Tony switches `horizon-v1-sandbox` source branch from
  `feat/v1-m1` to `feat/v1-m2`.
- Claude waits for Tony's "Phase 2 verification authorised"
  message.
- Claude performs §15.2 sandbox verification (read-only only).
- Claude reports findings; stops for Tony's merge authorisation.

### 19.4 Phase 3 — Merge
- On Tony's "Authorised: merge PR #<n>" message, Claude performs
  pre-merge §15.3 checks, merges, performs post-merge §15.4
  checks, reports.

### 19.5 Phase 4 — Beta 10 visual check (Tony-side)
- Tony performs the Beta 10 visual check (same pattern as M1
  Retrospective §2).
- Result is reported back to Claude; Claude does not initiate
  this check.

### 19.6 Phase 5 — M2 retrospective
- After Tony confirms Phase 4 result, Claude is authorised
  (separately) to draft the M2 Retrospective document, mirroring
  the M1 Retrospective shape (§15.8 of M1 Retrospective).

---

## 20. Cross-document citation map

| This plan section | Cites | Source document |
|---|---|---|
| §1 Executive summary | M2 Scope Proposal §§1, 16; Independence Reset §11 | `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md`, `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` |
| §2 Resolved scope | M2 Scope Proposal §§3, 14, 15 | M2 Scope Proposal |
| §3 Branch name | M1 Implementation Plan §3 pattern | `HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` |
| §4 Scaffold | M1 Implementation Plan §4; Adapter Design Note §2.2 | M1 Plan, `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` |
| §5 Dependencies | M1 Implementation Plan §5 pattern | M1 Plan |
| §6 Fixtures | M1 Retrospective §§6, 7, 8; M2 Scope Proposal §6 | `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md`, M2 Scope Proposal |
| §7 Adapter derivation | Adapter Design Note §2.2 contract; M2 Scope Proposal §3.1 | Adapter Design Note, M2 Scope Proposal |
| §8 Tabs | Canon §1.1.3, §3, §10; M2 Scope Proposal §3.1 | `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md`, M2 Scope Proposal |
| §9 Components | M1 Implementation Plan §§7, 8 pattern | M1 Plan |
| §10 Read-only safeguards | Canon §10; M2 Scope Proposal §4 | Canon, M2 Scope Proposal |
| §11 Mocked/static | M2 Scope Proposal §6; M1 Retrospective §8 | M2 Scope Proposal, M1 Retrospective |
| §12 Deny-list | M2 Scope Proposal §4 | M2 Scope Proposal |
| §13 Beta 10 protection | M2 Scope Proposal §11; M1 Retrospective §§5, 9 | M2 Scope Proposal, M1 Retrospective |
| §14 Railway sandbox | M2 Scope Proposal §10; M1 Retrospective §9 | M2 Scope Proposal, M1 Retrospective |
| §15 Validation gates | M1 Implementation Plan §§14, 15; M2 Scope Proposal §15 | M1 Plan, M2 Scope Proposal |
| §16 Acceptance | M2 Scope Proposal §15 (A1–A13) | M2 Scope Proposal |
| §17 Rollback | M1 Implementation Plan §17 pattern | M1 Plan |
| §18 Stop conditions | M1 Implementation Plan §18 pattern; M2 Scope Proposal §4 | M1 Plan, M2 Scope Proposal |
| §19 Phase sequencing | M1 Implementation Plan §11; Execution Plan §11 | M1 Plan, `HORIZON_V1_EXECUTION_PLAN_v0.1.md` |
| §20 Citation map | (this section) | self |
| §21 Recommendations | M2 Scope Proposal §16 | M2 Scope Proposal |

---

## 21. Recommendations

21.1 **Hold M2 implementation until separate Tony
authorisation.** This plan does **not** authorise code.

21.2 **Honour the resolved decisions in §2.** Re-opening
resolved questions mid-implementation is itself a stop condition
(§18.11 in effect).

21.3 **Maintain the M1 governance pattern.** Explicit Tony
authorisation per phase; ChatGPT pre-merge review; strict
deny-list per authorisation; stop-for-review markers.

21.4 **Prefer adapter-level derivation.** New fixture capture is
a stop condition (§18.2) until separately authorised.

21.5 **Keep the diff small.** Single PR, tightly scoped to
frontend + adapters + tests.

21.6 **No new dependencies.** Charting / Gantt libraries are
explicitly excluded; CSS-grid for the Berth Timeline.

21.7 **Performance is a placeholder in M2.** Full content lives
in M2.5 / M3 by separate authorisation.

21.8 **Read-only by construction.** No operator action paths in
any form.

21.9 **Smart Ocean X framing is forbidden** in any new artefact
(Independence Reset §13 / §14).

21.10 **At M2 close, draft an M2 Retrospective** mirroring the
M1 Retrospective shape (M1 Retrospective §15.8).

21.11 **If any §18 stop condition fires, stop and report**, do
not work around it.

---

## End of M2 Implementation Plan

This plan is descriptive and prescriptive but **does not**
authorise M2 implementation.

Confirmed by this document:
- M2 **implementation is not authorised** by this document.
- Any M2 code work requires a separate explicit "Authorised:
  begin Horizon V1 M2 implementation" message from Tony after
  this plan is reviewed.
- M2 is planned to remain **fixture-backed and read-only**.
- M2 is planned to introduce **no live Beta 10 calls, no
  production `/api/*` calls, no auth, no RBAC, no ACK /
  COMMIT / DEFER / OVERRIDE / ESCALATE writes, no audit
  emission, no Stage E-prod activity, no database dependency,
  no `server.py` changes, no root `railway.toml` changes, and
  no production state changes**.
- The Smart Ocean X architectural dependency remains **formally
  closed** under the Independence / Architecture Reset
  (PR #51, `b0d9ff2`).
- The M2 Scope Proposal (PR #53, `8c73c55`) is the upstream
  authority for the resolved scope in §2.

**Next action:** ChatGPT engineering review of this plan, then
Tony's review. If approved and merged, Tony authorises a
separate **M2 implementation start** message as the next
governance step. M2 code does **not** begin until that
authorisation arrives.
