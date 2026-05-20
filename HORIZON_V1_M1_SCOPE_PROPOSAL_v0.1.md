# Horizon V1 — M1 Scope Proposal (v0.1)

**Status:** Scope proposal — planning only, no implementation
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review)
**Authoritative inputs (on `main @ 4a6b01f`):**
- `HORIZON_V1_M0_RETROSPECTIVE_v0.1.md` (PR #46) — directly informs §5, §13
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40) — adapter contract
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` (PR #39) — source-shape truth
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43) — UX rules
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md` (PR #44) — lifecycle treatment
- `HORIZON_V1_SANDBOX_PROVISIONING_PLAN_v0.1.md` (PR #42) — sandbox topology
- `HORIZON_V1_IMPLEMENTATION_STRATEGY_v0.1.md` (PR #34) — milestone sequencing
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30) — auth posture
**M0 close commit:** `a1161cf` (PR #45 merge)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise M1 implementation.** It proposes
M1 scope, boundaries, and acceptance criteria for review. An
implementation plan (analogous to M0 Implementation Plan v0.2)
must be drafted, reviewed, and authorised separately before any
M1 code is written.

---

## 1. Executive summary

M1 introduces the **adapter layer** and **polling-based
read-only refresh of captured operational data** to the V1
frontend, exercising the full Adapter Design Note contract
against representative `/api/summary` responses **without
touching Beta 10, without calling any live backend endpoint,
without enforcing auth, and without writing anything anywhere**.

**Explicitly:** M1 does **not** call live Beta 10 `/api/summary`.
M1 does **not** call any backend endpoint. M1 is **read-only
and fixture-backed** — it reads recorded `/api/summary`
snapshots committed to the repo. Live Beta 10 backend
integration is **M2+** scope, conditional on same-origin /
auth / RBAC questions being separately resolved.

M1 is a read-only milestone. Per Tony's three directional
decisions:

1. ACK as a write is deferred to M1.5 / M2. **M1 emits zero
   audit events and writes nothing to any backend.**
2. Auth and RBAC are **display-only / mock** in M1. The static
   `VTSO` pill from M0 stays; no real session cookie is
   acquired; no API endpoint is gated. Real same-origin auth,
   server-side RBAC, and audit attribution are deferred to M2+
   unless separately authorised.
3. M1 focuses on **read-only, fixture-backed operational data
   integration and limited UI surface expansion**, not
   operational authority.

The data path M1 implements is the **complete fetch wrapper →
polling hook → adapter → ViewSummary → components pipeline**,
end-to-end. The endpoint that pipeline targets in M1 is a **set
of recorded `/api/summary` fixtures served from the sandbox's
own static bundle** — same-origin to the sandbox URL, no auth
required, no cross-origin or CORS complications. This proves the
adapter contract against real-shaped data while keeping Beta 10
production entirely uninvolved.

The deliverables are:

- `frontend/src/api/horizon.js` (single public entry point)
- `frontend/src/api/adapters/*.js` (per Adapter Note §4)
- `frontend/src/hooks/useSummary.js` (polling hook)
- `frontend/fixtures/*.json` (recorded `/api/summary` responses
  — committed to repo)
- Real implementations of two or three operational surfaces
  (HorizonHeader counts, ConditionsBar tiles, partial Dashboard
  tab) consuming adapter output
- A small date library (`date-fns-tz` or `luxon`)
- Fixture-based adapter unit tests
- `frontend/railway.json` likely unchanged (M0 config still
  correct; sandbox still serves static `dist/`)

What M1 does **not** do, in any form: write to any backend,
enforce permissions client-side, emit audit events, ship a DSW
commit flow, scaffold a Replay surface, build mobile / executive
/ stakeholder consoles, modify `server.py`, activate Stage
E-prod, or touch the root `railway.toml`.

M1 closes when the adapter is implemented + tested + the sandbox
URL renders the partial Dashboard against fixture-fed captured
operational data with all the same Beta 10 protection guarantees
from M0.

---

## 2. M1 objective

**Prove the V1 data pipeline end-to-end against representative
captured data, without calling any live backend and without
touching Beta 10.**

That single objective subsumes:

- The adapter is exercised against fixture data shaped exactly
  like Beta 10's real `/api/summary` (per Shape Spike §3) and
  produces the `ViewSummary` shape the M0 components already
  consume (per Adapter Note §2.2 + §17.2)
- The fetch + polling layer demonstrates that "data refresh
  every N seconds" works end-to-end without leaking memory,
  flickering the UI, or producing race conditions on tab
  visibility / network reconnect
- A subset of operational surfaces (HorizonHeader stats,
  ConditionsBar tiles, Dashboard KPI block) consume adapter
  output rather than the M0 static `sample.js`
- The sandbox URL serves the M1 build with the same DEMO
  labelling, the same Beta 10 protection invariants, and the
  same Railway service-config pattern as M0
- The Adapter Design Note's normalisation rules (renames,
  defaults, derivations, time / unit / severity / status
  mappings, missing-domain handling, error handling) are
  validated by passing fixture-based unit tests

Once M1 closes, M1.5 / M2 can introduce the first **write** (ACK)
against a real authenticated endpoint with a well-tested adapter
already in place. M1 buys M2 confidence that the read path is
sound.

---

## 3. What M1 is

M1 is **a fixture-backed, read-only, adapter-driven integration
of the V1 React shell**. The end-to-end data pipeline (fetch
wrapper → polling hook → adapter → `ViewSummary` → components)
is exercised completely, but the endpoint it targets is a set
of captured `/api/summary` JSON fixtures committed to the repo
— **not** live Beta 10 production, **not** any backend
endpoint.

Concretely:

- **`frontend/src/api/horizon.js`** — single public entry point.
  Exports `fetchSummary()` (returns a parsed `ViewSummary`),
  `subscribe(handler)` (optional; alternative to polling hook),
  and the `useSummary()` React hook.
- **`frontend/src/api/adapters/summaryAdapter.js`** — top-level
  raw → `ViewSummary` transformation per Adapter Note §5.
- **`frontend/src/api/adapters/conditionsAdapter.js`** — weather
  + tides + ukc merge per Adapter Note §6.
- **`frontend/src/api/adapters/vesselAdapter.js`** — vessel
  normalisation with risk join per Adapter Note §7.
- **`frontend/src/api/adapters/conflictAdapter.js`** — conflict
  normalisation per Adapter Note §8 (including `cascade: null`
  fallback per §8.8).
- **`frontend/src/api/adapters/guidanceAdapter.js`** — guidance +
  alert-shaped conflicts per Adapter Note §9.
- **`frontend/src/api/adapters/dashboardAdapter.js`** — dashboard
  metrics per Adapter Note §10.
- **`frontend/src/api/adapters/etdRiskAdapter.js`** — etd-risk
  array per Adapter Note §11.
- **`frontend/src/api/adapters/time.js`** — ISO → JS Date +
  port-local HH:MM helpers per Adapter Note §13 (uses the new
  date library).
- **`frontend/src/api/adapters/status.js`** — severity / status
  / source mappings per Adapter Note §14.
- **`frontend/src/api/adapters/units.js`** — visibility nm ↔ km
  per Adapter Note §13.4.
- **`frontend/src/hooks/useSummary.js`** — polling hook with
  configurable interval (default 30s), tab-visibility pause,
  error retry with backoff, and last-known-good caching.
- **`frontend/fixtures/brisbane-busy.json`** — recorded
  `/api/summary` response (Brisbane, mid-shift with multiple
  conflicts) — primary M1 fixture.
- **`frontend/fixtures/brisbane-quiet.json`** — recorded
  `/api/summary` response (Brisbane, low activity, zero
  conflicts) — empty-state baseline.
- **`frontend/fixtures/melbourne-sim.json`** — recorded Melbourne
  response demonstrating different `sim_vessel_count`.
- **(Optional) `frontend/fixtures/null-fields.json`** — derived
  variant with nullable fields populated to exercise adapter
  defence per Adapter Note §15.2.
- **(Optional) `frontend/fixtures/malformed.json`** — derived
  variant with bad-type values to exercise §15.3 defence tests.
- **Adapter unit tests** — fixture-based snapshot tests per
  Adapter Note §16. Pure-function tests; no DOM, no browser.
- **A small date library** — `date-fns-tz` or `luxon` (Tony
  decision in §19 open questions). DST-aware. Adds one to two
  new transitive deps.
- **Real implementations of three M0 surfaces** consuming
  adapter output rather than `sample.js`:
  - `HorizonHeader` — stat tiles bound to adapter
    (`portStatus.vesselsInPort`, `dashboardMetrics.pilotOps12h
    + tugOps12h`, `portStatus.criticalConflicts`,
    `conditions.rating`)
  - `ConditionsBar` — 6 tiles bound to adapter conditions block
  - **Partial Dashboard tab** in `CenterPanel` — KPI tile group
    (Operations + Performance + Safety) + ETD risk table +
    berth utilisation summary (heatmap deferred to M2 if too
    heavy)
- **Sandbox redeploy** to `feat/v1-m1` via Tony-side dashboard
  branch switch.

That is M1 in full.

---

## 4. What M1 is not

M1 is **NOT**:

- A real `/api/summary` call against Beta 10 production
- An authenticated session against any endpoint
- A login form
- A role switcher with real RBAC
- A DSW (Decision Support Window)
- A What-If scenario builder
- A Port Brief downloader
- An ACK affordance that writes anywhere
- A COMMIT / DEFER / OVERRIDE / ESCALATE affordance
- A real audit log viewer (no `GET /api/audit` exists; nothing
  for V1 to consume in M1)
- A replay workspace
- A Berth Timeline tab
- A VTS Map tab
- A Pilotage tab
- A Performance tab
- A Shift Log tab
- A vessel roster with interactivity
- A port switcher
- A mobile surface
- A stakeholder surface
- An executive surface
- A `server.py` modification of any kind
- A root `railway.toml` modification
- A new audit event type definition
- An activation of `AUDIT_EMISSION_ENABLED`
- A change to production `DATABASE_URL`
- A Stage E-prod step
- A Beta 10 deploy modification
- A `horizon-prod` interaction
- A custom domain on the sandbox
- A change to any Phase 0 protected file

Anything resembling these in an M1 PR is **scope creep** and
must be rejected.

---

## 5. Decisions inherited from M0 / M0.5

Per the M0 Retrospective + Tony's directional decisions:

| # | Decision | Source | Applied in M1 |
|---|---|---|---|
| 1 | ACK as write deferred to M1.5 / M2 | Tony directional + Lifecycle Reconciliation §12.1 | M1 emits **zero** audit events |
| 2 | Auth display-only / mock; real auth deferred to M2+ | Tony directional + M0 Retrospective §9.5 Option A or C | M1 keeps static `VTSO` pill; no login form; no session cookie acquisition |
| 3 | M1 focus = read-only data + UI expansion | Tony directional | All M1 deliverables are read-only |
| 4 | Sandbox stays on its own Railway project | Sandbox Provisioning Plan §3 | `horizon-v1-sandbox` continues to host M1; `horizon-prod` untouched |
| 5 | `frontend/railway.json` is the sandbox config source | M0 Retrospective §5.2 | M1 likely does not edit it; the M0 build / start commands still apply |
| 6 | Root `railway.toml` remains Beta 10 only | M0 Retrospective §5.4 + §11.5 | M1 does not touch the root config |
| 7 | Two-phase deploy pattern (local → sandbox) | M0 Retrospective §3.3 | M1 PR opens after local validation; sandbox deploy verification is Phase 2 |
| 8 | `feat/v1-m1` is a new branch off `main` (Option B from M0 Retrospective §9.2) | M0 Retrospective recommendation | New branch; one Tony-side dashboard source switch |
| 9 | Stage E-prod paused | Implementation Strategy §9.4 | Unchanged in M1 |
| 10 | DesignVerificationSwatch removed (or relegated to a `/dev` route) | Canon §10.3 + M0 Retrospective §7.3 | M1 removes the swatch from default operator view; the real lifecycle pills/glyphs are now exercised through real conflict data from fixtures |
| 11 | Workspace-scoped OAuth — discipline-only safeguard | M0 Retrospective §11.2 | Project-scoped `RAILWAY_TOKEN` may be created in M1 prep if Tony chooses |
| 12 | Two-Railway-projects model | M0 Retrospective §5.1 | Unchanged |

---

## 6. M1 data integration model

### 6.1 Endpoint targets

M1 implements the **full data pipeline** (fetch wrapper → polling
hook → adapter → ViewSummary → components) but targets a
**fixture endpoint inside the sandbox's own static deploy
artefact**, not Beta 10's production `/api/summary`.

Specifically:

- M1 fetches from **`/fixtures/brisbane-busy.json`** (and other
  fixtures), served by the same Railway static server (`serve`)
  that hosts the React bundle
- The fixtures are **recorded snapshots** of Beta 10's actual
  `/api/summary` response — same shape as the Shape Spike §3
  documents, just frozen-in-time JSON files
- Fixtures live at `frontend/fixtures/*.json` and ship in the
  Vite `dist/` artefact via a small build step (or directly in
  `frontend/public/`, depending on Vite convention — implementer
  detail)

### 6.2 Why fixture-fed instead of Beta 10 live

Three reasons:

1. **No auth requirement.** Beta 10's `/api/summary` is gated by
   `horizon_session` cookie. The sandbox URL is on a different
   subdomain than Beta 10's production URL, so cookies don't
   cross. Solving the cross-origin auth problem requires either
   CORS changes to `server.py` (M0 Retrospective §9.5 Option B —
   not authorised) or waiting for `/v1/*` to mount on the Beta
   10 domain (M0 Retrospective §9.5 Option A — M2+). Fixture-
   fed sidesteps the problem entirely.
2. **No Beta 10 risk.** A bug in the M1 fetch wrapper / polling
   hook cannot accidentally hammer production. The fixture
   endpoint is static.
3. **Deterministic verification.** Adapter tests can reproduce
   exact behaviour against the same fixtures. CI / review
   reproducibility is high.

The trade-off: M1 does not technically prove the adapter against
Beta 10's actual live response. But the fixtures **are** real
recorded `/api/summary` responses from Beta 10 — the same data,
just static. The adapter exercise is the same.

### 6.3 Fixture capture

The fixtures are captured **once** by Tony (or by a small
authorised manual step) during M1 prep:

- SSH or direct authenticated browser session against Beta 10
  production (or a local Beta 10 dev environment)
- `curl -b "horizon_session=<token>" .../api/summary > frontend/fixtures/brisbane-busy.json` for each port + scenario
- Three recorded fixtures minimum: `brisbane-busy.json`,
  `brisbane-quiet.json`, `melbourne-sim.json`
- Optional derived fixtures: `null-fields.json` (manually edit
  to set nullable fields to null), `malformed.json` (manually
  edit to inject wrong-type values for defence testing)
- Fixtures are scrubbed of any sensitive data before commit
  (operator names, real customer agent names, etc. — though
  Beta 10 already uses simulated names by default per
  `port_profiles.py`)
- Fixtures are committed to the repo at `frontend/fixtures/` as
  ordinary content

Fixture capture is a **Tony-side prep step**, not part of M1
implementation. The M1 PR consumes already-captured fixtures.

### 6.4 What the pipeline proves end-to-end

Even with fixture data, M1 exercises:

- `fetchSummary()` makes a real network request (to the static
  fixture URL on the sandbox)
- The fetch wrapper handles HTTP errors (test by deleting a
  fixture and observing graceful empty-state)
- The fetch wrapper handles network errors (test by simulated
  network throttling)
- The polling hook handles tab-visibility pause / resume
- The polling hook handles polling cadence (default 30s)
- The polling hook avoids race conditions (fetch in-flight when
  next interval fires)
- The adapter normalises shape correctly
- The adapter handles `cascade: null`, `delta: null`, missing
  domains per Adapter Note §12
- The adapter passes fixture-based unit tests
- The components consume `ViewSummary` correctly
- The UI re-renders cleanly on poll
- "Last successful poll" timestamp displays in the header
- "Connection lost" banner appears on simulated fetch failure

That is a complete data pipeline test, minus only the actual
Beta 10 endpoint URL — which the M1 fetch wrapper is
architected to swap in at M2+ via a single configuration
variable.

### 6.5 Configuration for the swap

The fetch wrapper accepts a base URL configurable via build-time
env var `VITE_API_BASE` (Vite's standard pattern). In M1 this
defaults to `/fixtures` (same-origin to the sandbox). In M2+
this can be flipped to `/api` (when `/v1/*` mounts on the Beta
10 domain) or to a cross-origin URL once CORS is added — without
adapter code change. The pipeline is architected for the swap;
M1 just doesn't make it.

---

## 7. Adapter scope

Per Adapter Design Note §5–§14, M1 implements:

| Section | Adapter responsibility | M1 status |
|---|---|---|
| §5 | Top-level mapping | Full — all top-level renames |
| §6 | Conditions merge (`weather` + `tides` + `ukc` → `conditions`) | Full |
| §7 | Vessel normalisation + risk join | Full |
| §8 | Conflict normalisation including `cascade: null` and option subset | Full |
| §9 | Guidance + alert-shaped conflicts ordering | Full |
| §10 | Dashboard metrics | Full |
| §11 | ETD risk array | Full |
| §12 | Missing Domain Strategy (`shiftLog`, `auditLog`, `cascade` null + `missingDomains` array) | Full |
| §13 | Time + unit normalisation | Full (requires date library — see §19) |
| §14 | Severity / status / source mappings | Full |
| §15 | Error handling (missing arrays, null fields, unexpected types, partial response, safe empty states) | Full |
| §16 | Testing strategy (fixture-based snapshot tests) | Full per §16.3 (snapshot + assertions + empty-state + defence + round-trip) |

The complete adapter module set per §4 is in scope. No
shortcuts. Per Adapter Note §3.3, **all field renames live in the
adapter**; components import only the public `horizon.js` entry.

---

## 8. Polling / read-only refresh model

### 8.1 Polling hook contract

```
const { data, isLoading, error, lastUpdated, isStale, refetch } =
  useSummary({ pollInterval: 30000 });
```

Where:

- `data` — current `ViewSummary` (or `null` before first
  successful fetch)
- `isLoading` — boolean for initial fetch
- `error` — last fetch error (or `null`)
- `lastUpdated` — JS Date of last successful fetch
- `isStale` — true if `now - lastUpdated > 2 * pollInterval`
- `refetch` — function to force an immediate poll

### 8.2 Polling behaviour

- Initial fetch on mount
- Subsequent fetches every `pollInterval` ms (default 30000)
- Pause polling when `document.hidden` is true; resume when
  visible
- Retry with exponential backoff on fetch failure (3 attempts:
  1s, 3s, 9s); on giving up, set `error` and continue normal
  polling cadence
- Last-known-good data remains in `data` during retry and error
  states (component continues to render the last successful
  response with a "stale" indicator)
- Cleanup: `clearInterval` on unmount

### 8.3 Read-only — strictly

The polling hook makes **only** `GET` requests. No `POST` / `PUT`
/ `PATCH` / `DELETE` anywhere in the M1 codebase. A grep audit
in M1 acceptance: `grep -rn "POST\|PUT\|PATCH\|DELETE\|method:" frontend/src/api/`
returns only the `method: 'GET'` line in the fetch wrapper.

### 8.4 No auth header

The fetch wrapper does **not** attach an `Authorization` header,
does **not** set `credentials: 'include'`, does **not** read or
write any cookie. Same-origin static fixture endpoint requires
no auth.

When M2+ swaps to a real authenticated endpoint, `credentials:
'include'` will be added then — a one-line change. M1 does not
ship that line.

### 8.5 No mutations

No M1 surface offers an affordance that mutates state. ACK
buttons, COMMIT buttons, DEFER inputs, OVERRIDE selectors,
ESCALATE routes — none exist in M1. The decision-card surface
stays hidden (its placeholder remains from M0).

---

## 9. UI surfaces in scope

Three M0 surfaces become real (adapter-backed); two new partial
surfaces are introduced.

### 9.1 HorizonHeader — adapter-backed

Same component as M0, now consumes adapter output:

- Stat tile 1: `summary.portStatus.vesselsInPort`
- Stat tile 2: `summary.dashboardMetrics.pilotOps12h + tugOps12h`
  (labelled "Movements 12h" for accuracy)
- Stat tile 3: `summary.portStatus.criticalConflicts` (with pulse
  Dot if > 0)
- Stat tile 4: `summary.conditions.rating`
- DEMO · SIMULATION pill remains (still demo data — fixtures
  are recorded snapshots, not live)
- VTSO role pill remains (static; no role switcher)
- Live clock remains (real `Date.now()`)
- AMS Group co-brand remains

### 9.2 ConditionsBar — adapter-backed

Same component, now consumes `summary.conditions`:

- Rating pill with severity tone
- Wind / Swell / Visibility / Pressure / Tide / UKC tiles bound
  to corresponding `conditions` fields
- `(demo)` markers stay on each tile (still recorded data)
- New tile: "Last update" showing `summary.timestamp`
  port-local HH:MM

### 9.3 Dashboard tab — partial

Inside `CenterPanel`, replace the M0 placeholder with a
partial Dashboard tab. Per Canon §4.5 Dashboard composition:

**In M1:**
- KPI tile group (three rows: Operations, Performance, Safety)
  bound to `summary.dashboardMetrics`
- ETD risk table — top 5 entries from `summary.etdRisk` sorted
  by `riskScore` descending; each row shows `vesselName`,
  `riskLevel` pill, primary `riskFactor`, `riskScore` as a
  small bar
- Berth utilisation summary — single line "Currently 88%
  occupied (7/8); next 48h forecast 74%" derived from
  `summary.dashboardMetrics.berthUtilisationPct` and
  `forecastUtilisation48h`. Heatmap (per Canon §4.5 description)
  deferred to M2 if it requires more component primitives than
  M1 supports

**Not in M1:**
- The full berth heatmap visualisation
- Tab navigation (the centre panel has one "Dashboard" view
  only in M1; the other 5 tabs from Canon §4.5 remain
  unscoped)

### 9.4 LeftPanel — placeholder remains; DesignVerificationSwatch removed

Per the inherited decision (§5 item 10), the
`DesignVerificationSwatch` is **removed** from the operator
view in M1. The lifecycle pill / glyph / colour treatments are
now exercised against **captured conflict fixtures** drawn
from recorded `/api/summary` snapshots (conflicts of various
severities will render with their pills and glyphs in the M1
alert list if/when M1 ships the alert list; otherwise via the
Dashboard's ETD risk table for risk levels). These are
captured snapshots, not live conflicts from a running
backend.

The LeftPanel itself remains a placeholder in M1 because the
alert list + Active Decision card surfaces are scheduled for a
later milestone (M2+ per Canon §4.4 and Execution Plan §11).

### 9.5 RightPanel — placeholder remains

No `vesselRoster` interactivity, no audit log (no backend
source exists). Placeholder stays from M0.

### 9.6 Persistent DEMO banner — remains

The top-edge banner "Horizon V1 — sandbox · demo data · not for
operational use" stays. Even with adapter-fed real-shaped data,
the sandbox is still serving recorded fixtures, not live data —
the banner remains accurate and required.

---

## 10. UI surfaces out of scope

Explicitly **not** in M1 (deferred to M2+ unless otherwise noted):

| Surface | Deferred to |
|---|---|
| Tab navigation across all 6 tabs | M2 |
| Berth Timeline tab | M2 |
| VTS Map tab | M2 or M3 |
| Pilotage tab | M2 |
| Performance tab | M2 |
| Shift Log tab | V1.x (no backend source today) |
| Active Decision card in LeftPanel | M2 |
| Alerts list in LeftPanel | M2 |
| Vessel roster (read-only) in RightPanel | M2 |
| Audit log right-panel tab | V1.x (no `GET /api/audit` endpoint) |
| DSW (Decision Support Window) | M3 |
| What-If scenario builder | M5 |
| Port switcher | M4 |
| Login flow / real auth | M2+ (per Tony directional decision) |
| Role switcher with real RBAC | V1.1+ |
| Mobile surface | V1.5 |
| Stakeholder surface | V1.5 |
| Port Executive console | V1.x |
| Replay workspace | V1.6 |
| Decision-commit endpoint | M7 |
| Audit emission writes | M7 / V1.x (Stage E-prod) |
| Cascade synthesis | not in V1 per Adapter Note §8.8 |
| Berth heatmap (full visualisation) | M2 (optional in M1 if trivial; otherwise M2) |

---

## 11. Auth / RBAC treatment in M1

### 11.1 No real auth

Per Tony directional decision and M0 Retrospective §9.5 Option C
(mock-auth):

- No login form ships in M1
- No session cookie is acquired
- No `Authorization` header is added
- No `credentials: 'include'` on fetch
- The fixture endpoint requires no auth (it's a static file on
  the same origin as the React bundle)

### 11.2 Display-only role

The HorizonHeader continues to render the static `VTSO` pill
from M0. **No role switcher** in M1. If the user clicks the
pill, nothing happens.

### 11.3 No client-side RBAC

The M1 codebase contains zero `if (role === 'HM')` style gates.
The Permission Model §11 anti-pattern (frontend-only access
control) is preserved: components render whatever data the
adapter outputs; nothing is hidden or gated client-side based on
role.

### 11.4 No client-side identity assertion

The M1 frontend does not claim a user identity. No "currently
logged in as Halvorsen" badge. No identity object in app state.
The static VTSO label is a hard-coded UI affordance only.

### 11.5 What this preserves for M2+

When M2+ introduces real auth (whichever option from M0
Retrospective §9.5 is later authorised):

- The fetch wrapper gains `credentials: 'include'` — one-line
  change
- The polling hook handles 401 responses with redirect to login
  — small addition
- The HorizonHeader's static `VTSO` pill becomes role-bound to
  the session — small addition
- Server-side RBAC enforcement remains a **backend** concern
  (V1.1+ per Implementation Strategy §17), not a frontend
  concern at any point

---

## 12. Audit / lifecycle treatment in M1

### 12.1 Zero audit events

M1 emits **zero** audit events. The audit module is not
imported into any frontend code. The Phase 0 backend audit
emission paths are not exercised by M1 because M1 does not
call Beta 10's backend.

### 12.2 No new audit event types

No `OPERATOR_ACKNOWLEDGED`, no `OPERATOR_ACTED`, no
`RECOMMENDATION_CLOSED`, no `AUDIT_READ`, no
`INACTION_DESIGNATED` — none defined, none emitted in M1.

These are sequenced per Lifecycle Reconciliation §11 to M1.5 /
M2 / V1.x / V1.6.

### 12.3 Lifecycle states render from fixtures

The real `ViewSummary` shape includes a `conflicts` array.
Recorded fixtures contain conflicts at various lifecycle states
(whatever Beta 10 was emitting at capture time — typically
RECOMMENDED). M1 components render those conflicts with the
canonical pill / glyph / colour treatments per Canon §4.

If the recorded fixtures don't cover all 7 lifecycle states
(likely — production may only emit RECOMMENDED in normal
operation), the **DesignVerificationSwatch from M0 is removed**
without replacement. The canon treatments are exercised
naturally against whatever the fixtures contain. If broader
state coverage is needed for review, derived fixtures (one per
state) can be added per the M0 sample.js pattern.

### 12.4 No false-state UI

No "Decision logged", no "Audit trail preserved", no "Resolution
applied to live schedule" text anywhere in M1. The Canon §10.3
anti-pattern remains forbidden.

### 12.5 No real decision-commit affordance

Per §9.4 and §11, no ACK / COMMIT / DEFER / OVERRIDE / ESCALATE
buttons. The Active Decision card placeholder stays from M0.

### 12.6 Audit log right tab — still placeholder

No `GET /api/audit` endpoint exists. The right-panel Audit Log
tab placeholder from M0 remains unchanged in M1. Closing it
requires V1.x backend work.

---

## 13. Railway / sandbox deployment model

### 13.1 Sandbox project unchanged

`horizon-v1-sandbox` remains the M1 deployment target. Same
project, same service, same domain, same Config Path setting
(`/frontend/railway.json`).

### 13.2 Source branch switch

The single Tony-side dashboard action for M1:

- In Railway dashboard, on the `horizon-v1-sandbox` service →
  Settings → Source → **Branch** change from `feat/v1-m0` to
  **`feat/v1-m1`**

This is the only Railway-side change M1 requires. Per the M0
governance pattern, Claude does not perform this change; Tony
does.

### 13.3 `frontend/railway.json` likely unchanged

The M0 build / start commands continue to apply:

- Build: `npm install && npm run build`
- Start: `npx serve -s dist -l $PORT`
- Restart: ON_FAILURE, max 10

M1 does not need different commands. The same `frontend/dist/`
artefact + same `serve` static-file server pattern works for
M1's fixture-fed adapter.

If a static-asset routing rule is needed for the
`/fixtures/*.json` path (so `serve` returns the right
Content-Type), the implementer adds an `--cors` or
`-s` flag combination in `frontend/package.json` `start` script,
not in `railway.json`. Implementer detail.

### 13.4 No changes to `horizon-prod` Railway project

Zero. Sandbox provisioning plan §3 separation rule maintained.

### 13.5 No new Railway projects

`horizon-v1-preview` / `horizon-v1-staging` / `horizon-v1-prod`
(per Execution Plan §8) remain deferred to M2+. M1 deploys to
the same sandbox.

### 13.6 No custom domain

Same Railway-issued `*.up.railway.app` URL as M0. No custom
domain on the sandbox.

### 13.7 Auto-deploy from `feat/v1-m1`

After the branch switch, Railway will auto-deploy from
`feat/v1-m1` on every push (or only the first push if Tony
explicitly disables auto-deploy and then triggers manually).
Default expectation: auto-deploy is on.

### 13.8 No `serve` removal in M1

Per M0 Retrospective §11.7, `serve` may be removable in M1 if
Railway's static-site service supports `dist/` natively. This
is an **optional** simplification. If pursued in M1, it's a
small `package.json` + `frontend/railway.json` edit. If not
pursued, `serve` continues to do its job. The default M1 plan
assumes `serve` stays.

---

## 14. Branch and PR sequencing

### 14.1 Branch: `feat/v1-m1`

Created from `main @ 4a6b01f` (the post-M0.5 retrospective HEAD)
by Claude during M1 implementation start.

Lifecycle:

- Long-lived through M1 build
- Receives all M1 commits
- Sandbox auto-deploys from this branch (post-Tony source
  switch)
- Merges to `main` only after M1 acceptance + Tony merge
  authorisation
- Preserved post-merge (not `--delete-branch`) if the sandbox
  continues to deploy from it for downstream verification
  before M2 begins; deleted whenever M2's source switch happens

### 14.2 Phased reporting

Same as M0:

**Phase 1 — Local implementation complete:**
- Adapter modules + tests + polling hook + Dashboard partial
  implemented
- `cd frontend && npm install` (with new date library) +
  `npm run build` produces clean `dist/`
- Adapter unit tests pass against all fixtures
- All validation gates from §15 green
- M1 PR opened against `main` (no merge yet)
- Phase 1 implementation report issued

**Phase 2 — Sandbox deploy verification:**
- Tony switches sandbox source branch to `feat/v1-m1`
- Railway auto-deploys; Claude runs read-only verification
- Sandbox URL renders the partial Dashboard + adapter-fed
  HorizonHeader + ConditionsBar
- Periodic fixture polling visible in browser devtools (network
  tab shows periodic GETs to `/fixtures/*.json` only — no
  `/api/*` calls, no calls to any live backend)
- Adapter handles `cascade: null` / `delta: null` / missing
  domains correctly (test by serving a derived `null-fields`
  fixture)
- Phase 2 verification report issued
- Tony merge authorisation

### 14.3 PR opened against `main`

M1 PR opens against `main`. Title pattern: `feat(v1-m1): M1
Phase 1 — adapter, polling, partial Dashboard (read-only)`.

### 14.4 Fixture prep precedes M1 implementation

Per §6.3, Tony captures 3+ recorded fixtures before M1
implementation starts. Fixtures are committed to the repo at
`frontend/fixtures/` in a separate prep PR (very small) OR as
the first M1 commit. Either is acceptable; the prep PR pattern
is cleaner because it lets Claude implement adapter + tests
against an already-stable fixture set.

### 14.5 Date library prep

Tony decision in §19 — which date library. Then Claude adds
`date-fns-tz` (or `luxon`) to `frontend/package.json` and runs
`npm install` as part of the M1 implementation start.

### 14.6 No new branches beyond `feat/v1-m1` + scope-proposal branch

This proposal lives on `docs/v1-m1-scope-proposal`. M1
implementation lives on `feat/v1-m1`. No other branches.

---

## 15. Validation gates

Pre-PR (Phase 1) gates:

| # | Gate | How verified |
|---|---|---|
| 1 | `tests/test_beta10_regression.py` 46/46 | `python3.10 -m pytest tests/test_beta10_regression.py -q` on every commit |
| 2 | Protected-file diff outside `frontend/` is empty | `git diff --name-only main..feat/v1-m1` excluding `frontend/*` returns empty |
| 3 | `cd frontend && npm install` succeeds | New date library installs cleanly; lockfile updated |
| 4 | `cd frontend && npm run build` succeeds | Vite produces `dist/` with HTML + JS + CSS |
| 5 | `frontend/dist/` not committed | `.gitignore` enforces; `git ls-files | grep dist` empty |
| 6 | `frontend/node_modules/` not committed | Same |
| 7 | Adapter unit tests pass | `npm test` (or whatever test runner is added — Vitest most likely) returns success on all fixtures |
| 8 | No `POST`/`PUT`/`PATCH`/`DELETE` HTTP method in any source file | `grep -rn "method:.*['\"](POST\|PUT\|PATCH\|DELETE)" frontend/src/` empty |
| 9 | No `credentials: 'include'` anywhere | `grep -rn "credentials.*include" frontend/src/` empty |
| 10 | No new audit event types defined | `grep -rn "OPERATOR_ACKNOWLEDGED\|OPERATOR_ACTED\|AUDIT_READ" frontend/` empty (or only in string-literal documentation comments) |
| 11 | No "Audit trail preserved" / "Decision logged" / "Resolution applied" text | `grep -rn "Audit trail preserved\|Decision logged\|Resolution applied" frontend/src/` empty |
| 12 | No real login form | `grep -rn "<form.*login\|action.*login" frontend/src/` empty |
| 13 | No role switcher | `grep -rn "setRole\|roleOptions" frontend/src/` empty |
| 14 | DEMO labels all present (per M0 §9 list, possibly minus the removed DesignVerificationSwatch row) | Manual grep |
| 15 | Visual side-by-side against M0 build | Local browser open of `frontend/dist/index.html` |
| 16 | Browser console clean | DevTools — no errors, no warnings |
| 17 | Network tab: only `/fixtures/*.json` calls on the local dev build; only same-origin static requests on the sandbox | DevTools |
| 18 | PRs #27, #28, #29 untouched | timestamp check |

If any gate fails, M1 PR is not opened until resolved.

### 15.1 Adapter test coverage targets

Per Adapter Note §16, the adapter test suite covers:

1. Snapshot test per fixture (3+ fixtures, 3+ snapshots)
2. Per-domain assertions (vessel renames, severity mapping,
   conditions merge, cascade absence, risk join)
3. Empty-state test (empty `{}` input returns full `ViewSummary`
   with empty defaults)
4. Defence test (malformed fixture doesn't throw; logs warning;
   substitutes defaults)
5. Round-trip test (View → JSON.stringify → JSON.parse produces
   equivalent view)

Acceptance: all test categories present, all passing.

---

## 16. Acceptance criteria

M1 closes when **all** of the following are true:

1. M1 PR opened, reviewed by ChatGPT, authorised by Tony,
   merged
2. `cd frontend && npm run build` produces a clean `dist/`
   artefact locally
3. Adapter unit tests pass on the developer machine
4. Sandbox URL renders the **partial Dashboard** plus the
   adapter-fed HorizonHeader + ConditionsBar, all wired through
   the adapter (i.e. no `sample.js` consumed at runtime — only
   fixtures via the fetch wrapper)
5. Browser devtools network tab on the sandbox URL shows
   periodic `GET /fixtures/brisbane-busy.json` (or equivalent)
   every ~30 seconds, no other API calls
6. Adapter handles `cascade: null`, `delta: null`, missing
   domains correctly when a derived fixture exercises them
7. Regression gate `tests/test_beta10_regression.py` 46/46
8. `server.py`, root `index.html`, audit helpers, Phase 0
   migrations, deploy manifests, `requirements.txt`, root
   `railway.toml`, `port_profiles.py`, `tests/` untouched
9. PRs #27, #28, #29 still untouched
10. Beta 10 demo flow verified working post-merge (Tony
    visual check, same as M0)
11. No new audit event type, no audit emission call, no Stage
    E-prod activation
12. Sandbox URL not linked from `horizon-prod`
13. M0 ViewSummary contract still respected — components
    consume the same `ViewSummary` shape M0 sample.js had,
    just sourced from the adapter

### 16.1 Phase split

| # | Item | Phase |
|---|---|---|
| 1 | M1 PR opened, reviewed, authorised, merged | spans both phases |
| 2 | Local `dist/` builds | Phase 1 |
| 3 | Adapter unit tests pass | Phase 1 |
| 4 | Sandbox renders adapter-fed UI | Phase 2 |
| 5 | Polling visible in devtools | Phase 2 |
| 6 | Adapter handles edge fixtures | Phase 1 (unit tests) + Phase 2 (visual on sandbox) |
| 7 | Regression gate 46/46 | Phase 1 |
| 8 | Protected files untouched | Phase 1 |
| 9 | PRs #27, #28, #29 untouched | Phase 1 + Phase 2 |
| 10 | Beta 10 demo post-merge | Phase 2 (Tony-side) |
| 11 | No audit / Stage E-prod activation | both |
| 12 | Sandbox URL not linked from prod | both |
| 13 | ViewSummary contract preserved | Phase 1 |

If Railway is unstable during Phase 2, Sandbox Provisioning
Plan §18.1 fallback applies — M1 may close on Phase 1 only,
with items 4, 5, 6 (sandbox-side), 10 deferred to a small
follow-up.

---

## 17. Rollback plan

Per the M0 pattern:

```
git revert <M1 merge commit>
# Optional cleanup:
rm -rf frontend/src/api/ frontend/src/hooks/useSummary.js frontend/fixtures/
```

That is the full rollback. No state migrations, no env-var
flips, no audit chain reconciliation, no Beta 10 impact.

Sandbox redeploys to its pre-M1 state (M0 build at commit
`1f5447dc` if Tony reverts the source-branch switch as well; or
the next deploy from `feat/v1-m1` after the revert commit if
not).

`horizon-prod` is unaffected — Beta 10 deploy artefact remains
byte-identical because M1 did not touch any Beta 10 file.

`feat/v1-m0` branch remains preserved on remote (the sandbox
fallback target).

---

## 18. Stop conditions

Implementation **stops immediately** if any of the following
occurs:

| Condition | Action |
|---|---|
| Any Beta 10 protected file appears in the diff | Stop, report, do not commit |
| `tests/test_beta10_regression.py` fails | Stop, investigate |
| Scope creep — any component / feature outside §9 list arrives in the diff | Stop, remove the scope-creep diff, report |
| New audit event type defined anywhere | Stop, report — audit type additions need backend schema review + separate Tony authorisation |
| Any `POST` / `PUT` / `PATCH` / `DELETE` in any frontend source file | Stop, report — M1 is strictly read-only |
| `credentials: 'include'` introduced | Stop — that's M2+ scope |
| Login form scaffolded | Stop — that's M2+ |
| Any change to `server.py` | Stop — separate authorisation required |
| Any change to root `railway.toml` | Stop — Beta 10 protection |
| Production env var detected on sandbox during read-only verification | Stop, report |
| DB / add-on attached to sandbox | Stop, report |
| Custom production Horizon domain attached to sandbox | Stop, report |
| Sandbox deployment fails repeatedly with build errors | Stop, report; consider local-build-only fallback per Sandbox Provisioning Plan §18.1 |
| Any sign of `horizon-prod` modification | Stop immediately |
| Any sign of Stage E-prod activation | Stop immediately |
| `AUDIT_EMISSION_ENABLED` change | Stop immediately |
| Preview Postgres incident bleeds into M1 path | Stop, report — M1 has zero dependency on preview Postgres |
| ChatGPT review identifies a substantive concern | Stop merge; revise per review |
| Tony issues "pause M1" instruction | Stop, await further instruction |

---

## 19. Open questions

### 19.1 Date library — `date-fns-tz` or `luxon`?

Both are DST-aware. Both have similar API surfaces. Trade-offs:

- **`date-fns-tz`** — smaller bundle (~30 KB minified); tree-
  shakable; functional API; pairs with `date-fns` which adds
  another ~20 KB if used
- **`luxon`** — slightly larger (~70 KB minified); object-
  oriented API; single import for everything; well-known
  predecessor of date-fns-tz

**Proposed default:** `date-fns-tz`. Smaller bundle aligns with
M0's 159 KB JS target; tree-shaking keeps growth minimal.

**Decision needed:** before M1 implementation starts. Cheap to
reverse later (swap one import path).

### 19.2 Vitest, Jest, or no test framework?

The adapter tests need a test runner. Vitest is the natural
choice given Vite already in use; Jest is the legacy industry
default; "no test framework" (use plain assertions in script
files) is a minimal-dep extreme.

**Proposed default:** Vitest. Native Vite integration; minimal
config; jsdom not needed (adapter tests are pure-function);
adds 2-3 dev dependencies.

**Decision needed:** before M1 starts. Affects `package.json`
devDependencies.

### 19.3 Fixture capture method

§6.3 sketched: Tony captures fixtures from Beta 10 production
via authenticated `curl`. Alternatives:

- A. **Manual `curl`** as described
- B. **A small node script committed to `frontend/scripts/`** that
  runs against a local Beta 10 dev environment (server.py
  spinning up locally with simulated data)
- C. **Run the Beta 10 backend in a one-off Docker container**
  locally, capture, commit

**Proposed default:** A. Simplest, no new tooling.

**Decision needed:** Tony's choice when M1 starts. Doesn't
affect the M1 plan structure.

### 19.4 Number of fixtures

Minimum 3 (`brisbane-busy`, `brisbane-quiet`, `melbourne-sim`).
Add `darwin-mixed` for fourth-port coverage? Add
`null-fields` + `malformed` for defence tests?

**Proposed default:** 3 captured + 2 derived (`null-fields`,
`malformed`). 5 total. Captures all major adapter behaviours.

**Decision needed:** before M1 starts.

### 19.5 Polling interval

30 seconds proposed (default). Beta 10 production polling cadence
should be confirmed against existing dashboard polling — the
Beta 10 `index.html` does `setTimeout`-style polling at some
interval; matching it preserves familiar refresh feel.

**Proposed default:** 30 seconds.

**Decision needed:** can be tuned in M1 implementation; not a
blocking decision.

### 19.6 Berth utilisation summary or full heatmap?

Per §9.3, the partial Dashboard ships a summary line by default
("Currently 88% occupied"). The full heatmap (Canon §4.5
description) requires more component primitives — probably 18 ×
8 cells, colour-coded by occupancy state per hour. If the
heatmap is straightforward against existing primitives, ship it
in M1; if it needs new component work, defer to M2.

**Proposed default:** ship the summary line in M1; defer the
full heatmap to M2 unless it slips in trivially.

**Decision needed:** implementer judgement during M1 build.

### 19.7 Optional `serve` removal

Per M0 Retrospective §11.7, if Railway's static-site offering
supports `dist/` natively, `serve` can be removed. Optional
M1 cleanup.

**Proposed default:** leave `serve` in place. The optimisation
is cosmetic; the M0 setup works.

**Decision needed:** can be deferred to M1.5.

### 19.8 Project-scoped `RAILWAY_TOKEN`?

Per M0 Retrospective §11.2, a sandbox-scoped token would tighten
the credential surface. Optional pre-M1 governance hygiene.

**Proposed default:** Tony decides. Either is acceptable for
M1. If the workspace-scoped OAuth from M0 is convenient and
Tony wants to defer the scoped-token creation, that's fine.

**Decision needed:** Tony-side; not blocking M1.

---

## 20. Recommendations

### 20.1 Adopt the M0 pattern wholesale for M1

The Scope Proposal → Implementation Plan v0.2 → Phase 1 → Phase
2 → Retrospective cycle worked for M0. M1 should follow the
same cycle. This is a known-good template.

### 20.2 Keep the adapter PR small and focused

M1's PR will likely be larger than M0's (adapter + tests +
polling + Dashboard partial). The temptation to also add more
Dashboard surface, or to scaffold the alert list, or to add the
Active Decision card while in there must be resisted. Defer to
M2.

### 20.3 Adapter tests are the M1 quality signal

The fixture-based adapter tests are the artefact that proves the
Adapter Design Note contract. They should be **comprehensive**
(per §16.3 — snapshot + assertions + empty + defence + round-
trip) and run on every push. Cheaper to add later milestones'
adapter extensions if M1's test foundation is solid.

### 20.4 Defer auth complexity

The cleanest M1 stays mock-auth. M0 Retrospective §9.5 Option A
(wait for M2+ same-origin) or Option C (mock-auth in M1) are
both compatible. The fixture-fed pipeline sidesteps the cross-
origin auth question entirely.

### 20.5 Defer write paths to M1.5 / M2

ACK is the first write the V1 design corpus introduces. Adding
it to M1 risks scope creep and conflates the "read pipeline
proven" milestone with the "first write semantics decided"
milestone. Keeping them separate is worth the small delay.

### 20.6 Stop after M1 to assess (mirroring M0 Scope Proposal §23.7)

M1 closes; pause; write an M1 retrospective; then authorise
M1.5 or M2. The retrospective pattern caught real issues in M0
(the Railway config-precedence lesson) and is worth repeating.

### 20.7 Plan v0.2 will follow

This is **the scope proposal**, not the implementation plan.
Once Tony reviews and authorises this scope, a separate
`HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` (mirroring M0's
v0.2) will detail the exact file scaffold, dependency budget,
DEMO label placement, two-phase sequencing, and ChatGPT review
checklist. The implementation plan is not in scope of this
document.

---

## End of M1 scope proposal

**Status:** v0.1 scope proposal — planning only
**Implementation status:** None
**Next action:** ChatGPT engineering review of the 20 sections,
then Tony's decision on (a) merging this proposal, (b)
resolving the §19 open questions, and (c) authorising drafting
of an M1 Implementation Plan v0.1 (which will be a separate PR
following the M0 v0.2 pattern).

M0 is closed and stable. The V1 planning corpus is now **18
documents** on `main` after this proposal merges (17 prior + 1
new). M1 remains unstarted until explicitly authorised.
