# Horizon V1 — /api/summary Frontend Adapter Design Note (v0.1)

**Status:** Design note — planning only, no implementation
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review)
**Authoritative inputs (merged on `main @ f371e47`):**
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` (PR #39) — the source-shape truth
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38) — the M0 constraints
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` (PR #37)
- Prior seven V1 foundation documents on `main`
**Pending input (not yet authorised):**
- Claude Design UX/UI handoff in `v1-handoff/`
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise implementation.** It does **not**
create any code file. It defines the **contract** an adapter will
later honour when M1 is separately authorised. No `frontend/`
directory is created by this PR. No JavaScript is written. No
`server.py` change is proposed. No `package.json` is added.

---

## 1. Executive Summary

### 1.1 Why an adapter is required

The Shape Spike (`HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md`)
established that the real Beta 10 `/api/summary` response differs
materially from the assumptions in the UX/UI handoff and the
prototype `data.js`. The differences span field naming, nesting,
type, units, time format, and the presence of entire domains
(`shiftLog`, `cascade`, audit log) that have **no backend source
today**.

Without a normalisation layer, V1 React components built against
the prototype shape would silently fail to render, double-render
under stale field names, or display ISO timestamps where short
HH:MM strings were expected. The adapter is the **single seam**
that prevents this.

### 1.2 What the adapter protects

- **Components from backend evolution.** When `/api/summary`
  adds fields, removes fields, renames fields, or shifts nesting,
  the adapter changes — the components do not.
- **Backend from frontend assumptions.** The adapter absorbs
  every field-rename request that would otherwise tempt
  engineers to "just change `server.py` to match the prototype".
  `server.py` stays untouched.
- **Operators from misleading displays.** The adapter labels
  "no backend source today" surfaces explicitly (e.g. shift log,
  cascade, audit log) so the UI never silently fabricates data
  while *appearing* live.
- **Future M1.x adapters from sprawl.** A single normalisation
  module — pure functions, no classes — prevents the adapter
  from drifting into a parallel domain model.

### 1.3 What the adapter does NOT do

- **No business logic.** Operational decisions, conflict ranking
  heuristics, recommendation ordering all stay where they
  belong (today: backend; tomorrow: an explicit, audited
  decision module).
- **No RBAC enforcement.** The adapter does not gate fields by
  role. Role-scoped projection is V1.1+ and lives in the
  backend (per Implementation Strategy §17).
- **No audit emission.** The adapter does not generate audit
  events. Audit is the backend's responsibility; in V1.0 it is
  paused (Stage E-prod deferred).
- **No state.** The adapter is pure functions only. Input: raw
  API JSON. Output: normalised view-model JSON. No singleton,
  no cache, no class hierarchy.
- **No actions.** The adapter handles only the **read path**
  (`GET /api/summary`). Write-path adapters (for what-if /
  decision commit) are separate concerns and out of scope.

### 1.4 Why backend changes are not required yet

Every mapping documented below is achievable in the frontend
alone. `server.py` byte-identical to Beta 10 baseline. The
Spike confirmed the existing response carries enough information
(in a different shape) to drive every V1.0 VTSO console surface
that has a real-data source.

Surfaces that have **no** real-data source today (shift log,
cascade, audit log) are handled by the **Missing Domain
Strategy** in §12 — they are not made up; they are either
deferred to V1.x or rendered with explicit placeholder markers.

Backend changes anticipated in V1.x (apply-decision endpoint,
audit retrieval, shift events, role-scoped projection) are
out of scope of this design note and out of scope of M1.

---

## 2. Source and Target Shapes

### 2.1 Source shape — `RawSummary`

The **source shape** is the actual Beta 10 `/api/summary` JSON
response documented in Shape Spike §3. It has 26 top-level keys,
nested objects for `weather` / `tides` / `dashboard` /
`port_status` / `port_profile`, and arrays for `vessels` /
`berths` / `pilotage` / `towage` / `conflicts` / `guidance` /
`etd_risk` / `berth_utilisation` / `port_tugs` / `port_gangs`.

**Authority:** `server.py` `build_summary()` return statement,
lines 2462–2513 at commit `b381bce`. Any divergence between
this design note and that return statement is a defect in **this
design note**, not in the backend.

**Stability:** the source shape is Beta 10 and is intended to
remain stable through V1.0. Any planned backend evolution will
be communicated as a versioned change to the adapter, not as a
silent breakage.

### 2.2 Target shape — `ViewSummary`

The **target shape** is the normalised view model that V1 React
components consume. It is **defined by this document** and is
versioned independently of the backend.

`ViewSummary` is **not** the prototype's `data.js` shape. It is
the prototype shape *adjusted for* the realities the Spike
uncovered:

- ISO timestamps converted to Date objects + display strings
- Severities normalised to a single case
- Status vocabulary normalised
- Missing-source domains labelled, not fabricated
- Risk merged onto the vessel from the separate `etd_risk` array
- DSW options unwrapped from `decision_support.options`

The target shape's top-level keys are (provisional, may evolve
in v0.2):

```
timestamp           — JS Date object
generatedAtIso      — original ISO string, preserved for audit
portId              — e.g. "BRISBANE"
portName            — e.g. "Port of Brisbane"
portTimezone        — IANA TZ string from port_profile.timezone
portStatus          — port-level counts (occupied/available/etc.)
conditions          — merged from weather + tides + ukc
vessels             — array of normalised Vessel view models
berths              — array of normalised Berth view models
conflicts           — array of normalised Conflict view models
guidance            — array of Guidance view models
pilotage            — array of normalised Pilotage view models
towage              — array of normalised Towage view models
dashboardMetrics    — V1 KPI block
etdRisk             — array of EtdRisk view models (keyed by vessel)
liveness            — per-domain live/sim flags
dataSource          — single source label
missingDomains      — explicit list of "no backend source" tabs
```

The target shape is what every React component imports. The
adapter is the only module that imports the source shape.

### 2.3 Prototype shape — non-authoritative reference

The prototype shape in `v1-handoff/prototype/data.js` is
**reference only**. It informed visual design, not architecture.
Where the prototype's shape diverges from the target shape, the
target shape wins.

When M0 builds against a static `sample.js`, that sample's
shape **should be the target shape**, not the prototype shape.
This way M0's components are already wired for the M1 adapter
output — only the source of the data changes between M0 and M1.

---

## 3. Adapter Principles

### 3.1 Backend stays unchanged

No mapping in this document requires a `server.py` edit. The
adapter is the contract; the backend is the source of truth;
the components consume the contract output. If a future M1
discovery requires a backend change, that is an explicit,
separately-authorised V1.x decision — not an adapter
side-effect.

### 3.2 Adapter is frontend-only

Lives in `frontend/src/api/` (see §4). No Python. No build step
beyond Vite's existing transpile. No Node tooling beyond the
M0 dependency budget (React, ReactDOM, Vite, plugin) plus
**one date library** to be selected in §13 (likely `date-fns-tz`
or `luxon`).

### 3.3 No business logic hidden in UI components

Components consume the target shape and render. They do not
re-derive timestamps, do not re-map severities, do not look up
vessels from a separate array. All such transformations happen
in the adapter exactly once per response.

### 3.4 All field renames centralised

Every rename (`vessel_id → vesselId`, `cargo_type → cargo`,
`berth_id → berth`, etc.) is declared in the adapter. Grepping
the components for `vessel_type` should find zero hits.

### 3.5 Missing values handled explicitly

Every nullable field in the source has a defined behaviour in
the adapter:

- pass through `null` (UI displays "—")
- convert to a sentinel value (e.g. "Unassigned")
- omit the field entirely (UI conditionally renders)

These choices are documented per-field in §7–§11.

### 3.6 Timestamps normalised

Adapter outputs both a JS Date and a port-local display string
for every timestamp. UI components never call `new Date(...)`
themselves.

### 3.7 Source / confidence preserved

Every domain carries its source label (live / simulation /
unknown) and the adapter exposes per-block booleans
(`liveness.vessel`, `liveness.weather`, `liveness.tide`).
Confidence on decision-support options is preserved verbatim
(strings, not converted to numeric — see §8.6).

### 3.8 No fake audit / action states

The adapter does **not** synthesise audit events. It does
**not** create "decision applied" indicators. It does not
populate the right-panel Audit Log tab from `/api/summary`
data (there is no source). See §12.

### 3.9 No RBAC enforcement in adapter

Filtering, masking, or hiding fields by role is **not** the
adapter's job. The Permission Model and Information Architecture
require **server-side projection**. The adapter receives whatever
the server sent. If V1.1+ introduces role-scoped responses, the
adapter simply consumes the smaller payload — no role logic in
the adapter.

---

## 4. Proposed Adapter Location

**No files are created by this PR.** The conceptual location
for the future M1 adapter:

```
frontend/src/api/
├── horizon.js                  ← single public entry point
│                                  exported: fetchSummary(), subscribe(), etc.
├── adapters/
│   ├── summaryAdapter.js       ← top-level rawSummary → ViewSummary
│   ├── conditionsAdapter.js    ← weather + tides + ukc → conditions
│   ├── vesselAdapter.js        ← vessels[*] + etd_risk[*] → Vessel[*]
│   ├── conflictAdapter.js      ← conflicts[*] → Conflict[*]
│   ├── guidanceAdapter.js      ← guidance[*] + alert-shaped conflicts
│   ├── dashboardAdapter.js     ← dashboard + port_status → dashboardMetrics
│   ├── etdRiskAdapter.js       ← etd_risk[*] → EtdRisk[*]
│   ├── time.js                 ← ISO ↔ port-local helpers
│   ├── status.js               ← status / severity / source mappings
│   └── units.js                ← visibility nm ↔ km, etc.
└── fixtures/                   ← sample raw payloads for tests (M1)
    ├── brisbane-live.json
    ├── melbourne-sim.json
    ├── empty-conflicts.json
    └── null-fields.json
```

**This is conceptual.** No file is created until M1 is
authorised. The structure is documented here so M1's
implementation PR can simply cite this section.

**Public API surface** (the **only** thing components are
permitted to import):

```
import { fetchSummary, useSummary } from 'frontend/src/api/horizon';
```

Everything in `frontend/src/api/adapters/` is internal to the
adapter module. Components never reach inside.

---

## 5. Top-Level Mapping Contract

Per-key mapping from `RawSummary` (source) to `ViewSummary`
(target):

| Source key (RawSummary) | Target key (ViewSummary) | Transformation |
|---|---|---|
| `generated_at` | `timestamp`, `generatedAtIso` | parse ISO → JS Date for `timestamp`; preserve raw ISO for audit |
| `port_name` | `portName` | direct |
| `port_profile.id` | `portId` | direct |
| `port_profile.timezone` | `portTimezone` | direct (default `"Australia/Brisbane"` if absent) |
| `port_status` | `portStatus` | snake_case → camelCase; pass through |
| `weather` + `tides` + `ukc` | `conditions` | merge — see §6 |
| `vessels` + `etd_risk` | `vessels` | per-item merge + normalise — see §7 |
| `berths` | `berths` | normalise per-item; preserve raw segment data for Gantt |
| `conflicts` (full array, sorted) | `conflicts` | per-item normalise — see §8 |
| `guidance` | `guidance` | per-item normalise — see §9 |
| `pilotage` | `pilotage` | per-item normalise (rename fields) |
| `towage` | `towage` | per-item normalise |
| `dashboard` + `port_status` | `dashboardMetrics` | merge — see §10 |
| `etd_risk` | `etdRisk` | per-item normalise — see §11 |
| `port_profile.using_live_*` | `liveness` | extract booleans for `vessel`, `weather`, `tide` |
| `data_source_label` | `dataSource` | direct (display string only) |
| `port_profile.available_ports` | `availablePorts` | direct |
| — | `missingDomains` | computed list — see §12 |

**Top-level source keys NOT mapped into `ViewSummary`**
(intentionally — these are not needed for the V1.0 VTSO console):

- `lookahead_hours` (constant 48, can be added later if needed)
- `data_source` (subsumed by `liveness`)
- `scraped_at` (preserved inside `dataSource.scrapedAt` if needed)
- `port_tugs`, `port_gangs` (deferred until resource panel is built)
- `port_geo` (deferred until VTS map tab is built)
- `dukc`, `esg` (deferred — V1.x analytics surface)
- `arrival_ukc` (deferred — separate Arrival UKC view in V1.x)
- `berth_utilisation` (kept available but renamed to `berthUtilisation`)

**Domains in the target shape with NO source key** (handled per
§12 — Missing Domain Strategy):

- `shiftLog` (no `shift_log` in source)
- `cascade` on individual conflicts (no `cascade` field)
- `auditLog` (no `/api/audit` endpoint exists)
- `replayPackages` (V1.6 — out of scope)
- Stakeholder feed (V1.5 — out of scope)
- Role-scoped projections (V1.1+ — out of scope)

---

## 6. Conditions Adapter

### 6.1 Mapping

| `RawSummary` source | Target `conditions` field | Transformation |
|---|---|---|
| `weather.conditions` (e.g. "Good") | `rating` (e.g. "GOOD") | uppercase |
| `weather.wind_speed_kts` | `windSpeedKts` | direct |
| `weather.wind_direction_deg` | `windBearingDeg` | direct |
| `weather.wind_direction_label` | `windDirLabel` | direct |
| `weather.wind_beaufort` | `windBeaufort` | direct |
| `weather.swell_height_m` | `swellHeightM` | direct |
| `weather.swell_period_s` | `swellPeriodS` | direct |
| `weather.swell_direction_label` | `swellDirLabel` | direct |
| `weather.visibility_nm` | `visibilityNm`, `visibilityKm` | both — see §13.4 for nm↔km conversion |
| `weather.pressure_hpa` | `pressureHpa` | direct |
| `weather.source` | `weatherSource` ("live" / "simulation") | direct |
| `tides.current_height_m` | `tideHeightM` | direct |
| `tides.state` (rising / falling) | `tideState` | direct |
| `tides.next_event_time` (ISO) | `tideNextTime`, `tideNextLabel` | parse + label ("HW"/"LW") |
| `tides.min_today_m`, `max_today_m` | `tideMinM`, `tideMaxM` | direct |
| `ukc.value`, `ukc.status` | `ukcM`, `ukcStatus` ("ok"/"caution"/"critical") | direct |

### 6.2 Handoff fields with no real source

| Handoff field | Treatment |
|---|---|
| `wind_gust` | omit; UI hides the gust display (M1 acceptance criterion) |
| `pressure_trend` ("steady"/"rising"/"falling") | omit OR derive heuristically by comparing to a prior poll (out of scope for V1.0 — omit) |
| `sea_temp` | omit; UI hides the sea-temp tile (or renders "—") |

### 6.3 Defaults

If `weather` block is entirely absent (network failure, cache
empty), `conditions` is `null` and the UI displays a "Conditions
unavailable" banner. **The adapter does not fabricate weather.**

---

## 7. Vessel Adapter

### 7.1 Identity

| Source | Target | Notes |
|---|---|---|
| `id` (e.g. "V001") | `vesselId` | rename |
| `name` | `name` | direct |
| `imo` | `imo` | direct |
| `flag` | `flag` | direct |

### 7.2 Status

| Source `status` | Target `status` | Notes |
|---|---|---|
| `"berthed"` | `"berthed"` | direct |
| `"confirmed"` | `"confirmed"` | preserve — display "Inbound (confirmed)" |
| `"scheduled"` | `"scheduled"` | preserve — display "Scheduled" |
| `"at_risk"` | `"at_risk"` | preserve — display with at-risk decoration |
| `"arrived"` | `"arrived"` | preserve — terminal state before berthing |
| `"departed"` | `"departed"` | preserve — filter out of active roster by default |

The target shape **adopts the backend's 6-state vocabulary**
(not the prototype's 4-state). This is a deliberate decision
documented in Shape Spike §10 #2: the backend vocabulary is
information-richer; collapsing to the prototype's 4 states loses
the distinction between "confirmed" / "scheduled" / "at_risk"
which operators need.

UI components are responsible for any per-status visual treatment
(badge colour, sort order, icon). The adapter does **not** group
statuses.

### 7.3 Source

| Source `source` | Target `source` | Notes |
|---|---|---|
| `"ais"` | `"ais"` | direct |
| `"mst"` | `"mst"` | direct |
| absent (simulated) | `"sim"` | adapter default |
| anything else | `"unknown"` | adapter default for safety |

### 7.4 ETA / ETD / ATA / ATD

| Source | Target | Notes |
|---|---|---|
| `eta` (ISO 8601 UTC) | `etaIso`, `eta`, `etaShort`, `etaLocalShort` | preserve ISO; provide JS Date; provide short display; provide port-local short |
| `etd`, `ata`, `atd` | same pattern as `eta` | nullable — `null` preserved |

The four short/local strings are computed once by the adapter
(see §13). UI components never call `new Date(...)`.

### 7.5 Berth

| Source | Target | Notes |
|---|---|---|
| `berth_id` | `berth` (string) | rename; `null` preserved |
| (lookup in `berths[]` by id) | `berthName` | adapter joins; "Unassigned" if null |

### 7.6 Dimensions

| Source | Target | Notes |
|---|---|---|
| `loa` | `loaM` | rename for unit clarity |
| `draught` | `draftM` | spelling normalisation + unit clarity |
| `cargo_type` | `cargo` | rename |
| `vessel_type` | `type` | rename |
| `pilotage_required` | `pilotageRequired` | camelCase |
| `towage_required` | `towageRequired` | camelCase |

### 7.7 Risk

The adapter performs a **join** between `vessels[*]` and
`etd_risk[*]` (keyed by `vessel_id`):

| Source | Target | Notes |
|---|---|---|
| `etd_risk[*].risk_score` (matched by vessel_id) | `riskScore` (0–100) | join |
| `etd_risk[*].risk_level` | `riskLevel` ("low"/"medium"/"high"/"critical") | direct |
| `etd_risk[*].risk_factors` | `riskFactors` (array) | direct |
| (no match in etd_risk) | `riskScore: 0`, `riskLevel: "low"`, `riskFactors: []` | adapter default for vessels without risk record |

### 7.8 Missing AIS fields

Handoff documents `mmsi`, `sog`, `cog`, `range_nm`, `beam` on
vessels. The real backend does **not** carry these. The
adapter:

- **Does not** invent these fields
- Includes them as `null` in the target shape with a comment in
  `vesselAdapter.js` referencing this section
- UI components that depend on these fields must either hide
  the field or display "—"

These five fields are **AIS-source fields**. They could be
populated by a backend change to surface AIS data through to
the response (V1.x), but this is **not in scope** for M1.

### 7.9 Position

| Source | Target |
|---|---|
| `lat` | `lat` |
| `lon` | `lon` |

### 7.10 Notes

| Source | Target |
|---|---|
| `notes` (or `null`) | `notes` (string or `null`) |
| `agent` | `agent` |

---

## 8. Conflict Adapter

### 8.1 Identity

| Source | Target |
|---|---|
| `id` (8-char hash) | `conflictId` |
| `conflict_type` (e.g. "berth_overlap") | `type` |
| `signal_type` ("CONFLICT" / "WARNING" / "ADVISORY" / "WEATHER") | `signalType` |
| `data_source` ("live" / "simulated") | `dataSource` |

### 8.2 Severity

| Source `severity` | Target `severity` |
|---|---|
| `"critical"` | `"CRITICAL"` |
| `"high"` | `"HIGH"` |
| `"medium"` | `"MEDIUM"` |
| `"low"` | `"LOW"` |

Uppercase target consistent with handoff convention. The mapping
is exhaustive and lives in `status.js`.

### 8.3 Title / description

Backend does **not** carry a `title`. The adapter derives it:

```
title = `${humanise(conflict_type)}: ${vessel_names.join(' vs ')}`
```

Where `humanise` is:

- `"berth_overlap"` → `"Berth overlap"`
- `"berth_not_ready"` → `"Berth not ready"`
- `"pilotage_window"` → `"Short pilotage notice"`
- `"tug_double_book"` → `"Tug double-booked"`
- (others → title-case + space-separated)

`description` is passed through unchanged.

### 8.4 Vessels

| Source | Target |
|---|---|
| `vessel_ids` | `vesselIds` |
| `vessel_names` | `vesselNames` |

### 8.5 Berth / timing

| Source | Target |
|---|---|
| `berth_id` | `berth` (nullable) |
| `berth_name` | `berthName` (nullable) |
| `conflict_time` (ISO) | `conflictTimeIso`, `conflictTime` (Date), `conflictTimeShort` (HH:MM) |

### 8.6 Decision support / options / deadline

| Source | Target |
|---|---|
| `decision_support.recommended_option_id` | `recommendedOptionId` |
| `decision_support.recommended_reasoning` | `recommendedReasoning` |
| `decision_support.confidence` ("high" / "medium") | `confidence` (string, **preserved as string**) |
| `decision_support.decision_deadline` (ISO) | `decisionDeadlineIso`, `decisionDeadline` (Date), `decisionDeadlineMinutes` (computed at adapt-time; UI recomputes for live countdown) |
| `decision_support.options[*]` (== `sequencing_alternatives[*]`) | `options[*]` — flattened up to the conflict level |

**Confidence is preserved as a string.** The handoff documents
a fraction (`0.88`). The backend emits `"high"` / `"medium"`.
The adapter does **not** invent a fraction — `"high"` and
`"medium"` are kept as strings; the UI renders them as text
("Confidence: High") or as a 2-state pill. If V1.x backend
later emits a fraction, the adapter can absorb that change and
expose a numeric field; for now, do not fabricate.

### 8.7 Per-option mapping

For each `options[*]` (which is also each `sequencing_alternatives[*]`):

| Source | Target |
|---|---|
| `id` | `id` |
| `label` | `label` |
| `desc` / `description` | `description` |
| `safety` (number) / `safety_score` | `safetyScore` |
| `delay` (string like "+0h 30m") | `delayLabel`, plus optionally `delayMinutes` derived |
| `cost` (string like "$8.0K") | `costLabel` |
| `confidence` (number 0–100, per option) | `confidence` (preserved per source — may be a number on options) |
| `recommended` (bool) | `recommended` |
| `impactSummary` | omit (not in backend) |
| `resource` | omit (not in backend) |

**Note:** per-option `confidence` from the prototype is a
number (e.g. 88); the backend's `sequencing_alternatives`
options may not carry confidence at all (only the wrapper
`decision_support.confidence` is a string). The adapter
preserves whatever is present per-option; if absent, the
field is `null` and the UI hides the per-option confidence
display.

### 8.8 Cascade fallback

The prototype's DSW step 2 (Downstream impact) renders a
PRIMARY / +1 / +2 cascade tree. **The backend does not carry
this.** The adapter:

- Sets `cascade: null` (not `[]`) so UI can detect the absence
- M1 acceptance: if `cascade` is `null`, the DSW step 2 either
  is hidden (V1.0 baseline) or displays "Downstream impact —
  computed by Horizon Engine in V1.x"

The adapter does **not** synthesise a heuristic cascade. That
would be silent fabrication — see §18.1.

### 8.9 Other passthrough

| Source | Target |
|---|---|
| `resolution_options` (plain strings) | `resolutionGuidance` (array of strings) |
| `safety_score` (post-attached, "Low"/"Medium"/"High") | `overallSafetyLabel` |

---

## 9. Guidance / Alert Adapter

The left-panel "Alerts & Guidance" component renders a unified
list. The adapter produces it by merging two sources:

### 9.1 Guidance items

`RawSummary.guidance[*]` are alerts the backend has generated
that are **not** conflicts (e.g. weather advisories that didn't
become a CONFLICT, port-rule notifications, resource notices).

Mapping (per item):

| Source field | Target field |
|---|---|
| `id` | `id` (prefixed `"g-"`) |
| `category` ("WEATHER" / "BERTH" / "NAVIGATION" / "OPS") | `category` |
| `severity` ("CRITICAL" / "WARNING" / "ADVISORY" / "INFO") | `severity` (case as source) |
| `title` | `title` |
| `detail` | `detail` |
| `deadline` (string) | `deadline` |
| `ts` | `timestampShort` |

### 9.2 Alert-shaped conflicts

`RawSummary.conflicts[*]` with `signal_type` ∈ `"WARNING"` or
`"ADVISORY"` or `"WEATHER"` also appear in the left-panel alert
list (not the DecisionCard, which is reserved for `signal_type:
"CONFLICT"`).

Mapping:

| Source | Target | Notes |
|---|---|---|
| `conflicts[i].id` | `id` (prefixed `"c-"`) | distinguish from guidance |
| (derived from `conflict_type`) | `category` | e.g. `"berth_overlap"` → `"BERTH"`, `"pilotage_window"` → `"NAVIGATION"`, weather-derived → `"WEATHER"` |
| `severity` | uppercased | per §8.2 |
| `(derived title)` | `title` | per §8.3 |
| `description` | `detail` | direct |
| `(derived from decisionDeadline)` | `deadline` | "Action by HH:MM" or "Action within Hh Mm" |

### 9.3 Critical conflict isolation

The **first** `signal_type: "CONFLICT"` (after sorting) is
extracted into the target shape's `activeDecision` field — that
is what the DecisionCard renders. Other `CONFLICT` items can
also appear in the alert list, or be hidden, per UI design.

### 9.4 Ordering

The adapter sorts the combined alerts list by:

1. Severity (CRITICAL → WARNING → ADVISORY → INFO)
2. Deadline ascending (nearer first)
3. Timestamp descending (newer first)

### 9.5 No fabrication

The adapter does **not** insert "system" alerts, "welcome"
banners, or audit-log-derived advisories. If the backend
returns zero `guidance` and zero `conflicts`, the alert list
is empty. The UI displays an empty-state message ("No active
alerts").

---

## 10. Dashboard Metrics Adapter

The target `dashboardMetrics` merges the backend's `dashboard`
(11 fields) with selected fields from `port_status` (10 fields).

### 10.1 Mapping

| Source | Target | Group |
|---|---|---|
| `dashboard.berth_utilisation_pct` | `berthUtilisationPct` | Operations |
| `dashboard.forecast_utilisation_48h` | `forecastUtilisation48h` | Operations |
| `port_status.vessels_in_port` | `vesselsInPort` | Operations |
| `port_status.vessels_expected_24h` | `vesselsExpected24h` | Operations |
| `port_status.vessels_departing_24h` | `vesselsDeparting24h` | Operations |
| `port_status.active_conflicts` | `activeConflicts` | Operations |
| `port_status.critical_conflicts` | `criticalConflicts` | Operations |
| `dashboard.on_time_departure_pct` | `onTimeDeparturePct` | Performance |
| `dashboard.avg_dwell_hours` | `avgDwellHours` | Performance |
| `dashboard.pilot_ops_12h` | `pilotOps12h` | Performance |
| `dashboard.tug_ops_12h` | `tugOps12h` | Performance |
| `dashboard.vessels_at_risk` | `vesselsAtRisk` | Safety |
| `port_status.pilots_available` | `pilotsAvailable` | Resources |
| `port_status.tugs_available` | `tugsAvailable` | Resources |

### 10.2 Missing handoff fields

Handoff documents `movements_6h`. Real backend does not have
this. Closest equivalent: `pilot_ops_12h + tug_ops_12h` — but
the windows differ (6h vs 12h). The adapter:

- **Does not** invent `movements_6h`
- Provides `pilotOps12h` and `tugOps12h` separately
- UI tile that wanted `movements_6h` either uses the 12h fields
  or is removed for V1.0

### 10.3 Trend / direction

The handoff's KPI cards show `trend` (e.g. `"+2"`) and `dir`
("up" / "down" / "flat"). The backend does **not** provide
trends. V1.0 either:

- Omits the trend (preferred for V1.0 — fewer false signals)
- Computes trends client-side by comparing sequential polls
  (M1.x enhancement)

For V1.0 baseline, the adapter sets trend fields to `null`.

---

## 11. ETD Risk Adapter

Even though risk is already joined onto each `Vessel`
(per §7.7), `ViewSummary.etdRisk` is **also** exposed as a
top-level array for the dashboard's "ETD risk" table.

### 11.1 Mapping

| Source `etd_risk[*]` | Target `etdRisk[*]` |
|---|---|
| `vessel_id` | `vesselId` |
| `vessel_name` | `vesselName` |
| `risk_score` | `riskScore` |
| `risk_level` | `riskLevel` |
| `risk_factors` (array) | `riskFactors` (array) — first item also exposed as `reasonPrimary` |

### 11.2 Missing handoff field

Handoff documents `delta` (e.g. `"+2h 00m"`). The backend does
**not** carry this. The adapter:

- Sets `delta: null`
- UI displays "—" or hides the column
- Computing `delta` would require comparing against an "original
  scheduled" ETD that the backend does not currently track —
  this is a V1.x backend enhancement, not an adapter
  responsibility

---

## 12. Missing Domain Strategy

### 12.1 Principle

For every domain that has no backend source, the adapter does
**one** of:

A. **Mark explicit absence** (`null` / `[]` + flag)
B. **Delegate to UI placeholder** (UI displays "coming in V1.x")
C. **Defer the surface entirely** (no UI for it in V1.0)

The adapter **never** fabricates data. Fabrication looks
identical to real data downstream and creates the false-state
risks documented in the UX/UI Handoff Validation §13.

### 12.2 Per-domain treatment

| Domain | Source today | Adapter behaviour | UI behaviour in V1.0 |
|---|---|---|---|
| Shift Log | none — no `shift_events` endpoint | sets `shiftLog: null` in `ViewSummary`; adds `"shiftLog"` to `missingDomains` | UI hides Shift Log tab OR shows a placeholder card: "Shift Log — coming in V1.x" |
| Replay | none — V1.6 capability | not in `ViewSummary` at all | no Replay surface in V1.0 |
| Stakeholder feed | none — V1.5 mobile surface | not in `ViewSummary` (different consumer anyway) | no stakeholder surface in V1.0 |
| Audit Log (right panel) | no `GET /api/audit` endpoint | sets `auditLog: null`; adds `"auditLog"` to `missingDomains` | UI either hides the Audit Log right-panel tab OR shows session-only events labelled clearly as session-scope |
| Executive analytics (rollups) | partially in `dashboard`, but no shift comparison, no week-over-week | passes through what exists | UI Performance tab uses what's available; no fabricated trends |
| Role-scoped projection | none — single shape served to all roles | adapter does nothing; the future role gate is server-side | no role-scoped behaviour in V1.0 |
| Cascade (PRIMARY / +1 / +2) | none | sets `cascade: null` per conflict | DSW step 2 either hidden in V1.0 or displays "Downstream impact — computed by Horizon Engine in V1.x" |
| Pilotage (partial) | full pilotage array present | full normalisation | full Pilotage tab supported in V1.0 |
| Towage | full towage array present | full normalisation | Towage rendered as part of pilotage / VTS in V1.0 |
| Resource roster (tugs, gangs) | `port_tugs`, `port_gangs` arrays present | passthrough renamed to `portTugs`, `portGangs` | optional V1.0 — VTSO console may use it; not blocking if deferred |
| Berth Gantt segments | `berths[*].segments` not present in current backend (it's the prototype's invention) | adapter computes Gantt segments by sorting `vessels[*]` by ETA/ETD per berth | V1.0 Gantt is computed client-side from vessel timings |

### 12.3 The `missingDomains` field

`ViewSummary.missingDomains` is an array of strings — every
domain the adapter could not populate from the source.

UI components import this field to conditionally hide tabs /
panels / cards. **No UI tab silently shows blank**; either it
populates from real data, or it is hidden, or it shows an
explicit "coming later" message.

### 12.4 Placeholders allowed only when labelled

If a surface ships in V1.0 with placeholder content (e.g. the
right-panel Audit Log tab with session-only events), the
placeholder is **labelled** ("Session events only — production
audit ledger arrives in V1.x"). Unlabelled placeholders are
treated as defects.

---

## 13. Time and Unit Normalisation

### 13.1 ISO timestamp → JS Date

Every ISO 8601 UTC timestamp the backend emits is parsed once
in the adapter using the chosen date library. The adapter
exposes:

- `*Iso` — original string (for audit / logs)
- `*` (no suffix) — JS Date
- `*Short` — HH:MM in UTC (for systems work)
- `*LocalShort` — HH:MM in `port_profile.timezone`

### 13.2 UTC vs port-local context

`port_profile.timezone` is the authority. Per port:

| Port | IANA timezone |
|---|---|
| BRISBANE | `Australia/Brisbane` |
| MELBOURNE | `Australia/Melbourne` |
| DARWIN | `Australia/Darwin` |
| GEELONG | `Australia/Melbourne` (same as Melbourne) |

Brisbane does not observe daylight saving; Melbourne does.
**The date library must be DST-aware.** Naïve `Date#toLocaleString`
is acceptable for display but not for arithmetic.

### 13.3 HH:MM display

The default display format for V1.0 is `HH:mm` (24-hour, no
seconds). Where a date is in the future today: `"HH:mm"`. Where
in tomorrow or beyond: `"DD MMM HH:mm"`.

For relative countdowns (e.g. DSW deadline), the adapter
exposes `decisionDeadline` as a Date; the UI computes
`"in 2h 14m"` on each render tick. The adapter does **not**
return a stale relative-time string.

### 13.4 Visibility — nm ↔ km

| Field | Source | Target |
|---|---|---|
| Visibility | `weather.visibility_nm` (nautical miles) | `visibilityNm` AND `visibilityKm` (computed: `nm × 1.852`) |

Both are exposed; the UI picks one. Default display in V1.0 is
**nautical miles** (matches operator convention). If V1.x
introduces a unit-preference setting, the UI selects between
the two without adapter changes.

### 13.5 Tide units

| Source | Target |
|---|---|
| `tides.current_height_m` | `tideHeightM` (metres) |
| `tides.min_today_m`, `max_today_m` | `tideMinM`, `tideMaxM` |

Units in the field names; no conversion needed.

### 13.6 Delay duration formatting

When the adapter exposes computed durations (e.g.
`decisionDeadlineMinutes`), it provides a raw number
(`"minutes from now"`). UI components format for display:

- `> 60 min` → `"Xh Ym"` (e.g. `"2h 14m"`)
- `≤ 60 min`, `> 0` → `"X min"` (e.g. `"23 min"`)
- `≤ 0` → `"overdue"` with a critical visual treatment

The adapter does not return formatted strings — the UI
component owns presentation.

---

## 14. Severity / Status Normalisation

All categorical mappings live in `frontend/src/api/adapters/status.js`.

### 14.1 Severity (conflicts, guidance)

| Source (lowercase) | Target (uppercase) |
|---|---|
| `"critical"` | `"CRITICAL"` |
| `"high"` | `"HIGH"` |
| `"medium"` | `"MEDIUM"` |
| `"low"` | `"LOW"` |
| unknown | `"INFO"` (fallback) |

### 14.2 Conditions rating

| Source (title case) | Target (uppercase) |
|---|---|
| `"Excellent"` | `"EXCELLENT"` |
| `"Good"` | `"GOOD"` |
| `"Moderate"` | `"MODERATE"` |
| `"Poor"` | `"POOR"` |
| unknown | `"UNKNOWN"` (fallback — UI displays "—") |

### 14.3 Vessel status

The target preserves the backend's 6-state vocabulary
(§7.2). No collapse, no rename.

### 14.4 Data source

| Source | Target |
|---|---|
| `"ais"` | `"AIS"` (uppercase for display) |
| `"mst"` | `"MST"` |
| `"qships"` | `"QShips"` |
| `"live"` (generic) | `"Live"` |
| `"simulation"` | `"Simulation"` |
| absent | `"Simulation"` |

For boolean live-vs-not, the adapter uses the
`port_profile.using_live_*` flags directly.

### 14.5 Unknown / fallback values

Every mapping table includes a fallback ("unknown" or
"UNKNOWN"). The adapter **never** throws on unrecognised source
values — it logs a console warning in non-production builds
and returns the fallback. This protects components from
runtime errors when the backend introduces a new value.

---

## 15. Adapter Error Handling

### 15.1 Missing arrays

If `RawSummary.vessels` is `undefined` or not an array, the
adapter returns `vessels: []`. Components render the empty
state. The adapter logs a warning.

Same treatment for: `berths`, `pilotage`, `towage`, `conflicts`,
`guidance`, `etd_risk`, `berth_utilisation`.

### 15.2 Null fields

Every nullable field has a defined behaviour (§7–§11). The
adapter passes `null` through where the UI must distinguish
"no value" from "zero value" (e.g. `eta: null` means "no ETA
assigned"; `eta: '...'` parsed to a Date means "this is the
ETA").

### 15.3 Unexpected types

If a field has the wrong type (e.g. `risk_score: "high"` when
the adapter expected a number), the adapter:

- Logs a warning in non-production builds
- Substitutes a safe default (`0` for numbers, `null` for
  strings, `[]` for arrays, `false` for booleans)
- Continues processing (does not throw)

This is **defensive** — the components must not crash because
of a backend type drift. Any defensive substitution is logged.

### 15.4 Stale data

The adapter does not have a stale-data concept of its own —
each call processes whatever JSON it received. The UI polling
layer (`useSummary()` hook, M1) is responsible for indicating
"last successful poll" and "data may be stale".

### 15.5 Failed fetch

Outside the adapter's concern. The fetch wrapper in
`horizon.js` handles HTTP errors, network failures, and 401
auth errors. The adapter only ever receives a successfully-
parsed JSON object.

If a previous successful response is cached and a poll fails,
the UI can display the last-known `ViewSummary` with a
"connection lost" badge. The adapter is stateless and not
involved.

### 15.6 Partial response

If `/api/summary` returns a response missing critical fields
(e.g. no `port_profile`), the adapter:

- Returns a `ViewSummary` with `null` for the missing block
- Adds the block name to `missingDomains`
- UI displays a banner: "Some Horizon data is currently
  unavailable. Displaying last-known values."

### 15.7 Safe empty states

The target shape is **always** parseable. Even on extreme
failure (empty `{}` input), the adapter returns a `ViewSummary`
with every key set to its empty-state default (`null` or `[]`).
UI components must render against this empty state without
crashing.

---

## 16. Testing Strategy for Future M1

### 16.1 Adapter tests are fixture-based

When M1 is authorised, the adapter implementation arrives with
unit tests using static fixtures. **No live backend is required
to test the adapter** — the fixtures are committed JSON.

### 16.2 Fixture inventory

`frontend/fixtures/` (created in M1, not now):

| Fixture | Source | Purpose |
|---|---|---|
| `brisbane-live.json` | recorded Beta 10 `/api/summary` (Brisbane, mid-shift with conflicts) | smoke-test all mappings |
| `melbourne-sim.json` | recorded `/api/summary` (Melbourne, simulation only, no live AIS) | exercise simulation path |
| `geelong-quiet.json` | recorded `/api/summary` (Geelong, zero conflicts) | exercise empty-state |
| `darwin-mixed.json` | recorded `/api/summary` (Darwin, mixed live/sim) | exercise mixed sources |
| `empty-conflicts.json` | derived (any port) with `conflicts: []` | empty-state baseline |
| `null-fields.json` | derived: vessels with `eta=null`, conflicts with `berth_id=null`, etc. | nullable-field handling |
| `malformed.json` | derived: `risk_score: "string"`, `severity: 42`, etc. | type-defence handling |

These fixtures are **recorded captures** plus **derived
variations**. They are checked into the repo in M1 (not now)
and are **not** generated dynamically.

### 16.3 Test types

1. **Snapshot tests** — for each fixture, the adapter output is
   compared against a committed snapshot. Any unintentional
   change in adapter behaviour is caught.
2. **Per-domain assertions** — explicit tests for vessel status
   mapping, severity mapping, conditions merge, cascade absence
   handling, risk join.
3. **Empty-state tests** — given an empty `{}` input, the
   adapter returns a fully-formed `ViewSummary` with empty
   defaults.
4. **Defence tests** — malformed input does not throw; warnings
   are logged; defaults are substituted.
5. **Round-trip tests** — `View → JSON.stringify → JSON.parse`
   produces an equivalent view (no `Date` serialisation
   surprises).

### 16.4 What is NOT tested at the adapter layer

- Backend correctness (that's `tests/test_beta10_regression.py`)
- Auth, polling cadence, error retries (that's `useSummary`'s
  job)
- Component rendering (that's the component test suite, M1)
- End-to-end flows (M9 — operational hardening — only)

### 16.5 No backend tests required for adapter

The adapter does not touch Python. No `pytest` work. No
changes to `tests/test_beta10_regression.py`. The Beta 10
regression gate continues to validate `server.py` independent
of adapter work.

---

## 17. M0 / M1 Boundary

### 17.1 M0 does not use this adapter

Per M0 Scope Proposal §10, M0 makes zero API calls. M0 renders
against a static `frontend/src/data/sample.js`. **The adapter
does not exist in M0.**

### 17.2 M0 may shape its mock data as the target view model

This is the **key recommendation** for M0: when M0 creates
`sample.js`, it should be shaped as `ViewSummary` (the target
shape) — **not** as the prototype's `data.js` shape. This way,
when M1 introduces the adapter and live API integration, the
components don't change — only the source of `ViewSummary` does
(static file → adapter output).

This means M0 needs to know the target shape. This design
note is the input M0 uses to write `sample.js`.

### 17.3 M1 introduces read-only live API integration

M1 (per Execution Plan §11) wires the adapter, the fetch
wrapper, and the polling hook. M1 acceptance includes:

- `fetchSummary()` returns a valid `ViewSummary` against all
  four ports
- All adapter tests pass
- No component renders `undefined` from any fixture
- The fetch wrapper handles 401 (re-auth) and network errors
  gracefully

### 17.4 No action writes in M1

M1 is **read-only**. No `POST /api/whatif`, no port switch
(deferred to M4+), no decision commit. Action wiring lives in
later milestones with their own authorisations.

### 17.5 What lives in which milestone

| Concern | M0 | M1 | M2+ |
|---|---|---|---|
| Static shell + tokens | ✓ | | |
| `ViewSummary` target shape used in sample | ✓ | | |
| Fetch wrapper | | ✓ | |
| Adapter implementation | | ✓ | |
| Adapter tests | | ✓ | |
| Polling hook | | ✓ | |
| Login flow | | ✓ | |
| `server.py` static-route at `/v1/*` | | | M2+ (separate auth) |
| Tab content beyond placeholders | | partial | M3+ |
| DSW | | | M3 |
| What-If integration | | | M5 |
| Decision commit (audit-linked) | | | M7 |
| Audit log retrieval | | | M8 |
| Replay workspace | | | M8/M9 |

---

## 18. Risks

### 18.1 Silent data fabrication

**Risk:** the adapter quietly fills in values the backend never
provided (e.g. a fake `cascade`, a synthesised `delta`, a
guessed `pressure_trend`). Operators see plausible-looking data
and act on it; the data is invented.

**Mitigation:** §12 (Missing Domain Strategy) — explicit `null`
plus `missingDomains` list. Code review checklist for adapter
PRs includes "does any branch introduce a non-passthrough,
non-derived value?" If yes, justify in the PR description or
remove.

### 18.2 UI overpromising audit / replay / action state

**Risk:** UI components import a "decision committed" badge or
"audit chain intact" indicator that the adapter populates from
client state, not backend state. Operators believe the system
is more advanced than it is.

**Mitigation:** the adapter does **not** expose any field that
implies action / audit completion. The DSW step 5 (per UX/UI
Handoff Validation §9.4) must be reframed to "Decision
recorded locally — production audit pending V1.x". The adapter
contributes nothing to this surface other than passing through
what the backend says (which is currently: nothing).

### 18.3 Wrong unit display

**Risk:** the adapter forgets a unit conversion (visibility,
sea temp, distances). UI shows "12" when it should show "22"
or vice versa.

**Mitigation:** unit conversion lives in `units.js` as a small
table. Unit tests for `units.js` are mandatory in M1. Display
strings in components always include the unit (`"22 km"`,
`"12 nm"`, `"1.4 m"`) — no bare numbers.

### 18.4 Hidden backend coupling

**Risk:** components import `summaryAdapter.js` directly and
add ad-hoc transformations there ("just need one more thing").
The adapter grows into a domain model.

**Mitigation:** §4 — components import only `horizon.js`.
Adapter internals are not exported publicly. PR review
catches violations.

### 18.5 Prototype shape becoming assumed truth

**Risk:** an engineer reads `v1-handoff/prototype/data.js` and
believes it is the API contract, building a component against
prototype field names rather than `ViewSummary`.

**Mitigation:** this design note is the canonical contract.
The prototype is reference-only per §2.3. M1 acceptance
includes a grep audit: components must not contain
`fleet`, `etdRisk` (camelCase from prototype), `decision.cascade`,
etc., except as references to `ViewSummary` fields.

### 18.6 Frontend-only security assumptions

**Risk:** an engineer hides a field in the adapter ("the
stakeholder UI doesn't show vessel agent") and assumes that
hides it from the network. The raw response still carries it
in DevTools.

**Mitigation:** the adapter does **not** filter by role.
Sensitive-info redaction is server-side (Information
Architecture §13 + future role-scoped `/api/summary` in
V1.1+). The adapter does not pretend to do security.

### 18.7 Adapter divergence from backend evolution

**Risk:** backend ships a new field; adapter doesn't know
about it; UI fails to use the new capability for months.

**Mitigation:** adapter has an "unknown fields" log in
non-production builds — when `RawSummary` carries a key the
adapter doesn't reference, the adapter logs it. Quarterly
review of unknown-field logs surfaces backend additions.

---

## 19. Recommendations

### 19.1 An adapter is required before M1

Authorise the implementation of the adapter (per this contract)
as part of M1. M1 cannot ship without it.

### 19.2 No backend changes are required yet

`server.py` byte-identical. No new endpoints. No new fields.
The current `/api/summary` carries enough information for every
V1.0 surface that has a real-data source.

Backend evolution (apply-decision endpoint, `GET /api/audit`,
shift-event endpoint, role-scoped projection) is V1.x and is
**not** a precondition of M1. It is a precondition of later
milestones (M7, M8, V1.1+).

### 19.3 UX/UI handoff can be authorised as design input only

The handoff defines the **visual** contract (Horizon Dark
tokens, component hierarchy, DSW conceptual flow). It does
**not** define the **data** contract — this design note does.

When Tony authorises the handoff (separately), the authorisation
should explicitly cite:

- This design note as the data contract
- The Shape Spike as the source-shape reference
- The UX/UI Handoff Validation as the authoritative caveats

### 19.4 M0 can proceed after sandbox provisioning and explicit authorisation

M0 (per Scope Proposal §12) is the static scaffold. It can be
authorised once:

1. `horizon-v1-sandbox` Railway project is provisioned
2. UX/UI handoff is authorised as design input
3. This adapter design note is merged
4. Tony explicitly authorises M0 against Scope Proposal §12

**Key M0 deliverable update:** M0's `frontend/src/data/sample.js`
should be shaped as `ViewSummary` (the target shape defined in
this note), **not** the prototype's `data.js` shape. This
positions M0's components to swap in the adapter output during
M1 without rework.

### 19.5 M1 must not proceed without adapter implementation and tests

M1 acceptance must include:

- `frontend/src/api/horizon.js` + `adapters/` directory
- The mapping contract from §5–§14 implemented
- Fixture-based tests per §16
- All seven risk categories from §18 surfaced in the M1 PR
  description with mitigations referenced

If any of these is missing at M1 review, M1 is not authorised
to merge.

### 19.6 Adapter design note must be merged before M0 authorisation

Without this design note on `main`, M0's static `sample.js`
cannot be shaped to match the target view model — and rework
will be required during M1. Merging this note first eliminates
that rework.

---

## End of design note

**Status:** v0.1 design note — planning only
**Implementation status:** None
**Next action:** ChatGPT engineering review, then Tony's
decision on merging this note alongside the prior planning
corpus. Once merged, the document becomes the canonical data
contract for V1.0 frontend work and is cited by both the M0
authorisation (for `sample.js` shape) and the M1 authorisation
(for adapter implementation and tests).
