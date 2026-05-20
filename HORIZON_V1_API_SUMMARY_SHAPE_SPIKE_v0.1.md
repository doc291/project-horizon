# Horizon V1 — /api/summary Shape Verification Spike (v0.1)

**Status:** Verification spike — inspection-only, no code changes
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review)
**Authoritative inputs (merged on `main @ b381bce`):**
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` (PR #37)
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38)
- Seven prior V1 foundation documents on `main`
**Pending input (not yet authorised):**
- Claude Design UX/UI handoff in `v1-handoff/`
  (`V1-Implementation-Prompt.md`, `prototype/data.js`, `prototype/*.jsx`)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**Scope of this document:** static analysis of `server.py` to
derive the authoritative `/api/summary` response shape, compared
against the four artefacts the V1 frontend handoff and validation
work assume. Documentation/inspection only. **No code runs. No
HTTP request is made. No file outside this markdown is created.**

---

## 1. Executive Summary

The actual Beta 10 `/api/summary` response **does not match the
handoff assumptions**. The two shapes are **roughly addressing
the same domains** (vessels, berths, conflicts, weather, tides,
pilotage, etd risk) but **differ substantively in naming,
nesting, and several missing/added top-level keys**.

**Largest gaps:**

| Gap | Severity for M1 |
|---|---|
| Top-level key naming — `timestamp` vs `generated_at`, `dashboard_metrics` vs `dashboard`, `etdRisk` vs `etd_risk` | Low — pure rename, mechanical |
| Top-level shape — handoff documents `conditions` as a single block; real response splits into `weather` + `tides` | Medium — restructure |
| Handoff's `weather_alerts` does not exist as a top-level key — weather alerts are **merged into `conflicts`** with `signal_type: "WEATHER"` | Medium — frontend must filter |
| Prototype `data.js` shape (`fleet`, `alerts`, `decision`, `shiftLog`, `kpis`) **is materially different** from both the documented handoff shape AND the real response | High — `data.js` cannot be used as a drop-in mock unless adapted |
| Vessel object — handoff documents `mmsi`, `eta` (HH:MM string), `etd`, `risk_score`, `cargo`, `range_nm`, `sog`, `cog`; real response has `id`, `imo`, `vessel_type`, `loa`, `draught`, `cargo_type`, `eta` (ISO 8601 UTC), `etd` (ISO 8601 UTC), `ata`, `atd`, `lat`, `lon`, **but no `mmsi`, `sog`, `cog`, `risk_score` on the vessel object** (risk lives in `etd_risk`, not on the vessel) | High — adapter required |
| Conflict object — handoff documents `cascade`, `options` with `safety_score`/`delay_hours`/`cost_estimate`/`confidence`/`recommended`; real response uses `sequencing_alternatives`, `decision_support` (with nested `options`, `confidence`, `recommended_option_id`, `decision_deadline`), `resolution_options` (plain string list); shape is similar in spirit but **different field names and an extra nesting level** | High — adapter required for DSW |
| `dashboard_metrics` (handoff) vs `dashboard` + `port_status` (real, split across two top-level keys) | Medium |
| Several real top-level keys not in handoff: `port_status`, `port_tugs`, `port_gangs`, `port_geo`, `berth_utilisation`, `ukc`, `arrival_ukc`, `dukc`, `esg`, `port_profile`, `lookahead_hours`, `data_source`, `data_source_label`, `scraped_at` | Medium — frontend can ignore unknown keys, but the additional capability is significant |
| Time formats — handoff describes `"14:30"` (HH:MM strings); real response uses **ISO 8601 UTC strings** (e.g. `"2026-05-20T04:30:00+00:00"`) | High — every time-display component needs formatting |
| Conditions classification — handoff: `"EXCELLENT" \| "GOOD" \| "MODERATE" \| "POOR"`; real: `"Excellent" \| "Good" \| "Moderate" \| "Poor"` (title case) | Low — case normalisation |
| Severity strings — handoff: `"CRITICAL" \| "HIGH" \| "MEDIUM" \| "LOW"`; real: `"critical" \| "high" \| "medium" \| "low"` (lowercase) | Low — case normalisation |

**Is M0 affected?** **No.** M0 (per `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` §10) makes **zero API calls**. The shell renders against a static `sample.js` lifted from `v1-handoff/prototype/data.js`. M0 is unaffected by backend-shape gaps because M0 doesn't talk to the backend.

**Is M1 affected?** **Yes — materially.** M1 (App Shell + Auth + read-only `/api/summary` polling, per Execution Plan §11) requires a **frontend adapter layer** to translate the real response shape into whatever the React components expect. Without that layer, the components built against `data.js` shape will silently fail to render fields, double-render fields under different names, or display ISO-timestamp strings where HH:MM was expected.

**Recommendation:** before M1 is authorised, a small adapter design note (§12.3 below) must be produced, with an explicit per-domain mapping table. The adapter should live in `frontend/src/api/horizon.js` (or equivalent) as the **single normalisation seam**. Components should consume the adapted shape, not the raw response. No backend change is required.

---

## 2. Method

### 2.1 Capture method

**Static analysis of `server.py`** at `origin/main @ b381bce`, with cross-reference to `weather.py` and `port_profiles.py`. No HTTP request was made. No process was started. The authoritative answer for "what does `/api/summary` return?" is the return statement of `build_summary()` (`server.py:2462–2513`); all sub-shapes are determined by the helper functions referenced in that return.

**Rationale for static over runtime capture:**

- M0 Scope Proposal §7 forbids runtime changes; spawning a local server is not a *change* but it adds operational complexity without information gain — the Python source is the deterministic spec.
- Running the production endpoint would require authenticated session access and Tony-side cookie handling; the spike does not need to be at that overhead.
- Static analysis catches **all** branches (live AIS, MST fallback, QShips fallback, simulation fallback) including ones a live capture would miss.

### 2.2 Ports inspected

All four V1.0 ports inspected via static reading of `port_profiles.py`:

| Port | `sim_vessel_count` | UN/LOCODE | Lat/Lon source |
|---|---|---|---|
| BRISBANE | 13 | AUBNE | profile |
| MELBOURNE | 9 | AUMEL | profile |
| DARWIN | 7 | AUDRW | profile |
| GEELONG | 7 | AUGEX | profile |

All four use the **same `build_summary()` function and the same return shape**. Differences across ports are values, not shape (see §6).

### 2.3 Authenticated session required?

**Yes.** `server.py:2936–2964` shows `/api/summary` is gated by `_is_authenticated()`. Unauthenticated requests are redirected to `/login`. The auth path is the existing Beta 10 `horizon_session` cookie issued by `POST /login`. This is **unchanged from Beta 10** and not affected by anything in V1 planning so far.

### 2.4 Timestamp of capture (static)

Static analysis performed against `server.py` at commit `b381bce` (origin/main), 2026-05-20.

### 2.5 Data classification

The real response is a **mix** of live and simulated data:

- **Vessels:** preferred source is AISStream (live); falls back to MST cache (live); falls back to QShips scrape (live, Brisbane only); final fallback is `make_vessels()` simulation. Real responses include both live and simulated vessels — distinguished by `source` field on individual vessel objects.
- **Weather:** Open-Meteo live (when cache warm); simulated fallback. Source distinguished by `weather.source = "live" | "simulation"`.
- **Tides:** BOM live (when cache warm); cosine simulation fallback. Source via `port_profile.using_live_tidal_data`.
- **Conflicts:** always derived in-memory from vessels + berths + pilotage + towage + weather. Tagged `data_source: "live" | "simulated"` per conflict.
- **Berths, pilotage, towage, etd_risk, dashboard, ukc, dukc, esg:** always derived in-memory (no external source).

This **matters for M1**: the frontend must accept that any `/api/summary` response is mixed-source, with the source labelled per-object (vessels) or per-block (weather, tides). The handoff documents this only at the vessel level.

---

## 3. Actual /api/summary Top-Level Shape

From `server.py:2462–2513`, the return value of `build_summary()` has exactly these top-level keys (26 total):

```
port_name                  (string)
generated_at               (ISO 8601 UTC string)
lookahead_hours            (integer, currently 48)
data_source                (string: "live" | source-key like "qships" | "simulation")
data_source_label          (string, human-readable)
scraped_at                 (ISO 8601 string or null)
port_status                (object — 10 fields, see §3.1)
vessels                    (array of vessel objects, see §3.2)
berths                     (array of berth objects)
pilotage                   (array of pilotage event objects)
towage                     (array of towage event objects)
port_tugs                  (array — tug roster)
port_gangs                 (array — mooring gang roster)
conflicts                  (array of conflict objects — includes both
                            operational and weather-derived conflicts,
                            sorted by severity then time, see §3.3)
guidance                   (array of guidance objects)
port_geo                   (object — port geometry)
weather                    (object — 10 fields, see §3.4)
tides                      (object — tide state)
berth_utilisation          (array — berth utilisation rollup)
etd_risk                   (array of etd-risk objects, see §3.5)
dashboard                  (object — 11 KPI fields, see §3.6)
ukc                        (object — under-keel clearance current state)
arrival_ukc                (object — arrival-window UKC projections)
dukc                       (object — DUKC time-series)
esg                        (object — ESG data)
port_profile               (object — 12 fields including
                            active port id, display name, timezone,
                            live-data flags, available_ports list)
```

### 3.1 `port_status` (10 fields)

```
berths_occupied, berths_available, berths_total,
vessels_in_port, vessels_expected_24h, vessels_departing_24h,
active_conflicts, critical_conflicts,
pilots_available, tugs_available
```

### 3.2 Vessel object (per item in `vessels`)

```
id              (e.g. "V001")
name            (e.g. "MV Nordic Star")
imo             (string, deterministic 7-digit hash)
vessel_type     (string — "Container", "Bulk Carrier", "Tanker", etc.)
flag            (string)
loa             (number, metres)
draught         (number, metres)        ← real uses "draught", not "draft"
cargo_type      (string)
status          ("berthed" | "confirmed" | "scheduled" |
                 "at_risk" | "arrived" | "departed")
berth_id        (e.g. "B04")
eta             (ISO 8601 UTC)          ← real is ISO, handoff says HH:MM
etd             (ISO 8601 UTC)
ata             (ISO 8601 UTC or null)
atd             (ISO 8601 UTC or null)
pilotage_required (bool)
towage_required (bool)
agent           (string)
notes           (string or null)
lat             (number)
lon             (number)
source          (string, set by AIS/MST builders — e.g. "ais", "mst";
                 absent on simulated vessels from make_vessels)
```

**Fields the handoff lists that are NOT on the vessel object:** `mmsi`, `sog`, `cog`, `range_nm`, `cargo` (real uses `cargo_type`), `draft` (real uses `draught`), `risk_score` (real returns this in a **separate `etd_risk` array**, indexed by `vessel_id`), `type` (real uses `vessel_type`).

### 3.3 Conflict object (per item in `conflicts`)

From `server.py:919–938`:

```
id                        (8-char hash)
conflict_type             ("berth_overlap" | "berth_not_ready" |
                           "pilotage_window" | "tug_double_book" |
                           etc., plus weather-derived types)
signal_type               ("CONFLICT" | "WARNING" | "ADVISORY" | "WEATHER")
severity                  ("critical" | "high" | "medium" | "low")
                          ← lowercase, NOT uppercase as handoff says
vessel_ids                (array of vessel ids)
vessel_names              (array of vessel names)
berth_id                  (string or null)
berth_name                (string or null)
conflict_time             (ISO 8601 UTC)
description               (string)
resolution_options        (array of plain strings — high-level guidance)
sequencing_alternatives   (array of structured option objects —
                           handoff calls these "options" with
                           safety_score/delay/cost/confidence)
decision_support          (object — see §3.3.1)
data_source               ("live" | "simulated")
safety_score              ("Low" | "Medium" | "High" — added post-build)
```

Note: weather-derived conflicts share this shape and appear in the same `conflicts` array, tagged `signal_type: "WEATHER"`. **There is no separate top-level `weather_alerts` key.**

#### 3.3.1 `decision_support` (per `conflict`)

From `server.py:941–962`:

```
recommended_option_id   (string)
recommended_reasoning   (string)
confidence              ("high" | "medium")    ← string, not 0–1 fraction
decision_deadline       (ISO 8601 UTC)
options                 (array — duplicate of sequencing_alternatives)
```

**This is the DSW data shape.** It differs from the handoff's documented `conflict.options[].{safety_score, delay_hours, cost_estimate, confidence, recommended}` in **at least four ways:**

1. Options live under `conflict.decision_support.options`, not `conflict.options`
2. `confidence` is a string (`"high"` / `"medium"`), not a 0–1 fraction
3. `recommended` is conveyed by `recommended_option_id` matching the option's id, not a boolean flag on each option (though individual options may also carry `recommended: true` in `sequencing_alternatives`)
4. The handoff's `cascade` (PRIMARY / +1 / +2 tree) **does not exist** in the real response

### 3.4 Weather object

From `weather.py:79–91`:

```
wind_speed_kts            (integer)
wind_direction_deg        (integer)
wind_direction_label      (compass label, e.g. "ENE")
wind_beaufort             (integer)
swell_height_m            (number)
swell_period_s            (integer)
swell_direction_label     (string)
visibility_nm             (number)              ← nautical miles, NOT km
pressure_hpa              (integer)
conditions                ("Excellent" | "Good" | "Moderate" | "Poor")
source                    ("live" | "simulation")
```

**Units differ from handoff:** real uses **nautical miles** for visibility (`visibility_nm`); handoff documents `visibility_km`. Real uses **knots** for wind (matches handoff). Real has **no `wind_gust`**, no `pressure_trend`, no `sea_temp` at the weather block — those are handoff inventions.

### 3.5 `etd_risk` items

```
vessel_id      (string)
vessel_name    (string)
risk_score     (0–100 integer)
risk_level     ("low" | "medium" | "high" | "critical")
risk_factors   (array of human-readable strings)
```

Handoff's `etd_risk[].delta` (e.g. `"+2h 00m"`) **does not exist** in real response. Handoff's `etd_risk[].reason` is approximated by `risk_factors[0]` (an array, not a string).

### 3.6 `dashboard` (handoff calls this `dashboard_metrics`)

From `server.py:1912–1924`:

```
berth_utilisation_pct      (integer)
forecast_utilisation_48h   (integer)
on_time_departure_pct      (integer)
avg_dwell_hours            (number)
vessels_at_risk            (integer)
active_conflicts           (integer)
critical_conflicts         (integer)
pilot_ops_12h              (integer)
tug_ops_12h                (integer)
vessels_in_port            (integer)
vessels_expected_24h       (integer)
```

Handoff documents only **4 fields** (`vessels_in_port`, `movements_6h`, `active_conflicts`, `berth_utilisation`). Real has **11 fields**, and `movements_6h` does not exist (real uses `pilot_ops_12h` and `tug_ops_12h` instead).

---

## 4. Comparison Against V1-Implementation-Prompt.md

### 4.1 Top-level domain table

| Handoff domain (V1-Implementation-Prompt.md §2) | Real response | Classification |
|---|---|---|
| `port` | not present at top level; `port_profile.id` holds the active port id | **Renamed / nested** |
| `port_name` | `port_name` | **Matches** |
| `unlocode` | not at top level; lives inside `port_profile` via `profile["unloco"]` — confirmed NOT in the response | **Missing** |
| `timestamp` | `generated_at` | **Renamed** |
| `conditions` (single block, with wind/swell/visibility/pressure/tide/sea_temp) | split into `weather` (without tide/sea_temp) + `tides` (separate top-level) | **Structurally different** |
| `vessels` | `vessels` | **Matches** (top-level) but item shape differs — see §4.2 |
| `berths` | `berths` | **Matches** (top-level); item shape close but `eta_next` is handoff invention |
| `conflicts` | `conflicts` | **Matches** (top-level) but item shape differs — see §4.3 |
| `guidance` | `guidance` | **Matches** (top-level); item shape needs verification |
| `weather_alerts` | not a top-level key; weather alerts are merged into `conflicts` with `signal_type: "WEATHER"` | **Missing as top-level / merged** |
| `pilotage` | `pilotage` | **Matches** (top-level); item shape close but differs in field names |
| `towage` | `towage` | **Matches** (top-level) |
| `dashboard_metrics` | `dashboard` | **Renamed**; field set differs — see §3.6 |
| `etd_risk` | `etd_risk` | **Matches** (top-level); item shape differs — see §3.5 |

### 4.2 Vessel object (per `vessels[*]`)

| Handoff field | Real field | Classification |
|---|---|---|
| `vessel_id` | `id` | **Renamed** |
| `name` | `name` | Matches |
| `imo` | `imo` | Matches |
| `mmsi` | (not on vessel object) | **Missing** |
| `type` | `vessel_type` | **Renamed** |
| `status` (inbound/berthed/anchorage/departing) | `status` (berthed/confirmed/scheduled/at_risk/arrived/departed) | **Different vocabulary** |
| `source` (ais/mst/sim) | `source` (set only on AIS/MST vessels) | **Partial — absent on simulated** |
| `eta` (HH:MM) | `eta` (ISO 8601 UTC) | **Different format** |
| `etd` (HH:MM or null) | `etd` (ISO 8601 UTC) | **Different format** |
| `berth` | `berth_id` | **Renamed** |
| `draft` | `draught` | **Renamed (spelling)** |
| `loa` | `loa` | Matches |
| `beam` | (not present) | **Missing** |
| `range_nm` | (not present) | **Missing** |
| `sog` | (not present) | **Missing** |
| `cog` | (not present) | **Missing** |
| `lat` | `lat` | Matches |
| `lon` | `lon` | Matches |
| `cargo` | `cargo_type` | **Renamed** |
| `agent` | `agent` | Matches |
| `flag` | `flag` | Matches |
| `risk_score` | (lives in `etd_risk[].risk_score`, NOT on vessel) | **Relocated** |

**Severity:** vessel-object adapter is high-priority before M1.

### 4.3 Conflict object (per `conflicts[*]`)

| Handoff field | Real field | Classification |
|---|---|---|
| `conflict_id` | `id` | **Renamed** |
| `type` | `conflict_type` | **Renamed** |
| `severity` (CRITICAL/...) | `severity` (critical/...) | **Case difference** |
| `title` | (not present — handoff convention) | **Missing** (frontend must derive from description / type) |
| `description` | `description` | Matches |
| `vessels` (ids array) | `vessel_ids` | **Renamed** |
| `berth` | `berth_id` | **Renamed** |
| `window_start` | (not present — conflict_time is the trigger) | **Missing / different semantics** |
| `window_end` | (not present) | **Missing** |
| `deadline_minutes` | (derived from `decision_support.decision_deadline` — ISO timestamp, not minutes) | **Different representation** |
| `data_source` | `data_source` | Matches |
| `cascade[]` (PRIMARY / +1 / +2) | (not present) | **Missing** — handoff invention |
| `options[]` with safety_score/delay_hours/cost_estimate/confidence/recommended | `decision_support.options[]` and `sequencing_alternatives[]`, with `safety_score`/`delay`/`cost`/`confidence`/`recommended_option_id` | **Renested + renamed + reshaped** |

### 4.4 Conditions block

Handoff documents a single `conditions` object with all of: rating, wind_speed, wind_gust, wind_dir, wind_bearing, swell_height, swell_period, swell_dir, visibility_km, visibility_label, pressure_hpa, pressure_trend, sea_temp, tide_height, tide_state, tide_next, tide_next_label, tide_min, tide_max.

Real response splits this:

- **Weather (`/api/summary.weather`):** wind_speed_kts, wind_direction_deg, wind_direction_label, wind_beaufort, swell_height_m, swell_period_s, swell_direction_label, visibility_nm, pressure_hpa, conditions (this is the "rating"), source.
- **Tides (`/api/summary.tides`):** separate top-level block; field names need own audit (not exhaustively inspected here — recommended follow-up below).

**Missing from real:** `wind_gust`, `pressure_trend`, `sea_temp`. (Sea temp may be in the `tides` block — recommended verification.)

**Unit mismatch:** `visibility_km` (handoff) vs `visibility_nm` (real).

### 4.5 `etd_risk` items

| Handoff | Real | Classification |
|---|---|---|
| `vessel` (name) | `vessel_name` | **Renamed** |
| `risk` (number) | `risk_score` | **Renamed** |
| `reason` (single string) | `risk_factors` (array of strings) | **Different cardinality** |
| `delta` (e.g. "+2h 00m") | (not present) | **Missing** |
| — | `vessel_id` | **Added** |
| — | `risk_level` (categorical) | **Added** |

### 4.6 `dashboard_metrics` / `dashboard`

See §3.6. Real has 11 fields; handoff documents 4. `movements_6h` (handoff) does not exist; closest real equivalents are `pilot_ops_12h` and `tug_ops_12h` (different window).

---

## 5. Comparison Against prototype/data.js

`data.js` (188 lines, read in full during the UX/UI Handoff Validation) exports a single global `window.HZ_DATA` with these top-level keys:

```
fleet, conditions, alerts, decision, shiftLog, berths,
pilotage, kpis, etdRisk
```

These are the names the React prototype components consume.

| `data.js` field | Real `/api/summary` field | Classification |
|---|---|---|
| `fleet[*]` | `vessels[*]` | **Renamed at array level** |
| `fleet[*].risk` | `etd_risk[*].risk_score` (separate array) | **Moved off vessel** |
| `fleet[*].pilot` ("CONF" / "PEND" / "—") | (not on vessel object — pilotage events live in separate `pilotage` array) | **Missing on vessel** |
| `fleet[*].variance` (e.g. "+0:23") | (not present — derive from `eta` vs original schedule) | **Missing** |
| `fleet[*].sog` | (not present) | **Missing** |
| `fleet[*].range` | (not present) | **Missing** |
| `fleet[*].source` ("AIS" / "SIM") | `vessels[*].source` (lowercase, only on AIS/MST sources) | **Case + presence difference** |
| `conditions` (one object with `ukc`, `seaTemp`, nested wind/swell/tide/visibility/pressure) | split across `weather` + `tides` + `ukc` (top-level) | **Heavy restructure** |
| `alerts[*]` (with category, severity, title, detail, deadline, ts) | partially in `conflicts[*]` (where signal_type ∈ WARNING/ADVISORY/WEATHER); some content in `guidance[*]` | **No clean mapping** |
| `decision` (single object — same as DSW's data) | first/critical entry from `conflicts[*]` with `decision_support` attached | **Different shape** |
| `decision.cascade[]` (PRIMARY/+1/+2) | (does not exist anywhere in real response) | **Missing** |
| `decision.options[*]` (with safety/delay/cost/confidence/recommended/impactSummary/resource) | `conflicts[*].decision_support.options` (matching subset; no `impactSummary` or `resource`) | **Subset / different field names** |
| `decision.deadlineSec` (integer seconds) | derived from `decision_support.decision_deadline` (ISO timestamp) | **Different format** |
| `decision.isNew` (bool) | (not present) | **Missing** |
| `shiftLog[*]` | **NO equivalent in real response** | **Missing** — entire concept |
| `berths[*]` (segments with start/end percentage) | `berths[*]` (different shape — occupancy state, not Gantt percentages) | **Different shape** |
| `pilotage[*]` (time, vessel, from, to, pilot, tugs, status) | `pilotage[*]` (vessel_id, scheduled_time, pilot_name, direction, etc.) | **Different field names** |
| `kpis.operations[*]`, `kpis.performance[*]`, `kpis.safety[*]` | `dashboard` (single flat object) + `port_status` (counts) | **Different aggregation** |
| `etdRisk[*]` (camelCase, with vessel/risk/reason/delta) | `etd_risk[*]` (snake_case, different fields) | **Renamed + reshaped** |

**Single biggest concern:** `shiftLog` (the Shift Log tab) **has no backend source today**. The prototype's Shift Log is entirely fabricated. The Beta 10 backend does not emit shift events, does not track shift state, does not have a handover concept. The Shift Log tab cannot ship in V1.0 with real data unless backend work is added — and that work is V1.x per the Operational Workflow Model.

---

## 6. Port-by-Port Differences

**Shape: identical across all four ports.** Same `build_summary()` function, same top-level keys, same nested shapes.

**Values that vary by port** (per `port_profiles.py`):

| Port | `sim_vessel_count` | Compulsory towage LOA | Pilots available | Tugs available |
|---|---|---|---|---|
| BRISBANE | 13 | (per profile) | per profile | per profile |
| MELBOURNE | 9 | per profile | per profile | per profile |
| DARWIN | 7 | per profile | per profile | per profile |
| GEELONG | 7 | per profile | per profile | per profile |

**QShips data source applies only to BRISBANE** (`server.py:2355`). The other three ports always fall through to AISStream → MST → simulation. This means BRISBANE may carry `data_source: "qships"` where the others can only carry `"live"`, `"simulation"`, or the AIS sources.

**Live-data availability** depends on:

- AISStream API key configured + cache fresh
- MST cache populated
- BOM weather cache populated
- BOM tide cache populated
- Open-Meteo weather cache populated

If none of the above are warm, the response will be entirely simulated, with `using_live_*` flags all `false` in `port_profile`. **In Beta 10 production today the production `DATABASE_URL` is unset and Stage E-prod is paused**, but the **data-source caches are independent of the audit DB** — production demos run on a mix of live AIS + simulated fallbacks regardless.

**M1 implication:** the frontend must handle "all simulated" gracefully — every domain may show `source: "simulation"` and the UI must still render without alarming the operator.

---

## 7. Data Quality / Source Differences

### 7.1 AIS vs MST vs SIM (vessel layer)

Per `server.py:2295–2369`, the vessel source priority is:

1. **AISStream** (live WebSocket) — vessels carry `source: "ais"`
2. **MST cache** (live, AIS via MST) — vessels carry `source: "mst"`
3. **QShips scrape** (live, Brisbane only) — vessels may not carry an explicit `source`
4. **Simulation** (`make_vessels()`) — vessels carry no `source` field

The handoff's prototype `data.js` uses `source: "AIS"` / `"SIM"` (uppercase). The real response uses `source: "ais"` / `"mst"` (lowercase), absent on simulated vessels. **Adapter must normalise** case and default missing `source` to a "sim" or "unknown" indicator.

### 7.2 Missing values

- `etd` may be null on vessels that haven't been assigned a departure time
- `ata` is null until the vessel berths
- `atd` is null until the vessel departs
- `berth_id` may be null on inbound vessels not yet assigned
- `eta_next` (handoff invention) is not in the real berth object — the next vessel must be derived by sorting vessels by ETA filtered to that berth

### 7.3 Nullable fields

Many fields are nullable; the frontend must defensively handle `null` for: `eta`, `etd`, `ata`, `atd`, `berth_id`, `berth_name`, `notes`, `scraped_at`, `bom_station_id`, plus all conflict-side optional fields (`sequencing_alternatives` may be empty, `decision_support` may be `None` for non-berth-overlap conflicts).

### 7.4 Inconsistent types

- `safety_score` (post-attached to conflicts) is a **string** (`"Low"`/`"Medium"`/`"High"`); handoff `safety_score` (on options) is an **integer** (0–100). Same word, different meaning.
- `confidence` in `decision_support` is a **string** (`"high"`/`"medium"`); handoff `confidence` on options is **a fraction (0–1)** (e.g. `0.88`). Same word, different type.
- `severity` in conflicts is **lowercase**; handoff documents uppercase.

### 7.5 Timestamp formats

- Real: ISO 8601 UTC (`"2026-05-20T04:30:00+00:00"` style, produced by `fmt(now)`)
- Handoff documented shape: short HH:MM strings (e.g. `"14:30"`)
- Prototype `data.js`: HH:MM strings AND `deadlineSec` (integer seconds)

**Every time-display component needs format normalisation in the adapter.** Local timezone display is also required (each port profile carries a `timezone` field; the frontend must convert UTC to the port's local time for display).

### 7.6 Units

| Quantity | Handoff | Real |
|---|---|---|
| Wind speed | knots | knots — **matches** |
| Wind gust | knots | not present |
| Swell height | metres | metres — **matches** |
| Swell period | seconds | seconds — **matches** |
| Visibility | **kilometres** | **nautical miles** — **mismatch** |
| Pressure | hPa | hPa — **matches** |
| Sea temperature | °C | not present |
| LOA | metres (implicit) | metres — **matches** |
| Draft / Draught | metres | metres — **matches** |
| Range to port | nautical miles | not present |
| SOG | knots | not present |
| Tide height | metres | metres (likely — `tides` block not exhaustively inspected) |

**One unit mismatch (visibility) requires conversion.** Several handoff fields don't exist (gust, sea temp, range, SOG, COG, beam).

---

## 8. Adapter Requirements

A frontend adapter — recommended location `frontend/src/api/horizon.js` — should provide the following normalisations. **Every per-component data consumer should call into the adapter, never the raw API.**

### 8.1 Field renames (top level)

```
generated_at        → timestamp
dashboard           → dashboardMetrics
```

(or update components to use the real names; adapter direction is a M1 design choice.)

### 8.2 Field renames (vessel)

```
id                  → vesselId
vessel_type         → type
draught             → draft
cargo_type          → cargo
berth_id            → berth
```

### 8.3 Field renames (conflict)

```
id                                  → conflictId
conflict_type                       → type
vessel_ids                          → vessels
berth_id                            → berth
decision_support.options            → options
```

### 8.4 Defaults

| Field | Default when absent |
|---|---|
| `source` on vessel | `"sim"` |
| `notes` | `null` (preserve) |
| `eta`, `etd`, `ata`, `atd` | `null` (preserve; UI shows "—") |
| `berth_id` | `null` (UI shows "Unassigned") |
| `cascade` (handoff field) | `[]` (does not exist in real response — frontend should render without it) |

### 8.5 Derived values

| Derived field | Computation |
|---|---|
| `vessel.risk_score` | look up by `vessel.id` in `etd_risk[*].risk_score`; default 0 |
| `vessel.eta_local` | convert `vessel.eta` UTC to `port_profile.timezone` |
| `vessel.eta_short` | format `vessel.eta` as HH:MM in port-local TZ |
| `conflict.title` | derive from `conflict_type` + `vessel_names` (e.g. "Berth overlap: PACIFIC VOYAGER vs BRISBANE STAR") |
| `conflict.deadline_minutes` | compute from `decision_support.decision_deadline` minus now |
| `conditions.rating` | promote `weather.conditions` |
| `conditions.visibility_km` | convert `weather.visibility_nm * 1.852` |
| `decision.deadlineSec` | convert `decision_deadline` ISO to seconds-until |

### 8.6 Arrays vs objects

The prototype's `decision` is a single object; the real response's `conflicts` is an array. The adapter must:

- Select the **first critical conflict** (or first conflict if no critical) as the "active decision"
- Provide both the full `conflicts[]` array (for left-panel alert list) and a `decision` object derived from `conflicts[0]` for the DecisionCard / DSW

### 8.7 Severity / status mappings

```
real severity                → handoff severity
"critical"                   → "CRITICAL"
"high"                       → "HIGH"
"medium"                     → "MEDIUM"
"low"                        → "LOW"

real conditions              → handoff rating
"Excellent"                  → "EXCELLENT"
"Good"                       → "GOOD"
"Moderate"                   → "MODERATE"
"Poor"                       → "POOR"

real vessel status           → handoff vessel status
"berthed"                    → "berthed"
"confirmed"                  → "inbound" (?? requires PM decision)
"scheduled"                  → "inbound"
"at_risk"                    → "inbound" (?? add at-risk decoration)
"arrived"                    → "berthed" (terminal)
"departed"                   → "departing" or excluded
```

**Mapping decision required from the operational stakeholders** — `confirmed` / `scheduled` / `at_risk` do not map cleanly to the handoff's 4-status vocabulary (`inbound` / `berthed` / `anchorage` / `departing`). Either expand the handoff statuses or collapse the real ones (with information loss).

### 8.8 Time formats

- Adapter converts all ISO timestamps to:
  - A `Date` object (for arithmetic)
  - A port-local HH:MM string (for display)
  - A relative-time string (for countdowns, e.g. "in 2h 14m")

### 8.9 Source indicators

Adapter exposes per-block source booleans (`isVesselLive`, `isWeatherLive`, `isTideLive`) plus an overall `isLive` derived from `port_profile.using_live_vessel_data`.

### 8.10 Fields with no backend source (handle in UI)

- `shiftLog` — **no backend source today**. Either omit the Shift Log tab from V1.0, or populate from a separate (not-yet-built) `GET /api/shift-events` endpoint added in V1.x.
- `decision.cascade` (PRIMARY / +1 / +2) — not in backend. Either remove from the DSW step 2 or compute heuristically from related conflicts in the same time window (engineering judgement required).
- `audit log` (right-panel right tab) — no `GET /api/audit` endpoint exists. V1.0 may show session-only events; backend audit retrieval is V1.x per Execution Plan §11 M8.

---

## 9. M0 Impact

**None.**

Per `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md`:

- §10 (API Usage Scope): "M0 makes ZERO HTTP calls to the Beta 10 backend."
- §9 (Frontend Scaffold Scope): "**No `frontend/src/api/`** — no API client in M0."
- §2 (What M0 IS): "static rendering from a sample data file lifted from the handoff prototype — no live `/api/summary` call yet."

M0 renders from `frontend/src/data/sample.js`. The shape of `sample.js` can be **whatever the components are designed against** — likely the prototype's `data.js` shape, since the components are built from `panels.jsx`, `tabs.jsx`, etc. **M0 is unaffected by backend-shape gaps.**

**Confirmed:** M0 can proceed (once otherwise authorised) independently of backend shape. The adapter work is a **prerequisite for M1, not M0**.

---

## 10. M1 Impact

**M1 is materially affected.** Before M1 (App Shell + Auth + read-only `/api/summary` polling) can be authorised, the following must be resolved:

1. **Adapter design note.** A small follow-up document (or part of M1's authorisation note) that codifies §8 above into an adapter contract — what gets renamed, what gets derived, what defaults. The adapter is the **single seam** between Beta 10's response and the React components.

2. **Status-vocabulary decision.** The real `status` set (`berthed` / `confirmed` / `scheduled` / `at_risk` / `arrived` / `departed`) does not map cleanly to the handoff's (`inbound` / `berthed` / `anchorage` / `departing`). Either the V1 frontend adopts the backend vocabulary (likely safer), or a mapping with information loss is accepted. **Tony / operational stakeholders decide.**

3. **`shiftLog` decision.** Either drop the Shift Log tab from V1.0, or accept that it shows fabricated / session-only data until a backend shift-event endpoint is built (V1.x).

4. **`cascade` decision.** Either drop DSW step 2 (Downstream impact) for V1.0, or accept that it's computed heuristically by the frontend (no backend source).

5. **Audit log decision.** Either drop the right-panel Audit Log tab for V1.0, or accept it showing session-only events.

6. **Time-zone decision.** Confirm that the adapter converts `port_profile.timezone` — and clarify which timestamps in the response are UTC vs local. (`fmt()` in `server.py` writes UTC; this is implicit in the response.)

7. **Severity / rating case normalisation.** Already a small detail; adapter handles it but the **decision** is that the frontend internally uses UPPERCASE (per handoff) and the adapter converts on the way in.

These six decisions are blocking for M1, not for M0.

---

## 11. Risks

### 11.1 Prototype overfitting to assumed data

**Risk:** components built directly against `data.js` shapes will silently fail on real data. Fields will be `undefined`; values will not render; conditional rendering may show empty placeholders without errors.

**Mitigation:** every component documents its **prop contract**; the adapter is the single point of normalisation; an M1 acceptance criterion is "no component renders `undefined` from real response data on any of the four ports."

### 11.2 UI fields with no backend source

**Risk:** Shift Log, decision cascade, audit log, vessel.sog/cog/range, vessel.mmsi all have no backend source today. Hard-coding placeholders or showing them blank gives operators a false sense that the data is available but stale.

**Mitigation:** §10 #3, #4, #5 — either drop the surfaces or label them explicitly ("Shift Log — coming in V1.x") so operators are not misled.

### 11.3 Misleading decision / audit states

Already captured in the UX/UI Handoff Validation §13.3 (false sense of audit completion). The shape spike confirms it: there is **no `apply-decision`** endpoint, no audit-emission for operator commits in production, no `GET /api/audit`. The DSW commit flow must not claim "audit trail preserved" until M7 / V1.x.

### 11.4 Inconsistent port data

**Risk:** Brisbane may carry `data_source: "qships"` and the others `"simulation"`. A component that displays "Live: AIS" on the assumption AISStream is always primary will show inconsistent labels across ports.

**Mitigation:** rely on `port_profile.using_live_vessel_data` (boolean) for the live-data indicator, not on the data_source label.

### 11.5 Silent frontend failures

**Risk:** the prototype assumes presence of `decision`, `alerts`, etc. — the real `/api/summary` may legitimately return zero conflicts, zero guidance items, or an entirely simulated state. The shell must render gracefully for "quiet" port states.

**Mitigation:** M1 acceptance criterion includes rendering against an empty-conflicts response (e.g. via a what-if scenario or a quiet simulation seed).

### 11.6 ISO timestamp parsing pitfalls

**Risk:** the real response uses ISO 8601 with explicit `+00:00`. Naïve `new Date(...)` parsing in JS works, but display formatting must explicitly convert to the port's timezone (each port profile carries one). Off-by-an-hour bugs at AEST/AEDT boundaries are common.

**Mitigation:** adapter uses a date library (likely `date-fns-tz` or `luxon`) — add to the M1 dependency budget; this means M1 dependency budget exceeds M0's (React, ReactDOM, Vite, plugin) and **needs explicit M1 authorisation**.

### 11.7 Adapter as architectural creep

**Risk:** the adapter grows into a domain layer (e.g. "Conflict" class with methods). Then it becomes a parallel domain model that has to be kept in sync with both the backend and the components.

**Mitigation:** adapter is **purely transformational**, no methods, no class hierarchy. Input: raw API JSON. Output: normalised JSON. Pure functions only. This is a §10 #1 design constraint in the M1 authorisation.

---

## 12. Recommendations

### 12.1 Confirm a frontend adapter layer IS required

**Yes — required for M1.** The gap between the real response and the prototype's assumed shape is large enough that components built against the prototype shapes will not render real data correctly without normalisation. The adapter is a small, well-bounded module (~200–400 lines) that pays for itself many times over by isolating the backend evolution from the component code.

### 12.2 Confirm M0 can proceed

**Yes — M0 is unaffected.** M0 makes no API calls; M0 renders against static `sample.js`. The adapter is M1's concern.

### 12.3 Confirm M1 requires an adapter spike

**Yes.** Before M1 is authorised, a small adapter design note must be produced (likely a single markdown file, similar in size to this spike — ~600–900 lines). That note codifies §8 (the renames, defaults, derivations, type conversions, status mapping, severity mapping) into a concrete adapter contract. Once the contract is set, the adapter implementation itself is a single small PR inside M1.

### 12.4 Confirm whether the UX/UI handoff needs amendments

**Yes — five amendments documented in the UX/UI Handoff Validation §9 still stand. This spike adds two more:**

- §9.9 (new): the handoff's documented `/api/summary` shape is **not** the real Beta 10 shape — annotate the handoff (or this spike's findings) so future readers do not treat V1-Implementation-Prompt.md §2 as authoritative for the API. Either update the handoff in place (preferred) or carry this spike alongside it as the canonical correction.

- §9.10 (new): `vessel.status` vocabulary mismatch requires a design decision (see §10 #2). The handoff's 4-state vocabulary cannot be mapped from the real 6-state vocabulary without information loss.

### 12.5 Confirm whether any backend change is required yet

**No.** The backend is correct for Beta 10 production. **All gaps are resolved on the frontend** via the adapter. **No `server.py` change is required for M0 or M1.**

Backend changes are anticipated in **V1.x** for:

- A `GET /api/audit` endpoint (M8 — Execution Plan §11)
- A shift-event endpoint (for the Shift Log tab) (V1.x)
- A decision-commit endpoint (DSW step 4 / 5) (M7 — Execution Plan §11)
- Role-scoped projection of `/api/summary` (V1.1+ per Implementation Strategy §17)

None of these are required to begin M0 or M1.

### 12.6 Recommended next steps in sequence

1. ChatGPT engineering review of this spike (this PR — when opened)
2. Merge this spike to `main` after review
3. Authorise a small adapter design note (single markdown PR, similar pattern to this spike)
4. Provision `horizon-v1-sandbox` Railway project (Tony-side)
5. Authorise M0 implementation citing M0 Scope Proposal §12 (the static-sample-data scaffold)
6. Authorise M1 implementation citing this spike + the adapter design note (the live-API integration with adapter)

---

## End of spike

**Status:** v0.1 shape verification spike — inspection-only
**Implementation status:** None
**Next action:** ChatGPT engineering review, then Tony's decision on (a) merging this spike alongside the prior planning corpus, and (b) commissioning an adapter design note before M1.
