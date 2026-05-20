# Horizon V1 — M1 Implementation Plan (v0.1)

**Status:** Implementation plan — planning only, no implementation
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review), future M1 implementer
**Authoritative inputs (on `main @ 01fbd61`):**
- `HORIZON_V1_M1_SCOPE_PROPOSAL_v0.1.md` (PR #47) — M1 scope contract
- `HORIZON_V1_M0_RETROSPECTIVE_v0.1.md` (PR #46) — lessons + cycle pattern
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40) — adapter contract
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` (PR #39) — source-shape truth
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43) — UX rules
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md` (PR #44) — lifecycle treatment
- `HORIZON_V1_SANDBOX_PROVISIONING_PLAN_v0.1.md` (PR #42) — sandbox topology
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38) + M0 Implementation Plan v0.2 — cycle template
**M0 close commit:** `a1161cf` (PR #45 merge)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise M1 implementation.** It is the
concrete implementation plan for review. M1 code starts only on
Tony's explicit authorisation message citing this document.

---

## 1. Executive summary

This is the concrete implementation plan for **M1 — Adapter-
backed read-only operational data pipeline with fixture
backing** (per `HORIZON_V1_M1_SCOPE_PROPOSAL_v0.1.md`).

M1 introduces three new file trees under `frontend/`:

- `frontend/src/api/` — adapter modules + public entry point
  (~10 files)
- `frontend/src/hooks/useSummary.js` — polling hook
- `frontend/fixtures/` — captured `/api/summary` JSON snapshots
  (5 files)

M1 also extends three existing M0 surfaces (`HorizonHeader`,
`ConditionsBar`, `CenterPanel` partial Dashboard) to consume
adapter output instead of the static `sample.js`. The `sample.js`
file is **kept** as a reference for the M0 demo but is no longer
imported by M1 components at runtime.

M1 emphatically **does not** call any live backend. The fetch
wrapper targets the sandbox's own static `/fixtures/*.json`
endpoint (same-origin to the sandbox URL). No CORS, no auth, no
cross-domain cookie, no `credentials: 'include'`. Live Beta 10
integration is **M2+ scope** conditional on same-origin / auth /
RBAC questions being resolved.

The dependency budget adds two packages (`date-fns-tz` runtime,
`vitest` dev) plus one peer (`@vitest/ui` optionally for local
test UX). `serve` stays. Root `railway.toml` stays untouched.
`frontend/railway.json` stays untouched (the M0 build / start
commands continue to apply). The sandbox source branch switches
from `feat/v1-m0` to `feat/v1-m1` via a single Tony-side Railway
dashboard action between Phase 1 and Phase 2 of M1.

M1 closes when:

- Adapter implementation matches Adapter Note §5–§14 contract
- Vitest unit tests pass against all 5 fixtures (snapshot +
  assertions + empty-state + defence + round-trip per §16.3)
- Polling hook handles tab-visibility / backoff / last-known-
  good caching
- HorizonHeader + ConditionsBar + partial Dashboard tab consume
  adapter output
- Sandbox URL serves the M1 build with periodic `/fixtures/*`
  polls visible in browser devtools
- All Beta 10 protection invariants from M0 held
- Tony post-merge Beta 10 visual check passes

This document is the contract for that work.

---

## 2. Resolved decisions

From M1 Scope Proposal §19, Tony has resolved:

| # | Decision | Resolved value |
|---|---|---|
| 1 | Date library | **`date-fns-tz`** (smaller bundle than luxon; tree-shakable; pairs with `date-fns`) |
| 2 | Test framework | **Vitest** (Vite-native; minimal config; jsdom not required for pure-function adapter tests) |
| 3 | Fixture capture method | **Manual `curl` from Beta 10 production**; Tony-side prep step |
| 4 | Fixture count | **5 total** — 3 captured (Brisbane busy, Brisbane quiet, Melbourne sim) + 2 derived (null-fields, malformed) |
| 5 | Polling interval | **30 seconds** default; configurable via `useSummary({ pollInterval })` |
| 6 | Berth heatmap | **Summary line only** in M1 (e.g. "Currently 88% occupied (7/8); next 48h forecast 74%"); full 18×8 heatmap deferred to M2 |
| 7 | `serve` removal | **Leave in place**; works correctly today, removal is cosmetic and can happen any time |
| 8 | Project-scoped `RAILWAY_TOKEN` | **Optional governance hygiene**; not blocking M1; workspace-scoped OAuth from M0 continues to satisfy current verification needs |

All eight decisions are encoded throughout this plan.

---

## 3. Intended branch name

**`feat/v1-m1`** — the M1 working branch per Sandbox Provisioning
Plan §6.1 + M0 Retrospective §9.2 Option B.

Branched off **`origin/main @ 01fbd61`** (the M1 Scope Proposal
merge commit).

Lifecycle:

- Long-lived through M1 build
- Receives all M1 commits (likely 3 to 6 commits — fixture
  capture + adapter modules + polling hook + UI surfaces +
  tests + DesignVerificationSwatch removal cleanup)
- Sandbox auto-deploys from this branch **after** Tony's
  source-branch switch in Railway dashboard (Phase 2)
- Merges to `main` only after M1 acceptance + Tony merge
  authorisation
- **Preserved on remote** post-merge (not `--delete-branch`)
  if sandbox continues to deploy from it for downstream
  verification before M2 begins. Deleted whenever Tony decides
  to switch sandbox to a future `feat/v1-m2` branch

The existing `feat/v1-m0` branch (still at `1f5447dc`) remains
on remote as the M0 reference point and the current sandbox
source until Tony's source-branch switch.

---

## 4. File / folder scaffold

All new files inside `frontend/`. Nothing outside `frontend/` is
modified.

### 4.1 New directories and files

```
frontend/
├── src/
│   ├── api/                                  ← NEW
│   │   ├── horizon.js                        ← public entry
│   │   └── adapters/                         ← NEW
│   │       ├── summaryAdapter.js             ← top-level
│   │       ├── conditionsAdapter.js          ← weather + tides + ukc
│   │       ├── vesselAdapter.js              ← vessels + risk join
│   │       ├── conflictAdapter.js            ← conflicts (cascade: null)
│   │       ├── guidanceAdapter.js            ← guidance + alert ordering
│   │       ├── dashboardAdapter.js           ← dashboard metrics
│   │       ├── etdRiskAdapter.js             ← etd_risk
│   │       ├── time.js                       ← ISO + port-local helpers
│   │       ├── status.js                     ← severity / status mappings
│   │       └── units.js                      ← nm ↔ km
│   ├── hooks/                                ← NEW
│   │   └── useSummary.js                     ← polling hook
│   └── ... (existing M0 files extended; see §10)
├── fixtures/                                 ← NEW
│   ├── brisbane-busy.json                    ← captured
│   ├── brisbane-quiet.json                   ← captured
│   ├── melbourne-sim.json                    ← captured
│   ├── null-fields.json                      ← derived
│   └── malformed.json                        ← derived
├── tests/                                    ← NEW
│   ├── setup.js                              ← Vitest setup
│   └── adapters/
│       ├── summaryAdapter.test.js
│       ├── conditionsAdapter.test.js
│       ├── vesselAdapter.test.js
│       ├── conflictAdapter.test.js
│       ├── guidanceAdapter.test.js
│       ├── dashboardAdapter.test.js
│       ├── etdRiskAdapter.test.js
│       ├── time.test.js
│       ├── status.test.js
│       └── units.test.js
├── vitest.config.js                          ← NEW (or inline in vite.config.js)
└── package.json                              ← MODIFIED (deps + scripts)
└── package-lock.json                         ← MODIFIED (auto)
```

### 4.2 Existing M0 files modified

```
frontend/
├── src/
│   ├── App.jsx                               ← MODIFIED — pass useSummary() output to children
│   ├── layout/
│   │   ├── HorizonHeader.jsx                 ← MODIFIED — adapter-backed stat tiles
│   │   ├── ConditionsBar.jsx                 ← MODIFIED — adapter-backed tiles + "Last update" tile
│   │   ├── CenterPanel.jsx                   ← MODIFIED — partial Dashboard tab (§10.4)
│   │   └── LeftPanel.jsx                     ← MODIFIED — remove DesignVerificationSwatch; placeholder remains
│   └── data/
│       └── sample.js                         ← UNCHANGED — kept as reference; no longer imported at runtime
```

### 4.3 Files NOT touched

```
frontend/
├── package.json                              ← MODIFIED (above)
├── package-lock.json                         ← MODIFIED (above)
├── vite.config.js                            ← MODIFIED if vitest config goes here (otherwise unchanged)
├── index.html                                ← UNCHANGED
├── .gitignore                                ← UNCHANGED
├── railway.json                              ← UNCHANGED (M0 build/start commands still correct)
└── src/
    ├── main.jsx                              ← UNCHANGED
    ├── components/
    │   ├── Card.jsx                          ← UNCHANGED
    │   ├── Pill.jsx                          ← UNCHANGED
    │   ├── CategoryPill.jsx                  ← UNCHANGED
    │   ├── Dot.jsx                           ← UNCHANGED
    │   └── Icon.jsx                          ← UNCHANGED
    ├── layout/
    │   └── RightPanel.jsx                    ← UNCHANGED (placeholder remains)
    └── styles/
        ├── tokens.css                        ← UNCHANGED
        ├── base.css                          ← extended slightly if Dashboard tab needs new styles
        └── layout.css                        ← extended slightly for Dashboard layout
```

Everything outside `frontend/` is **immutable** during M1.

### 4.4 Net file count delta

New files: ~30 (10 adapter modules + 1 hook + 5 fixtures + 11 test
files + ~3 configuration files).
Modified files: ~5 (App.jsx, HorizonHeader.jsx, ConditionsBar.jsx,
CenterPanel.jsx, LeftPanel.jsx) + package.json + package-lock.json
+ optionally vite.config.js + maybe base.css / layout.css.

---

## 5. Dependency changes and justification

### 5.1 New runtime dependencies

| Package | Version target | Justification |
|---|---|---|
| `date-fns-tz` | `^3.0.0` | DST-aware time zone handling per Adapter Note §13. Selected per M1 Scope Proposal §19.1. Tree-shakable; ~30 KB minified |
| `date-fns` | `^3.0.0` | Peer dep of `date-fns-tz`; used for parsing/formatting. Tree-shakable; adds ~20 KB |

### 5.2 New dev dependencies

| Package | Version target | Justification |
|---|---|---|
| `vitest` | `^2.0.0` | Test framework per M1 Scope Proposal §19.2 decision. Vite-native; minimal config |
| `@vitejs/plugin-react` | (already present) | Reused by Vitest for JSX/React file handling |

### 5.3 Unchanged dependencies from M0

| Package | Notes |
|---|---|
| `react` `^18.3.0` | Unchanged |
| `react-dom` `^18.3.0` | Unchanged |
| `serve` `^14.2.4` | **Unchanged** — Railway runtime static-file server. Per §19.7 decision, stays in place |
| `vite` `^5.4.0` | Unchanged |

### 5.4 Final `package.json` structure (target)

```json
{
  "dependencies": {
    "react": "^18.3.0",
    "react-dom": "^18.3.0",
    "serve": "^14.2.4",
    "date-fns": "^3.0.0",
    "date-fns-tz": "^3.0.0"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.3.0",
    "vite": "^5.4.0",
    "vitest": "^2.0.0"
  },
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview",
    "start": "serve -s dist -l ${PORT:-3000}",
    "test": "vitest run",
    "test:watch": "vitest"
  }
}
```

The `engines.node >= 18` pin from M0 remains. The new `test` /
`test:watch` scripts are added; nothing else in `scripts`
changes.

### 5.5 Bundle size budget

M0 produced 159 KB JS (gzip ~51 KB). M1 adds adapter code +
polling hook + `date-fns-tz` / `date-fns` subset. Realistic M1
bundle estimate:

- M0 baseline: 159 KB
- Adapter modules (~10 files, mostly pure transforms): +15–25 KB
- Polling hook: +1–2 KB
- `date-fns-tz` + `date-fns` (tree-shaken subset for what M1
  uses): +15–30 KB
- **Total M1 bundle estimate: 190–215 KB** (gzip ~60–75 KB)

Acceptable for V1.0 sandbox. If the bundle grows beyond 250 KB,
investigate (likely `date-fns` over-imports).

### 5.6 No new Python / requirements.txt changes

`requirements.txt` is untouched. No Python deps added.

---

## 6. Fixture capture plan

Per M1 Scope Proposal §6.3 + §19.3:

### 6.1 Capture target

Beta 10 production endpoint:
`https://project-horizon-production-a03c.up.railway.app/api/summary`

(Or a local Beta 10 dev environment if Tony prefers.)

### 6.2 Capture sequence (Tony-side prep)

This happens **before** M1 implementation begins. The fixtures
are committed in a small prep PR (or as the first commit on
`feat/v1-m1`) so the adapter implementation has stable inputs.

Steps:

1. Tony obtains an authenticated `horizon_session` cookie from
   a logged-in Beta 10 production session
2. For each fixture, Tony runs:

```bash
curl -b "horizon_session=<token>" \
     "https://project-horizon-production-a03c.up.railway.app/api/summary" \
     | jq '.' > frontend/fixtures/brisbane-busy.json
```

(Possibly with port switching between captures — `POST
/api/set_port {"port":"BRISBANE"}` then capture, then
`{"port":"MELBOURNE"}` etc. — each set_port call requires the
same authenticated cookie.)

### 6.3 Captured fixtures (3)

| Fixture | Capture conditions |
|---|---|
| `brisbane-busy.json` | Brisbane, mid-shift, with at least one CRITICAL conflict + non-trivial vessel count (typically 10+ vessels in port) |
| `brisbane-quiet.json` | Brisbane, low-activity moment, zero conflicts, small vessel count |
| `melbourne-sim.json` | Melbourne, any moment (Melbourne's `sim_vessel_count = 9` differs from Brisbane's 13; exercises adapter handling of different port shapes) |

### 6.4 Derived fixtures (2)

After the 3 captured fixtures are committed, Tony or Claude
derives:

| Fixture | Derived how |
|---|---|
| `null-fields.json` | Copy of `brisbane-busy.json` with selected fields manually edited to `null` (vessel `etd`, vessel `notes`, conflict `berth_id`, conflict `decision_support`, etc.) — exercises adapter null handling per Adapter Note §15.2 |
| `malformed.json` | Copy of `brisbane-busy.json` with selected fields manually corrupted (numeric where string expected, string where bool expected, missing required keys) — exercises adapter defence per Adapter Note §15.3 |

The derived fixtures live alongside the captured ones in
`frontend/fixtures/`. They are committed and form the test
basis for §15.2 and §15.3 defence tests.

### 6.5 Sensitive-data sanitisation

Beta 10's `port_profiles.py` already uses simulated vessel
names by default. Real customer names, real agent contacts, or
real operator handles should not appear in captures. Tony scans
each captured fixture before commit and replaces any visible
real identifiers with simulated equivalents.

If `port_profiles.py` ever connects to real AIS / MST data, the
sanitisation step becomes more important. For V1.0 sandbox-only
review, the risk is low because Beta 10 production uses
simulated data anyway, but the discipline applies.

### 6.6 Capture is a prep step, not part of M1 implementation

The fixture capture is **Tony-side** and happens before the
adapter implementation begins. Claude does not capture
fixtures (no Beta 10 production auth path from Claude).

The fixtures arrive as either:

- A small prep PR from Tony titled "chore(v1-m1): capture
  /api/summary fixtures for M1" — 5 JSON files, no other
  changes; OR
- The first commit on `feat/v1-m1` (Claude commits the
  fixtures Tony provides via direct message)

Either pattern is acceptable. The prep-PR approach gives
ChatGPT a chance to review the fixture set before adapter
implementation locks against them. Recommended.

---

## 7. Fixture file structure

### 7.1 Fixture format

Each `frontend/fixtures/*.json` is a **verbatim** JSON dump of
Beta 10's `/api/summary` response. Pretty-printed (one key per
line) for diff-friendliness. Final newline. UTF-8.

Top-level keys per Shape Spike §3 (26 keys):

```
port_name, generated_at, lookahead_hours, data_source,
data_source_label, scraped_at, port_status, vessels, berths,
pilotage, towage, port_tugs, port_gangs, conflicts, guidance,
port_geo, weather, tides, berth_utilisation, etd_risk,
dashboard, ukc, arrival_ukc, dukc, esg, port_profile
```

These are the raw shape the adapter normalises. The adapter
does the work of producing the `ViewSummary` shape (per Adapter
Note §2.2); the fixtures stay raw.

### 7.2 Fixture size

Beta 10's typical `/api/summary` response is 50–150 KB JSON
(pretty-printed). Five fixtures total ≈ 250–750 KB committed to
the repo. Acceptable; no LFS needed.

### 7.3 Served at runtime

The Vite build is configured to include `frontend/fixtures/` as
static assets so they ship in `dist/fixtures/*.json` and the
sandbox `serve` static file server returns them on `GET
/fixtures/brisbane-busy.json` etc.

Two options for Vite to include the directory:

| Option | How |
|---|---|
| **A. Move to `frontend/public/fixtures/`** | Vite copies anything in `public/` verbatim to `dist/`. Simplest. |
| **B. Vite config addition** | Add a custom Rollup plugin in `vite.config.js` to copy `frontend/fixtures/*.json` into `dist/fixtures/`. More explicit but more config. |

**Default: Option A** (use `frontend/public/fixtures/`). Simpler
and idiomatic for Vite.

If Option A is taken, the fixture path inside the repo becomes
`frontend/public/fixtures/*.json` and the file scaffold in §4.1
updates accordingly. (Implementer detail; the plan headline
`frontend/fixtures/` shorthand stays for readability.)

### 7.4 Fixture rotation in polling

For the polling hook to demonstrate "data refresh" visibly, the
sandbox can either:

| Option | Behaviour |
|---|---|
| A. Single fixture | Polling refetches the same `brisbane-busy.json` every 30s — proves the polling mechanism works (last-updated timestamp ticks; bandwidth used; cache logic exercised) |
| B. Rotation | Polling cycles through the 3 captured fixtures (busy → quiet → melbourne → busy → …) demonstrating UI refresh when values actually change |

**Default: Option A.** Simpler; M1 is about proving the pipeline,
not simulating live data churn. If rotation adds operational
value during M1 review, it's a trivial change in the polling
hook.

---

## 8. Adapter implementation plan

Per Adapter Design Note §5–§14, each adapter module is a **pure
function** taking the raw input and returning the normalised
ViewSummary section.

### 8.1 Module-by-module checklist

Each module is roughly 30–80 lines + ~50–150 lines of test:

| Module | Inputs | Outputs | Test focus |
|---|---|---|---|
| `summaryAdapter.js` | Whole raw `/api/summary` JSON | `ViewSummary` per Adapter Note §2.2 | Snapshot per fixture; missing fields → defaults; `missingDomains` populated |
| `conditionsAdapter.js` | `raw.weather` + `raw.tides` + `raw.ukc` | `conditions` block per §6 | Merge correctness; visibility nm + km; missing fields graceful |
| `vesselAdapter.js` | `raw.vessels[]` + `raw.etd_risk[]` | `Vessel[]` with risk joined | Field renames; risk join by `vessel_id`; missing AIS fields → `null` |
| `conflictAdapter.js` | `raw.conflicts[]` | `Conflict[]` with `cascade: null` | Severity uppercase; derived `title`; option flattening; recommended-option-id preservation |
| `guidanceAdapter.js` | `raw.guidance[]` + `raw.conflicts[]` | Ordered alert list | Merging + sorting by severity / deadline / timestamp |
| `dashboardAdapter.js` | `raw.dashboard` + `raw.port_status` | `dashboardMetrics` block | Field renames; group-by-category |
| `etdRiskAdapter.js` | `raw.etd_risk[]` | `EtdRisk[]` with `delta: null` | Field renames; sort by risk score |
| `time.js` | ISO strings + IANA timezone | JS Date + port-local HH:MM | DST handling; sub-second precision; UTC vs local |
| `status.js` | Lowercase severity / status / source strings | Uppercase canonical + tone | Closed-set mappings; unknown → fallback |
| `units.js` | Nautical-miles number | km computed | `visibility_nm * 1.852` exact |

### 8.2 Per-module structure

```js
// vesselAdapter.js — example structure
import { join } from './_joinHelper'; // hypothetical
import { mapStatus, mapSource } from './status';
import { isoToParts } from './time';

export function adaptVessels(rawVessels = [], rawEtdRisk = [], portTimezone) {
  if (!Array.isArray(rawVessels)) return [];
  const riskByVesselId = Object.fromEntries(
    (rawEtdRisk || []).map(r => [r.vessel_id, r])
  );
  return rawVessels.map(v => {
    const risk = riskByVesselId[v.id];
    return {
      vesselId: v.id,
      name: v.name,
      imo: v.imo,
      type: v.vessel_type,
      status: v.status,
      source: mapSource(v.source),
      etaIso: v.eta,
      etaLocalShort: v.eta ? isoToParts(v.eta, portTimezone).short : null,
      // ... rest of field renames
      riskScore: risk?.risk_score ?? 0,
      riskLevel: risk?.risk_level ?? 'low',
      riskFactors: risk?.risk_factors ?? [],
    };
  });
}
```

Components import only `frontend/src/api/horizon.js`. The
adapters/* directory is internal. Per Adapter Note §4 public-
API surface.

### 8.3 No business logic in adapters

Each adapter is a **transformation**, not a decision module.
Per Adapter Note §3.3, no operational logic (conflict ranking,
recommendation ordering) goes in the adapter — that lives in
the backend (today) and a future decision module (V1.x).

### 8.4 Defence behaviour per Adapter Note §15

Every adapter handles:

- Missing arrays → empty array
- Null fields → preserved as null
- Unexpected types → defaults substituted; warning logged in
  non-production builds
- Partial response → fields set to `null` in `ViewSummary`;
  added to `missingDomains` array

`malformed.json` fixture exercises these paths in unit tests.

### 8.5 Cascade and missing domains explicit

Per Adapter Note §8.8 + §12.3:

- `cascade: null` on every adapted Conflict — adapter does
  **not** synthesise cascades
- `delta: null` on every adapted EtdRisk per Adapter Note §11.2
- `ViewSummary.missingDomains` includes `"shiftLog"`,
  `"auditLog"`, `"cascade"` for M1

UI components honour these absences by hiding or labelling the
affected surfaces; the adapter never fakes data.

### 8.6 No fabricated data

Adapter never creates values not present in the raw input
(except adapter-computed derivations like `visibilityKm =
nm * 1.852` and the time-format conversions). All §13 derived
values are explicitly listed; no other derivation is sanctioned.

### 8.7 Public entry point

`frontend/src/api/horizon.js`:

```js
export { fetchSummary } from './_fetchWrapper';
export { useSummary } from '../hooks/useSummary';
// adaptSummary etc. are NOT re-exported — internal only
```

Components import `useSummary` from `horizon.js`, never from
`hooks/useSummary.js` directly. This preserves the seam.

---

## 9. Polling hook implementation plan

### 9.1 `useSummary` hook contract

Per M1 Scope Proposal §8.1:

```js
const { data, isLoading, error, lastUpdated, isStale, refetch }
  = useSummary({ pollInterval: 30000 });
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `data` | `ViewSummary | null` | `null` | Current data |
| `isLoading` | `boolean` | `true` until first fetch | Initial fetch in progress |
| `error` | `Error | null` | `null` | Last fetch error if any |
| `lastUpdated` | `Date | null` | `null` | Time of last successful fetch |
| `isStale` | `boolean` | `false` | True if `now - lastUpdated > 2 * pollInterval` |
| `refetch` | `function` | — | Force immediate fetch |

### 9.2 Polling lifecycle

```js
import { useEffect, useState, useRef, useCallback } from 'react';
import { fetchSummary } from '../api/horizon';

const DEFAULT_POLL_MS = 30000;
const BACKOFF_DELAYS = [1000, 3000, 9000]; // ms

export function useSummary({ pollInterval = DEFAULT_POLL_MS } = {}) {
  const [data, setData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const aliveRef = useRef(true);
  const timerRef = useRef(null);

  const performFetch = useCallback(async () => { /* ... */ }, []);

  // Initial fetch
  // Polling interval
  // Tab visibility pause/resume
  // Cleanup on unmount

  return { data, isLoading, error, lastUpdated, isStale, refetch };
}
```

### 9.3 Tab-visibility pause / resume

Polling pauses when `document.visibilityState === 'hidden'` and
resumes when `'visible'`. Implemented via
`document.addEventListener('visibilitychange', ...)`. On resume,
trigger an immediate fetch to refresh stale data.

### 9.4 Backoff on failure

On fetch error, retry at `BACKOFF_DELAYS[0]` (1s), then
`[1]` (3s), then `[2]` (9s). After three failures, set `error`
state and return to normal `pollInterval` cadence. Each
successful fetch resets the backoff sequence.

### 9.5 Last-known-good cache

If a fetch fails but `data` was previously set, **`data` is
preserved** so the UI keeps rendering the last good response
with a "stale" or "connection lost" badge (controlled by
`isStale`).

### 9.6 Concurrent-fetch protection

If a fetch is in-flight when the next interval fires, the new
fetch is **skipped** (single-flight pattern). No
`AbortController` needed for fixtures (they return fast); add
in M1.x if real `/api/summary` introduces multi-second response
times.

### 9.7 Strictly GET

The hook makes only `GET` requests. Per M1 Scope Proposal §8.3
+ §15 gate 8.

### 9.8 No auth header

Per §11 of the Scope Proposal — no `credentials: 'include'`, no
`Authorization` header, no cookie reads in the fetch wrapper.

---

## 10. UI surfaces to update

Per M1 Scope Proposal §9.

### 10.1 `App.jsx` — adapter wiring

Replace:

```jsx
import { SAMPLE } from './data/sample.js';
// ...
<HorizonHeader summary={SAMPLE} />
<ConditionsBar conditions={SAMPLE.conditions} />
```

With:

```jsx
import { useSummary } from './api/horizon';
// ...
function App() {
  const { data, isLoading, error, lastUpdated, isStale } = useSummary();
  if (!data) {
    return <EmptyState isLoading={isLoading} error={error} />;
  }
  return (
    <div className="hz-app">
      <div className="hz-demo-banner">...</div>
      <HorizonHeader summary={data} lastUpdated={lastUpdated} isStale={isStale} />
      <ConditionsBar conditions={data.conditions} />
      <div className="hz-shell">
        <LeftPanel />
        <CenterPanel data={data} />
        <RightPanel />
      </div>
    </div>
  );
}
```

`EmptyState` is a small new component (could live inline) showing
the Horizon Dark shell with a "Loading..." or "Connection lost"
banner.

`sample.js` is **not** imported. Kept on disk as historical
reference + as a fallback for local dev if Tony wants offline
mode (out of scope for M1).

### 10.2 `HorizonHeader.jsx` — adapter-backed

Per M1 Scope Proposal §9.1:

- Stat tile 1: `summary.portStatus.vesselsInPort`
- Stat tile 2: `summary.dashboardMetrics.pilotOps12h + tugOps12h` (labelled "Movements 12h")
- Stat tile 3: `summary.portStatus.criticalConflicts` (pulse dot if > 0)
- Stat tile 4: `summary.conditions.rating`
- DEMO · SIMULATION pill remains (still fixture data, not live)
- VTSO role pill stays (static)
- Live clock remains (real `Date.now()` — UI only, not data)
- New: "Last update HH:MM" indicator near the clock, showing
  `lastUpdated` formatted in port timezone. If `isStale`, the
  indicator turns amber.

### 10.3 `ConditionsBar.jsx` — adapter-backed

Per M1 Scope Proposal §9.2:

- Rating pill bound to `conditions.rating`
- Wind / Swell / Visibility / Pressure / Tide / UKC tiles
  bound to corresponding `conditions` fields
- Each tile `(demo)` marker remains
- New tile: **"Last update"** showing `summary.timestamp`
  port-local HH:MM (alongside / instead of duplicating the
  header's indicator)

### 10.4 `CenterPanel.jsx` — partial Dashboard tab

Replace the M0 placeholder with the **partial Dashboard tab**.

Per M1 Scope Proposal §9.3 + resolved decision #6 (heatmap
summary line only):

```jsx
<main className="hz-panel hz-panel-center">
  <DashboardTab data={data} />
</main>
```

`DashboardTab` is a new component family inside
`frontend/src/features/dashboard/`:

```
frontend/src/features/dashboard/
├── DashboardTab.jsx          ← composition
├── KpiTileGroup.jsx          ← three rows: Operations, Performance, Safety
├── EtdRiskTable.jsx          ← top-5 risk table
└── UtilisationSummary.jsx    ← single-line summary
```

Each is ~50–100 lines. None require new component primitives
beyond M0's `Card`, `Pill`, `Dot`.

**KPI tile groups** bound to `summary.dashboardMetrics`:
- Operations: `vesselsInPort`, `vesselsExpected24h`, `activeConflicts`
- Performance: `berthUtilisationPct`, `onTimeDeparturePct`, `avgDwellHours`
- Safety: `vesselsAtRisk`, `criticalConflicts`, `pilotOps12h` (or `tugOps12h`)

**ETD risk table:**
- Top 5 from `summary.etdRisk` sorted by `riskScore` desc
- Columns: vessel name, risk level (Pill), primary `riskFactor`,
  bar showing `riskScore` (0–100)

**Utilisation summary line:**
- "Currently {N}% occupied ({occupied}/{total} berths); next
  48h forecast {forecastPct}%" derived from
  `summary.dashboardMetrics.berthUtilisationPct`,
  `summary.portStatus.berthsOccupied`,
  `summary.portStatus.berthsTotal`,
  `summary.dashboardMetrics.forecastUtilisation48h`

**Not in M1:** tab navigation (centre panel has one Dashboard
view only); full berth heatmap; other 5 tabs.

### 10.5 `LeftPanel.jsx` — DesignVerificationSwatch removed

Per M1 Scope Proposal §9.4:

- DesignVerificationSwatch is **removed**
- M0 placeholder card stays ("Operator alerts & decision card —
  arrive in later milestones")
- No alert list scaffolded in M1

This is a small deletion + leaves the rest of `LeftPanel.jsx`
intact.

### 10.6 `RightPanel.jsx` — unchanged

Still placeholder. No vessel roster, no Audit Log tab. Same as
M0.

### 10.7 Empty / loading / error states

While `data === null` (first fetch in flight), the shell
renders empty header + empty conditions bar + empty centre
panel with a small "Loading…" overlay. After fetch:

- Success → render with adapter output
- Error with no prior `data` → render shell + "Connection lost"
  banner; provide a Retry button (calls `refetch()`)
- Error with stale `data` → render with stale data + amber
  "Connection lost — showing last good values" banner

All three states use Canon §3 colour and component primitives.
No new visual language introduced.

---

## 11. Components to add or extend

### 11.1 New components

| Component | Location | Purpose |
|---|---|---|
| `EmptyState` | `frontend/src/components/EmptyState.jsx` | Loading / error / no-data fallback state |
| `DashboardTab` | `frontend/src/features/dashboard/DashboardTab.jsx` | Composition of KPI + ETD risk + utilisation |
| `KpiTile` | `frontend/src/features/dashboard/KpiTile.jsx` | Individual KPI cell (value + label + trend-placeholder) |
| `KpiTileGroup` | `frontend/src/features/dashboard/KpiTileGroup.jsx` | Three-row grouping |
| `EtdRiskTable` | `frontend/src/features/dashboard/EtdRiskTable.jsx` | Top-5 table |
| `RiskBar` | `frontend/src/features/dashboard/RiskBar.jsx` | 0–100 inline progress bar |
| `UtilisationSummary` | `frontend/src/features/dashboard/UtilisationSummary.jsx` | Single-line text component |

Each component is small (30–80 lines JSX + minimal CSS).

### 11.2 Extended (existing) components

| Component | Extension |
|---|---|
| `HorizonHeader.jsx` | Add "Last update HH:MM" indicator next to clock; remove `SAMPLE` import |
| `ConditionsBar.jsx` | Add "Last update" tile or merge into header; ensure all tiles bind to adapter output |
| `App.jsx` | Replace `SAMPLE` import with `useSummary` hook; pass `data` through; handle loading/error/stale states |
| `CenterPanel.jsx` | Replace placeholder with `<DashboardTab data={data} />` |
| `LeftPanel.jsx` | Remove `DesignVerificationSwatch` and its imports |

### 11.3 Pill / Dot / Card / Icon / CategoryPill — unchanged

The M0 component primitives are reused as-is. No new shared
components in `frontend/src/components/`.

---

## 12. What must remain mocked / static

Even with adapter-backed UI, several aspects remain mocked:

| Surface | Mock status |
|---|---|
| Fixture endpoint | Same-origin static files, not Beta 10 live |
| Auth | No login form; no real session cookie; static VTSO role pill |
| ACK / COMMIT / DEFER / OVERRIDE / ESCALATE | No affordances ship |
| DSW | Not implemented |
| Audit log right tab | Placeholder; no real `/api/audit` exists |
| Decision card in LeftPanel | Placeholder; no Active Decision rendered from fixtures (even if conflicts contain decision-shaped data; M2+ surfaces it) |
| Port switcher | None |
| Multiple tabs in CenterPanel | One tab (Dashboard) only |
| Stakeholder / mobile / executive surfaces | None |
| Replay | None |
| Server-side RBAC | None — anti-pattern §10.2 forbids frontend-only |
| AI provenance display | Not in M1; deferred to V1.x per Information Architecture §16 |

These mocks are clearly labelled per §14 below.

---

## 13. What must NOT be implemented

Per M1 Scope Proposal §4 + §10 + §12 + §18:

- ❌ Live Beta 10 `/api/summary` calls (any HTTP request to
  `project-horizon-production-*` from M1 frontend code)
- ❌ Any HTTP method other than `GET` (no POST/PUT/PATCH/DELETE
  anywhere)
- ❌ `credentials: 'include'`, cookie reads, `Authorization`
  headers
- ❌ Login form / session acquisition
- ❌ Real role switcher with RBAC effects
- ❌ ACK / COMMIT / DEFER / OVERRIDE / ESCALATE affordances
- ❌ DSW modal scaffolding
- ❌ What-If scenario builder
- ❌ Port Brief download
- ❌ Berth Timeline / VTS Map / Shift Log / Pilotage /
  Performance tabs
- ❌ Tab navigation in CenterPanel (M2)
- ❌ Vessel roster interactivity in RightPanel
- ❌ Audit Log right-panel tab (no backend source exists)
- ❌ Active Decision card rendered from fixtures (M2)
- ❌ Replay workspace
- ❌ Stakeholder / mobile / executive surfaces
- ❌ Port switcher
- ❌ New audit event types defined anywhere
- ❌ `server.py` modification
- ❌ Root `index.html` modification
- ❌ Root `railway.toml` modification
- ❌ `frontend/railway.json` modification (M0 config still
  correct)
- ❌ `tests/test_beta10_regression.py` modification (the Python
  regression gate stays exactly where it is)
- ❌ `requirements.txt` / `Procfile` / `deploy/` /
  `port_profiles.py` modification
- ❌ Stage E-prod activation
- ❌ Production `DATABASE_URL` change
- ❌ `AUDIT_EMISSION_ENABLED` toggling
- ❌ Custom production Horizon domains on the sandbox
- ❌ Modifications to PRs #27, #28, #29
- ❌ Modifications to preview Postgres / `preview-audit-activation`

If any of these appears in an M1 diff, the PR is rejected
without merge.

---

## 14. DEMO / read-only labelling requirements

Per M1 Scope Proposal §9 + §11 and inherited from M0:

| Surface | Label |
|---|---|
| Top-edge persistent banner | `Horizon V1 — sandbox · demo data · not for operational use` (unchanged from M0) |
| HorizonHeader data-source pill | `DEMO · FIXTURE` (refined from M0's `DEMO · SIMULATION` to be more accurate — the data source is now recorded fixtures, not simulation; alternative: keep `DEMO · SIMULATION`) |
| ConditionsBar tiles | each labelled `(demo)` (unchanged) |
| "Last update" indicator | shows port-local HH:MM in the header |
| "Connection lost" / "Stale" banner | amber-bordered Card at the top of the centre panel when `isStale` or `error` |
| Loading state | "Loading Horizon V1 sandbox…" |
| Vessel cards (if any) | labelled `(demo data)` |
| Dashboard KPI tiles | no extra label needed — fixtures, demo banner is sufficient |
| Footer / metadata | optional small "Data source: fixtures · Last update HH:MM" line |

**Decision needed during M1 implementation:** keep `DEMO ·
SIMULATION` from M0 (more familiar) or refine to `DEMO ·
FIXTURE` (more accurate). Both are honest. Implementer chooses
during M1 build; the M1 PR notes the choice.

---

## 15. Beta 10 protection approach

Same approach as M0, with one addition:

### 15.1 Phase 0 protected file list — unchanged

`server.py`, `audit.py` + 5 helpers, `db.py`, `tenant.py`,
`index.html` (root), `requirements.txt`, `railway.toml` (root),
`Procfile`, `deploy/`, `port_profiles.py`,
`tests/test_beta10_regression.py`, `alembic/`, plus
`phase-0-complete @ 4ad4aae` tag.

Pre-PR verification:
`git diff --stat main..feat/v1-m1` against each of the above
must return empty.

### 15.2 Regression gate gated on every commit

`tests/test_beta10_regression.py` 46/46 verified before every
`git push` to `feat/v1-m1`.

### 15.3 `frontend/` is the **only** path with changes

`git diff main..feat/v1-m1 --name-only | grep -v "^frontend/"`
returns empty.

### 15.4 No new Python files

`git diff main..feat/v1-m1 --name-only -- '*.py'` returns
empty.

### 15.5 Beta 10 demo flow post-merge

Tony post-merge visual check (same as M0): `/` loads, `/login`
works, port switcher works across Brisbane / Melbourne /
Geelong / Darwin.

### 15.6 `frontend/railway.json` — unchanged

The M0 build/start commands continue to apply for M1. Any
change to `frontend/railway.json` requires separate
authorisation.

---

## 16. Railway sandbox deployment handling

### 16.1 Sandbox project, service unchanged

`horizon-v1-sandbox` (project id `396c846d-...`), service
`horizon-v1-sandbox` (id `f3eacfc6-...`), env `production`,
Config Path `/frontend/railway.json`, root directory
`frontend`, Railway-issued domain only.

### 16.2 Source-branch switch (Tony-side, between Phase 1 and Phase 2)

Single Tony-side dashboard action:

- In Railway dashboard, on the `horizon-v1-sandbox` service →
  Settings → Source → change branch from `feat/v1-m0` to
  **`feat/v1-m1`**
- Save
- Railway auto-deploys from `feat/v1-m1` head

This is the **only** Railway-side change M1 requires. Claude
does not perform this — per §16.2 governance of Sandbox
Provisioning Plan, Tony executes.

### 16.3 No new Railway projects

`horizon-v1-preview` / `horizon-v1-staging` / `horizon-v1-prod`
(per Execution Plan §8) remain deferred. M1 deploys to the same
sandbox.

### 16.4 No env-var changes

The sandbox's existing 11 Railway defaults are sufficient. No
new env vars introduced.

### 16.5 No DB / add-ons attached

Per Sandbox Provisioning Plan §10 + §12 — no Postgres add-on,
no Redis, no volumes. Verified per-Phase-2.

### 16.6 No custom domain

Same Railway-issued URL: `horizon-v1-sandbox-production.up.railway.app`.

### 16.7 `horizon-prod` untouched

The Beta 10 production Railway service is on a different
project (`Project-Horizon`) and listens to `main` only. M1
work on `feat/v1-m1` does not trigger `horizon-prod` deploys.
When `feat/v1-m1` merges to `main`, Beta 10 auto-redeploys
from `main` — byte-identical because no Beta 10 file changed.

### 16.8 Preview Postgres still unrelated

The `preview-audit-activation` environment crash is a separate
Tony / Railway support thread. M1 has zero dependency on it.

---

## 17. Validation gates

Pre-PR (Phase 1) gates, in order:

| # | Gate | How verified |
|---|---|---|
| 1 | `tests/test_beta10_regression.py` 46/46 | `python3.10 -m pytest tests/test_beta10_regression.py -q` on every commit |
| 2 | Protected-file diff empty | `git diff --stat main..feat/v1-m1` against Phase 0 protected list returns empty |
| 3 | No Python file in diff | `git diff --name-only main..feat/v1-m1 -- '*.py'` empty |
| 4 | Only `frontend/` changes | `git diff --name-only main..feat/v1-m1 \| grep -v "^frontend/"` empty |
| 5 | `cd frontend && npm install` succeeds | New `date-fns-tz`, `date-fns`, `vitest` deps install cleanly; lockfile updated |
| 6 | `cd frontend && npm run build` succeeds | Vite produces `dist/`; bundle within size budget (§5.5) |
| 7 | `cd frontend && npm test` passes | All Vitest adapter unit tests pass against all 5 fixtures |
| 8 | `frontend/dist/` not committed | `.gitignore` enforces |
| 9 | `frontend/node_modules/` not committed | Same |
| 10 | No `POST`/`PUT`/`PATCH`/`DELETE` method | `grep -rn "method:.*['\"](POST\|PUT\|PATCH\|DELETE)" frontend/src/` empty |
| 11 | No `credentials: 'include'` | `grep -rn "credentials.*include" frontend/src/` empty |
| 12 | No new audit event types | `grep -rn "OPERATOR_ACKNOWLEDGED\|OPERATOR_ACTED\|AUDIT_READ" frontend/src/` empty (comment references in strings acceptable) |
| 13 | No "Audit trail preserved" / "Decision logged" text | `grep -rn "Audit trail preserved\|Decision logged" frontend/src/` empty |
| 14 | No real login form | `grep -rn "action.*login\|<form.*login" frontend/src/` empty |
| 15 | No role switcher with real RBAC effects | `grep -rn "setRole\|currentUser\|roleOptions" frontend/src/` empty |
| 16 | DEMO labels present | Manual grep — 8+ DEMO / fixture / placeholder occurrences in source |
| 17 | Manual visual check of local `frontend/dist/index.html` | Open in browser; shell renders; KPI tiles populated from fixture; ETD risk table populated; no console errors |
| 18 | Network panel: only `/fixtures/*.json` calls | DevTools — zero `/api/*` requests |
| 19 | PRs #27, #28, #29 untouched | Timestamp check |

If any gate fails, the M1 PR is not opened until resolved.

### 17.1 Adapter test coverage targets

Per Adapter Note §16, the test suite covers:

1. **Snapshot tests** — `summaryAdapter.test.js` includes one
   snapshot per fixture (3 captured + 2 derived = 5 snapshots).
   `vitest --update-snapshots` to regenerate on intentional
   changes; CI runs with `vitest run` (no update).
2. **Per-domain assertions** — explicit tests in
   `vesselAdapter.test.js`, `conditionsAdapter.test.js`,
   `conflictAdapter.test.js`, etc. for the specific
   transformations per Adapter Note §6–§14.
3. **Empty-state tests** — given `{}` input, adapter returns a
   fully-formed `ViewSummary` with empty defaults.
4. **Defence tests** — `malformed.json` exercises type
   substitution; warnings logged (not thrown).
5. **Round-trip tests** — `View → JSON.stringify → JSON.parse`
   produces an equivalent view (no Date serialisation
   surprises — needs explicit handling in
   `summaryAdapter.test.js`).

Acceptance: all 5 categories present, all passing on `npm test`.

---

## 18. Acceptance criteria

M1 closes when **all** of the following are true:

1. M1 PR opened, reviewed by ChatGPT, authorised by Tony,
   merged
2. `cd frontend && npm install && npm run build` succeeds
   locally; `dist/` artefact within bundle budget
3. `cd frontend && npm test` passes (all Vitest categories,
   all 5 fixtures)
4. Sandbox URL renders the partial Dashboard + adapter-fed
   HorizonHeader + ConditionsBar against fixture data — i.e.
   `sample.js` no longer imported at runtime; adapter is the
   only data source
5. Browser devtools network tab on the sandbox URL shows
   periodic `GET /fixtures/brisbane-busy.json` every ~30s; no
   other API calls
6. Adapter correctly handles `cascade: null`, `delta: null`,
   missing domains (verified via derived `null-fields` /
   `malformed` fixtures in unit tests and at runtime if
   sandbox is configured to serve them)
7. Regression gate `tests/test_beta10_regression.py` 46/46
8. `server.py`, root `index.html`, audit helpers, Phase 0
   migrations, deploy manifests, `requirements.txt`, root
   `railway.toml`, `port_profiles.py`, `tests/` untouched
9. PRs #27, #28, #29 still untouched
10. Beta 10 demo flow verified working post-merge (Tony visual
    check, same as M0)
11. No new audit event type, no audit emission call, no Stage
    E-prod activation
12. Sandbox URL not linked from `horizon-prod`
13. ViewSummary contract preserved — adapter output matches
    Adapter Note §2.2 keys
14. DesignVerificationSwatch removed from LeftPanel; lifecycle
    state visual now exercised against fixture-derived
    conflicts

### 18.1 Phase split

| # | Item | Phase |
|---|---|---|
| 1 | M1 PR opened, reviewed, authorised, merged | spans both phases |
| 2 | Local `dist/` builds | Phase 1 |
| 3 | Vitest tests pass | Phase 1 |
| 4 | Sandbox renders adapter-fed UI | Phase 2 |
| 5 | Polling visible in devtools on sandbox | Phase 2 |
| 6 | Adapter handles edge fixtures | Phase 1 (tests) + Phase 2 (visual on sandbox) |
| 7 | Regression gate | Phase 1 (and re-verified post-merge) |
| 8 | Protected files untouched | Phase 1 |
| 9 | PRs #27, #28, #29 untouched | both |
| 10 | Beta 10 demo post-merge | Phase 2 (Tony-side) |
| 11 | No audit / Stage E-prod | both |
| 12 | Sandbox URL not linked from prod | both |
| 13 | ViewSummary contract preserved | Phase 1 |
| 14 | DesignVerificationSwatch removed | Phase 1 |

If Railway is unstable during Phase 2, Sandbox Provisioning
Plan §18.1 fallback applies — M1 may close on Phase 1 only,
with items 4, 5, 6 (sandbox-side), 10 deferred to a small
follow-up.

---

## 19. Rollback plan

Per the M0 pattern:

```bash
git revert <M1 merge commit>
# Optional file-system cleanup:
rm -rf frontend/src/api/ \
       frontend/src/hooks/useSummary.js \
       frontend/src/features/dashboard/ \
       frontend/fixtures/ \
       frontend/public/fixtures/ \
       frontend/tests/
# Restore sample.js consumption in App.jsx (manually or via
# revert of the App.jsx changes in the M1 merge commit)
```

That is the full rollback. No state migrations, no env-var
flips, no audit chain reconciliation, no Beta 10 impact.

Sandbox after rollback:

- If Tony reverts the source-branch switch (`feat/v1-m1` →
  `feat/v1-m0`), sandbox serves the M0 build at commit
  `1f5447dc` — known good
- If Tony leaves the source on `feat/v1-m1`, the next deploy
  ships the revert commit's state (empty `frontend/src/api/`,
  `App.jsx` back to importing `sample.js`)

`horizon-prod` is unaffected — Beta 10 deploy artefact remains
byte-identical because M1 did not touch any Beta 10 file.

`feat/v1-m0` branch remains preserved on remote as the fallback
target.

---

## 20. Stop conditions

Implementation **stops immediately** if any of the following
occurs:

| Condition | Action |
|---|---|
| Any Beta 10 protected file appears in the diff | Stop, report, do not commit |
| `tests/test_beta10_regression.py` fails | Stop, investigate |
| `cd frontend && npm test` fails any category | Stop, fix, re-run |
| Scope creep — any feature outside §10 list arrives in the diff | Stop, remove, report |
| New audit event type defined anywhere | Stop — separate backend schema authorisation required |
| Any `POST` / `PUT` / `PATCH` / `DELETE` in frontend source | Stop — M1 is strictly read-only |
| `credentials: 'include'` introduced | Stop — M2+ scope |
| Login form scaffolded | Stop |
| Any change to `server.py` | Stop |
| Any change to root `railway.toml` | Stop |
| Any change to `frontend/railway.json` | Stop — M0 config still correct; changing requires separate authorisation |
| Production env var detected on sandbox during verification | Stop, report |
| DB / add-on attached to sandbox | Stop, report |
| Custom Horizon domain attached to sandbox | Stop, report |
| Sandbox deployment fails repeatedly (Railway config issue) | Stop, diagnose, report; consider local-build-only fallback |
| `horizon-prod` modification detected | Stop immediately |
| Stage E-prod activation detected | Stop immediately |
| `AUDIT_EMISSION_ENABLED` toggled | Stop immediately |
| Preview Postgres affecting M1 | Stop, report — M1 has zero dependency on preview Postgres |
| Bundle size exceeds 300 KB | Stop, investigate (likely date library over-imports); resolve before continuing |
| ChatGPT review identifies substantive concern | Stop merge; revise per review |
| Tony issues "pause M1" instruction | Stop, await further instruction |

---

## 21. Phase 1 / Phase 2 sequencing

### 21.1 Pre-Phase-1 prep (Tony-side)

1. Tony decides whether fixtures are captured in a prep PR or
   in the first commit on `feat/v1-m1`
2. Tony captures 3 fixtures from Beta 10 production per §6.2
3. Tony commits fixtures (or hands them to Claude for the
   first M1 commit)
4. Tony authorises M1 Phase 1 implementation

### 21.2 Phase 1 — Local implementation complete

Claude actions:

1. Create branch `feat/v1-m1` off `origin/main @ 01fbd61` (or
   the head after fixture prep PR merge)
2. Add `date-fns-tz`, `date-fns`, `vitest` to `package.json`;
   run `npm install`
3. Create `frontend/fixtures/` (if Tony hasn't already
   committed)
4. Add derived `null-fields.json` and `malformed.json`
5. Implement adapter modules in `frontend/src/api/adapters/`
6. Implement `frontend/src/api/horizon.js` public entry
7. Implement `frontend/src/hooks/useSummary.js` polling hook
8. Write Vitest tests for each adapter module
9. Refactor `App.jsx` to use `useSummary()` instead of
   `SAMPLE`
10. Extend `HorizonHeader.jsx`, `ConditionsBar.jsx` to consume
    adapter output
11. Implement `DashboardTab` + `KpiTileGroup` +
    `EtdRiskTable` + `UtilisationSummary` + `RiskBar`
12. Replace `CenterPanel.jsx` placeholder with `<DashboardTab data={data} />`
13. Remove `DesignVerificationSwatch` from `LeftPanel.jsx`
14. Add `EmptyState.jsx` for loading / error / stale states
15. Run validation gates per §17
16. Commit + push to `feat/v1-m1`
17. Open M1 PR against `main` (Phase 1 report)
18. Stop

### 21.3 Tony-side actions between Phase 1 and Phase 2

1. ChatGPT engineering review of the M1 PR
2. Tony reviews
3. Tony updates the Railway sandbox source-branch from
   `feat/v1-m0` to `feat/v1-m1`
4. Railway auto-deploys; Tony reports the deploy result

### 21.4 Phase 2 — Sandbox deploy verification (Claude)

1. Read-only verification of the sandbox via `railway status`
   - status SUCCESS / instance RUNNING
   - configFile `/frontend/railway.json`
   - buildCommand / startCommand correct
   - env vars defaults-only
   - no add-ons
   - Railway-default domain only
2. Public HTTP probe of sandbox URL
3. Visual check (Tony-side; Claude has no browser) — load
   sandbox URL, confirm partial Dashboard renders with KPI
   values from fixtures, network tab shows periodic `GET
   /fixtures/brisbane-busy.json`
4. Issue Phase 2 verification report
5. Tony merge authorisation

### 21.5 Post-merge

1. `horizon-prod` auto-redeploys from `main` (byte-identical
   Beta 10)
2. Tony post-merge Beta 10 visual check
3. M1 close
4. Optional M1 retrospective document (recommended; mirrors
   M0.5 retrospective)

---

## 22. Cross-document citation map

| Plan section | Cites |
|---|---|
| §1 Executive summary | M1 Scope Proposal §1; Adapter Note §2.2 + §17.2 |
| §2 Resolved decisions | M1 Scope Proposal §19 (1–8 verbatim) |
| §3 Branch | Sandbox Provisioning Plan §6.1; M0 Retrospective §9.2 |
| §4 Scaffold | Adapter Note §4 |
| §5 Dependencies | M1 Scope Proposal §19.1 + §19.2; M0 Scope Proposal §9.7 (M0 deps still apply) |
| §6 Fixture capture | M1 Scope Proposal §6.3 + §19.3 |
| §7 Fixture structure | Shape Spike §3 (raw shape) |
| §8 Adapter implementation | Adapter Note §5–§16 |
| §9 Polling hook | M1 Scope Proposal §8 + Adapter Note §4 |
| §10 UI surfaces | M1 Scope Proposal §9; Canon §1, §4.2, §4.3, §4.5 |
| §11 Components | Canon §3.4 (Card, Pill, Dot — reused); M1 Scope Proposal §9.3 |
| §12 Mocked | M1 Scope Proposal §11, §12 |
| §13 Not implemented | M1 Scope Proposal §4, §10; Canon §11.5 |
| §14 DEMO labels | M1 Scope Proposal §9 (extending M0 Scope Proposal §9 labels) |
| §15 Beta 10 protection | M0 Scope Proposal §6; Sandbox Provisioning Plan §17 |
| §16 Railway deployment | Sandbox Provisioning Plan §6, §16.2; M0 Retrospective §5 |
| §17 Validation gates | M0 Implementation Plan v0.2 §12 (template); M1 Scope Proposal §15 (extended) |
| §18 Acceptance | M1 Scope Proposal §16 + Phase split |
| §19 Rollback | M0 Scope Proposal §19 |
| §20 Stop conditions | M0 Implementation Plan v0.2 §15; M1 Scope Proposal §18 |
| §21 Phase sequencing | M1 Scope Proposal §14 |

---

## 23. Recommendations

### 23.1 Adopt the M0 cycle wholesale (again)

This is the second milestone in the V1 sequence. The pattern
that worked for M0 should work for M1. Trust the cycle; don't
short-cut review.

### 23.2 Keep the adapter PR tight

M1 has more surface than M0 (adapter + tests + polling + UI
extensions). Resist scope creep — defer anything that wants to
add a second tab, a real ACK affordance, a Replay surface, etc.

### 23.3 Fixture capture is the load-bearing prep

The 3 captured fixtures determine what the adapter exercises
end-to-end. Capture diverse moments — busy Brisbane, quiet
Brisbane, Melbourne with different shape — to give the snapshot
tests real coverage.

### 23.4 The derived fixtures are the defence story

`null-fields.json` and `malformed.json` exercise the adapter's
defence paths per Adapter Note §15. Without them, the test
suite passes against happy-path only. Don't skip the derived
fixtures — they're cheap to produce and they catch real bugs.

### 23.5 The DesignVerificationSwatch removal is the right
M1 cleanup

Per M0 Retrospective §7.3 and M1 Scope Proposal §5 item 10. The
swatch was M0 scaffold; with real fixture-derived conflicts
flowing through the adapter, the swatch is redundant. Remove
it as part of M1 — don't carry it forward indefinitely.

### 23.6 Defer ACK / write paths past M1

Tony's directional decision stands: M1 read-only; ACK write
deferred to M1.5 / M2. Honouring this preserves the M1 review
surface (it's the adapter contract + UI extensions, no write
semantics).

### 23.7 M1 retrospective recommended at close

Per M0 Scope Proposal §23.7 — pause at M1 close, write a brief
retrospective, then authorise M2. The retrospective discipline
caught real issues in M0 and is worth repeating.

### 23.8 Document fixture source SHA in a code comment

When fixtures are committed, the commit message records *which
Beta 10 deploy* they were captured from (commit SHA of
`server.py` at capture time, or just the date of capture).
This makes "what does Beta 10 actually return today?" answerable
in 3 months without re-capturing.

### 23.9 Vitest snapshot updates require explicit authorisation

When adapter changes intentionally modify output shape (e.g.
M1.5 adds a new field), the Vitest snapshot needs an update
(`vitest --update-snapshots`). The update should land in its
own commit with a clear rationale, not silently as part of an
unrelated PR. The snapshot diff is part of the review record.

---

## End of M1 Implementation Plan v0.1

**Status:** v0.1 implementation plan — planning only
**Implementation status:** None
**Next action:** ChatGPT engineering review of the 23 sections,
then Tony's decision on (a) merging this plan, and (b)
authorising fixture-capture prep + M1 implementation start.

M0 is closed. M1 scope is contracted (PR #47). This plan is the
contract for M1 build. M1 implementation begins only on
Tony's explicit authorisation message citing this document.
