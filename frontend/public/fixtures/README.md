# Horizon V1 — M1 fixtures

**Status:** Captured + derived for M1 adapter unit tests + Phase 2
sandbox runtime polling. Local-only capture; **no Beta 10 production
data**; **no production HTTP calls** during capture.

**Capture date:** 2026-05-20
**Source commit:** `5b6a306` (`main` head of `project-horizon` repo
at capture time; M1 Implementation Plan v0.1 merge commit)
**Capture method:** local `server.py` instance via Option 4b of
`HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` (and the fixture
strategy comparison message that preceded it)

## Why these files exist

`HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` §16 requires the M1
adapter test suite to exercise fixture-based snapshot tests +
per-domain assertions + empty-state + defence + round-trip
behaviour. `HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` §6.3
specified three captured fixtures plus two derived edge-case
fixtures.

The Vite build copies anything under `frontend/public/` verbatim
into `frontend/dist/`, so at runtime the sandbox serves these
JSON files at `GET /fixtures/<name>.json` — same-origin to the
sandbox URL, no auth, no cross-origin or CORS complication. M1's
adapter pipeline polls these endpoints from the React app
exactly as documented in the M1 Scope Proposal §6 +
Implementation Plan §6.

## Capture environment

Local `python3.10 server.py` instance bound to
`127.0.0.1:19191`, started with:

```bash
PORT=19191 HORIZON_PORT=BRISBANE python3.10 server.py
```

Environment variables **deliberately unset** so the server falls
through to its simulated data sources:

- `DATABASE_URL` (unset → audit emission no-op, matching Beta 10
  production posture)
- `AISSTREAM_API_KEY` (unset → AISStream WebSocket disabled)
- `MST_API_KEY` (unset → MyShipTracking fallback disabled)
- `BOM_*`, `OPEN_METEO_*` (unset → simulation fallback for
  weather/tides)

Authentication used the default Beta 10 dev credentials
(`HORIZON_USER=horizon` / `HORIZON_PASS=ams2026`) — these are
the same defaults `server.py` ships with and are **not**
production credentials. The session cookie was discarded
immediately after capture.

The local server was shut down immediately after the three
captures completed. **No Beta 10 production endpoint was
contacted at any point during capture.**

## Captured fixtures (3)

| File | Port | Vessels | Conflicts | Notes |
|---|---|---|---|---|
| `brisbane-busy.json` | Brisbane | 13 | 9 | Default Brisbane simulation state; multiple berth-overlap conflicts visible |
| `melbourne-sim.json` | Melbourne | 9 | 18 | After local `/api/set_port {"port":"MELBOURNE"}`; smaller fleet (`sim_vessel_count=9`) but more dense conflict surface |
| `brisbane-quiet.json` | Brisbane | 13 | 0 | **Derived** from a fresh Brisbane capture by zeroing `conflicts: []`, `guidance: []`, and updating downstream counts (`port_status.active_conflicts`, `dashboard.active_conflicts`, etc.) — the Brisbane simulation always emits conflicts, so this fixture exists to exercise the empty-conflict-list code path. Counted in the "captured" set because the underlying vessel / weather / dashboard data is real local-server output. |

Top-level structure matches Beta 10's `/api/summary` response
(26 top-level keys per `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md`
§3).

`data_source: "mock"` and `data_source_label: "<port> —
Simulation"` confirm the simulated fallback was used (not real
AIS / MST / QShips data).

`weather.source` reports `"live"` (this is the `weather.py`
module's own self-label inside the simulated path — it indicates
the module *would* have used live data if API keys were
configured; in this capture environment it served the simulated
fallback). No real external weather data was retrieved.

## Derived fixtures (2)

| File | Derived from | Purpose |
|---|---|---|
| `null-fields.json` | `brisbane-busy.json` | Exercises adapter null-handling paths per `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` §15.2. Selected vessel + conflict + etd_risk fields are nulled or removed. |
| `malformed.json` | `brisbane-busy.json` | Exercises adapter defensive paths per Adapter Note §15.3 (wrong types substituted with defaults; unknown severity → fallback; missing top-level key handled). |

### `null-fields.json` — specific transformations applied

- `vessels[0].etd` → `null`
- `vessels[1].ata` → `null`, `vessels[1].atd` → `null`
- `vessels[2].notes` → `null`
- `vessels[3].source` → `null`
- `vessels[4].source` → key removed entirely
- `conflicts[0].berth_id` → `null`
- `conflicts[0].berth_name` → `null`
- `conflicts[1].decision_support` → `null` (and similarly for
  subsequent conflicts that had it set — total 8 affected to
  exercise the missing-`decision_support` code path)
- `etd_risk[0].risk_factors` → `[]`
- `etd_risk` array truncated by 2 entries from the end →
  exercises the risk-join missing-match path per Adapter Note
  §7.7 (vessels without a corresponding `etd_risk` entry should
  default to `riskScore: 0` / `riskLevel: 'low'` /
  `riskFactors: []`)

Still has 26 top-level keys. Still valid JSON.

### `malformed.json` — specific transformations applied

- `vessels[0].loa` → `"big"` (string instead of number)
- `vessels[1].draught` → `null` (number replaced with null)
- `conflicts[0].severity` → `"EXPLOSIVE"` (unknown value not in
  Adapter Note §14.1's closed severity set — should map to
  `"INFO"` fallback)
- `weather.visibility_nm` → `"foggy"` (string instead of number)
- `dashboard.berth_utilisation_pct` → `200` (out-of-range; valid
  type but UI should clamp for display)
- `etd_risk` → `{}` (object instead of array — adapter should
  treat as missing per Adapter Note §15.1 and return empty
  array)
- `port_profile` → key removed entirely (exercises partial-
  response handling per Adapter Note §15.6)

Has 25 top-level keys (`port_profile` removed).

## What is NOT in these fixtures

- **No real customer data.** All vessel names are simulated
  (per `port_profiles.py` defaults: "MV PACIFIC VOYAGER",
  "MV BRISBANE STAR", etc.)
- **No real operator handles.** Pilot / mooring-gang names are
  the simulated defaults
- **No real agent contacts.** `agent` fields are the simulated
  defaults
- **No production session cookies.** Local capture cookie
  discarded immediately
- **No production API keys.** None configured during capture
- **No production timestamps.** `generated_at` reflects local
  server clock at capture time, not Beta 10 production state
- **No live AIS / MST / BOM / Open-Meteo data.** All external
  sources fall through to simulation fallbacks

## How to re-capture

To regenerate the three captured fixtures (e.g. if Beta 10
evolves and the response shape changes):

```bash
# From repo root
cd /path/to/project-horizon

# Start local server
PORT=19191 HORIZON_PORT=BRISBANE python3.10 server.py &
SERVER_PID=$!
sleep 5

# Authenticate
COOKIE=$(curl -s -i -X POST -d "username=horizon&password=ams2026" \
  http://127.0.0.1:19191/login | grep -i "^Set-Cookie:" | head -1 \
  | tr -d '\r' | sed 's/Set-Cookie: //I; s/;.*//')

# Capture Brisbane
curl -s -b "$COOKIE" http://127.0.0.1:19191/api/summary \
  | jq '.' > frontend/public/fixtures/brisbane-busy.json

# Switch to Melbourne and capture
curl -s -b "$COOKIE" -X POST -H "Content-Type: application/json" \
  -d '{"port":"MELBOURNE"}' http://127.0.0.1:19191/api/set_port
sleep 2
curl -s -b "$COOKIE" http://127.0.0.1:19191/api/summary \
  | jq '.' > frontend/public/fixtures/melbourne-sim.json

# Derive brisbane-quiet from a fresh Brisbane capture by zeroing conflicts
curl -s -b "$COOKIE" -X POST -H "Content-Type: application/json" \
  -d '{"port":"BRISBANE"}' http://127.0.0.1:19191/api/set_port
sleep 2
curl -s -b "$COOKIE" http://127.0.0.1:19191/api/summary \
  | jq '.conflicts = [] | .guidance = [] | .port_status.active_conflicts = 0 | .port_status.critical_conflicts = 0 | .dashboard.active_conflicts = 0 | .dashboard.critical_conflicts = 0' \
  > frontend/public/fixtures/brisbane-quiet.json

# Shutdown
kill $SERVER_PID
unset COOKIE
```

Derived fixtures (`null-fields.json`, `malformed.json`) are
regenerated by applying the documented transformations to the
new `brisbane-busy.json`.

## Validation

Each fixture passes `jq empty`. All five fixtures together total
~990 KB (well under any LFS threshold). Top-level key counts:

| File | Top-level keys |
|---|---|
| `brisbane-busy.json` | 26 |
| `brisbane-quiet.json` | 26 |
| `melbourne-sim.json` | 26 |
| `null-fields.json` | 26 |
| `malformed.json` | 25 (`port_profile` intentionally removed) |

## Not for production use

These fixtures are **demo / test data only**. They exist solely
to exercise the M1 adapter against representative
`/api/summary` shape. They must not be served from
`horizon-prod`, must not be referenced by Beta 10's `server.py`,
and must not be treated as authoritative records of any real
operational moment.

The `frontend/public/fixtures/` path is consumed only by the
M1 React frontend at the sandbox URL
(`horizon-v1-sandbox-production.up.railway.app`). The sandbox
banner labels every visible data point as DEMO per
`HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` §11.5.
