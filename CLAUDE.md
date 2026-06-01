# Project Horizon — CLAUDE.md

## Read this before doing anything.

Horizon is a live port operations intelligence platform built by AMS Group.
It is deployed at: `https://project-horizon-production-a03c.up.railway.app`
GitHub: `doc291/project-horizon` (private, single `main` branch)

**Owner:** Tony Trajceski (tony@trajceski.net) — AMSG Horizon division founder.
Tony is non-technical but deeply knowledgeable about the maritime domain. Explain
changes in operational terms, not implementation jargon. Always get explicit approval
before deploying anything.

---

## Governance — Stakeholder Lens Contract (binding)

**Any stakeholder-lens work — new lens, existing lens edit, lens data layer,
lens renderer, lens acceptance harness — MUST read and comply with
[`docs/governance/LENS-CONTRACT.md`](docs/governance/LENS-CONTRACT.md) before
any code is written.**

A stakeholder lens is **not** complete because it renders correctly. A
stakeholder lens is complete **only** when it satisfies the Lens Contract and
represents the stakeholder's operational world — Sections A (Operational
Context), B (Current Watch/Shift), C (Forward Pressure), and D (Exceptions),
each per the contract.

Governance failures take precedence over UI polish. A lens that fails any of
acceptance criteria LC-1 through LC-14 is not shipped, demoed, or merged,
regardless of how good it looks.

Every commit touching a stakeholder lens MUST declare compliance in its
message in the form:

```
lens-contract: LC-1✓ LC-2✓ LC-3✓ LC-4✓ LC-5✓ LC-6✓ LC-7✓ LC-8✓ LC-9✓
                LC-10✓ LC-11✓ LC-12✓ LC-13✓ LC-14<status>
```

Amendments to the contract are recorded in
[`docs/governance/LENS-CONTRACT-CHANGELOG.md`](docs/governance/LENS-CONTRACT-CHANGELOG.md).
Silent edits to the contract are prohibited.

---

## Architecture (current state — Beta 10)

This is a **monolithic single-file application**. Everything lives in two files:

| File | Lines | Role |
|---|---|---|
| `server.py` | ~4,130 | Entire backend — HTTP server, auth, all API endpoints, conflict detection, guidance generation, weather/tides, decision support, What If scenario engine, Port Brief PDF generation, AIS/MST data integration, vessel simulation |
| `index.html` | ~3,110 | Entire frontend — dashboard, guidance panel, decision cards, vessel roster, berth timeline, conditions bar, DUKC, ESG, performance tabs, all CSS, all JS |

There is **no framework** (no Flask, no Django, no Express). The backend uses Python's
built-in `http.server.BaseHTTPRequestHandler` with `ThreadingHTTPServer`. The frontend
is vanilla HTML/CSS/JS with no build step.

### Supporting modules

| File | Purpose |
|---|---|
| `weather.py` | Live weather from Open-Meteo API (free, no key) + deterministic simulation fallback. 30-min cache per port. |
| `port_profiles.py` | Port definitions: lat/lon, berths, tidal params, wind limits, bridge restrictions. Ports: Brisbane (AUBNE), Melbourne (AUMEL), Geelong (AUGEX), Darwin (AUDRW). |
| `aisstream_scraper.py` | AISStream.io WebSocket connector — primary live vessel data source. Requires `AISSTREAM_API_KEY` env var. |
| `mst_scraper.py` | MarineTraffic expected arrivals API — secondary vessel data. Requires `MST_API_KEY`. 12-hour cache (`_MST_REFRESH_HOURS = 12` in server.py line ~133). |
| `bom_tides.py` | Australian BOM tidal data scraper — live tide heights and predictions. |
| `vessel_scraper.py` | Ports Victoria QShips vessel scraper (Melbourne/Geelong only). |
| `qships_scraper.py` | Alternative QShips scraper path. |

### Data source priority

When `build_summary()` assembles port state (server.py ~line 2280):
1. **AISStream** (if configured and not stale) — real-time AIS positions
2. **MST cache** (if available) — expected arrivals from MarineTraffic
3. **Vessel scraper** — QShips data for Melbourne/Geelong
4. **Simulation** — deterministic fake inbound vessels seeded by port UNLOCODE

In production, AISStream is the active primary source. Simulated vessels have IDs
prefixed with `SIM-` and `vessel.source = "sim"`. Live vessels have `source = "ais"`
or `"mst"`.

### Simulated data (important to understand)

Horizon generates **simulated inbound vessels** in every port. These are deliberately
assigned to occupied berths to create realistic berth conflicts for demo purposes.
This means:

- Most "critical conflicts" involve one real berthed vessel + one simulated inbound
- Decision cards for these conflicts show "Live Data" provenance (misleading — known issue)
- The vessel roster shows a `LIVE AIS + SIMULATED OPS` label to flag this mix
- Weather and tides are real (Open-Meteo / BOM). Conflicts and arrivals are simulated.

---

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `HORIZON_PORT` | No (default: BRISBANE) | Active port on startup. Values: BRISBANE, MELBOURNE, GEELONG, DARWIN |
| `PORT` | No (default: 8000) | HTTP port |
| `HORIZON_USER` | No (default: horizon) | Login username |
| `HORIZON_PASS` | No (default: ams2026) | Login password |
| `AISSTREAM_API_KEY` | Yes for live AIS | AISStream.io WebSocket API key |
| `MST_API_KEY` | Yes for MarineTraffic | MarineTraffic expected arrivals API key |
| `SMTP_HOST` | No | Email server for Port Brief delivery |
| `SMTP_PORT` | No (default: 587) | SMTP port |
| `SMTP_USER` | No | SMTP username |
| `SMTP_PASS` | No | SMTP password |
| `SMTP_FROM` | No | Sender address for Port Brief emails |
| `BRIEF_RECIPIENTS` | No | Comma-separated email list for Port Brief |

---

## API endpoints

### Public (no auth)
- `GET /login` — login page
- `GET /health` — system status page (pre-demo check)
- `GET /api/health-data` — JSON diagnostic data

### Authenticated (session cookie)
- `GET /` — main dashboard (serves index.html)
- `GET /api/summary` — full port state JSON (vessels, berths, conflicts, guidance, weather, tides, pilotage, towage, dashboard metrics, ETD risk, DUKC)
- `GET /api/port-brief` — PDF Port Brief download
- `GET /api/brief-config` — email configuration for Port Brief
- `GET /api/diag` — diagnostic info
- `GET /api/debug` — debug data
- `GET /api/scrape` — trigger vessel scrape
- `GET /api/aisstream-status` — AISStream connection status
- `GET /api/mst-status` — MST cache status
- `GET /mobile` — mobile companion app
- `POST /api/set_port` — switch active port
- `POST /api/whatif` — run shadow scenario (What If / Scenario Builder)
- `POST /api/apply-whatif` — apply scenario overlay to live state
- `POST /api/clear-whatif` — clear scenario overlay
- `POST /api/send-brief` — email Port Brief to recipients

---

## Deployment

- **Platform:** Railway (railway.app), auto-deploy from `main` branch
- **Build:** `pip install -r requirements.txt && playwright install chromium --with-deps || true`
- **Start:** `python3 server.py`
- **Single service, single instance, no database**
- **No tags, no branch protection, no .gitignore** (these should be added)
- `__pycache__/server.cpython-310.pyc` is tracked in git (shouldn't be — add to .gitignore)

### To deploy
Push to `main` → Railway auto-deploys. There is no staging environment.

### To redeploy without code changes
Trigger manual redeploy in Railway dashboard. This resets all in-memory caches
(MST, weather, AIS connections).

---

## Key backend functions (server.py)

| Function | ~Line | Purpose |
|---|---|---|
| `build_summary()` | 2280 | Central data assembly — everything the frontend needs |
| `detect_conflicts()` | 952 | Operational conflict detection (berth overlap, berth not ready, pilotage window, towage, ETA variance, bridge restrictions) |
| `detect_weather_alerts()` | 1463 | Weather alert generation (wind, swell, visibility, bridge air draft) |
| `build_guidance()` | 1179 | Generates guidance panel items from conflicts + proactive alerts |
| `_conditions()` | In weather.py:37 | Operational conditions rating (Excellent/Good/Moderate/Poor) |
| `fetch_weather()` | In weather.py:94 | Open-Meteo live fetch + cache + simulation fallback |
| `make_tides()` | 1637 | Tide data assembly (BOM live + cosine model fallback) |
| `_safety_score_for_conflict()` | 1916 | Safety score per conflict |
| `compute_etd_risk()` | ~2100 | ETD risk ranking for all vessels |
| `make_berth_utilisation()` | ~2050 | Berth utilisation metrics |

### Conditions thresholds (weather.py `_conditions()`)
These are **universal across all ports** (not port-specific):
- **Excellent:** wind < 10 kts AND swell < 1.0m AND no precipitation
- **Good:** wind < 16 kts AND swell < 1.5m
- **Moderate:** wind < 22 kts AND swell < 2.0m
- **Poor:** everything else

The `wind_limit_berthing` and `wind_limit_critical` values in port_profiles.py are
used for berthing restriction alerts, NOT for the conditions rating.

---

## Key frontend sections (index.html)

| Section | ~Line | Notes |
|---|---|---|
| Conditions bar CSS | 105-124 | `.cbar-cond.Good`, `.cbar-cond.Poor`, etc. |
| Guidance item CSS | 240-249 | `.guidance-item`, severity colours |
| Guidance category CSS | 248 | `.guidance-cat-label` (new — uncommitted) |
| Decision card CSS | 251-264 | `.conflict-card`, `.sev-badge`, `.signal-badge` |
| `renderGuidance()` | 1733 | Guidance panel renderer |
| `renderDecisions()` | ~1757 | Decision card renderer |
| `renderConditionsBar()` | ~1683 | Weather conditions display |
| Vessel roster renderer | ~2720 | Table rows with status pills |
| Vessel roster source label | ~2705 | Dynamic `LIVE AIS + SIMULATED OPS` label |
| Port Brief email section | ~3280 | DSW render function for embedded JS |

---

## Ports

| Port | ID | UNLOCODE | Coordinates | Notes |
|---|---|---|---|---|
| Brisbane | BRISBANE | AUBNE | -27.38, 153.17 | Default port. 10 berths. |
| Melbourne | MELBOURNE | AUMEL | -37.82, 144.92 | 6 berths. Bolte Bridge restrictions. QShips data. |
| Geelong | GEELONG | AUGEX | -38.13, 144.35 | 6 berths. Lower wind thresholds. Shares BOM station with Melbourne. More exposed to Southern Ocean swell. |
| Darwin | DARWIN | AUDRW | -12.45, 130.84 | 7 berths. Cyclone season Nov-Apr. |

---

## Uncommitted changes (as of handoff)

**Guidance category labels** — adds a domain category (BERTH, WEATHER, NAVIGATION, RESOURCES, OPS) beside each guidance item's severity label. Changes:
- `server.py`: `_GUIDANCE_CATEGORY` dict + `"category"` key in `build_guidance()`
- `index.html`: `.guidance-cat-label` CSS + ternary render in `renderGuidance()`
- Ready to commit and deploy. Tested via code inspection (could not run locally).

---

## Known issues

1. **Decision card provenance badge misleading** — shows "Live Data" for conflicts involving simulated vessels. The `data_source` field on conflicts is set to "live" when the berthed vessel is real, even if the inbound vessel is simulated. Fix: check if any `vessel_id` has `SIM-` prefix and downgrade provenance label.

2. **No .gitignore** — `__pycache__/*.pyc` is tracked. Multiple backup .zip files and .docx files are untracked in the working directory.

3. **No tags or branches** — all work is on `main`. Should tag Beta 10 (`git tag -a beta-10 8a26fc3`), create a protected demo branch, and separate development.

4. **MST cache TTL is 12 hours** — `_MST_REFRESH_HOURS = 12` (server.py ~line 133). Plan was to reduce to 2 hours post-demo. Only matters if AISStream is down (MST is the fallback).

5. **Docstring says "Beta 9"** — server.py line 3 still says Beta 9. Should be updated.

6. **Single monolith** — server.py at 4,130 lines is hard to navigate. Future refactoring should extract conflict detection, guidance generation, and weather into proper modules.

7. **Geelong weather coordinates** — the coordinates (-38.128, 144.352) are in open water west of Corio Bay. If Poseidon's operations are inside the inner harbour, these may need refinement for more representative swell readings.

---

## Post-demo work queue (approved or discussed, not yet implemented)

| Item | Status | Notes |
|---|---|---|
| Commit guidance category labels | Ready | Uncommitted changes in server.py + index.html |
| Tag Beta 10 | Approved | `git tag -a beta-10 8a26fc3` |
| Create .gitignore | Approved | Exclude `__pycache__/`, `.DS_Store`, `*.zip`, backup files |
| Fix provenance badge | Assessed, paused | Check for SIM- prefix in vessel_ids, downgrade badge |
| Reduce MST cache TTL | Approved post-demo | Change `_MST_REFRESH_HOURS` from 12 to 2 |
| Create demo branch | Recommended | Separate Railway services for demo vs dev |
| AWS migration | Under discussion | Move from Railway to AMSG AWS environment |
| Kyber integration | Pre-build phase | See Project Kyber folder — pilotage operating system |
| Cargo demand intelligence (V2) | Roadmap | Brisbane construction MVP first |

---

## Hard rules

1. **Never deploy without Tony's explicit approval.** Always report what will change and wait for "approved" or "go".
2. **Never modify conflict detection, decision support, or What If logic** without thorough assessment and approval. These are demo-critical.
3. **Always test across all four ports** (Brisbane, Melbourne, Geelong, Darwin) after any change.
4. **The frontend displays backend values directly** — conditions, guidance, decisions are all calculated server-side. The frontend is a renderer, not a calculator.
5. **Simulated vessels have `SIM-` prefix IDs and `source: "sim"`** — use these markers to distinguish real from simulated data.
6. **Weather and tides are real data** — Open-Meteo and BOM. Don't confuse these with the simulated vessel/conflict data.
7. **Port switching happens via `POST /api/set_port`** — this changes a global variable. The server serves one port at a time.
8. **Git operations may fail from mounted VMs** — lock files can appear in `.git/`. If git commands fail, ask Tony to run them from his local terminal.

---

## Related projects (same parent folder)

- **Project Kyber** — Pilotage Operating System. Has its own CLAUDE.md, CONTEXT.md, INTEGRATION_CONTRACT.md. Separate codebase, separate Railway deployment (planned). See Kyber folder.
- **horizon-lab/** — Prototype screens (screen-0 through screen-3) and website design files. Not deployed.

---

## Running locally

```bash
# No external dependencies needed for basic operation (uses stdlib http.server)
# But requirements.txt has: beautifulsoup4, requests, reportlab, websocket-client
pip install -r requirements.txt

# Default port (Brisbane)
python3 server.py

# Specific port
HORIZON_PORT=MELBOURNE python3 server.py

# Open http://localhost:8000
# Login: horizon / ams2026 (defaults)
```

Note: AISStream and MST data require API keys. Without them, the app falls back to
simulation for vessel data. Weather and tides work without keys (Open-Meteo is free,
BOM scraper needs no auth).
