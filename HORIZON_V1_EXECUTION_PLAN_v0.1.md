# Horizon V1 — Execution Plan & Engineering Roadmap (v0.1)

**Status:** Engineering execution plan — V1 implementation roadmap
**Document version:** 0.1
**Date:** 2026-05-15
**Authoritative inputs (merged on `main`):**
- The V1 design foundation on `main`:
  - `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30, `4c4940f`) — WHO
  - `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31, `6d09b3d`) — HOW
  - `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` (PR #32, `3e747dc`) — WHERE
  - `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md` (PR #33, `406de24`) — WHAT
  - `HORIZON_V1_IMPLEMENTATION_STRATEGY_v0.1.md` (PR #34, `e2284d4`) — HOW TO BUILD
- The V1 stakeholder review pack: `HORIZON_V1_REVIEW_PACK_v0.1.md` (PR #35, open)

**Pending inputs (NOT yet authorised — referenced as planning assumptions only):**
- Claude Design UX/UI handoff in `v1-handoff/`:
  - `V1-Implementation-Prompt.md` — React frontend specification
  - `prototype/` — reference JSX, complete CSS, design tokens, 13 screenshots

> **UX/UI handoff authorisation status — PENDING.** As of this
> document's date, the `v1-handoff/` package has **not been formally
> authorised by Tony** as an implementation input. Every reference to
> the handoff in this document is a **planning assumption only**.
> No frontend build work, no `frontend/` directory creation, no
> `package.json`, no design-token extraction, no React component
> scaffolding, and no `server.py` static-file routing change may
> begin from this document until the handoff is **separately and
> explicitly authorised**. If the handoff is later modified, replaced,
> or rejected, the sections that reference it (notably §5, §11
> milestones M0–M9, §13, §15) must be revised before any
> implementation can be authorised against them.

**Implementation status:** None. **This is not implementation approval.**
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

This document is the **engineering execution plan** for Horizon V1.
Where the V1 foundation (five v0.1 documents) defines WHO / HOW /
WHERE / WHAT / HOW TO BUILD, this document defines **the actual
implementation sequence Claude Code and future engineers should
follow** to get from Beta 10 to V1.0, with every milestone, gating
rule, rollback expectation, and governance constraint made explicit.

The plan reconciles two inputs that arrived separately:

1. **The V1 architecture foundation** (PRs #30–#35) — the formal
   design quintet establishing role-based access, structured
   operational workflows, role-specific screen surfaces,
   information-architecture substrate, and engineering strategy.
   **Status:** merged on `main` (PR #35 open for review).

2. **The Claude Design frontend handoff** (`v1-handoff/`) — a
   React frontend specification with a 4-week phased plan that
   would replace `index.html` against the existing Beta 10 backend
   (server.py untouched, /api/summary unchanged).
   **Status:** **PENDING formal authorisation.** Referenced here
   as a planning assumption only — not as an implementation
   instruction.

If both inputs are accepted, they are **complementary, not
competing**. The handoff would then become the **immediate V1.0
deliverable** — a production-grade React frontend replacing the
Beta 10 monolithic HTML, riding on the existing API during the
transition. The architecture foundation's broader work (per-user
RBAC, role-scoped `/api/summary` projection, multi-tenant
isolation, persistent operational state) is the **next phase** —
introduced incrementally once the frontend foundation is stable.

This is the **frontend-first replacement strategy** that *would*
let V1 start delivering customer-visible value within weeks rather
than quarters, while keeping the backend evolution gated behind
explicit authorisation per the Implementation Strategy §16
governance rules. The strategy is **contingent on formal
authorisation of the UX/UI handoff** — if that handoff is rejected
or materially changed, §5 and the M0–M5 milestones must be revised
before any frontend work can be authorised.

**This document does not authorise implementation.** It is v0.1 of
an evolving execution plan. Two gates must close before V1.0
implementation can begin:

1. The V1 foundation must be reviewed and accepted (per Review
   Pack §10).
2. The Claude Design UX/UI handoff (`v1-handoff/`) must be
   formally authorised as an implementation input by Tony.

Until both gates close, this execution plan is a planning artefact
only. It does **not** approve frontend build work, server.py
modifications, environment provisioning, or repository structure
changes.

---

## 2. Current Technical Baseline

What exists today, what is locked, and what is in flight.

### 2.1 Beta 10 architecture (Phase 0)

| Property | State |
|---|---|
| `phase-0-complete` tag | `4ad4aae` (immutable Beta 10 baseline) |
| `origin/main` HEAD | `e2284d4` (foundation quintet + drift through #35) |
| Python runtime | 3.10.20 (matches Railway production) |
| Backend | Single file `server.py` (~4,130 lines), `BaseHTTPRequestHandler` + `ThreadingHTTPServer` |
| Frontend | Single file `index.html` (~3,110 lines), vanilla HTML/CSS/JS, no build |
| Auth | Shared single-user via `HORIZON_USER`/`HORIZON_PASS` env vars |
| Deploy | Railway, auto-deploy from `main` |
| Marketing site | `horizon.ams.group` from `deploy/` (separate host routing) |
| Database | **No `DATABASE_URL` set in production** (deliberate; Phase 0 posture) |
| Audit emission | No-op in production (`AUDIT_EMISSION_ENABLED` not configured) |
| Regression gate | `tests/test_beta10_regression.py` — 46/46 passing |

### 2.2 server.py — what it owns today

- HTTP server (auth, request routing, response generation)
- Vessel state management (AISStream live, MST cache, QShips fallback, simulation)
- Berth state management (hardcoded `port_profiles.py`)
- Conflict detection engine (`detect_conflicts()`)
- Recommendation generation (`_build_decision_support()`)
- Weather and tide integration (BOM)
- What-if scenario engine (shadow simulation)
- Decision support synthesis (sequencing alternatives)
- All `/api/*` endpoints
- Authentication and session management
- Port Brief PDF generation
- Security headers on every response (6 headers per Phase 0 hardening)

**This file is protected.** V1 work does not modify it. See §6.

### 2.3 index.html — what it owns today

- Server-rendered shell (text/html response)
- Embedded JavaScript polling `/api/summary` every ~30 seconds
- Vessel map rendering (SVG)
- Conflict cards, decision cards, guidance feed
- Per-port profile rendering
- Weather/tide visualisations
- Vessel roster + search
- What-if scenario UI

**This file is the replacement target.** V1 work introduces a new
React frontend in `frontend/` that progressively replaces it.

### 2.4 Audit preview validation status

Phase 1.2 (preview-only audit activation) completed in May 2026:

| Stage | Status |
|---|---|
| Stage A (psycopg runtime dep) | ✅ merged via PR #23 |
| Stage B-preview (Postgres provisioned) | ✅ Tony-confirmed |
| Stage C-preview (Alembic migrations) | ✅ via PR #27 deploy runner |
| Stage D-preview (smoke test, 6 events) | ✅ via PR #28 deploy runner |
| Drill-preview (rollback validation, 6 events) | ✅ via PR #29 deploy runner |
| Preview re-pointed to `main` | ✅ Tony-confirmed |
| Stage E-prod (production activation) | ⏸ **PAUSED** per V1 RBAC recommendation |

The preview environment has a fully migrated audit schema with 12
activation-evidence rows in the per-tenant chain. Stage E-prod
remains paused until V1 RBAC ships, per the consistent recommendation
across all five foundation documents.

### 2.5 Regression gate

`tests/test_beta10_regression.py` locks the Beta 10 baseline. 46
test methods across 8 test classes. Validates:

- `build_summary()` shape and key set
- Security headers on every response (6 exact)
- Public paths set unchanged
- All protected GETs redirect 302 to `/login?next=...`
- All 5 protected POSTs return 401 unauthenticated
- Asset SHA-256 hashes (logo.png, logo.svg, amsg-logo.png,
  mobile-icon.png)
- All 5 audit helpers no-op under DATABASE_URL unset + psycopg
  blocked
- Source-level event_type scope (only 6 authorised events)

**The gate is the protected invariant.** Any V1 change that breaks
it requires deliberate reviewer authorisation to update the baseline,
not a silent gate adjustment.

### 2.6 Protected surfaces

The following files / directories are **protected** during V1 work:

| Asset | Why |
|---|---|
| `server.py` | Beta 10 backend; V1 frontend rides on it unchanged |
| `audit.py` | Phase 0.5b writer; locked |
| 5 audit helper modules | Phase 0.7a/b/c, 0.8a baseline locked |
| `db.py`, `tenant.py` | Phase 0 foundation |
| `index.html` | Beta 10 frontend; coexists with React during V1 |
| `migrations/versions/0001*–0004*` | Phase 0 migration set |
| `tests/test_beta10_regression.py` | The regression gate itself |
| `port_profiles.py` | Beta 10 port definitions |
| `requirements.txt` | Beta 10 runtime deps (alembic, sqlalchemy already added) |
| `railway.toml`, `Procfile` | Beta 10 deploy config |
| `deploy/*` | Marketing site assets |

These files **MUST NOT be modified** by V1 implementation work
unless explicit reviewer authorisation grants the exception.

---

## 3. V1.0 Target Outcome

What "V1.0 complete" means in operational and technical terms.

### 3.1 Operational outcome

V1.0 ships when an AMS Group customer can:

- Log in with their own user credentials (not shared)
- See operational data filtered to their role and scope
- Acknowledge, resolve, escalate, and (HM-level) approve / override
  recommendations
- Compose and accept structured shift handovers
- View an executive trend dashboard
- Coordinate with pilotage, towage, mooring, and terminal staff via
  the stakeholder feed
- Replay any incident from the audit ledger with chain integrity

V1.0 is **functionally validated** when a real Harbour Master, VTSO,
and Shift Supervisor (operational stakeholder review per Review Pack
§10.2) can drive a complete shift end-to-end without leaving the
application.

### 3.2 Technical outcome

V1.0 ships when:

- The React frontend (per `v1-handoff/`) is operational and replaces
  `index.html` for V1 routes
- The role-scoped `/api/summary` endpoint (per Permission Model §8)
  is server-side enforced
- Per-user RBAC schema (per Implementation Strategy §17.3) is migrated
  and seeded
- The Phase 0 deferred event types (`OPERATOR_ACKNOWLEDGED`,
  `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION`, plus V1 candidates) are emitting
  per workflow lifecycle
- The audit chain integrity check (`verify_chain`) passes on the
  V1.0 production audit ledger
- The Beta 10 regression gate **still passes** alongside V1-specific
  regression gates
- V1.0 deploys to a separate Railway project (`horizon-v1-prod`)
  with its own Postgres
- Beta 10 production (`horizon-prod`) continues operating unchanged

### 3.3 What V1.0 does NOT have to deliver

Per Implementation Strategy §19.5:

- AI features (substrate ready; consumption is V1.x+)
- Cross-tenant federation (V2+)
- Native mobile apps beyond stakeholder PWA
- Customer-tenanted infrastructure beyond Railway
- Authority delegation (acting HM)
- External regulator / auditor read access
- Multi-monitor / kiosk display modes

These are V1.x or V2+ scope.

### 3.4 Customer-readiness criterion

V1.0 is **customer-ready** when:

- A demonstration to an actual maritime operations team produces
  enthusiastic operational endorsement (not just polite interest)
- A first customer engagement can be scoped without "we'll need to
  build that" caveats for core operational verbs
- The audit ledger is sufficient for a regulatory dry-run review
- The rollback procedures have been validated in preview

---

## 4. Implementation Principles

Seven principles govern all V1 implementation work. Where any
specific decision conflicts with these principles, the principle
wins.

### 4.1 Beta 10 protection first

`phase-0-complete @ 4ad4aae` and `tests/test_beta10_regression.py`
are non-negotiable. V1 work never breaks the regression gate
silently. Baseline updates are reviewer-authorised, deliberate, and
documented per PR.

### 4.2 No direct-to-production

Every V1 change flows through preview environments with explicit
acceptance gates before production. The Phase 1.2 audit activation
pattern (Stage A → B-preview → C-preview → D-preview → Drill →
B-prod → C-prod → D-prod → E-prod) generalises to all V1 changes.
No exception.

### 4.3 Frontend-first replacement strategy

V1.0 ships a new React frontend that **rides on the existing Beta 10
backend API** during the transition. `server.py` is not rewritten in
V1.0; it continues serving `/api/summary` exactly as Beta 10 does.
Backend evolution (per-user auth, role-scoped projection, RBAC
schema) is sequenced AFTER the frontend foundation is stable.

This is the most controversial principle: it temporarily defers some
of what Implementation Strategy §17.1 calls "rewrite `server.py`"
into V1.x rather than V1.0. The rationale:

- A working React frontend on the existing API is a **shippable
  customer-visible improvement** delivered in weeks
- Rewriting `server.py` at the same time as introducing React is
  two coupled changes — failure mode is "everything broken at once"
- Backend evolution can be staged behind feature flags once the
  frontend is operationally proven
- The regression gate continues to validate Beta 10 backend
  throughout V1 frontend work, providing a known-good fallback

### 4.4 Backend compatibility first

All V1 frontend code consumes the **existing** `/api/summary`
response shape. When the backend evolves (role-scoped filtering,
new fields, lifecycle states), the changes are **additive**:

- Existing fields keep their semantics
- New fields appear; old fields persist for one-version-back
  compatibility
- Old endpoints continue to respond until V1 has fully migrated off
  them
- API breakage is a deliberate cross-cutting concern, not an
  incidental side effect

### 4.5 No breaking API changes

For the lifetime of V1.0:

- `/api/summary` returns the same top-level shape Beta 10 returns
  (additive only)
- `/api/whatif`, `/api/apply-whatif`, `/api/clear-whatif` continue
  serving the same contract
- Authentication continues to accept the Beta 10 cookie format
  (until per-user auth is enabled, when it accepts BOTH formats for
  a transition window)
- `/api/health-data` shape stable
- Asset endpoints (`/logo`, `/amsg-logo`, `/favicon.ico`) unchanged

V2 may introduce breaking changes; V1.0 does not.

### 4.6 Progressive replacement over big-bang rewrite

Each V1 capability replaces a Beta 10 capability one at a time, with
both running concurrently during the transition:

- The React frontend coexists with `index.html` (different routes
  initially)
- Per-user auth coexists with shared-session auth (different login
  paths)
- Role-scoped `/api/summary` coexists with un-filtered `/api/summary`
  (route prefix differentiates)
- Persistent operational state coexists with in-process globals
  (V1 reads from DB, Beta 10 from globals)

Each replacement is an independently reviewable PR. The whole system
is never simultaneously in-flight.

### 4.7 Governance before speed

Every V1 implementation decision routes through the governance pattern
established in Phase 0 / Phase 1:

- Claude proposes a plan
- Tony authorises the plan
- Claude implements the plan
- Claude reports back with diff, tests, validation
- Tony reviews and authorises merge
- Production deploys are gated on preview + acceptance

The pattern adds 1-2 days per major change versus "just implement
it" but has prevented every silent regression in Phase 0 and Phase
1. The discipline scales to V1.

---

## 5. Frontend Migration Strategy

> **Pending-input notice.** This entire section is contingent on
> formal authorisation of the Claude Design UX/UI handoff
> (`v1-handoff/`). Every directive below — Vite, `frontend/`
> directory, design-token extraction, component structure, route
> prefixes — is described as a **planning assumption** based on
> the handoff's current content. None of it is authorised for
> implementation. If the handoff is amended or rejected during
> review, this section must be revised before any milestone in
> §11 can be authorised.

The React frontend introduction would be the single largest V1.0
deliverable. This section defines, conditionally, how it would
happen if the handoff is authorised as-is.

### 5.1 React frontend introduction

Assuming the handoff is authorised, per
`v1-handoff/V1-Implementation-Prompt.md`:

- New directory `frontend/` at the repo root
- Vite-based React app (build output to `frontend/dist/`)
- Design tokens, component library, and layout grid per the
  prototype CSS in `v1-handoff/prototype/Horizon V1.html`
- All visual references from `v1-handoff/prototype/screenshots/`

The React app is **client-side rendered** and consumes the existing
`/api/summary` JSON contract.

### 5.2 Coexistence with existing index.html

Both frontends ship in the same Beta 10 deploy initially:

- `index.html` continues to serve at `/` (Beta 10 route)
- React frontend serves at `/v1/` (new route prefix)
- Routes for new V1 features (login flow, dashboards, DSW) live
  under `/v1/*`
- Beta 10 routes (`/api/*`, `/login`, etc.) continue unchanged
- The frontend selection is per-user (V1 routes serve the React
  app; Beta 10 routes serve the existing experience)

This coexistence means **a customer demo can use either UI** during
the V1.0 transition, supporting both legacy and new-customer
demonstrations.

### 5.3 Routing strategy

```
/                            → index.html (Beta 10)
/login                       → Beta 10 login form
/api/*                       → Beta 10 endpoints (unchanged)

/v1/                         → React app (V1.0)
/v1/login                    → React login flow
/v1/dashboard                → React dashboard
/v1/dsw/:conflict_id         → Decision Support Window
... etc per the prototype
```

`server.py` is modified **only** to serve the React app's static
build at `/v1/*` and to serve the SPA index.html for client-side
routing. **No business logic in server.py changes.**

### 5.4 API compatibility strategy

The React app consumes:

- `GET /api/summary` — the primary data source (per the pending V1-Implementation-Prompt.md §2)
- `POST /api/set_port` — port switching
- `POST /api/whatif` / `apply-whatif` / `clear-whatif` — scenario engine
- `GET /api/port-brief` — PDF generation
- `GET /api/aisstream-status`, `/api/mst-status` — data source status

All endpoints respond exactly as Beta 10 does. The React frontend
adapts to the existing shape; the backend does not adapt to the
frontend.

### 5.5 CSS / design-token migration

The complete design system is provided in the pending handoff at
`v1-handoff/prototype/Horizon V1.html` as a `<style>` block. If the
handoff is authorised, the migration would be:

1. Extract the `<style>` block to `frontend/src/styles/tokens.css`
   and `frontend/src/styles/base.css`
2. Beta 10's `index.html` CSS is **not modified** (token migration is
   a V1 concern; Beta 10's tokens stay frozen)
3. The V1 design system is **distinct from Beta 10's** — different
   primary accent (teal vs sky blue), different background depth
   system, different typography
4. V1 frontend never imports Beta 10 CSS

### 5.6 State management strategy

Assuming the handoff is authorised, per
`v1-handoff/V1-Implementation-Prompt.md`:

- **No external state library required for V1.0.** React local state
  + `useReducer` for complex state + a custom `useSummary()` hook
  for polling
- The /api/summary response is the **canonical state**; the React
  app caches it briefly between polls
- Future V1.x may introduce Zustand or Redux if state complexity
  warrants (deferred design decision per Open Question §17.1)

### 5.7 Polling vs live updates

V1.0 uses **polling** (30-second cadence, per Beta 10's pattern):

- Simple, well-understood
- Survives intermittent connectivity
- No server-side push infrastructure needed
- Matches the Beta 10 backend's existing pattern

V1.x may introduce SSE or WebSocket for sub-second updates
(deferred per Notification Model §12.10 + Implementation Strategy
§20.4). The architecture is open; the V1.0 implementation is
polling.

### 5.8 Role-scoped rendering strategy

V1.0 ships **client-side role-scoped rendering** initially:

- The login response includes a role claim
- The React app uses the role claim to decide which screens / panels
  / actions to render
- This is **not yet server-side filtering** — `/api/summary` returns
  the same data regardless of role
- Server-side filtering (per Permission Model §8) is layered in
  during V1.1+ (per the milestones in §11)

**Important caveat:** client-side rendering alone is NOT a security
boundary. V1.0's client-side role filtering is an ergonomic
arrangement, not an authorisation enforcement. Server-side enforcement
arrives in V1.1 before any customer engagement.

---

## 6. Backend Compatibility Strategy

How the Beta 10 backend continues serving V1 without modification.

### 6.1 Existing /api/summary compatibility

`/api/summary` returns Beta 10's existing shape unchanged. The React
frontend consumes:

```json
{
  "port": "BRISBANE",
  "port_name": "Port of Brisbane",
  "conditions": { ... },
  "vessels": [ ... ],
  "berths": [ ... ],
  "conflicts": [ ... ],
  "guidance": [ ... ],
  "weather_alerts": [ ... ],
  "pilotage": [ ... ],
  "towage": [ ... ],
  "dashboard_metrics": { ... },
  "etd_risk": [ ... ]
}
```

**Same shape, same fields, same semantics** as today. No additions in
V1.0.

### 6.2 Future /api/v1/* evolution

In V1.1+, new endpoints appear under `/api/v1/*` for:

- Per-user authentication (`/api/v1/auth/login`,
  `/api/v1/auth/logout`)
- Role-scoped summary (`/api/v1/summary` with server-side filtering)
- Action endpoints (`/api/v1/actions/acknowledge`, `resolve`, etc.)
- Shift management (`/api/v1/shifts`)
- Audit reads (`/api/v1/audit`)

These coexist with Beta 10 endpoints. Beta 10 endpoints are NOT
deprecated, replaced, or removed during V1.0.

### 6.3 Backward compatibility expectations

- For each Beta 10 endpoint that gets a V1 counterpart, both serve
  concurrently
- Removing a Beta 10 endpoint is a **breaking change** and requires
  explicit reviewer authorisation
- A removed endpoint is replaced with HTTP 410 Gone + redirect
  guidance, not silent 404
- The transition window for any endpoint removal is at least one
  V1.x release cycle

### 6.4 Protected endpoints

These endpoints are NEVER modified or removed during V1:

- `/health`, `/api/health-data` — public; Phase 0 validation gate
- `/logo`, `/amsg-logo`, `/favicon.ico`, `/apple-touch-icon.png` —
  assets; SHA-256 hash-locked per regression gate
- `/login`, `/logout` — auth surface
- The marketing site at `horizon.ams.group` — separate host routing

### 6.5 Auth transition strategy

V1.0 introduces per-user auth **alongside** Beta 10's shared auth:

1. **Phase A (M1):** React app uses Beta 10's `/login` endpoint with
   the shared `HORIZON_USER`/`HORIZON_PASS`. Sessions function as
   today. Role claim is hardcoded to `vtso` (or similar) for
   initial testing.
2. **Phase B (M5):** Per-user auth introduced via new
   `/api/v1/auth/login` endpoint. RBAC schema migrated (per
   Implementation Strategy §6.3 sub-steps). Both auth paths work
   concurrently.
3. **Phase C (V1.x):** Beta 10 shared auth retired from V1 routes
   (still works for Beta 10 routes). React app exclusively uses
   per-user auth.

### 6.6 Recommendation / action endpoint evolution

In V1.0 (M0-M4), the React frontend uses Beta 10's existing
endpoints. The Decision Support Window (DSW) reads conflict /
recommendation data from `/api/summary` and applies what-if
scenarios via `/api/apply-whatif`.

In V1.1+ (M7), the action endpoints arrive:

- `POST /api/v1/actions/acknowledge` — VTSO acknowledges a
  recommendation
- `POST /api/v1/actions/resolve` — VTSO resolves a conflict
- `POST /api/v1/actions/defer` — HM defers a recommendation
- `POST /api/v1/actions/override` — HM overrides a recommendation
- `POST /api/v1/actions/escalate` — VTSO/SS escalates

Each endpoint emits its corresponding audit event per Workflow
Model §14.3 and Information Architecture §12.8.

---

## 7. Recommended Repository Structure

How the repo evolves to accommodate V1 alongside Beta 10. This is
**proposed**, not implemented.

### 7.1 Current Beta 10 structure (preserved)

```
/
├── server.py                      ← Beta 10 backend (PROTECTED)
├── index.html                     ← Beta 10 frontend (PROTECTED)
├── port_profiles.py               ← Beta 10 port data (PROTECTED)
├── audit.py                       ← Phase 0 audit writer (PROTECTED)
├── session_audit.py               ← Phase 0 helper (PROTECTED)
├── conflict_audit.py              ← Phase 0 helper (PROTECTED)
├── recommendation_audit.py        ← Phase 0 helper (PROTECTED)
├── recommendation_presented_audit.py ← Phase 0 helper (PROTECTED)
├── operator_action_audit.py       ← Phase 0 helper (PROTECTED)
├── db.py                          ← Phase 0 (PROTECTED)
├── tenant.py                      ← Phase 0 (PROTECTED)
├── aisstream_scraper.py           ← Beta 10 (PROTECTED in V1.0)
├── mst_scraper.py                 ← Beta 10 (PROTECTED in V1.0)
├── qships_scraper.py              ← Beta 10 (PROTECTED in V1.0)
├── vessel_scraper.py              ← Beta 10 (PROTECTED in V1.0)
├── bom_tides.py                   ← Beta 10 (PROTECTED in V1.0)
├── weather.py                     ← Beta 10 (PROTECTED in V1.0)
├── requirements.txt               ← runtime deps (PROTECTED — adds only)
├── requirements-dev.txt           ← dev deps (mutable)
├── railway.toml, Procfile         ← deploy config (PROTECTED)
├── tests/                         ← Beta 10 tests + regression gate
├── migrations/versions/           ← Phase 0 migrations (PROTECTED)
├── alembic.ini                    ← Alembic config
├── scripts/                       ← preview-deploy scripts
└── deploy/                        ← marketing site (PROTECTED)
```

### 7.2 Proposed V1 additions (NOT IMPLEMENTED YET)

```
/
├── frontend/                      ← NEW: V1 React frontend
│   ├── index.html                 ← Vite HTML shell
│   ├── vite.config.js
│   ├── package.json
│   ├── src/
│   │   ├── main.jsx
│   │   ├── App.jsx
│   │   ├── api/horizon.js         ← API client
│   │   ├── styles/                ← V1 design tokens
│   │   ├── components/            ← shared atoms
│   │   ├── layout/                ← shell + panels
│   │   ├── features/              ← per-feature modules
│   │   └── hooks/                 ← React hooks
│   └── dist/                      ← Vite build output (served by server.py at /v1/*)
│
├── shared/                        ← NEW: shared types and contracts
│   ├── types.ts                   ← API response shapes
│   └── constants.js               ← shared constants
│
├── api/                           ← NEW: V1 API route handlers (V1.1+)
│   ├── auth.py                    ← /api/v1/auth/*
│   ├── actions.py                 ← /api/v1/actions/*
│   ├── shifts.py                  ← /api/v1/shifts/*
│   ├── audit.py                   ← /api/v1/audit
│   └── briefs.py                  ← /api/v1/briefs/*
│
├── services/                      ← NEW: business logic modules (V1.1+)
│   ├── rbac.py                    ← role/permission resolution
│   ├── lifecycle.py               ← recommendation lifecycle state machine
│   ├── escalation.py              ← escalation chain logic
│   ├── handover.py                ← shift handover logic
│   └── analytics.py               ← executive metrics
│
├── audit/                         ← NEW: V1 audit-event helpers (V1.1+)
│   ├── operator_acknowledged_audit.py
│   ├── operator_deferred_audit.py
│   ├── operator_overrode_audit.py
│   ├── operator_escalated_audit.py
│   ├── handover_created_audit.py
│   ├── handover_accepted_audit.py
│   ├── shift_opened_audit.py
│   ├── shift_closed_audit.py
│   ├── deadline_passed_audit.py
│   ├── session_ended_without_action_audit.py
│   ├── audit_read_audit.py
│   ├── incident_opened_audit.py
│   └── incident_replayed_audit.py
│
├── replay/                        ← NEW: replay engine (V1.6)
│   ├── reconstructor.py           ← audit-event → operational state
│   ├── timeline.py                ← timeline composition
│   └── exporter.py                ← incident package export
│
├── integrations/                  ← NEW: V1-specific integration adapters (V1.5+)
│   ├── kyber/                     ← Kyber API/event boundary
│   ├── terminal/                  ← Terminal system adapters
│   └── ...
│
├── docs/                          ← NEW: V1 implementation documentation
│   └── runbooks/                  ← V1 operational runbooks
│
└── v1-handoff/                    ← EXISTING: design handoff artefacts (reference only)
```

### 7.3 What stays in `/`

The repo root continues to hold:

- Phase 0 / Beta 10 Python modules (protected)
- V1 architecture documents (the foundation quintet + review pack +
  this execution plan)
- Decision notes and runbooks (Phase 0 / Phase 1 governance artefacts)

### 7.4 What is explicitly NOT recommended

- **No monorepo split.** Frontend and backend share the same repo;
  the cognitive overhead of a split repo exceeds the benefit.
- **No microservice subdirectory.** V1.0 ships as a modular
  monolith. `services/` is internal logical organisation, not
  process boundary.
- **No `legacy/` or `beta10/` directory.** Beta 10 code stays where
  it is; protecting it via convention (and the regression gate)
  rather than renaming.

---

## 8. Environment Strategy

How the deploy topology evolves to support V1 alongside Beta 10.

### 8.1 Beta 10 production

Railway project `horizon-prod`, deploying from `main`:

- URL: `horizon.amsgroup.com.au`
- Marketing site: `horizon.ams.group`
- Tracking branch: `main`
- `DATABASE_URL`: unset
- `AUDIT_EMISSION_ENABLED`: not configured
- Status: production, customer-facing, demo-ready

**Unchanged by V1 work.** This continues to serve Beta 10 demos and
existing customer presentations throughout V1 development.

### 8.2 V1 sandbox

Railway project `horizon-v1-sandbox` (NEW, separate from Beta 10):

- Purpose: V1 frontend development environment
- Tracking branch: `v1/sandbox` or `feat/v1-*` per branch
- `DATABASE_URL`: set (sandbox Postgres)
- `AUDIT_EMISSION_ENABLED`: false initially
- Status: developer-facing, not customer-visible
- Backend: Beta 10's `server.py` unchanged, served at all routes,
  React frontend served at `/v1/*`

**This is where V1 frontend implementation lives.** Developers push
to `feat/v1-*` branches; Railway auto-deploys preview environments
for each branch.

### 8.3 V1 preview

Railway project `horizon-v1-preview` (NEW):

- Purpose: pre-merge validation environment for V1 features
- Tracking branch: per-branch previews (Railway auto-creates)
- `DATABASE_URL`: set (preview Postgres, separate from sandbox)
- `AUDIT_EMISSION_ENABLED`: false initially; flipped on per drill
- Status: reviewer-facing; gated promotion to staging

Used for the V1 equivalent of the Phase 1.2 preview drill — Stage
A (deploy clean) → Stage B (RBAC tests) → Stage C (action endpoint
emission) → Stage D (smoke) → Stage E (RBAC + emission live in
preview).

### 8.4 V1 staging

Railway project `horizon-v1-staging` (NEW):

- Purpose: customer-facing demo / acceptance environment
- Tracking branch: `v1/staging`
- `DATABASE_URL`: set (staging Postgres)
- `AUDIT_EMISSION_ENABLED`: true (staging acceptance posture)
- Status: customer-demo-ready; pre-production

Promotion to staging requires preview validation. Customer acceptance
testing happens here before production cutover.

### 8.5 V1 production

Railway project `horizon-v1-prod` (NEW):

- Purpose: V1 customer-facing production
- Tracking branch: `v1/main`
- `DATABASE_URL`: set (production Postgres)
- `AUDIT_EMISSION_ENABLED`: true (production audit live)
- Status: V1.0 customer-facing

Promotion to V1 production requires staging validation. Customer
cutover is a per-customer migration plan, not a global flag flip.

### 8.6 Audit database

Per Information Architecture §11 and Implementation Strategy §15.4:

- V1 production gets its own Postgres add-on (NOT shared with
  Beta 10)
- Audit chain is per-tenant; one chain per customer organisation
- Schema is the Phase 0 schema (`0001`-`0004` migrations) + V1
  closed-set extensions (`0005+` migrations for new event types)
- Backup: Railway's automated backups (daily, 7d retention on Hobby;
  configurable on Pro)
- Retention: 90 days hot per tenant initially; longer via per-tenant
  retention_class (V1.x)

### 8.7 Future AWS target

Per Implementation Strategy §15.6, V1.x may migrate to AWS:

- Customer compliance requirements (AMS tenancy)
- Larger customer workloads exceeding Railway scaling limits
- Customer-managed Postgres (RDS) preference

V1.0 ships on Railway. The AWS migration is V1.x or V2 and is a
separate planning conversation.

### 8.8 Environment promotion path

```
sandbox  →  preview  →  staging  →  production
   ↑           ↑           ↑           ↑
   |           |           |           |
feat/v1-*  feat/v1-*    v1/staging   v1/main
branch     branch       branch       branch
push       PR open      merge to     merge to v1/main
deploys    deploys      v1/staging   (manual gate)
auto       auto         (review      after staging
                        gate)        acceptance
```

Each promotion has explicit gates: regression gate passes, smoke
test passes, reviewer authorisation recorded.

---

## 9. Branching & PR Strategy

How V1 work is structured in git, branch by branch.

### 9.1 Branch naming conventions

| Prefix | Use | Examples |
|---|---|---|
| `docs/` | Documentation-only PRs | `docs/v1-permission-model-v0.1`, `docs/v1-execution-plan-v0.1` |
| `feat/v1-` | V1 feature implementation | `feat/v1-frontend-scaffold`, `feat/v1-dashboard-tab` |
| `fix/v1-` | V1 bug fixes | `fix/v1-login-redirect`, `fix/v1-dsw-step-3` |
| `chore/v1-` | V1 chore / tooling | `chore/v1-vite-config`, `chore/v1-test-runner` |
| `chore/preview-` | Phase-0-style preview-only DO-NOT-MERGE | (existing PRs #27, #28, #29 unchanged) |
| `fix/beta-10-` | Beta 10 bug fixes | `fix/beta-10-aisstream-stale` |
| `chore/beta-10-` | Beta 10 chore | `chore/beta-10-dependency-upgrade` |

### 9.2 Long-lived branches

- `main` — Beta 10 production branch; also where all V1 design
  documents are merged (PRs #30–#35 etc.)
- `v1/main` — V1 production integration branch (NEW; created when
  V1.0 implementation begins)
- `v1/staging` — V1 staging environment branch (NEW)
- `v1/sandbox` — V1 sandbox environment branch (NEW; optional)

### 9.3 Implementation phases by milestone

Each milestone (M0–M9 per §11) corresponds to a logical PR set:

- **One PR per logical step** — e.g. "Add Vite config" is one PR,
  "Add Card component" is another
- **No mega-PRs** — large changes split into reviewable pieces
- **Each PR is independently mergeable** — no cross-PR
  dependencies that require atomic merge

### 9.4 PR sizing expectations

| Size | Lines added | Reviewer time | Use case |
|---|---|---|---|
| XS | < 50 | minutes | Bug fix, single-component tweak |
| S | 50–200 | < 30 min | New component, new utility |
| M | 200–500 | 30–60 min | New feature panel, layout change |
| L | 500–1000 | 1–2 hours | New milestone slice |
| XL | > 1000 | requires split | Generally rejected; ask to split |

PRs exceeding L size require explicit reviewer pre-approval before
opening.

### 9.5 Review requirements

Every V1 implementation PR requires:

- A clear title following the convention `<scope>: <subject>` (e.g.
  `feat(v1): add Card component with severity variants`)
- A PR description with: Summary, Scope (in/out), Diff stat,
  Validation summary, Test plan
- The regression gate passing (`tests/test_beta10_regression.py` —
  46/46)
- V1-specific test additions where new logic is introduced
- An explicit reviewer sign-off (Tony) before merge
- No auto-merge enabled

### 9.6 Protected branches

- `main` — protected; no direct pushes; PR + review required
- `v1/main` (when created) — protected; no direct pushes
- `v1/staging` (when created) — protected; merge-from-`v1/main` only

Force-push to any protected branch is prohibited.

### 9.7 Deployment gating

| Branch | Auto-deploy to | Gate |
|---|---|---|
| `main` | Beta 10 production | passes regression gate; reviewer approval |
| `v1/sandbox` (or feat/v1-* branches) | V1 sandbox | passes CI; auto-deploy |
| `v1/preview-*` or per-branch preview | V1 preview | passes CI; auto-deploy for review |
| `v1/staging` | V1 staging | merge from `v1/main` + reviewer approval |
| `v1/main` | V1 production | merge from approved branch + Tony's explicit go-ahead |

No deployment to V1 production without:

1. CI passes (regression gate + V1 tests)
2. Staging validation
3. Tony's explicit production-deploy authorisation
4. Customer-specific cutover plan (per V1 customer engagement)

---

## 10. Claude Code Engineering Workflow

How Claude Code participates in V1 implementation under the
established governance pattern.

### 10.1 What Claude Code may implement autonomously

(After Tony's explicit pre-authorisation for the specific task.)

- Following an authorised implementation plan to write code
- Running tests and reporting results
- Making small, scoped commits within an authorised PR
- Pushing to a feature branch
- Opening a PR with full description and test results
- Updating documentation in `docs/` directories

### 10.2 What requires explicit authorisation

Everything that creates state or affects production:

- Branch creation
- Initial PR creation for a new feature (the plan needs
  authorisation; then the PR opening is part of the plan)
- Merge approval (always Tony's call; Claude never merges
  unilaterally)
- Push to production
- Environment variable changes
- Provisioning new Railway / AWS resources
- Database operations (migrations, seed data, manual queries)
- Modifications to protected files (§2.6)

### 10.3 Implementation reports

Every implementation PR (or significant intermediate step) produces
a report containing:

- **PR URL** and commit SHA
- **Diff summary** — `git diff --stat`
- **Files verified unchanged** — explicit check against protected
  files
- **Tests run** — regression gate result, full suite result, any
  new test additions
- **Live behaviour probes** — for endpoint changes, in-process
  probes confirming expected behaviour (per Phase 0 / Phase 1
  pattern)
- **What this PR does NOT do** — explicit scope boundaries
- **Open questions or follow-ups** — flagged for separate review

### 10.4 Regression requirements

Every V1 implementation PR must:

- Keep `tests/test_beta10_regression.py` at 46/46 (no test removals,
  no baseline drift unless explicitly authorised)
- Add V1-specific tests for any new business logic
- Pass the full `pytest tests/` suite

When a V1 PR requires the regression gate baseline to change (e.g.
new endpoint added to `EXPECTED_PUBLIC_PATHS`, new event type in
the authorised whitelist), the baseline change is part of the same
PR with explicit justification in the PR body.

### 10.5 Rollback expectations

Every V1 PR must:

- Be reverted cleanly via `git revert` if necessary (no merges that
  introduce architectural changes too invasive to revert)
- Identify rollback steps in the PR body for any environment-level
  side effects (env vars, migrations, data seeds)
- Use feature flags for new behaviours when possible, so rollback
  is a flag flip rather than a deploy revert

### 10.6 Deploy approval expectations

For V1 deploys to staging or production:

- The PR that triggers the deploy is reviewed and approved by Tony
- Pre-deploy smoke test on the destination environment
- Acceptance window monitored (5–15 minutes post-deploy)
- Rollback drill verified before the first customer-facing deploy
- Sign-off recorded in the V1 deploy runbook (TBD per V1.0
  implementation)

### 10.7 Protected-file rules

Modifications to protected files (§2.6) require:

- Pre-implementation plan submitted and authorised
- Rationale documented in PR body
- Risk assessment (what could break, how to detect, how to roll back)
- Regression gate result post-change
- Reviewer authorisation tied to the specific change, not blanket

A modification to `server.py` for V1 is the load-bearing example: in
V1.0 this happens **only** to add the static-file serving for the
React app at `/v1/*`. Any other server.py change in V1.0 requires
explicit re-authorisation.

### 10.8 What Claude Code does NOT do

- **Self-merge.** Every merge requires Tony's go-ahead.
- **Push to production branches without prior approval.**
- **Modify protected files without explicit authorisation.**
- **Bypass CI gates** — if the regression gate fails, the work
  stops until the cause is understood.
- **Make architectural decisions silently** — design questions are
  surfaced and authorised, not absorbed into implementation.
- **Activate `AUDIT_EMISSION_ENABLED` in production** — that is a
  Stage E-prod decision, separate from V1 implementation.

---

## 11. V1.0 Milestones

> **Pending-input notice.** Milestones M0–M5 depend on the
> Claude Design UX/UI handoff (`v1-handoff/`) being formally
> authorised. None of these milestones may be authorised for
> implementation until that gate closes. M6–M9 are also
> downstream of M0–M5 and therefore inherit the same dependency.

V1.0 implementation would break into ten concrete milestones
(M0–M9). Each has a defined goal, dependencies, acceptance
criteria, and rollback criteria. **Each milestone requires its own
explicit authorisation under the §10 governance workflow before
any code may be written.**

### M0 — Frontend Scaffold + Design Tokens

**Goal:** establish the React build pipeline and design system
foundation.

**Dependencies:** V1 foundation review acceptance **AND** formal
authorisation of the `v1-handoff/` UX/UI handoff as an
implementation input.

**Deliverables:**
- New `frontend/` directory with Vite + React project
- Design tokens (CSS custom properties from
  `v1-handoff/prototype/Horizon V1.html`)
- Shared components: Card, Pill, CategoryPill, Dot, Icon set
- Basic styles (reset, base typography, layout primitives)
- Vite build output served at `/v1/*` (single `server.py` change to
  serve static files at this prefix)

**Acceptance criteria:**
- `npm run build` produces a `frontend/dist/` artefact
- Beta 10 routes (`/`, `/api/*`, `/login`, etc.) continue working
  unchanged
- `/v1/` route serves the React shell
- Regression gate still passes 46/46
- `server.py` diff is < 20 lines (only static-file serving added)
- Design tokens verified against the prototype CSS

**Rollback criteria:**
- Revert the PR that added `/v1/*` routing in `server.py`
- Delete `frontend/dist/` from the deploy artefact
- Beta 10 routes unaffected because they never depended on V1

### M1 — App Shell + Auth

**Goal:** complete the React app shell and wire up auth to the Beta
10 backend.

**Dependencies:** M0.

**Deliverables:**
- `HorizonHeader` component (sticky, 72px, stat tiles + clock)
- `ConditionsBar` component (weather/tide strip)
- 3-column shell layout (`360px | 1fr | 380px`)
- React login page (consumes existing `POST /login`)
- Session management (cookie-based)
- Polling infrastructure (`useSummary()` hook, 30s cadence)

**Acceptance criteria:**
- A user can log in via the React app and reach the shell
- The shell polls `/api/summary` every 30 seconds
- Cross-browser testing (Chrome, Safari, Firefox latest)
- Regression gate passes

**Rollback criteria:**
- Disable the `/v1/login` route in `server.py` (revert PR)
- Beta 10's `/login` remains as the primary auth path

### M2 — Dashboard + Polling

**Goal:** wire up the central dashboard tab and validate the
polling-driven data flow.

**Dependencies:** M1.

**Deliverables:**
- `Dashboard` tab with KPI tiles, ETD risk table, berth heatmap
- `KpiTile`, `EtdRisk`, `BerthHeatmap` components
- Data-binding from `/api/summary` JSON shape
- 30-second polling visibly updates the dashboard
- Loading and error states handled

**Acceptance criteria:**
- Dashboard renders correctly with live AISStream data
- KPI tiles update on poll
- Cross-port testing (Brisbane, Melbourne, Geelong, Darwin) per
  `v1-handoff/V1-Implementation-Prompt.md` §6
- Regression gate passes

**Rollback criteria:**
- Hide the V1 dashboard route in the React router
- Beta 10 `/` continues serving the existing dashboard

### M3 — Operational Panels

**Goal:** complete the LeftPanel + RightPanel + remaining tabs.

**Dependencies:** M2.

**Deliverables:**
- `LeftPanel` (Alerts + Decision Card preview)
- `RightPanel` (Vessel Roster + Audit Log placeholder)
- Center tabs: Berth Timeline, Shift Log, VTS Map, Pilotage,
  Performance
- All tab components per the prototype

**Acceptance criteria:**
- All 6 tabs render and switch correctly
- LeftPanel alerts and decision card render with live data
- RightPanel vessel roster filterable/sortable
- Regression gate passes

**Rollback criteria:**
- Disable specific tabs in the router if any one tab is broken;
  others continue working

### M4 — DSW Flow

**Goal:** implement the Decision Support Window (5-step modal
flow).

**Dependencies:** M3.

**Deliverables:**
- `DSW` component (full-screen modal overlay)
- 5 step components: Signal, Cascade, Options, Commit, Ledger
- Stepper navigation
- Decision countdown timer
- Integration with `/api/whatif` / `/api/apply-whatif` for scenario
  application

**Acceptance criteria:**
- Operator can open DSW from a decision card and complete the
  5-step flow
- What-if scenario application via existing Beta 10 endpoint works
- DSW header shows persistent decision context across steps
- Regression gate passes

**Rollback criteria:**
- Disable DSW launch button on decision cards
- Beta 10 `/api/whatif*` endpoints continue working for legacy UI

### M5 — Role-Aware Rendering

**Goal:** introduce per-user RBAC and role-scoped frontend
rendering.

**Dependencies:** M4. **This is the first milestone that touches
backend logic beyond static-file serving.**

**Deliverables:**
- New Alembic migration `0005_v1_rbac` (users, roles, permissions,
  user_roles, user_scopes per Permission Model §9.1)
- New `/api/v1/auth/login` endpoint serving per-user auth
- Role claim in session
- React app reads role claim and filters visible panels per
  Permission Model §5 + Screen Architecture §4.2
- Backward-compat: Beta 10's `/login` continues to work for the
  `index.html` UI

**Acceptance criteria:**
- Demo tenant has seeded users with different roles
- Each user sees only their authorised panels (VTSO sees
  operational, Executive sees aggregated, etc.)
- Beta 10 regression gate still passes (because the new endpoint is
  additive; existing endpoints unchanged)
- V1 RBAC tests added and passing

**Rollback criteria:**
- Disable `/api/v1/auth/login` in `server.py`
- React app falls back to client-only role rendering (less secure
  but functional)
- RBAC migration is reversible (Alembic downgrade to `0004`)

### M6 — What-If Integration

**Goal:** make the React DSW fully integrated with the what-if
scenario engine and add scenario authoring.

**Dependencies:** M4 + M5.

**Deliverables:**
- React-side scenario composer (adjust ETAs, swap berths, mark
  vessels delayed)
- Live what-if execution via existing `POST /api/whatif`
- Result comparison view (current vs scenario)
- "Apply to live" workflow with HM approval (per Workflow Model
  §10.5)

**Acceptance criteria:**
- VTSO can run a what-if scenario from the DSW
- HM can approve a what-if application
- Audit trail captures what-if application (Phase 0.8a already live)
- Regression gate passes

**Rollback criteria:**
- Disable scenario composer in React; what-if continues via the
  Beta 10 UI

### M7 — Audit-Linked Actions

**Goal:** introduce V1 action endpoints (acknowledge, resolve,
defer, override, escalate, sign-off) with audit emission.

**Dependencies:** M5.

**Deliverables:**
- New endpoints: `/api/v1/actions/acknowledge`, `resolve`, `defer`,
  `override`, `escalate`, `close`, `sign-off`
- New audit helpers per Information Architecture §12.8:
  - `OPERATOR_ACKNOWLEDGED`
  - `OPERATOR_DEFERRED` (Phase 0 reserved, now emits)
  - `OPERATOR_OVERRODE` (Phase 0 reserved, now emits)
  - `OPERATOR_ESCALATED` (V1 candidate)
  - `OPERATOR_SIGNED_OFF` (V1 candidate)
  - `RECOMMENDATION_CLOSED` (V1 candidate)
- New Alembic migration `0006_v1_audit_events` extending the
  closed-set CHECK constraint
- Deliberate update to `tests/test_beta10_regression.py` authorised
  event_type list (reviewer-approved baseline change)
- Reason code catalogues (closed sets per Workflow Model §8.5,
  §10.3, §10.6)

**Acceptance criteria:**
- VTSO can acknowledge a recommendation via the React UI
- HM can defer / override with mandatory reason code
- Audit ledger captures all V1 action events
- `verify_chain` returns clean across the V1 action events
- Regression gate passes (baseline update is part of this PR)

**Rollback criteria:**
- Disable V1 action endpoints in `server.py`
- React UI falls back to read-only mode (no action buttons)
- Migration `0006` reversible via Alembic downgrade

### M8 — Replay Foundation

**Goal:** introduce the Replay & Incident Review surface for HM and
Executive consumption.

**Dependencies:** M7.

**Deliverables:**
- Replay query endpoint `GET /api/v1/audit?window=<>&filter=<>`
- Replay timeline component
- Recommendation chain visualisation
- Audit chain integrity status indicator
- Incident open/close endpoints
- `AUDIT_READ` audit emission for replay surfaces
- `INCIDENT_OPENED` / `INCIDENT_REPLAYED` events

**Acceptance criteria:**
- HM can open the replay surface and reconstruct a shift's events
- Replay loads in < 5 seconds for shift-scope queries
- Chain integrity status displays correctly
- Regression gate passes

**Rollback criteria:**
- Disable replay endpoint and React route
- Audit ledger continues accumulating; replay is purely a read
  surface, no data corruption possible

### M9 — Operational Hardening

**Goal:** prepare V1.0 for customer deployment with observability,
monitoring, runbooks, and the V1.0 acceptance gate.

**Dependencies:** M8.

**Deliverables:**
- Observability stack (structured logging, metrics endpoint, error
  tracking) per Implementation Strategy §15.2
- V1-specific regression gate (`tests/test_v1_regression.py`)
- Performance baselines for /api/v1/summary, action endpoints,
  replay queries
- Audit chain integrity cron job (hourly `verify_chain`)
- V1.0 production deploy runbook
- V1.0 rollback runbook
- V1.0 customer onboarding runbook

**Acceptance criteria:**
- All V1-specific regression tests passing
- Performance baselines met (per Implementation Strategy §19.2)
- Runbooks documented and reviewer-approved
- Customer-acceptance dry-run completed on staging

**Rollback criteria:**
- Documentation is non-functional; rollback is per-component (e.g.
  observability stack)

### Milestone summary

| M | Goal | Touches server.py? | Audit events emitted |
|---|---|---|---|
| M0 | Frontend scaffold + tokens | Yes (static routing only) | none new |
| M1 | App shell + auth | No | existing only |
| M2 | Dashboard + polling | No | existing only |
| M3 | Operational panels | No | existing only |
| M4 | DSW flow | No | existing only |
| M5 | Role-aware rendering | Yes (new /api/v1/auth) | existing only |
| M6 | What-If integration | No (uses existing) | existing only |
| M7 | Audit-linked actions | Yes (new /api/v1/actions/*) | new event types |
| M8 | Replay foundation | Yes (new /api/v1/audit) | AUDIT_READ, INCIDENT_* |
| M9 | Operational hardening | No code change | none new |

---

## 12. Regression & Protection Strategy

How V1 work is protected from accidentally breaking Beta 10 or
itself.

### 12.1 Beta 10 regression rules

`tests/test_beta10_regression.py` is the protected invariant.
Throughout V1 work:

- **46/46 must pass on every PR** before merge
- **Baseline updates require explicit reviewer authorisation** —
  documented in the PR body with rationale
- **Asset hash baselines are updated only when assets are
  deliberately changed** (e.g. a new logo); never silently
- **Authorised event_type list is extended deliberately** when new
  V1 audit events arrive (M7); reviewer-approved
- **Public paths set is updated only on deliberate routing changes**

### 12.2 Snapshot expectations

The Phase 0.7b §1.4.1 decision-time snapshot is the canonical
example of "snapshot what the engine knew when it acted." V1 work
must preserve this:

- Every `RECOMMENDATION_GENERATED` continues to embed the snapshot
- New V1 events that depend on prior state (defer, override) embed
  references to the prior recommendation's snapshot
- Replay correctness depends on snapshot completeness — replay
  tests validate this

### 12.3 Visual regression considerations

For the React frontend specifically:

- **Component-level snapshot tests** (Jest + React Testing Library)
  for shared components (Card, Pill, etc.)
- **Visual regression via screenshots** (e.g. Percy or Chromatic)
  is deferred to V1.x — V1.0 ships without it
- **Manual cross-browser validation** is part of each milestone's
  acceptance (Chrome, Safari, Firefox latest)
- **Cross-port validation** per `v1-handoff/V1-Implementation-Prompt.md`
  §6: Brisbane, Melbourne, Geelong, Darwin

### 12.4 API compatibility checks

For any backend change:

- **Contract test** verifies the response shape matches the
  documented contract
- **Diff against Beta 10** when an endpoint is shared — the response
  must not break Beta 10 consumers
- **Versioned endpoints** (`/api/v1/*`) avoid shared-contract issues
- **Schema versioning** via `schema_version` field in responses
  when shape evolves (V1.x)

### 12.5 Audit protection

- `audit.py`, all 5 audit helpers, and Phase 0 migrations are
  protected
- New V1 audit helpers follow the Phase 0 helper pattern (same
  defaults, same fallback behaviour, same lazy-import safety)
- Every new event type has production-safe no-op tests (per
  `tests/test_session_audit.py` model)
- The hash chain is append-only; no row mutation, no row deletion
- `verify_chain` integrity tests are part of the V1 regression suite

### 12.6 Protected branches

`main`, `v1/main`, and `v1/staging` are protected:

- No direct pushes
- PR + reviewer approval required
- No force pushes
- No CI bypass

### 12.7 What is NOT regression-tested

Per Implementation Strategy §17.10 — V1 frontend visual design
specifics. The Beta 10 regression gate validates Beta 10 invariants;
V1-specific invariants are added incrementally (M9 ships
`tests/test_v1_regression.py`).

---

## 13. Deployment & Release Strategy

How V1 changes get from a developer's machine to a customer's
production environment.

### 13.1 Preview deployments

Per-branch preview environments on Railway:

- Every `feat/v1-*` or `fix/v1-*` branch auto-deploys to a preview
  URL
- The preview URL is in the PR description
- The reviewer can manually validate the change on preview before
  approval
- The Phase 1.2 preview pattern (Stages B–D before E) generalises
  to V1 — preview validation precedes staging

### 13.2 Staging validation

After PR merge to `v1/main`, the change deploys to staging:

- Automated smoke test: `/api/v1/summary` returns expected shape;
  `/api/v1/health` returns 200
- Audit chain integrity check
- Cross-port validation (4 ports)
- Acceptance window: 5–15 minutes monitored

If staging fails any acceptance check, the deploy is reverted (per
Implementation Strategy §13.6 rollback strategy).

### 13.3 Production release gates

Promotion from staging to production requires:

- Staging acceptance: all smoke tests pass, no WARN-level audit
  errors
- Reviewer approval: Tony's explicit production-deploy
  authorisation
- Customer-specific cutover plan (per V1 customer engagement)
- Rollback drill on staging confirmed within the last 14 days

### 13.4 Rollback procedure

Per Implementation Strategy §13.6:

- **Feature flag flip** for flag-controlled changes (preferred)
- **Deploy revert** via Railway deployment history (PR revert merge
  to `v1/main`)
- **Database rollback** via Alembic downgrade for schema changes
  (rare; prefer feature-flagged forward-only)
- **Full rollback** to V1.0 baseline tag if a major incident
  warrants

The rollback playbook is part of the V1.0 deploy runbook (M9).

### 13.5 Release sign-off requirements

Each V1.0 customer deployment requires:

- A signed checklist confirming:
  - Beta 10 regression gate passes
  - V1 regression gate passes
  - Staging acceptance complete
  - Customer-specific configuration verified
  - Rollback drill rehearsed
- The signed checklist is part of the audit trail (stored alongside
  the deploy commit SHA)
- Tony's name and timestamp on the sign-off

The format mirrors the Phase 0 exit runbook sign-off.

---

## 14. Technical Debt Strategy

What V1 inherits from Beta 10 and how each item is treated.

### 14.1 What stays temporarily

These Beta 10 concerns persist into V1.0 and are addressed in V1.x:

- **In-process globals** (`_PORT_PROFILE`, `_WHATIF_OVERLAY`,
  `_mst_cache`) — Beta 10 uses these; V1.0 React frontend rides on
  them via `/api/summary`. V1.1+ migrates to persistent state.
- **Shared session auth** — Beta 10's `HORIZON_USER`/`HORIZON_PASS`
  continues serving Beta 10 routes; V1's per-user auth ships at M5
  for V1 routes only.
- **AISStream + MST + QShips integration code** — Beta 10's
  scrapers continue feeding `/api/summary`. V1.x may refactor to
  the integration adapter pattern per Information Architecture §11.
- **Beta 10's `index.html`** — coexists with the React frontend;
  removal is V1.x at earliest, when V1 has feature parity.

### 14.2 What gets wrapped

- **`/api/summary`** — V1.1+ adds server-side role-scoped filtering
  as a wrapper, leaving the underlying computation in Beta 10's
  `server.py`. The wrapper inspects the session role and trims
  fields server-side.
- **`/api/whatif*`** — V1.0 React frontend uses these unchanged.
  V1.x may wrap them with a versioned `/api/v1/whatif*` interface
  but the Beta 10 endpoints remain functional.

### 14.3 What gets rewritten

These get full rewrites in V1.x (NOT V1.0):

- **`server.py` monolith** — V1.x splits into modular services per
  Implementation Strategy §10.3. V1.0 retains the monolith.
- **`index.html` frontend** — V1.0 introduces the React frontend at
  `/v1/*`; V1.x removes `index.html` from the codebase when feature
  parity is reached.
- **Single-shared-login auth** — V1.0's M5 introduces per-user auth;
  V1.x retires the env-var auth from V1 routes.
- **Hardcoded conflict scenarios** (B03, B04) — replaced by
  general detection on real port data; V1.x.
- **`port_profiles.py`** — V1.x migrates to `config.ports` table per
  Permission Model §9 + Information Architecture §17.

### 14.4 What gets deferred

These do NOT happen in V1.0:

- **Multi-tenant in single deployment** — V2+ (per Implementation
  Strategy §14.2)
- **Microservices split** — V2+
- **AWS migration** — V1.x at earliest
- **External regulator access** — V1.x (deferred per Permission
  Model §14.4)
- **AI features** — V1.x+ (substrate only in V1.0)
- **Authority delegation** — V1.x (per Workflow Model §17.17)
- **Cross-port escalation** — V1.x (when multi-port tenants exist)

### 14.5 Debt visibility

Each item above is captured in:

- **PR descriptions** when relevant code is touched
- **`HORIZON_CAPABILITY_BACKLOG.md`** for forward-looking
  capabilities (already on `main`)
- **V1 milestone retros** when each milestone closes

Debt is paid down deliberately, not silently. New V1 work that
incurs additional debt requires explicit reviewer acknowledgement.

---

## 15. Risks & Failure Modes

What could go wrong, and what to do about it.

### 15.1 Frontend drift from operational model

**Risk:** the React frontend implements something that doesn't match
the workflow model (e.g. a state transition not in Workflow Model
§7).

**Detection:** workflow model is the authoritative reference;
mismatches surface during reviewer validation or operational
stakeholder review.

**Mitigation:**
- Every React component that represents a workflow stage
  cross-references the workflow document
- Operational stakeholder review (per Review Pack §10.2) happens
  before V1.0 ships
- M3 acceptance includes operational walk-through

### 15.2 API coupling

**Risk:** the React frontend becomes tightly coupled to Beta 10's
`/api/summary` shape, making backend evolution painful.

**Detection:** when V1.1 introduces role-scoped projection, the
React app has too many assumptions about full-shape responses.

**Mitigation:**
- Build the React data layer with shape adapters (the React app's
  API client normalises responses to a canonical V1 shape)
- Document the shape adapter as the V1-side compatibility boundary
- Test the adapter independently from UI components

### 15.3 Role leakage

**Risk:** a role's view inadvertently includes data they should not
see (e.g. Executive sees free-text resolution notes).

**Detection:** stakeholder review identifies the leak; or worse,
post-launch discovery by a customer.

**Mitigation:**
- M5 milestone explicitly validates per-role filtering
- Server-side enforcement at /api/v1/summary (M5 prerequisite for
  customer deployment)
- Test cases for each role verify what's returned and what's NOT
- "Hidden destinations must not appear at all" per Permission Model
  §10.7

### 15.4 Replay incompleteness

**Risk:** the audit ledger lacks events needed to reconstruct a
specific decision; replay surfaces it as "cannot reconstruct."

**Detection:** M8 replay tests; live replay attempts by HM in
staging.

**Mitigation:**
- Every workflow state transition emits an audit event per Workflow
  §14
- M7 (audit-linked actions) ships all V1.2-level audit emissions
- M8 (replay foundation) includes coverage validation
- Pre-launch, an internal incident is simulated end-to-end and
  replayed for completeness verification

### 15.5 Audit gaps

**Risk:** a V1 action that should emit an audit event doesn't (e.g.
a code path missed during M7 implementation).

**Detection:** internal review of action endpoints against the
authoritative event list; staging acceptance includes
emission-coverage check.

**Mitigation:**
- Every V1 action endpoint has a test that verifies the
  corresponding audit event is emitted
- The Phase 0 helper pattern (production-safe tests for emission)
  is replicated for V1 helpers
- M9 hardening includes an audit-coverage audit

### 15.6 CSS inconsistency

**Risk:** the V1 React frontend's CSS drifts from the design tokens
during implementation, producing visual inconsistency.

**Detection:** visual review against `v1-handoff/prototype/`
screenshots; cross-component visual audit.

**Mitigation:**
- All tokens are CSS custom properties in `tokens.css`; no
  hardcoded values in components
- Component-level snapshot tests for shared atoms
- Visual regression testing via Percy / Chromatic (V1.x deferred;
  V1.0 ships with manual review)

### 15.7 Deployment confusion

**Risk:** a V1 change is deployed to Beta 10 production instead of
V1 production (or vice versa) because the Railway projects look
similar.

**Detection:** environment-tagged smoke test immediately after
deploy verifies the deployed image is on the intended branch /
project.

**Mitigation:**
- Distinct Railway project names (`horizon-prod` for Beta 10;
  `horizon-v1-prod` for V1)
- Distinct env vars per project (`DATABASE_URL` set only on V1
  projects)
- Smoke test immediately reports which project / branch / commit
  is live
- Tony confirms the deploy target before authorising production
  push

### 15.8 Beta 10 contamination

**Risk:** a V1 change inadvertently modifies a protected Beta 10
file or breaks the regression gate, contaminating the Beta 10
baseline.

**Detection:** the regression gate fails; the explicit
protected-files check in implementation reports (§10.3) catches
out-of-scope modifications.

**Mitigation:**
- The regression gate runs on every PR
- Protected-files check is part of every implementation report
- The `phase-0-complete @ 4ad4aae` tag is immutable; any rollback
  to Beta 10 baseline is unambiguous
- Branch protection rules on `main` prevent direct pushes
- Tony has explicit authorisation authority over every merge

---

## 16. Success Criteria

V1.0 is ready for customer deployment when ALL of the following are
true.

### 16.1 Operational readiness

- ✓ A VTSO can complete an entire shift (login → operational
  decisions → handover → logout) without leaving the application
- ✓ A Harbour Master can review the shift, approve overrides, and
  sign off the Port Brief
- ✓ A Shift Supervisor can compose a handover, accept the next
  shift's incoming handover, and review the VTSO action queue
- ✓ A Port Executive can view weekly trends and read the daily
  Port Brief
- ✓ A Stakeholder (pilot / towage / mooring) can confirm an
  assignment and report a delay on mobile
- ✓ An incident can be replayed end-to-end from the audit ledger

### 16.2 Engineering readiness

- ✓ All 10 milestones (M0–M9) complete
- ✓ `tests/test_beta10_regression.py` passes 46/46 throughout
- ✓ V1-specific regression tests (`tests/test_v1_regression.py`)
  pass
- ✓ Audit chain integrity (`verify_chain`) passes on V1.0
  production ledger
- ✓ Performance baselines met (per Implementation Strategy §19.2):
  /api/v1/summary p95 < 500ms; action-to-audit p95 < 500ms;
  shift-scope replay < 5s
- ✓ All protected files unchanged outside authorised exceptions
- ✓ No direct-to-production deploys recorded

### 16.3 Deployment readiness

- ✓ V1 sandbox, preview, staging, production environments operational
- ✓ Staging validation runbook documented and exercised
- ✓ Production deploy runbook documented
- ✓ Rollback runbook documented and rehearsed
- ✓ Observability stack operational (logs, metrics, error tracking)
- ✓ Audit chain integrity cron job running

### 16.4 Customer-demo readiness

- ✓ A demonstration on V1 staging to a maritime operations team
  produces enthusiastic operational endorsement
- ✓ The 4 ports (Brisbane, Melbourne, Geelong, Darwin) all
  demonstrate end-to-end
- ✓ No "we'll need to build that" caveats for core operational
  verbs
- ✓ The V1.0 demo is preferred over the Beta 10 demo for
  new-customer pitches

### 16.5 Replay readiness

- ✓ Incident replay surface operational
- ✓ Replay timeline correctly reconstructs a 24-hour shift
- ✓ Recommendation chains expand correctly
- ✓ Escalation chains reconstruct end-to-end
- ✓ Audit chain integrity status visible on every replay
- ✓ Incident package export (JSON + PDF + hash proof) functional

### 16.6 Audit readiness

- ✓ All Phase 0 reserved event types (`OPERATOR_DEFERRED`,
  `OPERATOR_OVERRODE`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION`) emitting in V1
- ✓ V1 candidate event types (`OPERATOR_ACKNOWLEDGED`,
  `OPERATOR_ESCALATED`, `HANDOVER_CREATED`, `HANDOVER_ACCEPTED`,
  `SHIFT_OPENED`, `SHIFT_CLOSED`, `AUDIT_READ`, `INCIDENT_OPENED`,
  `INCIDENT_REPLAYED`) emitting where workflow lifecycle dictates
- ✓ Every audit event carries `user_id`, `role`, `scope`,
  `session_id` in payload
- ✓ Hash chain verifies clean across the entire V1.0 audit ledger
- ✓ Stage E-prod activated for V1.0 production (separately
  authorised per Implementation Strategy §9.4)

---

## 17. Open Questions

Questions surfaced by the execution planning that warrant explicit
resolution before V1.0 implementation begins or during the V1.0
phase.

### 17.0 UX/UI handoff authorisation (blocking)

**Question:** is the Claude Design UX/UI handoff (`v1-handoff/`)
formally authorised as an implementation input for V1.0?

**Status:** **PENDING — blocking gate.** This execution plan was
written treating the handoff as a planning assumption only. No
M0–M9 milestone may be authorised until this gate closes.

**Decision needed:** before any V1.0 implementation work begins.
A negative or modified decision requires §5 and §11 to be revised
before authorisation can proceed.

### 17.1 React state library

**Question:** does V1.0 use React's built-in state (local +
useReducer + Context) or introduce Zustand / Redux / Jotai /
similar?

**Default:** built-in state for V1.0; revisit at V1.x if complexity
warrants.

**Decision needed:** before M1 starts.

### 17.2 Live updates architecture

**Question:** when does V1.x introduce SSE or WebSocket beyond
polling?

**Default:** polling for V1.0; SSE/WebSocket deferred.

**Decision needed:** V1.0 ships on polling; the architectural slot
for push transport is part of the V1.0 design (decision deferred
to V1.x).

### 17.3 Event bus timing

**Question:** when is `OPERATOR_ESCALATED` named (verb) vs
`ESCALATION_CREATED` (artefact)?

**Default per Workflow Model §17.14:** decision pending; both
defensible.

**Decision needed:** before M7 starts.

### 17.4 Replay storage model

**Question:** does V1.6 store pre-computed replay packages or
generate on-demand from raw audit data?

**Default:** on-demand from raw audit data (no pre-computation);
caching is a V1.x optimisation.

**Decision needed:** before M8 starts.

### 17.5 Multi-tenant timing

**Question:** does V1.0 support multi-tenant-in-one-deployment, or
only one-tenant-per-deployment per Implementation Strategy §14.2?

**Default:** one-tenant-per-deployment in V1.0; multi-tenant is V2+.

**Decision needed:** if a first-customer engagement materialises
with multi-port-per-tenant requirements, this may need V1.x bring-
forward.

### 17.6 AWS migration timing

**Question:** when does V1 migrate from Railway to AWS?

**Default:** Railway for V1.0; AWS deferred.

**Decision needed:** if customer compliance requirements emerge
mid-V1 development, the migration may need V1.x bring-forward.

### 17.7 Mobile strategy

**Question:** is the V1.5 stakeholder mobile experience a PWA
served from the same React app, or a separate native app?

**Default per Implementation Strategy §13.3:** PWA from the same
React build.

**Decision needed:** before M5 starts (affects build pipeline
configuration).

### 17.8 Notification delivery

**Question:** what transport (SSE / WebSocket / push / SMS /
email) does V1.0 use for stakeholder notifications?

**Default:** in-app SSE; APNs/FCM for stakeholder mobile; SMS as
critical-alert fallback; email for digests.

**Decision needed:** before M7 starts (affects stakeholder feed
integration).

### 17.9 Offline / failover mode

**Question:** what does V1.0 do when network connectivity to the
backend is lost?

**Default:** show "offline" indicator; queue actions for re-submit
on reconnect; cache recently-viewed state in IndexedDB for read.

**Decision needed:** before M2 starts (affects polling + state
infrastructure).

---

## 18. Recommendations

For V1.0 implementation start:

### 18.1 Treat this as Execution Plan v0.1

Expect iteration to v0.2 as the V1 foundation review (per Review
Pack §10) produces feedback. The execution plan reflects the
authoritative foundation; foundation changes flow through here.

### 18.2 Review before implementation begins

This execution plan must be reviewed by:

- **AMS leadership** for governance and resource alignment
- **Engineering team** for technical feasibility and milestone
  realism
- **Operational stakeholders** for milestone-acceptance criterion
  validation

The review can happen in parallel with the V1 foundation reviews
(per Review Pack §10), since they engage different concerns.

### 18.3 Begin V1.0 only after execution plan review AND UX/UI handoff authorisation

The V1 design foundation (PRs #30–#34 merged) plus this execution
plan (PR open) plus the V1 review pack (PR #35) must all be
reviewed and accepted **and** the Claude Design UX/UI handoff
(`v1-handoff/`) must be **separately and formally authorised** by
Tony as an implementation input before V1.0 code is written. No
exceptions. If any of those four gates does not close, the relevant
sections of this execution plan must be revised before
authorisation can proceed.

### 18.4 Keep Stage E-prod paused

Reinforces the consistent recommendation across all five
foundation documents and the review pack:

- Stage E-prod (production audit emission activation) is **paused**
- It is **not** a V1.0 acceptance criterion to activate it
- The decision to activate is **separate from V1 implementation**
- The recommendation (per Implementation Strategy §9.4 Option B):
  activate Stage E-prod AFTER V1's per-user identity is on every
  audit row, so the production ledger opens cleanly with V1
  attribution rather than Beta-10-shaped `O-1` attribution

### 18.5 Preserve Beta 10 throughout implementation

- `phase-0-complete @ 4ad4aae` remains the immutable Beta 10
  baseline
- `tests/test_beta10_regression.py` remains the active CI gate
- Beta 10 production (`horizon-prod`) continues serving demos
  throughout V1 development
- Customer V1 commitments do NOT cause Beta 10 work; Beta 10 fixes
  are bug-fix-only

### 18.6 Use the Claude Code governance pattern from §10

The Phase 0 / Phase 1 governance pattern scales to V1. Claude
proposes, Tony authorises, Claude implements, Claude reports back,
Tony reviews and merges. The discipline adds 1-2 days per major
change but has prevented every silent regression in the prior
phases.

---

**End of v0.1.** Reviewer comments expected before v0.2.

The V1 design foundation now comprises **seven documents**:

1. Permission Model v0.1 (PR #30) — WHO
2. Workflow Model v0.1 (PR #31) — HOW
3. Screen Architecture v0.1 (PR #32) — WHERE
4. Information Architecture v0.1 (PR #33) — WHAT
5. Implementation Strategy v0.1 (PR #34) — HOW TO BUILD
6. Review Pack v0.1 (PR #35, open) — STAKEHOLDER SYNTHESIS
7. **Execution Plan v0.1 (this PR) — IMPLEMENTATION ROADMAP**

Together they define the V1.0 implementation contract. No V1.0
code is written until all seven are reviewed and accepted by
operational stakeholders, the architecture team, and engineering.
