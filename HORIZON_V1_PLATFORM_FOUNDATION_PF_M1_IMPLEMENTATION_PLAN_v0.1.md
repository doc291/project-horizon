# Horizon V1 — Platform Foundation PF-M1 Implementation Plan (v0.1)

**Document status:** Draft for review — **implementation plan only**
**Document type:** Platform Foundation milestone implementation plan
**Target stream:** Future architecture / V1 Platform Foundation only
  **— Beta 10 is excluded by the Immutability Rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 1ac81fe`
  (post-PR #72 — PF-M1 Scope Proposal v0.1)
**Date:** 2026-05-21
**Scope of authority:** Defines the controlled implementation plan
  for **PF-M1**: authentication, role-based access control, identity
  model, organisation / stakeholder model, and port-scoped access
  foundations. This is **a plan**, not an authorisation. It does
  **not** authorise PF-M1 implementation, does **not** select a
  vendor / framework / database / IdP, does **not** modify any
  previously merged document, and does **not** touch any code,
  infrastructure, or runtime.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_PLATFORM_FOUNDATION_PF_M1_SCOPE_PROPOSAL_v0.1.md` (PR #72, `1ac81fe`)
- `HORIZON_V1_PLATFORM_FOUNDATION_PHASE0_ALIGNMENT_v0.1.md` (PR #71, `9f77ba1`)
- `HORIZON_V1_PLATFORM_FOUNDATION_v0.1.md` (PR #68, `0228265`)
- `HORIZON_V1_M2_RETROSPECTIVE_v0.1.md` (PR #70, `fd73ca8`)
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59)
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54) — pattern precedent
- `HORIZON_CAPABILITY_BACKLOG.md` (HC-001 PCAM, HC-002 VTS Spatial)
- Beta 10 Immutability Rule (in force)
- Tony-direction state-alignment update (in-session, 2026-05-21)

---

## 1. Executive summary

PF-M1 introduces authenticated identity, role-based access
control, organisation / stakeholder modelling, and port-scoped
permissions to the Horizon V1 platform. After PF-M1, every
operational surface is gated by an authenticated user with a
server-side permission set scoped to specific ports and specific
stakeholder organisations.

This plan **does not select** a backend language, framework,
database engine, ORM, API style, IdP, or deployment topology.
Those decisions are deferred to **slice-level scope amendments**
made under explicit Tony authorisation **during** PF-M1
implementation, or to PF-M2 where they belong structurally. The
plan establishes architectural shape, sequencing, file / module
boundaries, validation gates, and stop conditions — not vendor
commitments.

The plan honours every constraint from the PF-M1 Scope Proposal
(PR #72) §5 — server-authoritative, IdP-agnostic, no
frontend-trusted permissions, multi-stakeholder by default,
port-scoped by default, audit-ready (not audit-emitting),
Centre Panel Navigation is not port switching, OI not
foundational, Kyber boundary, Smart Ocean X independence,
Beta 10 Immutability, no premature complexity, hybrid commercial
posture, no new npm deps without scope amendment.

PF-M1 is delivered in a slice-by-slice sequence (per the M2
precedent), each slice tightly scoped, individually reviewable,
individually merged. Phase 0 is an explicit authorisation gate:
no branch is created, no code is written, no tests are written
until Tony issues an explicit Phase 1 authorisation message.

This is **a plan**, not implementation authorisation. PF-M1
implementation requires a separate explicit *"Authorised: begin
Horizon V1 PF-M1 implementation, Phase 1 local only"* message
from Tony after this plan is reviewed and merged.

---

## 2. Resolved PF-M1 scope

The PF-M1 Scope Proposal (PR #72) §3 in-scope list is adopted
unchanged. The 14 in-scope items are summarised here for cross-
reference; the canonical detail is in §3 of the Scope Proposal.

| § | In-scope item |
|---|---|
| 3.1 | Authentication direction (server-side login / session / re-auth; IdP-agnostic) |
| 3.2 | User identity model (stable id, display attributes, resource-safe codes for PII) |
| 3.3 | Organisation / stakeholder model (port-authority, terminal operators, pilot orgs, tug operators, regulator) |
| 3.4 | Port-scoped access (per-user, server-enforced) |
| 3.5 | Role-based access control (RBAC, 8 workflow profiles + org-specific roles) |
| 3.6 | Least privilege |
| 3.7 | Server-authoritative permissions |
| 3.8 | Login / session posture |
| 3.9 | Multi-stakeholder readiness for Melbourne / Ports Victoria |
| 3.10 | Future multi-port user support (model only; UX deferred to PF-M8) |
| 3.11 | Identity / permissions / operational-actions separation |
| 3.12 | Login UI surface |
| 3.13 | Authenticated frontend boot |
| 3.14 | Sandbox identity defaults |

The 20 out-of-scope items from Scope Proposal §4 are adopted
unchanged. The 14 architectural constraints from Scope Proposal
§5 are adopted unchanged and re-enforced in §15 / §19 below.
Acceptance criteria A1–A15 from Scope Proposal §7 are adopted
unchanged and re-stated in §17 below.

---

## 3. Intended branch name

**Proposed branch:** `feat/v1-pf-m1`

**Branched off:** `origin/main` at the time of the Phase 1
authorisation. The current baseline is `1ac81fe`, but additional
merges may land between this plan and the Phase 1 authorisation;
the branch is created off whichever commit is current at that
moment.

The branch is **not** created by this plan. It is created
**only after** Tony's explicit "Authorised: begin Horizon V1
PF-M1 implementation, Phase 1 local only" message. Until that
message arrives, no branch exists, no commits exist, no PRs
exist.

Slice-level branches may use a slice suffix (e.g.
`feat/v1-pf-m1-slice-1-identity`) when a slice is large enough
to warrant a sub-branch. Each slice PR targets `main` directly
(M2 precedent).

---

## 4. File / folder scaffold

The scaffold below is **planned**, not created. Final paths
are subject to small adjustments during Phase 1 implementation
without re-planning, provided they remain within the §15
boundaries.

### 4.1 New backend code (server-side)

A new server-side identity module / package, in a path **separate
from the Beta 10 `server.py`**.

Working name (subject to change): `platform/` at the repo root,
or `horizon_platform/` if Python conventions prefer the
underscored form. The final name is decided at Phase 1 slice 1.

```
platform/                                # NEW (PF-M1)
├── identity/                            # User, Organisation, Role catalogue
│   ├── models.py / .ts / etc.           # Domain entity definitions
│   ├── persistence.py                   # Persistence-engine-agnostic interface
│   ├── catalogue.py                     # Role catalogue + permission bundles
│   └── service.py                       # User / org / role service operations
├── auth/                                # Login, session, request-boundary middleware
│   ├── session.py                       # Session creation / validation / invalidation
│   ├── middleware.py                    # Auth + RBAC enforcement at the request boundary
│   ├── login.py                         # Login / logout / re-auth handlers
│   └── secret_bootstrap.py              # Session secret loading (env var for sandbox)
├── access/                              # Port-scope + RBAC evaluation
│   ├── port_scope.py                    # Per-user port-scope queries + filters
│   ├── permissions.py                   # Role → permission evaluation
│   └── filters.py                       # Read-path port-scope filters used by API layer
├── admin/                               # Minimal admin surface for user / role / port-scope
│   └── handlers.py                      # Admin endpoints (PF-M1 minimal; PF-M2 polishes)
└── tests/                               # Unit + integration tests (server-side)
    ├── test_identity.py
    ├── test_auth.py
    ├── test_access.py
    └── test_server_authoritative.py     # Proves server-side enforcement; UI never authoritative
```

The directory `platform/` is **new**; it does not overlap with
the existing Beta 10 `server.py` path. The Phase 1 slice 1
locks the chosen language / framework in this directory; the
choice is constrained by §15.2 (deferred, not pre-selected).

### 4.2 New frontend code

```
frontend/src/
├── auth/                                # NEW (PF-M1)
│   ├── AuthContext.jsx                  # React context providing { user, isAuthenticated, login, logout, signedInScope }
│   ├── useAuth.js                       # Hook exposing the auth context
│   ├── ProtectedRoute.jsx               # Wrapper that gates authenticated routes
│   └── api.js                           # Client-side helpers calling the auth backend
├── features/login/                      # NEW (PF-M1)
│   ├── LoginPage.jsx                    # /login route component
│   └── LoginForm.jsx                    # Form fields (org id, username, password, submit)
└── layout/HorizonHeader.jsx             # MODIFIED — add logout affordance + signed-in indicator
```

### 4.3 Modified frontend code (minimal)

- `frontend/src/App.jsx` — route the user through the auth boundary
  before rendering the M2 shell
- `frontend/src/layout/HorizonHeader.jsx` — add a minimal logout
  affordance (adjacent to the existing clock; M2 hygiene-slice
  precedent for header changes)
- `frontend/src/api/horizon.js` — pass auth credentials with the
  fetch; switch from `VITE_API_BASE` fixture default to a real
  authenticated API endpoint when configured

### 4.4 New configuration

```
config/                                  # NEW (PF-M1; final location subject to choice)
├── organisations.example.yaml           # Sandbox org catalogue (Ports Victoria stakeholder mix)
├── roles.example.yaml                   # Sandbox role catalogue (8 workflow profiles + regulator)
└── port_scopes.example.yaml             # Sandbox port-scope assignments
```

The `.example.yaml` files document the expected shape; live
configuration is environment-loaded (env var or secret store).

### 4.5 New tests (frontend + backend)

```
frontend/tests/
├── auth/
│   ├── authContext.test.jsx             # AuthContext provides expected shape
│   ├── protectedRoute.test.jsx          # Unauthenticated → redirect to /login
│   └── loginPage.test.jsx               # Login form renders, has expected fields, no operator-action affordances
├── layout/
│   ├── (existing M2 tests preserved)
│   └── header.test.jsx                  # MODIFIED — assert logout affordance + signed-in indicator
└── integration/
    └── port_scope_filtering.test.jsx    # Authenticated frontend filters M2 views by port-scope

platform/tests/                          # NEW (server-side tests)
├── test_identity.py
├── test_auth.py
├── test_access.py
└── test_server_authoritative.py
```

### 4.6 Files explicitly NOT touched in PF-M1

- `server.py` (root) — untouched. Beta 10 Immutability Rule.
- `port_profiles.py` (root) — untouched.
- All six audit helper modules (`audit.py`, `conflict_audit.py`,
  `operator_action_audit.py`, `recommendation_audit.py`,
  `recommendation_presented_audit.py`, `session_audit.py`) —
  untouched. PF-M1 produces audit-relevant **logs** structurally;
  it does **not** modify the existing Phase 0.7 audit helpers.
- `railway.toml` (root) — untouched.
- `frontend/railway.json` — untouched.
- `frontend/public/fixtures/*.json` — untouched.
- `tests/test_beta10_regression.py` — untouched.
- `tests/test_darwin_demo_card.py` — untouched.
- Existing M2 adapters (`vtsAdapter`, `berthTimelineAdapter`,
  `shiftLogAdapter`, `pilotageAdapter`, `dashboardAdapter`,
  `summaryAdapter`, `vesselAdapter`, `conflictAdapter`,
  `etdRiskAdapter`, `guidanceAdapter`, `conditionsAdapter`, plus
  helpers) — **untouched**. PF-M1 wraps the M2 frontend in an
  auth boundary; the adapters themselves don't need to change.
- Existing M2 tab components (`VtsTab`, `BerthTimelineTab`,
  `ShiftLogTab`, `PilotageTab`, `PerformanceTab`,
  `DashboardTab`) — **untouched**. Same rationale.
- `frontend/src/hooks/useSummary.js` — touched only if absolutely
  necessary to pass auth credentials; the change is a minimal
  parameter addition, not a re-architecture.
- `CenterPanel.jsx`, `CenterPanelNav.jsx` — **untouched**.
  Centre Panel Navigation remains view navigation only.
- `LeftPanel.jsx`, `RightPanel.jsx` — **untouched** by default;
  may receive minor auth-context awareness if Phase 1 review
  shows it is necessary.

---

## 5. Dependency posture (no vendor lock-in)

PF-M1 deliberately **does not** lock in:

| Layer | Choice | When chosen |
|---|---|---|
| Backend language / framework | **Deferred.** Python + plain `http.server` is the Beta 10 precedent; Python + FastAPI / Flask / Litestar are candidates; Node / TypeScript shares the frontend stack | Phase 1 slice 1, under a scope amendment with explicit rationale |
| Persistence engine | **Deferred.** SQLite for sandbox is plausible; Postgres for production-ready posture is plausible | Phase 1 slice 1, under a scope amendment |
| ORM / query layer | **Deferred.** Choice follows persistence engine | Phase 1 slice 1 |
| API style | **Deferred.** PF-M1 has a small API surface (login, logout, session check, port-scope query, admin); REST + JSON is the simplest default. GraphQL / gRPC remain options for PF-M2 | Phase 1 slice 1 — minimum REST surface; PF-M2 may consolidate |
| IdP | **Deferred.** PF-M1 ships a custom default suitable for sandbox; external IdP integration is PF-M2 or PF-M7 | Future milestone scope |
| Session-token format | **Deferred.** HMAC-signed cookie (Beta 10 precedent) or short-lived signed JWT. Default proposal: HMAC-signed cookie for sandbox continuity | Phase 1 slice 2 |
| Frontend auth state management | **Reused.** React `useState` + Context (consistent with M2 patterns); no new state-management library | Implementation-time |
| Test framework | **Reused.** Vitest for frontend (M2 precedent); pytest for server-side (Beta 10 precedent extended into `platform/`) | Implementation-time |

**No new npm dependency** on the frontend is anticipated. If
specific Phase 1 slices require one, an explicit scope amendment
is requested (Scope Proposal §5.14 / stop condition §8.10).

**Server-side dependencies** are bound by the language /
framework choice in Phase 1 slice 1. If Python is chosen and
the auth implementation needs (for example) `passlib` or
`itsdangerous` or `psycopg`, the slice 1 scope amendment lists
them. If TypeScript is chosen, the slice 1 amendment lists the
chosen npm deps. No silent additions.

---

## 6. Identity / organisation / stakeholder model (conceptual shape)

The data model is described conceptually here. The schema (table
/ document / column layout) is decided at Phase 1 slice 1 in
the persistence-engine scope amendment.

### 6.1 Conceptual entities

| Entity | Purpose |
|---|---|
| **Organisation** | A port-authority customer, terminal operator, pilot organisation, tug operator, regulator, or partner |
| **OrganisationType** | The stakeholder class (`port_authority`, `terminal_operator`, `pilot_organisation`, `tug_operator`, `regulator`, `partner`) |
| **User** | An identity within exactly one organisation (multi-org membership deferred to PF-M8) |
| **Role** | A named permission bundle (e.g. `vtso`, `harbour_master`, `port_executive`, `pilotage_coordinator`, `towage_coordinator`, `terminal_operator`, `admin`, `replay_audit_user`, plus org-specific) |
| **PermissionBundle** | The set of action classes a role can perform within a port-scope (PF-M1: read-only action classes; PF-M4 introduces ACK / DEFER / etc.) |
| **Port** | A port served by this deployment — references the existing port catalogue (Brisbane, Melbourne, Geelong, Darwin, future ports) |
| **PortScopeAssignment** | A (User, Port, Role) triple recording which user has which role at which port |
| **Session** | A signed session record (cookie-backed or token-backed); ties to a User |

### 6.2 Key constraints in the model

- A User belongs to exactly **one** Organisation. Multi-
  organisation membership is **not** supported in PF-M1.
- A User holds **zero or more** PortScopeAssignments. A user with
  zero PortScopeAssignments cannot read operational data.
- A PortScopeAssignment carries a Role. The same user can be
  VTSO at port A and Coordinator at port B (different Roles
  per Port).
- Organisations of type `port_authority` may be associated with
  one or more Ports; non-`port_authority` Organisations are
  associated with ports through the PortScopeAssignment of
  their users.
- The Organisation entity is the unit of **tenant isolation**.
  PF-M1 does not implement multi-tenant infrastructure
  (deferred to PF-M8) but the model is multi-tenant-shaped.
- Resource-safe identifiers (per PR #57 §5.9) — for pilot users
  in pilot Organisations, the User display attribute may be a
  code (e.g. `PILOT_BNE_PSP_03`) rather than a real name. This
  is a per-organisation policy, not a global one.

### 6.3 Ports Victoria stakeholder shape (example, not commitment)

A sandbox seed for the Melbourne / Ports Victoria scenario:

| Organisation | Type | Example users | Port-scope |
|---|---|---|---|
| Ports Victoria | `port_authority` | VTSO, Harbour Master, executives, admin | Melbourne, Geelong (multi-port within the same port_authority) |
| DP World Melbourne | `terminal_operator` | Terminal operator | Melbourne (specific berths only — finer scope deferred to PF-M2 if needed) |
| Patrick Terminals | `terminal_operator` | Terminal operator | Melbourne |
| Melbourne Marine Pilots | `pilot_organisation` | Pilotage coordinator, pilots | Melbourne |
| Svitzer Melbourne | `tug_operator` | Towage coordinator | Melbourne |
| Marine Safety Victoria | `regulator` | Replay / Audit User | Melbourne, Geelong (read-only audit access — fully exercised in PF-M3+) |

This is **example seed data** for sandbox testing, not a
commitment of customer relationships.

---

## 7. Role / permission model

### 7.1 Initial role catalogue (sandbox; production catalogue is configuration)

| Role key | Description | Default permissions (PF-M1) |
|---|---|---|
| `vtso` | Vessel Traffic Services Operator (per Workflows §3.1) | Read all operational data in port-scope |
| `harbour_master` | Harbour Master (Workflows §3.2) | Read all operational data in port-scope; read full audit detail in port-scope |
| `port_executive` | Port Authority Executive / Viewer (Workflows §3.3) | Read aggregated KPIs in port-scope; **not** read fine-grained operator actions |
| `pilotage_coordinator` | Pilotage Coordinator (Workflows §3.4) | Read pilotage queue + relevant vessel context in port-scope |
| `towage_coordinator` | Towage Coordinator (Workflows §3.5) | Read towage schedule + relevant context in port-scope |
| `terminal_operator` | Terminal Operator (Workflows §3.6) | Read own terminal's berths only (finer-grained than port) |
| `admin` | Admin / System Owner (Workflows §3.7) | User / role / port-scope provisioning within their Organisation; **no** silent operational read |
| `replay_audit_user` | Replay / Audit User (Workflows §3.8) | Read audit history in port-scope; **no** edit |

### 7.2 Permission shape

Each Role's PermissionBundle is a set of `(action_class, scope_class)` pairs:

| action_class | scope_class | Meaning |
|---|---|---|
| `read.operational` | port-scope | Read operational data (vessels, conflicts, etc.) within the user's port-scope |
| `read.audit` | port-scope | Read audit-relevant events within the user's port-scope (PF-M3+ ; in PF-M1, structurally logged events only) |
| `read.aggregated` | port-scope | Read KPI / executive summaries (no operator identity) |
| `read.audit_full` | port-scope | Read audit-detail including operator identity (Harbour Master + regulator) |
| `read.terminal` | terminal-scope | Read own terminal's berths (terminal operator finer scope) |
| `admin.user` | organisation | Provision / disable users within the admin's organisation |
| `admin.role` | organisation | Edit role-permission mappings within the admin's organisation |
| `admin.port_scope` | organisation | Edit port-scope assignments within the admin's organisation |

The catalogue is **extensible at configuration time**, not code
time, per Scope Proposal §3.5. Adding a new Role does not
require a code deploy.

### 7.3 What PF-M1 does NOT add

- **No write action classes.** ACK / DEFER / APPLY / REJECT /
  ESCALATE are PF-M4 scope.
- **No cross-organisation read permissions.** Cross-stakeholder
  data sharing is PF-M8+ scope under explicit governance.
- **No deny-permissions.** PF-M1 is allow-only; deny-rules
  added later only if a real use case emerges.

---

## 8. Port-scope model

### 8.1 Assignment shape

```
PortScopeAssignment {
  user_id        : reference to User
  port_id        : reference to Port (Brisbane, Melbourne, Geelong, Darwin, …)
  role_key       : reference to Role
  organisation_id: derived from user.organisation (consistency check at write time)
  created_at     : timestamp
  created_by     : reference to admin User
  effective_from : timestamp (default: now)
  effective_to   : nullable timestamp (default: null = indefinite)
}
```

### 8.2 Read-path filter (server-authoritative)

Every read of operational data is wrapped in a port-scope filter:

1. Resolve the authenticated user
2. Look up the user's PortScopeAssignments
3. Compute the user's effective port-scope set: ports where the
   user has at least one active (effective_from ≤ now and
   (effective_to is null or effective_to > now)) PortScopeAssignment
4. Filter the read result to that port set
5. If the user has no active port-scope, return an empty result
   (not an error; the UI surfaces a clear "no port access" state
   per A5)

The filter is enforced at the **request boundary**. A read
handler **cannot** opt out of the filter. Phase 1 slice 4 tests
include a "directly-call-handler-without-filter" negative test
that fails compilation / fails at runtime.

### 8.3 What PF-M1 does NOT introduce

- **No port-context switcher UI.** Multi-port users see a
  union of their ports' data in PF-M1 read views. The port-
  context-switch affordance (operator chooses one active port at
  a time) is PF-M8 scope under explicit governance.
- **No Centre Panel Navigation modification.** Centre Panel
  Navigation remains view navigation only. The PF-M1 tests
  re-affirm this (Scope Proposal A7).

---

## 9. Session / login posture

### 9.1 Login flow

1. User navigates to `/login`
2. User enters organisation identifier (or it is inferred from a
   per-org subdomain in a future milestone), username, and
   password
3. Server validates credentials, issues a signed session
   (cookie-backed; HMAC-signed)
4. Browser stores the cookie; subsequent operational requests
   carry it
5. Server validates the cookie on every request via the
   request-boundary auth middleware
6. Logout invalidates the session **server-side** (server-side
   blocklist or session-token invalidation depending on
   implementation choice in slice 2)

### 9.2 Session secret bootstrap

- Sandbox: an environment-variable-bootstrapped session secret
  is acceptable (sandbox-only)
- Production: a managed-secret-store-bootstrapped secret is
  required (PF-M2 hardens this)

### 9.3 Session timeout

- Idle timeout (default proposal: 30 minutes)
- Absolute timeout (default proposal: 8 hours, matching a typical
  operational shift)
- Both configurable per deployment

### 9.4 Re-authentication for sensitive operations

- Admin actions (user / role / port-scope provisioning) require
  re-authentication if the last auth event was more than 15
  minutes prior
- Default proposal; final values are slice-level Implementation
  Plan amendments

### 9.5 What PF-M1 does NOT add

- **No MFA.** Multi-factor authentication is a separate
  hardening decision; arrives when first-real-customer security
  requirements demand it
- **No social login.** Auth0 / Clerk / Okta / port-authority SSO
  integrations are PF-M2 or PF-M7 scope
- **No password-rotation policy enforcement.** Sandbox accepts a
  static password; production posture is PF-M2 hardening scope
- **No password complexity rules** beyond a minimum length
  (default proposal: ≥ 12 characters; configurable)

---

## 10. Server-side auth boundary (request middleware)

### 10.1 Boundary placement

The request-boundary middleware sits **before** any operational
handler. Every operational endpoint passes through:

1. **Authentication** — extract session cookie / token, validate,
   resolve user; reject with 401 if invalid
2. **Authorization** — check that the user's PermissionBundle
   covers the action class of the requested endpoint; reject
   with 403 if not
3. **Port-scope filter** — for read endpoints, attach the user's
   port-scope set to the request context; the read handler
   consumes the filtered set
4. **Logging** — emit a structured audit-relevant log line (per
   Scope Proposal §5.6) for the request

### 10.2 What PF-M1 does NOT do

- **No business logic** in the middleware. Authentication,
  authorization, and port-scope filtering are the only concerns.
- **No request-validation framework.** That is PF-M2 (Platform
  Workflows §5.1 typed contracts arrive at PF-M2; PF-M1's
  endpoints are small and hand-validated)
- **No rate limiting**, **no security-header middleware**, **no
  error-response framework**. All PF-M2 hardening scope.

---

## 11. Frontend auth boundary expectations

### 11.1 Authenticated-boot sequence (slice 5 or 6)

```
App.jsx
  1. AuthProvider wraps the entire app
  2. AuthProvider on mount: GET /api/auth/me (the user's identity + port-scope)
     - if 401: render <LoginPage />
     - if 200: store the user in AuthContext, render the M2 shell
  3. M2 shell components consume useSummary() unchanged; horizon.js
     attaches the auth cookie automatically
  4. If a fetch returns 401 mid-session (idle timeout): clear
     AuthContext, redirect to /login
```

### 11.2 Frontend permission usage

- The frontend **may** hide UI affordances based on the user's
  Role set for UX clarity (e.g. hide the future ACK button on a
  decision card if the user has no `write.operational`
  permission)
- The frontend **never** authoritative-checks permissions. Every
  request is server-authoritative.
- Tests explicitly verify the dual property: (a) the UI hides
  what it should, and (b) the server rejects the same operation
  when called directly (Scope Proposal §5.3 / A6).

### 11.3 What PF-M1 does NOT introduce

- **No new operator-action affordances.** PF-M1 ships only the
  login form and the logout affordance. The decision-card
  placeholder on the right rail (post-PR #69) remains a
  placeholder; real ACK / DEFER / APPLY / OVERRIDE / ESCALATE
  arrive in PF-M4
- **No port-context switcher.** Re-affirmed. Centre Panel
  Navigation is view navigation only

---

## 12. Test strategy

### 12.1 Test categories

| Category | Where | What it proves |
|---|---|---|
| **Server-side unit** | `platform/tests/test_identity.py`, `test_auth.py`, `test_access.py` | Identity / auth / port-scope-filter logic is correct in isolation |
| **Server-side server-authoritative** | `platform/tests/test_server_authoritative.py` | Calling protected endpoints **directly** (without UI) is rejected; the UI is **never** the security gate |
| **Frontend auth-context** | `frontend/tests/auth/*` | AuthContext provides expected shape; ProtectedRoute behaves correctly; LoginPage renders / submits |
| **Frontend integration** | `frontend/tests/integration/port_scope_filtering.test.jsx` | Authenticated frontend filters M2 views by port-scope (uses fixtures that simulate the server's filtered response) |
| **No port switching** | `frontend/tests/layout/centerPanelNav.test.jsx` (existing) + new integration test | Centre Panel Navigation does not gain port-switching affordance |
| **No frontend-trusted permissions** | `platform/tests/test_server_authoritative.py` + frontend equivalent | Negative tests: bypass UI affordance, call endpoint directly, observe rejection |
| **Beta 10 regression** | `tests/test_beta10_regression.py` (existing; untouched) | Beta 10 behaviour unchanged |
| **Darwin demo card** | `tests/test_darwin_demo_card.py` (existing; untouched) | Darwin demo invariants unchanged |

### 12.2 Specific invariants the test suite must prove

1. **No anonymous operational access** — every protected endpoint
   returns 401 to unauthenticated callers (test_server_authoritative.py)
2. **Port-scope filter cannot be bypassed** — calling a read
   handler with an auth token from a user without scope-for-port-X
   returns no data for port X, even when port X is requested
   explicitly
3. **Server rejects what the UI hides** — if a user's role hides an
   admin endpoint in the UI, calling the same endpoint with that
   user's auth cookie returns 403
4. **Centre Panel Navigation has no port selector** — `<select>`,
   `<option>`, "Select port" / "Switch port" / "Change port" labels
   absent from all rendered UI (existing M2 test pattern continued)
5. **Cross-organisation read returns 403** — a terminal-operator user
   reading pilot-organisation roster data is rejected, even within
   the same port
6. **No-port-scope user sees no operational data** — a user with
   zero PortScopeAssignments receives an empty result on every
   operational read (not an error)
7. **Logout invalidates server-side** — a session token / cookie
   that has been logged out cannot be reused, even if the client
   replays it
8. **Beta 10 untouched** — protected-files probe returns 0 hits at
   every merge
9. **Independence Reset framing** — no "Smart Ocean X" string in
   any PF-M1 artefact
10. **Kyber boundary** — no PF-M1 code, comment, or copy references
    Kyber or approaches the Kyber boundary

### 12.3 Test framework choices

- **Server-side:** pytest (Python) or Vitest (TypeScript / Node) —
  determined at slice 1 with the language choice
- **Frontend:** Vitest (M2 precedent continues)
- **Frontend integration:** `react-dom/server.renderToStaticMarkup`
  for markup snapshots; for interactive auth-flow integration tests,
  the slice may need a DOM environment (`jsdom` or `happy-dom`).
  Adding a DOM environment **requires a scope amendment** per
  Scope Proposal §5.14 / stop condition §8.10

---

## 13. Beta 10 protection approach

PF-M1 protects Beta 10 by **construction**, not by convention.

13.1 **No `server.py` modification.** PF-M1 introduces new
server-side code in `platform/` (or equivalent), separate from
the Beta 10 `server.py`. Protected-files probe at every merge.

13.2 **No `port_profiles.py` modification.** The port catalogue
is referenced by the new platform code; the file itself is
read-only from PF-M1's perspective.

13.3 **No audit-helper modification.** All six existing audit
helpers untouched. PF-M1 produces audit-**relevant** logs in its
own logging path; the existing Phase 0.7 audit chain is not
extended or replaced by PF-M1.

13.4 **No root `railway.toml` modification.** Beta 10 deploy
config untouched.

13.5 **No `frontend/railway.json` modification.** V1 sandbox
frontend deploy config unchanged.

13.6 **No fixture modification.** The M2 fixture set
(`frontend/public/fixtures/`) is untouched. The fixture-fed
sandbox-mode fallback remains as a dev convenience; production
uses the real authenticated API.

13.7 **No Beta 10 regression gate change.**
`tests/test_beta10_regression.py` remains 46/46 at every merge.

13.8 **No Darwin demo-card gate change.**
`tests/test_darwin_demo_card.py` remains 8/8.

13.9 **Sandbox bundle inspection** — at Phase 2 sandbox deploy,
the bundle is checked for the absence of any Beta 10 hostname
and any production URL not explicitly authorised. Same probe
discipline as M2.

13.10 **Tony performs the post-PF-M1 Beta 10 visual check** —
same pattern as the M1 / M2 retrospective steps. Beta 10
behaviour visually unchanged is the canonical confirmation.

---

## 14. M2 frontend compatibility approach

PF-M1 wraps the M2 frontend in an auth boundary **without
modifying** the existing M2 components or adapters.

### 14.1 Unchanged M2 surfaces

- All five M2 tab components (`VtsTab`, `BerthTimelineTab`,
  `ShiftLogTab`, `PilotageTab`, `PerformanceTab`) — untouched
- All four M2-specific adapters (`vtsAdapter`,
  `berthTimelineAdapter`, `shiftLogAdapter`, `pilotageAdapter`)
  — untouched
- All existing M1 adapters and helpers — untouched
- `CenterPanel.jsx`, `CenterPanelNav.jsx` — untouched
- `LeftPanel.jsx`, `RightPanel.jsx`, `HorizonHeader.jsx` (except
  logout affordance), `ConditionsBar.jsx` — untouched in shape;
  `HorizonHeader.jsx` may receive a minor logout affordance
- `frontend/public/fixtures/*` — untouched

### 14.2 Changed M2 surfaces (minimal)

- `App.jsx` — wraps existing children in `<AuthProvider>` and
  `<ProtectedRoute>`. Existing children render identically when
  authenticated
- `horizon.js` — passes auth credentials; when in sandbox-mode
  fallback, behaviour matches M2 exactly
- `HorizonHeader.jsx` — adds a minimal logout affordance and
  signed-in indicator. The existing brand area + stats + clock
  layout is preserved (no header redesign; PR #63 logo hygiene
  precedent applies)

### 14.3 Backward-compatibility flag

A configuration flag `VITE_DISABLE_AUTH=true` permits sandbox-
mode operation (M2 behaviour, no auth) for dev convenience. This
flag is **forbidden** in production deployments. The flag's
presence is checked at boot; production deploys fail-fast if it
is set.

---

## 15. PF-M2 separation approach

PF-M1 must **not** drift into PF-M2 scope. Specific separations
to enforce:

| Concern | PF-M1 owns | PF-M2 owns |
|---|---|---|
| **API surface** | Minimum endpoints needed for auth + identity (login, logout, /me, /port-scope, admin endpoints) | Full V1 API surface (the 13 domains from Workflows §7); versioning policy; OpenAPI / IDL contract |
| **Observability** | Structured log lines for auth-relevant events | Metrics, tracing, alerting, audit-chain integrity probe, observability dashboards |
| **Hardening** | Basic auth posture (HTTPS, signed cookies, rejection of malformed requests) | Rate limiting, comprehensive security headers, error-response framework, formal request validation |
| **External IdP** | Custom default only | Auth0 / Clerk / Okta / port-authority SSO integrations |
| **Production deployment** | Sandbox deploy only (Phase 2) | Production deployment process, CI / CD pipelines, secret-store hardening |

If PF-M1 implementation discovers a PF-M2 concern is blocking
PF-M1 acceptance (Scope Proposal A1–A15), the stop condition
fires (§19 below) and a scope amendment is requested. Silent
drift into PF-M2 is forbidden.

---

## 16. Validation gates

### 16.1 Phase 1 (local) validation gates per slice

Every PF-M1 slice must pass these gates before opening a PR:

- Server-side unit tests pass (chosen test framework)
- Frontend Vitest tests pass
- `npm run build` is clean
- `tests/test_beta10_regression.py`: **46/46 PASS**
- `tests/test_darwin_demo_card.py`: **8/8 PASS**
- Combined Python gate: **54/54 PASS**
- Protected-files probe: **0 hits**
- Bundle-size soft guard (Scope Proposal A12): JS bundle stays
  within M2 baseline + 30 % (currently 225.81 kB; soft guard
  ≈ 293 kB)
- No new npm dependency (or, if needed, an explicit scope
  amendment has been approved before commit)

### 16.2 Phase 2 (sandbox) validation gates

- All Phase 1 gates green at the slice's merge commit
- Tony switches `horizon-v1-sandbox` source branch to
  `feat/v1-pf-m1` (Tony-side; execution agent does not perform
  Railway changes)
- Sandbox build succeeds (Tony-side observation)
- Sandbox bundle inspection: no production URLs; no Beta 10
  hostname; auth endpoint correctly configured
- A manual sandbox login + a sandbox-mode-fallback check both
  succeed

### 16.3 Phase 3 (merge) validation gates

- All Phase 1 + Phase 2 gates green
- PR is CLEAN / MERGEABLE / not draft
- ChatGPT review (if requested) has signed off
- Tony's explicit merge authorisation message received

### 16.4 Phase 4 (post-merge) validation

- `origin/main` advanced to the slice's merge SHA
- Beta 10 regression gate green on main
- Darwin demo-card gate green on main
- PRs #27 / #28 / #29 untouched
- `phase-0-complete` baseline immutable

### 16.5 Phase 5 (Beta 10 visual check) — Tony-side

- Tony performs the Beta 10 visual check (M1 / M2 retrospective
  precedent)
- Beta 10 production unchanged is the canonical confirmation
- This check is **non-substitutable** by the agent

---

## 17. Acceptance criteria

Adopted unchanged from the PF-M1 Scope Proposal §7 (A1–A15).
Each criterion is verifiable; verification is performed during
PF-M1 acceptance verification at milestone close.

| # | Criterion (PF-M1 Scope Proposal §7 verbatim) |
|---|---|
| A1 | Anonymous requests to operational endpoints return 401 |
| A2 | Cross-port access returns 403; treated as Sev-1 if it ever succeeds |
| A3 | Cross-organisation access requires explicit cross-org permission (none configured in PF-M1 sandbox by default) |
| A4 | Role catalogue maps cleanly to the 8 Operational Platform Workflows §3 profiles; org-specific roles addable via configuration |
| A5 | A no-port-scope user is presented with a clear "no port access" state, not a broken page |
| A6 | Server-authoritative — direct (non-UI) calls to protected endpoints are rejected 401 / 403 |
| A7 | Centre Panel Navigation remains view navigation only — no port selector |
| A8 | Sandbox test users exist for every required role / port-scope combination |
| A9 | Audit-relevant events during PF-M1 (login, logout, port-scope changes, role-permission edits) are logged structurally for PF-M3 ingestion |
| A10 | `tests/test_beta10_regression.py` remains 46/46 throughout PF-M1 |
| A11 | `tests/test_darwin_demo_card.py` remains 8/8 throughout |
| A12 | Bundle-size soft guard — frontend additions stay within M2 baseline + 30 % (≈ 293 kB) |
| A13 | Independence Reset framing preserved — no Smart Ocean X dependency framing |
| A14 | Kyber boundary in force — no PF-M1 decision approaches the boundary |
| A15 | Beta 10 untouched — protected-files probe 0 hits at every merge; `server.py` / root `railway.toml` / Beta 10 page bundle / audit helpers (in their current form) untouched |

---

## 18. Rollback plan

### 18.1 Pre-merge rollback (during implementation)

- All PF-M1 work occurs on `feat/v1-pf-m1` (and per-slice
  sub-branches if used). Any slice can be force-reset locally
  on explicit Tony authorisation, or the branch deleted entirely
- No production state is touched at any point; no rollback of
  production is required
- Local persistence (sandbox database) can be reset by recreating
  the file (SQLite) or recreating the schema (Postgres) per
  the slice 1 choice

### 18.2 Post-merge rollback

- A critical defect discovered post-merge → standard git revert
  of the slice's merge commit
- The PF-M1 persistence is new in PF-M1; reverting the code
  reverts the schema initialisation (the slice 1 choice ensures
  schema is created by the same code that the revert removes)
- A revert produces an empty `platform/` directory and removes
  the new frontend `auth/` directory; the M2 frontend continues
  to operate in sandbox-mode-fallback (M2 behaviour)

### 18.3 Sandbox rollback

- If sandbox bundle inspection at Phase 2 fails, Phase 2 stops
  immediately. Tony switches the sandbox source branch back to
  the previous (M2-final or PF-M1-prior-slice) state to restore
  known-good operation
- The implementer diagnoses locally on `feat/v1-pf-m1` and
  re-requests Phase 2 verification

### 18.4 Beta 10 rollback

- Beta 10 is **not touched** by PF-M1 by construction. There is
  nothing to roll back

### 18.5 Phase-0 baseline

- `phase-0-complete @ bf93373` is immutable through any rollback.
  The tag is not touched

---

## 19. Stop conditions / deny-list

PF-M1 implementation **stops immediately** and requests separate
Tony authorisation if any of the following occurs (adopted from
PF-M1 Scope Proposal §8, with two implementation-period
additions):

19.1 An item from Scope Proposal §4 (out of scope) is required
to satisfy any acceptance criterion. PF-M2 / PF-M3 / PF-M4+
work must not silently creep into PF-M1.

19.2 A frontend-trusted permissions pattern is proposed
("hidden in UI is enough"). Scope Proposal §5.3 violation.

19.3 A single-tenant assumption appears in the persistence
schema, the API surface, or the configuration model. Scope
Proposal §5.4 violation.

19.4 A port-scope filter is bypassed in any read path. Scope
Proposal §5.5 violation. Sev-1.

19.5 The Centre Panel Navigation gains a port-selector
affordance. Scope Proposal §5.7 violation.

19.6 An OI dependency is introduced anywhere in PF-M1. Scope
Proposal §5.8 violation.

19.7 A decision approaches the Kyber boundary. Scope Proposal
§5.9 violation. Explicit gate before proceeding.

19.8 Smart Ocean X framing appears in any PF-M1 artefact.
Scope Proposal §5.10 / Independence Reset violation.

19.9 A Beta 10 file is touched (`server.py`, root
`railway.toml`, audit helpers in their current form, Beta 10
page bundle). Scope Proposal §5.11 violation.

19.10 A new npm dependency is required on the frontend without
explicit scope amendment. Scope Proposal §5.14 violation.

19.11 The Beta 10 regression gate
(`tests/test_beta10_regression.py`) drops below 46/46.

19.12 The Darwin demo-card gate
(`tests/test_darwin_demo_card.py`) drops below 8/8.

19.13 The bundle-size soft guard (Scope Proposal A12) is
exceeded by more than 20 % (i.e. > 352 kB).

19.14 A multi-tenant infrastructure decision is required to
satisfy an acceptance criterion. Scope Proposal §4.10
violation.

19.15 An operator-action write path is required to satisfy an
acceptance criterion. Scope Proposal §4.6 violation.

19.16 An immutable audit ledger is required to satisfy an
acceptance criterion. Scope Proposal §4.4 violation.

19.17 A PR diff exceeds the tightly-scoped PF-M1 boundary
(e.g. drifts into PF-M2 API foundation work, PF-M3 audit-ledger
work, integration adapters).

19.18 ChatGPT review surfaces a material finding that requires
re-planning rather than mid-PR amendment.

### 19.19 NEW (implementation-period): backend language / framework choice cannot be deferred to slice 1

If a slice prior to slice 1 needs to land code that implies a
specific language / framework / persistence choice, stop. The
choice must be made deliberately at slice 1 with a scope
amendment.

### 19.20 NEW (implementation-period): the existing M2 frontend behaviour breaks under sandbox-mode fallback

If the auth-context wiring breaks the M2 read-only fixture-fed
behaviour (`VITE_DISABLE_AUTH=true` mode), stop. The PF-M1 work
must be additive; M2's behaviour in sandbox-mode-fallback is the
baseline.

When any stop condition fires, the implementer posts a stop
notice with the specific condition number and waits for explicit
Tony direction.

---

## 20. Phase sequencing

### 20.1 Phase 0 — Authorisation gate (pre-code)

Tony posts: **"Authorised: begin Horizon V1 PF-M1 implementation,
Phase 1 local only."** Until this message arrives:
- No `feat/v1-pf-m1` branch exists
- No code is written
- No tests are written
- No Railway change is made
- No fixture change is made

### 20.2 Phase 1 — Local implementation (slice-by-slice)

Recommended slice sequence (subject to slice 1 framework choice):

| Slice | Focus | Files (approx) |
|---|---|---|
| **Slice 1** | Identity model + persistence + framework choice (scope amendment captured) | `platform/identity/*`, persistence engine bootstrap, config skeleton |
| **Slice 2** | Login / session / logout handlers + session secret bootstrap | `platform/auth/*`, request-boundary middleware |
| **Slice 3** | Role catalogue + permission model | `platform/access/permissions.py`, role catalogue config |
| **Slice 4** | Port-scope assignment + filtered read helper + RBAC enforcement | `platform/access/port_scope.py`, `platform/access/filters.py` |
| **Slice 5** | Login UI surface on the frontend | `frontend/src/auth/*`, `frontend/src/features/login/*` |
| **Slice 6** | Authenticated boot + ProtectedRoute wiring + horizon.js auth header + logout affordance in HorizonHeader | `frontend/src/App.jsx`, `frontend/src/auth/*`, `frontend/src/api/horizon.js`, `frontend/src/layout/HorizonHeader.jsx` |
| **Slice 7** | Minimal admin surface (user / role / port-scope provisioning) | `platform/admin/*` + minimal admin UI |
| **Slice 8** | Sandbox identity defaults + test users + Ports Victoria seed | `config/*.example.yaml`, sandbox bootstrap scripts |
| **Slice 9** | Acceptance verification + PF-M1 retrospective draft | (documentation slice; mirrors the M2 acceptance verification flow) |

Each slice:
- Opens its own PR titled `feat(pf-m1): <slice topic> (slice N)`
- Pauses for Tony's review and explicit "Approved for merge"
- Merges with a single tightly-scoped diff
- Continues to the next slice only after the previous merges

### 20.3 Phase 2 — Sandbox verification

After Phase 1 is complete (all slices merged):
- Tony switches `horizon-v1-sandbox` source branch from
  `feat/v1-m1` (or wherever it currently points) to
  `feat/v1-pf-m1`
- The execution agent performs read-only sandbox verification:
  bundle inspection, deploy status, login flow smoke test,
  sandbox-mode-fallback check, no production URLs in bundle
- The execution agent reports findings and stops for Tony's
  merge authorisation (final PF-M1 sign-off)

### 20.4 Phase 3 — Acceptance verification + retrospective

- Acceptance verification report drafted (mirrors the M2
  acceptance verification report; A1–A15 verified)
- PF-M1 Retrospective drafted (mirrors PR #70 M2 Retrospective)
- Both merged under separate authorisations

### 20.5 Phase 4 — Tony's Beta 10 visual check (Tony-side)

- Tony confirms Beta 10 production behaviour is unchanged
- Result reported back; Claude does not initiate this check

### 20.6 Phase 5 — Close PF-M1, prepare PF-M2

- PF-M1 closed
- PF-M2 scope-proposal drafting may be authorised as a
  separate next step

---

## 21. Cross-document citation map

| This plan section | Cites | Source document |
|---|---|---|
| §1 Executive summary | Scope Proposal §1, §5 | PF-M1 Scope Proposal (PR #72) |
| §2 Resolved scope | Scope Proposal §3, §4, §5 | PF-M1 Scope Proposal |
| §3 Branch name | M2 Implementation Plan §3 pattern | PR #54 |
| §4 Scaffold | Scope Proposal §3, §6.1 | PF-M1 Scope Proposal |
| §5 Dependency posture | Scope Proposal §5.2, §5.12, §5.14; Platform Foundation §12.6 | PF-M1 Scope Proposal, PR #68 |
| §6 Identity model | Scope Proposal §3.2, §3.3; Workflows §3 | PR #72, PR #59 |
| §7 Role model | Scope Proposal §3.5; Workflows §3 | PR #72, PR #59 |
| §8 Port-scope model | Scope Proposal §3.4, §5.5 | PR #72 |
| §9 Session posture | Scope Proposal §3.8 | PR #72 |
| §10 Server-side auth boundary | Scope Proposal §3.7, §5.3 | PR #72 |
| §11 Frontend auth boundary | Scope Proposal §3.12, §3.13 | PR #72 |
| §12 Test strategy | Scope Proposal §7 (A1–A15) | PR #72 |
| §13 Beta 10 protection | Beta 10 Immutability Rule; Scope Proposal §5.11 | in-session governance, PR #72 |
| §14 M2 frontend compatibility | M2 Implementation Plan precedent; Centre Panel Navigation governance | PR #54, PR #67, in-session |
| §15 PF-M2 separation | Scope Proposal §4.1–§4.4; Platform Foundation §3, §7 | PR #72, PR #68 |
| §16 Validation gates | M2 Implementation Plan §15 precedent | PR #54 |
| §17 Acceptance criteria | Scope Proposal §7 (A1–A15) | PR #72 |
| §18 Rollback plan | M2 Implementation Plan §17 precedent | PR #54 |
| §19 Stop conditions | Scope Proposal §8 + 2 implementation-period additions | PR #72 |
| §20 Phase sequencing | M2 Implementation Plan §19 precedent | PR #54 |
| §21 Citation map | (this section) | self |
| §22 Recommendations | Scope Proposal §11 | PR #72 |

---

## 22. Recommendations

22.1 **Hold PF-M1 implementation until separate Tony
authorisation.** This plan does **not** authorise code.

22.2 **Honour the resolved scope from the Scope Proposal**
(adopted unchanged in §2). Re-opening resolved questions
mid-implementation is itself a stop condition (§19.18).

22.3 **Maintain the M2 governance pattern.** Explicit Tony
authorisation per phase and per slice; ChatGPT pre-merge review
where helpful; strict deny-list per authorisation; stop-for-
review markers.

22.4 **Defer framework / database / IdP choice to slice 1 scope
amendment.** Do not commit prematurely (§5).

22.5 **Keep slice diffs tight.** Each slice opens a single PR
against `main`. The M2 cycle delivered 7 slice PRs successfully;
PF-M1 follows the same pattern with the recommended 9 slices in
§20.2.

22.6 **No new dependencies without explicit scope amendment.**
Frontend especially — the M2 baseline (React + Vite + Vitest +
date-fns + date-fns-tz + serve) is sufficient unless a clear
need is documented (§5).

22.7 **Maintain Beta 10 Immutability throughout.** Protected-
files probe at every merge; Tony Beta 10 visual check at PF-M1
close.

22.8 **Maintain Independence Reset framing.** No Smart Ocean X
dependency framing in any PF-M1 artefact (§19.8).

22.9 **Maintain Kyber boundary.** No PF-M1 decision approaches
the Kyber boundary (§19.7).

22.10 **At PF-M1 close, draft a PF-M1 Retrospective** mirroring
PR #70 M2 Retrospective shape. The retrospective records what
worked, what should change before PF-M2 begins, and the items
deferred.

22.11 **If any §19 stop condition fires, stop and report**, do
not work around it.

22.12 **Do not auto-progress from this plan to PF-M1
implementation.** A separate explicit "Authorised: begin Horizon
V1 PF-M1 implementation, Phase 1 local only" message from Tony
is required (§20.1).

---

## End of PF-M1 Implementation Plan v0.1

This plan is descriptive and prescriptive but **does not**
authorise PF-M1 implementation.

Confirmed by this document:
- PF-M1 implementation is **not** authorised by this document.
- Any PF-M1 code work requires a separate explicit "Authorised:
  begin Horizon V1 PF-M1 implementation, Phase 1 local only"
  message from Tony after this plan is reviewed.
- The PF-M1 scope is unchanged from the Scope Proposal (PR #72)
  §3 / §4 split.
- All architectural constraints from Scope Proposal §5 are
  enforced throughout this plan.
- All acceptance criteria from Scope Proposal §7 (A1–A15) are
  adopted unchanged.
- The 18 stop conditions from Scope Proposal §8 are adopted,
  plus 2 implementation-period additions (§19.19, §19.20).
- Beta 10 remains isolated and immutable.
- Centre Panel Navigation remains view navigation only — no
  port selector is introduced by PF-M1.
- Ocean Intelligence is integration-shaped (PF-M7), not
  foundational; PF-M1 does not reference it architecturally.
- Kyber boundary in force.
- Smart Ocean X independence preserved (Independence Reset).
- No vendor / framework / database / IdP is selected by this
  plan; choices are deferred to slice 1 under scope amendment.
- All concrete PF-M1 work (Phase 0 authorisation, Phase 1
  slices, Phase 2 sandbox, Phase 3 acceptance + retrospective,
  Phase 4 Beta 10 visual check, Phase 5 close) is gated on
  separate explicit Tony authorisation per step.

**Next action:** Tony's review of:
- §3 branch name + §4 scaffold (location / shape of new
  modules)
- §6 / §7 / §8 conceptual model (identity / role / port-scope)
- §17 acceptance criteria
- §19 stop conditions (especially the two implementation-period
  additions)
- §20 slice sequencing

If approved and merged, the next governance step is Tony's
explicit "Authorised: begin Horizon V1 PF-M1 implementation,
Phase 1 local only" message. Until that message arrives, no
PF-M1 code is written.
