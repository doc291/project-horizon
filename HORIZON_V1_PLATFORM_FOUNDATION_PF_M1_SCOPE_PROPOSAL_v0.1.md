# Horizon V1 — Platform Foundation PF-M1 Scope Proposal (v0.1)

**Document status:** Draft for review — **scope proposal only**
**Document type:** Platform Foundation milestone scope proposal
**Target stream:** Future architecture / V1 Platform Foundation only
  **— Beta 10 is excluded by the Immutability Rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 9f77ba1`
  (post-PR #71 — Platform Foundation Phase 0 Alignment)
**Date:** 2026-05-21
**Scope of authority:** Defines the proposed scope for **PF-M1**:
  authentication, role-based access control, and platform identity
  foundations for Horizon V1. This is **a scope proposal**, not an
  authorisation. It does **not** authorise PF-M1 implementation,
  does **not** modify any previously merged document, and does
  **not** touch any code, infrastructure, or runtime.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_PLATFORM_FOUNDATION_PHASE0_ALIGNMENT_v0.1.md` (PR #71, `9f77ba1`)
- `HORIZON_V1_M2_RETROSPECTIVE_v0.1.md` (PR #70, `fd73ca8`)
- `HORIZON_V1_PLATFORM_FOUNDATION_v0.1.md` (PR #68, `0228265`)
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59)
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)
- `HORIZON_CAPABILITY_BACKLOG.md` (HC-001 PCAM, HC-002 VTS Spatial)
- Beta 10 Immutability Rule (in force)
- Tony-direction state-alignment update (in-session, 2026-05-21):
  Melbourne / Ports Victoria as first live-client target; PF-M1 /
  PF-M2 intentionally separate; HC-002 deferred; OI pluggable, not
  foundational; commercial posture hybrid systems-integrator + SaaS

---

## 1. Executive summary

PF-M1 is the first Platform Foundation milestone — the bootstrap
that every subsequent V1 platform capability depends on.

**PF-M1's job is to introduce authenticated identity, role-based
access control, and stakeholder-aware port-scoped permissions to
the Horizon V1 platform.** Until PF-M1 lands, all V1 surfaces are
fixture-fed and anonymous; after PF-M1, every operational surface
is gated by an authenticated user with a server-side permission
set scoped to specific ports and specific stakeholder
organisations.

PF-M1 anticipates the **Melbourne / Ports Victoria** first-live-
client posture (recorded in the state-alignment update). Ports
Victoria is a multi-stakeholder enterprise deployment with port-
authority, terminal operators, pilot organisations, and tug
operators sharing the same physical port with distinct
organisational identities. PF-M1's identity and authorisation
model is designed for this multi-stakeholder reality from day
one — not retrofitted later.

PF-M1 does **not** introduce:
- The full V1 API surface (PF-M2)
- Observability or platform hardening (PF-M2)
- The immutable append-only audit ledger (PF-M3)
- Operator-action write paths (PF-M4)
- The recommendation lifecycle (PF-M5)
- The replay surface (PF-M6)
- Ingestion / integration adapters (PF-M7)
- Multi-tenant infrastructure (PF-M8)
- The VTS spatial surface (HC-002, deferred to PF-M9 or later)
- Operational write-back integration (PF-M10)

PF-M1 introduces **only** what an authenticated, port-scoped,
RBAC-enforced operational surface needs in order to gate every
subsequent platform capability behind a real identity. The
existing M2 fixture-fed frontend continues to operate, but now
authenticated — login becomes a prerequisite to seeing anything
operational.

This document is a **scope proposal**, not an implementation
authorisation. PF-M1 implementation requires a separate explicit
"Authorised: begin Horizon V1 PF-M1 implementation" message after
this scope proposal is reviewed and merged, then a PF-M1
Implementation Plan PR (separate authorisation), and then a
PF-M1 Phase 0 readiness gate per the established pattern.

---

## 2. Problem statement

### 2.1 What V1 has today

After M2 (closed via PR #70), the V1 frontend is fixture-fed,
read-only, and **anonymous**:

- Anyone with the deploy URL can load the page
- The port shown is whichever fixture the deploy is currently
  configured to serve
- There is no concept of a user, a role, or a port-scope
  assignment
- No operator action is possible (no write paths)
- No audit trail is emitted (no operator actions to audit)

This posture is correct for M2: fixture-fed read-only surfaces
must remain anonymous-by-construction to avoid implying live
operational integration.

### 2.2 Why this must change before any live deployment

For any live-client deployment — particularly Melbourne / Ports
Victoria, which is enterprise-grade and multi-stakeholder —
anonymous access is **categorically unacceptable**:

- A VTSO seeing a different port's data is a Sev-1 security
  defect (cross-port data leakage; Platform Foundation §8.6)
- A terminal operator seeing pilot-organisation rosters is a
  PII / commercial-confidentiality breach
- An executive seeing operator identities is a privacy /
  workplace concern
- A partner / integration consumer hitting the operational API
  without auth is a security and data-integrity failure
- The first regulatory or insurance audit will fail immediately
  if there is no record of *who* saw *what*, *when*

### 2.3 Why PF-M1 must precede every other platform capability

From Platform Foundation v0.1 (PR #68) and the Alignment document
(PR #71) §3:

- **Auth must precede write paths.** An operator action requires
  an authenticated actor.
- **Auth must precede RBAC enforcement.** RBAC is by-identity.
- **Auth must precede audit emission.** Audit rows must attribute
  to a real authenticated actor.
- **Auth must precede multi-tenant deployment.** Tenant isolation
  is by-organisation-identity.

PF-M1 is therefore the **first** capability in the platform
sequence (per the Alignment document §3, PF-M1 = auth + RBAC).
Every subsequent milestone (PF-M2 API foundation, PF-M3 audit,
PF-M4 operator actions, …) depends on PF-M1 being live.

### 2.4 Why "auth in the frontend" is not the answer

The Operational Platform Workflows (PR #59) §5.6 / §6.7 and the
Platform Foundation (PR #68) §4.7 already establish that
**permissions are server-authoritative**:

> "Hidden in UI" is never a sufficient security control.

Therefore PF-M1 must introduce a real server-side identity and
authorisation layer — not a frontend-only login gate that hides
affordances. The current V1 frontend (M1 + M2) consumes a static
fixture from `frontend/public/fixtures/`; PF-M1's server-side
identity layer is the seam to a future API that the same
frontend will consume after PF-M2 lands.

### 2.5 Why enterprise-grade stakeholder separation matters from day one

The Tony-direction state-alignment update (2026-05-21) recorded
that Melbourne / Ports Victoria is the most likely first live-
client target. Ports Victoria is an enterprise deployment with:

- A port-authority customer (Ports Victoria itself)
- Terminal operators (separate organisations) operating berths
- Pilot organisations (separate, possibly multiple) providing
  pilotage services
- Tug operators (separate organisations) providing towage
- A regulator (Victorian government / Marine Safety Victoria)
  with audit / oversight interest

An identity model that conflates these stakeholders cannot serve
this customer. PF-M1 must model these as **distinct
organisations** with their own users, their own roles, their own
permission scopes — sharing a port physically without sharing
identity or commercially sensitive data.

A naive single-tenant model would force a re-architecture before
the first deployment. PF-M1 avoids that by getting the model
right from the start, **without** building the multi-tenant
*infrastructure* (which is PF-M8).

---

## 3. Resolved scope (in-scope for PF-M1)

PF-M1's in-scope items are listed below. Each item is a structural
property of the platform; the implementation specifics belong to
the PF-M1 Implementation Plan (separate later artefact).

### 3.1 Authentication direction

- **Server-side authentication** for every operational HTTP
  endpoint
- **Login / logout flows** with a recoverable session model
  (signed-cookie or short-lived signed-token pattern — final
  choice deferred to the Implementation Plan, but the existing
  Beta 10 `hz_sess` HMAC-cookie pattern is the precedent)
- **Re-authentication policy** for sensitive operations
  (admin actions, audit export) — placeholder; details in
  Implementation Plan
- **Login UI surface** on the V1 frontend — minimal, accessible,
  no operator-action affordances (PF-M1 has no operator actions)

PF-M1 does **not** select an external IdP (Auth0, Clerk, Okta,
port-authority SSO). The auth model must be IdP-agnostic and
support a custom default plus pluggable external IdP integration
later (PF-M2 or PF-M7).

### 3.2 User identity model

- **User entity** with stable identifier, display attributes
  (name, email, organisation), authentication credentials
  (handled per the security posture), creation / disable
  timestamps, last-login record
- **Resource-safe identity codes** for PII-sensitive roles (per
  HC-001 / PR #57 §5.9; e.g. `PILOT_BNE_PSP_03` style codes for
  pilot identities, not full names)
- **Organisation / stakeholder attribution** on every user — see
  §3.3
- **No silent admin elevation** — admins are a distinct identity
  class, not an attribute of operational users

### 3.3 Organisation / stakeholder model

This is the **key Melbourne / Ports Victoria enabler**.

- **Organisation entity** representing a port-authority customer,
  a terminal operator, a pilot organisation, a tug operator, or
  a regulator
- Each organisation has its own users, its own roles, its own
  port-scope assignments
- A user belongs to exactly **one** organisation (multi-
  organisation membership is deferred to PF-M8 or later)
- Cross-organisation data visibility is explicit: a terminal
  operator user sees their own berths in their assigned ports;
  they do **not** see pilot organisation rosters by default
- Organisations are **tenant-isolation-shaped** — they prepare
  the identity model for multi-tenant infrastructure (PF-M8)
  without building the infrastructure now

### 3.4 Port-scoped access

- Each user holds zero or more **port-scope assignments**
- Port-scope is a set of (organisation, port) pairs the user can
  read / act on
- Every read of operational data is filtered by port-scope
  server-side
- Every write of operational data (future PF-M4+) is authorised
  by port-scope server-side
- A user with no port-scope sees no operational data
- A user with one port-scope sees one port (and behaves like
  M2's current single-port view, but now authenticated)
- A user with multiple port-scopes will be able to switch active
  port context **at the platform layer** (deferred to PF-M8;
  PF-M1 establishes the structure but does not implement the
  UX of switching)

**Centre Panel Navigation remains centre-panel view navigation
only.** Port-context switching, when it arrives, is a separate,
audited, platform-layer affordance — not part of centre-panel
navigation.

### 3.5 Role-based access control (RBAC)

- **Role catalogue** mapping to the eight workflow profiles in
  Operational Platform Workflows §3 (VTSO / Coordination
  Operator, Harbour Master, Port Authority Executive, Pilotage
  Coordinator, Towage Coordinator, Terminal Operator, Admin,
  Replay / Audit User) plus any organisation-specific roles
  that emerge during PF-M1 design (e.g. partner-organisation
  admin)
- Each role maps to a **permission bundle** describing:
  - Read scopes (which entities the role can read)
  - Action classes (the action classes the role can perform —
    in PF-M1, only `read`; operator-action classes ACK / DEFER /
    APPLY / REJECT / ESCALATE arrive in PF-M4)
  - Admin scopes (admin-only roles have admin permissions on
    their organisation's users / roles / port-scopes)
- A user holds zero or more roles **per port-scope** — the same
  user may be a VTSO at port A and a Coordinator at port B
- Role-permission mapping lives in **configuration**, not hard-
  coded. PF-M1 ships an initial catalogue; M3+ may add roles
  via configuration without code changes

### 3.6 Least privilege

- Users receive **only** the permissions their assigned roles
  require — no implicit grants
- Admin is a separate identity class (see §3.2)
- Cross-port and cross-organisation access requires explicit
  scope assignment; no role implicitly grants it

### 3.7 Server-authoritative permissions

- Every read and every write is authorised server-side at the
  request boundary
- The frontend may **hide affordances** based on role for UX
  clarity, but server-side enforcement is authoritative
- "Hidden in UI" is **never** a sufficient security control
- The Phase 1.1 auth-on-what-if pattern from the Beta 10 heritage
  (`server.py:do_POST` 401 guards) is the implementation
  precedent for write-path protection — generalised in PF-M1 to
  cover all operational endpoints

### 3.8 Login / session posture

- HTTPS-only operational endpoints (production posture; sandbox
  may run mixed for dev convenience but production must be
  HTTPS-only)
- Session secrets in a managed secret store (PF-M2 hardens this;
  PF-M1 uses an environment-variable bootstrap acceptable for
  sandbox-only operation)
- Session rotation supported without breaking the future audit
  chain (PF-M3) — the audit chain attributes by user identity,
  not session ID, so session rotation is operationally safe
- Session timeout: idle and absolute, configurable
- Logout invalidates the session server-side; not just client-
  side cookie clearing

### 3.9 Multi-stakeholder readiness for Melbourne / Ports Victoria

- The identity model supports the Ports Victoria stakeholder
  mix from PF-M1 design:
  - **Port-authority customer** organisation (Ports Victoria)
  - **Terminal operator** organisations (DP World, Patrick, …)
  - **Pilot organisation(s)** (multiple possible)
  - **Tug operator(s)** (multiple possible)
  - **Regulator** (Marine Safety Victoria, oversight role with
    read-only audit access — implementation deferred to PF-M3+,
    but the role exists in the catalogue)
- A user of one organisation cannot see another organisation's
  PII / commercial data without an explicit cross-organisation
  read permission (which PF-M1 does not implement; future
  cross-stakeholder permissions are PF-M8+ scope)

### 3.10 Future multi-port user support

- The data model supports a user with multiple port-scope
  assignments
- The platform layer surfaces a port-context-switch affordance
  for such users — **but** the UI for this is **NOT**
  implemented in PF-M1. PF-M1 establishes the structure; the
  UX of port-context switching arrives in a later milestone
  alongside multi-tenant infrastructure (PF-M8)

### 3.11 Clear separation between identity, permissions, and operational actions

- **Identity** (who the user is) lives in the user model
- **Permissions** (what the user is allowed to do) live in the
  RBAC mapping
- **Operational actions** (what the user actually does) are NOT
  in PF-M1 scope — those are PF-M4+

This separation matters because:
- The identity layer can evolve (external IdP integration) without
  touching the permissions model
- The permissions model can evolve (new roles, new scopes) without
  touching the operational-action layer
- The operational-action layer (PF-M4+) consumes both via clean
  interfaces

### 3.12 Login UI surface

- The V1 frontend gains a login page reachable via a known URL
  (likely `/login`, matching the Beta 10 precedent)
- The login page is intentionally minimal: organisation
  identifier, username, password, submit
- No "remember me" cookie of arbitrary lifetime — session
  posture is per §3.8
- No social-login affordances in PF-M1 (those would be PF-M2 or
  PF-M7 if an external IdP integration is authorised)
- The login page does not surface operational data to
  unauthenticated callers
- The Horizon and AMS logos (from PR #63) appear on the login
  page using the same assets

### 3.13 Authenticated frontend boot

- The V1 frontend boot sequence is augmented:
  - On load, check authentication
  - If unauthenticated → redirect to login page
  - If authenticated → load the user's port-scope assignments
    and proceed to the M2 surface, filtered by port-scope
- The existing M2 components remain unchanged in shape; what
  changes is the data they consume now comes from a port-scope-
  filtered server response instead of a static fixture
- The static fixture path remains as the **sandbox / dev
  fallback** (VITE_API_BASE) and is explicitly disabled in
  production deployments

### 3.14 Sandbox identity defaults

- The sandbox (`horizon-v1-sandbox`) gets a default set of test
  users covering each role
- Test users have **resource-safe** identifiers that signal
  their test status (e.g. `test-vtso-mel`, `test-hm-mel`,
  `test-pilot-mel`)
- The default set is **not** present in production; production
  bootstraps with an admin user only and provisions real users
  via the admin surface

---

## 4. Out of scope (explicitly deferred)

The following items are **out of scope for PF-M1**. Each is a
separate Platform Foundation milestone (or a different scope
entirely) under separate explicit authorisation.

| # | Item | Belongs to |
|---|---|---|
| 4.1 | Full V1 API surface (versioning, OpenAPI / IDL contract, deprecation policy, all 13 API domains beyond identity) | **PF-M2** |
| 4.2 | Observability stack (structured logs, metrics, tracing, alerting, audit-chain probe) | **PF-M2** |
| 4.3 | Platform hardening (rate limiting, request validation framework, error-handling middleware, security headers comprehensive review) | **PF-M2** |
| 4.4 | Immutable append-only audit ledger with hash chain | **PF-M3** |
| 4.5 | Recommendation lifecycle persistence and presentation events | **PF-M3** |
| 4.6 | Operator-action write paths (ACK / DEFER / REJECT / ESCALATE / APPLY) | **PF-M4** |
| 4.7 | Notification orchestration (outbound dispatch to pilot dispatcher, tug operator, etc.) | **PF-M5** |
| 4.8 | Replay surface (snapshot derivation, swimlane timeline backend, audit-export workflow) | **PF-M6** |
| 4.9 | Integration / ingestion adapters (AIS, BoM, PMS, Ocean Intelligence) | **PF-M7** |
| 4.10 | Multi-tenant infrastructure (per-tenant isolation at infrastructure level, per-tenant secrets, per-tenant observability scopes) | **PF-M8** |
| 4.11 | VTS spatial surface (HC-002) | **PF-M9 or later** |
| 4.12 | Operational write-back integration (Horizon updating PMS, statutory reporting, billing) | **PF-M10** |
| 4.13 | External IdP integration (Auth0, Clerk, Okta, port-authority SSO) | **PF-M2 or PF-M7** depending on commercial path |
| 4.14 | Production deployment (the platform deployment topology, infrastructure choice, CI/CD pipeline for production) | Separately authorised; targeted FY28 H1 per PR #71 §5 |
| 4.15 | Multi-organisation membership (a single user belonging to multiple organisations simultaneously) | **PF-M8 or later** |
| 4.16 | Port-context-switch UX for multi-port users | **PF-M8** |
| 4.17 | Stage E-prod activation | Out of V1 platform scope |
| 4.18 | Beta 10 modification | **Never** — Beta 10 Immutability Rule |
| 4.19 | Production auth implementation against a real customer's IdP / SSO | Separately authorised once PF-M2 and PF-M7 are stable |
| 4.20 | Railway / deployment-platform changes | Separately authorised under deployment governance |

If any item in §4 is required to satisfy a PF-M1 acceptance
criterion, PF-M1 stops and a separate authorisation is requested.

---

## 5. Architectural constraints

The following constraints are **load-bearing** for PF-M1 and
apply to every implementation decision.

### 5.1 Server-authoritative

Every read, every write, every authorisation decision is
server-side. The frontend is one consumer of the identity /
permissions API. No business logic, no permission decisions,
no port-scope filtering lives only in the browser.

### 5.2 IdP-agnostic

PF-M1's identity model does not lock in a specific external IdP.
A custom default implementation is acceptable for PF-M1 sandbox;
external IdP integration (Auth0 / Clerk / Okta / port-authority
SSO) is a later, separately-authorised decision.

### 5.3 No frontend-trusted permissions

The frontend may hide affordances based on the user's role set
for UX clarity, but server-side enforcement is the authoritative
gate. Test patterns must verify both: (a) the UI hides what it
should, and (b) the server rejects the same operation when called
directly without the UI.

### 5.4 Multi-stakeholder by default

The identity model treats organisations as first-class. A single-
tenant assumption is forbidden in PF-M1, even if the first live-
client deployment serves only one port-authority customer
initially. Subsequent stakeholders (terminal operators, pilot
organisations, tug operators) must be addable without re-
architecture.

### 5.5 Port-scoped by default

Every operational read is filtered by port-scope server-side. The
filter is not optional. A user with no port-scope sees no
operational data.

### 5.6 Audit-ready, audit-emitting later

PF-M1 prepares the audit-row shape (actor identifier, timestamp,
session, action, target, outcome) but does **not** implement the
append-only hash-chained ledger. That is PF-M3. Audit-relevant
events that occur during PF-M1 (login, logout, port-scope
assignment changes, role-permission-mapping changes) are
**logged structurally** so PF-M3 can ingest them retroactively
when the ledger is live.

### 5.7 Centre Panel Navigation is not port switching

Re-affirmed from the M2 close governance:
- The M2 Centre Panel Navigation is centre-panel view navigation
  only (Dashboard / Berth Timeline / Shift Log / VTS / Pilotage /
  Performance)
- Port-context switching for multi-port users is a distinct
  platform-layer affordance, **NOT** part of the centre panel
- PF-M1 does **not** introduce a port selector in any UI surface
- PF-M1's data model supports multi-port users, but the UX of
  switching is deferred to PF-M8

### 5.8 Ocean Intelligence is not foundational

Per the Tony-direction state-alignment update:
- OI is a **pluggable API data source**, not a Horizon
  dependency
- The platform must preserve the ability to operate **without**
  OI
- Horizon owns coordination logic and operational state
  regardless of ETA source
- PF-M1 does not reference OI in any architectural decision; OI
  becomes an integration adapter in PF-M7

### 5.9 Kyber boundary in force

PF-M1 work does not cross the Kyber boundary. Any decision that
approaches the boundary requires explicit authorisation via a
dedicated scope-proposal cycle. Specifically: PF-M1's auth
posture must not include cryptographic primitives or session
hardening choices that touch the Kyber boundary; standard
HMAC-cookie or short-lived-signed-token patterns are
sufficient.

### 5.10 Smart Ocean X independence

No Smart Ocean X dependency framing appears in PF-M1
artefacts, code, copy, or configuration. The Horizon Independence
/ Architecture Reset (PR #51) framing is preserved verbatim.

### 5.11 Beta 10 Immutability

PF-M1 work does not touch Beta 10. `server.py`, root
`railway.toml`, audit helpers in their current Beta 10 form,
and the Beta 10 page bundle are out of scope. PF-M1 introduces
**new** server-side identity code in a path / module separate
from the Beta 10 surface.

### 5.12 No premature complexity

Per Platform Foundation §12.1, the simplest design that satisfies
the constraints is preferred. A single-process auth implementation
backed by a small relational schema is acceptable for PF-M1; the
infrastructure for horizontal scaling, durable session storage at
scale, and external IdP integration arrive when concrete operational
need demands them, not pre-emptively.

### 5.13 Hybrid commercial posture preserved

Per the Tony-direction state-alignment update: the commercial
posture is hybrid systems-integrator + platform SaaS. PF-M1's
identity model must support:
- **Managed deployments** (AMSG-hosted, single-tenant or multi-
  tenant configurable)
- **Configurable per-port deployments** (port-scope assignments
  are configuration, not code)
- **Operational integration work** (custom integration adapters
  per port-authority — but those adapters land in PF-M7)
- **Future multi-tenant SaaS posture** (the model is multi-
  tenant-ready; the infrastructure catches up in PF-M8)

### 5.14 No new npm dependencies on the frontend without scope amendment

The login UI surface (§3.12) and the authenticated-boot wiring
(§3.13) should reuse the existing M2 React + Vite stack. If a
specific dependency proves necessary (e.g. a session-management
helper, a form-handling library), it requires an explicit scope
amendment, **not** silent addition during implementation.

---

## 6. Implementation boundaries

These boundaries describe **where PF-M1 stops** in physical
terms.

### 6.1 New server-side code

PF-M1 introduces new server-side code for:
- User and Organisation entity persistence
- Role catalogue and permission-mapping persistence
- Port-scope-assignment persistence
- Login / logout handlers
- Session creation, validation, and invalidation
- Request-boundary auth middleware
- Port-scope-filtered read helpers (the shape of the future
  `GET /api/summary` filter; the full API surface lands in PF-M2)
- Admin endpoints for user / role / port-scope provisioning

The new code is in **a module / path / package separate from
the Beta 10 `server.py`**. PF-M1 does not modify `server.py`.
The deployment topology (whether the new module runs in the
same process as Beta 10's `server.py`, or a separate process,
or a separate service) is a PF-M1 Implementation Plan decision,
not a scope decision.

### 6.2 New frontend code

PF-M1 introduces new frontend code for:
- A login page route (likely `/login`)
- An authentication context / hook used by the existing
  `useSummary` and any future hooks
- An authenticated boot sequence
- A logout affordance (likely in the existing header — placement
  TBD in Implementation Plan)
- Role-aware affordance hiding (UX-only; not authoritative)

### 6.3 New persistence

PF-M1 introduces persistence for users, organisations, roles,
permissions, port-scope assignments, and sessions. The storage
choice (relational / document / hybrid; specific engine;
specific schema) is a PF-M1 Implementation Plan decision. The
**audit ledger is not PF-M1's persistence concern** — that is
PF-M3.

### 6.4 New configuration

PF-M1 introduces configuration for:
- Organisation catalogue per deployment (which organisations
  exist; their stakeholder type)
- Role catalogue per deployment (which roles exist; their
  permission bundles)
- Port catalogue per deployment (which ports the deployment
  serves; same shape as the existing port-profile structure)
- Session-secret bootstrap (environment-variable acceptable for
  sandbox; managed secret store for production)

### 6.5 No new deployment

PF-M1 does **not** deploy to production. The PF-M1 sandbox
deployment is a separately authorised PF-M1 Phase 2 step (per
the M2 pattern). Production deployment is targeted FY28 H1
per PR #71 §5 under separate authorisation.

### 6.6 No new Beta 10 touch

Re-affirmed: `server.py`, root `railway.toml`, Beta 10 audit
helpers in their current shape, and the Beta 10 page bundle are
out of scope. PF-M1's protected-files probe is identical to
M2's at every merge.

---

## 7. Acceptance criteria

PF-M1 is accepted when each of A1–A15 is verifiable. Each
criterion will be checked during PF-M1 acceptance verification
(separate document at PF-M1 close, mirroring the M2 acceptance
verification report).

| # | Criterion |
|---|---|
| **A1** | A user cannot access any operational surface without authentication. Anonymous requests to operational endpoints return 401. |
| **A2** | A user accesses **only** the operational surfaces their port-scope assignment allows. Cross-port access (a user reading data from a port not in their scope) returns 403 and is treated as a Sev-1 security defect if it ever succeeds. |
| **A3** | An organisation's users see **only** their organisation's data unless an explicit cross-organisation permission is set (no cross-organisation permissions are configured for PF-M1 sandbox by default). |
| **A4** | The role catalogue maps cleanly to the eight Operational Platform Workflows §3 profiles (VTSO, Harbour Master, Executive, Pilotage Coordinator, Towage Coordinator, Terminal Operator, Admin, Replay/Audit User). Additional organisation-specific roles may be added without code changes (configuration only). |
| **A5** | A user with no port-scope assignment sees no operational data and is presented with a clear "no port access — contact your admin" state, not a broken page. |
| **A6** | The login / logout / session-validation paths are server-authoritative. The frontend never makes authorisation decisions independently. Tests verify that calling protected endpoints directly without the UI (curl-style) is rejected with 401 / 403 as appropriate. |
| **A7** | Centre Panel Navigation remains centre-panel view navigation only. No port selector exists in any PF-M1 surface. The M2 test suite (`centerPanelNav.test.jsx`) continues to assert this; PF-M1 adds an integration test confirming the property at the platform level. |
| **A8** | Sandbox test users exist for each role / port-scope combination required to exercise the PF-M1 surface. Production deployment bootstraps with an admin user only. |
| **A9** | The audit-relevant events that occur during PF-M1 operation (login, logout, port-scope assignment changes, role-permission-mapping changes) are logged structurally so PF-M3 can ingest them retroactively. |
| **A10** | Beta 10 regression gate `tests/test_beta10_regression.py` remains green at 46/46 throughout PF-M1, both pre-merge and post-merge. |
| **A11** | Darwin demo-card gate `tests/test_darwin_demo_card.py` remains green at 8/8 throughout. |
| **A12** | Bundle-size soft guard: PF-M1 frontend additions stay within the post-M2 baseline + 30% (currently 225.81 kB JS; soft guard is therefore approximately 293 kB). If PF-M1 exceeds this, the scope is re-evaluated. |
| **A13** | Independence Reset framing preserved — no Smart Ocean X dependency framing anywhere in PF-M1 artefacts, code, copy, or configuration. |
| **A14** | Kyber boundary in force — no PF-M1 decision approaches the boundary; if it ever does, an explicit authorisation gate is triggered before proceeding. |
| **A15** | Beta 10 untouched — no PF-M1 commit modifies `server.py`, root `railway.toml`, the Beta 10 page bundle, or any of the six audit helper modules in their current form. The protected-files probe returns 0 hits at every merge. |

---

## 8. Deny-list / stop conditions

PF-M1 implementation **stops immediately** and requests a separate
Tony authorisation if any of the following occurs (mirroring the
M2 Implementation Plan §18 pattern).

8.1 An item from §4 (out of scope) is required to satisfy any
acceptance criterion. PF-M2 / PF-M3 / PF-M4+ work must not
silently creep into PF-M1.

8.2 A frontend-trusted permissions pattern is proposed (e.g.
"the UI hides this action so the server doesn't need to check").
This is a §5.3 constraint violation.

8.3 A single-tenant assumption appears in the persistence schema,
the API surface, or the configuration model. §5.4 violation.

8.4 A port-scope filter is bypassed in any read path. §5.5
violation. Sev-1.

8.5 The Centre Panel Navigation gains a port-selector affordance.
§5.7 violation.

8.6 An OI dependency is introduced anywhere in PF-M1. §5.8
violation. OI is integration-shaped (PF-M7), not
foundational.

8.7 A decision approaches the Kyber boundary. §5.9 violation.
Explicit gate before proceeding.

8.8 Smart Ocean X framing appears in any PF-M1 artefact.
§5.10 / Independence Reset violation.

8.9 A Beta 10 file is touched (`server.py`, root `railway.toml`,
audit helpers, Beta 10 page bundle). §5.11 violation.

8.10 A new npm dependency is required on the frontend. §5.14
requires explicit scope amendment.

8.11 The Beta 10 regression gate
(`tests/test_beta10_regression.py`) drops below 46/46.

8.12 The Darwin demo-card gate (`tests/test_darwin_demo_card.py`)
drops below 8/8.

8.13 The bundle-size soft guard (A12) is exceeded by more than
20% (i.e. > 352 kB). Scope re-evaluation required.

8.14 A multi-tenant infrastructure decision is required to
satisfy an acceptance criterion. §4.10 violation — multi-tenant
infrastructure is PF-M8.

8.15 An operator-action write path is required to satisfy an
acceptance criterion. §4.6 violation — operator actions are
PF-M4.

8.16 An immutable audit ledger is required to satisfy an
acceptance criterion. §4.4 violation — the audit ledger is
PF-M3. (PF-M1 logs audit-relevant events structurally; that is
not the same as implementing the ledger.)

8.17 A PR diff exceeds the tightly-scoped PF-M1 boundary
(e.g. drifts into PF-M2 API foundation work, PF-M3 audit-ledger
work, integration adapters).

8.18 ChatGPT review surfaces a material finding that requires
re-planning rather than mid-PR amendment.

When a stop condition fires, the implementer posts a stop notice
with the specific condition number from this list and waits for
explicit Tony direction.

---

## 9. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 9.1 | Identity-model design proves insufficient for Ports Victoria's actual stakeholder structure when real-customer requirements arrive | Medium | High | Engage Ports Victoria SMEs during PF-M1 design; treat the identity model as the highest-review-priority artefact in the Implementation Plan |
| 9.2 | Persistence-engine choice locks the platform into a substrate that cannot scale to multi-tenant later | Medium | Medium | Choose a substrate with documented multi-tenant scaling patterns; defer the actual multi-tenant deployment to PF-M8 but avoid choices that preclude it |
| 9.3 | Session-management complexity grows during implementation (e.g. SSO, MFA, social-login affordances creep in) | Medium | Medium | §5.14 + §8.10 stop conditions; external IdP integration is explicitly PF-M2 / PF-M7 |
| 9.4 | Frontend-trusted permission patterns leak in because they are easier than server-side enforcement | High | High | §5.3 + §8.2 stop conditions; every PR adds tests confirming server-side enforcement |
| 9.5 | The audit-relevant events logged during PF-M1 (§5.6 / A9) are not structurally ingestible by the PF-M3 audit ledger when it arrives | Low | Medium | Document the event shape in the PF-M1 Implementation Plan; ensure PF-M3 scope explicitly covers retroactive ingestion |
| 9.6 | Centre Panel Navigation regression — a future contributor adds a port-selector affordance to the existing tab strip | Low | Medium | M2 test suite already asserts absence; PF-M1 adds an integration test confirming the property at the platform level (A7) |
| 9.7 | OI dependency creeps in through a well-meaning "use OI's ETA as default" decision | Low | Medium | §5.8 + §8.6 stop conditions; PF-M1 architectural review explicitly checks |
| 9.8 | Beta 10 touch through accidental file edits | Very low | Very high | Protected-files probe at every merge (M2 precedent); §5.11 + §8.9 stop conditions |
| 9.9 | Bundle size exceeds A12 soft guard due to login UI dependencies | Low | Medium | §5.14 + §8.10 + §8.13 stop conditions |
| 9.10 | PF-M1 sandbox deployment surfaces real-world auth bugs that delay PF-M2 start | Medium | Medium | PF-M2 scope-proposal drafting can begin in parallel after PF-M1 implementation is underway and the auth surface is stable in sandbox |
| 9.11 | Smart Ocean X framing reintroduction via a copy-paste from older AMSG materials | Low | Medium | Independence Reset §13 / §14 framing required; ChatGPT review gate at PF-M1 close |
| 9.12 | Kyber boundary crossing through an inadvertent cryptographic-primitive choice | Low | High | §5.9 + §8.7 stop conditions; cryptographic choices reviewed at scope-proposal and implementation-plan stages |
| 9.13 | PF-M1 milestone duration grows beyond the FY27 H1 directional target | Medium | Medium | Slice-by-slice authorisation pattern (M2 precedent); each slice is independently reviewable; scope creep triggers stop condition |
| 9.14 | Stakeholder-model decisions made in PF-M1 conflict with eventual multi-tenant infrastructure decisions in PF-M8 | Low | High | PF-M8 scope proposal must cite the PF-M1 stakeholder model; multi-tenant infrastructure must be additive to the PF-M1 model, not a replacement |
| 9.15 | A first-real-customer security review demands a posture beyond PF-M1's scope (e.g. MFA, comprehensive audit trail of all reads) | Medium | High | Build it in PF-M2 / PF-M3 when customer-specific requirements arrive; the FY28 H1 first-live-client window includes time for these additions |

No risk in this register is currently high-likelihood AND
high-impact except 9.4 (frontend-trusted permissions). The
mitigation pattern is tight: stop condition + test enforcement +
review gate at every merge.

---

## 10. Open questions

These remain unresolved at the scope-proposal level. The PF-M1
Implementation Plan PR must resolve each before implementation
begins.

### 10.1 Identity model

10.1.1 What is the **organisation hierarchy** model? Flat (every
org is peer)? Or a port-authority-customer parent with terminal-
operator / pilot-organisation / tug-operator children?

10.1.2 What is the **organisation onboarding** flow? Admin creates
organisations through the admin surface? Pre-seeded in
configuration? Both?

10.1.3 How are **resource-safe identifiers** generated for new
users? Auto-generated (e.g. `PILOT_BNE_PSP_03` pattern)? Admin-
chosen? Either?

10.1.4 Does a user have a single **display name** or do
display-name conventions vary by organisation type (port-
authority uses real names; pilot organisations use resource-safe
codes)?

### 10.2 Authentication

10.2.1 **Session vs token** — HMAC-signed cookie (Beta 10
precedent) or short-lived signed JWT? Default proposal:
HMAC-signed cookie for sandbox (continuity); revisit at PF-M2.

10.2.2 **Password posture** — for sandbox, a single shared
password per role is acceptable; for production, what minimum
posture is required (length, complexity, rotation, history)?

10.2.3 **MFA** — required at PF-M1, or PF-M2/PF-M3? Default
proposal: not required in PF-M1; revisit when real-customer
security requirements arrive.

10.2.4 **Re-authentication for sensitive operations** — what
operations require it, and what is the re-auth window? Default
proposal: admin actions, audit export. Re-auth window: 15
minutes.

10.2.5 **Logout behaviour** — server-side session
invalidation + client-side cookie clearing. Confirmed.

### 10.3 RBAC

10.3.1 Where do **role definitions** live? In code? In a
configuration file? In persistence?

10.3.2 Can a user hold **multiple roles per port**? Default
proposal: yes (matches the eight workflow profiles, where some
operators legitimately hold composite roles).

10.3.3 What is the **role-permission-edit workflow**? Admin
edits via the admin surface? Configuration-only edits requiring
a deploy? Default proposal: admin surface for runtime edits;
configuration for initial bootstrap.

10.3.4 Are there **deny-permissions** (a role that explicitly
denies an action) or only allow-permissions? Default proposal:
allow-only for PF-M1; deny-permissions if a use case emerges
later.

### 10.4 Port-scope

10.4.1 Are **port-scopes per-role** or **per-user**? Default
proposal: per-user (a user has a port-scope set; a role applies
within each port-scope).

10.4.2 Can a port-scope have **per-port-role variations** for
the same user (VTSO at port A; Harbour Master at port B)?
Default proposal: yes.

10.4.3 How is **port-scope assignment changes** audited? Per A9,
structurally logged for PF-M3 ingestion; the schema of those
logs is an Implementation Plan decision.

### 10.5 Frontend

10.5.1 What is the **login page route** — `/login` (Beta 10
precedent) or a new convention?

10.5.2 What is the **authenticated-boot UX** when the user is
authenticated but has no port-scope? Display a clear "no port
access — contact your admin" state per A5. The Implementation
Plan specifies the exact copy and routing.

10.5.3 Does the **logout affordance** live in the header, the
right rail, an account menu, or somewhere else? Default
proposal: a minimal affordance in the header, adjacent to the
clock.

10.5.4 What happens to the **fixture-fed fallback** during PF-M1?
Default proposal: it remains as the sandbox / dev-mode fallback
behind a clear configuration gate, with explicit warnings when
used.

### 10.6 Persistence

10.6.1 What is the persistence substrate? Default proposal:
relational (SQLite / Postgres) for PF-M1 sandbox; the choice
locks in for PF-M2 onward.

10.6.2 What is the **migration strategy**? Inline schema
versioning? Alembic? Custom?

10.6.3 What is the **backup posture**? Sandbox can run without
backups; production must have a backup strategy by PF-M2.

### 10.7 Stakeholder modelling for Ports Victoria

10.7.1 Are **regulator** roles (Marine Safety Victoria, oversight
read-only) modelled in PF-M1 or deferred? Default proposal:
modelled in the catalogue but not exercised in PF-M1 sandbox
(no regulator test user); exercised when real-customer
requirements arrive.

10.7.2 Are **partner integrations** (e.g. systems-integrator
partners accessing the API on behalf of a customer) modelled in
PF-M1? Default proposal: deferred to PF-M7 (integration scope);
PF-M1 does not need this.

10.7.3 What is the **org-context-switch** posture for a user
who legitimately belongs to multiple organisations? Default
proposal: deferred to PF-M8; PF-M1 forbids multi-organisation
membership.

### 10.8 Governance

10.8.1 What is the **PF-M1 Phase 2 sandbox deploy** approach —
deploy to `horizon-v1-sandbox` after Phase 1 local validation,
mirroring M2? Default proposal: yes.

10.8.2 Who performs the **PF-M1 security review** at close?
Internal AMSG only, or external pen-test? Default proposal:
internal review for PF-M1; external pen-test at PF-M2 close.

10.8.3 How is **rollback** handled during PF-M1 if a slice
introduces a regression? Default proposal: git revert (same as
M2); since no production exists yet, rollback is purely
code-level.

### 10.9 Sequencing

10.9.1 What are the **PF-M1 slices**? Default proposal (subject
to Implementation Plan):
- Slice 1: Identity model + persistence
- Slice 2: Login / session / logout
- Slice 3: RBAC + permission model
- Slice 4: Port-scope filtering + authenticated boot
- Slice 5: Login UI surface on the frontend
- Slice 6: Admin surface (user / role / port-scope provisioning)
- Slice 7: Test users + sandbox bootstrap
- Slice 8: Acceptance verification + retrospective draft

10.9.2 Does the **admin surface** ship in PF-M1 or PF-M2? Default
proposal: PF-M1 (it's part of the identity / RBAC foundation); a
minimal admin surface is sufficient — fancy admin UX is PF-M2 or
later.

10.9.3 Should the **frontend login UI** ship in the same slice as
the server-side auth, or as a separate slice? Default proposal:
separate slice. Server auth first; frontend UI second.

---

## 11. Recommendation

### Recommended posture: **Approve PF-M1 scope as proposed.**

Rationale:
- PF-M1 is the unblock for every subsequent platform capability.
  Delaying it cascades the entire PR #71 §3 sequence.
- The scope is tightly bounded: identity + RBAC + port-scope, no
  more. The deny-list / stop-conditions catalogue (§8) makes
  scope creep operationally costly to attempt.
- The Melbourne / Ports Victoria stakeholder posture is built in
  from PF-M1 design (§3.3, §5.4) without forcing multi-tenant
  infrastructure now (§4.10).
- The Centre Panel Navigation, Ocean Intelligence, Kyber, Smart
  Ocean X, and Beta 10 boundaries are restated and enforced in
  §5 + §8.
- The acceptance criteria (§7 A1–A15) are concrete, verifiable,
  and tie back to the Operational Platform Workflows §6 security
  posture.
- The risk register (§9) has no high-likelihood / high-impact
  risk except 9.4 (frontend-trusted permissions), and that risk
  has a stop-condition mitigation.

### Concrete next steps under this recommendation

1. **Tony's review** of this scope proposal, with focus on:
   - §3 in-scope items — any missing? Any over-scoped?
   - §4 out-of-scope items — anything that should move into PF-M1?
   - §7 acceptance criteria — A1–A15 — accept all? Adjust any?
   - §8 deny-list / stop conditions — accept the discipline?
   - §10 open questions — any that need Tony's decision before
     Implementation Plan drafting?
2. **Optional ChatGPT engineering review** of §3 (in-scope),
   §5 (architectural constraints), and §7 (acceptance criteria).
3. **Merge** this scope proposal onto `main` once review is
   complete.
4. **Tony's explicit authorisation message** of the form
   *"Authorised: draft Horizon V1 PF-M1 Implementation Plan"*.
   Until this message arrives, no Implementation Plan drafting
   begins.
5. **PF-M1 Implementation Plan PR** drafted in the shape of the
   M2 Implementation Plan (PR #54): file scaffold, dependencies,
   slice sequencing, validation gates, rollback plan, stop
   conditions, phase sequencing, citation map.
6. **Tony's explicit Phase 0 authorisation message** of the form
   *"Authorised: begin Horizon V1 PF-M1 implementation,
   Phase 1 local only"*. Until this message arrives, no code is
   written, no branch exists.

### What this scope proposal explicitly does NOT do

- It does **not** authorise PF-M1 implementation. Implementation
  requires a separate explicit Tony authorisation per step 6
  above.
- It does **not** modify M0 / M1 / M2 / Platform Foundation
  v0.1 / Platform Foundation Phase 0 Alignment.
- It does **not** select a persistence engine, IdP, framework,
  or deployment platform. Those decisions live in the
  Implementation Plan.
- It does **not** touch Beta 10. Beta 10 Immutability Rule in
  force.
- It does **not** authorise any Railway / production change.

---

## End of PF-M1 Scope Proposal v0.1

**Status: scope proposal only. Not authorised for implementation.**

Confirmed by this document:
- This is a **scope proposal**. No implementation, no migration,
  no vendor / framework / database / deployment-platform
  selection, no deployment, no runtime change is authorised.
- M0 / M1 / M2 and all merged Platform Foundation / Alignment /
  Capability-Backlog documents are honoured and unmodified.
- The Beta 10 Immutability Rule is in force throughout. Beta 10
  is permanently isolated; no PF-M1 work touches it.
- The Horizon Independence / Architecture Reset framing is
  preserved — no Smart Ocean X dependency framing anywhere in
  this document.
- The Kyber boundary remains in force.
- The Centre Panel Navigation is centre-panel view navigation
  only — **not** port switching. PF-M1 does not introduce a
  port selector in any UI surface.
- Ocean Intelligence is integration-shaped (PF-M7), not
  foundational.
- PF-M1 scope is tightly bounded to identity + RBAC + port-
  scope. Everything else is explicitly deferred to subsequent
  Platform Foundation milestones per the §3 / §4 split.
- All concrete PF-M1 work (Implementation Plan, implementation,
  sandbox deploy, retrospective) is gated on its own scope
  proposal artefact, its own implementation plan, and Tony's
  explicit authorisation per milestone step.

**Next action:** Tony's review of §3 (in-scope), §7 (acceptance
criteria), §8 (deny-list), and §10 (open questions). Once
review is complete and Tony's authorisation message arrives,
PF-M1 Implementation Plan drafting may begin as a separate next
step.
