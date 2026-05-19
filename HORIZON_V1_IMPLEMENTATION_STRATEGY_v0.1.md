# Horizon V1 — Implementation Strategy & Engineering Roadmap (v0.1)

**Status:** Design draft — V1 planning only
**Document version:** 0.1
**Date:** 2026-05-15
**Companion documents (the V1 design quartet, all on `main`):**
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30, merged at `4c4940f`) — WHO
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31, merged at `6d09b3d`) — HOW
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` (PR #32, merged at `3e747dc`) — WHERE
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md` (PR #33, merged at `406de24`) — WHAT
**Implementation status:** None. **This is not implementation approval.**
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

This document defines **HOW Horizon V1 should actually be engineered**
and transitioned from the Beta 10 demo into a deployment-ready
operational platform. Where the V1 design quartet defined WHO / HOW /
WHERE / WHAT, this document defines the **engineering execution
strategy** — what gets built first, what is preserved, what is
rewritten, how branches and deployments are governed, how Claude Code
participates in the engineering work, and what acceptance criteria
mark V1.0 ready for customer deployment.

This document is **explicit on five non-negotiable engineering
constraints**:

1. **Beta 10 remains a protected commercial baseline**, tagged
   `phase-0-complete @ 4ad4aae`. Beta 10 is not retired by V1; it
   continues as a separate deployment posture while V1 is built in
   parallel.
2. **V1 engineering is structurally separate from Beta 10 engineering.**
   Different branches, different deploy targets, different review
   posture. A V1 commit cannot break Beta 10; a Beta 10 fix cannot
   destabilise V1.
3. **The Kyber boundary is preserved.** Horizon V1 does not depend
   on Kyber code; future integration is API/event-based with
   de-identified resource model.
4. **No direct-to-production deploys, ever.** All V1 changes flow
   through preview environments with explicit acceptance gates
   before production.
5. **Regression gate is the explicit guard against accidental
   drift.** `tests/test_beta10_regression.py` (46/46 passing,
   protecting the Beta 10 baseline at `4ad4aae`) is the
   non-negotiable line. V1 implementation will require deliberate
   updates to the gate baseline; those updates are reviewer-
   authorised, never silent.

**This document does not authorise any implementation.** It is v0.1
of an evolving strategy. The Phase 0 Beta 10 baseline remains
protected; no V1 code is written until this document and its four
companions have been reviewed and accepted by operational
stakeholders, the architecture team, and the engineering review
process.

The document also defines, in §16, **the governance rules under which
Claude Code participates in V1 engineering** — what Claude may
propose, what requires explicit human authorisation, what is never
implemented without review, and what defaults apply to branch
naming, PR scope, and merge approval.

---

## 2. Current State Assessment

Before defining a target, this section captures what currently
exists, what works, what is operationally proven, and what is
explicitly out of scope for V1 work.

### 2.1 Beta 10 Production State

| Property | Current state |
|---|---|
| Production URL | Railway: `horizon.amsgroup.com.au` |
| `phase-0-complete` tag | `4ad4aae` (immutable Beta 10 baseline) |
| `origin/main` HEAD | `406de24` (V1 design quartet merged; runtime unchanged) |
| Python runtime | 3.10.x |
| Deploy target | Railway single web process |
| Database | **`DATABASE_URL` unset in production** (deliberate) |
| Audit emission | no-op in production (`AUDIT_EMISSION_ENABLED` not configured) |
| Auth model | Shared single-user login (`HORIZON_USER` / `HORIZON_PASS` env vars) |
| Frontend | Server-rendered HTML + JavaScript in `index.html` |
| Regression gate | `tests/test_beta10_regression.py` — 46/46 passing |
| Marketing site | `horizon.ams.group` (separate host routing) |

### 2.2 What Works Today

- Real-time vessel monitoring (AISStream + MyShipTracking + QShips fallback)
- Conflict detection (berth_overlap, pilot_conflict, weather_risk,
  ukc issues) for the demo's hardcoded scenarios + live AIS-driven cases
- Recommendation generation with decision-time snapshot (Phase 0.7b)
- Decision card UI with sequencing alternatives
- What-if scenario simulator (`/api/whatif`, `/api/apply-whatif`,
  `/api/clear-whatif`)
- Port Brief PDF generation (`/api/port-brief`, `/api/send-brief`)
- Multi-port profile selection (Brisbane primary; Melbourne, Geelong,
  Darwin available)
- Audit-pipeline infrastructure (audit.py, 5 helper modules,
  hash-chained ledger, advisory-lock per-tenant insert)
- Schema migrations (Alembic, validated end-to-end through Stage D
  preview drill)
- Beta 10 regression gate (operational invariants protected)
- Six security headers on every response
- Authenticated POST endpoints with explicit `_is_authenticated()` guards

### 2.3 What Is Currently Limited

- **Single shared login** — no multi-user identity; every user is `O-1`
- **No role-based access control** — every authenticated user sees the
  same view
- **No persistent operational state** — `_PORT_PROFILE`, `_WHATIF_OVERLAY`,
  `_MST_CACHE` are in-process globals; lost on redeploy
- **Server-rendered HTML** — limited interactivity; not amenable to
  rich role-specific consoles
- **No shift / handover** concept — operational state has no temporal
  bounds
- **No structured escalation** — no defer / override / approval workflow
- **No incident / replay surface** — audit ledger exists but no review tooling
- **No notifications** — operators must actively check the dashboard
- **No multi-tenant isolation** — `ams-demo` tenant is the only tenant;
  the audit schema supports multi-tenant but operational code assumes single
- **No external integrations beyond AIS / BOM** — Kyber, QShips push,
  terminal systems all conceptual

### 2.4 What Is Explicitly Out of Scope for V1

- **Beta 10 rewrite.** Beta 10 continues as-is. Improvements to Beta 10
  are bug fixes only, not feature evolution.
- **Kyber implementation.** Kyber is a separate product. V1 may consume
  Kyber APIs but does not implement Kyber-internal concerns.
- **AI features.** V1 establishes the substrate for AI consumption
  (per Information Architecture §15) but does not ship AI features.
- **Cross-tenant features.** V1.0 ships single-tenant (per customer);
  multi-tenant federation is V2+.
- **Mobile pilot apps beyond stakeholder surface.** Operational consoles
  remain desktop-primary; native apps are out of scope for V1.0.
- **Customer-specific business logic.** V1 ships tenant-configurable
  defaults; customer-specific operational quirks are addressed via
  tenant configuration, not branched code.

---

## 3. Beta 10 Preservation Strategy

Beta 10 is not retired by V1. It is preserved as a protected commercial
baseline and continues to be useful for demos, customer presentations,
and as a reference implementation. V1 builds alongside, not on top of.

### 3.1 What Beta 10 Preservation Means Concretely

- `phase-0-complete @ 4ad4aae` remains the immutable reference tag
- `tests/test_beta10_regression.py` remains the active CI gate
- The current Railway production deployment continues to track
  `main` and serve the Beta 10 surface
- The marketing site at `horizon.ams.group` continues to operate
- Beta 10 receives **bug-fix-only** changes; no feature additions
- Any production Beta 10 change must pass the regression gate
  byte-equality assertions

### 3.2 Beta 10 vs V1 — Branch and Deploy Separation

V1 work happens on **a parallel set of branches**, with deploy
infrastructure (Railway projects, env vars, databases) separate from
Beta 10's:

| Property | Beta 10 | V1 |
|---|---|---|
| Source branches | `main` (Beta 10 only); `fix/*` for bug fixes | `v1/*` (long-lived integration branches); `feat/v1-*` for features |
| Default branch | `main` | `v1/main` (proposed) OR continued use of `main` with explicit V1 vs Beta 10 file separation |
| Production deploy | Railway: `horizon.amsgroup.com.au` from `main` | Railway: separate project / environment from `v1/main` |
| Preview deploys | Railway preview environments off branch | Railway preview environments off V1 branches |
| Database | unset (no Postgres) | Postgres provisioned per environment |
| Regression gate | `tests/test_beta10_regression.py` (46/46) | V1-specific gates ADDED, Beta 10 gate continues to pass |

The decision between **two separate Railway projects** vs **one
project with separate environments** is a V1.0 architecture question
(open question §20). The principle is independence; the implementation
is open.

### 3.3 Protected Files / Surfaces

The following are **protected** under Beta 10 preservation:

- `server.py` — the Beta 10 single-file application
- `audit.py` — the audit writer (Phase 0.5b)
- All 5 audit helpers (`session_audit.py`, `conflict_audit.py`,
  `recommendation_audit.py`, `recommendation_presented_audit.py`,
  `operator_action_audit.py`)
- `db.py`, `tenant.py` — Phase 0 foundation
- `index.html` — Beta 10 frontend
- `migrations/versions/0001_*` through `0004_*` — Phase 0 migration set
- `tests/test_beta10_regression.py` — the regression gate itself
- `port_profiles.py` — Beta 10 port definitions
- `requirements.txt` — Beta 10 runtime dependencies
- `railway.toml`, `Procfile` — Beta 10 deploy configuration
- All `deploy/*` marketing site assets

These files **MUST NOT be modified in V1 branches** unless a Beta 10
bug fix is the explicit purpose. V1 work uses **new** files,
**new** modules, **new** routes, **new** assets.

### 3.4 Beta 10 Lifecycle Beyond V1.0 Ship

Once V1.0 reaches customer deployment, Beta 10's role transitions:

- **Demos and customer trials** of pre-V1 functionality continue on
  Beta 10
- **Pre-V1 customer engagements** that started against Beta 10 are
  migrated explicitly (per-customer migration plan)
- **Beta 10 deprecation** is a separate operational decision made
  after V1 has been operationally validated; Beta 10 is not
  automatically retired

### 3.5 What "Preservation" Does NOT Mean

- It does **not** mean Beta 10 is frozen forever — security fixes,
  bug fixes, and necessary dependency upgrades continue
- It does **not** mean Beta 10's data model is the V1 data model —
  V1 has its own schema, its own RBAC tables, its own
  recommendation lifecycle representation
- It does **not** mean Beta 10's UI is preserved as the V1 UI — V1
  has its own frontend (per Screen Architecture)

The principle is **continuity for the demo / commercial commitment**,
not stagnation.

---

## 4. V1 Target Architecture

The high-level V1 architecture, derived from the design quartet.

### 4.1 V1 Target System Shape

```
                       ┌─────────────────────┐
                       │  External Sources   │
                       │  AIS / BOM / QShips │
                       │  Kyber / Terminal   │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │ Normalisation Layer │
                       │ (source attributed) │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │ Operational State   │  ◄── tenant-scoped
                       │      Model          │       per-port-scoped
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │  Decision Engine    │
                       │  (Conflicts +       │
                       │   Recommendations)  │
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │  Role-Scoped API    │  ◄── server-side filtering
                       │  Layer              │       per session role + scope
                       └──────────┬──────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
              ▼                   ▼                   ▼
       ┌────────────┐      ┌────────────┐      ┌────────────┐
       │   Web      │      │   Mobile   │      │  External  │
       │   Clients  │      │ Stakeholder│      │ Integration│
       │ (VTSO, HM, │      │            │      │  (Kyber,   │
       │  SS, Exec) │      │            │      │   Regul.)  │
       └────────────┘      └────────────┘      └────────────┘
              │                   │                   │
              └───────────────────┼───────────────────┘
                                  │ (operator actions)
                                  ▼
                       ┌─────────────────────┐
                       │ Audit Event Capture │  ◄── append-only
                       │ (Hash-chained)      │       per-tenant chain
                       └──────────┬──────────┘
                                  │
                       ┌──────────▼──────────┐
                       │ Replay & Analytics  │
                       │      Layer          │
                       └─────────────────────┘
```

### 4.2 Key Differences from Beta 10

| Concern | Beta 10 | V1 |
|---|---|---|
| Frontend | Monolithic `index.html` with embedded JS | Modern web framework (TBD per §12), role-specific routes |
| Backend | Single-file `server.py` with `BaseHTTPRequestHandler` | Structured web framework (TBD), modular services |
| Auth | Shared single login | Per-user RBAC with sessions, roles, scopes |
| State | In-process globals | Tenant-scoped Postgres operational state |
| Audit | Per-event audit emit (live + deferred event types) | Same hash-chained ledger, expanded event types |
| Database | None in production | Postgres in production (tenant-isolated) |
| Multi-tenant | Single tenant (`ams-demo`) | Multi-tenant ready (one DB per tenant initially) |
| Notifications | None | Structured push (SSE / WebSocket / external push) |

### 4.3 Architecture Decisions That Are Open

- **Web framework choice** — FastAPI, Flask, Starlette, or other?
  Phase 0 used `BaseHTTPRequestHandler`; V1 will need a structured
  framework. (§17 discusses)
- **Frontend framework choice** — React, Vue, SvelteKit, or HTMX-style
  server-driven UI? Affects build pipeline and team skill mix. (§12)
- **Notification transport** — SSE, WebSocket, external push, or
  hybrid? (Workflow §17.15, Screen Arch §12.10)
- **Database hosting** — Railway Postgres, AWS RDS, or other?
  (Phase 1.2 activation runbook addresses options)
- **Deployment platform** — Railway (current) vs Kubernetes (future)?
  (§13 discusses)

These are **deferred to V1.0 architecture review**; this document
does not pre-empt those decisions.

---

## 5. Core Architectural Primitives

V1 is built on five primitives that all engineering work must respect.

### 5.1 Primitive 1 — Tenant-Scoped Everything

Every operational record, every audit event, every config row carries
`tenant_id` and is bounded by it. Cross-tenant access is **prohibited
at the database and API layers** in V1. The schema enforces this; the
API layer enforces it; client-side requests are scoped to a tenant via
session claim.

This primitive is non-negotiable. It is the load-bearing element of
multi-tenant safety and customer data isolation.

### 5.2 Primitive 2 — Session-Carries-Identity

Every authenticated session carries:

- `user_id` (the authenticated individual)
- `tenant_id` (the customer organisation)
- `port_id` (the active port — switchable for multi-port users)
- `role` (primary role for this session)
- `session_id` (the bounded operating context)
- `shift_id` (the active shift this session participates in)

These fields appear on **every API request** as session claims and on
**every audit event payload** as actor attribution. No operational
action lacks identity attribution. This primitive is the substrate of
permission enforcement and replay reconstruction.

### 5.3 Primitive 3 — Server-Side Authorisation

All authorisation decisions happen server-side per Permission Model
§8.7. The client never trusts a flag like "should this button appear?"
— the server simply does not return the data or accept the action
that the role isn't authorised for.

The frontend reflects the server's authorisation result; it does not
make authorisation decisions of its own.

### 5.4 Primitive 4 — Append-Only Audit

The audit ledger is append-only per Phase 0.5b. No `UPDATE`, no
`DELETE`, no `TRUNCATE` of `audit.events` or `audit.payloads`. Hash
chain integrity is the verification mechanism (`verify_chain`).

This primitive constrains:
- Schema design (no nullable hash fields, no soft-delete columns)
- Data retention strategy (partition drop for retention, not row delete)
- Migration patterns (no destructive migrations on audit tables)
- Replay reliability (the chain is reconstructible)

### 5.5 Primitive 5 — Single Operational Ownership

Per Workflow Model §6: a conflict, recommendation, or escalation has
**exactly one owner** at any moment. Ownership transfers are explicit
and auditable. There is no concept of "joint ownership" or "shared
ownership" — only delegation, escalation, or handover, each producing
an audit event.

This primitive constrains:
- Database design (ownership is a structured field on operational
  records)
- API design (ownership transfers go through specific endpoints, not
  arbitrary updates)
- UI design (ownership is always visible per Screen Arch §2 / §5.10)

---

## 6. RBAC & Session Evolution Strategy

The transition from Beta 10's shared login to V1's per-user RBAC.

### 6.1 Current Beta 10 Auth Model (recap)

```
HORIZON_USER / HORIZON_PASS env vars → single shared session token →
every authenticated user is operator_handle "O-1"
```

### 6.2 V1 Target Auth Model

```
Per-user credentials (password initially, OIDC / SSO future) →
session with claims (user, tenant, port, role, scope, shift) →
every authenticated user has their own audit attribution
```

### 6.3 Migration Path (Engineering Sequence)

| Step | Engineering work | Risk |
|---|---|---|
| V1.0.1 | Add `users`, `roles`, `permissions`, `user_roles`, `user_scopes` tables (per Permission Model §9.1) via new migration `0005_v1_rbac` | Low — schema-only |
| V1.0.2 | Add user-password auth endpoint alongside existing env-var auth (both work) | Low — additive |
| V1.0.3 | Seed initial users for the `ams-demo` tenant (operator_handle preserved as `O-1` for the existing demo user; new users get fresh handles) | Low — data only |
| V1.0.4 | Switch session model to carry role + scope claims (env-var auth grants a default role for backwards compat) | Medium — touches session middleware |
| V1.0.5 | Add server-side role-filtered `/api/summary` projection (per Permission Model §8) | Medium — touches the highest-traffic endpoint |
| V1.0.6 | Add `/api/v1/auth/login` and deprecate env-var auth (Beta 10 deploys keep env-var, V1 deploys use new endpoint) | Medium — backward-compat preserved |

### 6.4 Session Lifecycle in V1

- **Login** → `SESSION_STARTED` (Phase 0.6 live) with full claim set
- **Role switch** (V1.x) → new session_id; old session ends with
  `SESSION_ENDED`
- **Port switch** → not a new session; emits `PORT_CONTEXT_CHANGED`
  (V1 candidate event)
- **Logout** → `SESSION_ENDED` (Phase 0.6 live)
- **Logout with pending items** → `SESSION_ENDED_WITHOUT_ACTION`
  (Phase 0 reserved, V1.2 emits)
- **Session timeout** → `SESSION_ENDED` with `reason='timeout'`

### 6.5 Backward Compatibility with Beta 10

For the duration of V1 development:

- Beta 10 production continues using env-var auth (HORIZON_USER /
  HORIZON_PASS) — **NOT changed**
- V1 development environments use the new user/password auth
- The two paths share the `audit` and `session_audit` modules
  (already designed to accept any actor_handle, not just `O-1`)
- When V1.0 ships, customer V1 deployments use new auth; Beta 10
  demos continue with shared login

### 6.6 Engineering Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Migration of audit ledger actor_handles | Audit ledger is append-only; old `O-1` rows remain; new rows carry per-user handles. No reconciliation needed. |
| Session-token incompatibility across deploys | Different token format prefix (e.g. `v1-` prefix); session middleware detects and dispatches |
| Permission-check overhead on every request | Permissions cached in session at login; refresh on role change only |
| Role-scoped `/api/summary` adds latency | Filter computed once per response; should be sub-ms vs the data composition itself |

---

## 7. Recommendation Lifecycle Engine Strategy

V1 extends the recommendation lifecycle from Beta 10's "detected →
generated → presented" to the full Workflow Model §7 lifecycle.

### 7.1 Current Beta 10 Lifecycle

```
detected (Phase 0.7a) → generated (Phase 0.7b, with snapshot) →
presented (Phase 0.7c, scoped to api_summary surface) → [END]
```

After "presented," Beta 10 has no operational concept of acknowledge,
resolve, defer, override, escalate, or close. These are V1 territory.

### 7.2 V1 Target Lifecycle

```
detected → generated → presented → acknowledged → resolved
                                  ↘                   ↑
                                   deferred ── revisit ─┘
                                  ↘
                                   overridden → closed
                                  ↘
                                   escalated → (chain) → closed
                                  ↘
                                   expired (DEADLINE_PASSED) → closed
```

### 7.3 Engineering Components Required

| Component | Purpose | Built in phase |
|---|---|---|
| `recommendations` table | Persist recommendation lifecycle state | V1.0 |
| `recommendation_actions` table | Append-only log of operator actions per recommendation | V1.1 |
| `action_notes` table | Free-text and structured-reason annotations | V1.1 |
| Acknowledge endpoint | `POST /api/v1/actions/acknowledge` | V1.1 |
| Resolve endpoint | `POST /api/v1/actions/resolve` | V1.1 |
| Defer endpoint | `POST /api/v1/actions/defer` | V1.2 |
| Override endpoint | `POST /api/v1/actions/override` | V1.2 |
| Escalate endpoint | `POST /api/v1/actions/escalate` | V1.3 |
| Close endpoint | `POST /api/v1/actions/close` | V1.2 |
| Deadline scanner | Background job emitting `DEADLINE_PASSED` | V1.2 |
| Defer revisit scheduler | Re-presents at `defer_until` | V1.2 |

### 7.4 Audit Event Extensions Required

Per Workflow Model §14.3 and Information Architecture §12.8:

- V1.1: `OPERATOR_ACKNOWLEDGED`
- V1.2: `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION`, `RECOMMENDATION_CLOSED`,
  `OPERATOR_SIGNED_OFF`
- V1.3: `OPERATOR_ESCALATED`/`ESCALATION_CREATED`,
  `ESCALATION_RESOLVED`

Each new event type requires:
- Alembic migration extending the closed-set CHECK constraint on
  `audit.events.event_type`
- Reviewer-authorised update to `tests/test_beta10_regression.py`
  authorised event-type list (the regression gate currently locks
  the 6 currently-emitted events; each addition is a deliberate
  baseline change)
- New helper module OR extension of existing helpers
- New tests for production-safe no-op behaviour, payload shape,
  dedup, lifecycle isolation (mirroring Phase 0.7a/b/c/0.8a
  patterns)

### 7.5 Engineering Risks

| Risk | Mitigation |
|---|---|
| Recommendation state divergence between in-memory and persisted | Single source of truth: persisted; in-memory is a cache |
| Deadline scanner spawning unbounded background load | Bounded to per-tenant, per-shift scope; rate-limited |
| Reason code catalogue drift across tenants | Closed catalogue per tenant, configured via admin; changes audited |
| Override of an already-deferred recommendation | Lifecycle state machine enforces valid transitions; invalid attempts return 409 |

---

## 8. Operational State & Ownership Strategy

V1 introduces persistent operational state (vs Beta 10's in-process
globals) and the single-ownership model from Workflow §6.

### 8.1 What State Becomes Persistent in V1

| State | Beta 10 | V1 |
|---|---|---|
| Active port profile | `_PORT_PROFILE` global | `sessions.active_port_id` |
| What-if overlay | `_WHATIF_OVERLAY` global | `whatif_overlays` table per tenant per port |
| MST cache | `_mst_cache` global | Redis or per-process LRU (transient; doesn't need DB) |
| Active shift | n/a | `shifts` table per tenant |
| Pending recommendations | recomputed each `/api/summary` | `recommendations` table; lifecycle persisted |
| Active conflicts | recomputed each poll | recomputed; persisted only via audit events |
| Ownership state | n/a | `recommendation_ownership` field or join table |

### 8.2 Ownership Schema (conceptual)

A recommendation's ownership lifecycle:

```
recommendations table:
  id, tenant_id, conflict_id, recommended_option_id,
  state ('presented' | 'acknowledged' | 'resolved' | 'deferred' |
         'overridden' | 'escalated' | 'closed'),
  owner_user_id, owner_role, owner_assigned_at,
  decision_deadline, ...

ownership_transfers table (audit-linked):
  recommendation_id, from_user_id, to_user_id, transfer_reason,
  transferred_at, transfer_event_id
```

### 8.3 Engineering Sequence

| Phase | Work |
|---|---|
| V1.0 | `shifts`, `sessions`, `whatif_overlays` persisted; in-process globals deprecated |
| V1.1 | `recommendations` table; ownership-at-presentation auto-assignment |
| V1.2 | Ownership-transfer on defer/override |
| V1.3 | Ownership-transfer on escalation; handover-time bulk transfer |

### 8.4 State Migration from Beta 10

There is **no data migration** from Beta 10 to V1. Beta 10 stores no
persistent operational state; V1 starts fresh. Customer V1 deployments
begin with empty state and accumulate from first session.

### 8.5 Race Condition Considerations

The single-ownership invariant requires careful concurrency design:

- Two VTSOs simultaneously acknowledging the same recommendation:
  the first-arriving acknowledgement wins; the second sees a 409
  with the current owner identified
- Ownership transfers go through a single endpoint with a SELECT FOR
  UPDATE on the recommendation row to prevent races
- Handover-time bulk transfer happens within a single transaction
  with shift lock

Phase 0's per-tenant advisory lock pattern (Phase 0.5b) is reusable
for these.

---

## 9. Replay & Audit Evolution Strategy

The audit ledger is the operational ground truth (per Information
Arch §1). V1 extends it with V1 event types and a structured replay
surface.

### 9.1 Audit Ledger — What Stays

- Phase 0.5a schema (partitioned tables, monthly partitions)
- Phase 0.5b writer (advisory-lock, canonical-JSON hash, per-tenant
  genesis)
- Phase 0.5c performance baseline (≥100 events/sec/tenant)
- `verify_chain` integrity check
- The hash chain itself is **NEVER reset, broken, or migrated** —
  V1 events append to the same per-tenant chain

### 9.2 Audit Ledger — What Extends

- Closed-set extensions for V1 event types (per §7.4, §14.3 candidates)
- Payload-level additions: `user_id`, `role`, `scope`, `session_id`
  on every event (no schema change required; lives in jsonb payload)
- New helper modules for new event types (mirroring Phase 0 patterns)

### 9.3 Replay Surface (V1.6 deliverable)

The replay surface (Screen Architecture §11) needs:

| Component | Purpose |
|---|---|
| Replay query API | `GET /api/v1/audit?window=<>&filter=<>&scope=<>` |
| Replay timeline component | Frontend rendering of audit events on a scrubbable timeline |
| Replay decision-snapshot viewer | Render the §1.4.1 snapshot embedded in `RECOMMENDATION_GENERATED` |
| Replay chain visualisation | Render escalation chains, ownership transfers |
| Incident package exporter | `POST /api/v1/audit/export-incident` returns JSON + PDF + hash proof |
| `AUDIT_READ` emission | Every review surface emits this meta-event |

### 9.4 Stage E-prod Activation Coupling

The persistent audit activation (Stage E-prod, currently paused) and
V1 are coupled but **not strictly sequential**:

- **Option A: Stage E-prod first, V1 audit-shaping later.** Activate
  persistent audit on Beta 10 production, accept that Beta 10
  actor_handles are `O-1`-shaped, and later V1 events have richer
  attribution. The audit ledger contains a mix of Beta-10-style and
  V1-style rows.
- **Option B: V1 first, Stage E-prod with V1 actor model.** Wait until
  V1 ships per-user identity, then activate persistent audit so the
  ledger opens cleanly with V1 attribution. Beta 10 audit emission
  remains no-op throughout.
- **Recommendation (consistent across all four quartet docs):**
  Option B, unless commercial considerations demand Option A.

This is an explicit operational decision; both paths are documented
and defensible.

### 9.5 Replay Performance Targets

- Shift-scope replay (8-12h, single tenant): < 5 seconds load
- Multi-day replay (7d): < 30 seconds; progress indicator acceptable
- Multi-month replay (30d): < 2 minutes; chunked rendering acceptable
- All replays bounded by tenant; no cross-tenant queries

The Phase 0.5c benchmark (≥100 events/sec/tenant) is the *write*
side; *read* performance for replay is a V1.6 engineering deliverable.

---

## 10. API & Service Boundary Strategy

V1 evolves from Beta 10's monolithic `server.py` to a structured API
surface. This section defines the API boundary strategy.

### 10.1 Beta 10 API (what exists)

```
GET  /api/summary
POST /api/whatif | /api/apply-whatif | /api/clear-whatif
POST /api/set_port | /api/send-brief
GET  /api/health-data | /api/aisstream-status | /api/mst-status
GET  /api/port-brief | /api/brief-config
POST /login | /logout
GET  /logo | /amsg-logo | /favicon.ico | /apple-touch-icon.png
GET  /health
GET  /, /index.html
```

All served by `BaseHTTPRequestHandler` in `server.py`.

### 10.2 V1 API Target Shape

Versioned, structured, role-scoped:

```
/api/v1/
   auth/login | auth/logout
   summary  (role-scoped)
   actions/acknowledge | actions/resolve | actions/defer |
   actions/override | actions/escalate | actions/close
   shifts | shifts/handover | shifts/note
   audit  (read; role-scoped)
   audit/export-incident
   briefs/current | briefs/{id}/signoff | briefs/{id}/distribute
   berths/availability | berths/{id}/unavailable |
   berths/{id}/schedule-maintenance | berths/{id}/ready
   stakeholders/assignments | stakeholders/confirm |
   stakeholders/report-delay | stakeholders/report-issue
   admin/users | admin/roles | admin/permissions |
   admin/reason-codes
```

(Per Permission Model §8 and Screen Architecture §15.)

### 10.3 Service Boundary Strategy

V1.0 likely ships as a **modular monolith** (single deployable
binary, internally structured by domain). Microservices are explicitly
**not** required for V1.0:

| Module (logical) | Responsibility |
|---|---|
| `auth` | session lifecycle, role-claim issuance |
| `tenant` | tenant resolution and scope enforcement |
| `operational_state` | normalised vessel/berth/movement state |
| `decision_engine` | conflict detection, recommendation generation |
| `lifecycle` | recommendation lifecycle state transitions |
| `shift` | shift open/close, handover management |
| `escalation` | escalation chain management |
| `audit` | the existing Phase 0 module + V1 helpers |
| `analytics` | derived metric computation |
| `replay` | audit query + incident package |
| `notification` | push routing (transport-dependent) |
| `integration` | external source adapters (AIS, BOM, Kyber, ...) |

A microservices split is a V1.x+ decision based on scaling needs;
V1.0 prioritises shipping over premature distribution.

### 10.4 API Versioning Strategy

- V1 endpoints live under `/api/v1/`
- Future versions (V2+) live under `/api/v2/`
- Beta 10 endpoints (`/api/*` without version prefix) **are NOT
  altered or replicated under `/api/v1/`** — they remain as Beta 10
  surface
- V1 deploys serve **only** `/api/v1/` endpoints (no `/api/*` legacy)
- Beta 10 deploys serve **only** `/api/*` (no `/api/v1/`)
- This avoids version-mixing in a single deployment

### 10.5 OpenAPI Specification

V1.0 should ship with an OpenAPI 3 specification published as part of
the deploy artefact. This enables:

- Auto-generated client libraries for tenant integration
- Auto-generated docs at `/api/v1/docs`
- Contract testing in CI
- Easier Kyber and partner integration

### 10.6 API Authentication and Authorisation

- All `/api/v1/` endpoints require an authenticated session (per §6)
- The auth middleware extracts session claims and attaches them to
  the request context
- Each endpoint's permission requirement is declared in code; the
  middleware compares declared requirement to session claim
- Permission denials return 403 (not 401 — auth was valid, scope was
  insufficient)
- Unauthenticated requests return 401 (or redirect to `/login` for
  HTML routes)

---

## 11. Integration Strategy

External integrations evolve from Beta 10's polled HTTP/WebSocket
pulls to a structured integration model per Information Arch §11.

### 11.1 Beta 10 Integrations (current)

- **AISStream** — WebSocket, pulled in `aisstream_scraper.py`
- **MST** — HTTP API, polled in `mst_scraper.py`
- **QShips** — JSON feed, polled in `qships_scraper.py`
- **BOM tides** — HTTP API, polled in `bom_tides.py`
- **BOM weather** — HTTP API, polled in `weather.py`
- **Vessel scraper** — HTTP API, polled in `vessel_scraper.py`

### 11.2 V1 Integration Model

V1 integrations follow a consistent pattern:

```
External Source (HTTP / WebSocket / event stream)
    │
    ▼
Adapter Module (translates source-specific → canonical)
    │
    ▼
Source Confidence Tagging (adds source attribution, confidence,
                          freshness)
    │
    ▼
Cache Layer (per-source, configurable TTL)
    │
    ▼
Normalisation Bus (canonical records reach the operational state model)
    │
    ▼
Operational State (consumed by decision engine, role-scoped API)
```

### 11.3 New V1 Integrations

| Integration | V1 phase | Purpose |
|---|---|---|
| Kyber (API/event) | V1.5+ | Pilot assignment context (downstream consumer of operational events; upstream provider of assignment events) |
| Terminal systems | V1.x | Cargo readiness, crane scheduling, terminal access (bidirectional) |
| Towage systems | V1.x | Tug roster, availability (when towage providers have systems) |
| Regulator read-access | V1.x (deferred) | Read-only audit ledger access for regulatory review |
| AI/ML services | V1.x+ (deferred) | Substrate ready; consumption rules per Information Arch §15 |

### 11.4 Kyber Boundary Engineering

Per Permission Model §11, Workflow Model §13.6, Screen Architecture §9.8,
Information Architecture §11.6:

- **No source dependency** on Kyber code in Horizon V1
- **API/event-based integration** when warranted (V1.5+)
- **De-identified resource model** — Horizon does not store named
  pilot identities; capability descriptors only
- **Federated authentication** — Kyber and Horizon manage their own
  sessions; no shared auth
- **Bidirectional events:**
  - Kyber → Horizon: assignment created / pilot accepted / pilot
    on board / pilot off
  - Horizon → Kyber: conflict detected affecting an assignment /
    weather hold / schedule change

The Kyber integration is **opt-in** per tenant; tenants without Kyber
use operator-entered pilotage assignment (V1.0 default).

### 11.5 Integration Failure Handling

Per Information Arch §16:

- Source stale beyond threshold → fallback to next source path
- Source unreachable → fallback + user-surfaced warning
- Source data conflict → primary wins; conflict logged and surfaced
- Source recovers → emit recovery event; resume normal flow

All source failures and recoveries emit audit events
(`DATA_SOURCE_STALE_DETECTED`, `DATA_SOURCE_RESTORED` candidates per
Information Arch §16.8).

---

## 12. Frontend Evolution Strategy

V1 introduces structured role-specific frontends per Screen
Architecture. This section defines the engineering transition from
Beta 10's `index.html` to V1's role-scoped consoles.

### 12.1 Current Beta 10 Frontend

- Single `index.html` (~3MB+ rendered) with embedded JavaScript
- Server-rendered shell + client-side polling of `/api/summary`
- Per-port profile rendering driven by JS
- Monolithic; one frontend serves all roles (since Beta 10 has one
  effective role)

### 12.2 V1 Frontend Architecture

Multiple role-specific frontends sharing a common framework:

- **VTSO console** (desktop primary; per Screen Arch §5)
- **Shift Supervisor console** (desktop; §6)
- **Harbour Master console** (desktop; §7)
- **Executive dashboard** (mobile + desktop; §8)
- **Stakeholder mobile** (mobile primary; §9)
- **Admin/configuration** (desktop only; §15.7)
- **Replay & Incident Review** (desktop only; §11)

Each is served from the same V1 backend with role-routing at login.

### 12.3 Frontend Framework Decision (deferred)

Open question §20. Candidates:

| Framework | Pros | Cons |
|---|---|---|
| **HTMX + Server-rendered** | Closest to Beta 10 style; minimal client complexity; no build pipeline | Limited richness for VTSO operational console; harder to do complex interactions |
| **React** | Mature ecosystem; rich UX; strong typing with TypeScript | Larger team skill requirement; build pipeline complexity |
| **Vue** | Smaller than React; reasonable UX; good for ops dashboards | Smaller ecosystem; team familiarity |
| **SvelteKit** | Compile-time; smaller runtime; good for ops UI | Smaller community; less mature than React |

**Recommendation pending architecture review.** The frontend framework
decision affects team hiring, build pipeline, and deployment shape.
This is a V1.0 architecture-team decision.

### 12.4 Frontend Engineering Sequence

| Phase | Work |
|---|---|
| V1.0 | Frontend shell + login + VTSO console foundational layout (no actions yet) |
| V1.1 | VTSO action surfaces (acknowledge, resolve, escalate, what-if) |
| V1.2 | HM console + authority actions |
| V1.3 | SS console + handover surfaces |
| V1.4 | Executive dashboard |
| V1.5 | Stakeholder mobile app (separate codebase or PWA, TBD) |
| V1.6 | Replay & Incident Review surface |

### 12.5 Frontend / Backend Contract

The role-scoped `/api/summary` response is the **single contract** the
frontend depends on. Any backend change that alters the response
shape requires:

- Versioning of the response schema (`schema_version` field)
- Frontend migration in lockstep
- Backward-compatible window for in-flight deploys (one-version-back
  support)

### 12.6 Beta 10 Frontend Coexistence

Beta 10's `index.html` is **unchanged** by V1 frontend work. V1
frontends are served from V1 deploys; Beta 10 continues serving
`index.html` from Beta 10 deploys.

---

## 13. Deployment & Environment Strategy

V1 deployment evolves to a multi-environment, multi-tenant-ready
posture.

### 13.1 Beta 10 Deployment (current)

```
Railway project "horizon-prod"
   └── Service: horizon (Python + Procfile)
        └── env: HORIZON_USER, HORIZON_PASS, TOKEN_SECRET, ...
        └── DATABASE_URL: NOT SET
        └── AUDIT_EMISSION_ENABLED: not configured
        └── Branch tracked: main
```

Marketing site: `horizon.ams.group` from `deploy/` directory.

### 13.2 V1 Deployment Target Topology

```
Railway project "horizon-v1-prod" (NEW, separate from Beta 10)
   └── Service: horizon-v1 (Python + framework)
        └── Postgres add-on (V1 production data)
        └── env: per-tenant secrets, V1-specific
        └── DATABASE_URL: SET (V1 production)
        └── AUDIT_EMISSION_ENABLED: set per V1 activation plan
        └── Branch tracked: v1/main

Railway project "horizon-v1-preview" (NEW)
   └── Service: preview deployments for v1/* branches
        └── Postgres add-on (preview)
        └── Same env shape as prod (with preview secrets)
        └── For pre-merge validation

Railway project "horizon-prod" (EXISTING — Beta 10)
   └── UNCHANGED
```

Customer-specific V1 deployments may have their own projects:
`horizon-v1-customer-<name>` per the multi-tenant strategy (§14).

### 13.3 Branch-to-Environment Mapping

| Branch | Deploy target |
|---|---|
| `main` | Beta 10 production (`horizon-prod`) |
| `v1/main` | V1 production (`horizon-v1-prod`) |
| `feat/v1-*`, `fix/v1-*` | V1 preview environments |
| `chore/preview-*` (Phase 0 DO-NOT-MERGE) | Existing preview environments only; never merge |

### 13.4 Deployment Promotion Path

```
feat/v1-<feature>     →  v1-preview deploy  →  reviewer accepts
                                                       │
                                                       ▼
                                            merge to v1/main
                                                       │
                                                       ▼
                                            v1-prod deploy (automatic on merge)
                                                       │
                                                       ▼
                                            Stage-style acceptance checks
                                                       │
                                                       ▼
                                            sign-off recorded in runbook
```

### 13.5 Promotion Gates

Each V1 deploy promotion requires:

- Regression gate: `tests/test_beta10_regression.py` + V1-specific gate
  passing
- Smoke test on preview environment
- Reviewer approval (no auto-merge to v1/main)
- Stage-style acceptance per V1 activation runbook (TBD per phase)

### 13.6 Rollback Strategy

V1 rollback is identical in pattern to Phase 1.2's audit activation:

- **Environment-variable flip** for feature-flag-controlled changes
  (preferred)
- **Deploy revert** via Railway's deployment history (PR revert merge
  to v1/main)
- **Database rollback** via Alembic downgrade for schema changes (rare;
  prefer feature-flagged forward-only migrations)

### 13.7 Production-Safety Rules

- **No direct-to-production deploys, ever.** All V1 changes go through
  preview environments first.
- **No silent env-var changes** in production; every change is
  authorised in a runbook entry.
- **Beta 10 production is not the V1 testing ground.** Beta 10
  receives Beta-10-fix PRs only; V1 work never lands on the
  `horizon-prod` project.

---

## 14. Multi-Tenant & Customer Isolation Strategy

V1 ships multi-tenant-ready but initially single-tenant per
deployment.

### 14.1 V1.0 Posture: One Tenant Per Deployment

Each customer gets:

- Their own Railway project (or AWS account, when migrated)
- Their own Postgres database
- Their own configuration (users, roles, permissions, port profiles,
  reason codes)
- Their own audit ledger (no shared chains)

The `tenant_id` column on every record enforces isolation **within**
a database; the per-deployment topology enforces isolation **between**
customers.

### 14.2 Future Multi-Tenant in Single Deployment (V2+)

When economics or operational reasons warrant, multiple tenants may
share a deployment:

- Tenant-aware queries on every SELECT
- Row-level security (Postgres RLS) as a backup defence
- Per-tenant resource limits (connection pools, request rate)
- Per-tenant audit chain (already supported by Phase 0.5b schema)

This is **explicitly V2+**; V1 does not implement.

### 14.3 Customer Onboarding Pattern (V1.0)

```
1. Sales contract signed
2. Engineering: provision Railway project "horizon-v1-customer-<name>"
3. Engineering: deploy v1/main; run migrations
4. Engineering: seed customer tenant (config.tenant row, port profiles,
   initial admin user)
5. Customer: receive admin login
6. Customer: configure users, roles, scopes via admin UI
7. Customer: connect external sources (AIS keys, BOM stations, Kyber
   integration if applicable)
8. Acceptance testing on customer's preview env
9. Production cutover
```

### 14.4 Demo Tenant Posture

The `ams-demo` tenant (Beta 10) does **NOT** migrate to V1. It remains
on Beta 10. V1 deployments may have their own `<customer>-demo`
tenant or seed data for demonstrations.

### 14.5 Per-Customer Configuration

Per Information Architecture §17 (admin configuration):

- Reason code catalogues (escalation, defer, override) are
  per-tenant
- Port profiles are per-tenant
- User accounts and role assignments are per-tenant
- Feature flags are per-tenant (e.g. `enable_kyber_integration`,
  `enable_executive_dashboard`)
- Notification routing preferences are per-tenant (and per-role
  within a tenant)

### 14.6 Cross-Tenant Boundary Enforcement

- Database: `tenant_id` on every operational and audit table; queries
  always filter by it
- API: session carries `tenant_id`; middleware rejects mismatches
- Audit: per-tenant chain; cross-tenant chain inspection prohibited
- UI: tenant context permanently visible (per Screen Arch §4.3)

---

## 15. Infrastructure & Observability Strategy

V1 production needs operational observability that Beta 10 doesn't
have today.

### 15.1 Current Beta 10 Observability

- Railway deployment logs (rotated; ~7d retention on Hobby tier)
- Manual `/api/health-data` checks
- No application metrics
- No distributed tracing
- No external log aggregation

### 15.2 V1 Target Observability Stack

| Concern | Solution candidate |
|---|---|
| Application logs | Structured logging (JSON) to stdout; aggregated by Logflare / Datadog / similar |
| Application metrics | Prometheus-format metrics endpoint (`/metrics`); scraped by Grafana / similar |
| Distributed tracing | OpenTelemetry SDK; sent to Honeycomb / Datadog / Tempo |
| Database metrics | Railway's built-in Postgres metrics + pg_stat_statements |
| Audit chain integrity monitoring | Hourly cron job running `verify_chain`; alert on failure |
| Error tracking | Sentry or equivalent |
| Uptime monitoring | External service (UptimeRobot / Pingdom / Datadog Synthetics) |

### 15.3 Observability Build Sequence

| Phase | Deliverable |
|---|---|
| V1.0 | Structured logging; basic health endpoint; error tracking integration |
| V1.1 | Metrics endpoint; basic dashboard |
| V1.2 | Audit chain integrity cron |
| V1.3 | Distributed tracing |
| V1.4 | Log aggregation with searchable interface |
| V1.5 | Synthetic monitoring + alerting |
| V1.x | SLO-based alerting; on-call rotation; runbook automation |

### 15.4 What Observability Costs

Each tool above has a recurring cost. The aggregate is a real V1
operational expense. Cost estimates should be part of the V1.0
architecture review.

### 15.5 Self-Hosted vs SaaS Trade-offs

| Concern | Self-hosted | SaaS |
|---|---|---|
| Recurring cost | Lower | Higher |
| Operational burden | Higher | Lower |
| Sensitive data exposure | Lower | Depends |
| Time to value | Slower | Faster |

For V1.0, **prefer SaaS** unless cost or data-sensitivity requirements
push toward self-hosted. The team should be building Horizon, not
managing observability infrastructure.

### 15.6 Infrastructure Future: Beyond Railway

Railway is appropriate for V1.0 demo and early-customer deployments.
Larger customer engagements may require:

- AWS / GCP / Azure (customer's preferred cloud)
- Kubernetes for multi-service orchestration (if microservices emerge
  in V2+)
- Customer-tenanted VPCs for network isolation
- Customer-managed Postgres (RDS / Cloud SQL / Azure DB)

The infrastructure-to-customer mapping is a V1.x+ commercial decision.
V1.0 ships on Railway.

---

## 16. Claude Code Governance & Engineering Rules

This section codifies how **Claude Code participates in V1
engineering** under the same governance pattern that protected Phase 0
and Phase 1.

### 16.1 Default Posture

Claude Code operates under **explicit-authorisation-required**
governance:

- Claude does NOT initiate code changes without an explicit
  authorisation from the operator
- Claude proposes plans before implementing
- Every implementation step has an explicit scope statement
- Claude reports back with diff, tests, and validation; the operator
  decides on merge

### 16.2 What Claude Code May Propose Without Authorisation

- Analysis of existing code (read-only)
- Plans for proposed changes (markdown documents, no code)
- Identification of inconsistencies between documents
- Suggestions for architectural improvements
- Test diagnostics
- Validation runs against existing code

### 16.3 What Requires Explicit Authorisation

- **Any file creation or modification**
- **Any branch creation**
- **Any commit**
- **Any push to remote**
- **Any PR creation**
- **Any merge**
- **Any database write**
- **Any environment variable change**
- **Any third-party API call that mutates state**

### 16.4 What Is Never Implemented Without Review

- **Production deploys** — always through preview + acceptance gates
- **Schema migrations affecting audit tables** — append-only invariants
- **Changes to `tests/test_beta10_regression.py`** — baseline updates
  are reviewer-authorised
- **Changes to `audit.py`** — Phase 0.5b is locked
- **Changes to the 5 audit helper modules** — Phase 0.7a/b/c, 0.8a
  baseline locked
- **Changes to `server.py`** during V1 — Beta 10 server is protected
- **Changes to Kyber-boundary contract** — separate document review
- **Activation of `AUDIT_EMISSION_ENABLED` in production** — Stage
  E-prod is a separate operational decision

### 16.5 Branch Naming Conventions

| Prefix | Use |
|---|---|
| `docs/` | Documentation-only PRs (the V1 design quartet, runbooks, decision notes) |
| `feat/v1-` | V1 feature implementation |
| `fix/v1-` | V1 bug fixes |
| `chore/v1-` | V1 chore / tooling work |
| `chore/preview-` | Phase-0-style preview-only DO-NOT-MERGE deploy targets (e.g. PRs #27, #28, #29) |
| `fix/beta-10-` | Beta 10 bug fixes (target Beta 10's branch) |
| `chore/beta-10-` | Beta 10 chore work |

### 16.6 PR Scope Rules

- **One PR = one logical change.** Mixed PRs (feature + refactor + fix)
  are discouraged and likely to be requested to split.
- **PR title is informative.** Read-only viewer of the PR list should
  understand what each PR does without opening it.
- **PR description includes:**
  - Summary
  - Scope (what's included and what's not)
  - Diff stat
  - Test results
  - Production-safety verification (the Phase 0 invariants)
  - Test plan checklist
- **DO-NOT-MERGE PRs** are clearly flagged in title + drafted status
  + body
- **Linked design documents** referenced where relevant

### 16.7 Merge Approval Rules

- **No self-merges.** All merges require an explicit reviewer
  authorisation (Tony's go-ahead in practice).
- **No auto-merge enabled** on any V1 branch.
- **No force-pushes to `main` or `v1/main`** (or any shared branch).
- **No bypass of CI gates.** If the regression gate fails, the PR
  doesn't merge until the failure is understood and addressed.

### 16.8 Stage E-prod Treatment

Stage E-prod (production audit activation) remains **explicitly
paused** until:

1. V1 RBAC is in place (so per-event payload carries
   user_id/role/scope/session_id)
2. Operational stakeholders explicitly authorise the activation
3. The activation runbook (`phase1-audit-activation-runbook.md`) is
   followed gate-by-gate

Until then, V1 may be developed without Stage E-prod being active.

### 16.9 Test Discipline

- **No new features without tests.** Every new endpoint, every new
  helper module, every new state machine transition has test
  coverage.
- **Regression-gate updates are deliberate.** When V1 work requires
  the Beta 10 regression gate baseline to change (new endpoint
  added, new auth posture, etc.), the gate change is reviewer-
  authorised in its own PR or with explicit gate-update justification
  in the PR body.
- **Production-safe no-op tests are mandatory** for audit helpers, per
  Phase 0 pattern.

### 16.10 Documentation Discipline

- **Design documents precede implementation.** The V1 quartet
  precedes V1 code. Future V1 evolution (V2, AI features, etc.) gets
  its own design documents first.
- **Decision notes capture deferrals.** Each deliberate scope
  reduction or deferral is documented (Phase 0's
  `phase0-008b-deadline-passed-deferral.md` is the model).
- **Runbooks capture operational procedures.** Phase 1's
  `phase1-audit-activation-runbook.md` is the model.

### 16.11 What Triggers Re-authorisation

If a planned implementation reveals a precondition gap (the alembic
in requirements.txt example from Stage C-preview), Claude **stops and
proposes the precondition fix** rather than silently expanding scope.

---

## 17. Technical Debt & Rewrite Decisions

V1 is an opportunity to address specific Beta 10 design choices that
have proven limiting. This section identifies them and recommends
rewrite vs preservation.

### 17.1 `server.py` Monolith

- **Beta 10 status:** ~4500-line single file with `BaseHTTPRequestHandler`
- **V1 decision:** **REWRITE.** V1 uses a structured web framework
  (TBD per §4.3) with modular services per §10.3
- **Rationale:** the monolith was right for Beta 10's demo scope; V1
  needs maintainability across multiple developers and features

### 17.2 In-Process Globals (`_PORT_PROFILE`, `_WHATIF_OVERLAY`, `_mst_cache`)

- **Beta 10 status:** module-level globals for operational state
- **V1 decision:** **REWRITE.** V1 persists operational state in
  Postgres per §8
- **Rationale:** globals don't survive redeploys, can't multi-instance,
  can't audit changes

### 17.3 Single Shared Auth (`HORIZON_USER` / `HORIZON_PASS`)

- **Beta 10 status:** env-var-driven shared session token
- **V1 decision:** **REWRITE.** V1 has per-user RBAC per §6
- **Rationale:** Beta 10's auth model can't support multiple
  operators with different roles

### 17.4 Server-Rendered `index.html`

- **Beta 10 status:** large monolithic HTML with embedded JS
- **V1 decision:** **REWRITE.** V1 has structured frontends per
  Screen Architecture and §12
- **Rationale:** role-specific consoles can't be served from a single
  monolithic frontend

### 17.5 Hardcoded Demo Conflicts (B03, B04)

- **Beta 10 status:** specific conflict scenarios hardcoded in
  detection logic (`b03_alternatives`, `b04_alternatives`)
- **V1 decision:** **REPLACE WITH GENERAL DETECTION.** V1's conflict
  engine works on real port data; demo scenarios are seeded data
  rather than hardcoded logic
- **Rationale:** hardcoded scenarios are demo-specific and don't
  generalise to customer ports

### 17.6 Audit Module (`audit.py` + 5 helpers)

- **Beta 10 status:** Phase 0.5b writer, Phase 0.7a/b/c helpers,
  Phase 0.8a operator-action helper
- **V1 decision:** **PRESERVE AND EXTEND.** The audit foundation is
  excellent and shouldn't be rewritten. V1 adds new helpers and new
  event types via the same pattern
- **Rationale:** Phase 0 audit infrastructure is production-tested,
  performance-validated, and integrity-verified. Rewriting it would
  be expensive and risky for no gain

### 17.7 Phase 0 Migration Set (`0001`-`0004`)

- **Beta 10 status:** initial migrations for config schema, audit
  schema, tenant seed
- **V1 decision:** **PRESERVE.** V1 adds new migrations (`0005+`) for
  RBAC, recommendations, shifts, etc., but does not modify Phase 0
  migrations
- **Rationale:** stable foundation; any modification would require
  preview-environment migration replay

### 17.8 Beta 10 Regression Gate (`tests/test_beta10_regression.py`)

- **Beta 10 status:** locks the Beta 10 baseline at `phase-0-complete`
- **V1 decision:** **PRESERVE AS GATE; UPDATE BASELINE DELIBERATELY
  WHEN V1 SHIPS.** Until V1.0 ships, the gate locks the Beta 10
  invariants. When V1.0 reaches its own production posture, the
  V1.0-equivalent regression gate is added alongside (NOT replacing)
- **Rationale:** the gate has prevented multiple subtle regressions
  during Phase 1 and remains valuable for Beta 10 protection

### 17.9 Beta 10 Logos and Marketing Site

- **Beta 10 status:** `logo.png`, `logo.svg`, `amsg-logo.png`,
  `mobile-icon.png`, `deploy/` marketing assets
- **V1 decision:** **PRESERVE.** V1 may have its own visual identity
  (V1.x design decision); marketing site continues at
  `horizon.ams.group`
- **Rationale:** commercial commitment; brand continuity

### 17.10 Phase 1 Hardening Items

- **Status:** Phase 1.1 (what-if POST auth hardening) shipped;
  `phase1-whatif-auth-hardening.md` documents the rationale
- **V1 decision:** **PRESERVE.** V1's authorisation model
  (server-side, per-permission, deny-by-default) generalises this
  pattern to all endpoints
- **Rationale:** consistency with V1's overall RBAC posture

---

## 18. Build Sequencing & Milestones

V1 builds in seven phases, V1.0 through V1.6, with explicit milestones.

### 18.1 V1.0 — Foundation Milestone

**Deliverables:**
- New web framework (TBD) replacing `server.py` for V1 endpoints
- `auth` module: user/password login, per-user sessions, role + scope
  claims
- `tenant` module: tenant resolution, scope enforcement
- `operational_state` module: persisted in Postgres (V1 schema
  migrations `0005+`)
- RBAC schema: users, roles, permissions, user_roles, user_scopes
- Role-scoped `/api/v1/summary` endpoint (read-only initially)
- Basic VTSO console (foundational layout; no actions yet)
- Frontend shell with left-rail navigation (role-filtered)
- Login flow (separate from Beta 10 env-var auth)
- V1.0 deploy target on Railway (separate project)
- V1.0-specific regression gate (alongside Beta 10 gate)

**Acceptance criteria:**
- AMS demo tenant V1.0 deploy is operational on a preview environment
- VTSO login works; VTSO sees role-scoped data; HM login works (sees
  the same data, more actions enabled in V1.2)
- Beta 10 regression gate still passes
- All audit emission still no-op in production-equivalent preview
  posture (V1.0 emits with V1 user_id/role/scope/session_id payload
  shape, but Stage E-prod activation remains paused)

### 18.2 V1.1 — VTSO Action Surface Milestone

**Deliverables:**
- Acknowledge action endpoint + UI
- Resolve conflict action + UI
- Run / preview what-if (preserves Beta 10 functionality, V1-shaped)
- Escalate action (creation only; SS receipt in V1.3)
- Add notes to actions
- `OPERATOR_ACKNOWLEDGED` audit event emission
- Conflict resolution audit event emission
- Reason code catalogue (V1.0 baseline + admin-editable)

**Acceptance:**
- VTSO can resolve a conflict end-to-end
- Audit trail captures all VTSO actions with reason codes
- Performance: action-to-audit-row latency < 500ms p95

### 18.3 V1.2 — Harbour Master Authority Milestone

**Deliverables:**
- HM authority console
- Override action with mandatory reason code
- Defer action with `defer_until`
- Port closure approval workflow
- Sign-off Port Brief workflow
- Approve what-if for live application
- `OPERATOR_OVERRODE`, `OPERATOR_DEFERRED`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION` audit emission (Phase 0 reserved
  becomes live)
- `OPERATOR_SIGNED_OFF` audit event
- Deadline scanner background job
- Defer revisit scheduler

**Acceptance:**
- HM can approve port closure end-to-end
- HM can override a recommendation with reason
- Deadline misses surface as `DEADLINE_PASSED` events
- All HM actions auditable; replay reconstructs decisions

### 18.4 V1.3 — Shift Supervisor & Handover Milestone

**Deliverables:**
- SS console
- Handover composer
- Handover acceptance flow
- Escalation queue (inbound)
- VTSO action review surface
- `OPERATOR_ESCALATED` / `ESCALATION_CREATED` (naming decision per
  Workflow §17.14) audit event
- `ESCALATION_RESOLVED` audit event
- `HANDOVER_CREATED`, `HANDOVER_ACCEPTED` audit events
- `SHIFT_OPENED`, `SHIFT_CLOSED` audit events
- `AUDIT_READ` meta-event emission for review surfaces

**Acceptance:**
- Full shift lifecycle: open → operational → handover → close
- Multi-step escalation chain reconstructable
- Handover transfers pending items cleanly
- SS reviews VTSO actions; emits `AUDIT_READ`

### 18.5 V1.4 — Executive Dashboard Milestone

**Deliverables:**
- Executive Dashboard surface
- KPI tile computations
- Trend chart infrastructure (7d / 30d / 90d rolling)
- Daily Port Brief reader
- Incident summary reader
- Override / escalation rate analytics
- `AUDIT_READ` emission for Executive consumption
- Aggregated metrics caching strategy

**Acceptance:**
- Executive can see trends over rolling windows
- Daily Port Brief renders correctly
- Trend computations match raw audit query results (validation)
- Performance: dashboard load < 3s

### 18.6 V1.5 — Stakeholder Mobile Milestone

**Deliverables:**
- Stakeholder mobile app or PWA (decision per §12.4)
- Login (Horizon-direct in V1.5; federation deferred)
- Assignment feed
- Confirm assignment action
- Report delay action
- Report issue action
- Push notification routing (transport decision per Notification
  Model §12)
- Kyber boundary API contract definition (no implementation)
- `OPERATOR_ACTED` for stakeholder actions
- `STAKEHOLDER_REPORTED_ISSUE` audit event
- `STAKEHOLDER_NOTIFIED` audit event

**Acceptance:**
- Pilot, tug operator, mooring crew can confirm assignments on mobile
- Delay reports reach VTSO operational console
- No "@" character (no email addresses) in any audit payload

### 18.7 V1.6 — Replay & Incident Review Milestone

**Deliverables:**
- Replay & Incident Review surface
- Replay timeline component
- Event reconstruction view
- Recommendation chain visualisation
- Escalation chain visualisation
- Audit chain integrity status
- Incident package export (JSON + PDF + hash proof)
- `INCIDENT_OPENED`, `INCIDENT_REPLAYED`, `INCIDENT_FINDING_RECORDED`
  audit events

**Acceptance:**
- HM can replay a shift's worth of events
- Replay shows full decision chains for recommendations
- Incident package exports cleanly with hash-chain integrity proof
- Performance: shift-scope replay < 5s, 7d replay < 30s

### 18.8 V1.x — Beyond Initial Six Phases

- Authority delegation surface
- VTSO defer permission (if operational pattern emerges)
- Cross-port escalation (multi-port tenants)
- Federated stakeholder auth
- Cross-stakeholder dependency surfacing
- External regulator / auditor read access
- AI advisory features (per Information Architecture §15)
- Customer-tenanted infrastructure migration (AWS / Kubernetes)
- V2 architecture decisions (microservices, multi-tenant per
  deployment)

---

## 19. Success Criteria

V1.0 is considered ready for customer deployment when ALL of the
following are true:

### 19.1 Functional Criteria

- ✓ A VTSO can log in, see role-scoped operational data, acknowledge
  and resolve a real conflict end-to-end with audit trail
- ✓ A Harbour Master can review VTSO actions, override a
  recommendation with reason, sign off a Port Brief
- ✓ A Shift Supervisor can compose a handover, accept an incoming
  handover, review escalations
- ✓ An Executive can view weekly trends and read the daily Port Brief
- ✓ A Stakeholder can confirm an assignment and report a delay on
  mobile
- ✓ An incident can be replayed from the audit ledger with full chain
  integrity

### 19.2 Non-Functional Criteria

- ✓ All authenticated `/api/v1/summary` responses are correctly
  role-scoped (server-side filtering verified)
- ✓ `tests/test_beta10_regression.py` continues to pass (Beta 10
  protection)
- ✓ V1.0-specific regression gate (TBD per V1.0 architecture review)
  passes
- ✓ Audit chain integrity (`verify_chain`) passes on V1.0 deploy's
  audit ledger
- ✓ Performance: `/api/v1/summary` p95 < 500ms; action-to-audit p95
  < 500ms; shift-scope replay < 5s
- ✓ Production safety: V1.0 deploy is operationally safe
  (rollback drill on preview passes)

### 19.3 Operational Criteria

- ✓ Customer onboarding runbook documented
- ✓ V1.0 deployment runbook documented
- ✓ V1.0 rollback runbook documented
- ✓ V1.0 incident response runbook documented
- ✓ Observability stack operational (logs, metrics, error tracking)
- ✓ Audit chain integrity cron job operational

### 19.4 Governance Criteria

- ✓ `phase-0-complete @ 4ad4aae` baseline still protected
- ✓ Beta 10 production still operational (and unchanged by V1 work)
- ✓ Kyber boundary preserved (no source dependency)
- ✓ All four V1 design documents (Permission, Workflow, Screen,
  Information) on `main` with v1.0 or later versions accepted by
  stakeholders
- ✓ Stage E-prod activation decision made (executed OR explicitly
  deferred to a known later date)

### 19.5 What V1.0 Is NOT Required to Deliver

- AI features (per Information Architecture §15; substrate-only)
- Cross-tenant federation (per §14.2)
- Native mobile apps beyond stakeholder PWA
- Customer-tenanted infrastructure (per §15.6)
- Authority delegation (per Workflow §17.17)
- External regulator access (per Permission Model §14.4)
- Multi-monitor / kiosk display modes (per Screen Architecture
  §17.2-17.3)

These are V1.x or V2+ scope.

---

## 20. Open Questions

Surfaced for review before V1.0 architecture lock.

### 20.1 Framework & Stack Decisions

1. **Web framework choice for V1 backend.** FastAPI, Flask,
   Starlette, Litestar, Django, other? Affects team hiring,
   deployment shape, performance characteristics.
2. **Frontend framework choice.** HTMX, React, Vue, Svelte, other?
   Affects team skill mix, build pipeline.
3. **Database hosting.** Railway Postgres for V1.0; AWS RDS for V2?
   Customer-managed Postgres for high-touch customers?
4. **Notification transport.** SSE, WebSocket, external push (APNs /
   FCM), SMS, email, hybrid? Affects backend architecture.

### 20.2 Deployment & Branching

5. **Single-repo vs split-repo.** Continue one repo with Beta 10 +
   V1 separation by directory / branch, OR split V1 into its own
   repo? Single repo preserves history continuity; split repo
   reduces cognitive overhead.
6. **Branch strategy.** `v1/main` as a long-lived integration branch
   off `main`, OR use directory-based separation in `main` (e.g.
   `/v1/server.py`, `/v1/migrations/`)? Both work; trade-offs in
   merge complexity vs file proliferation.
7. **Customer-specific deployment pattern.** One Railway project
   per customer? Customer-specific branches? Configuration only?

### 20.3 Auth & Identity

8. **Auth provider for V1.0.** Self-rolled user/password initially,
   then OIDC integration with customer IDPs? Or OIDC from V1.0?
9. **Stakeholder authentication.** Horizon-direct, port-issued,
   federated with Kyber, customer-managed?

### 20.4 Multi-Tenant Strategy

10. **One Postgres per tenant, or multi-tenant in one Postgres
    from V1.0?** Information Architecture defaults to
    per-deployment isolation; V1.0 architecture review will decide
    whether to support multi-tenant in one DB sooner.
11. **Customer-specific port profiles.** Tenant-config driven
    (V1.0 default) vs customer-customisable engine logic (V1.x+)?

### 20.5 Engineering Process

12. **CI/CD platform.** GitHub Actions (current Phase 0 implicit
    posture), or Railway-native CI, or separate? Affects test
    discipline and gate enforcement.
13. **Code review process.** All PRs require explicit reviewer
    sign-off (current pattern); pair-review for V1 work? Code-owner
    enforcement on protected files?
14. **Linting and style.** Ruff, Black, Pylint, Mypy — which combination
    for V1?

### 20.6 Stage E-Prod Coupling

15. **Activate Stage E-prod before V1 ships, or after?**
    Information Architecture §9.4 documents both options; the
    decision is operational/commercial.
16. **Stage E-prod activation scope.** Activate on `ams-demo` tenant
    only, or on first customer tenant simultaneously?

### 20.7 Operational Concerns

17. **On-call rotation.** When does V1.0 customer commitment require
    24/7 on-call coverage? Affects team size and cost.
18. **Customer support tier.** Self-service portal, email-only,
    phone, on-site? Affects platform UX and pricing.
19. **Data retention contracts.** What retention promises does
    Horizon make to customers in V1.0 contracts? Affects audit
    retention class enforcement (Phase 0.5a partition-drop
    strategy needs automation).

### 20.8 V1 Demonstration Strategy

20. **Beta 10 vs V1 demo strategy.** When V1.0 is ready, which
    surface is the primary commercial demo? Beta 10 stays for legacy
    customer presentations; V1.0 becomes new-customer pitch?

---

## 21. Recommendations

For the V1.0 engineering review:

### 21.1 Treat This Document as Implementation Strategy v0.1

Expect iteration to v0.2, v0.3, ... as engineering review,
architecture review, and operational stakeholders push back on
specific decisions. The strategy is foundational; structural
objections surfaced here are cheaper than after V1.0 implementation
begins.

### 21.2 Lock the V1 Design Quartet Before V1.0 Engineering Begins

The four design documents (Permission, Workflow, Screen, Information)
+ this strategy = five foundational documents. **No V1.0 code is
written until all five are reviewed and accepted by operational
stakeholders and the architecture team.** Implementation against an
unstable foundation is design debt.

### 21.3 Preserve Beta 10 Explicitly

The Beta 10 baseline (`phase-0-complete @ 4ad4aae`) is the protected
commercial commitment. V1 development never breaks Beta 10. The
regression gate is the explicit guard.

### 21.4 Preserve Kyber Boundary

Horizon V1 does not depend on Kyber code. Future integration is
API/event-based with de-identified resource model. Engineering work
that introduces a Kyber-internal dependency is rejected; the
boundary is non-negotiable.

### 21.5 Govern Claude Code Participation Explicitly

Section 16 codifies the rules. Continue the Phase 0 / Phase 1
pattern: Claude proposes, Claude implements on explicit
authorisation, Claude reports back with diff + tests + validation;
Tony decides on merge.

### 21.6 Maintain Stage E-Prod as a Separate Operational Decision

The recommendation across all four design documents to keep Stage
E-prod paused until V1 RBAC ships **continues to stand**. The
trade-off is commercial vs evidentiary; both paths are defensible;
the operator decides.

### 21.7 No Direct-to-Production, Ever

V1 changes always go through preview environments with explicit
acceptance gates before production. The preview-first pattern
established in Phase 1.2 (Stages B → C → D → Drill before E)
generalises to all V1 changes.

### 21.8 Test-First and Audit-First

Every new feature has tests and audit emission designed before
implementation. The Phase 0.7a/b/c/0.8a pattern (helper + tests +
no-op-in-production verification) is the model for V1 audit work.

### 21.9 Document Decisions, Especially Deferrals

Each deliberate scope reduction, deferral, or rejection gets a
decision note (the Phase 0.8b deferral note is the model). This
prevents historical decisions from being silently reversed.

### 21.10 V1 Ships When Success Criteria Are Met, Not on a Calendar

The success criteria in §19 are the V1.0 readiness gate. Calendar
pressure is acknowledged but does not override the criteria.
Customer deployments against an unready V1.0 are reversed by
unhappy customers; the cost of waiting is lower than the cost of
shipping early.

---

**End of v0.1.** Reviewer comments expected before v0.2.

The V1 design foundation now comprises **five documents**:

1. Permission Model v0.1 (PR #30) — WHO
2. Workflow Model v0.1 (PR #31) — HOW
3. Screen Architecture v0.1 (PR #32) — WHERE
4. Information Architecture v0.1 (PR #33) — WHAT
5. **Implementation Strategy v0.1 (this PR) — HOW TO BUILD**

Together they define everything required to engineer V1.0. No V1.0
code is written until all five are reviewed and accepted by
operational stakeholders, the architecture team, and engineering.
