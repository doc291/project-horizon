# Horizon V1 — Platform Foundation (v0.1)

**Document status:** Draft for review — **planning only**
**Document type:** Backend / platform foundation planning
**Target stream:** Future architecture / V1 platform foundation only
  **— Beta 10 is excluded by the Immutability Rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ eb4d441`
  (post-PR #67 — M2 Phase 1 final slice: Centre Panel Navigation)
**Date:** 2026-05-21
**Scope of authority:** Plans the **backend / platform foundation**
  required for Horizon V1 to evolve from a frontend operational
  interface into a deployable operational platform. **Planning only**
  — does **not** authorise implementation, migration, vendor
  selection, framework lock-in, schema freeze, API freeze, deployment,
  or any runtime change. Does **not** modify any previously merged
  document.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58)
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59)
- `HORIZON_M2_ALIGNMENT_REVIEW_v0.1.md` (PR #60)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54)
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- Beta 10 Immutability Rule (state-alignment update, recorded
  in-session)

---

## 1. Executive summary

Horizon V1 is transitioning from a **frontend operational interface**
(M0 + M1 + M2) into a **deployable operational platform**. M2
completed the operational surface foundations: six read-only
centre-spine views (Dashboard, Berth Timeline, Shift Log, VTS,
Pilotage, Performance placeholder) wired by in-memory Centre Panel
Navigation, all fixture-fed and read-only by construction.

This document defines the **backend and platform foundation
direction** beneath those surfaces. It is **planning only**. It does
not authorise implementation, migration, vendor selection, or any
runtime change.

Three load-bearing principles frame everything below:

1. **Beta 10 is permanently isolated** under the Beta 10 Immutability
   Rule. No platform foundation work targets Beta 10. All evolution
   happens in V1 / sandbox / future production streams. Beta 10
   remains the protected commercial trust surface and the reference
   operational build — not a precedent where its choices conflict
   with secure platform design.
2. **Horizon is AMSG-owned, AMSG-built, AMSG-operated** per the
   Horizon Independence / Architecture Reset (PR #51). No Smart
   Ocean X dependency framing is permitted in any platform-
   foundation artefact.
3. **Server-authoritative everything.** Authentication, authorisation,
   business logic, audit emission, and decisioning live on the
   server. The frontend (web today; mobile / iPad pilot view / ECDIS
   interop tomorrow) is one of several consumers of a typed API
   surface, never the place where authoritative rules live.

The document covers fifteen sections: architectural goals, core
platform domains, authentication and RBAC direction, operational-
state architecture, audit and replay architecture, API architecture
direction, multi-port tenancy model, data ingestion and integration
architecture, operational decision lifecycle, security and
deployment direction, technology constraints and philosophy,
cross-references to existing Horizon documents, non-goals, and open
questions.

Nothing in this document is authorised for implementation. Every
concrete next step — scope proposal, implementation plan, vendor
selection, deployment — requires its own separate explicit Tony
authorisation.

---

## 2. Architectural goals

The platform foundation must, by design, achieve the following.
Each goal is a load-bearing property that subsequent scope proposals
will be measured against.

### 2.1 API-first platform

Every operationally meaningful surface is served by a typed,
versioned API. The web frontend is **one** consumer; the future
iPad pilot view (per PR #57), future ECDIS interop, future mobile
operator surfaces, and any partner / integration consumer use the
same API surface (with appropriate authorisation scopes).

### 2.2 Secure authenticated access

Every operational endpoint requires authentication. Anonymous
access is permitted only for clearly public, non-operational
resources (health endpoint with no operational data, version
endpoint). The Phase 1.1 auth-on-what-if pattern from the Beta 10
heritage is the precedent for write-path protection; V1 generalises
this to all operational endpoints.

### 2.3 Role-based operational access

A user holds zero or more roles, each with explicit permissions
scoped to actions and data. Roles map to the workflow profiles
already documented in Operational Platform Workflows §3
(VTSO / Coordination Operator, Harbour Master, Port Authority
Executive, Pilotage Coordinator, Towage Coordinator, Terminal
Operator, Admin, Replay/Audit User).

### 2.4 Multi-port tenancy

A user assigned to one port sees only that port's data. A user
assigned to multiple ports (e.g. cross-port supervisors) has an
explicit, audited port-context switcher **at the platform layer**
— not in the M2 Centre Panel Navigation (which is view navigation
only).

### 2.5 Immutable auditability

Audit rows are append-only and cryptographically chained. The
existing Phase 0.7 hash-chain model is the precedent. Post-hoc
tamper is detectable; rows are not editable after write; audit
writes are synchronous in the operator-intent path with explicit
recovery semantics if the audit write fails.

### 2.6 Operational-state integrity

The authoritative current state of vessels, berths, movements,
resources, constraints, conflicts, and decisions lives **on the
server**. The frontend renders presentation derivations of that
state. The frontend cannot fabricate operational state, cannot
mutate it via DOM tricks, and cannot bypass server-side
authorisation.

### 2.7 Replayability

The operator-visible state at any meaningful historical timestamp
is reconstructible. Replay is observational; replay cannot edit
history. This supports incident review, training, regulatory
inquiry, and operational learning.

### 2.8 Integration readiness

External integrations (AIS, BoM, port-authority PMS, pilot
organisation systems, tug operator systems, terminal operator
systems, statutory reporting) consume the platform's API or are
consumed via documented integration contracts. No external
integration reaches into the operational store directly.

### 2.9 Deterministic decision traceability

Every recommendation Horizon surfaces is traceable to a
deterministic, documented rule. No opaque ML black box drives
operator-facing decisions in V1. ML may inform analytics in later
phases (per the Pilot Proximity App note §10.2 and Operational
Platform Workflows §11.4.1), but never replaces the deterministic
rule layer.

### 2.10 Separation of coordination logic from presentation logic

The Operational Platform Workflows §5.2 principle is reaffirmed:
no business logic is reachable only through the browser. The
frontend computes formatting, layout, mode selection, and hover
state — never authoritative operational rules.

---

## 3. Core platform domains

Conceptual domains. Domain names are stable; internal structure is
not authorised by this document.

### 3.1 Identity & Access
Users, roles, sessions, tokens, port-scope assignments,
re-authentication policy.

### 3.2 Port Context
Port catalogue (Brisbane, Melbourne, Geelong, Darwin, future
ports), port profile (timezone, UN/LOCODE, AIS source binding,
BoM station, channel geometry), port-scope assignment, and
port-context-switch audit for multi-port users.

### 3.3 Operational State
The authoritative current state: vessels, port calls, movements,
berths, resources (pilots, tugs, mooring gangs), constraints,
weather snapshot, tidal snapshot. Owned by the server.

### 3.4 Vessel & Movement State
Vessel master records, port-call lifecycle, movement records
(inbound / outbound / shift), AIS-derived position and intent,
service-attached records (pilotage, towage, mooring, terminal).

### 3.5 Coordination & Decisioning
Conflict detection, recommendation construction, decision-card
lifecycle, operator-action handling (ACK / DEFER / APPLY / REJECT
/ ESCALATE), sequencing alternatives, decision-support payload.

### 3.6 Notifications
Outbound dispatch to stakeholders (pilot dispatcher, tug operator,
terminal operator, vessel agent) through configured channels;
delivery records; subscription management.

### 3.7 Audit & Replay
Append-only event ledger, per-tenant genesis, hash chain, replay
snapshot derivation, audit export workflow, retention policy.

### 3.8 Integration & Ingest
External-feed catalogue (AIS providers, BoM, PMS, etc.); ingest
health and freshness; per-feed credentials; reconciliation rules
when sources disagree.

### 3.9 Configuration & Rules
Port-specific rules (UKC thresholds, pilotage notice windows,
bridge restrictions, tug bollard-pull requirements, channel speed
zones), port profile metadata, integration credentials,
feature-flag catalogue.

### 3.10 Health & Observability
Liveness / readiness probes, structured operational logging,
metrics, distributed tracing, alerting, audit-export status,
on-call escalation.

---

## 4. Authentication & RBAC direction

**Future direction only. M2 does not implement this.**

### 4.1 Authenticated users
Every user is authenticated. Anonymous operational access is
forbidden. Authentication produces a session or short-lived signed
token carrying user identity, current role set, and current
port-scope set.

### 4.2 RBAC
A user holds zero or more roles. Each role maps to a permission
bundle. Permissions are scoped to:
- **port** — per-port data isolation
- **resource** — finer-grained scope (e.g. only this terminal's
  berths)
- **action class** — what kinds of operations the role can perform
  (read only; ACK only; ACK / DEFER / APPLY; admin)

### 4.3 Least privilege
Users receive only the permissions their role requires. Admin is
separate from any operational role. Admins do not have silent
operational read unless they also hold an operational role.

### 4.4 Port-scoped access
A user assigned to a single port sees only that port's data.
Cross-port data leakage is a Sev-1 security defect.

### 4.5 Future multi-port users
Cross-port supervisors / executives / multi-port pilot
organisations may exist. Their port-context selection is an
explicit, audited platform-layer feature — **not** part of M2
Centre Panel Navigation (which is view navigation only).

### 4.6 Separation between operational users and executives
Operational users (VTSO, Harbour Master, Coordinators, Terminal
Operator) hold operational permissions; executives hold read-only
strategic-view permissions. The executive view shows aggregated /
exception data; it never surfaces operator identity or fine-grained
operator actions to the executive role.

### 4.7 No frontend-trusted permissions
The frontend may hide affordances based on role for UX clarity,
but server-side enforcement is authoritative. "Hidden in UI" is
never a sufficient security control.

### 4.8 No anonymous operational access
Restated for emphasis. Health endpoints (e.g. liveness) may be
anonymous if they expose no operational data. Everything else
requires auth.

**M2 does not implement any of this yet.** The current M2 sandbox
is fixture-fed and read-only by construction; auth is deferred to
M3+ under separate authorisation.

---

## 5. Operational-state architecture

The platform foundation requires a clear conceptual separation
between five state categories. Conflating them is the dominant
source of subtle operational bugs, audit failures, and replay
inaccuracy in observed industry systems.

### 5.1 Live operational state
The authoritative *current* state of vessels, berths, movements,
resources, constraints. Server-owned. The source of truth for the
operator's "what is happening now" picture.

### 5.2 Display state
The UI's current view: which mode is active, which tab is
selected (M2 Centre Panel Navigation), which row is hovered,
which filter is applied. Client-owned. Not auditable except
where it captures operator intent (e.g. an explicit port-context
selection by a multi-port user, which is a platform-layer audited
event).

### 5.3 Decision state
The lifecycle of every decision card and recommendation: created,
presented, acknowledged, deferred, applied, expired, rejected,
escalated. Server-owned. Drives the audit trail.

### 5.4 Audit state
The append-only, hash-chained ledger of operator actions, system
events with operational significance, and admin actions.
Immutable; cryptographically defensible; per-tenant genesis.

### 5.5 Replay state
A derived view reconstructable from the audit ledger plus the
operational data stream at any historical timestamp. Replay
shows what the operator was looking at when they made each
decision. Read-only.

### 5.6 Integration state
The metadata about each external feed: source identity,
authentication state, last successful poll, last error, freshness,
reconciliation precedence when sources disagree. Distinct from
operational state — integration state describes the *pipes*, not
the *contents*.

### Why this matters operationally and regulatorily

- **Operational reliability:** when display state leaks into
  operational state, switching tabs can silently corrupt
  authoritative data. When operational state leaks into display
  state, a UI bug can silently mis-represent reality.
- **Audit defensibility:** if the audit ledger is conflated with
  operational state, every operational change requires an audit
  write, performance suffers, and the chain becomes fragile. The
  audit ledger must capture *intent and outcome*, not *every byte
  of state at every moment*.
- **Replay accuracy:** replay must reconstruct what the operator
  saw, not the current state of the world. Without explicit
  separation, replay drifts as operational state evolves.
- **Regulatory and insurance posture:** maritime regulators and
  insurance underwriters require demonstrable separation of intent
  (operator action) from outcome (operational consequence) for
  incident review.
- **Integration robustness:** integration state lives next to
  operational state but must fail independently. A stale AIS feed
  must not corrupt the operational vessel record; it must be
  visibly tagged as stale.

---

## 6. Audit & replay architecture

**Future direction only. M2 does not implement final audit.**

### 6.1 Immutable operational events
Every operationally significant event is captured as an
append-only ledger row: vessel detected, ETA updated, conflict
created, recommendation presented, operator acknowledged,
operator deferred, operator applied, recommendation expired,
admin action.

### 6.2 Decision lifecycle persistence
Each decision card's lifecycle (created → presented → operator
action → outcome → expired) persists as a chain of audit rows.
The full chain is reconstructable from the ledger without
relying on the live operational store.

### 6.3 Recommendation tracking
Every recommendation surfaced to a user has a corresponding
"presented" audit row (per the existing
recommendation-presented-audit pattern from Phase 0.7c).
Operators cannot silently ignore a recommendation without leaving
a trail; even "no action" is captured if the recommendation
expires.

### 6.4 Acknowledgement tracking
ACK / DEFER / APPLY / REJECT / ESCALATE actions are operator
intent and are audited with: actor, timestamp, authenticated
session, port scope, decision card ID, action type, reason
(where applicable), chosen alternative (for APPLY).

### 6.5 Replay snapshots
At each material state change, a replay snapshot derivation is
performed (or computed on demand from the ledger). A Replay /
Audit Mode user (per Operational Platform Workflows §3.8) can
reconstruct the operator-visible state at any prior timestamp.

### 6.6 Event timelines
The Canon §8 swimlane is the user-facing surface for replay.
The underlying data is the audit ledger plus the operational
data stream. The platform's job is to make the join correct,
performant, and defensible.

### 6.7 Operator traceability
Every operator action is traceable to a specific user, session,
port scope, and timestamp. The audit chain prevents repudiation.
Misattribution (operator A's session writing operator B's action)
is a Sev-1 defect.

### 6.8 Regulatory defensibility
The audit ledger must satisfy the evidentiary requirements of
relevant regulators (state Marine Acts, maritime insurance,
incident investigators). The hash chain is the technical
foundation; retention policy (open question, §15) is the
governance side.

### Explicit clarification on M2 Shift Log

**The M2 Shift Log tab is NOT the final audit architecture.** Per
PR #64's commit message:

> The V1 captured /api/summary fixtures do not carry an events[]
> array. The adapter therefore derives the Shift Log view ONLY
> from explicit, timestamped, already-present facts in
> ViewSummary. The adapter explicitly does NOT invent operator-
> action events. Every emitted row carries source: "derived" and
> the tab disclosure copy ("DERIVED · NOT AUDIT LEDGER") makes
> the observational nature explicit.

When the real audit ledger lands (M3 / M4 under separate scope),
the Shift Log surface is replaced by a query into the ledger.
The frontend seam (the `shiftLogAdapter`) is forward-compatible:
the underlying data source switches from derivation to
authoritative ledger query without component rewrites.

---

## 7. API architecture direction

### 7.1 API domains
Per Operational Platform Workflows §7, the high-level API domain
list is: **auth / users / roles**, **ports**, **vessels**,
**movements**, **berths**, **resources**, **constraints**,
**conflicts**, **decisions / recommendations**, **notifications**,
**audit / replay**, **integrations / ingest**, **system health**.

### 7.2 Frontend / backend separation
The frontend is one consumer of these APIs. The backend owns the
domain model, the rules, the persistence, and the audit chain.
No domain logic is reachable only through the browser.

### 7.3 Typed contracts
Domain entities are typed at the API boundary. Type definitions
come from a single source of truth (OpenAPI / JSON Schema /
typed IDL — choice deferred). Both backend and frontend consume
the generated types so contract drift is caught at build time.

### 7.4 Versioning philosophy
- Semantic versioning at the API level.
- Major-version cuts carry a ≥ 90-day deprecation notice.
- Internal-only APIs are clearly marked and excluded from the
  stability commitment.
- Backwards-incompatible changes require migration plans for
  every known consumer (web frontend, future iPad pilot view,
  future ECDIS, partner integrations).

### 7.5 Event vs polling considerations
M1 / M2 use polling (`useSummary` at 30 s). Polling is correct
for low-frequency aggregate state. Event-driven update is correct
for time-critical signals (proximity alerts, conflict surfaces).
The platform foundation must permit both — typically polling for
the bulk view, with selective server-sent events (SSE) or
WebSocket for time-critical channels.

### 7.6 Future SSE / WebSocket considerations
- **SSE is the strong default** for one-way server → client
  push: simple protocol, HTTP-friendly, no separate auth dance,
  natural reconnect. Suitable for proximity alerts, conflict
  surface updates, decision-card lifecycle events.
- **WebSocket** is warranted only when bidirectional
  low-latency channels are required (none anticipated in V1).
- Either path must respect server-side authorisation: the
  push stream is scoped to the authenticated user's port set
  and role permissions.

### 7.7 Server-authoritative decisioning
Conflict detection, severity classification, decision-support
construction, recommendation ranking — all server-side. The
frontend renders the result; it does not compute it.

### 7.8 No frontend business-rule ownership
Restated. The M2 Alignment Review §3.3 stop condition for this
is in force ("if an adapter would have to compute a business
rule to populate a tab, stop and report") and remains in force
for all M3+ work.

---

## 8. Multi-port tenancy model

### 8.1 Port-scoped access
A user authenticated to a port has access to that port's data
only. The platform enforces this at every read and every write,
not just at the UI layer.

### 8.2 Tenant isolation
Where multiple port-authority customers share the platform
(future), data isolation is at the tenant level **above** the
port level. A port-authority customer's audit chain has a
distinct genesis from any other customer's. Cross-tenant access
is permitted only via explicit shared-resource constructs (e.g. a
designated VTS provider serving multiple port authorities) and is
itself audited.

### 8.3 Future multi-port organisations
Some organisations operate across multiple ports (cross-port
pilot organisations, multi-port tug operators, port-authority
groups with several ports). Their users hold port-scope sets
rather than a single port. The platform models this as a
many-to-many relationship between users and ports, with explicit
per-port roles.

### 8.4 Role inheritance concepts
A user's effective permission set is the union of their role
permissions, intersected with their port scope. Role inheritance
(e.g. "Harbour Master implies VTSO read") is a configuration
concern, not a hard-coded model. Inheritance rules are themselves
auditable when changed.

### 8.5 Operational separation boundaries
A user cannot:
- Read data from a port outside their scope.
- Acknowledge a recommendation issued in a port outside their scope.
- Be impersonated by an admin without an audit trail.
- See PII (pilot identity, master name, where applicable) outside
  their scope.

### 8.6 Cross-port leakage treated as Sev-1
Any incident where a user reads or writes data from a port not in
their scope is a Sev-1 security defect, triggers immediate
incident response, and requires a postmortem.

### 8.7 Explicit confirmation: Centre Panel Navigation is not port switching

**The M2 Centre Panel Navigation (PR #67) is centre-panel view
navigation only.** It switches between Dashboard, Berth Timeline,
Shift Log, VTS, Pilotage, and Performance. It does **not** switch
ports. A user remains within their authorised port context for
the entire session. Multi-port users will receive a separate,
explicit, audited port-context-switch affordance at the platform
layer in M3+ — not in centre-panel navigation. The M2 test suite
already enforces the absence of any port-switching affordance in
the centre-panel layer.

---

## 9. Data ingestion & integration architecture

### 9.1 AIS ingestion
Multiple AIS providers (AISStream, MST, port-authority direct
feeds) feed the platform via separate adapters. Each adapter
normalises to a common internal `Vessel` event shape. Reconciliation
chooses the highest-confidence source per port-vessel pair, with
documented precedence and explicit observability into which
source the operator is currently seeing.

### 9.2 Weather and tidal ingestion
BoM (or equivalent local bureau) is the canonical Australian source.
Future ports may use other bureaux. The same adapter pattern
applies: source-specific adapter → normalised internal event
shape. Tidal observations and forecasts are treated separately
(observed values are operational data; forecasts are predictions).

### 9.3 Pilotage integration
Authoritative pilotage systems (when integrated) feed the platform;
they are not replaced by it. Per the Pilot Proximity App note
(PR #57) §5.4: "Horizon is not a pilotage operating system." The
platform consumes pilotage data; it does not direct it.

### 9.4 Berth planning integration
Berth schedule and readiness data may come from port-authority
PMS systems (DP World, Patrick, etc.) and from terminal operator
systems. Each is a separate integration; reconciliation rules
apply when terminal-side and port-authority-side disagree.

### 9.5 Event normalisation
Every external event arriving at the platform is normalised to a
typed internal event shape before it touches operational state.
Normalisation is deterministic, source-attributed, and
unit-tested.

### 9.6 Source attribution
Every operationally-visible value carries its source identity (which
feed, which provider, which timestamp). Operators can see at a
glance where a value came from. Confidence is derived from source
identity plus freshness.

### 9.7 Freshness / state confidence
A "stale" indicator is computed per source per port. The
operator-visible UI surfaces staleness explicitly (per the M1
stale-data banner pattern). Stale data is not silently used as
current.

### 9.8 Integration adapters
Each external system has its own adapter. Adapters are
independently versioned, independently tested, and independently
deployable. Adapter failure does not cascade into operational
state corruption — at worst it produces a stale or empty
operational view, which the UI signals explicitly.

### 9.9 Retry / error isolation
Adapter-level retries are bounded (exponential backoff with a
ceiling), surface their state to observability, and do not block
the operational pipeline. Persistent errors page on-call.

### 9.10 Degraded-mode handling
When a primary feed is unavailable, the platform falls through to
secondary sources per documented precedence. The operator sees
"degraded — using secondary AIS" rather than silent fallback. If
all feeds are unavailable, the operator sees "no live vessel data
— revert to VHF" or equivalent explicit fallback.

---

## 10. Operational decision lifecycle

The conceptual lifecycle below extends Operational Platform Workflows
§4 with explicit attention to where each step is owned, what it
emits, and how it is audited. Each step is described in
implementation-neutral terms; the implementation belongs to M3+
under separate authorisation.

### 10.1 Ingest
External feeds (AIS, BoM, PMS, etc.) arrive at the platform. The
ingest layer normalises, source-attributes, deduplicates, and
forwards to operational state. **Audit emission:** ingest events
recorded at the integration layer; not in the operator audit
chain (which is for operator intent).

### 10.2 Evaluate
The rules layer continuously evaluates constraints against the
current operational state: UKC vs tide, LOA vs berth, pilotage
notice windows, bridge restrictions, wind / swell thresholds,
cyclone-season rules. Deterministic. Documented. Reviewable.

### 10.3 Detect
A constraint violation or imminent violation produces a candidate
conflict. The detection layer enriches with conflict type,
severity, contributing vessels / resources, and time window.

### 10.4 Recommend
The recommendation layer constructs decision-support payloads
for CONFLICT-class detections: alternatives with cost / risk
metadata, recommended option, deadline. Construction is
deterministic and documented.

### 10.5 Present
The decision card is surfaced to the operator. A presentation
event is audited (per the existing recommendation-presented audit
pattern). The operator may now act.

### 10.6 Acknowledge / act
The operator's input (ACK / DEFER / APPLY / REJECT / ESCALATE)
is captured server-side, authorisation-checked, and committed.
**Audit emission:** OPERATOR_ACTED row with actor, timestamp,
session, port scope, decision card ID, action type, reason,
chosen alternative.

### 10.7 Audit
The action and its outcome are written to the append-only ledger.
The hash chain advances. Notifications fire to downstream
stakeholders if applicable (per §3.6).

### 10.8 Replay
At any subsequent time, a Replay / Audit Mode user can
reconstruct what the operator saw at each step. Replay never
edits history.

### Lifecycle invariants
- No step is automated end-to-end. Every operator-facing action
  requires a human in the loop.
- No step can be skipped: presentation is required before
  acknowledgement; acknowledgement is required before outcome.
- Every step emits exactly one audit row (or zero, for purely
  internal evaluation steps). Multiple steps emitting from the
  same operator action are explicitly chained (one row per
  semantic event).

---

## 11. Security & deployment direction

**Future direction only. Beta 10 remains isolated and immutable.**

### 11.1 Environment separation
- **Beta 10 production** — preserved commercial trust surface.
  Immutable. No V1 traffic, no V1 deploys.
- **V1 sandbox** (`horizon-v1-sandbox` today) — experimentation
  and pre-production validation.
- **V1 production** (future) — deployable operational platform
  serving live customers. Separately authorised, separately
  deployed, separately monitored.
- **Per-customer environments** (future, multi-tenant) — may be
  separate environments or logical tenants within a shared
  environment; that is a deployment architecture choice deferred
  to M5+ scope.

### 11.2 Production isolation
V1 production runs on AMSG-owned infrastructure (current sandbox
uses Railway; production platform choice is deferred per
Operational Platform Workflows §11.2.2). Production is firewalled
from sandbox; secrets are scoped per-environment; observability
is per-environment.

### 11.3 Sandbox isolation
The sandbox carries no live customer data. It runs against
fixtures (today) or test datasets (future). Sandbox traffic does
not cross-pollute production. Sandbox deploys are independent of
production deploys.

### 11.4 Secret management
Production secrets live in a managed secret store (cloud KMS, or
equivalent — choice deferred per Operational Platform Workflows
§11.3.2). Secrets are never committed to git. Secrets rotation is
operational, audited, and supported without invalidating the
audit chain.

### 11.5 Operational logging
Structured logs at the platform layer cover every request, every
auth decision, every audit emission, every integration event.
Logs are correlated by request ID and user session. Logs do not
leak PII or secrets.

### 11.6 Monitoring
Liveness, readiness, error rate, latency percentiles, audit-chain
integrity, integration freshness, on-call alert paths. SLOs are
declared per surface (web frontend availability, API availability,
audit-emission latency, etc.).

### 11.7 Future pen-test / security review
Before any live-client deployment, V1 undergoes a formal external
security review and penetration test. Findings remediated to an
explicit bar (no Sev-1 / Sev-2 open at go-live; Sev-3 with named
owner and remediation date). Annual re-assessment as a minimum
cadence.

### 11.8 Production governance
Deployments to V1 production require explicit Tony authorisation.
The two-phase pattern (Phase 1 local → Phase 2 sandbox → merge →
explicit production deploy) is extended; production deploys
themselves are governed by their own check-list (in line with
the M2 Implementation Plan §19 pattern, scaled up).

### Explicit: Beta 10 remains isolated and immutable.

**Restated for the record.** Beta 10 is a protected commercial
demonstration environment. No V1 feature, fix, enhancement, UI
change, architectural change, API change, UX improvement, or
data-model change is **ever** promoted / merged / cherry-picked /
backported / manually replicated into Beta 10. The narrow
exception (critical demo-blocking defect with explicit Tony
approval) does not apply to platform foundation work.

---

## 12. Technology constraints & philosophy

### 12.1 Avoid premature complexity
The simplest design that satisfies the goals is preferred. Add
complexity only when a concrete operational need demands it. A
monolith with clear domain boundaries is acceptable for V1
production; microservices arrive when scale or organisational
boundaries demand them, not before.

### 12.2 Operational clarity over fashionable architecture
Operators must understand what the platform is doing and why.
Architectures that obscure operational behaviour for the sake of
elegance are anti-patterns. A boring, debuggable, traceable
platform serves operators better than a clever one.

### 12.3 Deterministic systems preferred over opaque AI orchestration
Per §2.9: every recommendation traceable to a documented
deterministic rule. ML may inform analytics; ML does **not**
replace the rule layer for operator-facing decisions in V1.

### 12.4 Operational trust over automation hype
"The platform does X automatically" is an anti-feature when X is
a safety-critical decision. Per Operational Platform Workflows §3
("must-never-be-automated"): operator actions remain operator
actions. Automation is reserved for non-operational concerns
(monitoring, secret rotation, deploys).

### 12.5 Gradual platform hardening
V1 production goes live with the minimum viable security and
observability posture, **then hardens**. The hardening sequence
is documented: which controls are required at first live-client
deploy, which arrive at first multi-tenant deploy, which arrive
at first regulatory audit. This is a Tony-side strategic
sequencing decision under separate authorisation.

### 12.6 No framework lock-in from this document
This document deliberately does **not** select:
- A backend language / framework
- A database (relational / document / hybrid)
- An ORM / query layer
- An API style (REST + JSON / GraphQL / gRPC / hybrid)
- An event-stream substrate (in-process / Kafka / NATS / Redis
  Streams)
- An IdP / auth provider
- A deployment platform (Railway / AWS / Azure / GCP / AMSG-owned)
- A frontend framework beyond the M1-shipped React + Vite
- An observability stack

Each choice is a separate scope-proposal decision under separate
explicit Tony authorisation.

---

## 13. Relationship to existing Horizon documents

This document **extends** the merged Horizon documentation set;
it does **not** modify any of them.

| Document | Relationship to this foundation document |
|---|---|
| `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58) | UX Direction governs the operator-facing posture. This foundation document is the back-end mirror: right rail = action surface implies server-authoritative decisioning; centre = VTS implies a server-authoritative spatial pipeline; role-specific modes imply server-side RBAC |
| `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59) | Platform Workflows is the canonical workflow + principles document. This foundation document is the next level of detail beneath each principle: API domains, state-category separation, lifecycle, integration |
| `HORIZON_M2_ALIGNMENT_REVIEW_v0.1.md` (PR #60) | Alignment Review's drift risks (§3.3 no frontend-heavy logic, §3.4 state separation, §3.7 no Beta 10 patterns) are *constraints* this foundation document operationalises |
| `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57) | Pilot Proximity is a future downstream consumer of this platform foundation. §3.3 Pilotage Coordinator workflow and §5.x architectural guardrails feed directly into this document's API and audit direction |
| Beta 10 Immutability Rule | Hard constraint throughout. Every section that names a deployment, runtime, or environment reaffirms Beta 10 isolation |
| `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51) | Independence Reset framing is preserved throughout. No Smart Ocean X dependency framing |
| `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54) | M2 Plan is the implementation precedent: scope proposal → implementation plan → explicit start authorisation. The same pattern applies to each platform foundation milestone |
| `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53) | M2 Scope is the precedent for how a backend / platform milestone is scoped: explicit in-scope, out-of-scope, deny-list, open questions |
| `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43) | Canon §1.1.3 tab set and §10 anti-patterns are honoured; canon does not change |
| `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40) | The Adapter Design Note's ViewSummary contract is the frontend seam to the future live API. This foundation document does not modify the contract; it specifies how the backend produces it |

---

## 14. Non-goals

The following are explicitly **out of scope for this document**.
Each is a separate, later scope-proposal decision under explicit
authorisation.

- **Implementation** of anything described above
- **Migrations** from any current state (Beta 10 or sandbox) to a
  future platform
- **Database selection / lock-in** (relational / document /
  hybrid choice)
- **Framework selection / lock-in** (backend language /
  framework / ORM / API style)
- **Kubernetes rollout** or any specific deployment-platform
  selection
- **Production deployment** of V1
- **Auth implementation** (IdP integration, session machinery,
  RBAC enforcement layer)
- **Live RBAC implementation**
- **Live audit engine** (append-only ledger, hash chain at
  production scale)
- **Live replay engine** (snapshot derivation, swimlane timeline
  backend)
- **Stage E-prod activation** (Stage E-prod remains paused)
- **Multi-tenant production** rollout
- **Vendor / partner integration** contracts
- **SLA / SLO commitments** to customers
- **Pricing / commercial model** decisions
- **Marketing positioning** beyond Independence Reset framing
- **Beta 10 modification** of any kind (Immutability Rule)
- **Smart Ocean X framing reintroduction** (Independence Reset)
- **Kyber-boundary crossing** in any artefact

---

## 15. Open questions

Recorded for Tony direction, maritime SME input,
deployment / security review, and operational governance review.
**None of these blocks anything in M2.** They are inputs to the
first platform-foundation scope-proposal cycle when that begins.

### 15.1 Direction (Tony)

15.1.1 Which port is the first live-client target for V1
production? Brisbane (strongest data maturity), Darwin (now
demo-verified), or another?

15.1.2 What is the expected concurrent user count at first
live deployment? 5? 25? 100? This affects every infrastructure
sizing decision.

15.1.3 What is the desired sequence of capability arrival —
auth first, then audit, then write-path, or some other order?
This is a Tony-side strategic sequencing decision.

15.1.4 What is the commercial framing for production —
per-port subscription, per-tenant enterprise, hybrid?

15.1.5 Is there a target date for first live-client production
deployment? Real or directional?

### 15.2 Architecture / engineering

15.2.1 Storage choice — relational + append-only audit table,
document store, hybrid, or something else?

15.2.2 Deployment platform — Railway, AWS, Azure, GCP, or
AMSG-owned bare metal / private cloud?

15.2.3 Event-stream substrate — in-process async (sufficient
for M3 / M4), durable substrate (Kafka, NATS, Redis Streams,
required by M5+), or none yet?

15.2.4 API style — REST + JSON with OpenAPI, GraphQL, hybrid?

15.2.5 Backend language / framework — Python (continuity with
Beta 10 heritage), Node / TypeScript (continuity with V1
frontend), Go, Rust, other?

15.2.6 Auth IdP — custom, Auth0, Clerk, Okta, port-authority
SSO, per-tenant choice?

15.2.7 Audit chain — extend the existing Phase 0.7 model, or
re-design from scratch for V1?

15.2.8 Replay snapshot derivation — on-demand from audit ledger
(simpler; possibly slow), pre-computed snapshots (faster; more
storage), or hybrid?

### 15.3 Security / regulatory

15.3.1 What is the audit-retention policy? Months, years,
indefinite? Subject to legal advice.

15.3.2 What is the data-residency requirement per port / per
customer? Australian-resident data is the baseline; per-customer
overrides possible.

15.3.3 Who performs the pre-go-live pen-test and security
review?

15.3.4 What is the maritime-insurance posture for V1
production? Subject to insurer review.

15.3.5 What is the incident-response on-call structure pre-go-
live? Tony only, plus a named technical on-call, plus a SecOps
function?

### 15.4 Maritime SME

15.4.1 Are the workflow profiles in Operational Platform
Workflows §3 complete for the first live-client port? Any port-
specific roles missing (e.g. Bunker Coordinator, Customs
Liaison)?

15.4.2 Are the integration sources in §9 complete? Which
port-authority systems must V1 integrate with for first
live deployment (PMS, statutory reporting, customs, billing)?

15.4.3 Are the audit categories in §6 sufficient for incident
investigation, regulatory inquiry, and insurance review?

15.4.4 What is the acceptable operator-action-to-server latency
from the user's perspective?

### 15.5 Governance

15.5.1 At what cadence is the Beta 10 Immutability Rule
re-affirmed in platform-foundation artefacts? Per scope
proposal? Per implementation plan? Per retrospective? (Default
proposal: cite in every scope proposal and every implementation
plan; re-affirm in every retrospective.)

15.5.2 Who, in addition to Tony, may authorise platform-
foundation milestones? Currently Tony-only; a succession /
coverage policy may be needed as the platform scales.

15.5.3 How is the Independence Reset framing audited across
the platform-foundation document stream? Per-document grep
at PR review time, or a documented review checklist?

15.5.4 How does the Kyber boundary interact with platform
foundation work? Specifically: does any platform foundation
decision approach the Kyber boundary, and if so, what is the
explicit authorisation path?

---

## End of Platform Foundation v0.1

**Status: planning only. Not authorised for implementation.**

Confirmed by this document:
- This is a **planning document**. No implementation, no migration,
  no vendor selection, no framework lock-in, no deployment, no
  runtime change is authorised.
- M0 / M1 / M2 are not retroactively modified.
- The Operational UX Direction (PR #58), Operational Platform
  Workflows (PR #59), M2 Alignment Review (PR #60), Pilot
  Proximity App note (PR #57), Component Canon (PR #43),
  Independence Reset (PR #51), and all M2 implementation slices
  (PRs #61 – #67) are honoured and unmodified.
- The Beta 10 Immutability Rule is in force throughout. Beta 10
  is permanently isolated; no platform foundation work touches it.
- The Independence / Architecture Reset framing is preserved —
  no Smart Ocean X dependency framing anywhere in this document.
- The Kyber boundary remains in force.
- The M2 Centre Panel Navigation is **centre-panel view navigation
  only**, not port switching. Multi-port port-context selection
  is a separate, audited, platform-layer feature deferred to M3+.
- All concrete platform foundation work (auth, RBAC, audit
  engine, replay engine, integration adapters, production
  deployment) is gated on its own scope proposal, its own
  implementation plan, and Tony's explicit authorisation
  per milestone.

**Next action:** Tony's review of the architectural goals (§2),
the state-category separation (§5), and the open questions (§15).
Optional ChatGPT engineering review of the API / data / security
direction (§7, §8, §11). If approved and merged, this document
becomes the upstream reference for the first platform foundation
scope-proposal cycle (likely covering auth + RBAC, per the
Operational Platform Workflows §11.4.2 sequencing question — but
that sequencing is itself a Tony-side decision per §15.1.3).
