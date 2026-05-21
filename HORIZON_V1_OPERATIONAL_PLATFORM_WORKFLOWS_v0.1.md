# Horizon V1 — Operational Platform Workflows (v0.1)

**Document status:** Draft for review — **future architecture document**
**Document type:** Operational workflows + platform architecture expectations
**Target stream:** V1 sandbox / future architecture only
  **— Beta 10 is excluded by the immutability rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ cc2f2ea`
  (post-PR #58 — Operational UX Direction)
**Date:** 2026-05-21
**Scope of authority:** Defines the operational workflows and platform
  architecture expectations for Horizon V1 as a real deployable
  platform. **Principles and intent only** — does **not** authorise
  implementation, vendor selection, contract negotiation, schema
  freeze, API freeze, or any runtime change. Does **not** modify any
  previously merged document. Does **not** alter Beta 10 in any
  way.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58, `cc2f2ea`)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54)
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53)
- `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md` (PR #52)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md`
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md`
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md`
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md`
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md`

---

## 1. Executive summary

Horizon V1 is **not a visual refresh of Beta 10.** It is the
transition from a protected commercial demonstration build to a
real deployable operational platform.

Beta 10 is permanently isolated under the **Beta 10 Immutability
Rule**: it is a preserved commercial trust surface, a stable demo
environment, and a reference operational build. No V1 feature,
fix, enhancement, UI change, architectural change, API change, UX
improvement, or data-model change is ever promoted, merged,
cherry-picked, backported, or manually replicated into Beta 10
(except under the narrow critical-demo-blocking-defect exception
with explicit Tony approval).

This document records what Horizon V1 must **be** as a platform
once that separation is in force:

- A **real backend service** with a clean, versionable, typed
  API surface — not a static page bundle.
- **Authenticated multi-user access** with role-based
  authorisation enforced server-side, not in the browser.
- **Auditable operator actions** captured to an append-only,
  cryptographically chained ledger.
- A **read-only operational surface today** that evolves toward
  selective, deliberately authorised write paths under explicit
  scope governance.
- **Clear separation between operational state, decision state,
  and display state** so each surface can evolve independently.
- **Integration-ready data boundaries** so that AIS, BoM, port
  authority, pilot organisation, and tug/mooring feeds can be
  connected without re-architecting the core.
- **No dependency on Beta 10 runtime patterns** where those
  patterns conflict with secure platform design.
- **No Smart Ocean X dependency framing** anywhere in the V1
  programme — Horizon is AMSG-owned, AMSG-built, AMSG-operated
  under the Independence / Architecture Reset (PR #51,
  `b0d9ff2`).

This document is **principles and intent**, not implementation
authorisation. M2 scope (PR #53) and M2 implementation plan
(PR #54) remain the next concrete step under separate
authorisation. M3+ scope proposals will cite this document and
must honour each principle below.

---

## 2. Target operating model

### 2.1 What Horizon V1 supports operationally

A real port using Horizon V1 expects the platform to support:

- **Continuous operational awareness** for the on-watch VTSO,
  Harbour Master, and Shift Supervisor across an entire shift,
  not just for a demo window.
- **Multiple concurrent users in multiple roles**, each
  authenticated, each operating within an explicit permission
  boundary, each producing auditable actions where they act.
- **Multiple data sources** — AIS (AISStream, MST, port-authority
  feeds), BoM (tides, weather, swell), port-authority operational
  data (berth assignments, pilotage queue), tug operator feeds,
  mooring gang status, terminal readiness, and (future) ECDIS
  bridge data.
- **Operational events** that drive decisions — vessel detected,
  ETA updated, conflict predicted, conflict resolved,
  acknowledgement issued, sequencing decision applied, deferral
  recorded, escalation raised.
- **Operator decisions** captured with intent, reasoning,
  timestamp, actor, and outcome — even when the operator's
  decision is "defer" or "do nothing".
- **Audit and replay** — every operator-visible state at every
  meaningful moment must be reconstructible after the fact, for
  incident review, training, and regulatory inquiry.
- **Notification and coordination** — downstream stakeholders
  (pilots, tugs, terminal, mooring, agent) receive the right
  information at the right time, through the channels they
  already use.
- **Per-port scoping** — a multi-port deployment serves Brisbane,
  Melbourne, Geelong, Darwin, and future ports without
  cross-port data leakage.

### 2.2 What Horizon V1 is not

- It is **not** a VTS replacement. VTS remains the official
  traffic-management authority (see the Pilot Proximity Companion
  App note, PR #57, for the established boundary).
- It is **not** a navigation system. It does not issue vessel
  commands; it does not give COLREGS recommendations as if they
  were instructions; it does not direct course or speed.
- It is **not** a port management system (PMS) replacement —
  it integrates with existing PMS surfaces, billing, customs,
  and statutory reporting; it does not replace them.
- It is **not** a black-box ML recommendation engine — every
  recommendation Horizon surfaces must be traceable to a
  deterministic, documented rule set.
- It is **not** Beta 10 with nicer styling. The platform
  architecture is materially different. (Beta 10 remains valuable
  as a reference operational build, not as an implementation
  precedent.)

### 2.3 Deployment posture

- **Production stream:** to be defined separately. Not authorised
  by this document. Hosted on AMSG infrastructure (Railway today;
  may evolve to a different platform as the deployment posture
  matures).
- **Sandbox stream:** `horizon-v1-sandbox` Railway service —
  the experimentation and pre-production validation environment.
- **Demo / reference stream:** Beta 10 — **immutable** under the
  Beta 10 Immutability Rule. No V1 traffic, no V1 deploys, no V1
  backports.
- **Local stream:** developer machines — used for fixture-based
  validation per the M1 / M2 pattern.

---

## 3. Core user workflows

Each role below has a workflow profile with seven attributes:
**objective**, **primary screen / mode**, **inputs**, **decisions
/ actions supported**, **what is visible**, **what is writable**,
**what must be audited**, **what must never be automated**.

These are **conceptual workflows**, not implementation specifications.

### 3.1 VTSO / coordination operator

- **Objective:** maintain continuous traffic, conflict, and
  sequencing awareness across the active shift; coordinate
  pilot, tug, mooring, terminal, and vessel-side parties to
  resolve operational conflicts.
- **Primary screen / mode:** Coordination View — right-rail
  decision cards + centre-spine VTS spatial surface +
  supporting KPI strip (per Operational UX Direction §3).
- **Inputs:** live AIS, BoM environmental, port operational
  state, pilotage queue, tug schedule, mooring status, terminal
  readiness, vessel ETA updates, weather warnings, channel state.
- **Decisions / actions supported (M3+ future, not authorised
  here):** acknowledge a conflict / recommendation; defer with a
  reason and a re-prompt time; apply a sequencing alternative;
  escalate to Harbour Master; record an out-of-band radio
  exchange.
- **Visible:** all operational data scoped to the user's port(s);
  current decision cards; conflict and recommendation list;
  spatial map with vessel positions and tracks; conditions strip;
  shift log.
- **Writable (M3+ future):** ACK / DEFER / APPLY / ESCALATE on
  decision cards; free-text operational note (audited); explicit
  "no action" with reason.
- **Must be audited:** every ACK, DEFER, APPLY, ESCALATE; every
  out-of-band note; every login / logout; every port-context
  switch within a session.
- **Must never be automated:** decision execution. Horizon never
  acknowledges, applies, defers, or escalates on the user's
  behalf. Recommendations are surfaced; humans decide.

### 3.2 Harbour Master

- **Objective:** maintain accountability for safe port
  operations; review escalations; sign off on materially
  significant operational decisions; review incidents and
  near-misses; oversee shift supervisors and VTS performance.
- **Primary screen / mode:** Coordination View with elevated
  visibility — full decision history, escalations queue, shift
  performance summary; optionally Executive View when reviewing
  trends.
- **Inputs:** everything the VTSO sees + escalations directed
  to Harbour Master + summary metrics over shift / day / week.
- **Decisions / actions supported (M3+):** approve / reject
  escalated decisions; record sign-offs; close incident records.
- **Visible:** full operational picture for the port(s) under
  Harbour Master authority; cross-shift visibility; full audit
  trail visibility for review.
- **Writable (M3+):** sign-offs; incident records; performance
  notes (all audited).
- **Must be audited:** every sign-off, every incident-record
  edit, every escalation decision, every override of a VTSO
  recommendation, every login / logout.
- **Must never be automated:** approval of escalations,
  incident closure, override of a VTSO acknowledgement.

### 3.3 Port authority executive / viewer

- **Objective:** maintain strategic awareness of port performance,
  trends, and exceptions; receive briefings; understand
  operational KPI movement; not operationally responsible.
- **Primary screen / mode:** Executive View — KPI surface as
  default focal point; trend lines; exception escalation feed;
  utilisation forecast.
- **Inputs:** aggregated, derived operational data; trend
  series; exception summary.
- **Decisions / actions supported:** none operational (this is a
  read-only role by design). May acknowledge having read a
  briefing.
- **Visible:** KPI dashboards; trend lines; exception summary;
  high-level shift summary; **not** individual operator
  identities or fine-grained operator actions (PII / audit
  scoping per §6).
- **Writable:** none operational. May annotate briefings (kept
  inside the executive scope; not surfaced to operators).
- **Must be audited:** every login / logout; every annotation;
  every export of operational data outside the platform.
- **Must never be automated:** sign-off, instruction, or any
  apparent operational action. The executive view must not
  appear to issue operational instructions.

### 3.4 Pilotage coordinator

- **Objective:** dispatch, schedule, and brief pilots for
  inbound / outbound movements; maintain pilot availability
  awareness; coordinate with VTSO on pilotage timing.
- **Primary screen / mode:** Pilotage View — pilotage queue,
  pilot roster, current movements, pilot proximity / status,
  briefing context (and, in future, integration with the Pilot
  Proximity Companion App per PR #57).
- **Inputs:** pilotage requests; vessel ETA / ATA; pilot roster
  and certifications; pilot location / readiness; pilotage notice
  windows.
- **Decisions / actions supported (M3+):** assign a pilot to a
  movement; reassign; mark pilot unavailable; record a
  cancellation; coordinate with VTSO on timing changes.
- **Visible:** pilotage queue (own port); pilot roster (with
  resource-safe identifiers); current vessel positions where
  relevant to pilotage; pilot proximity (future).
- **Writable (M3+):** pilot assignments; cancellations;
  unavailability records (all audited).
- **Must be audited:** every assignment / reassignment /
  cancellation; every roster change; every pilot-status
  override.
- **Must never be automated:** pilot assignment, reassignment,
  or cancellation. Horizon may recommend; humans assign.

### 3.5 Towage coordinator

- **Objective:** dispatch tugs to scheduled operations;
  maintain tug availability awareness; coordinate with VTSO and
  pilotage on timing.
- **Primary screen / mode:** Pilotage / Towage operations view
  (likely a shared composite mode with Pilotage in early
  implementations) — tug roster, current jobs, double-booking
  flags, tug-fleet status.
- **Inputs:** towage schedule; tug roster and bollard-pull
  capacity; tug location / status; vessel size and movement
  requirements; weather constraints affecting tug operations.
- **Decisions / actions supported (M3+):** assign tugs to jobs;
  reassign; mark tug unavailable; record cancellation;
  coordinate timing changes.
- **Visible:** towage schedule (own port); tug roster;
  conflict / double-booking flags; current vessel positions
  where relevant.
- **Writable (M3+):** tug assignments; cancellations;
  unavailability records (all audited).
- **Must be audited:** every assignment / reassignment /
  cancellation; every tug-fleet status edit.
- **Must never be automated:** tug dispatch. Horizon may
  recommend; humans dispatch.

### 3.6 Terminal operator

- **Objective:** maintain berth readiness for inbound vessels;
  coordinate with VTSO and Harbour Master on berth-readiness
  timing; surface terminal-side constraints (crane availability,
  stevedore readiness).
- **Primary screen / mode:** Berth Timeline View (Canon §1.1.3
  + UX Direction §3.4) scoped to the operator's terminal — own
  berths only; current and next vessels at each berth; readiness
  vs. ETA gap.
- **Inputs:** berth schedule; vessel ETA / ATA; terminal-side
  constraints (cranes, stevedores, equipment); cargo type and
  quantity; weather constraints.
- **Decisions / actions supported (M3+):** mark berth-readiness
  time; record terminal-side delay; flag readiness concern.
- **Visible:** own terminal's berths and current / next vessels
  on those berths; readiness vs. ETA picture; conditions strip;
  **not** other terminals' detailed berth content.
- **Writable (M3+):** berth-readiness times; delay notes;
  readiness-concern flags (all audited).
- **Must be audited:** every readiness-time edit; every
  delay-note submission; every readiness-concern flag.
- **Must never be automated:** berth-readiness declaration.
  Horizon may estimate; humans declare.

### 3.7 Admin / system owner

- **Objective:** provision users, roles, ports, integrations;
  oversee platform configuration; review system health; manage
  data-retention and audit-export policies.
- **Primary screen / mode:** Admin Console (separate from
  operational modes). High-friction by design — explicit
  confirmations, frequent re-authentication, narrow scope per
  action.
- **Inputs:** user roster; role catalogue; port catalogue;
  integration credentials (handled per §6 secrets posture);
  retention policy; export requests.
- **Decisions / actions supported (M3+):** create / disable
  user; assign / revoke role; create / disable port; configure
  integration; rotate credentials; trigger audit export;
  retire data per retention policy.
- **Visible:** user / role / port / integration catalogues;
  system-health surface; audit-export status; **not**
  operational data unless the admin also holds an operational
  role (no silent over-privilege).
- **Writable (M3+):** all of the above admin objects.
- **Must be audited:** every admin action without exception;
  every credential rotation; every export; every retention
  trigger; every re-authentication.
- **Must never be automated:** user / role / port provisioning;
  credential rotation; data deletion.

### 3.8 Replay / audit user

- **Objective:** reconstruct the operational picture at a past
  timestamp for incident review, training, regulatory inquiry,
  or learning.
- **Primary screen / mode:** Replay / Audit Mode (Canon §8;
  UX Direction §3.4) — swimlane timeline + per-timestamp
  operational-state snapshot.
- **Inputs:** audit ledger; replay snapshots; selected
  time window; scope (port, role, incident ID).
- **Decisions / actions supported:** **none operational.**
  Replay is read-only. The user may take notes; notes are
  separate from the operational record.
- **Visible:** the operator-visible state at each replayed
  timestamp; the audit trail of operator actions; the data
  source provenance per displayed value.
- **Writable:** out-of-band review notes (kept in a separate
  review record). The replayed operational state itself is
  **immutable** — replay cannot edit history.
- **Must be audited:** every replay session (who replayed what
  window for what stated reason).
- **Must never be automated:** any change to the replayed
  state. Replay is observation, not editing.

---

## 4. Operational workflow sequence

The end-to-end flow Horizon V1 supports — from vessel detection
through audit / replay — is described conceptually below. Each
step has a clear data input, a clear actor, a clear visible
surface, and an explicit audit posture. **No step is implemented
or authorised by this document.**

### 4.1 Vessel detected / ingested
- **Trigger:** new AIS detection (AISStream / MST / port-authority
  feed) within the port bounding box, or a port-call notification
  from the port authority's existing system.
- **Actor:** automated ingest.
- **Visible:** vessel appears on the VTS spatial surface and in
  vessel lists.
- **Audit:** ingestion event recorded (vessel ID, source, time,
  port).
- **Notes:** ingestion is automatic; classification of the
  vessel as "in scope" (commercial transit, recreational, etc.)
  is deterministic and documented.

### 4.2 ETA established
- **Trigger:** AIS-derived ETA, agent-supplied ETA, or
  port-authority schedule entry.
- **Actor:** ingest + reconciliation (multiple sources may
  disagree; deterministic reconciliation rule chosen highest-
  confidence source with documented precedence).
- **Visible:** vessel ETA on Berth Timeline; ETA risk on the
  Coordination View; upstream notification to the assigned
  pilotage / towage coordinator if changes exceed a threshold.
- **Audit:** ETA-source-and-value snapshot at each material
  change.

### 4.3 Movement sequence created
- **Trigger:** vessel scheduled for an inbound / outbound /
  shift movement.
- **Actor:** automated based on schedule + pilotage / towage
  assignment (where present).
- **Visible:** the movement appears in Pilotage View, Towage
  View, Berth Timeline; rendered on the VTS spatial surface as
  a planned path.
- **Audit:** movement-record creation; subsequent edits.

### 4.4 Constraints evaluated
- **Trigger:** continuous evaluation on each data refresh.
- **Actor:** automated; deterministic rule set per
  port-handbook and COLREGS (no opaque ML in v1).
- **Constraints evaluated:** UKC against tide; LOA / draught
  against berth limits; bridge air-draught (where applicable);
  pilotage notice windows; tug bollard-pull adequacy;
  cyclone-season operational rules (Darwin Port Handbook 2026,
  for example); wind / swell thresholds.
- **Visible:** constraint state on the relevant surface;
  green / amber / red severity per Canon §3.
- **Audit:** constraint-state transitions are part of the
  replay snapshot stream; not separately auditable as operator
  actions because no human acted.

### 4.5 Conflict detected
- **Trigger:** constraint evaluation produces an unresolved
  conflict between two operational items (berth overlap,
  pilotage notice short, tug double-booking, ETA variance,
  bridge restriction, etc.).
- **Actor:** automated.
- **Visible:** as a card in the right rail (CONFLICT signal
  type), and (in M3+) overlaid on the centre-spine VTS surface.
- **Audit:** conflict-creation event captured; subsequent
  state changes captured.

### 4.6 Decision card generated
- **Trigger:** a CONFLICT-class conflict requires an operator
  decision.
- **Actor:** automated server-side (per detect_conflicts +
  decision-support construction logic, conceptually equivalent
  to the existing Beta 10 generator but architecturally
  separate in V1).
- **Visible:** the decision card appears in the right rail
  with full decision-support payload (recommended option,
  reasoning, confidence, deadline, alternatives).
- **Audit:** decision-card-creation event captured.

### 4.7 Recommendation reviewed
- **Trigger:** operator opens the decision card.
- **Actor:** the operator.
- **Visible:** the decision card detail surface; alternatives
  with cost / risk metadata; visible expiration / deadline.
- **Audit:** **read / presented** events captured per the
  existing recommendation-presented audit pattern (every
  read of a recommendation is auditable, separate from any
  action taken).

### 4.8 Operator acknowledges / defers / rejects / applies
- **Trigger:** the operator's decision input.
- **Actor:** the authenticated operator.
- **Decision types (M3+):**
  - **ACK** — acknowledged; no operational change.
  - **DEFER** — postponed with a reason and a re-prompt time.
  - **REJECT** — explicitly declined; reason captured.
  - **APPLY** — chosen alternative is recorded and downstream
    parties notified (notification mechanism per §4.10).
- **Visible:** the decision card updates its state; the
  Coordination View reflects the new state; the shift log
  records the action.
- **Audit:** **OPERATOR_ACTED** event captured with actor,
  timestamp, signed-cookie session, port, decision card ID,
  decision type, reason (if applicable), chosen alternative
  (if APPLY). The actor handle in the audit row matches the
  authenticated user; never a generic "demo user".

### 4.9 Recommendation outcome captured
- **Trigger:** completion of §4.8 with a non-ACK action.
- **Actor:** automated.
- **Visible:** outcome reflected in operational state; conflict
  marked resolved / deferred / rejected; downstream surfaces
  updated.
- **Audit:** outcome record linked back to the OPERATOR_ACTED
  event and the original decision card.

### 4.10 Downstream stakeholders notified
- **Trigger:** §4.8 APPLY action (and, for some decision
  classes, ESCALATE).
- **Actor:** automated dispatch through configured channels
  (in-app banner, email, SMS, integration callback to PMS,
  etc. — channels per port configuration).
- **Visible:** notification appears in the recipient's surface
  (e.g. pilot dispatcher sees the new assignment); a
  notification record is visible in the originating operator's
  view for confirmation.
- **Audit:** notification-dispatch event recorded (recipient
  scope, channel, payload digest — not full payload if PII
  considerations apply).

### 4.11 Audit record captured
- **Trigger:** any operator action; any system action with
  operational significance; any admin action.
- **Actor:** automated.
- **Visible:** the audit ledger is **not** routinely visible to
  operators; it is visible to Replay / Audit Mode users (§3.8)
  and to Admin (§3.7) with appropriate filtering.
- **Audit posture:** append-only; cryptographically chained
  (hash chain per the Phase 0.7 model); per-tenant genesis;
  no audit row is editable post-write; signing-key rotation per
  the platform security posture (§6).

### 4.12 Replay record preserved
- **Trigger:** any material operator-visible state change.
- **Actor:** automated.
- **Visible:** Replay / Audit Mode reconstructs the operator-
  visible state at any chosen timestamp.
- **Audit posture:** the replay snapshot stream is derived
  from the audit ledger + the operational data stream; it is
  immutable and is not a separate writable surface.

---

## 5. Platform architecture principles

These principles are **non-negotiable for V1 as a real
operational platform**. They apply to all V1 sandbox and future
production work. They do not retroactively apply to Beta 10
(Beta 10 is immutable).

### 5.1 API-first backend
- Every operationally meaningful surface is served by a typed,
  versioned API.
- The frontend (web, mobile, future iPad pilot view, future
  ECDIS interop) is **one of several** consumers of those APIs.
- No business logic is reachable only through the browser.

### 5.2 No business logic trapped in the frontend
- Conflict detection, decision-support construction, severity
  classification, constraint evaluation, audit emission, and
  authorisation **all live on the server side**.
- The frontend computes presentation (formatting, layout,
  hover state, mode selection) but never authoritative
  operational logic.

### 5.3 Frontend consumes APIs
- The web app (M1, M2, M3+) and any future mobile / pilot /
  ECDIS surface consume the same API surface (with appropriate
  authorisation scopes).
- API stability is a first-class concern. Versioning,
  deprecation, and change-management are owned by the
  platform team.

### 5.4 Typed domain models
- Domain entities (User, Port, Vessel, PortCall, Movement,
  Berth, Resource, Constraint, Conflict, DecisionCard,
  Recommendation, Notification, AuditEvent, ReplaySnapshot,
  IntegrationSource — see §8) are typed at the API boundary.
- Type definitions are generated from a single source of
  truth (e.g. OpenAPI / JSON Schema / typed IDL) and consumed
  by both backend and frontend.

### 5.5 Event-driven coordination where appropriate
- Operational events (ETA-changed, conflict-detected,
  decision-applied, notification-dispatched) flow through an
  internal event stream so multiple subscribers (audit,
  replay, integrations, UI push) can react independently.
- Events are durable, ordered per aggregate, and replayable.
- Event-driven is a **server-side architectural pattern** —
  it does not imply WebSocket push to every UI client by
  default; that is a separate UX decision.

### 5.6 Server-side authorisation
- Authorisation is enforced on the server, on every request,
  regardless of UI affordance.
- The UI may hide affordances based on role, but server-side
  enforcement is the authoritative check.
- "Hidden in UI" is **never** a sufficient security control.

### 5.7 Immutable audit records
- Audit rows are append-only.
- Audit rows are hash-chained (per-tenant genesis, signed
  chain) so any post-hoc tamper is detectable.
- Audit rows are written synchronously in the request path
  where operator intent is captured, with explicit recovery
  semantics if the audit write fails (the operation does
  **not** silently proceed without an audit record).

### 5.8 Clear separation of operational state, decision state,
and display state
- **Operational state** — the authoritative current state of
  vessels, berths, movements, resources, constraints. Server-
  owned; the source of truth.
- **Decision state** — the lifecycle of every decision card
  and recommendation (created, presented, acknowledged,
  deferred, applied, expired). Server-owned; auditable.
- **Display state** — the UI's current view (selected mode,
  selected tab, hovered item, filter state). Client-owned;
  not auditable except where it captures operator intent
  (e.g. selecting a port context).

### 5.9 Testable services
- Each service has a clear interface, deterministic outputs
  for deterministic inputs, and a test suite that exercises
  it independently of the UI.
- Test fixtures are checked in; live external feeds are not
  required for unit / integration tests.
- The pattern proven in M1 (fixture-fed pipeline; deterministic
  conflict generation; Vitest for adapters; regression gate
  for Beta 10 stability) is the precedent.

### 5.10 Integration-ready data boundaries
- External integrations (AIS, BoM, port authority PMS, pilot
  organisation, tug operator, terminal operator) consume the
  API surface or are consumed via a documented integration
  contract.
- No integration reaches into the operational store directly;
  every integration crosses a boundary that is owned,
  monitored, and auditable.
- Integration credentials are managed under §6 secrets
  posture, never embedded in code or fixtures.

---

## 6. Security and governance

### 6.1 Authenticated access
- Every operational endpoint requires authentication.
- Anonymous access is permitted **only** for clearly public,
  non-operational resources (e.g. health endpoint with no
  operational data).
- The Phase 1.1 auth hardening pattern (auth on what-if POSTs)
  is the precedent for what / api / write endpoints look like.

### 6.2 Role-based access control (RBAC)
- Authorisation is by role, mapped to scoped permissions.
- A user has zero or more roles, each with explicit
  permissions to read / act / admin within a defined scope.
- Scopes include **port** (per-port data isolation),
  **resource** (e.g. only this terminal's berths), and
  **action class** (e.g. ACK only, vs ACK / DEFER / APPLY).

### 6.3 Least privilege
- Users receive only the permissions their role requires.
- "Admin" is a separate concern from operational role; admins
  do not have silent operational read unless they hold an
  operational role too.
- Role assignment is auditable (§3.7).

### 6.4 Port-scoped access
- A user assigned to a single port sees only that port's data
  in operational views.
- Multi-port users (e.g. cross-port supervisors) have an
  explicit port-context switcher that records the
  port-context-change event (§3.1, audited).
- Cross-port data leakage is treated as a Sev-1 security
  defect.

### 6.5 Auditable user actions
- Every state-changing user action emits an audit row (§4.11,
  §5.7).
- Read actions on highly sensitive surfaces (Replay / Audit
  Mode, Admin Console) emit read-audit rows.
- Audit rows include actor, timestamp, session, port scope,
  action, target, before/after summary (where appropriate),
  outcome.

### 6.6 Secure session / token handling
- Sessions use HMAC-signed cookies (the existing `hz_sess`
  pattern is the precedent) or short-lived signed tokens.
- Session secret rotation is supported without invalidating
  the audit chain.
- Sensitive operations (admin, audit export) may require
  re-authentication within a short window.

### 6.7 No unauthenticated operational endpoints
- The Phase 1.1 hardening (auth on what-if POSTs) closed an
  existing gap. V1 must not reopen it; every operational
  POST / PATCH / DELETE / PUT requires auth at the application
  layer regardless of network perimeter.

### 6.8 Admin actions logged
- Every admin action (create user, assign role, rotate
  credential, configure integration, trigger export) is
  audited with a separate, more detailed audit category than
  operator actions.
- Admin actions also produce a real-time alert to a configured
  channel (so admin mis-use is detectable in near real time).

### 6.9 Sensitive operational data protected
- PII (pilot names, certificate numbers, master names where
  applicable) handled per §5.9 of the Pilot Proximity App note
  (de-identified / resource-safe patterns).
- Commercial sensitivity (cargo manifests, agent contracts)
  treated as restricted; visible only to roles with explicit
  scope.
- All data encrypted in transit (TLS) and at rest.
- Backups encrypted; backup access audited.

### 6.10 Future penetration / security review expected
- Before any live-client deployment, V1 must undergo a
  formal security review and external penetration test.
- Findings must be remediated to an explicit security bar
  (no Sev-1 / Sev-2 open at deployment; Sev-3 with named
  owner and remediation date).
- The Kyber boundary (cryptographic posture decision recorded
  in the existing Horizon programme materials) remains in
  force.

---

## 7. API surface expectations

High-level API **domains**. No implementation, no endpoint
signature, no schema authorised by this document.

| Domain | Purpose |
|---|---|
| **auth / users / roles** | Login, logout, session management, user provisioning, role assignment, port-scope mapping |
| **ports** | Port catalogue, port profile metadata (read-only for most operators; admin-writable) |
| **vessels** | Vessel master (name, IMO, MMSI, type, draught, owner / agent) and per-port-call state |
| **movements** | Inbound / outbound / shift movements; status; ETA / ATA / ETD / ATD; pilotage and towage linkage |
| **berths** | Berth catalogue; readiness time; current and next vessel; berth-side constraints |
| **resources** | Pilot roster; tug fleet; mooring gangs; their availability and assignments |
| **constraints** | UKC, LOA / draught, pilotage notice, bridge restrictions, wind / swell thresholds, port-rule references |
| **conflicts** | Detected conflicts; their lifecycle (open, acknowledged, deferred, applied, resolved, expired) |
| **decisions / recommendations** | Decision cards; decision support payloads; operator actions (ACK / DEFER / APPLY / REJECT / ESCALATE); recommendation outcomes |
| **notifications** | Outbound notification dispatch and delivery records |
| **audit / replay** | Audit-row read endpoints (scoped); replay snapshot retrieval; audit export |
| **integrations / ingest** | Integration source catalogue; ingest health; per-source freshness; integration credentials (admin-only) |
| **system health** | Liveness, readiness, version, build-id, feature flags (limited public surface) |

### 7.1 API stability commitments

- All operational APIs are versioned (semantic versioning;
  major-version compatibility window stated explicitly).
- Deprecation notice ≥ 90 days for any major-version cut.
- Internal-only APIs are clearly marked and excluded from
  the stability commitment.

### 7.2 What is explicitly NOT in the V1 API surface

- No vessel-command API. Horizon does not command vessels.
- No COLREGS-instruction API. Horizon does not issue
  COLREGS-class instructions.
- No public unauthenticated operational endpoint (other than
  health).
- No bulk-export of raw audit ledger via public API
  (admin-only, with explicit per-export audit row).
- No write-API into the audit ledger; the ledger is
  append-only via the platform's own logic, not via an
  external integration.

---

## 8. Data model expectations

Conceptual entities only. No schema, no field list, no
storage choice authorised by this document.

| Entity | Purpose |
|---|---|
| **User** | Identity record for a human operator, executive, or admin. Carries roles. |
| **Role** | Named permission bundle (e.g. VTSO, Harbour Master, Executive, Pilotage Coordinator, Terminal Operator, Admin). |
| **Port** | Physical port (Brisbane, Melbourne, Geelong, Darwin, future) with profile metadata. |
| **Vessel** | Vessel master record (name, IMO, MMSI, type, dimensions, owner / agent). |
| **PortCall** | One inbound + occupation + outbound cycle of a vessel at a port. Aggregates movements, berthing, services. |
| **Movement** | A single transit (inbound, outbound, shift) of a vessel with associated pilotage / towage. |
| **Berth** | Physical berth or terminal slot. Carries max LOA / max draught / readiness time / current occupant. |
| **Resource** | Pilot, tug, mooring gang. Carries availability, certification, and assignment state. |
| **Constraint** | A rule (UKC, LOA, pilotage notice, wind, etc.) evaluated against operational state. |
| **Conflict** | A detected violation or near-violation of one or more constraints involving one or more vessels / berths / resources. |
| **DecisionCard** | A CONFLICT-class conflict surfaced to an operator with decision-support payload. |
| **Recommendation** | A specific suggested action attached to a decision card (e.g. "Delay V006 by 90 minutes"). |
| **Notification** | An outbound message to a stakeholder (pilot dispatcher, tug operator, terminal, vessel agent, etc.). |
| **AuditEvent** | An append-only row recording a state change, operator action, system event, or admin action. |
| **ReplaySnapshot** | A reconstructable view of the operator-visible state at a given timestamp. |
| **IntegrationSource** | A configured external feed (AIS, BoM, PMS, etc.) with credentials, health, and freshness. |

### 8.1 Cross-entity relationships (informal)

- A `PortCall` aggregates `Movement` records.
- A `Movement` references a `Vessel`, zero or more `Berth`
  records (origin, destination), and one or more `Resource`
  assignments (pilot, tug, mooring gang).
- A `Conflict` references one or more entities (vessels,
  berths, resources, movements) and one or more `Constraint`
  rules.
- A `DecisionCard` references one `Conflict` and zero or
  more `Recommendation` items.
- An `AuditEvent` references the user actor (if applicable),
  the affected entity, and the operation type.
- A `ReplaySnapshot` is derived from `AuditEvent` stream +
  operational data at the timestamp.

### 8.2 Storage / persistence

- Storage choice (relational, document, hybrid) is **not**
  authorised by this document.
- Existing Beta 10 patterns are **not** a binding precedent
  where they conflict with secure platform design.
- The audit ledger requires append-only semantics with
  cryptographic chaining (per the Phase 0.7 model).

---

## 9. UX relationship

This document **honours and extends** the merged Operational
UX Direction (PR #58):

9.1 **Right rail = action / coordination surface.** The
operator workflows in §3 assume the right rail is the
persistent surface for decision cards, recommendations, ETD
risks, acknowledgements, sequencing issues, and coordination
tasks (UX Direction §3.1).

9.2 **Centre = VTS / spatial operational surface.** §3.1
(VTSO) and §3.4 (Pilotage Coordinator) assume the centre-spine
default is the spatial / VTS surface, not a KPI dashboard, for
those roles in M3+ (UX Direction §3.2).

9.3 **KPIs = supporting context.** The Executive View (§3.3)
is the only role profile where KPIs are the focal point. For
all other roles, KPIs are the persistent top-strip context
(UX Direction §3.3).

9.4 **Role-specific modes anticipated architecturally.** The
seven workflow profiles in §3 + Replay / Audit Mode in §3.8
correspond to the modes anticipated in UX Direction §3.4
(Executive / Coordination / VTSO / Pilotage / Incident /
Replay-Audit). Plus this document adds Pilotage Coordinator,
Towage Coordinator, Terminal Operator, and Admin as
operationally distinct profiles that may compose modes from
the UX Direction set rather than each requiring a unique mode.

9.5 **Canon §1.1.3 tab set preserved.** The six canonical
centre-spine tabs (Dashboard, Berth Timeline, Shift Log, VTS,
Pilotage, Performance) remain the canonical set. Mode selection
re-prioritises which tab is default per role; it does not add
new tabs.

9.6 **Pilot Proximity Companion App note (PR #57)**
relationship: the Pilotage Coordinator workflow (§3.4) and the
Pilotage View mode (UX Direction §3.4) are the operational
context that future Pilot Proximity work hangs off. This
document does not re-open or re-scope that note.

---

## 10. Non-negotiable boundaries

These are **hard constraints**, recorded so any future
proposal that would breach them is itself a stop condition.

10.1 **Do not touch Beta 10.** Per the Beta 10 Immutability
Rule. No feature / fix / UI / architectural / API / UX /
data-model / V1 / M2 / M3 work is promoted, merged,
cherry-picked, backported, or manually replicated into
Beta 10 (except critical demo-blocking defect with explicit
Tony approval).

10.2 **Do not backport V1 work to Beta 10.** Even when a V1
change would improve Beta 10. The improvement does not
justify the cross-contamination.

10.3 **Do not use Beta 10 as architecture precedent where it
conflicts with secure platform design.** Beta 10 is a
preserved demo build. Its architectural choices reflect a
demo posture, not a real platform posture. V1 architects on
principles, not on Beta 10 nostalgia.

10.4 **Do not create runtime / code changes from this
document.** This is a principles document. Implementation
requires its own scope proposal → implementation plan →
explicit Tony authorisation cycle.

10.5 **Do not modify M2 scope.** M2 (PR #53 + PR #54) ships
as planned. This document adds forward-looking direction; it
does not amend approved M2 acceptance criteria.

10.6 **Do not touch Railway, production, env vars, or
existing protected PRs (#27 / #28 / #29).**

10.7 **Do not implement anything.** No code, no scaffolds,
no migrations, no API endpoints, no schemas, no UI components
are authorised by this document.

10.8 **Do not modify the audit ledger model in production**
without a dedicated audit-evolution scope proposal. The
existing Phase 0.7 hash-chain model is the platform precedent.

10.9 **Do not introduce Smart Ocean X dependency framing.**
Independence / Architecture Reset (PR #51) framing is
preserved — Horizon / AMSG Horizon / Horizon by AMS Group
only.

10.10 **Do not present derived recommendations as if they
were COLREGS instructions or VTS directives.** Horizon is
decision support, not navigation authority (per PR #57's
established boundary).

10.11 **Do not automate decision execution.** Every
acknowledgement, defer, apply, reject, escalate is a human
action, surfaced by the UI and recorded in the audit ledger
with the authenticated user as actor.

10.12 **Do not weaken the regression gate.** The Beta 10
regression gate (`tests/test_beta10_regression.py`, 46/46)
remains the Beta 10 stability lock. V1 work does not modify
or weaken it.

10.13 **Do not skip the security review before live-client
deployment.** No live-client production deployment without
the §6.10 review and pen-test.

10.14 **Do not deploy V1 sandbox content to Beta 10
production.** `horizon-v1-sandbox` is a separate Railway
project from `Project-Horizon / production`. The two must
stay separate.

10.15 **Do not let "while we are here" fixes drift in.** Per
the Beta 10 Immutability Rule's working-mode guidance.

---

## 11. Open questions

These are deliberately deferred to subsequent scope proposals
and to Tony / maritime SME input. They are **not** blockers
for this document's acceptance.

### 11.1 Operating-model questions

11.1.1 Which port is the first **live-client target** for V1?
Brisbane (strongest data maturity), Darwin (now demo-verified),
or another?

11.1.2 What is the expected concurrent user count at the first
live port? 5? 25? 100?

11.1.3 What is the expected daily operational data volume
(events / decisions / audit rows / port-call records)?

11.1.4 Which integrations are **must-have for V1 live**
(AISStream? port-authority PMS? BoM? pilot organisation?) vs
**nice-to-have**?

### 11.2 Architecture questions

11.2.1 What is the storage choice for V1 (relational, document,
hybrid)? The audit chain implies relational + append-only;
operational state may have different needs.

11.2.2 What is the deployment platform for V1 production?
Railway is the current sandbox host; production may be a
different choice (AWS, Azure, GCP, AMSG-owned).

11.2.3 What is the event-stream substrate for §5.5? In-process
async, Kafka, NATS, Redis Streams, or other?

11.2.4 What is the API style — REST + JSON, GraphQL, or
hybrid? OpenAPI is the strong default but GraphQL has clear
benefits for multi-consumer (web, mobile, ECDIS) surfaces.

### 11.3 Security questions

11.3.1 What identity provider (IdP) does V1 use? Custom,
Auth0 / Clerk / Okta, port-authority SSO, or per-tenant
choice?

11.3.2 What is the secrets management posture? Cloud KMS,
HashiCorp Vault, Railway secrets, AMSG-owned secret store?

11.3.3 What is the audit-retention policy? Months, years,
indefinite? Legal advice required.

11.3.4 What is the data-residency requirement per port /
per customer? Australian-resident data is the assumed baseline
but may be customer-specific.

### 11.4 UX / workflow questions

11.4.1 What is the **default mode per role** at first sign-in
(UX Direction §7.1)?

11.4.2 What is the **mode-switch UX**? Visible toggle, role-
inferred only, or settings-only override?

11.4.3 How does **Incident Mode** trigger — manual operator
selection, automatic from a Sev-1 conflict, or both?

11.4.4 What **operator-action latency** is acceptable from
the user's perspective (e.g. ACK to next-screen-update)?

### 11.5 Audit / governance questions

11.5.1 Where do **out-of-band radio exchanges** get captured?
Manual operator note (§3.1)? Integration with port radio
recording (where available)? Both?

11.5.2 What is the **incident-record** lifecycle and who can
edit it?

11.5.3 What is the **audit-export workflow** for regulatory
inquiry and legal discovery?

11.5.4 How is **replay accuracy** validated (i.e. proving
that what Replay shows really matches what the operator saw
at that timestamp)?

### 11.6 Maritime SME questions

11.6.1 Does the workflow set in §3 match how port-coordination
roles actually operate in Brisbane / Melbourne / Geelong /
Darwin today?

11.6.2 Are the proposed audit categories (operator action,
admin action, integration health, replay session) sufficient
for regulatory and insurance contexts?

11.6.3 Is the proposed boundary between Horizon and VTS
(decision support, not traffic management) acceptable to the
relevant Harbour Master and VTS authority for each pilot port?

11.6.4 Which existing port-authority systems (PMS, statutory
reporting, customs, billing) must Horizon integrate with for
each pilot port?

---

## End of Operational Platform Workflows v0.1

**Status: forward-looking architecture / workflow document.
Not authorised for implementation.**

This document is principles and intent. It establishes the
operating model, workflows, architecture principles, security
posture, API and data-model expectations, UX relationship, and
non-negotiable boundaries that future V1 implementation work
must honour.

Confirmed by this document:
- **Beta 10 is excluded** under the Beta 10 Immutability Rule.
  No V1 work proposed here may touch Beta 10.
- **M0 / M1 production behaviour is unchanged.**
- **M2 scope (PR #53) and M2 implementation plan (PR #54) are
  not retroactively modified.**
- **The Component & Interaction Canon (PR #43) and the
  Operational UX Direction (PR #58) are honoured, not
  modified.**
- **The Independence / Architecture Reset (PR #51) framing is
  preserved** — no Smart Ocean X dependency framing.
- **No code, fixture, Railway, env-var, or infrastructure
  change is authorised by this document.**
- **No implementation is authorised by this document.**
- All future work targeting V1 / sandbox / future streams
  remains gated on explicit Tony authorisation per the
  established scope-proposal → implementation-plan → start
  pattern.

**Next action:** Tony's review. Optional ChatGPT engineering
review. If approved and merged, this document becomes the
forward-looking architecture / workflow constraint that M3+
scope proposals must cite (alongside the Operational UX
Direction).
