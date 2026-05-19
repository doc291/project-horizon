# Horizon V1 — User & Permission Model (v0.1)

**Status:** Design draft — V1 planning only
**Document version:** 0.1
**Date:** 2026-05-15
**Source:** `Horizon-V1-User-Roles.docx` (T. Trajceski, 2026-05-20, "1.0 — Draft")
**Implementation status:** None. **This is not implementation approval.**
**Audit ledger baseline:** Phase 0 `phase-0-complete @ 4ad4aae`; current
`main` `83af57d`.

---

## 1. Executive Summary

This document is the V1 role and permission foundation for Project
Horizon. It converts the draft user-roles material into a formal model
that can guide V1 architecture, RBAC implementation, UI/CX design,
audit semantics, API surface design, and database evolution.

V1 moves Horizon from a **single shared-login Beta 10 demo posture** to
a **role-scoped, auditable, production operating system** for port
operations. Each user account holds a primary role; each role sees a
filtered view of the same underlying operational data; each action that
changes state or signals authority lands in the audit ledger with the
user, role, scope, and reason attached.

The model formalises **six roles** ordered from operational to
strategic — VTSO, Harbour Master, Shift Supervisor, Port Executive,
Port Stakeholders, and Marine Infrastructure — and proposes a build
sequence that begins with VTSO (the role closest to Beta 10's existing
interface) and ends with Marine Infrastructure (likely a permission
within other roles before it becomes a standalone role).

**This document does not authorise any implementation.** It is the
v0.1 of an evolving model that will be reviewed, challenged, and
versioned before any V1 code is written. The Phase 0 Beta 10 baseline
remains protected at `phase-0-complete @ 4ad4aae`; nothing in this
document changes Beta 10 behaviour or the current production posture.

---

## 2. Design Principles

The following five principles govern every role, permission, scope,
and audit decision in V1. Where future implementation choices conflict
with these principles, the principle wins.

1. **See what you need, nothing more.**
   Each role receives a filtered view of the same underlying data
   model. Filtering happens server-side, not in the frontend. A role
   that does not need a vessel's UKC numbers, or a pilot's roster,
   does not receive them in the response payload.

2. **Act within your authority.**
   Actions are scoped to the role. A VTSO can resolve a conflict; only
   a Harbour Master can close the port. A Shift Supervisor can escalate
   a decision; only the Harbour Master can sign it off. Authority
   boundaries are explicit, encoded in permissions, and enforced at
   the API layer.

3. **Every operational action is auditable.**
   Acknowledgements, resolutions, overrides, escalations, defer-and-
   approve actions all land in the audit ledger with user identity,
   role, scope, and (where required) a reason code. The Phase 0
   audit infrastructure (`audit.events`, hash-chained per-tenant) is
   the foundation; V1 extends it to operator-side actions that did
   not yet exist in Beta 10.

4. **Role is not the same as permission.**
   A role is a primary operating mode (e.g. "Harbour Master"). A
   permission is a single authorised capability (e.g. `approve_port_
   closure`). Roles bundle permissions for ergonomics; permissions
   are the actual authorisation primitive. A user may eventually
   hold more than one role or carry permissions outside their
   primary role's bundle.

5. **Scope matters.**
   User access must be scoped by **port**, **tenant**, **shift**,
   **assignment**, or **operational authority** as appropriate. A
   Harbour Master in Melbourne does not see Darwin's data by default.
   A pilot does not see vessels they are not assigned to. A shift
   supervisor reviews actions taken during their shift, not since the
   beginning of recorded history. Scope is a first-class concept in
   the data model, not an afterthought.

---

## 3. Core Access Model

V1 introduces six foundational concepts. Each has a specific
responsibility; conflation between them is the most common source of
RBAC drift and must be avoided in V1 architecture.

| Concept | Definition |
|---|---|
| **User** | An individual account with authentication credentials. One human, one account. Shared logins are NOT permitted in V1. |
| **Role** | A primary operating mode. Six roles defined in §4. A user has at least one primary role; V1 begins with a single primary role per user but the data model must permit multiple. |
| **Permission** | A single authorised capability or action (e.g. `acknowledge_guidance`, `approve_port_closure`). Permissions are the atomic authorisation primitive. Roles are bundles of permissions. |
| **Scope** | The operational boundary in which a permission applies. Scope types include port, tenant, shift, assignment, and authority class. A permission without a scope is meaningless in V1. |
| **Tenant** | A customer or port-operating organisation. The audit ledger's `tenant_id` partition key (per Phase 0). Multi-tenant isolation is enforced at every API and DB layer. |
| **Session** | A bounded authenticated operating context. A session carries the user, the active role, the current scope (e.g. active port), and a session-id that lands on every audit event the user produces. |

### Key design decisions encoded in this model

- **Users may eventually need multiple permissions or scopes beyond
  their primary role's defaults.** V1 must NOT model permissions as a
  hardcoded property of role alone. The conceptual model is:
  `effective_permissions = primary_role.permissions ∪ explicitly_granted_permissions`
  scoped by `effective_scopes`. V1.0 may ship with a single-role-per-
  user constraint, but the underlying schema must permit the
  generalisation without migration when V1.x lifts that constraint.

- **A user's authority class is an attribute of their role, not a
  separate flag.** Authority class (`operator`, `supervisor`,
  `authority`, `executive`, `external`, `infrastructure`) is what
  the audit ledger's `actor_type` and `actor_handle` columns
  ultimately express.

- **The active role and scope at session-start are recorded on every
  audit event in that session.** Role-switching (if/when V1.x
  supports it) generates a new session-id, not a mutation of the
  active session.

---

## 4. Role Catalogue

The six V1 roles, listed in implementation-priority order. Each
profile maps to Horizon's existing `/api/summary` data model and to
the actions the role can take in V1.

### 4.1 VTSO (VTS Operator) — Build Priority 1

**Purpose.** Primary operational user. Real-time vessel management,
conflict resolution, weather response. The always-on, heads-down role
— closest to what Horizon Beta 10 already provides. The bulk of V1 UI
investment will focus here.

**Usage pattern.** Always-on, typically per shift, multiple concurrent
sessions across a watch.

**Data visibility.**
- **Full:** vessels, berths, conflicts, guidance, weather, tides,
  pilotage, towage, UKC, arrival UKC, dukc, etd_risk
- **Read only:** dashboard
- **Not visible:** ESG (executive concern), historical trend reports

**Actions allowed.**
- Acknowledge a guidance item
- Resolve a conflict (select a resolution option, add reasoning)
- Run a what-if scenario (shadow simulation, no live state mutation)
- Request a pilot or tug for an upcoming movement
- Flag a situation for the Harbour Master (escalation)
- Add free-text resolution notes to a conflict

**Actions NOT allowed.**
- Apply a what-if scenario to live state (requires Harbour Master
  authorisation in V1)
- Approve a port closure
- Override a berth assignment system-wide
- Sign off the Port Brief
- Review audit history beyond the current shift
- Administer users

**Audit implications.**
- Every VTSO action generates an `OPERATOR_ACTED` event with
  `actor_type='operator'`, the operator's handle, and the resolution
  reasoning in the payload.
- Acknowledgements of guidance generate `OPERATOR_ACKNOWLEDGED`.
- Escalations to the Harbour Master generate a specific event type
  (likely a new `OPERATOR_ESCALATED` — to be added to the closed set
  in V1.x).

**V1 build priority.** 1 (first).

**Open design questions.**
- Should a VTSO have a "draft resolution" state that the Shift
  Supervisor reviews before commitment, or are resolutions immediately
  binding?
- How does the VTSO see what the previous VTSO did at handover —
  is the shift log built into the VTSO console or only the Supervisor's?
- For multi-port deployments, can a VTSO log in once and see multiple
  ports they are watching, or is one session = one port?

### 4.2 Harbour Master — Build Priority 2

**Purpose.** Full visibility with drill-down on demand. The authority
figure for port closures, weather holds, and non-standard vessel
movements. Their decisions carry legal weight — this is where the
audit trail matters most.

**Usage pattern.** Daily or as-needed; on-call. Drills into specific
situations rather than the always-on console.

**Data visibility.**
- **Full:** vessels, berths, conflicts, guidance, weather, tides,
  pilotage, towage, UKC, dukc, etd_risk, ESG, dashboard
- **Read only:** none — Harbour Master sees everything
- **Not visible:** none

**Actions allowed.**
- Approve a port closure (formal decision with reason and duration)
- Override a berth assignment (reassign vessel against the system
  recommendation)
- Approve a what-if scenario for application to live state
- Sign off the Port Brief
- Review the full audit trail of VTSO and Shift Supervisor actions
- All VTSO actions (acknowledge, resolve, run what-if, flag, notes)

**Actions NOT allowed.**
- Confirm assignments as a pilot/towage stakeholder (different
  operational role)
- Report delays as an end-user stakeholder
- Administer users (separate tenant-admin permission, not role-derived
  in V1; possibly granted by separate permission)

**Audit implications.**
- Every approval generates `OPERATOR_ACTED` with `actor_type='authority'`
  (new authority-class actor type proposed for V1; closed set
  extension required).
- Port closures and overrides require a structured reason code in the
  payload (not free text alone).
- Sign-off events on the Port Brief generate a specific event
  (proposed: `OPERATOR_SIGNED_OFF`).
- Audit-trail reads by the Harbour Master generate a meta-audit event
  (`AUDIT_READ` — proposed) so the ledger records who reviewed what.

**V1 build priority.** 2.

**Open design questions.**
- Are approvals always immediate, or can the Harbour Master have
  "pending approval" workflow with the Shift Supervisor doing the
  preparatory work?
- Should there be a dual-authority requirement for port closures
  (HM + a deputy)?
- Audit retention for HM actions: is it tenant-default, or a longer
  regulatory retention class?

### 4.3 Shift Supervisor / Watch Lead — Build Priority 3

**Purpose.** Manages shift handovers, escalates to the Harbour
Master, and reviews VTSO actions before they reach authority-level
attention. The missing link between the operational floor and the
authority layer.

**Usage pattern.** Per shift (typically 8–12 hours). Active during
handover windows and on demand within the shift.

**Data visibility.**
- **Full:** vessels, conflicts, guidance
- **Read only:** dashboard, weather, tides, dukc detail, pilotage,
  towage, berths
- **Not visible:** ESG (executive concern); detailed audit history
  beyond their shift window

**Actions allowed.**
- Write a structured shift handover note (events, pending items,
  decisions, outstanding escalations)
- Escalate an issue formally to the Harbour Master
- Review the queue of VTSO actions since the last handover

**Actions NOT allowed.**
- Approve port closures (HM only)
- Override berth assignments (HM only)
- Apply or approve what-if scenarios (HM only)
- Sign off the Port Brief
- Administer users

**Audit implications.**
- Shift handover writes generate `SHIFT_HANDOVER_WRITTEN` (new event
  type proposed for V1.x).
- Escalations generate `OPERATOR_ESCALATED` (proposed) and link the
  source event(s) being escalated.
- VTSO-action reviews are pure reads but generate `AUDIT_READ` meta-
  events so the chain shows the review actually happened (provides
  evidence of supervisory oversight).

**V1 build priority.** 3.

**Open design questions.**
- In smaller ports the Harbour Master often IS the Shift Supervisor.
  Should V1 allow a user to hold both roles, or model this as the HM
  inheriting all SS permissions by default?
- What is the canonical "shift" boundary — wall-clock window, login
  session, or an explicit start-shift action?
- Can a Shift Supervisor delegate the handover write to a VTSO with
  HM approval, or must they write it themselves?

### 4.4 Port Executive — Build Priority 4

**Purpose.** Strategic oversight. Weekly or periodic check-in on port
performance, trends, and exceptions. Nothing on the Executive screen
should require immediate action — if it does, it belongs to another
role.

**Usage pattern.** Weekly / periodic. Mobile-friendly summary; not
designed for sustained operational use.

**Data visibility.**
- **Full:** dashboard (KPIs), berth utilisation, ESG
- **Read only:** weather (conditions only), trend reports, individual
  vessels (anonymised or de-identified where appropriate),
  incident summaries (aggregated audit trail)
- **Not visible:** real-time conflicts, guidance, pilotage detail,
  towage detail, UKC/dukc, etd_risk

**Actions allowed.**
- View the Port Brief (PDF or web preview)
- View trend reports (throughput 7/30/90d, dwell time averages,
  incident summaries, berth utilisation over time)

**Actions NOT allowed.**
- Anything operational. No acknowledge, no resolve, no escalate.
- Sign off the Port Brief (HM does that; Executive consumes it).
- Administer users.

**Audit implications.**
- Executive reads generate `AUDIT_READ` meta-events for trend reports
  and Port Brief views — provides evidence of executive oversight
  for governance.
- Executive role does NOT emit operational action events because the
  role cannot take operational actions.

**V1 build priority.** 4. (Trend calculations need accumulated
historical data; the longer V1 has been live, the more valuable this
view becomes.)

**Open design questions.**
- What anonymisation / de-identification does the Executive view need
  for individual vessel records, particularly for commercial sensitivity
  with line operators?
- Should Executive reports be self-serve or generated by Harbour
  Master sign-off?
- Multi-port executives (port group): does one Executive role span
  multiple ports, or does each port have its own Executive?

### 4.5 Port Stakeholders (Pilots, Towage, Mooring) — Build Priority 5

**Purpose.** Task-specific, mobile-first view. These users answer
one question: "What is my next job and what do I need to know?"

**Usage pattern.** Per assignment; intermittent; mobile-dominant.

**Data visibility.**
- **Scoped (assigned only):** vessels, berths
- **Scoped (own assignments):** pilotage, towage
- **Read only:** weather, tides
- **Not visible:** conflicts, guidance, dashboard, ESG, dukc,
  etd_risk, full vessel roster, other stakeholders' assignments

**Actions allowed.**
- Confirm receipt of a pilot, towage, or mooring assignment
- Report a delay or issue with a vessel movement (feeds into VTSO's
  guidance panel)

**Actions NOT allowed.**
- Anything beyond their own assignment scope.

**Audit implications.**
- Assignment confirmations generate `OPERATOR_ACTED` with
  `actor_type='stakeholder'` (proposed new actor type for the closed
  set) and the stakeholder handle.
- Delay reports generate a specific event (proposed:
  `STAKEHOLDER_REPORTED_DELAY` or reuse `OPERATOR_ACTED` with action
  type `report_delay`). To be decided in V1.x design.

**V1 build priority.** 5.

**Open design questions.**
- **Kyber boundary:** for pilots specifically, the assignment workflow
  (availability, rostering, sequencing) is handled by Kyber. Horizon's
  stakeholder role is the broader port-context feed Kyber consumes,
  not a replacement for Kyber's pilot UX.
- Authentication: do stakeholders authenticate against Horizon
  directly, or via a federated identity with Kyber / a port-issued
  identity? V1.0 probably uses Horizon-direct auth; V1.x may
  federate.
- Mobile-first design implications for offline operation and message
  delivery (push, SMS, in-app).

### 4.6 Marine Infrastructure / Maintenance — Build Priority 6

**Purpose.** Manages berth closures, crane schedules, dredging
windows, and infrastructure outages. In Beta 10 these are hard-coded
in `port_profiles.py`. V1 needs a surface for updating berth
availability in real time without code changes.

**Usage pattern.** As-needed; not always-on. Likely shared with a
small maintenance team.

**Data visibility.**
- **Full:** berths
- **Read only:** vessels (berthed only), weather, tides
- **Not visible:** conflicts, guidance, pilotage, towage, dashboard,
  ESG

**Actions allowed.**
- Set a berth unavailable (with expected return time and reason)
- Schedule a maintenance window (future berth closure)
- Update berth readiness (confirm a berth is back in service)

**Actions NOT allowed.**
- Anything operational beyond berth availability.

**Audit implications.**
- Berth availability changes generate a new event type (proposed:
  `BERTH_AVAILABILITY_CHANGED`) with the actor handle, reason, and
  the affected berth ID.
- This event must be visible to VTSO and Harbour Master so the
  operational console can render the availability change as a
  guidance item.

**V1 build priority.** 6 (last).

**Open design questions.**
- **Role or permission?** This is the central open question. In small
  ports, berth-availability management is likely a permission carried
  by the Harbour Master or Shift Supervisor, not a standalone login.
  The recommendation in §14 is to ship V1.0 with `manage_berth_
  availability` as a permission addable to other roles, and create a
  standalone Marine Infrastructure role only when the maintenance
  workflow's complexity justifies it.
- Does Marine Infrastructure need its own authentication, or share
  an HM/SS login with the permission granted?
- Notification model: who is alerted when a berth is set unavailable
  during a vessel approach?

---

## 5. Permission Matrix

The following matrix maps key V1 permissions to roles. Values are:

- **Full** — permission granted across the role's scope
- **Read only** — visibility but not action
- **Scoped** — granted within a narrower scope than the role's default
- **No** — not granted

| Permission | VTSO | Harbour Master | Shift Supervisor | Port Executive | Port Stakeholders | Marine Infrastructure |
|---|---|---|---|---|---|---|
| view_dashboard | Read only | Full | Read only | Full | No | No |
| view_vessels | Full | Full | Full | Read only | Scoped | Read only |
| view_berths | Full | Full | Read only | No | Scoped | Full |
| view_conflicts | Full | Full | Full | No | No | No |
| view_guidance | Full | Full | Full | No | No | No |
| view_weather_tides | Full | Full | Read only | Read only | Read only | Read only |
| view_pilotage | Full | Full | Read only | No | Scoped | No |
| view_towage | Full | Full | Read only | No | Scoped | No |
| view_audit_trail | No | Full | Scoped | Read only | No | No |
| acknowledge_guidance | Full | Full | No | No | No | No |
| resolve_conflict | Full | Full | No | No | No | No |
| run_what_if | Full | Full | No | No | No | No |
| apply_what_if | No | Full | No | No | No | No |
| approve_what_if | No | Full | No | No | No | No |
| escalate_to_harbour_master | Full | No | Full | No | No | No |
| approve_port_closure | No | Full | No | No | No | No |
| override_berth_assignment | No | Full | No | No | No | No |
| write_shift_handover | Scoped | Full | Full | No | No | No |
| review_vtso_actions | No | Full | Full | No | No | No |
| sign_off_port_brief | No | Full | No | No | No | No |
| confirm_assignment | No | No | No | No | Full | No |
| report_delay_issue | Scoped | Full | Full | No | Full | Scoped |
| manage_berth_availability | No | Full | No | No | No | Full |
| view_trend_reports | No | Full | Read only | Full | No | No |
| administer_users | No | Scoped | No | No | No | No |

### Matrix notes

- **`escalate_to_harbour_master` is `No` for the Harbour Master**:
  the HM is the escalation target, not an escalator.
- **`administer_users = Scoped` for Harbour Master**: V1.0 likely
  grants tenant-admin to a small set; the HM is the candidate
  default. May be split into a separate `TenantAdmin` permission set
  in V1.x.
- **`write_shift_handover = Scoped` for VTSO**: VTSOs can contribute
  notes within their session that the Shift Supervisor consolidates
  into the handover; only the Supervisor writes the canonical
  handover document.
- **`report_delay_issue = Scoped` for VTSO and Marine Infrastructure**:
  they can report delays within their own operational surface; the
  formal "report delay" stakeholder action is `Full` for Port
  Stakeholders because that is their primary operational verb.
- **`view_trend_reports = Read only` for Shift Supervisor**: SS can
  see handover-level trends but not full executive aggregates.

---

## 6. Action Authority Model

V1 introduces a structured action-authority vocabulary. Each verb
below has a defined semantic, a role-permission mapping, and a target
audit event type.

| Verb | Semantic | Allowed roles | Future audit event |
|---|---|---|---|
| **acknowledge** | Mark an item as seen and noted | VTSO, HM | `OPERATOR_ACKNOWLEDGED` (new) |
| **resolve** | Select a resolution option for a conflict; record reasoning | VTSO, HM | `OPERATOR_ACTED` (action_type: `conflict_resolve`) |
| **escalate** | Hand an item to a higher authority for decision | VTSO, SS | `OPERATOR_ESCALATED` (new) |
| **approve** | Authorise an action requiring HM-level authority | HM | `OPERATOR_ACTED` (action_type: e.g. `port_closure_approve`, `whatif_apply_approve`) |
| **override** | Authorise a deviation from system recommendation | HM | `OPERATOR_OVERRODE` (already reserved; deferred per `phase0-008b-deadline-passed-deferral.md`) |
| **defer** | Acknowledge but explicitly choose not to act now | HM (V1.0); VTSO (V1.x) | `OPERATOR_DEFERRED` (already reserved; deferred) |
| **close** | Terminate an active operational state (port closure) | HM | `OPERATOR_ACTED` (action_type: `port_close`) |
| **sign off** | Authorise a periodic artefact (Port Brief) | HM | `OPERATOR_SIGNED_OFF` (new) |
| **report** | Surface an issue or delay to the operational layer | Stakeholders, VTSO, MI | `STAKEHOLDER_REPORTED_*` (new) or `OPERATOR_ACTED` |
| **review** | Read audit trail (e.g. VTSO actions since handover) | SS, HM | `AUDIT_READ` (new meta-event) |

### Connection to deferred Phase 0 events

The Phase 0 closure (`phase0-008b-deadline-passed-deferral.md`)
explicitly deferred the following event types pending V1's
recommendation action surface:

- `DEADLINE_PASSED`
- `SESSION_ENDED_WITHOUT_ACTION`
- `OPERATOR_DEFERRED`
- `OPERATOR_OVERRODE`

This document **provides the action surface those events require**:

- `OPERATOR_DEFERRED` is emitted by the **defer** verb (HM in V1.0).
- `OPERATOR_OVERRODE` is emitted by the **override** verb (HM).
- `DEADLINE_PASSED` becomes meaningful once the recommendation
  lifecycle (§7) reaches "presented" with a pinned deadline and no
  `acknowledge`/`resolve`/`defer`/`escalate` action against it by
  the deadline.
- `SESSION_ENDED_WITHOUT_ACTION` is emitted by `session_audit` when a
  session ends with outstanding presented recommendations that
  received no operator action — requires the V1 lifecycle to track
  per-session pending items.

**These events remain deferred until V1.0 ships the recommendation
action surface (Phase V1.1 — VTSO).** V1.0 may stage the schema-side
groundwork (extending the closed set, defining the dedup keys) without
emitting them.

---

## 7. Recommendation Lifecycle Implications

V1 introduces a formal lifecycle for each recommendation. Beta 10
ended at "presented." V1 extends to acknowledgement, resolution,
deferral, override, escalation, and review.

| Stage | Who can see | Who can act | Audit event | Data required |
|---|---|---|---|---|
| **detected** | VTSO, HM, SS | none (system-only) | `CONFLICT_DETECTED` (Phase 0.7a, live) | conflict object |
| **generated** | VTSO, HM, SS | none (system-only) | `RECOMMENDATION_GENERATED` (Phase 0.7b, live, with §1.4.1 snapshot) | conflict + decision_support + snapshot |
| **presented** | VTSO, HM, SS | VTSO, HM | `RECOMMENDATION_PRESENTED` (Phase 0.7c, live, scoped to surface=`api_summary`) | recommendation_id, surface, actor_handle, displayed_at |
| **acknowledged** | VTSO, HM, SS | VTSO, HM | `OPERATOR_ACKNOWLEDGED` (new in V1) | recommendation_id, actor_handle, scope, reason (optional) |
| **accepted / resolved** | VTSO, HM, SS | VTSO, HM | `OPERATOR_ACTED` (action_type: `conflict_resolve`) (Phase 0.8a partially; V1 broadens) | recommendation_id, chosen_option_id, reason, scope |
| **deferred** | VTSO, HM, SS | HM (V1.0); VTSO (V1.x) | `OPERATOR_DEFERRED` (Phase 0 reserved; V1 emits) | recommendation_id, defer_until, reason (required), actor_handle |
| **overridden** | VTSO, HM, SS | HM | `OPERATOR_OVERRODE` (Phase 0 reserved; V1 emits) | recommendation_id, override_choice, reason (required), actor_handle |
| **escalated** | VTSO, HM, SS | VTSO, SS (escalate to HM) | `OPERATOR_ESCALATED` (new) | recommendation_id, source_actor, target_authority, reason |
| **closed** | VTSO, HM, SS | HM (closes the lifecycle) | `RECOMMENDATION_CLOSED` (new) or `CONFLICT_RESOLVED` (already reserved) | recommendation_id, closure_reason, actor_handle |
| **reviewed** | HM, SS | SS, HM | `AUDIT_READ` (new meta-event) | reviewed_recommendation_id, reviewer_actor, review_window |

### Lifecycle invariants

- A recommendation transitions to **closed** only after one of:
  resolved, deferred-to-expiry, overridden, or escalated-and-acted-on.
- A recommendation that reaches its `decision_deadline` (from the
  Phase 0.7b snapshot) without progressing past **presented** emits
  `DEADLINE_PASSED` — at that point operator inaction becomes part of
  the evidentiary record.
- A session that ends with one or more recommendations still in
  **presented** state without any action emits
  `SESSION_ENDED_WITHOUT_ACTION` — operator did not engage with the
  pending decision.
- All lifecycle transitions are append-only on the audit chain (no
  state mutation, no row updates).

---

## 8. API Implications

V1 will require a structured API surface. The following endpoints are
**conceptual** — this document does NOT authorise their implementation.
Names are subject to change. The shape and scope-filtering semantics
are the load-bearing properties.

### 8.1 Role-scoped `/api/summary`

The existing `/api/summary` endpoint stays, but **server-side
filtering** by role becomes mandatory:

- The session's role determines which top-level keys appear in the
  response
- Within `vessels`, `pilotage`, `towage`, and `berths`, scope
  filtering applies (e.g. a stakeholder sees only their assigned
  records)
- Sensitive operational data (full ETAs, free-text decision support,
  full UKC numbers) is not returned to roles that don't have
  `view_*` permission for it

### 8.2 New action endpoints

- `POST /api/v1/actions/acknowledge` — VTSO, HM
- `POST /api/v1/actions/resolve` — VTSO, HM
- `POST /api/v1/actions/escalate` — VTSO, SS
- `POST /api/v1/actions/defer` — HM (V1.0); VTSO (V1.x)
- `POST /api/v1/actions/override` — HM

Each endpoint accepts `recommendation_id` (or `conflict_id`),
`reason`, optional `notes`, and inherits actor/role/scope from the
session. Each one emits an audit event per §6.

### 8.3 Shift & handover

- `GET  /api/v1/shifts` — SS read full; VTSO read scoped to own shift
- `POST /api/v1/shifts/handover` — SS write
- `POST /api/v1/shifts/note` — VTSO contributes a note within the
  active shift

### 8.4 Audit read

- `GET /api/v1/audit` — HM full; SS scoped to own shift; Executive
  reads aggregated incident summaries only
- All reads emit `AUDIT_READ` meta-events so the chain captures who
  reviewed what

### 8.5 Briefs

- `GET  /api/v1/briefs/current` — HM, SS, Executive read
- `POST /api/v1/briefs/{id}/signoff` — HM only
- `POST /api/v1/briefs/{id}/distribute` — HM only (subject to
  sign-off precondition)

### 8.6 Berth availability

- `GET  /api/v1/berths/availability` — all operational roles
- `POST /api/v1/berths/{id}/unavailable` — MI permission holders
- `POST /api/v1/berths/{id}/schedule-maintenance` — MI permission
  holders
- `POST /api/v1/berths/{id}/ready` — MI permission holders

### 8.7 Critical filtering invariants

- **Server-side enforcement.** Frontend filtering is never sufficient.
  Every response that includes data must be filtered against the
  session's role and scope at the API layer, NOT trusted to client-
  side rendering choices.
- **Deny by default.** Any permission not explicitly granted is
  denied. New permissions must default to `No` for all existing
  roles.
- **No role inheritance shortcuts.** Higher-authority roles do not
  automatically inherit all lower-authority permissions. Each
  permission must be explicitly mapped. (This avoids accidental
  privilege bleed when a new permission is added.)

---

## 9. Database Implications

V1 introduces a structured user/RBAC schema. The conceptual model
below is NOT implementation — names, types, and relationships are
subject to design review. The load-bearing constraints are:
multi-tenant isolation, append-only audit posture, and explicit
scoping.

### 9.1 Core RBAC tables

| Table | Purpose | Notes |
|---|---|---|
| `users` | individual accounts with auth credentials | tenant_id FK; never shared logins |
| `roles` | catalogue of role definitions | seeded with the six V1 roles |
| `permissions` | catalogue of permission identifiers | seeded with the 25+ permissions in §5 |
| `role_permissions` | which permissions a role bundles | many-to-many; modifiable per tenant in V1.x |
| `user_roles` | which roles a user holds | one-to-many in V1.0; can extend to many-to-many in V1.x |
| `user_scopes` | which scopes apply per user | port_id, tenant_id, authority class, optional shift restriction |

### 9.2 Operational tables

| Table | Purpose | Notes |
|---|---|---|
| `shifts` | shift definition: start/end times, supervisor user_id | per-port, possibly per-team |
| `shift_handover` | structured handover notes per shift | written by Supervisor; readable per §5 |
| `recommendation_actions` | per-action records: recommendation_id, user_id, role, action_type, reason, timestamp | append-only; foreign key to recommendations and to audit.events |
| `action_notes` | free-text annotations on actions | scoped to action; preserves operator context for audit |
| `berth_availability_events` | append-only log of berth state changes | feeds the `manage_berth_availability` action and renders availability into operational view |
| `stakeholder_assignments` | which stakeholder is assigned to which vessel/movement | feeds the scoped visibility for stakeholders |

### 9.3 Audit linkage

The Phase 0 `audit.events` table gains additional payload
expectations in V1. The schema does not change at the column level
(the closed-set CHECK constraints expand to include new event types
in V1, but the rest is stable).

Every audit row in V1 has, in its payload:

- `user_id` (resolved from session)
- `role` (active role for this session)
- `scope` (active port_id, tenant_id, shift_id where applicable)
- `session_id` (the bounded session that originated this action)

These are payload-level additions, not schema-level. The Phase 0
audit writer accommodates this without migration changes.

### 9.4 Tenant isolation

All user/RBAC/operational tables are tenant-scoped. The audit ledger
remains per-tenant partitioned. **No cross-tenant data access is
permitted by any role**, including authority and executive roles. If
a Harbour Master operates across multiple ports owned by the same
tenant, that is in-tenant scope expansion, not cross-tenant access.

---

## 10. Frontend / UX Implications

Each role gets a distinct frontend experience. The CX/design work
itself happens in a separate stream (Claude Design / external design
review) and will consume this document as authoritative for the role
catalogue, permission matrix, and lifecycle.

### 10.1 VTSO — operational console

- The closest descendant of Beta 10's current UI
- Always-on, multi-panel layout (vessels, berths, conflicts,
  guidance, weather, tides)
- Action affordances: acknowledge, resolve, run what-if, escalate
- Conflict detail drawer with resolution options + reason field
- Live updates (polling or WebSocket; TBD)
- Designed for desktop primarily; tablet acceptable; mobile is not
  the primary target

### 10.2 Harbour Master — authority & review console

- Same data as VTSO but with authority actions visible:
  approve / override / sign off / close port
- Audit-trail navigator: search/filter/replay any user's actions
  within their authority scope
- Decision panel for what-ifs that require HM approval
- Port Brief preview + sign-off control
- Designed for desktop with mobile-friendly summary fallback

### 10.3 Shift Supervisor — handover & review console

- Structured handover composer: pending items, decisions made,
  outstanding escalations
- VTSO action review queue (filtered by shift window)
- Escalation history log
- Read-only operational dashboard
- Designed for desktop primarily

### 10.4 Port Executive — trend & exception dashboard

- KPI tiles: throughput, dwell time, berth utilisation, incident
  counts
- Trend charts (7/30/90 day rolling)
- Port Brief access (read-only)
- Incident summary panel (aggregated audit data)
- Designed for mobile and tablet primarily; desktop secondary

### 10.5 Port Stakeholders — mobile assignment feed

- Mobile-first single-screen view
- Next assignment / current assignment
- Weather / tide for assignment window
- Confirm assignment button (primary CTA)
- Report delay / issue button (secondary CTA)
- Designed mobile-only initially; desktop view is a courtesy

### 10.6 Marine Infrastructure — berth availability surface

- Berth list with status (available / occupied / maintenance / closed)
- Set-unavailable action with reason and expected return time
- Schedule-future-maintenance action with start/end times
- Update-readiness action when a berth returns to service
- Designed for desktop and tablet

### 10.7 Cross-role UX invariants

- **Role badge in the header** — every screen displays the active
  role so users never confuse which authority they're acting under
- **Active scope indicator** — port, tenant, and (if applicable)
  shift visible at all times
- **Action affordances respect permissions** — buttons for actions a
  role cannot take MUST NOT appear at all; greyed-out is not
  acceptable
- **Reason capture is mandatory for approvals, overrides, defers,
  and escalations** — the UI must enforce reason entry; the audit
  payload requires it

---

## 11. Kyber Boundary

Horizon and Kyber are **separate products with separate codebases**.
This boundary is explicit and must not erode in V1.

### What Kyber owns

- Detailed pilot rostering and assignment
- Pilot availability tracking
- Pilot-side job sequencing and acknowledgement workflow
- Pilot-specific mobile experience and operational tooling
- Pilot competency, training, and credential tracking

### What Horizon V1 may show

- Role-scoped pilotage context (which vessel, which berth, ETA window)
- Assignment status flags (assigned / confirmed / in-progress /
  completed) at the operational level
- Pilot identity at the de-identified/capability-based level (e.g.
  "Pilot A — qualified for this port + LOA class") rather than full
  rostering detail
- The broader port context Kyber's pilot users need but Kyber itself
  does not generate (weather, tides, conflict state, berth readiness)

### What Horizon V1 must NOT do

- Depend on Kyber source code or in-process Kyber library imports
- Mirror Kyber's roster state inside Horizon's database (Kyber is the
  system of record for rostering)
- Merge product UX boundaries — pilot users should know whether
  they're in Kyber or Horizon at any moment

### Integration model

When integration becomes warranted (V1.x at earliest), it is
**API/event based**:

- Kyber publishes assignment events that Horizon consumes for
  display
- Horizon publishes operational events (conflict detected, weather
  hold, schedule change) that Kyber consumes for pilot-side
  awareness
- Authentication is federated, not shared (each system manages its
  own session)
- De-identified / capability-based resource model: external IDs and
  capability descriptors, not internal pilot identities

V1.0 does not implement any Kyber integration. The integration
contract is a V1.x design conversation that begins after V1.0 ships.

---

## 12. Build Sequencing Recommendation

The build sequence below is **planning only**. Each phase requires
its own explicit authorisation before implementation begins. Phase
boundaries match the role priorities in §4 (1 → 6).

### V1.0 — RBAC foundation & role-scoped summary

- Users / roles / permissions / scopes schema
- Session model with role + scope claims
- `/api/summary` server-side filtering by role + scope
- Login flow extended to user/password (replaces Beta 10's shared
  HORIZON_USER/HORIZON_PASS env-var auth)
- Tenant + port-scope enforcement at every API entrypoint
- No new operational actions yet; deliverable is the
  authentication/authorisation skin around Beta 10's read surface

### V1.1 — VTSO action surface

- VTSO console (operational layout)
- Acknowledge / resolve / run-what-if / escalate actions
- Action audit emission (`OPERATOR_ACKNOWLEDGED`, etc.)
- Resolution notes capture
- Recommendation lifecycle stages: presented → acknowledged → resolved
- Closed-set CHECK constraint extension on `audit.events` for the
  new event types

### V1.2 — Harbour Master approval & audit review

- Harbour Master console
- Approval / override / sign-off / port-closure actions
- Audit-trail review surface (search, filter, drill-down)
- `OPERATOR_OVERRODE`, `OPERATOR_ACTED` with approval action types
- `AUDIT_READ` meta-event emission

### V1.3 — Shift handover & escalation

- Shift definition (start/end, supervisor user_id)
- Shift Supervisor console
- Structured handover composer + read
- Escalation workflow VTSO → SS → HM
- `OPERATOR_ESCALATED`, `SHIFT_HANDOVER_WRITTEN`
- VTSO action review queue scoped to shift window

### V1.4 — Executive trends

- Trend-data accumulation infrastructure (relies on V1.0–V1.3 data
  having been captured for long enough — likely 30+ days post-V1.3)
- Executive dashboard with KPIs and trend charts
- Port Brief preview and read access
- Aggregated incident summary

### V1.5 — Stakeholder scoped feed

- Stakeholder authentication (Horizon-direct in V1.0; federation
  decisions deferred)
- Mobile-first stakeholder UI
- Scoped assignment feed
- Confirm assignment + report delay actions
- Kyber API/event integration contract finalised (if not earlier)

### V1.6 — Marine Infrastructure permissions

- `manage_berth_availability` permission addable to existing roles
  (initially Harbour Master, optionally Shift Supervisor)
- Berth availability action surface (desktop)
- `BERTH_AVAILABILITY_CHANGED` event emission
- Optional: standalone Marine Infrastructure role if usage justifies

### V1.x deferrals

- `OPERATOR_DEFERRED` for VTSO (V1.0 has HM defer only)
- Federation with Kyber for pilot authentication
- External regulator / auditor read-only access (separate role
  design)
- Multi-role-per-user once the operational pattern is clear

---

## 13. Open Questions

These questions are flagged for review before V1.0 design lock. None
have answers in this document.

1. **Single role vs multiple roles per user.** V1.0 simplification
   says one primary role per user. Does the schema permit later
   multi-role grants without migration? (Recommended: yes.)

2. **Port-scoped vs multi-port users.** A Harbour Master in
   Melbourne should not see Darwin's data by default. Does V1.0
   enforce single-port scope per session, or allow port switching
   within the session with audit trail of port switches?

3. **Smaller-port role consolidation.** In small ports the Harbour
   Master often serves as Shift Supervisor. Does V1.0 model this as
   (a) HM inherits all SS permissions, (b) one user can hold both
   roles, or (c) a port-config flag merges the two roles into a
   single console?

4. **Notification model.** Which roles receive push (escalations,
   weather alerts, assignment confirmations)? What is the transport
   (SSE, WebSocket, external push, email)? V1.0 may ship with
   in-app-only and defer external push to V1.x.

5. **Stakeholder authentication model.** Horizon-direct, federated
   with a port-issued identity, federated with Kyber, or
   tenant-managed? V1.0 likely uses Horizon-direct; V1.x revisits.

6. **External regulator / auditor read-only access.** Port authorities
   may need to share operational data with maritime safety bodies,
   insurers, or government regulators. Is there a `RegulatoryAuditor`
   role in V1, or is this a separate read-only access mechanism
   layered on top? **Recommendation in §14:** defer.

7. **Marine Infrastructure: role or permission?** §4.6 leans toward
   permission for V1.0, standalone role only if/when warranted.

8. **Audit history visibility per role.** Harbour Master sees full
   history; Executive sees aggregated incidents; Shift Supervisor
   scoped to own shifts. Is the scoping by time window or by
   subject-id, or both?

9. **Reason codes vs free text.** Which actions require structured
   reason codes (vs free text notes) for compliance? At minimum:
   port closure, override, defer. Possibly: escalate. Reason codes
   need a closed catalogue per tenant.

10. **Approval workflow vs direct action.** Are all HM approvals
    immediate, or does the SS prepare them as "pending HM approval"
    so the HM only sees the queue? Affects UX and notification
    model.

11. **Audit retention class per role.** HM-level actions and
    sign-offs may need longer retention than routine VTSO
    acknowledgements. The Phase 0 schema's `retention_class` field
    supports this; V1 needs to populate it correctly per event
    type.

12. **Multi-tenancy posture for the demo tenant.** The AMS demo
    tenant is single-tenant on Railway. When V1 ships, customer
    tenants join. How are demo data and customer data isolated, and
    does the Harbour Master role apply to demo-only at first?

---

## 14. Recommendations

For the V1.0 design review:

1. **Treat this document as V1 permission model v0.1.** Expect
   iteration to v0.2, v0.3, ... as the design lock approaches. The
   model is foundational; surfacing structural objections now is
   cheaper than retro-fitting.

2. **Build VTSO first** (Phase V1.1 per §12). It validates the core
   data flow, action framework, audit emission, and reason-code
   capture before authority-level features are layered on. VTSO is
   the operational anchor; everything else extends or restricts it.

3. **Keep Marine Infrastructure as a permission first**, not as a
   standalone role. Add `manage_berth_availability` to the HM and SS
   permission bundles in V1.0–V1.5; only create a standalone MI
   role in V1.6 if usage volume or workflow complexity demands a
   dedicated console.

4. **Defer the external auditor role.** Until at least one customer
   tenant is operational and a real regulator engagement exists,
   building a read-only external-auditor role is speculative. When
   the need arrives, model it as a constrained subset of Harbour
   Master visibility, not a new role primitive.

5. **Do NOT implement until this model is reviewed and accepted.**
   V1.0 implementation (RBAC schema, role-scoped `/api/summary`,
   new login flow) is a substantial body of work that touches
   `server.py`, `audit.py`, `migrations/`, and the frontend. It
   requires:
   - Explicit acceptance of this v0.1 (or a versioned successor)
   - A V1 implementation runbook analogous to the Phase 1.2
     activation runbook
   - A Beta 10 regression-gate update strategy: V1.0 will
     necessarily change the Beta 10 baseline because the auth model
     changes; the regression gate's baseline must be deliberately
     updated by a reviewer, not silently broken
   - Ongoing protection of `phase-0-complete @ 4ad4aae` as the Beta
     10 reference point; V1.0 ships as a separate deployment
     posture, not a mutation of Beta 10

6. **Pause Stage E-prod (Phase 1.2 audit DB activation) until the
   V1 RBAC design is locked.** Activating production audit emission
   without the V1 user/role identity on each audit row would record
   actions against the shared `HORIZON_USER` / `O-1` actor handle,
   which is Beta-10-shaped data. V1.0's actor model is more
   informative; landing it before persistent audit goes live keeps
   the production ledger consistent from day one.

   (This is a recommendation only; the Phase 1.2 sequence can
   proceed under the existing Beta 10 actor model if commercial
   considerations require sooner activation. The trade is a
   short-lived run of Beta-10-shaped audit data preceding V1.0.)

---

**End of v0.1.** Reviewer comments expected before v0.2.
