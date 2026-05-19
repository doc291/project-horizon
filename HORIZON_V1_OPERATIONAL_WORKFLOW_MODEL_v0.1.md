# Horizon V1 — Operational Workflow Model (v0.1)

**Status:** Design draft — V1 planning only
**Document version:** 0.1
**Date:** 2026-05-15
**Companion document:** `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md`
  (PR #30, merged at `4c4940f`)
**Implementation status:** None. **This is not implementation approval.**
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

This document defines the **V1 operational workflow model** for
Project Horizon — the *temporal* and *coordination* dimension of
production port operations. Where the V1 User & Permission Model
(v0.1) answered **WHO** uses Horizon and **WHAT** they are
authorised to see and do, this document answers **HOW** Horizon is
operationally used during live port operations: how a shift opens
and closes, how conflict ownership flows from system detection to
operator resolution, how recommendations move through their
lifecycle, how authority escalates, how handovers transfer state
between watches, and how incidents are reconstructed post-hoc.

**Relationship to the permission model.** This document consumes
`HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` as the authoritative
contract for role definitions, permission grants, and scope rules.
It does not duplicate that material. Where this document references
a role's authority over a workflow stage, the authority itself is
defined in the permission model; this document defines only *when*
and *how* that authority is exercised, *what state changes* result,
and *what audit events* land in the ledger.

**This document does not authorise any implementation.** It is v0.1
of an evolving model. The Phase 0 Beta 10 baseline remains
protected at `phase-0-complete @ 4ad4aae`; no V1 workflow code,
state machine, screen, notification transport, or AI orchestration
will be built until both this workflow model and the permission
model have been reviewed, challenged, and accepted by operational
stakeholders.

---

## 2. Design Principles

The following six principles govern every workflow decision in V1.
They sit alongside (not replacing) the five RBAC principles from the
permission model. Where future implementation choices conflict with
these principles, the principle wins.

1. **Time is a first-class concept.**
   Decision deadlines, shift boundaries, handover windows, deferral
   revisit times, escalation timeouts and recommendation expiry are
   all *explicit state*, not implicit clock-watching. Time-pressure
   events emit audit rows. Operators see the time dimension of
   their work on every screen.

2. **Operational state has a single owner at any moment.**
   A conflict is owned by the system at detection, by a named VTSO
   at presentation, by a Shift Supervisor during escalation, by the
   Harbour Master during authority decision, and explicitly
   re-owned at handover. Two operators cannot simultaneously "own"
   the same operational decision; ownership transfers are auditable.

3. **Escalation is structured, not informal.**
   Every escalation has a defined source actor, a defined target
   authority, a structured reason code (not free text alone), and a
   closure event. Bypass paths (e.g. emergency direct-to-Harbour-
   Master) exist but are explicitly logged as bypasses, not silently
   re-routed escalations.

4. **Inaction is an auditable event once the V1 action surface exists.**
   Acknowledgements not followed by resolution, recommendations whose
   deadlines pass, sessions that end with pending items — all generate
   audit events. This is the operational substrate the Phase 0
   deferred events (`DEADLINE_PASSED`, `SESSION_ENDED_WITHOUT_ACTION`,
   `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`) require. They become
   emissible once V1.1 ships the action surface.

5. **Replay is the operational ground truth.**
   Every coordination decision must be reconstructible from the audit
   ledger after the fact. Replay is not optional tooling; it is the
   acceptance criterion against which the V1 audit surface is
   judged. If a future regulator, insurer, or post-incident reviewer
   cannot reconstruct the operational sequence of an event, the
   workflow model has failed.

6. **Workflow must respect role, permission and scope.**
   No workflow design supersedes the permission model. If a workflow
   step requires a permission a role does not hold, the workflow
   design changes — not the permission grant. Scope (port, tenant,
   shift, assignment) constrains workflow visibility at every step.

---

## 3. Operational State Vocabulary

V1 introduces twelve foundational state primitives. Each has a
specific responsibility; conflation between them is the most common
source of operational confusion and must be avoided in V1
architecture.

| Term | Definition |
|---|---|
| **shift** | A bounded operational window with a defined Shift Supervisor (or, in small ports, a Harbour Master acting as one). Has a start time, an end time, an active scope (port, tenant), and a unique `shift_id` that every audit event during the shift carries in its payload. |
| **watch** | A concurrent operating context within a shift. One or more VTSO sessions can be active on the same watch. Multiple watches may exist per shift (e.g. port-control vs. anchorage-control on a large port). |
| **ownership** | The current responsible actor for a piece of operational state (a conflict, a recommendation, an escalation). Exactly one owner at any moment. Transfers are explicit and auditable. |
| **pending queue** | The set of recommendations or escalations that have reached an actor but not yet been acted on (resolved / deferred / overridden / escalated). The pending queue is per-actor and per-shift; the handover workflow inventories it. |
| **escalation chain** | The ordered path an issue takes from the operational floor to authority: VTSO → Shift Supervisor → Harbour Master. The chain can be entered at any point and bypassed in emergencies; bypasses are explicitly logged. |
| **incident** | A coordinated sequence of operationally-significant events warranting post-hoc review (port closures, severe weather holds, high-severity conflicts with disputed resolution, vessel detention, etc.). An incident has a defined time window and a defined audience for replay. |
| **recommendation** | A system-generated proposal for operator action against a detected conflict. Carries a decision_deadline, a recommended option, alternatives, and the §1.4.1 decision-time snapshot already captured by Phase 0.7b. |
| **guidance** | A non-recommendation operational notice (weather alert, berth availability change, vessel state observation) that informs operators but does not require an explicit action. Acknowledgement is desirable; resolution is not the primary verb. |
| **action** | An operator-initiated state change against a recommendation, conflict, or guidance item. Bound to a user, role, scope, and reason where required. Always lands an audit event. |
| **closure** | The terminal lifecycle state for a recommendation, escalation, or incident. Closures cannot be undone (the audit chain is append-only); a re-opened item is a new lifecycle, not a mutation. |
| **handover** | The structured transfer of operational state between outgoing and incoming Shift Supervisors at shift boundaries. Comprises a written handover note, a pending-item inventory, an escalation queue snapshot, and an explicit acceptance event. |
| **replay window** | The time range over which the audit ledger is reconstructed for incident review or executive analysis. Bounded by tenant scope, role-permitted depth, and (in V1.x) regulatory retention class. |

### Notes

- A **shift** and a **watch** are NOT the same. A watch is a
  concurrent slice of a shift. One shift, multiple watches.
- **Guidance** and **recommendation** are deliberately separated.
  The recommendation lifecycle in §7 applies to recommendations
  only; guidance has a simpler acknowledge-only lifecycle.
- **Closure** is final. The chain is append-only; "re-opening" an
  incident creates a new incident_id linked back to the original.

---

## 4. Shift Lifecycle

The operational shift, end-to-end. Each state has a defined trigger,
primary actor, state change, audit implication, notification
implication, and (where present) open design questions.

### 4.1 Pre-shift

| Aspect | Detail |
|---|---|
| **Trigger** | Incoming Shift Supervisor arrives at console; previous shift not yet closed |
| **Primary actor** | Incoming Shift Supervisor |
| **State change** | None yet. The system is still in the previous shift's state. SS may be reading the outgoing handover draft. |
| **Audit implication** | `AUDIT_READ` events on the outgoing handover draft (proposed; meta-event so reviews are recorded) |
| **Notification implication** | None outbound. Inbound: SS sees pending items count and any flagged escalations |
| **Open questions** | Should pre-shift have a defined window (e.g. 15 minutes before scheduled handover) where the incoming SS is recognised by the system, or is it implicit until the formal handover-accept event? |

### 4.2 Shift open

| Aspect | Detail |
|---|---|
| **Trigger** | Either: (a) outgoing SS closes previous shift and the incoming SS issues a formal "open shift" action, or (b) the first VTSO logs in under a new wall-clock shift window when no shift is currently open |
| **Primary actor** | Shift Supervisor (preferred); VTSO (fallback, if no SS available) |
| **State change** | New `shift_id` issued; active scope established (port, tenant); VTSO sessions begin attaching to the new `shift_id` |
| **Audit implication** | `SHIFT_OPENED` (candidate future event — see §14) with payload: `shift_id`, opening actor, scope, opening time |
| **Notification implication** | Outbound: notify the active Harbour Master that a new shift is open and the supervisor identity |
| **Open questions** | How does the system reconcile when (a) a wall-clock shift window starts but no SS has arrived, or (b) two consecutive shifts overlap because the outgoing SS hasn't closed yet? |

### 4.3 Steady-state operations

| Aspect | Detail |
|---|---|
| **Trigger** | n/a — this is the default state during a shift |
| **Primary actors** | VTSOs (operational), SS (oversight), HM (on-call), Stakeholders (per-assignment) |
| **State change** | Events flow: conflicts detected, recommendations generated and presented, operators acknowledge / resolve / escalate, what-ifs run, weather observed |
| **Audit implication** | All Phase 0 emissible events fire normally (`CONFLICT_DETECTED`, `RECOMMENDATION_GENERATED`, `RECOMMENDATION_PRESENTED`, `OPERATOR_ACTED`, `SESSION_STARTED`/`SESSION_ENDED`); V1 adds `OPERATOR_ACKNOWLEDGED`, `OPERATOR_ESCALATED`, `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE` |
| **Notification implication** | Continuous: VTSOs see new recommendations as they're presented; SS sees escalations as they arrive; HM sees approval requests |
| **Open questions** | What's the cadence of "is everyone still active?" health checks during a long quiet shift? Does inactivity itself emit an event? |

### 4.4 Mid-shift handover or delegation

| Aspect | Detail |
|---|---|
| **Trigger** | SS takes a break and delegates to a VTSO temporarily, or partial handover within a shift (rare but operationally real) |
| **Primary actor** | Outgoing SS (initiating); VTSO or alternate SS (receiving) |
| **State change** | Acting-supervisor flag transfers; original SS still on the shift but not active; receiving actor temporarily holds escalation-target authority |
| **Audit implication** | `DELEGATION_GRANTED` / `DELEGATION_REVOKED` (candidate future events) with both actor handles, the reason, the time window |
| **Notification implication** | HM notified of delegation; all active VTSOs see the acting supervisor identity in the UI |
| **Open questions** | Is mid-shift delegation a V1.0 requirement, V1.x feature, or out-of-scope entirely? In some ports it's never used; in others it's daily. The decision affects schema and UI complexity. |

### 4.5 End-of-shift preparation

| Aspect | Detail |
|---|---|
| **Trigger** | Approaching scheduled shift boundary; outgoing SS begins drafting the handover note |
| **Primary actor** | Outgoing Shift Supervisor |
| **State change** | None yet. The shift is still active; the SS is reviewing the day's events, drafting the handover, inventorying pending items |
| **Audit implication** | `AUDIT_READ` events as the outgoing SS reviews the shift's audit history (proposed meta-event) |
| **Notification implication** | None outbound — this is a preparatory phase |
| **Open questions** | Is there a structured "draft handover" state distinct from the formal "handover write," or is the draft an in-memory UI state with no audit until it's submitted? |

### 4.6 Handover write

| Aspect | Detail |
|---|---|
| **Trigger** | Outgoing SS submits the handover note |
| **Primary actor** | Outgoing Shift Supervisor |
| **State change** | A `shift_handover` record is created and linked to the closing `shift_id`; the pending-item inventory is frozen; the escalation queue is snapshotted |
| **Audit implication** | `HANDOVER_CREATED` (candidate future event — see §14) with payload: `shift_id`, outgoing SS, handover note (or hash of it), pending item count, escalation queue depth |
| **Notification implication** | Incoming SS notified that the handover is ready for review; HM notified of any open escalations passing through the boundary |
| **Open questions** | Mandatory fields vs free-text? Can the handover be written-then-edited until it's accepted, or is the write event the freeze point? How are last-minute events (a conflict detected 30 seconds before handover) handled? |

### 4.7 Handover acceptance

| Aspect | Detail |
|---|---|
| **Trigger** | Incoming SS reads the handover and formally accepts |
| **Primary actor** | Incoming Shift Supervisor |
| **State change** | Operational ownership of pending items and escalation queue transfers to the incoming SS; the outgoing shift can now close; the new shift's `shift_id` becomes the active one |
| **Audit implication** | `HANDOVER_ACCEPTED` (candidate future event) with payload: incoming SS, outgoing `shift_id`, acceptance time, any clarification requests issued before acceptance |
| **Notification implication** | Outgoing SS notified of acceptance (they can leave); active VTSOs notified of the new supervisor identity |
| **Open questions** | Can acceptance be conditional ("I accept except for items X and Y, which need clarification before close")? What happens if the incoming SS refuses to accept — does the outgoing SS remain on duty? |

### 4.8 Shift close

| Aspect | Detail |
|---|---|
| **Trigger** | Handover accepted; outgoing SS issues formal close |
| **Primary actor** | Outgoing Shift Supervisor |
| **State change** | Outgoing `shift_id` marked closed; no further actions can attach to it; the audit ledger entries for that shift become read-only as historical record |
| **Audit implication** | `SHIFT_CLOSED` (candidate future event) with payload: closing `shift_id`, outgoing SS, close time, final pending-item count, final escalation queue depth |
| **Notification implication** | HM and incoming SS receive confirmation that the shift is formally closed |
| **Open questions** | Can a shift be closed without an accepted handover (e.g. if the incoming SS is unavailable)? If yes, what's the audit posture — does the next opening shift "inherit" the pending items via a different mechanism? |

---

## 5. Operational Authority Flow

This section defines who has what authority in the operational
workflow. The authority *grants* are defined in the permission model
(PR #30); this section defines how they *flow* during live
operations.

### 5.1 VTSO authority

- **Operational verbs:** acknowledge guidance, resolve a conflict,
  run a what-if, flag for escalation, add resolution notes
- **Scope:** active port; active shift; their own session
- **Authority limits:** cannot approve, override, close port, sign
  off Port Brief, or apply a what-if to live state
- **Escalation entitlement:** can escalate to SS at any time;
  emergency bypass to HM permitted (logged as bypass)

### 5.2 Shift Supervisor / Watch Lead authority

- **Operational verbs:** review VTSO actions, write/accept handover,
  escalate to HM, request clarification from outgoing SS
- **Scope:** active shift; all VTSOs and watches under it
- **Authority limits:** cannot approve port closure, override berth
  assignments, sign off Port Brief, or apply a what-if to live state
  (those are HM-only)
- **Acting-HM possibility:** in small ports with no HM on duty, SS
  may temporarily hold acting HM authority via explicit delegation
  (see §4.4 and §5.6)

### 5.3 Harbour Master authority

- **Operational verbs:** approve port closure, override berth
  assignment, approve a what-if for live application, sign off Port
  Brief, review full audit trail
- **Scope:** the port (or all ports under their authority class within
  the tenant)
- **Authority limits:** no cross-tenant access; no Stakeholder-side
  actions
- **Escalation receipt:** is the terminal authority within Horizon's
  operational scope; cannot escalate further within the system
  (regulatory escalation is out of band)

### 5.4 Executive read/review authority

- **Operational verbs:** read trend reports, read Port Brief, read
  aggregated incident summaries
- **Scope:** all ports in their tenant; aggregate level only
- **Authority limits:** zero operational action authority; no
  acknowledge, resolve, escalate, approve, override, defer, close, or
  sign-off
- **Audit posture:** all reads emit `AUDIT_READ` meta-events so the
  ledger records executive oversight

### 5.5 Stakeholder limited authority

- **Operational verbs:** confirm assignment receipt; report
  delay/issue
- **Scope:** their own assignments only
- **Authority limits:** zero visibility outside their assignment; no
  operational state changes beyond their two verbs
- **Kyber dependency:** for pilots, detailed assignment workflow is
  managed in Kyber; Horizon's stakeholder authority is the downstream
  acknowledgement plus delay-reporting surface

### 5.6 Delegation

- **Pattern:** outgoing actor → receiving actor; explicit scope and
  time window; explicit reason; audit-event-bracketed
  (`DELEGATION_GRANTED` ... `DELEGATION_REVOKED`)
- **Permissible delegations:**
  - SS → SS-equivalent or VTSO with supervisor permission, time-bounded
  - HM → SS as Acting HM (rare; emergency only; carries override and
    approve authority only for the delegated window)
- **Not permissible:** Executive → anyone (no operational authority
  to delegate); Stakeholder → anyone (scope cannot be widened);
  cross-tenant delegations

### 5.7 Emergency bypass

- **Trigger:** severe weather, vessel emergency, infrastructure
  failure where normal escalation chain is too slow
- **Mechanism:** VTSO escalates direct to HM, skipping SS
- **Audit posture:** `OPERATOR_ESCALATED` event carries an explicit
  `bypass=true` flag and a structured reason code; the skipped SS is
  notified retroactively
- **Limits:** bypass is permissible only for high-severity events
  (closed catalogue, not operator discretion); bypass abuse is
  detectable by trend analysis on the audit ledger

### 5.8 Conflict-of-authority handling

- **Pattern:** VTSO and HM disagree on resolution; SS and HM disagree
  on escalation closure; etc.
- **Resolution rule:** the higher authority's decision binds the
  outcome
- **Audit posture:** the lower authority's *preference* is preserved
  in the audit ledger via a `disagreement_recorded` payload field on
  the binding action's event; this captures evidentiary trail without
  blocking the operational decision
- **Boundary:** never silently overridden; the audit ledger must
  always show that a disagreement existed if one did

---

## 6. Conflict Ownership Model

A conflict is owned by exactly one actor (system or human) at any
moment. Ownership is auditable and explicitly transfers between
actors.

### 6.1 Ownership states

| Owner | Lifecycle stage | Audit posture |
|---|---|---|
| **system** | detected → generated → presented | Pre-human; events emitted under `actor_type='system'`, `actor_handle=null` |
| **VTSO (named)** | presented → acknowledged → in-progress | Assigned to active VTSO on the port watch; their handle on every action event |
| **Shift Supervisor (named)** | escalated → under-review | Transferred by `OPERATOR_ESCALATED`; SS handle on subsequent events until resolution or further escalation |
| **Harbour Master (named)** | escalated-to-authority → authority-decision-pending | Transferred by further `OPERATOR_ESCALATED`; HM handle on the eventual approve/override/defer event |
| **system again (unowned)** | closed | Post-human; events emitted under `actor_type='system'` for archival/closure bookkeeping |

### 6.2 Ownership transfer rules

1. **System → VTSO** on presentation. Automatic; the VTSO whose
   session is active on the port-watch is the assignee.
2. **VTSO → Shift Supervisor** on escalation. Explicit; SS handle
   replaces VTSO handle from this point.
3. **Shift Supervisor → Harbour Master** on further escalation.
   Same pattern.
4. **VTSO → Harbour Master (bypass)** on emergency escalation
   (§5.7). Skipped SS is notified.
5. **Any → next-shift equivalent** at handover. Pending-item
   inventory transfers; new shift's actor assumes ownership.
6. **Any → system** at closure. Ownership transfers back to system
   for archival.

### 6.3 Ownership during quiet periods

A specific failure mode: a recommendation is presented to a VTSO, the
VTSO acknowledges it, and then the VTSO is occupied with other work
for an extended period. The recommendation is *owned* by the VTSO
but no progress is being made.

- **Model:** ownership remains with the named VTSO until either the
  VTSO acts on it, the deadline passes (`DEADLINE_PASSED`), or the
  session ends (`SESSION_ENDED_WITHOUT_ACTION`)
- **System support:** the UI surfaces the pending-queue depth to the
  owning VTSO; the SS sees the queue too at handover review time
- **Open question:** should the system *force* a re-assignment after
  a stale period, or is that operator discretion?

### 6.4 Multi-watch ownership

If a port has multiple concurrent watches under one shift (port
control + anchorage control), the ownership rule extends: the watch
whose scope covers the conflict's location is the assignee.

- A conflict at Berth B04 is owned by the port-control watch's VTSO
- A conflict at the anchorage approach is owned by the
  anchorage-control watch's VTSO
- Cross-watch hand-overs at watch boundaries are a special case of
  the general handover workflow

### 6.5 Ownership audit implications

Every ownership transfer emits an event with both the prior and the
new owner's handles. The audit chain over a conflict's lifecycle
must be reconstructible to:

- Identify every named actor who held ownership
- Reconstruct the timing of each transfer
- Identify any periods of system-only ownership
- Reconstruct escalation paths (including any bypasses)

---

## 7. Recommendation Lifecycle — Operational View

The permission model (PR #30 §7) defined the lifecycle stages from
the RBAC angle. This section adds the *operational* angle: timing,
concurrency, and failure modes. Each stage has an owner, allowed
actors, expected time behaviour, audit event, notification behaviour,
and open questions.

### 7.1 detected

| Aspect | Detail |
|---|---|
| **Owner** | system |
| **Allowed actors** | none (system-only) |
| **Expected time** | sub-second from data ingestion; runs every `/api/summary` poll cycle |
| **Audit event** | `CONFLICT_DETECTED` (Phase 0.7a, emissible) |
| **Notification** | none — internal state only |
| **Open questions** | Should low-severity detections that don't progress to "generated" still emit an audit row? Phase 0.7a says yes (one row per detected conflict_id); is that the right level of noise? |

### 7.2 generated

| Aspect | Detail |
|---|---|
| **Owner** | system |
| **Allowed actors** | none (system-only) |
| **Expected time** | sub-second after `detected`; coincident in the same `build_summary()` run |
| **Audit event** | `RECOMMENDATION_GENERATED` (Phase 0.7b, emissible, with §1.4.1 decision-time snapshot) |
| **Notification** | none — the recommendation is generated but not yet visible to operators |
| **Open questions** | If two consecutive polls produce slightly different `recommended_option_id`s for the same conflict, is the change tracked as a `RECOMMENDATION_OBSOLETED` + new `RECOMMENDATION_GENERATED`, or as an in-place update with both options in the snapshot? The Phase 0.7b dedup-key answer is "different rec → different recommendation_id → re-emit." V1 inherits this. |

### 7.3 presented

| Aspect | Detail |
|---|---|
| **Owner** | system → VTSO (transfer on first presentation to an authenticated operator surface) |
| **Allowed actors** | VTSO (primary), HM (read on demand), SS (read on demand) |
| **Expected time** | one `/api/summary` poll cycle after `generated`; in practice 1–30 seconds |
| **Audit event** | `RECOMMENDATION_PRESENTED` (Phase 0.7c, emissible, scoped to `surface=api_summary` and to the actor_handle whose session presented it) |
| **Notification** | UI surfaces the recommendation in the VTSO's pending queue |
| **Open questions** | If the same recommendation is presented to two VTSOs on the same watch concurrently, do both get `RECOMMENDATION_PRESENTED` events with their own actor_handles (yes per current 0.7c dedup keying), and which of them is the "owner"? |

### 7.4 acknowledged

| Aspect | Detail |
|---|---|
| **Owner** | named VTSO (the acknowledger) |
| **Allowed actors** | VTSO, HM |
| **Expected time** | typical: seconds to minutes after presentation; high-severity recommendations expected sub-30-second acknowledge |
| **Audit event** | `OPERATOR_ACKNOWLEDGED` (V1 new event type — proposed) |
| **Notification** | SS sees the acknowledgement in their review queue at next handover or via live indicator |
| **Open questions** | Does acknowledgement carry a reason code, or is the acknowledgement itself the signal? Should a high-severity recommendation force a reason ("acknowledging because..." with a short note)? |

### 7.5 accepted / resolved

| Aspect | Detail |
|---|---|
| **Owner** | named VTSO (or HM if escalated) |
| **Allowed actors** | VTSO, HM |
| **Expected time** | varies wildly: from seconds (clear-cut conflicts) to hours (complex multi-vessel coordination) |
| **Audit event** | `OPERATOR_ACTED` with `action_type='conflict_resolve'` (Phase 0.8a covers `whatif_apply`/`whatif_clear`/`send_brief`; V1.1 adds conflict_resolve) |
| **Notification** | affected stakeholders (pilots, towage) notified of the chosen resolution; SS sees the resolution in review queue |
| **Open questions** | If a VTSO resolves a conflict but the resolution requires a what-if to be applied to live state, and only the HM can approve that — is the resolution complete or does it remain "pending approval"? Workflow tension between operational momentum and authority boundaries. |

### 7.6 deferred

| Aspect | Detail |
|---|---|
| **Owner** | HM (V1.0); VTSO (V1.x, possibly) |
| **Allowed actors** | HM in V1.0 |
| **Expected time** | deferral is a deliberate "not now" decision; the `defer_until` field defines when the recommendation re-enters the active queue |
| **Audit event** | `OPERATOR_DEFERRED` (Phase 0 reserved, V1.2 emits) with mandatory reason code, `defer_until` timestamp, defer-er's handle |
| **Notification** | the original presenter (VTSO) is notified of the deferral; at `defer_until` the recommendation re-presents |
| **Open questions** | What happens if `defer_until` is reached but the operational context has changed (the recommendation is now stale)? Does the system silently obsolete it, or does it re-present and the operator marks it obsolete explicitly? |

### 7.7 overridden

| Aspect | Detail |
|---|---|
| **Owner** | HM |
| **Allowed actors** | HM only |
| **Expected time** | typically immediate after the HM reviews the recommendation; not a "pending" state |
| **Audit event** | `OPERATOR_OVERRODE` (Phase 0 reserved, V1.2 emits) with mandatory reason code, override choice, the original recommendation_id |
| **Notification** | the VTSO is notified that their recommendation was overridden and the HM's chosen alternative is now operational; SS sees the override in review queue |
| **Open questions** | Does an override leave the original recommendation in an "overridden" terminal state, or does it close the recommendation and the HM's chosen alternative becomes a *new* recommendation that's immediately accepted? Audit-trail clarity favours the former. |

### 7.8 escalated

| Aspect | Detail |
|---|---|
| **Owner** | transfers from source to target along the escalation chain |
| **Allowed actors** | VTSO (escalating up), SS (escalating up), occasionally direct VTSO → HM (bypass) |
| **Expected time** | escalation creation is sub-second; escalation closure is operator-paced |
| **Audit event** | `OPERATOR_ESCALATED` (V1 new event type — proposed) with source, target, reason code, optional bypass flag |
| **Notification** | target receives an escalation alert; original source sees their escalation in their tracking queue |
| **Open questions** | Multi-step escalation: if a VTSO escalates to SS, who escalates to HM — does the SS create a new escalation (chained to the VTSO's), or does the original escalation transition target from SS to HM in place? §8 prefers the former for chain clarity. |

### 7.9 expired / deadline passed

| Aspect | Detail |
|---|---|
| **Owner** | named actor at the moment of expiry (whoever owned it at deadline) |
| **Allowed actors** | none (system-emitted event) |
| **Expected time** | exactly at `decision_deadline` (per the pinned deadline in the §1.4.1 snapshot from `RECOMMENDATION_GENERATED`) |
| **Audit event** | `DEADLINE_PASSED` (Phase 0 reserved, V1.x emits — requires V1's pinned-deadline lifecycle per the Phase 0 deferral note) |
| **Notification** | the current owner is notified; the SS sees deadline-passed events in their review queue; HM gets a daily summary |
| **Open questions** | What's the operational consequence of a passed deadline besides the audit row? Does the recommendation get auto-escalated? Does its severity uprank? Does the system auto-close it? V1.0 decision needed. |

### 7.10 closed

| Aspect | Detail |
|---|---|
| **Owner** | whoever issued the closure verb (resolve, override, or HM-issued close) |
| **Allowed actors** | VTSO (close-via-resolve), HM (close-via-override or explicit close) |
| **Expected time** | terminal state; instantaneous emission once the closing action is taken |
| **Audit event** | implied by `OPERATOR_ACTED` with resolve/override/close `action_type`; potentially a separate `RECOMMENDATION_CLOSED` event for clarity (proposed) |
| **Notification** | the lifecycle ends; the recommendation drops out of all pending queues |
| **Open questions** | Should closure require a final reason note, or is the `action_type` enough? For overrides, the reason is already mandatory; for resolutions, V1.0 may make it optional. |

### 7.11 reviewed

| Aspect | Detail |
|---|---|
| **Owner** | n/a — review is a read action, not a state-mutating one |
| **Allowed actors** | SS (per shift), HM (full audit), Executive (aggregated only) |
| **Expected time** | post-closure; reviews happen at handover, in incident replay, or via Executive trend reports |
| **Audit event** | `AUDIT_READ` (V1 new meta-event — proposed) with reviewer handle, time range, scope of items reviewed |
| **Notification** | the original actor whose actions were reviewed *may* be notified, depending on tenant policy (V1 open question — see §17) |
| **Open questions** | Should an `AUDIT_READ` be emitted for every record read, or aggregated to one event per review session? The trade is between fidelity and noise. |

---

## 8. Escalation Paths

Escalation is the structured mechanism by which operational issues
move up the authority chain. V1 defines four canonical paths.

### 8.1 VTSO → Shift Supervisor (primary escalation)

- **Trigger:** VTSO encounters an issue beyond their resolution
  authority or requiring shift-level oversight
- **Actor:** VTSO (source); SS (target)
- **Mechanism:** VTSO invokes "escalate" action on a recommendation
  or guidance item; structured reason code required
- **State change:** ownership transfers from VTSO to SS; the
  escalation enters the SS's review queue
- **Audit event:** `OPERATOR_ESCALATED` with `source_role=VTSO`,
  `target_authority=SS`, `reason_code`, original `recommendation_id`
  or `conflict_id`
- **Closure:** SS resolves the issue, further escalates, or returns
  it to the VTSO with notes

### 8.2 Shift Supervisor → Harbour Master (authority escalation)

- **Trigger:** SS encounters an issue requiring authority-level
  decision (approve, override, close port)
- **Actor:** SS (source); HM (target)
- **Mechanism:** SS invokes "escalate" with an explicit "to-HM" flag;
  reason code mandatory
- **State change:** ownership transfers SS → HM; the escalation
  enters the HM's pending approval queue
- **Audit event:** `OPERATOR_ESCALATED` with `source_role=SS`,
  `target_authority=HM`, `reason_code`, source escalation chain
  reference (the VTSO-source-escalation if applicable, so the full
  chain is reconstructible)
- **Closure:** HM approves, overrides, defers, or returns to SS

### 8.3 Direct VTSO → Harbour Master (emergency bypass)

- **Trigger:** severe operational situation where normal escalation
  chain is too slow (severe weather, vessel emergency, infrastructure
  failure)
- **Actor:** VTSO (source); HM (target); skipped SS notified
  retroactively
- **Mechanism:** VTSO invokes escalate with explicit "bypass" flag
  and high-severity reason code; SS notified within seconds
- **State change:** ownership transfers VTSO → HM directly; SS sees
  the bypass in their alerts queue and may join the operational
  conversation
- **Audit event:** `OPERATOR_ESCALATED` with `source_role=VTSO`,
  `target_authority=HM`, `bypass=true`, `reason_code` (closed catalogue
  limited to high-severity codes only)
- **Limits:** bypass reason codes are a constrained subset of normal
  reason codes; non-emergency bypasses are detectable in audit trend
  analysis and constitute policy violations

### 8.4 Multi-step escalation

- **Pattern:** a VTSO escalates to SS, SS escalates further to HM
  (because the issue is genuinely authority-level after SS review)
- **Mechanism:** each step is a distinct `OPERATOR_ESCALATED` event
  carrying a reference to the previous escalation_id; the chain is
  reconstructible end-to-end
- **State change:** ownership transfers stepwise (VTSO → SS → HM)
- **Audit event:** two `OPERATOR_ESCALATED` events, each linked
- **Closure:** the terminal authority (HM) issues an action that
  closes the entire chain; the closure event carries references back
  to all linked escalations

### 8.5 Escalation reason codes

Closed catalogue (V1.0 baseline; extensible per tenant in V1.x):

- `severity_critical` — high-severity conflict requiring authority
- `cross_authority_scope` — issue spans authority boundaries
- `regulatory_compliance` — issue requires authority sign-off for
  compliance
- `operator_uncertainty` — issue exceeds VTSO/SS authority interpretation
- `emergency_weather` — severe weather event (bypass-eligible)
- `emergency_vessel` — vessel emergency (bypass-eligible)
- `emergency_infrastructure` — infrastructure failure (bypass-eligible)
- `policy_request` — explicit operational policy request from authority
- `clarification_needed` — escalation seeks authority clarification, not
  decision

### 8.6 Escalation closure

Every escalation has a defined closure path:

- **Resolved by target:** target authority takes an action and closes
- **Returned to source:** target returns with guidance; source resumes
  ownership
- **Further escalated:** chain continues to next authority level
- **Withdrawn by source:** source rescinds the escalation (with audit
  note); ownership returns to source

In all cases, closure emits an event linking back to the original
escalation_id.

### 8.7 Failed or ignored escalation

- **Pattern:** an escalation sits in the target's queue without
  acknowledgement for an extended period
- **Detection:** notification timeout (V1 open question — what's the
  threshold per severity?)
- **Audit posture:** if a timeout fires, emit `ESCALATION_TIMEOUT`
  (candidate future event) and notify the next authority level up
  (or the alternate target if the primary is offline)
- **Open question:** does the system auto-re-escalate, or does it
  alert and rely on operator judgement?

### 8.8 Escalation audit semantics

The audit ledger must capture, for every escalation:

- Source actor and role
- Target authority
- Reason code
- Timestamp (creation and closure)
- Bypass flag (if applicable)
- Reference chain to any prior escalations
- Closure event reference

This allows post-hoc reconstruction of every authority-relevant
decision and is the foundation of the incident replay flow (§12).

---

## 9. Handover Workflow

Handover is the structured transfer of operational state between
outgoing and incoming Shift Supervisors. It is the highest-frequency
audit-significant event in a port's operational lifecycle.

### 9.1 Pre-handover review

- **Trigger:** outgoing SS approaches end-of-shift window
- **Activity:** outgoing SS reviews the shift's audit history
  (actions taken, decisions made, escalations open, pending queue
  depth); identifies items requiring narrative explanation in the
  handover note
- **Audit posture:** `AUDIT_READ` events as the SS navigates the
  audit trail
- **Duration:** varies; typically 15–60 minutes pre-handover

### 9.2 Outgoing handover creation

- **Trigger:** outgoing SS submits handover note via the supervisor
  console
- **Structured fields (proposed):**
  - Events of operational significance during the shift (free text,
    bounded length)
  - Pending recommendations and their owners
  - Open escalations and their target authorities
  - Outstanding HM approvals
  - Weather posture and forecast notes
  - Anything unusual requiring incoming SS attention
- **Audit event:** `HANDOVER_CREATED` (candidate future event — §14)
  with `outgoing_shift_id`, `outgoing_ss_handle`, `note_hash` (the
  full note stored in `shift_handover` table per PR #30 §9.2; the
  audit event carries a hash for integrity)

### 9.3 Pending item inventory

- **Captured at handover-create time:**
  - Active recommendations (not yet resolved/deferred/overridden/
    closed)
  - Open escalations (not yet closed by target authority)
  - Pending HM approvals (awaiting authority decision)
  - Active operator-deferred items with future `defer_until`
- **Snapshot semantics:** the inventory is frozen at the
  handover-create event; new items arriving between create and
  accept either go to the outgoing or the incoming shift (V1 open
  question — see §17)

### 9.4 Escalation queue transfer

- **Pattern:** open escalations in the outgoing shift's queue
  transfer to the incoming shift's queue at handover-accept
- **Ownership:** the target authority of each escalation does not
  change (HM stays HM); but the shift_id under which subsequent
  events fire becomes the incoming shift's
- **Audit posture:** every transferred escalation gets a synthetic
  event linking it to the incoming shift_id, so the chain over the
  escalation's lifetime is reconstructible across shift boundaries

### 9.5 Incoming read and acceptance

- **Trigger:** incoming SS reads the handover note and pending
  inventory
- **Activity:** reviews the handover content; may request
  clarification from outgoing SS before accepting
- **Audit posture:** `AUDIT_READ` events as the incoming SS
  navigates the handover and current state

### 9.6 Clarification requests

- **Pattern:** incoming SS asks outgoing SS for clarification on a
  specific item before accepting
- **Mechanism:** structured clarification field attached to the
  handover; both SSs see the conversation
- **Audit posture:** clarification exchanges are auditable but may
  not need event-level emission for routine cases (V1 design
  decision); high-stakes clarifications (e.g. unresolved escalations)
  should emit events
- **Resolution:** outgoing SS responds; if the response satisfies the
  incoming SS, they proceed to accept; if not, the outgoing SS may
  amend the handover note before formal accept

### 9.7 Formal ownership transfer

- **Trigger:** incoming SS issues "accept" action
- **State change:** operational ownership of pending items,
  escalation queue, and active recommendations transfers to the
  incoming shift's scope; outgoing shift is now eligible for close
- **Audit event:** `HANDOVER_ACCEPTED` (candidate future event)
  with incoming SS handle, outgoing shift_id, acceptance time,
  reference to the handover_id being accepted
- **Notification:** outgoing SS notified (can leave); HM notified
  (new supervisor identity); all active VTSOs notified

### 9.8 Handover audit trail

Post-handover, the full audit trail of the handover comprises:

- `HANDOVER_CREATED` event with outgoing SS handle
- Any `AUDIT_READ` events for the outgoing SS's pre-handover review
- Any clarification exchange events (if event-emitted)
- Any `AUDIT_READ` events for the incoming SS's review
- `HANDOVER_ACCEPTED` event with incoming SS handle
- Synthetic transfer events for each pending item/escalation
- `SHIFT_CLOSED` event for the outgoing shift
- `SHIFT_OPENED` event for the incoming shift (if not already open)

The chain is intentionally rich because the handover is the highest-
frequency point of operational responsibility transfer and the most
common surface for post-hoc review.

### 9.9 Open questions

- **Mandatory fields:** which handover fields are required vs
  optional? V1.0 may start with all-optional and tighten in V1.x as
  operational patterns emerge.
- **Handover format:** structured-only, or hybrid structured + free
  text? Hybrid likely; structured fields drive the inventory, free
  text captures narrative.
- **Edit-after-write:** can the outgoing SS edit the handover after
  submitting but before incoming SS accepts? Operationally yes (e.g.
  late-breaking events); audit-wise the edits must be versioned.
- **Concurrent handover:** what if outgoing SS hasn't finished
  writing when incoming SS arrives and wants to read? Real-time
  collaborative drafting, or strict "writer locks until submit"?

---

## 10. Override / Defer Workflow

The two highest-stakes authority-level actions get their own
section. Both require structured reason codes; both are subject to
executive review.

### 10.1 Defer

- **Verb semantic:** "I acknowledge this recommendation but
  explicitly choose not to act now"
- **Allowed actors:** HM (V1.0); possibly VTSO with restricted defer
  permission (V1.x)
- **Required inputs:**
  - `recommendation_id` (the recommendation being deferred)
  - `defer_until` (timestamp in the future when the recommendation
    re-enters the active queue)
  - `reason_code` (mandatory; closed catalogue)
  - `notes` (optional free text for nuance)

### 10.2 `defer_until` time

- **Bounds:** must be in the future, must be before the original
  `decision_deadline`, must be within the active shift's window OR
  carry an explicit cross-shift flag
- **Behaviour at `defer_until`:** the recommendation re-presents to
  whatever VTSO is on watch at that time; the deferring HM receives
  a notification

### 10.3 Defer reason

- **Closed catalogue** (V1.0 baseline; extensible per tenant in V1.x):
  - `awaiting_information` — additional data needed before deciding
  - `awaiting_weather` — decision depends on weather posture change
  - `awaiting_tide` — decision depends on tide window
  - `awaiting_stakeholder` — decision depends on pilot/towage
    confirmation
  - `low_severity_acceptable` — known-low-severity that doesn't
    warrant immediate action
  - `operational_priority_elsewhere` — higher-priority operation
    consuming attention
- **Free-text notes** field also captured for nuance

### 10.4 Revisit trigger

- **Primary:** the `defer_until` timestamp; system auto-re-presents
  the recommendation
- **Secondary:** the deferring HM (or current HM) explicitly invokes
  "revisit" before `defer_until` if context changes
- **Failure mode:** `defer_until` passes without the recommendation
  being addressed in the new lifecycle window → `DEADLINE_PASSED`
  fires per §7.9

### 10.5 Override

- **Verb semantic:** "I substitute my judgement for the system's
  recommended option"
- **Allowed actors:** HM only (V1.0)
- **Required inputs:**
  - `recommendation_id`
  - `override_choice` — one of:
    - explicit alternative from the recommendation's
      `sequencing_alternatives`
    - free-text description of a different action entirely (rare;
      requires extra justification)
  - `reason_code` (mandatory; closed catalogue)
  - `notes` (optional)

### 10.6 Override reason

- **Closed catalogue** (V1.0 baseline):
  - `local_knowledge` — HM has port-specific operational knowledge
    not captured in the engine
  - `policy_constraint` — operational policy constrains the system's
    recommended option
  - `stakeholder_constraint` — pilot/towage availability or
    constraints
  - `vessel_specific` — vessel characteristics (handling, crew,
    cargo) warrant a different choice
  - `regulatory_constraint` — regulatory requirement overrides system
    recommendation
  - `cost_consideration` — commercial trade-off rejection of system's
    cost-optimised choice
- **Free-text notes** field also captured

### 10.7 Alternative chosen

- **From recommendation's alternatives:** if the HM selects one of
  the `sequencing_alternatives` that the engine had already
  produced, the override is "alt-selected" — straightforward audit
  shape
- **External alternative:** if the HM articulates a different action
  entirely (not in the engine's alternatives), the override is
  "external" — requires more substantive notes; flagged in audit for
  executive review

### 10.8 Superseded recommendation

- The overridden recommendation enters a terminal "superseded by
  override" state — it does NOT re-present, even if `defer_until`-
  style timing would normally apply
- The HM's chosen alternative becomes the operational truth from
  this point onwards
- If the original conflict reappears (e.g. on the next `/api/summary`
  poll), the engine MAY generate a fresh recommendation — but the
  override applies to the original `recommendation_id` only, not to
  every future recommendation against the same conflict

### 10.9 Review obligation

- **Override audit:** every override is reviewable by HM (self),
  Executive (aggregate), and (in V1.x) external auditors
- **Trend monitoring:** override rate per HM per port is a tracked
  Executive metric — high override rates may signal engine
  miscalibration; low override rates may signal HM rubber-stamping
- **Quarterly review (V1.x):** structured review of overrides per
  port, owned by the Executive, with HM in attendance

### 10.10 Connection to deferred Phase 0 events

This workflow is the operational substrate the Phase 0 deferred
events require:

- `OPERATOR_DEFERRED` — fires on the defer action; the
  recommendation_id, defer_until, reason_code, and HM handle land
  in the payload
- `OPERATOR_OVERRODE` — fires on the override action; the original
  recommendation_id, override_choice (from-alts or external),
  reason_code, and HM handle land in the payload
- `DEADLINE_PASSED` — fires when a deferred recommendation's
  `defer_until` passes without revisit OR when the original
  `decision_deadline` passes without any action; carries the
  recommendation_id, the original owner, and the time of expiry

The §1.4.1 decision-time snapshot already pins the
`decision_deadline` (Phase 0.7b ships this). V1 adds the *response*
to that deadline.

---

## 11. Executive Review Flow

The Executive role consumes operational data without taking
operational action. V1 defines four review cadences.

### 11.1 Daily brief

- **Trigger:** end of the operational day (port-defined; typically
  ~18:00 local)
- **Producer:** HM signs off the Port Brief; system distributes
- **Consumer:** Executive (primary), HM (record), Shift Supervisor
  (context for next-day shift)
- **Content:** vessel movements, key conflicts and resolutions,
  notable escalations, weather posture, operational KPIs
- **Audit posture:** Executive reads emit `AUDIT_READ` (proposed)

### 11.2 Weekly trend review

- **Trigger:** weekly (typically Monday morning for the previous
  week)
- **Producer:** automated trend computation from accumulated audit
  data
- **Consumer:** Executive (primary)
- **Content:** throughput, dwell time averages, incident counts by
  severity, override rate by HM, escalation closure time by SS,
  pending-queue depth trends
- **Audit posture:** `AUDIT_READ` per trend query

### 11.3 Monthly incident review

- **Trigger:** monthly; explicitly scheduled
- **Producer:** Executive convenes; HM presents incidents from the
  month; SS supports with handover context
- **Consumer:** Executive (lead), HM, SS, optionally board / port
  trust representatives
- **Content:** replay of any incidents flagged for executive
  attention (§12); review of override patterns; review of
  escalation patterns; any operational policy recommendations
- **Audit posture:** `INCIDENT_REPLAYED` events for each incident
  reviewed (candidate future event — §14)

### 11.4 Exception review

- **Trigger:** ad-hoc; on the Executive's request, or auto-triggered
  by specific signals (e.g. severity-critical incident, override
  rate spike, audit-chain integrity warning)
- **Producer:** automated exception detection + manual Executive
  request
- **Consumer:** Executive
- **Content:** structured analysis of the triggering exception

### 11.5 What Executives CAN see

- KPI tiles (throughput, utilisation, incident counts)
- Trend charts (rolling windows)
- Daily Port Brief (signed-off version only)
- Aggregated incident summaries
- Override and escalation rate trends (per port, per HM, per SS)
- Replay of explicitly-flagged incidents

### 11.6 What Executives should NOT see

- Real-time conflict detail (operational, not strategic)
- Free-text resolution notes from individual VTSO actions
  (operational privacy)
- Full UKC numbers and detailed engineering data (technical, not
  decision-relevant at executive level)
- Pilot identities at named-individual level (use de-identified
  capability descriptors)
- Stakeholder-side delay reports (operational; not executive)

### 11.7 Whether executive reads should be audited

- **Recommendation:** yes, every executive read emits an
  `AUDIT_READ` meta-event
- **Rationale:** evidentiary record of governance oversight; supports
  external regulator scrutiny if/when it arrives; supports
  trend analysis of "who is paying attention to what"
- **Trade:** small audit-ledger noise vs. evidentiary completeness;
  the recommendation favours noise

### 11.8 Open questions

- **Anonymisation level for individual vessel records in Executive
  view** — commercial sensitivity with line operators
- **Self-serve trend reports vs HM-sign-off** — should the Executive
  pull reports directly, or do they receive HM-approved summaries?
- **Multi-port executive scope** — does one Executive role span
  multiple ports in a port group, or is each port discrete?
- **External board members as Executives** — distinct Executive role,
  or restricted Executive permissions?

---

## 12. Incident / Replay Flow

Post-hoc reconstruction of an operational decision is the audit
ledger's primary external-facing value. V1 defines an incident
lifecycle and a structured replay mechanism.

### 12.1 What counts as an incident

Operational events warranting post-hoc review. Closed catalogue
(V1.0 baseline):

- Port closure or weather hold
- Severe conflict with disputed resolution
- Vessel emergency (collision, grounding, fire, medical, security)
- Vessel detention or pilot refusal
- Infrastructure failure with operational impact
- Override of high-severity recommendation
- Cross-tenant operational coordination event (V1.x — when
  multi-tenant arrives)
- Regulatory inquiry (V1.x — when external auditor role arrives)

### 12.2 Replay trigger

- **Manual:** HM or Executive flags an event for review; assigns an
  `incident_id`
- **Automatic:** specific event patterns auto-flag (e.g. high-severity
  override + bypass escalation = candidate incident)
- **External:** regulator or insurance inquiry references a specific
  time window (V1.x; routed through the Executive)

### 12.3 Replay audience

- **HM** (primary) — reviews own and SS's actions
- **Executive** — reviews aggregated; may drill in for monthly
  reviews
- **Shift Supervisor** — supports HM's review with shift-window
  context
- **External regulator / auditor** — V1.x; read-only access via
  Executive-mediated query
- **NOT:** VTSO, Stakeholder (out of authority scope for replay
  view)

### 12.4 Replay data requirements

For a replay to be operationally meaningful, the audit ledger must
provide:

- Every operationally-relevant event in the time window
- Hash-chained intact across the entire window (Phase 0
  `verify_chain` proves this)
- Each event tagged with actor / role / scope / session_id
- Each ownership transfer auditable
- Each escalation chain reconstructible end-to-end
- Each authority decision (approve, override, defer, close) with
  reason code and notes
- Sufficient §1.4.1 decision-time snapshot data on every
  `RECOMMENDATION_GENERATED` to reconstruct *what the engine knew*
  at decision time

### 12.5 Audit chain requirements

- **No gaps:** every `sequence_no` from chain start to replay end
  must be present
- **No tamper:** `verify_chain` returns `ok=True` across the
  replay window
- **No silent re-seeding:** if the chain was reset (regulatory,
  forensic), the reset must itself be an audit event with explicit
  cause
- **Per-tenant integrity:** replay is bounded by tenant; no
  cross-tenant data leakage

### 12.6 Replay output

- **Timeline view:** chronological list of all events in the window
  with actor, action, target, and outcome
- **Authority chain view:** any escalation chains expanded to show
  source → target → closure path
- **Decision artefact view:** for each recommendation involved,
  show the §1.4.1 snapshot, the alternatives considered, the chosen
  action (or non-action), and the reason
- **Ownership transfer view:** ownership history of each contested
  state across the window
- **Audit chain integrity verification:** confirmation that
  `verify_chain` returns clean over the replay window

### 12.7 Regulator / external reviewer future role

- **Status:** deferred to V1.x (per PR #30 §14.4 — defer external
  auditor role)
- **When relevant:** when a customer tenant has regulatory
  obligations requiring external review access
- **Posture:** read-only; bounded by tenant and replay window;
  every read emits `AUDIT_READ`; access mediated by Executive
  approval

### 12.8 Missed escalation discovery

- **Pattern:** during replay, the reviewer notices an escalation
  that should have happened but didn't (e.g. a high-severity
  recommendation that aged out without escalation)
- **Output:** the replay tool surfaces this as a "missed
  escalation" finding for HM / Executive consideration
- **Audit posture:** the *discovery* is itself an event (proposed
  `INCIDENT_FINDING_RECORDED`) so the post-hoc analysis is part of
  the ongoing ledger

### 12.9 Open questions

- **Replay window granularity:** should the system enforce
  reasonable upper bounds (e.g. max 30 days per replay), or are
  multi-month replays acceptable?
- **Replay performance:** what's the acceptable query latency for a
  shift-scope replay vs. a multi-day replay? Affects DB indexing
  strategy.
- **Replay output format:** UI-rendered, exportable PDF, structured
  JSON, or all three?
- **Concurrent replay:** can two HMs replay the same window
  concurrently without affecting each other's audit `AUDIT_READ`
  trail?

---

## 13. Stakeholder Coordination Flow

Stakeholders (pilots, towage, mooring, terminal staff) consume
operational context to coordinate their assignments. V1 defines a
narrow but explicit workflow.

### 13.1 Assignment notification

- **Trigger:** VTSO or HM creates / confirms an assignment (vessel
  movement + pilot + tugs + mooring + berth window)
- **Notification:** stakeholders assigned to the movement receive
  push (transport TBD — V1.x decision)
- **Audit posture:** `STAKEHOLDER_NOTIFIED` (candidate future event)
  with stakeholder handle, assignment reference, notification
  channel, notification time

### 13.2 Confirmation

- **Trigger:** stakeholder receives notification, reviews the
  assignment, acknowledges receipt
- **Actor:** stakeholder
- **Mechanism:** mobile-first UI confirm button
- **Audit event:** `OPERATOR_ACTED` with
  `actor_type='stakeholder'` (proposed new actor type),
  `action_type='confirm_assignment'`, assignment reference

### 13.3 Delay reporting

- **Trigger:** stakeholder identifies a delay or issue affecting
  their assignment (pilot late, tug malfunction, mooring issue)
- **Actor:** stakeholder
- **Mechanism:** mobile-first UI delay-report flow with structured
  reason and optional notes
- **Audit event:** `OPERATOR_ACTED` with
  `action_type='report_delay'`, reason code, affected assignment,
  expected delay duration
- **Operational consequence:** the delay enters the VTSO's guidance
  panel; VTSO may need to re-plan or escalate

### 13.4 Issue reporting

- **Distinct from delay:** issues are operationally-impacting
  conditions not yet resulting in a delay (vessel condition
  observation, weather concern, equipment status)
- **Actor:** stakeholder
- **Mechanism:** mobile-first UI issue-report flow
- **Audit event:** `STAKEHOLDER_REPORTED_ISSUE` (candidate future
  event) with reason code, affected assignment
- **Operational consequence:** the issue enters the VTSO's guidance
  panel for awareness

### 13.5 Limited stakeholder visibility

Per PR #30 §4.5:

- Vessels: scoped to their own assignments only
- Berths: scoped to their own assignments only
- Pilotage / towage: own assignments only
- Weather / tides: read-only general view
- Conflicts / guidance / dashboard: NOT visible
- Other stakeholders' assignments: NOT visible

This is operationally narrow by design — the stakeholder role
answers "what is my next job and what do I need to know about it,"
nothing more.

### 13.6 Kyber boundary for pilot workflows

Per PR #30 §11:

- **Kyber owns:** pilot rostering, availability, job sequencing,
  competency tracking, pilot-side detailed UX
- **Horizon V1 stakeholder role:** shows pilots their broader port
  context (vessel ETAs, weather, tides, berth readiness for their
  assignment) and accepts their delay-/issue-reports back into the
  VTSO panel
- **Integration:** API/event-based when implemented; Horizon V1.0
  does not depend on Kyber code

### 13.7 Towage / mooring / terminal coordination

- Towage providers and mooring gangs use the Horizon stakeholder
  view directly (no dedicated system equivalent to Kyber)
- Terminal staff may use the stakeholder view for berth-side
  coordination (specific use cases TBD per port)
- All three groups have identical visibility scope rules and the
  same two operational verbs (confirm assignment, report
  delay/issue)

### 13.8 Cross-stakeholder dependencies

A vessel movement often involves a pilot, multiple tugs, and a
mooring gang. The movement is operationally coupled:

- If the pilot reports a 30-minute delay, all coupled stakeholders
  need to know
- If the tug reports a malfunction, the pilot may not be able to
  proceed safely
- Cross-dependency notification is a V1.x design point — V1.0
  delivers the individual stakeholder view; V1.x adds the coupling

### 13.9 Open questions

- **Authentication for stakeholders** — federated with Kyber,
  port-issued, or Horizon-direct?
- **Notification transport** — SSE, WebSocket, SMS, push
  notification, all-of-above per tenant?
- **Offline operation** — can a pilot view assignment and weather
  while their device is offline?
- **Cross-port stakeholders** — a pilot may work multiple ports
  within a tenant; does the system carry their assignment context
  across ports?
- **Stakeholder identity vs. capability** — Horizon shows
  "Pilot A — qualified for this port + LOA class" rather than
  "Pilot Jane Smith"; how much of the identity does the audit
  ledger record vs. de-identify?

---

## 14. Workflow to Audit Event Mapping

A consolidated mapping of workflow moments to audit event types.
Sorted by lifecycle area.

### 14.1 Currently emissible events (Phase 0 live)

| Workflow moment | Audit event | Notes |
|---|---|---|
| User authenticates | `SESSION_STARTED` | Phase 0.6 live |
| User logs out | `SESSION_ENDED` | Phase 0.6 live |
| Engine detects a new conflict | `CONFLICT_DETECTED` | Phase 0.7a live; dedup per `(tenant_id, conflict_id)` |
| Engine produces a recommendation | `RECOMMENDATION_GENERATED` | Phase 0.7b live; §1.4.1 decision-time snapshot embedded |
| Recommendation is delivered to an authenticated surface | `RECOMMENDATION_PRESENTED` | Phase 0.7c live; tagged with `actor_handle`, `surface` |
| Operator commits an explicit action (whatif_apply, whatif_clear, send_brief) | `OPERATOR_ACTED` | Phase 0.8a live |

### 14.2 Deferred events (Phase 0 reserved; V1 emits)

| Workflow moment | Audit event | Becomes emissible at |
|---|---|---|
| Operator defers a recommendation | `OPERATOR_DEFERRED` | V1.2 (HM defer action) |
| Operator overrides a recommendation | `OPERATOR_OVERRODE` | V1.2 (HM override action) |
| Recommendation deadline passes without action | `DEADLINE_PASSED` | V1.2 (requires pinned-deadline lifecycle) |
| Session ends with pending presented recommendations | `SESSION_ENDED_WITHOUT_ACTION` | V1.2 |

### 14.3 Candidate future events (V1+ proposals)

These are NOT in the closed event_type set in `audit.py` today.
Each would require a schema CHECK constraint extension (a
straightforward Alembic migration adding values to the closed set;
the migration would also touch the regression-gate test that pins
the authorised event_type list).

| Workflow moment | Proposed event | Section reference |
|---|---|---|
| Shift opens | `SHIFT_OPENED` | §4.2 |
| Shift closes | `SHIFT_CLOSED` | §4.8 |
| Handover note submitted | `HANDOVER_CREATED` | §4.6, §9.2 |
| Handover formally accepted | `HANDOVER_ACCEPTED` | §4.7, §9.7 |
| Escalation initiated | `ESCALATION_CREATED` | §8.1–§8.3 (NB: §7.8 also names `OPERATOR_ESCALATED` — V1 decision needed on whether the event is named after the verb or the artefact) |
| Escalation closed by target | `ESCALATION_RESOLVED` | §8.6 |
| Operator acknowledges a recommendation | `OPERATOR_ACKNOWLEDGED` | §7.4 |
| Authority delegation granted | `DELEGATION_GRANTED` | §4.4, §5.6 |
| Authority delegation revoked | `DELEGATION_REVOKED` | §4.4, §5.6 |
| Incident opened for review | `INCIDENT_OPENED` | §12.2 |
| Incident replayed | `INCIDENT_REPLAYED` | §11.3, §12.6 |
| Audit trail read by reviewer | `AUDIT_READ` | §7.11, §9.1, §9.5, §11.7, §12 |
| Berth availability changed | `BERTH_AVAILABILITY_CHANGED` | (from PR #30 §4.6) |
| Stakeholder receives assignment notification | `STAKEHOLDER_NOTIFIED` | §13.1 |
| Stakeholder reports a non-delay issue | `STAKEHOLDER_REPORTED_ISSUE` | §13.4 |
| Recommendation explicitly closed (vs. resolved) | `RECOMMENDATION_CLOSED` | §7.10 |
| Recommendation obsoleted (engine flipped) | `RECOMMENDATION_OBSOLETED` | already reserved in the Phase 0 closed set but not yet emitted |

### 14.4 Naming and scope notes

- The closed event_type set in `audit.py` is 25 values today
  (Phase 0). V1 expansion adds 12-18 candidate events. Each
  addition is a Phase-0.5a-style migration that extends the CHECK
  constraint.
- The regression gate test
  `test_no_emission_event_types_outside_authorised_set` would need a
  deliberate baseline update at V1.x.x to reflect each event added.
- V1 must NOT add events without explicit design review for each.
  Speculative additions dilute the ledger.

---

## 15. Workflow to Screen Implications

This document does NOT design screens. It defines what screens are
*operationally implied* by the workflow model. Claude Design / CX
work happens separately and consumes this section as authoritative
for what surfaces must exist; layout, navigation, and visual
treatment are downstream.

### 15.1 VTSO live watch console

- **Primary need:** sustain operational attention across multiple
  concurrent conflicts and recommendations
- **Implied surfaces:** active recommendations panel; conflict
  list with severity sort; guidance feed; weather/tides; vessel
  roster; pilotage/towage assignment overview
- **Required interactions:** acknowledge, resolve, run-what-if,
  escalate (with reason code), add notes
- **Time visibility:** every pending item shows time-since-presented
  and time-until-deadline

### 15.2 Shift Supervisor handover console

- **Primary need:** capture operational state across the shift and
  transfer it cleanly to the incoming SS
- **Implied surfaces:** structured handover composer; pending-item
  inventory; escalation queue; VTSO action review queue; outgoing
  vs. incoming SS view; clarification exchange
- **Required interactions:** write handover, accept handover,
  request clarification, escalate to HM, review VTSO action

### 15.3 Harbour Master approval console

- **Primary need:** decide on authority-level matters efficiently
  with full context
- **Implied surfaces:** pending approvals queue; full audit trail
  drill-down; override / defer / approve flows with reason capture;
  Port Brief preview and sign-off
- **Required interactions:** approve port closure, override
  recommendation, defer with deadline, sign off Port Brief, review
  audit by time window or by actor

### 15.4 Executive review dashboard

- **Primary need:** strategic oversight without operational
  engagement
- **Implied surfaces:** KPI tiles; trend charts (weekly/monthly);
  incident summary panel; Port Brief reader; override/escalation
  rate trends
- **Required interactions:** drill into trend (read), open incident
  replay (read), open Port Brief (read)

### 15.5 Stakeholder assignment feed

- **Primary need:** mobile-first, single-screen, "what's my next
  job"
- **Implied surfaces:** next assignment card; current assignment
  status; weather/tide for assignment window; confirm button;
  delay/issue report buttons
- **Required interactions:** confirm assignment, report delay,
  report issue

### 15.6 Incident replay view

- **Primary need:** reconstruct operational decisions post-hoc
- **Implied surfaces:** timeline; authority chain visualisation;
  decision-snapshot viewer; ownership-transfer history;
  chain-integrity status
- **Required interactions:** scrub time, drill into individual
  events, export findings, record `INCIDENT_FINDING_RECORDED`

### 15.7 Admin workflow configuration

- **Primary need:** tenant-administrative configuration of workflow
  parameters (reason code catalogues, shift definitions,
  escalation timeouts, retention classes)
- **Implied surfaces:** tenant config editor with role-permission
  matrix view; reason-code catalogue editor; shift definition
  editor
- **Required interactions:** edit catalogues, define shifts,
  configure notification routing
- **Note:** this surface only emerges after the V1.0 RBAC
  foundation; do not design before then

---

## 16. Build Sequencing Recommendation

The workflow model's build sequence parallels (and depends on) the
permission model's sequence. Each phase requires its own explicit
authorisation before implementation begins.

### V1.0 — Shift / session foundation and role-scoped operational context

- Shift schema (start, end, supervisor, scope)
- Session model carrying `shift_id` claim
- Role-scoped `/api/summary` (per PR #30 §8.1)
- Foundational ownership concept on existing conflict / recommendation
  records (system at detection; transfers on presentation)
- No new operational verbs yet; deliverable is the shift / session /
  scope skin around Beta 10's read surface
- Audit additions: `SHIFT_OPENED`, `SHIFT_CLOSED` event type extension
  to the closed set

### V1.1 — VTSO recommendation action surface

- VTSO console (operational layout per §15.1)
- Verbs: acknowledge, resolve, run-what-if, flag, add notes
- Recommendation lifecycle stages: presented → acknowledged →
  resolved
- Audit additions: `OPERATOR_ACKNOWLEDGED`, expanded `OPERATOR_ACTED`
  action types (`conflict_resolve`)
- Reason code capture for resolve actions

### V1.2 — Harbour Master defer / override / approval surface

- HM console (per §15.3)
- Verbs: approve, override, defer, close-port, sign-off
- Recommendation lifecycle stages: deferred, overridden, closed
- Audit additions: `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`
  (Phase 0 reserved events become emissible); also `DEADLINE_PASSED`
  per §7.9
- Reason code catalogues for defer and override

### V1.3 — Shift Supervisor handover and escalation

- SS console (per §15.2)
- Verbs: escalate, write-handover, accept-handover,
  request-clarification, review-VTSO-action
- Escalation chain (§8) end-to-end
- Audit additions: `HANDOVER_CREATED`, `HANDOVER_ACCEPTED`,
  `ESCALATION_CREATED` / `OPERATOR_ESCALATED` (naming decision),
  `ESCALATION_RESOLVED`, `AUDIT_READ` (review surface)
- Multi-step escalation chain reconstructability

### V1.4 — Executive review and trend model

- Executive dashboard (per §15.4)
- Trend computation infrastructure (needs accumulated V1.0–V1.3 data;
  typically 30+ days post-V1.3)
- Daily / weekly / monthly review cadences (§11.1–§11.4)
- Audit additions: `AUDIT_READ` for executive surfaces

### V1.5 — Stakeholder coordination feed

- Stakeholder mobile UI (per §15.5)
- Verbs: confirm-assignment, report-delay, report-issue
- Notification transport decision (open question §13.9)
- Kyber boundary contract finalised per PR #30 §11
- Audit additions: `STAKEHOLDER_NOTIFIED`,
  `STAKEHOLDER_REPORTED_ISSUE`, expanded `OPERATOR_ACTED` action
  types (`confirm_assignment`, `report_delay`)

### V1.6 — Incident replay and audit read model

- Incident lifecycle surface (per §15.6)
- Replay-driven audit queries
- `INCIDENT_OPENED`, `INCIDENT_REPLAYED`,
  `INCIDENT_FINDING_RECORDED` audit additions
- Performance hardening of audit reads at multi-day replay scope
- External-auditor read access (V1.x — deferred unless contractually
  required earlier)

### V1.x — Beyond the initial six phases

- Authority delegation (`DELEGATION_GRANTED`,
  `DELEGATION_REVOKED`) — only if/when small-port operational
  patterns require it
- VTSO defer permission (currently HM-only) — only if operational
  feedback demands
- Cross-port escalation — only when multi-port tenants exist
- Federated stakeholder auth — when Kyber integration warrants
- Cross-stakeholder coupling (pilot + tug + mooring dependency
  surfacing) — once V1.5 is live and operational patterns are clear

---

## 17. Open Questions

Surfaced for review before V1.0 design lock. None have answers in
this document. Numbering matches the planning recommendations from
the previous turn, plus additional questions surfaced during this
draft.

1. **Single supervisor vs co-supervision** in larger ports — is one
   SS authoritative per shift, or can two SSs share a shift with
   defined sub-scopes?

2. **Can a VTSO acknowledge another VTSO's pending item?**
   Currently the model says no (ownership stays with the named
   VTSO). But in a busy watch with one operator overwhelmed and
   another idle, this rule may be too rigid.

3. **Canonical shift boundary** — wall-clock window (8:00–16:00),
   login session, or explicit "start-shift" action? Different ports
   may operate differently.

4. **Deferral revisit cadence** — system-prompted at `defer_until`,
   operator-prompted before then, or always-deferred-once-decided?

5. **Override permanence** — is an override permanent, or does it
   eventually require closure / review? If reviewed, by whom and on
   what cadence?

6. **Cross-port escalation** — when multi-port tenants exist, can a
   Brisbane HM escalate to a tenant-level authority spanning multiple
   ports? What does that authority look like?

7. **Replay scope** — full audit window, per-incident bounded slice,
   or both? Performance / privacy / regulatory considerations all
   pull differently.

8. **Stakeholder push without Horizon authentication** — acceptable
   for low-stakes assignments (e.g. simple confirm receipt), or must
   every stakeholder always authenticate?

9. **Handover concurrency** — what if outgoing SS hasn't finished
   writing when incoming SS arrives and wants to read? Real-time
   collaboration, strict locking, or some hybrid?

10. **High-severity acknowledgement timeout** — if a VTSO doesn't
    acknowledge a high-severity item within N seconds, who else
    gets notified? Should the system auto-escalate?

11. **Acting Harbour Master delegation** — when an HM delegates to
    SS as Acting HM, is that a transient role assumption (with all
    HM permissions for a time window), or a separate role grant
    requiring re-authentication?

12. **Conflict ownership during quiet periods** — does anything own
    a presented conflict when no one is actively working it, or is
    ownership only "live" when there's an active session on the
    relevant watch?

13. **Whether this document becomes source of truth for deferred
    event semantics** — once `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`,
    `DEADLINE_PASSED`, `SESSION_ENDED_WITHOUT_ACTION` become
    emissible, this document defines *when* they fire. The Phase 0
    deferral note (`phase0-008b-deadline-passed-deferral.md`)
    defines *why* they were deferred. The combination is the
    canonical reference.

14. **Naming convention for escalation events** — `ESCALATION_CREATED`
    + `ESCALATION_RESOLVED` (artefact-named) vs. `OPERATOR_ESCALATED`
    (verb-named). Earlier sections lean toward verb-named; §14.3
    proposes artefact-named for clarity. Decision needed before
    V1.3.

15. **Notification transport** — SSE, WebSocket, push, SMS, email,
    Slack/Teams — per tenant, per role, per event severity? This
    needs an architectural decision before V1.1 ships.

16. **`AUDIT_READ` granularity** — emit per query, per page-view,
    per record? Trade between fidelity and ledger noise. Suggested
    default: per "review session" with the scope of items reviewed
    in the payload.

17. **Mid-shift delegation as a V1.0 feature** — required from day
    one, V1.x phase-in, or omitted entirely? Affects schema and UI.

18. **External regulator / auditor access** — V1.x or later;
    deferred per PR #30 §14.4 but the workflow model assumes the
    surface eventually exists (replay flow, `AUDIT_READ` events).
    Confirming the deferral target phase informs storage retention
    decisions.

19. **What constitutes "operationally significant" for shift-handover
    free-text** — guidance for SSs writing handovers; needed to
    avoid the spectrum of every-event-recorded vs. nothing-recorded.

20. **Reason-code catalogue ownership** — global vs. per-tenant
    customisable? The defaults defined in §8.5 and §10.3 / §10.6
    are starting points; tenants may want to extend.

---

## 18. Recommendations

For the V1.0 design review:

1. **Treat this document as workflow model v0.1.** Expect iteration
   to v0.2, v0.3, ... as operational stakeholders push back on
   assumptions. The temporal/state-machine model is foundational;
   structural objections are cheaper to surface now than after
   V1.1 ships.

2. **Review with operational stakeholders before implementation.**
   PR #30 and this document together define the entire V1
   user-and-workflow contract. Both must be reviewed with at least
   one actual operating Harbour Master, one actual VTSO, and one
   Shift Supervisor before V1.0 implementation begins. The role
   profiles in PR #30 and the lifecycle stages in this document
   are claims that operational reality will validate or correct.

3. **Do not code V1 workflow until both the permission model and
   the workflow model are accepted.** V1.0 implementation requires
   substantial server.py, audit.py, migrations/, and frontend
   changes. The risk of building against an unstable model is high.
   Accept the documents (v1.0 of each, or whatever version is
   stable) before any branch beyond docs/ is opened.

4. **Use this as input to Claude Design / CX work.** The screen
   implications in §15 are the operationally-required surfaces.
   Claude Design / external CX should consume this document as
   the source of truth for *what screens must exist*; visual,
   navigation, and interaction design is downstream.

5. **Use this to decide which V1 audit events are truly required.**
   The Phase 0 closed set of 25 event types is a strong baseline.
   V1 expansion (~12-18 new events per §14.3) is a substantial
   addition. Each new event should be challenged: "does the
   evidentiary record genuinely need this, or can it be inferred
   from existing events?" Default to "no" for new events; require
   explicit justification.

6. **Continue to keep Stage E-prod paused unless there is a separate
   commercial or operational reason to activate it.**
   Activating production audit emission before V1's user/role
   identity is on each audit row means recording Beta-10-shaped
   actor attribution into the persistent ledger. PR #30 §14.6
   already surfaced this recommendation; this document reinforces
   it. The trade-off is between a short-lived run of Beta-10-shaped
   audit data (cheap, recoverable) vs. blocking commercial
   demonstration of the audit capability. Treat as a business
   decision; both are defensible.

---

**End of v0.1.** Reviewer comments expected before v0.2.
