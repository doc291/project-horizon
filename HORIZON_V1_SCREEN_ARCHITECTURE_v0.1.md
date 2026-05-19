# Horizon V1 — Screen Architecture (v0.1)

**Status:** Design draft — V1 planning only
**Document version:** 0.1
**Date:** 2026-05-15
**Companion documents:**
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30, merged at `4c4940f`)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31, open for review)
**Implementation status:** None. **This is not implementation approval.**
**Visual design status:** None. **This is not visual design.**
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

This document defines the **V1 screen and navigation architecture**
for Project Horizon — the structural organisation of operational UI
surfaces that translate workflows into screens. It is the third
foundational V1 design document, completing the planning triad:

| Document | Answers | Scope |
|---|---|---|
| Permission Model (v0.1) | **WHO** uses Horizon and **WHAT** they can do | role / permission / scope contract |
| Operational Workflow Model (v0.1) | **HOW** Horizon is operationally used | temporal / coordination state machines |
| **Screen Architecture (v0.1, this doc)** | **WHERE** workflows and operational surfaces live | screen hierarchy, navigation, layout strategy, role-specific interaction model, device strategy |

This document is **not visual design**. It defines screen hierarchy,
navigation structure, operational layout strategy, the workflow-to-
screen mapping, the device strategy, and the role-specific
interaction model. It does NOT define colour, typography, iconography,
component-level visual treatment, interaction microcopy, or
animation. Those decisions are downstream and belong to Claude Design
/ external CX work.

This document **consumes the permission model and the workflow model
as authoritative inputs**. Where this document references a role's
authority over a screen surface, the authority itself is defined in
the permission model; where it references the lifecycle stage of a
recommendation or escalation, the stage semantics are defined in the
workflow model. This document defines only the *spatial* and
*navigational* projection of those contracts into UI.

**This document does not authorise any implementation.** It is v0.1
of an evolving structure. The Phase 0 Beta 10 baseline remains
protected at `phase-0-complete @ 4ad4aae`; no V1 screen code, route
layout, navigation framework, or front-end framework selection will
be done until this document and its two companions have been
reviewed and accepted by operational stakeholders and (separately) by
the design team.

---

## 2. Design Principles

The following eight principles govern every screen architecture
decision in V1. They sit alongside the principles of the permission
model and the workflow model. Where future implementation choices
conflict with these principles, the principle wins.

1. **Operational clarity over visual density.**
   Operations are 8-12 hour activities under cognitive load. A screen
   that crams more information per pixel may look impressive in a
   demo but degrades sustained operator performance. V1 prefers
   negative space, deliberate hierarchy, and progressive disclosure
   over information-saturated layouts.

2. **Time-critical information first.**
   Every screen surfaces time pressure (deadline approaching,
   escalation aging, handover window closing) before it surfaces
   passive context. The operator's eye should land on what needs
   action *now* before what is merely true.

3. **Ownership visibility always visible.**
   Every screen must display, at all times, who owns the current
   operational state (e.g. "VTSO Jane Smith owns this conflict",
   "HM Mark Brown is reviewing this override"). Ownership ambiguity
   is the most common failure mode of coordination systems.

4. **Actions must be explicit and auditable.**
   No silent state changes. Every operator-initiated action requires
   a deliberate UI gesture (button, confirmed dialog, or structured
   form) and produces an audit event per the workflow model §14.
   "Click-to-act" patterns that fire actions without confirmation are
   prohibited for actions with audit consequences.

5. **Escalation state must be obvious.**
   When something has been escalated, every screen that touches it
   should show that — the VTSO sees their escalation in their
   tracking queue, the SS sees it in their inbound queue, the HM
   sees it in their authority queue, and the original conflict shows
   "currently escalated to SS" on every operator's view of it.

6. **Workflows should minimise context switching.**
   A VTSO resolving a berth-overlap conflict should not need to
   navigate to three different screens to see weather, tides, and
   pilotage status. The conflict surface includes the operational
   context the conflict needs. Context switching is friction and
   friction degrades decision quality.

7. **Replay and review must mirror live operational views.**
   When an HM reviews an incident from three days ago, the screens
   they see should look and feel substantially like the live VTSO
   screens of three days ago — with the additional affordances of
   scrubbable time and decision-snapshot drilling. The cognitive
   continuity between live and replay is itself an audit guarantee.

8. **Mobile surfaces are role-specific, not full-platform replicas.**
   Mobile is for stakeholder coordination and executive summary
   review — not for VTSO operational work or HM authority decisions.
   We do not attempt to compress the operational console onto a
   phone. Each device tier has the screens appropriate to that tier
   and only those.

---

## 3. Application Architecture Overview

Horizon V1 organises into **seven major application domains**.
Each domain has a primary user audience, a primary operational
purpose, and clear boundaries against the others.

### 3.1 Live Operations

The always-on real-time operational layer. Vessel movements,
conflict detection, weather/tides, pilotage and towage assignments,
guidance feed. This is the operational nerve centre.

- **Primary audience:** VTSO; secondary: HM, SS (read access)
- **State:** continuously refreshing; sub-30-second polling cadence
- **Persistence:** transient operational state; persistent audit trail

### 3.2 Coordination & Recommendations

The recommendation lifecycle layer. Conflicts surfaced with
recommendations; what-if scenario surface; resolution and
acknowledgement actions; defer/override workflow.

- **Primary audience:** VTSO (action surface); HM (approval surface)
- **State:** per-recommendation lifecycle state (presented →
  acknowledged → resolved/deferred/overridden/escalated/closed)
- **Persistence:** audit events; recommendation_actions table per
  permission model §9.2

### 3.3 Shift & Handover

The supervisory coordination layer. Handover composition,
acceptance, pending-item review, VTSO action review, escalation
queue.

- **Primary audience:** Shift Supervisor; secondary: HM (review)
- **State:** per-shift bounded; transitions at handover events
- **Persistence:** shift_handover table; shift table; audit events

### 3.4 Executive Review & Trends

The strategic oversight layer. Daily Port Brief consumption,
weekly trend dashboards, monthly incident review, exception
review.

- **Primary audience:** Port Executive
- **State:** aggregated; computed from accumulated audit data
- **Persistence:** read from existing tables; no new operational
  state created by Executive consumption

### 3.5 Stakeholder Coordination

The peripheral participant layer. Mobile-first stakeholder
experience (pilots, towage, mooring, terminals); assignment
notifications; confirmation; delay/issue reporting.

- **Primary audience:** Port Stakeholders
- **State:** per-assignment scope; very narrow visibility
- **Persistence:** stakeholder_assignments table; audit events

### 3.6 Replay & Incident Review

The post-hoc reconstruction layer. Incident timeline replay,
recommendation chain visualisation, escalation chain reconstruction,
audit chain verification.

- **Primary audience:** HM (primary); Executive (monthly review); SS
  (shift context); external regulator/auditor (V1.x, deferred)
- **State:** reconstructed from audit ledger; bounded replay window
- **Persistence:** read-only against audit ledger; emits
  `AUDIT_READ` and `INCIDENT_REPLAYED` per workflow §14

### 3.7 Administration & Configuration

The tenant-administration layer. User management, role/permission
assignment, scope configuration, reason-code catalogues, shift
definitions, notification routing.

- **Primary audience:** tenant administrator (likely a permission
  granted to HM in V1.0)
- **State:** tenant configuration; rare changes
- **Persistence:** config schema tables per permission model §9

### 3.8 Domain relationships

These seven domains are not silos. Key operational relationships:

- **Live Operations** continuously feeds **Coordination &
  Recommendations** (every conflict detected becomes a recommendation
  candidate)
- **Coordination & Recommendations** feeds **Shift & Handover**
  (every recommendation action lives in some shift; handover
  inventories pending items)
- **Shift & Handover** feeds **Executive Review** (handover notes and
  shift summaries become inputs to the daily Port Brief)
- **Live Operations** + **Stakeholder Coordination** are coupled
  (stakeholder delay reports surface in the VTSO guidance panel)
- **Replay & Incident Review** is a cross-cutting consumer of all
  other domains' historical state
- **Administration** is upstream of all others (configuration changes
  affect every domain's behaviour)

---

## 4. Primary Navigation Model

V1 navigation must surface role-relevant work without forcing
operators to hunt for it, and must keep operational context visible
across all navigation transitions.

### 4.1 Top-level navigation structure

**Recommendation:** **persistent left rail** with role-filtered
primary destinations, plus a **persistent top bar** carrying global
operational context (active port, shift, user identity, notifications,
incident-active indicator).

The left rail is preferred over top navigation because:

- It scales: more primary destinations can appear without crowding
- It's stable: left-rail destinations don't reflow as the window
  resizes (top-nav items can wrap or hide behind a hamburger)
- It supports nested context: secondary navigation can appear in a
  sub-rail without overloading the top bar

The top bar is reserved for **persistent operational context**, not
primary navigation. Specifically:

- Active port indicator (with switch affordance if multi-port user)
- Active shift indicator + supervisor identity
- User identity + role badge
- Notification bell with unread count and severity indicator
- Active-incident banner (full-width, high-severity colour) when an
  incident is active in the current scope

### 4.2 Role-based navigation visibility

The left rail's contents differ per role. Each role sees only the
destinations its permissions grant access to. Hidden destinations
must NOT appear greyed-out — they must not appear at all (per
permission model §10.7).

**VTSO left rail:**
1. Live Operations
2. Coordination (conflicts + recommendations queue)
3. What-If
4. My Shift (read-only view of shift state + handover I contributed
   notes to)
5. Search (cross-cutting; replay-style read of operational history
   limited to current shift)

**Shift Supervisor left rail:**
1. Live Operations (read access; sees what VTSOs see)
2. Shift Console (handover, pending items, VTSO action review)
3. Escalations (queue inbound from VTSOs)
4. Coordination (read access)
5. Search (cross-shift read for current shift only)

**Harbour Master left rail:**
1. Authority Console (approvals, overrides, sign-offs)
2. Live Operations (full read)
3. Coordination (full read + approval surface)
4. Shift Review (read access to SS handover surface and audit trail)
5. Replay & Incidents (full access)
6. Administration (if tenant-admin permission granted)
7. Search (full audit read)

**Port Executive left rail:**
1. Dashboard (KPIs, trend tiles)
2. Port Brief (read)
3. Trends (weekly / monthly)
4. Incidents (aggregated; drill into specific reviews)

**Port Stakeholder left rail:**
- *Stakeholders use a mobile-first surface; there is no left rail in
  the traditional sense.* See §9 (Stakeholder Experience) and §13
  (Device Strategy).

**Marine Infrastructure** (when granted as a permission within HM
or SS):
- Adds a **Berths** destination to the HM or SS left rail
- Does NOT appear as a separate role on the left rail in V1.0 per
  permission model §14.3

### 4.3 Persistent operational context

The top bar's content is per-session and never disappears across
navigation:

- **Active port** — visible always; click to switch (multi-port
  users only)
- **Active shift** — visible always; includes shift_id + outgoing/
  incoming supervisor identity if mid-handover
- **User identity + role badge** — operator never sees a screen
  without knowing their own role
- **Notifications** — bell icon + count; clicking opens the
  notification drawer (§12)
- **Active-incident banner** — full-width banner appears when an
  incident is active in the current scope; remains until incident
  is closed; clicking jumps to the incident's Replay & Incident
  Review surface

### 4.4 Global port selector behaviour

Multi-port users (e.g. an HM authority class spanning multiple ports
in a tenant) see a port selector in the top bar:

- Default: most-recent active port for this user
- Switch: instant; emits a synthetic `PORT_CONTEXT_CHANGED` audit
  event (deferred capability per phase0-008b-deadline-passed-
  deferral.md analogue; treat as a future event for V1)
- All subsequent operations are scoped to the selected port until
  another switch
- Cross-port query (e.g. "show me incidents from all my ports") is
  a separate, explicitly-scoped action; never the default

### 4.5 Shift/session awareness

Every screen displays the active shift_id and supervisor identity in
the top bar. When the shift transitions:

- Outgoing supervisor sees a "handover in progress" indicator
- Incoming supervisor sees a "you are the incoming supervisor" indicator
- VTSO sessions see the new supervisor identity within seconds of
  formal handover acceptance

This is the visible substrate of the workflow model's
"single ownership of operational state at any moment" principle.

### 4.6 Notification access

The notification bell in the top bar opens a side drawer (not a
modal), allowing operators to triage notifications without losing
their current operational context. Notification model details
in §12.

### 4.7 Escalation visibility

Escalations are first-class navigation citizens:

- For VTSOs and SS: an Escalations destination appears on the left
  rail when they have at least one pending escalation; persists
  while any are open
- For HM: Escalations is always visible on the left rail
- A global Escalation indicator appears in the top bar when an
  escalation is targeted at the active user; visible across all
  screens

### 4.8 Active-incident visibility

When an incident is active in the user's scope:

- Top bar shows a high-severity-coloured banner
- Replay & Incident Review destination is highlighted in the left
  rail
- Active conflicts associated with the incident carry a visible
  incident-tag on every screen that shows them

### 4.9 Navigation primitives summary

V1 navigation comprises:

- **Persistent left rail** — primary destinations, role-filtered
- **Persistent top bar** — operational context, never hidden
- **Contextual sub-rail** — secondary navigation within a destination
  (e.g. inside Coordination, sub-rail for Active / Deferred /
  Overridden / Closed)
- **Contextual tabs** — for screens with parallel views of the same
  state (e.g. inside a conflict detail: Summary / Alternatives /
  History / Audit)
- **Command palette (V1.x)** — keyboard shortcut for power users
  (recommended deferral; V1.0 ships rail + bar only)
- **No modals for primary navigation** — modals are for confirmation
  dialogues only, not for switching screens

---

## 5. VTSO Operational Console

The VTSO console is the highest-traffic V1 surface and the closest
descendant of Beta 10's current UI. It is designed for desktop
primarily; tablet is acceptable; mobile is explicitly not the
primary target.

### 5.1 Layout strategy

The VTSO console organises into **four primary regions**:

1. **Live Vessel Map** — geospatial view of the port and approaches
2. **Active Movement Timeline** — temporal view of inbound/outbound
   vessel movements
3. **Coordination Stack** — conflict queue and recommendation queue
   (the primary action surface)
4. **Operational Context Strip** — weather, tides, pilotage/towage
   roster, port status

These four regions cohabit a single screen; they are NOT separate
tabs. Sustained operational attention requires all four to be
simultaneously visible.

### 5.2 Live Vessel Map

- Geographic visualisation of the port and approach areas
- Vessels as markers with velocity vectors
- Berths visible with status (available, occupied, maintenance)
- Channel restrictions and depth contours
- Click vessel → vessel detail overlay (drawer, not modal)
- Click berth → berth detail overlay
- **Must always remain visible** — operator situational awareness
  depends on it

### 5.3 Active Movement Timeline

- Horizontal timeline showing the next 4-6 hours
- Inbound vessels approaching (ETA markers)
- Outbound vessels departing (ETD markers)
- Pilotage and towage assignment windows overlaid
- Weather/tide windows overlaid
- Drag along timeline to see future state
- **Can collapse** to a compact ribbon when other regions need vertical
  space (e.g. during a complex conflict resolution)

### 5.4 Coordination Stack

- **Conflict Queue** — chronological list of active conflicts, severity-
  sorted, each showing ownership + time-pressure indicators
- **Recommendation Queue** — chronological list of active recommendations
  (presented but not yet acted on), each showing recommended option +
  decision deadline + acknowledged state
- Click conflict → conflict detail panel (replaces or augments the
  Coordination Stack; design decision deferred to Claude Design)
- Action affordances (acknowledge, resolve, run-what-if, escalate)
  are part of the conflict detail panel
- **Must always remain visible** — this is the action surface; hiding
  it defeats the console's purpose

### 5.5 Operational Context Strip

- **Weather panel** — current conditions + 6-hour forecast
- **Tides panel** — current height + next high/low
- **Pilotage roster** — assigned pilots and their current
  assignments (de-identified per stakeholder boundary)
- **Towage roster** — same for tugs
- **Port status** — pilots available, tugs available, berths
  occupied/available counts
- **Can collapse** when not actively referenced; expansion is one
  click

### 5.6 Communication / event feed

- A persistent guidance feed shows operational notices (weather
  alerts, berth availability changes, stakeholder delay reports)
- Each entry is acknowledgeable
- Stays in a side panel; can collapse to a notification-strip
- Acknowledgement emits `OPERATOR_ACKNOWLEDGED` (V1.1+)

### 5.7 Pending actions

- A dedicated pending-actions tile shows the count and severity
  distribution of:
  - Acknowledged-but-not-resolved recommendations
  - Resolved-but-pending-HM-approval items
  - Escalations the VTSO initiated, awaiting closure
- **Must always remain visible** — supports the workflow model's
  pending-queue concept

### 5.8 Escalation controls

- A dedicated escalate affordance on every conflict detail
- Structured reason-code selection (closed catalogue per workflow §8.5)
- Optional notes field
- Submitting emits `OPERATOR_ESCALATED` (or `ESCALATION_CREATED` —
  naming decision per workflow §17.14)
- Confirmation dialogue: must re-state who the target is (SS) and
  the reason code

### 5.9 What-if surface

- Accessible from any conflict (button on conflict detail)
- Opens a what-if workspace overlay (full-screen, not a modal —
  the operational console is paused while in what-if)
- Scenario composer (adjustments) on the left; shadow simulation
  result on the right
- "Apply to live" affordance is present but disabled for VTSO; only
  HM can approve a what-if for live application
- Emits `OPERATOR_ACTED` with action_type `whatif_apply` /
  `whatif_clear` (Phase 0.8a, already live)

### 5.10 Ownership indicators

Every conflict and every recommendation in the queues shows:

- Current owner (named actor + role badge)
- Time-since-ownership-transfer if relevant
- Pending-action indicator if the current actor has not yet acted

This is the visible substrate of the workflow model's
"single ownership at any moment" principle.

### 5.11 What must always remain visible

- Live Vessel Map
- Coordination Stack (conflict + recommendation queues)
- Pending actions tile
- Top bar (operational context, notifications, escalations)

### 5.12 What can collapse

- Active Movement Timeline (collapse to ribbon)
- Operational Context Strip (collapse to icons)
- Communication / event feed (collapse to strip)
- Vessel detail / berth detail drawers (always closed by default)

### 5.13 What must be single-click accessible

- Conflict detail (from queue)
- Recommendation acknowledge action (from queue)
- Escalate action (from conflict detail)
- What-if scenario composer (from conflict detail)
- Notifications drawer (from top bar)
- Current shift / supervisor identity (from top bar)

---

## 6. Shift Supervisor Console

The SS console is the supervisory layer — review, escalation,
handover. Distinct from the VTSO console in that it is action-low
and oversight-high.

### 6.1 Layout strategy

Three primary regions:

1. **Watch Overview** — what each VTSO is doing right now
2. **Coordination Stack** — read access to the operational queue, with
   review filters
3. **Supervisory Workspace** — handover composer, escalation queue,
   VTSO action review, pending defer/override items

The SS does NOT need the live vessel map at the operational density
the VTSO does. The map is available (one click) but not part of the
default visible surface.

### 6.2 Active watches view

- Lists current active VTSO sessions
- For each: VTSO name, port-watch scope, pending-action count,
  conflict ownership count, recent activity indicator
- Click VTSO → shadow view of their pending actions and recent
  decisions
- Operator workload signal: SS can see at a glance if one VTSO is
  overloaded and another is idle

### 6.3 Operator workload visibility

- Visual indication of pending queue depth per VTSO
- Time-to-acknowledge averages per VTSO over the current shift
- Escalation rate per VTSO
- **Not** a performance dashboard — the SS uses this for
  load-balancing decisions, not to score operators

### 6.4 Escalation queue

- All escalations targeted at the SS or escalated through the SS
- Filterable by severity, age, source actor
- Each row: source VTSO + role badge, reason code, time elapsed,
  associated conflict/recommendation
- Click escalation → detail panel with the full context the VTSO had
  when they escalated
- Action affordances: resolve, escalate-further-to-HM, return-to-source

### 6.5 Handover state

- During a shift: a "next handover at HH:MM" indicator
- Approaching end-of-shift: handover composer becomes visible (and
  required to be completed before close)
- During handover transition: outgoing SS sees "writing" indicator;
  incoming SS sees "reading and accepting" indicator
- After acceptance: previous-shift handover is read-only in the SS
  history view

### 6.6 Unresolved items

- A persistent tile showing items that have aged past expected
  resolution time:
  - Recommendations presented >N minutes without acknowledgement (N
    severity-dependent)
  - Escalations open >M minutes without target action
  - Deferred items approaching `defer_until` with no revisit signal
- Severity-coloured; older items pulse more
- Click → drill into the item

### 6.7 Pending defer items

- Items the HM (or VTSO in V1.x) has deferred, with their
  `defer_until` timestamps
- SS visibility allows them to flag any deferral that the
  operational context has invalidated (e.g. a defer-because-weather
  that's now stale)
- SS can NOT undo a deferral, but can escalate-back-to-HM with a
  "context changed" reason

### 6.8 Active overrides

- Items the HM has overridden, with the chosen alternative and
  reason
- SS visibility supports their oversight of authority decisions
- SS can NOT undo an override, but can escalate-to-HM with concerns

### 6.9 Staffing / coverage visibility

- Which VTSOs are on the current shift
- Which watch each is assigned to
- Any planned mid-shift handovers
- Coverage gaps if a VTSO logs out unexpectedly

### 6.10 Cross-watch coordination

When a port has multiple concurrent watches (port-control +
anchorage-control), the SS sees both:

- Per-watch pending action count
- Per-watch ownership distribution
- Cross-watch escalations (e.g. anchorage VTSO escalates to SS who
  routes to port-control VTSO)

### 6.11 What differentiates this from VTSO console

| Aspect | VTSO console | SS console |
|---|---|---|
| Primary verb | act | review + coordinate |
| Map prominence | always visible | one click away |
| Coordination Stack | the primary surface | read access + filter |
| Time horizon | now → next few hours | now + shift retrospect + handover |
| Action surface | acknowledge, resolve, escalate, run-what-if | escalate-to-HM, write/accept handover, review |
| Operator-name visibility | their own | every VTSO on shift |

---

## 7. Harbour Master Console

The HM console is the authority surface. Its job is to surface
decisions requiring authority-level action, support those decisions
with full context, and enforce the structured reasoning required for
audit defensibility.

### 7.1 Layout strategy

Three primary regions:

1. **Authority Decision Queue** — pending approvals, override
   reviews, sign-offs
2. **Operational Summary** — port-state context (compact, less dense
   than VTSO console)
3. **Audit / Replay Access** — one-click drill into any audit history
   the HM is authorised to read

### 7.2 Authority decision queue

- Ranked by severity and age
- Each row: action type (approve port closure / override
  recommendation / sign off Port Brief / defer / approve what-if),
  source escalation chain, reason code (if escalation), associated
  conflict/recommendation, time elapsed since escalated/requested
- Click row → decision panel with full context (vessel detail, weather,
  recommendation snapshot, alternatives, prior actions)
- Action affordances on the decision panel: approve, defer, override,
  return-to-source

### 7.3 Override approval surface

- Visible when HM clicks "override" on a recommendation
- Shows:
  - Original recommendation + system's recommended option
  - All alternatives the engine considered
  - Decision-time snapshot (§1.4.1 from Phase 0.7b)
  - HM's chosen alternative (must select)
  - **Mandatory reason code selector** (closed catalogue per workflow
    §10.6)
  - Free-text notes field
- **Cannot submit** without reason code
- Confirmation dialogue: re-states the override choice and reason

### 7.4 Defer / review queue

- Items the HM has deferred, with their `defer_until` timestamps and
  reasons
- Items deferred by others (V1.x VTSO defer) that the HM should be
  aware of
- Approaching-revisit items surface prominently as `defer_until`
  approaches

### 7.5 Port-state controls

- Active port-state indicator (open / restricted / closed)
- Toggle controls with structured reason capture
- Port closure approval workflow:
  - Reason code (closed catalogue: severe weather, security incident,
    infrastructure failure, regulatory directive, etc.)
  - Expected duration
  - Affected scope (full port / specific channels / specific berths)
  - Notification routing (which roles + stakeholders get push
    notification)
- Confirmation dialogue: full re-statement of the decision

### 7.6 Escalation review

- All escalations directed at the HM
- Differentiated by source path:
  - Standard chain (VTSO → SS → HM)
  - Emergency bypass (VTSO → HM, SS notified)
- Bypass escalations carry a visible "bypass" tag
- HM can drill into the full source-actor context

### 7.7 High-severity conflicts

- A dedicated panel surfaces all severity-critical conflicts
  regardless of ownership
- HM visibility ensures authority awareness even when the conflict is
  still owned by VTSO/SS
- Click → conflict detail with full context; HM can take ownership
  or remain observer

### 7.8 Operational summaries

- Compact dashboard:
  - Current port state
  - Active conflicts count by severity
  - Active recommendations count by lifecycle stage
  - Active escalations count
  - Active incidents count
  - Current shift identity and supervisor
  - Recent operational events (last 60 minutes summary)

### 7.9 Sign-off workflow

- Port Brief sign-off:
  - Brief preview surface (read mode)
  - Edit affordance is NOT present here — the Brief is composed
    upstream (see Admin / Configuration; or by automated generation
    from the audit trail in V1.x)
  - Sign-off affordance with confirmation dialogue
  - Emits `OPERATOR_SIGNED_OFF` (V1 new event type — workflow §6 /
    §14.3)

### 7.10 Audit / replay access

- One-click access to Replay & Incident Review (§11)
- Quick-filter affordances: by actor, by time window, by event type
- Recent reviews surface for ongoing investigations

### 7.11 What actions require structured reasoning

- Override of any recommendation: **mandatory reason code**
- Defer of any recommendation: **mandatory reason code**
- Port closure / restriction: **mandatory reason code**
- Sign-off Port Brief: confirmation only (the brief itself carries
  the structured content)
- Approve what-if for live application: **mandatory reason code**
  for non-trivial scenarios
- Berth override: **mandatory reason code**

### 7.12 What requires secondary confirmation

All authority-level actions require a confirmation dialogue that
re-states the action's effect, scope, and reason. Single-click
authority actions are explicitly prohibited. The confirmation is not
visual decoration; it is the boundary between "I clicked something"
and "I asserted this".

---

## 8. Executive Dashboard

The Executive Dashboard is strategic, not operational. It is
designed for periodic consumption (weekly / monthly), not sustained
attention. Mobile-friendly summary is the primary form factor;
desktop is secondary.

### 8.1 Layout strategy

A single-page-scrollable surface with **five sections**:

1. **Headline KPIs**
2. **Trend Surfaces**
3. **Incident Summaries**
4. **Operational Risk Indicators**
5. **Replay & Drill-In Surface**

### 8.2 Operational KPIs

Tile-based headline:

- Throughput (vessels in/out, last 7d / 30d / 90d)
- Average dwell time (last 30d, trend arrow vs prior 30d)
- Berth utilisation (rolling 7d average)
- Incidents (count last 30d by severity)
- Override rate (rolling 30d, by HM)
- Escalation closure time (rolling 30d average)
- Operator-time-to-acknowledge (rolling 30d average)

Each tile is single-glance; click drills into a trend chart.

### 8.3 Trend surfaces

Line / area charts over selectable windows (7d / 30d / 90d):

- Throughput trend
- Dwell time trend
- Incident frequency by severity
- Override rate by HM
- Escalation rate per shift
- Berth utilisation by berth

### 8.4 Incident summaries

- Aggregated count and severity distribution
- Most-recent incidents with one-line description
- Click any incident → opens Incident Replay (§11) in read-only
  executive view (aggregated; no individual operator detail beyond
  what the Executive's permission allows)

### 8.5 Operational risk indicators

Computed metrics surfacing signal-of-concern:

- Recommendation adoption rate (resolved / generated, by severity)
- Defer revisit success rate (deferrals that were revisited
  vs. expired)
- Override-then-incident rate (overrides followed by an incident
  within N hours — early signal of risk-taking)
- Audit chain integrity status (always present; green when
  `verify_chain` passes, red when not)

### 8.6 Recommendation adoption metrics

- Per recommendation type: how many were generated, presented,
  acknowledged, resolved, deferred, overridden, expired
- Trend over rolling windows
- Indicates engine calibration: low adoption rate may signal a
  misaligned engine

### 8.7 Escalation statistics

- Volume by severity
- Closure time distribution
- Bypass rate (emergency bypass usage)
- Top escalation reason codes by frequency

### 8.8 Defer / override analytics

- Override count by HM, by reason code, over rolling windows
- Defer revisit success rate over rolling windows
- Surfacing patterns: high override rate may signal engine
  miscalibration; high defer-then-expire rate may signal operational
  attention gaps

### 8.9 What executives should NOT see

Per permission model §11.6:

- Real-time conflict detail (operational, not strategic)
- Free-text resolution notes from individual VTSO actions (operational
  privacy)
- Full UKC numbers and detailed engineering data (technical, not
  decision-relevant at executive level)
- Pilot identities at named-individual level (use de-identified
  capability descriptors)
- Stakeholder-side delay reports at named-individual level
- Live operational state (the dashboard reflects aggregated data
  with at least minutes of latency)

### 8.10 Audit posture

Per permission model §11.7: every executive read emits an
`AUDIT_READ` meta-event (V1.4+). Executives see who is paying
attention to what.

---

## 9. Stakeholder Experience

Stakeholders (pilots, towage, mooring, terminals, shipping lines)
use a fundamentally different surface. **Mobile-first, single-screen,
narrow.** Per permission model §4.5 and workflow model §13.

### 9.1 Stakeholder categories

Five distinct stakeholder types, each with subtly different
operational concerns:

1. **Pilots** — assigned per vessel movement; primary concern is
   vessel approach context and movement timing
2. **Towage** — assigned per movement; primary concern is tug
   readiness and vessel arrival timing
3. **Terminals** — assigned per berthing; primary concern is berth
   readiness and cargo operations
4. **Mooring crews** — assigned per berthing; primary concern is
   mooring window and crew availability
5. **Shipping lines** — vessel operators with read access to their
   own vessels; primary concern is ETA / ETD accuracy

### 9.2 Common stakeholder surface design

All five stakeholder types share a common surface design with
type-specific data filtering:

- **Single screen** — no left rail, no top nav beyond identity
  and notifications
- **Assignment feed** — chronological list of assignments
  (current and upcoming)
- **Active assignment detail** — full context for the assignment
  currently in progress
- **Confirm assignment** — primary CTA when an assignment is
  newly notified
- **Report delay** — secondary CTA, structured reason capture
- **Report issue** — tertiary CTA, structured reason capture
- **Weather / tides** — operational context relevant to assignment
  window

### 9.3 Assignment feed

- Vertical list of assignments
- Each entry: vessel name, assignment type (pilot / tug / mooring /
  terminal / line), assignment window (start - end time), berth
- Visual hierarchy: current assignment at top; upcoming below;
  recent past collapsed
- Click assignment → assignment detail

### 9.4 Acknowledgement flow

When an assignment is notified:

- Push notification (transport TBD per §12.4)
- In-app notification badge
- Confirm CTA on the assignment detail surface
- Confirm emits `OPERATOR_ACTED` with `action_type='confirm_assignment'`
- After confirm, the assignment moves to "confirmed" state and the
  CTA changes to "report delay" / "report issue"

### 9.5 Delay reporting

- Tap "report delay" on an active assignment
- Structured form:
  - Reason code (closed catalogue: e.g. pilot late, vessel mechanical,
    weather, harbour traffic, equipment, crew, other)
  - Expected delay duration (minutes / hours)
  - Optional notes field
- Submit → emits `OPERATOR_ACTED` with `action_type='report_delay'`
- The delay enters the VTSO's guidance panel and may trigger
  re-coordination by the VTSO

### 9.6 Issue reporting

- Tap "report issue" — for operationally-impacting conditions not
  yet a delay
- Structured form (similar to delay, with issue-specific reason
  codes)
- Submit → emits `STAKEHOLDER_REPORTED_ISSUE` (V1 candidate event
  type — workflow §14.3)

### 9.7 Limited operational visibility

Stakeholders see ONLY:

- Their own assignments (current + recent past + upcoming)
- Weather and tides (read-only general)
- The vessel detail for their assigned vessels
- Berth detail for their assigned berths
- Pilotage / towage status of their own assignments

Stakeholders do NOT see:

- Other stakeholders' assignments (privacy + scope)
- Active conflicts / guidance / dashboard
- Other vessels not assigned to them
- Other berths not in their scope
- Anyone else's identity beyond what's necessary for coordination

### 9.8 Kyber boundary

Per permission model §11 and workflow model §13.6: **Horizon V1
does not depend on Kyber code.**

- For pilots specifically, Kyber owns rostering, availability,
  detailed pilot-side workflow
- Horizon's pilot surface is the *broader port context feed* —
  weather, tides, vessel approach, berth readiness — that pilots
  need but Kyber doesn't generate
- The Kyber app and the Horizon stakeholder app are separate
  experiences; pilots use both, knowing which is which
- Future integration (V1.x at earliest) is API/event-based

### 9.9 Mobile-first interaction model

- Single primary CTA visible at any time
- Touch targets sized for thumb-reach
- Forms are short (3-5 fields max)
- Confirmation is single-tap with brief animation
- No multi-pane layouts
- No swipe-only gestures (must be discoverable by tap)

### 9.10 Offline degradation

- Recently-viewed assignment detail is cached for read offline
- Action submissions (confirm, report delay, report issue) queue if
  offline; submit on reconnection with a clear indicator
- The system never silently drops a stakeholder action; if it
  cannot be submitted, the operator must see that explicitly

---

## 10. Shift & Handover Screens

Per workflow model §9. The shift / handover surface is part of the
Shift Supervisor console (§6) but warrants its own architectural
treatment because it is the densest workflow surface in the system.

### 10.1 Handover creation

- Accessible when approaching end-of-shift
- Structured composer with sections (per workflow §9.2):
  - **Operational events** (free text, narrative)
  - **Pending recommendations** (auto-populated; SS adds notes)
  - **Open escalations** (auto-populated; SS confirms or adds context)
  - **Outstanding HM approvals** (auto-populated)
  - **Weather posture and forecast** (free text)
  - **Unusual / requires attention** (free text)
- "Submit handover" button — primary CTA, with confirmation
- Submit emits `HANDOVER_CREATED` (V1 candidate event type)

### 10.2 Pending-item review

- Side panel listing every item that will transfer to the incoming
  shift
- Recommendations not yet resolved/deferred/overridden/closed
- Escalations not yet closed by target authority
- Pending HM approvals
- Active operator-deferred items with future `defer_until`
- Frozen at handover-create event time; new items arriving during
  handover transition handled per §9.4 (workflow open question)

### 10.3 Shift acceptance

- Incoming SS sees the handover note in read mode
- Tab for "request clarification" if needed
- Accept CTA — primary
- Reject CTA — secondary, only if circumstances genuinely warrant
- Accept emits `HANDOVER_ACCEPTED` (V1 candidate)

### 10.4 Operational note structure

The free-text "operational events" field should be **structured-by-
convention** even though it's free text:

- Time of event
- Type (conflict, weather, escalation, override, etc.)
- Brief narrative
- Reference to specific audit events where relevant

Claude Design will produce a template; the architecture's
responsibility is establishing that structured-by-convention is the
default, not unstructured stream.

### 10.5 Escalation transfer

- Open escalations transfer ownership to the incoming shift's scope
  at handover-accept event
- Target authority of each escalation does not change (HM stays HM)
- Visible to both outgoing and incoming SS during the transfer

### 10.6 Handover history

- Read-only view of previous handover notes
- Bounded by SS's own shift history (own scope only) OR by HM's
  full-tenant audit access (full scope, when reviewing for audit
  purposes)
- Click any historic handover → full read view + linked audit events

---

## 11. Replay & Incident Review Screens

Per workflow model §12. The replay / incident review surface is the
most cognitively demanding surface in V1 because it must
simultaneously:

- Reconstruct operational state from the audit ledger
- Present that state with the same clarity as the live VTSO console
  (per principle 7)
- Layer time-scrubbing affordances over that presentation
- Surface decision artefacts (snapshots, alternatives, reasons) at
  high fidelity

### 11.1 Replay timeline

- Horizontal timeline at top, showing the bounded replay window
- Scrubber allowing the reviewer to move through time
- Event markers along the timeline coloured by severity / type
- Zoom in/out to see different time granularities (minutes →
  hours → days)

### 11.2 Event reconstruction

Below the timeline, a **reconstructed operational console view** for
the currently-selected timeline position:

- Same Live Vessel Map, Active Movement Timeline, Coordination Stack
  the VTSO would have seen at that moment
- Operational Context Strip showing the weather, tides, port state
  of that moment
- This is the workflow model's principle 7 ("replay must mirror live
  operational views") made concrete

### 11.3 Recommendation chain

- For any recommendation in the replay window:
  - Lifecycle stages displayed as a horizontal chain
  - Click any stage → audit event for that transition
  - Decision-time snapshot (§1.4.1) accessible from the
    `RECOMMENDATION_GENERATED` event
  - Alternatives considered displayed
  - Final action (resolve / defer / override / escalate / close)
    with reason code and notes

### 11.4 Escalation chain

- For any escalation in the replay window:
  - Source actor / role → target authority chain
  - Each link is an audit event
  - Closure event linked at the end
  - Multi-step escalations: full chain reconstructible

### 11.5 Actor / ownership replay

- Side panel showing:
  - Who owned a given conflict/recommendation at the selected
    timeline position
  - Ownership transfer timeline for that item
- Click any actor → highlights all events that actor produced in
  the window

### 11.6 Vessel-state replay

- Geographic / kinematic state of vessels at the selected timeline
  position
- Reconstruction from `VESSEL_STATE_OBSERVED` audit events (Phase 0
  closed-set reserved, may emit in V1+)
- Note: V1.0 may not have continuous vessel-state event emission;
  the replay window may show interpolated state. V1.x decision.

### 11.7 Audit chain visibility

- An always-present indicator showing chain integrity status across
  the replay window: green = `verify_chain` passes, red = chain
  break detected
- Click → detail of any break (sequence_no, error_kind, break_at
  per Phase 0.5b)
- A chain break never silently passes — it is surfaced to the
  reviewer prominently

### 11.8 Replay filters

- By time range (preset windows: last hour, last shift, last 24h,
  arbitrary range)
- By event type (filter to escalations only, overrides only, etc.)
- By actor (filter to a specific user's actions)
- By severity (filter to critical / high only)
- Filters compose; reviewer can see "all overrides by HM Mark Brown
  in the last 30 days"

### 11.9 Incident package export

- Reviewer can export an incident package:
  - JSON dump of all audit events in the window
  - PDF summary of the replay (timeline, key events, decision
    chains)
  - Hash-chain verification artefact (signed proof that the
    exported package matches the ledger)
- Export emits `INCIDENT_REPLAYED` (V1 candidate event) with the
  reviewer's identity, window, and a hash of the exported package

### 11.10 Replay performance considerations

- A full-shift replay (8-12 hours of events for one tenant) must
  load and render in < 5 seconds
- A multi-day replay (e.g. 7 days) may take longer; loading
  indicator with progress
- Replay queries must be indexed-friendly against
  `(tenant_id, ts_event, sequence_no)` per Phase 0.5a schema

---

## 12. Notification & Alert Model

V1 introduces structured notifications. This section defines the
notification severity model; transport choices (push, SSE,
WebSocket, SMS, email) are deferred to V1 architecture decision
per workflow §17.15.

### 12.1 Alert severity levels

Four levels:

1. **Informational** — operational context (weather observed, vessel
   arrived). No action required.
2. **Operational** — action expected within shift (acknowledge
   recommendation, review escalation).
3. **High** — action expected within minutes (high-severity conflict,
   approaching deadline).
4. **Critical** — immediate authority attention required (emergency
   bypass escalation, port closure decision, audit chain integrity
   alarm).

### 12.2 Acknowledgement behaviour

- **Informational:** in-app only; no notification; passive feed entry
- **Operational:** in-app notification in the user's drawer; no push
  unless user has opted in
- **High:** in-app + push (transport per §12.4); persistent until
  acknowledged
- **Critical:** in-app + push + fall-through to alternate transport
  if not acknowledged within N seconds; alerts cascade to next
  authority level

### 12.3 Escalation-triggered alerts

- VTSO escalates → SS receives Operational-or-High notification
  (depending on escalation severity)
- SS escalates → HM receives High-or-Critical notification
- VTSO emergency-bypass-to-HM → HM receives Critical notification;
  SS receives High notification (after the fact)

### 12.4 Shift-change alerts

- Approaching end-of-shift: SS receives Operational notification
  N minutes before scheduled close
- Handover ready: incoming SS receives Operational notification
- Handover not accepted within N minutes after submit: HM receives
  High notification

### 12.5 Defer deadline alerts

- Approaching `defer_until`: deferring HM and on-shift VTSO receive
  Operational notification N minutes before
- `defer_until` passes without revisit: HM and SS receive High
  notification (chained to the recommendation; this is the operational
  substrate of `DEADLINE_PASSED`)

### 12.6 Override review alerts

- HM submits an override: SS receives Operational notification (for
  review awareness)
- Aggregated weekly override summary: Executive receives Operational
  notification

### 12.7 Executive alerts

- Severity-critical incident: Executive receives High notification
- Weekly trend digest: Executive receives Operational notification
- Audit chain integrity alarm: Executive AND HM receive Critical
  notification

### 12.8 Stakeholder notifications

- New assignment: stakeholder receives Operational push (in-app +
  external transport per §12.4)
- Assignment time change: Operational push
- Assignment cancellation: High push
- Severe weather affecting active assignment: High push

### 12.9 What is push-worthy

Push notifications (transport choices per §12.4) for:

- High and Critical severity events to current owners
- Operational events to stakeholders for their assignments
- Critical events fall through to alternate transport on
  non-acknowledgement

In-app-only (no push) for:

- Informational events
- Operational events to non-owners (e.g. read awareness)
- All Executive notifications (Executive is periodic-cadence by
  design; push would be inappropriate)

### 12.10 Push transport

Transport choice is a V1 architecture decision (per workflow §17.15):

- SSE (Server-Sent Events) — simplest; one-way; suits in-app
- WebSocket — bidirectional; more complex; suits real-time multi-
  party (replay collaboration)
- External push (APNs / FCM) — required for mobile stakeholders
  outside the in-app session
- SMS — fallback for critical alerts not acknowledged
- Email — slowest; for digests, Port Brief distribution

V1.0 likely ships SSE in-app + APNs/FCM for stakeholder mobile;
SMS fallback at V1.x; WebSocket only when collaborative replay
arrives.

### 12.11 Notification overload

If a single user receives many notifications in a short window:

- Same-class notifications collapse into "X new" badge
- Notification drawer groups by source / type
- The user can suppress class-of-notification temporarily
  (suppression is audited)

This addresses the workflow §17 notification overload concern.

---

## 13. Device Strategy

V1 is **explicitly multi-device but role-specific.** Not every role
gets every device. Per principle 8.

### 13.1 Desktop operational consoles

- **VTSO console** — desktop primary; tablet acceptable; mobile NOT
  supported as the primary work surface
- **SS console** — desktop primary; tablet acceptable; mobile NOT
  supported
- **HM authority console** — desktop primary; tablet acceptable;
  mobile read-only summary acceptable (NOT for authority actions)
- **Replay & Incident Review** — desktop only (the cognitive density
  of replay demands a large screen)

### 13.2 Tablet operational use

Tablet is a fallback / portable form for desktop surfaces:

- VTSO can use tablet for short periods (e.g. mobile during a
  walk-around); not as a sustained workstation
- SS can use tablet for handover review; primary creation on desktop
- HM can use tablet for non-authority-action review (read audit,
  read Port Brief)
- HM authority actions (override, defer, port closure) require
  desktop or a tablet with sustained network connectivity AND a
  larger screen — small tablets (phone-sized) are explicitly NOT
  authorised for these actions

### 13.3 Mobile stakeholder use

- Mobile is the primary form factor for stakeholders
- All stakeholder workflows (confirm assignment, report delay,
  report issue) work end-to-end on phone
- No desktop-only stakeholder feature

### 13.4 Executive mobile summaries

- Executive Dashboard is mobile-friendly
- Trend charts, KPI tiles, Port Brief reading all work on phone
- Incident replay drill-in is mobile-degraded (replay's cognitive
  density doesn't compress well); Executive can read the replay
  summary on phone but full timeline scrub requires tablet/desktop

### 13.5 Replay usability constraints

Replay requires:

- Large screen (desktop primary; tablet acceptable for review-only,
  not export)
- Pointer precision (mouse or precise touch with stylus)
- Sustained network connectivity (replay is data-intensive)

Replay is explicitly NOT designed for phone-screen use.

### 13.6 What workflows should NEVER happen on mobile

- VTSO sustained operational work (multiple concurrent conflicts +
  recommendations)
- HM authority actions (override, defer with reason, port closure,
  sign-off Port Brief)
- SS handover composition
- Replay export
- Tenant administration

The system **detects** when a mobile device attempts these and
either:

- Shows a read-only view with "use desktop for this action"
  guidance, OR
- Blocks the action with explicit error if attempted

Per principle 8: mobile is not a degraded desktop.

### 13.7 Cross-device session continuity

A user logging in on desktop, then on tablet (e.g. mobile during a
break) should resume operational context. The session_id may differ
across devices but the user identity is the same; audit events
distinguish device class in their payloads.

### 13.8 Display modes

- **Single-monitor** — V1.0 primary; all surfaces designed to work
  in one screen
- **Multi-monitor** (V1.x) — VTSO console may span; conflict detail
  on one monitor, map on another; deferred design
- **Kiosk / shared-display** (V1.x) — read-only Port Status display
  for situational awareness; deferred design

---

## 14. Screen-to-Workflow Mapping

This section provides an explicit mapping from workflow moments
(per workflow model) to the V1 screens they manifest on.

| Workflow moment | Primary screen | Secondary screen(s) | Notes |
|---|---|---|---|
| Conflict detected | VTSO Coordination Stack (queue) | HM Operational Summary (count); SS Coordination Stack (read) | Phase 0.7a `CONFLICT_DETECTED` lands as a queue entry |
| Recommendation generated | VTSO Coordination Stack (recommendation appears) | HM Operational Summary (count update) | Phase 0.7b `RECOMMENDATION_GENERATED` with §1.4.1 snapshot |
| Recommendation presented | VTSO Coordination Stack (visible to operator) | SS read; HM read | Phase 0.7c `RECOMMENDATION_PRESENTED` |
| Operator acknowledges | VTSO Coordination Stack action | SS VTSO action review queue update | V1.1 `OPERATOR_ACKNOWLEDGED` (new) |
| Operator resolves | VTSO Coordination Stack action; conflict detail | SS review queue update; HM operational summary | V1.1 `OPERATOR_ACTED` (action_type=conflict_resolve) |
| Operator escalates to SS | VTSO escalation control on conflict | SS Escalation Queue (new entry) | V1.3 `OPERATOR_ESCALATED` / `ESCALATION_CREATED` |
| SS escalates to HM | SS Escalation Queue action | HM Authority Decision Queue (new entry) | V1.3 escalation chain link |
| Emergency VTSO → HM bypass | VTSO escalation control with bypass | HM Authority Decision Queue (Critical); SS Escalation Queue (after-the-fact tag) | V1.3 with bypass flag |
| HM overrides | HM Override Approval surface | VTSO Coordination Stack (override notice); SS Active Overrides | V1.2 `OPERATOR_OVERRODE` |
| HM defers | HM Defer/Review queue action | VTSO Coordination Stack (deferred notice); SS Pending Defer Items | V1.2 `OPERATOR_DEFERRED` |
| HM approves port closure | HM Port-State Controls | All operational surfaces (port state indicator updates) | V1.2 `OPERATOR_ACTED` (action_type=port_close) |
| HM signs off Port Brief | HM Sign-Off Workflow | Executive Port Brief surface (signed version appears) | V1.2 `OPERATOR_SIGNED_OFF` (new) |
| SS writes handover | SS Handover Creation surface | Incoming SS sees pending handover indicator | V1.3 `HANDOVER_CREATED` (new) |
| SS accepts handover | SS Shift Acceptance surface | Outgoing SS receives acceptance notification | V1.3 `HANDOVER_ACCEPTED` (new) |
| Shift opens | SS Console (active shift indicator updates) | All operational surfaces (shift indicator updates) | V1.0 `SHIFT_OPENED` (new) |
| Shift closes | SS Console (formal close) | All operational surfaces (shift indicator updates) | V1.0 `SHIFT_CLOSED` (new) |
| Stakeholder receives assignment | Stakeholder app push + assignment feed | VTSO operational console (assignment status updates) | V1.5 `STAKEHOLDER_NOTIFIED` (new) |
| Stakeholder confirms | Stakeholder app confirm CTA | VTSO operational console (confirmed indicator) | V1.5 `OPERATOR_ACTED` (action_type=confirm_assignment) |
| Stakeholder reports delay | Stakeholder app delay form | VTSO Communication / Event Feed (new guidance entry) | V1.5 `OPERATOR_ACTED` (action_type=report_delay) |
| Replay request | HM Replay & Incident Review entry | Executive Incident Summaries drill-in | V1.6 `INCIDENT_REPLAYED` (new) |
| Audit history read | Replay & Incident Review (replay surface) | All review-eligible surfaces (HM authority console, SS handover history) | V1.3+ `AUDIT_READ` (new meta-event) |
| Berth availability changed | Marine Infrastructure (or HM/SS with permission) Berth surface | VTSO Coordination Stack (guidance notice); HM Operational Summary | V1.6 `BERTH_AVAILABILITY_CHANGED` (new) |
| Recommendation deadline passes | VTSO Pending Actions (highlighted); SS Unresolved Items | HM operational summary alert | V1.2 `DEADLINE_PASSED` (Phase 0 reserved) |
| Session ends with pending items | (system-emitted at logout) | None during session; appears in subsequent audit reviews | V1.2 `SESSION_ENDED_WITHOUT_ACTION` (Phase 0 reserved) |

---

## 15. Screen-to-Audit Mapping

This section defines which UI interactions emit audit events. The
mapping is consequential because it determines what the audit ledger
captures and therefore what replay can reconstruct.

### 15.1 UI interactions that emit audit events

| Interaction | Audit event | V1 phase |
|---|---|---|
| Successful login | `SESSION_STARTED` | live (Phase 0.6) |
| Logout (or session timeout) | `SESSION_ENDED` | live |
| Logout with pending presented recommendations | `SESSION_ENDED_WITHOUT_ACTION` | V1.2 |
| Acknowledge a recommendation | `OPERATOR_ACKNOWLEDGED` | V1.1 (new) |
| Resolve a conflict (submit resolution) | `OPERATOR_ACTED` (action_type=conflict_resolve) | V1.1 |
| Submit a what-if scenario | `OPERATOR_ACTED` (action_type=whatif_apply) | live (Phase 0.8a) |
| Clear an applied what-if | `OPERATOR_ACTED` (action_type=whatif_clear) | live |
| Approve a what-if for live (HM) | `OPERATOR_ACTED` (action_type=whatif_approve) | V1.2 |
| Override a recommendation | `OPERATOR_OVERRODE` | V1.2 (Phase 0 reserved) |
| Defer a recommendation | `OPERATOR_DEFERRED` | V1.2 (Phase 0 reserved) |
| Escalate to SS or HM | `OPERATOR_ESCALATED` (or `ESCALATION_CREATED`) | V1.3 |
| Resolve / close an escalation | `ESCALATION_RESOLVED` | V1.3 |
| Approve port closure | `OPERATOR_ACTED` (action_type=port_close) | V1.2 |
| Sign off the Port Brief | `OPERATOR_SIGNED_OFF` | V1.2 |
| Write handover note (submit) | `HANDOVER_CREATED` | V1.3 |
| Accept handover (incoming SS) | `HANDOVER_ACCEPTED` | V1.3 |
| Open replay window | `INCIDENT_REPLAYED` | V1.6 |
| Read audit trail (review surface) | `AUDIT_READ` | V1.3+ |
| Export incident package | `INCIDENT_REPLAYED` (with export flag) | V1.6 |
| Stakeholder confirms assignment | `OPERATOR_ACTED` (action_type=confirm_assignment) | V1.5 |
| Stakeholder reports delay | `OPERATOR_ACTED` (action_type=report_delay) | V1.5 |
| Stakeholder reports issue | `STAKEHOLDER_REPORTED_ISSUE` | V1.5 |
| Marine Infrastructure changes berth availability | `BERTH_AVAILABILITY_CHANGED` | V1.6 |

### 15.2 UI interactions that do NOT emit audit events

For clarity, the following common interactions are **explicitly
NOT audit-emitting**:

- Navigation between screens (left rail clicks, tab switches)
- Scrolling, filtering, sorting within a queue
- Opening / closing collapsible panels
- Hovering for tooltips
- Reading without explicit "drill-in" action (e.g. seeing a
  recommendation appear in your queue does not emit an event for
  you; the system already emitted `RECOMMENDATION_PRESENTED` for
  the surface)

This is deliberate: an audit-everything posture creates noise that
degrades the signal. The line is **state-changing or
authority-asserting actions emit events; passive reading and
navigation do not** (with the exception of `AUDIT_READ` for
explicit audit-trail review surfaces, where the read itself is the
authority signal).

### 15.3 Per-event payload requirements

Per workflow model §14: every audit event in V1 carries in its
payload:

- `user_id` (resolved from session)
- `role` (active role for this session)
- `scope` (active port, tenant, shift_id)
- `session_id`

These are payload-level additions to the existing Phase 0 audit
shape; no schema migration required.

### 15.4 Audit chain integrity in UI

The audit chain integrity status is surfaced in:

- Replay & Incident Review screens (always present indicator)
- HM Authority Console (subtle status indicator; alarm on break)
- Executive Dashboard Operational Risk Indicators (audit chain
  integrity tile)

A chain break is **never silently rendered**. Every screen that
depends on audit data either explicitly says "chain integrity OK"
or surfaces the break as a high-severity alert per §12.7.

---

## 16. Build Sequencing Recommendation

V1 screen rollout sequence, paralleling the workflow model's build
sequence (§16) and the permission model's build sequence (§12).
Each phase requires explicit authorisation before implementation.

### V1.0 — Foundational shell + VTSO console

- Login screen (real user/password auth replacing Beta 10 shared
  login)
- Persistent left rail + top bar framework
- Role-filtered navigation logic
- Initial VTSO console (Live Operations domain): live vessel map,
  active movement timeline, coordination stack, operational context
  strip, communication / event feed, pending actions tile
- Shift indicator in top bar
- Port selector for multi-port users (if applicable)
- No new action surface yet; deliverable is the role-scoped
  read-and-context shell

### V1.1 — Recommendation action surfaces

- VTSO console action affordances: acknowledge, resolve, run-what-if,
  add notes, flag for escalation
- Recommendation detail panel
- What-if scenario composer
- Escalation control (initiating to SS; submission flow only —
  receiving side ships in V1.3)
- Audit emission: `OPERATOR_ACKNOWLEDGED`, `OPERATOR_ACTED` (conflict_resolve)

### V1.2 — Harbour Master authority surfaces

- HM Authority Decision Queue
- Override Approval Surface (with mandatory reason code)
- Defer / Review Queue
- Port-State Controls (port closure approval workflow)
- Sign-Off Workflow (Port Brief)
- Approve What-If Surface
- Audit emission: `OPERATOR_OVERRODE`, `OPERATOR_DEFERRED`,
  `OPERATOR_SIGNED_OFF`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION`

### V1.3 — Shift handover and escalation

- Shift Supervisor Console (active watches, operator workload,
  unresolved items, pending defer items, active overrides, staffing /
  coverage)
- Escalation Queue (inbound; SS resolution / further-escalation
  flow)
- Handover Creation surface
- Handover Acceptance surface
- Handover History view
- Audit emission: `OPERATOR_ESCALATED` / `ESCALATION_CREATED`,
  `ESCALATION_RESOLVED`, `HANDOVER_CREATED`, `HANDOVER_ACCEPTED`,
  `SHIFT_OPENED`, `SHIFT_CLOSED`, `AUDIT_READ`

### V1.4 — Executive dashboard

- Executive Dashboard (KPIs, trend surfaces, incident summaries,
  operational risk indicators, recommendation adoption metrics,
  escalation statistics, defer/override analytics)
- Port Brief read surface
- Trend chart drill-in
- Incident Summary drill-in
- Audit emission: `AUDIT_READ` per executive read

### V1.5 — Stakeholder mobile feed

- Stakeholder mobile app (assignment feed, active assignment detail,
  confirm CTA, report delay form, report issue form)
- Notification routing for stakeholder events (transport per V1
  architecture decision)
- Kyber boundary contract finalised
- Audit emission: `OPERATOR_ACTED` (confirm_assignment, report_delay),
  `STAKEHOLDER_REPORTED_ISSUE`, `STAKEHOLDER_NOTIFIED`

### V1.6 — Replay workspace

- Replay & Incident Review surface (replay timeline, event
  reconstruction, recommendation chain, escalation chain, actor /
  ownership replay, vessel-state replay, audit chain visibility,
  replay filters, incident package export)
- Replay performance hardening at multi-day scope
- External-auditor read access (V1.x — deferred)
- Audit emission: `INCIDENT_OPENED`, `INCIDENT_REPLAYED`,
  `INCIDENT_FINDING_RECORDED`

### V1.x — Beyond the initial six phases

- Multi-monitor display modes (VTSO console spans)
- Kiosk / shared-display Port Status mode
- Command palette (keyboard shortcut for power users)
- AI-assisted operational surfaces (deferred; this document does
  not design AI features, only notes their architectural slot)
- Cross-tenant Executive scope (when multi-tenant arrives)
- External regulator / auditor read access

---

## 17. Open Questions

Surfaced for review before V1.0 design lock. None have answers in
this document. Numbering and content reflect both the permission
model's and workflow model's existing open-question sets where
relevant.

1. **Single-screen vs multi-workspace operations.** Does a VTSO
   ever need to operate two workspaces simultaneously (e.g. one
   for normal port operations, one for an active incident)? V1.0
   may default to single-workspace and add multi-workspace in
   V1.x.

2. **Multi-monitor support.** Does V1.0 design assume single-monitor,
   or first-class multi-monitor support? Affects layout decisions
   for VTSO and HM consoles.

3. **Kiosk / shared-display modes.** Are large shared screens
   (operational situational awareness displays) part of V1.0 or
   V1.x? Affects auth model (kiosk has no individual user
   identity; how is the kiosk distinguished in the audit ledger?)

4. **Replay performance requirements.** Acceptable load latency for
   a shift-scope replay (~8h); a multi-day replay (~7d); a
   multi-month replay (~30d). Affects DB indexing strategy and
   query architecture.

5. **Live vs delayed stakeholder visibility.** Should stakeholders
   see real-time vessel ETAs, or 5-minute-delayed (privacy +
   commercial considerations)? Per-tenant config decision.

6. **Notification overload handling.** What's the right grouping /
   suppression behaviour when a user receives 50+ notifications
   in a minute (e.g. severe weather affecting multiple
   recommendations)? Algorithmic vs operator-controlled?

7. **Mobile escalation approval safety.** Is it safe to allow an HM
   to approve overrides from a phone? Current §13.6 says no. Some
   ports may push for phone-side approval. Trade-off between
   responsiveness and decision quality.

8. **Offline / failover operational mode.** When network connectivity
   to Horizon is lost, what UI is operators see, what actions can
   they still take (queued for submit on reconnect), and how do
   subsequent audit events reconcile?

9. **Future AI-assisted operational surfaces.** Where should AI
   recommendations / summarisations appear in V1 architecture (so
   their architectural slot exists in V1.0+ even before AI
   features ship)?

10. **Whether replay requires separate permissions from live
    operations.** An HM has live operational visibility AND replay
    access today. Should there be a separate `view_replay`
    permission, or is it always coupled to authority visibility?

11. **Replay collaboration.** Can two reviewers (e.g. HM and SS)
    open the same incident replay simultaneously? Does the audit
    ledger record both?

12. **Active-incident banner UX.** Per §4.8, every screen surfaces
    an active-incident banner. What if multiple incidents are
    active in the user's scope simultaneously?

13. **Cross-port dashboard for HM authority class.** A port-group
    HM may want a cross-port operational summary; how does the
    multi-port view differ from the single-port operational
    console?

14. **Stakeholder authentication and notification.** Per workflow
    §17.15 — how do stakeholders authenticate (Horizon-direct,
    federated, port-issued), and what notification transport is
    primary (push, SMS, email)?

15. **Read-only Executive board access.** Should board-level
    executives see anonymised summaries only, with full executive
    detail reserved for operational executives? Per permission
    model §11.8.

---

## 18. Recommendations

For the V1.0 design review:

1. **Treat this document as Screen Architecture v0.1.** Expect
   iteration to v0.2, v0.3, ... as Claude Design and operational
   stakeholders push back on structural assumptions. The screen
   architecture is foundational; structural objections surface
   here are cheap to resolve, and expensive once visual design or
   front-end implementation is underway.

2. **Use this as the primary input to Claude Design / CX work.**
   Visual design (layout, typography, colour, iconography,
   interaction microcopy, animation) is downstream of this
   document. Claude Design should consume:
   - this document as authoritative for screen hierarchy,
     navigation, and role-specific interaction model
   - the permission model (PR #30) as authoritative for role
     and authority boundaries
   - the workflow model (PR #31) as authoritative for state-
     machine semantics and audit-event triggers

3. **Avoid visual design before operational architecture is
   accepted.** Resist the temptation to skip ahead to mockups or
   Figma boards. Visual design conducted against an unstable
   architecture is design debt; the architecture must stabilise
   first.

4. **Maintain operational-first UX philosophy.** Every design
   decision under V1 should be tested against the question:
   "does this serve the operator under sustained cognitive load,
   or does it serve the demo first?" Operations is the long-tail
   use case; design for it.

5. **Continue preserving Beta 10 separately from V1
   implementation.** The Phase 0 regression gate
   (`tests/test_beta10_regression.py`) is the explicit guard
   against accidental drift. V1 implementation will necessarily
   change the Beta 10 baseline because the entire auth model and
   the entire screen architecture change. The Beta 10 baseline
   must be deliberately updated by a reviewer at that point, not
   silently broken. The `phase-0-complete @ 4ad4aae` tag remains
   the immutable reference point.

6. **Continue keeping Stage E-prod paused** unless there is a
   separate commercial or operational reason to activate it
   sooner. The recommendation stands from PR #30 §14.6 and PR #31
   §18. Activating production audit before V1's user/role identity
   is on each audit row means recording Beta-10-shaped actor
   attribution into the persistent ledger. The trade-off is a
   business decision; both paths are defensible.

---

**End of v0.1.** Reviewer comments expected before v0.2.

Together, the three v0.1 documents (Permission Model, Workflow
Model, Screen Architecture) form the complete V1 design foundation.
No V1 implementation should begin until all three have been
reviewed and accepted by operational stakeholders.
