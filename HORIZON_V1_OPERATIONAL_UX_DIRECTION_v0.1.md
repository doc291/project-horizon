# Horizon V1 — Operational UX Direction (v0.1)

**Document status:** Draft for review — **forward-looking direction note**
**Document type:** UX principles / information-hierarchy alignment
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 5ca11db`
  (post-PR #56 — Darwin Decisions-panel pinned-slot fix)
**Date:** 2026-05-21
**Scope of authority:** Captures the **operational UX direction** that
  future Horizon V1 (M2+) layouts and components must align to. This
  document is **principles-only** — it does **not** authorise any UI
  rebuild, does **not** invalidate the existing
  `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md`, does **not** change
  any code, does **not** alter M1 production behaviour, and does **not**
  modify any previously merged document.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md`
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md`
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md`
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md`
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md`
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)

---

## 1. Executive summary

The current Horizon V1 layout (delivered through M0 and M1) is
**dashboard-first**: the central operational spine shows a KPI /
metrics dashboard as the default tab, with conditions ribbons and
status tiles dominating visual weight. This was the correct early-V1
posture — it gave executive and supervisor users a useful overview
surface and gave the engineering programme a tractable first surface
to deliver against.

However, for the **operational** user — particularly **VTSO and
coordination roles** — the dashboard-first hierarchy does **not**
match how they scan, decide, and act. Coordination users scan first
for *what requires attention now*, then for *what is moving spatially*,
then for *what becomes a problem next*. A KPI grid is not their
primary surface.

This document records the **operational UX direction** that future
Horizon V1 milestones (M2 and beyond) must align to:

1. **Decision cards / active coordination items → RIGHT operational rail.**
   The right rail becomes the persistent action-and-coordination
   surface, not a status sidebar.
2. **VTS / spatial awareness → PRIMARY central operational surface**
   in later milestones (long-term centre-spine default).
3. **KPI / dashboard surfaces → supporting context**, not the focal
   point of the operational view.
4. **Role-specific operational modes** are anticipated in the
   architecture: Executive View, Coordination View, VTSO View,
   Pilotage View, Incident Mode, Replay / Audit Mode.

This is a **direction note**, not a redesign brief. M1 production
behaviour is unchanged. M2 scope (PR #53) and M2 implementation plan
(PR #54) are not retroactively modified by this document. **The
direction applies forward, from M2 onward**, by constraining what M2
and later implementations may build and by anticipating M3+
re-architecture of the centre spine and right rail.

---

## 2. Operational insight

### 2.1 What operators actually scan for

The Component & Interaction Canon §0 preamble identifies the
safety-critical audience (VTSO, Harbour Master, Shift Supervisor,
Port Executive, Stakeholder, Marine Infrastructure operators). Of
these, the **coordination roles** (VTSO, Harbour Master, Shift
Supervisor) are the highest-frequency users and the ones whose
decisions are most safety-critical.

For coordination users, the scanning order is:

1. **What requires attention now** — escalations, conflicts,
   acknowledgements pending, sequencing decisions, ETD risks. *Action
   items.*
2. **What is moving spatially** — vessel positions, headings, speeds,
   crossing tracks, channel state, tide state. *Live operational
   picture.*
3. **What becomes a problem next** — predicted conflicts, timeline
   compression, berth-readiness gaps, pilotage notice gaps,
   tug / mooring resource conflicts. *Forward-looking risk.*

Static KPIs (vessels in port, average port time, utilisation
forecast) are scanned **after** these three, when the operator has
slack capacity. KPIs are also the surface that executives need
first — but executives are a smaller, lower-frequency audience.

### 2.2 The current V1 hierarchy

V1 M1 delivers, as the centre-spine default, a **Dashboard** tab
with a KPI grid, an ETD risk table, a utilisation summary, and a
conditions ribbon above. Decision cards live in a right-side
"Decisions" pane that is filter-driven (`signal_type === 'CONFLICT'
&& decision_support`) and currently shows them but is visually
secondary to the centre-spine dashboard.

This hierarchy was correct for:
- Early-V1 delivery (smallest surface that proves the read-only
  pipeline works end-to-end).
- The Darwin Ports demo audience (mixed executive / operator).
- The M0 and M1 acceptance criteria, which had to be deterministic
  and testable.

This hierarchy is **wrong** as a permanent posture for the operational
user. Future milestones must move toward an operator-first hierarchy
without invalidating M1.

### 2.3 The gap to close

The gap is **information hierarchy and operational intent**, not
visual styling. The same components (KPI tile, ETD risk table,
Decision card) can be re-positioned and re-weighted without a visual
rebuild. The work is in the architecture and intent, not in the
pixels.

---

## 3. Direction changes

Four direction changes apply forward from M2 onward.

### 3.1 Decision cards / active coordination items → RIGHT operational rail

**Long-term posture:** the right rail becomes the persistent
**action and coordination surface** of the operator's view, not a
status sidebar.

**Content the right rail will eventually carry (no implementation
implied by this document):**
- Active conflicts (CONFLICT-class items with populated decision
  support).
- Recommendations awaiting operator response.
- ETD risks above an attention threshold.
- Acknowledgements pending.
- Sequencing issues that need a decision.
- Coordination tasks (e.g. pilot notification, tug confirmation,
  mooring gang readiness).
- Timeline compression warnings.

**Why right rail, not centre:** the centre spine becomes the
spatial / movement surface (§3.2). Action items are best at the
edge — visible, ranked, scannable — without competing with the
spatial picture for screen real estate.

**M1 compatibility:** the M1 right-side panel already shows
Decisions. The direction here is to **expand the right rail's
operational role** and re-weight it from "sidebar" to "action
surface" — not to introduce a new region.

### 3.2 VTS / spatial awareness → PRIMARY central operational surface

**Long-term posture:** the centre operational spine's default
tab becomes the **VTS / spatial surface** — vessel movement,
spatial coordination, conflict overlays, predicted paths,
tidal / channel overlays, operational state — for the coordination
user.

**Content the centre spine will eventually default to (no
implementation implied here):**
- Vessel movement (live positions, headings, speeds).
- Spatial coordination (channel transit, berth approach,
  pilotage waypoints).
- Conflict overlays (CPA / TCPA-style proximity rendering,
  berth-overlap visualisation, sequencing).
- Predicted paths (vessel trajectories, planned pilotage paths).
- Tidal / channel overlays (depth, UKC, restricted zones, tide
  state).
- Operational state (channel direction, exclusion zones, weather
  warnings).

**Why centre, not right:** spatial awareness is the operator's
*primary working surface* — it is what they look at continuously,
not what they glance at occasionally. The centre is the highest-
attention real estate.

**M1 compatibility:** M1 ships the partial Dashboard tab as the
centre-spine default. Dashboard does not go away — it becomes one
of several tabs (Dashboard, Berth Timeline, Shift Log, VTS,
Pilotage, Performance — as already in Canon §1.1.3). The
direction here is to **promote VTS to the default tab** for
coordination users, not to remove Dashboard.

### 3.3 KPI / dashboard surfaces → supporting context

**Long-term posture:** KPIs remain important but become
**supporting context**, not the focal point of the operational
view.

**Where KPIs live:**
- A persistent KPI strip near the top of the screen (already
  partially present as the top operational ribbon and conditions
  ribbon) — glanceable, never the dominant surface.
- An Executive View mode (§3.4) where KPIs are the focal point
  for executives who explicitly select that mode.
- The Dashboard tab itself remains accessible for users who want
  a metrics-first view, but it is no longer the default for
  coordination users.

**Why de-focus KPIs:** for the coordination user, KPIs answer
*how are we doing?* — useful context, but not the action signal.
The action signal lives in §3.1 (decision cards) and the working
surface lives in §3.2 (spatial / VTS).

**M1 compatibility:** all M1 KPI work remains valid and shipped.
This document does not propose removing or changing any M1 KPI
implementation. The change is in *default focus* and *visual
weight* over time.

### 3.4 Role-specific operational modes

**Long-term posture:** the architecture must anticipate that
different roles need different default focuses, not a single
universal layout.

**Anticipated modes (no implementation implied here):**

| Mode | Audience | Default focus |
|---|---|---|
| **Executive View** | CEO, Harbour Master strategic level, port-authority executives | KPIs, trend lines, utilisation forecast, financial / commercial summary, exception escalation |
| **Coordination View** | Shift Supervisor, dispatcher | Decision cards (right), Berth Timeline + Shift Log + Pilotage status (centre), KPI strip (top) |
| **VTSO View** | VTSO on watch | VTS spatial surface (centre primary), Decision cards (right), nearby-vessel proximity, channel state |
| **Pilotage View** | Pilot, pilot dispatcher | Pilotage queue, current pilotage movement, the (future, separately authorised) Pilot Proximity Companion App data |
| **Incident Mode** | Any operator during an active incident | Incident-relevant surfaces only; muted KPIs; high-prominence escalation cards; live spatial state; audit-ready capture |
| **Replay / Audit Mode** | Post-incident reviewer, training, harbour-master review | Swimlane timeline (Canon §8), playback controls, snapshot of operator-visible state at each timestamp |

Mode selection is per-user (settings) and per-session
(temporary override). Mode selection is **not** an operator
action with audit semantics — it is a view preference.

**M1 compatibility:** M1 does not implement modes. The
architecture must not introduce structural decisions that make
adding modes harder later.

---

## 4. What changes from this document

### 4.1 Forward-looking constraints (applies to M2 and later)

Future scope proposals and implementation plans **must**:

A. Position decision cards / active coordination items toward the
   **right operational rail** — even when current M1 layout has
   them in a different visual position.
B. Treat the centre operational spine as **eventually** hosting
   the VTS / spatial surface as its default — do not lock in
   Dashboard-as-permanent-default through layout, routing, or
   navigation primitives.
C. Position KPIs as **supporting context** — never the sole or
   dominant surface in a coordination-user mode.
D. **Anticipate role-specific modes** in component composition.
   Avoid hard-coded single-mode assumptions in components, state
   shape, or routing.
E. Honour Canon §1.1.3 — all six centre-spine tabs (Dashboard,
   Berth Timeline, Shift Log, VTS, Pilotage, Performance) remain
   the canonical centre-spine tab set. This document re-prioritises
   *which is default for which audience*; it does not invalidate
   the tab set.
F. Honour the Independence Reset framing (PR #51) — no Smart
   Ocean X dependency framing in any artefact.

### 4.2 What this document does NOT change

- **M0 and M1 are preserved.** Both milestones remain CLOSED. No
  retrospective change.
- **M2 Scope Proposal (PR #53) and M2 Implementation Plan (PR #54)
  are not invalidated.** M2 remains read-only / fixture-backed /
  centre-spine-completion-focused, exactly as planned. This
  document does not require M2 to flip the default tab or
  re-implement the right rail.
- **The Beta 10 production runtime is unchanged.** Pre-demo
  freeze is honoured.
- **No code change is required by this document.**
- **No fixture change is required by this document.**
- **The Canon (PR #43) is not modified.** The Canon defines
  layout primitives; this document records operational direction.
  Both stand together.

### 4.3 Implication for M2 specifically

M2 (as planned in PR #54) delivers Berth Timeline, Shift Log,
VTS, Pilotage, and Performance placeholder — read-only, fixture-
backed. **None of this needs to change.** What this document
adds for M2 is a **prioritisation guidance**:

- When building the VTS tab, design it as **the future default
  tab for coordination users**, even if M2 keeps Dashboard as the
  initial default. Avoid VTS-as-afterthought rendering.
- When wiring the right rail, treat decision cards as
  **first-class action surface** content. Don't visually
  demote them.
- When wiring the tab switcher (M2 Implementation Plan §8.6),
  ensure it does not embed Dashboard-as-default-forever in
  routing or persistence semantics. Default tab should be
  swappable via configuration in a future M2.5 / M3 milestone.

These M2-period guidances are **constraints on the existing
plan**, not new scope items. They do not require new acceptance
criteria for M2 close.

### 4.4 Implication for M3 and beyond

The substantive re-architecture work this document anticipates
belongs to **M3 or later**, under its own scope proposal and
implementation plan:

- Right-rail re-architecture as a persistent action surface.
- VTS spatial surface as a first-class centre-spine implementation
  (with map, conflict overlays, channel geometry).
- Role-mode system with per-user and per-session selection.
- KPI strip as supporting context.

None of this is authorised by this document. None of it is in M2
scope.

---

## 5. Relationship to existing V1 documents

| Document | Relationship |
|---|---|
| Component & Interaction Canon | This document **honours** the Canon's layout primitives (four-region shell, three-column shell, tabbed centre spine, persistent rails). It re-prioritises which content lives where over time. **Canon is not modified.** |
| Information Architecture | This document **adds operational intent** — which information cluster takes priority for which audience. IA structure is preserved. |
| Screen Architecture | This document **adds default-screen-state guidance** by role. The screen states defined in Screen Architecture remain valid. |
| User Permission Model | This document references roles (Executive, Coordination, VTSO, Pilotage, Incident, Reviewer) that are partially named in the permission model. Mapping between role and default mode is left for the M3+ role-mode scope proposal. |
| Operational Workflow Model | The 11-stage workflow remains valid. This document does not alter the workflow; it alters which surfaces the operator looks at *while* the workflow executes. |
| Lifecycle Reconciliation | Untouched. The seven-state lifecycle ↔ eleven-stage workflow mapping is preserved. |
| M2 Scope Proposal / M2 Implementation Plan | This document **adds forward-looking constraints** (§4.3) but does **not** modify M2 acceptance criteria. M2 ships as planned. |
| Horizon Independence / Architecture Reset | Honoured throughout. No Smart Ocean X framing in this document. |
| Pilot Proximity Companion App note (PR #57, if merged) | The Pilotage View mode (§3.4) is the future operational home for the data layer that the Pilot Proximity Companion App would consume. |

---

## 6. Risks if direction is ignored

6.1 **Locking into a dashboard-first hierarchy in M2 components.**
If M2 introduces routing, persistence, or state primitives that
assume Dashboard-as-default-forever, M3+ re-architecture becomes
substantially more expensive. Mitigation: §4.3 M2-period
guidance.

6.2 **Visually demoting decision cards.** If M2 polishes the
Dashboard tab while leaving the right rail as a thin sidebar,
coordination users experience the inverted hierarchy more strongly,
not less. Mitigation: treat the right rail as a first-class
surface during M2 (visually balanced, not skinny-sidebar).

6.3 **VTS surface as an afterthought.** If M2's VTS tab is
implemented as a minimal vessel list with no consideration for
how it eventually becomes the centre-spine default, M3 has to
re-implement it. Mitigation: design the M2 VTS tab with the M3+
expansion in mind (component composition, data shape).

6.4 **Mode-blind component design.** If M2 components hard-code
single-role assumptions (e.g. "this KPI tile is for executives"
baked into the component), the role-mode system in M3+ requires
rewriting components rather than recomposing them. Mitigation:
components remain role-agnostic; mode-specific composition lives
in the page / layout layer.

6.5 **Drift from Canon.** This document does not modify the
Canon. If future implementers re-interpret Canon §1.1 in a way
that contradicts this direction, the contradiction must be
escalated rather than silently resolved. Mitigation: cite both
documents in M3+ scope proposals.

6.6 **Independence Reset drift.** Any future mode (Executive,
Coordination, VTSO, Pilotage, Incident, Replay/Audit) must use
the neutral Horizon framing — no Smart Ocean X positioning, no
external substrate framing. Mitigation: Independence Reset §13 /
§14 framing required in every mode artefact.

6.7 **Beta 10 freeze breach.** This document explicitly does not
authorise any Beta 10 runtime change. If a reviewer attempts to
implement any direction here in the V1 / Beta 10 production
runtime before separate authorisation, that is a freeze breach.
Mitigation: scope proposal → implementation plan → explicit
authorisation pattern, as established by the V1 programme.

---

## 7. Open questions

These are **not** blockers for M2. They are deliberately deferred
to the M3+ scope proposal.

7.1 **Default mode per role.** Should default mode be set by the
RBAC role on first sign-in, by per-user preference, or by
organisational configuration? Probably a mix; the precise
configuration policy is for the M3+ scope.

7.2 **Mode switching as a first-class UI affordance.** Should
there be a visible mode switcher (top-right toggle), or is mode
inferred from role with override only via settings?

7.3 **Incident Mode trigger.** Is Incident Mode entered manually
by a user, automatically by a system trigger, or both? What is
the auditable record of the mode transition?

7.4 **VTS surface depth.** How deep does Horizon's VTS surface go
before it overlaps with the official VTS authority? The Pilot
Proximity Companion App note (PR #57) already captures this
boundary for pilots. The same boundary must be re-considered for
operators in Horizon's centre spine.

7.5 **Replay / Audit Mode data source.** Replay sources the
operator-visible state at each historical timestamp. Where does
that snapshot live? In the audit ledger? In a separate
operator-state snapshot stream? This is an M3+ architecture
decision.

7.6 **Right-rail capacity.** How many simultaneous action items
can the right rail carry before pagination / prioritisation
becomes required? This is empirical — measured against real
operational load, not designed in advance.

7.7 **KPI strip composition.** Which KPIs warrant the persistent
top-strip slot vs the Executive View tab? Probably 3–5 KPIs
maximum in the strip.

7.8 **Mode interaction with Replay.** Can a user enter Replay
Mode from any mode, or only from a specific reviewer mode? The
Canon §8 Replay model implies any mode; this document does not
contradict that.

---

## 8. Recommendations and next steps

8.1 **Merge this document onto `main`** so M2 implementation work
(when authorised) has the forward-looking constraints visible
upstream.

8.2 **No M2 scope change.** PR #53 and PR #54 remain valid as
written. The §4.3 guidance for M2 is implementation-period advice,
not a scope amendment.

8.3 **M3+ scope proposal must cite this document.** When M3 (or
M2.5) is proposed, the scope proposal explicitly cites
`HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` and explains how the
proposal honours each of §3.1, §3.2, §3.3, §3.4.

8.4 **Capability backlog entries** for:
- Right-rail action surface re-architecture
- VTS-as-default-centre-spine
- KPI strip refinement
- Role-mode system (Executive / Coordination / VTSO / Pilotage)
- Incident Mode
- Replay / Audit Mode

These should be added to `HORIZON_CAPABILITY_BACKLOG.md` in a
separate, later PR — not in this document.

8.5 **Maritime SME validation.** Before M3+ scope is finalised,
validate §3.1–§3.4 with at least one VTSO, one Harbour Master,
and one Shift Supervisor. The same SME loop the Pilot Proximity
Companion App note (PR #57) anticipates can be reused.

8.6 **No implementation, no UI rebuild, no Beta 10 change**
authorised by this document. M2 implementation start remains
gated on a separate explicit Tony authorisation per the M2
Implementation Plan §19.1 (Phase 0).

8.7 **Independence Reset framing** must be honoured in every
follow-on artefact (M3+ scope, modes, components, role
descriptions, marketing).

---

## 9. Status

**Status: forward-looking direction note. Not authorised for
implementation.**

This document is principles-only. It establishes operational UX
direction so future M2 implementation work and M3+ re-architecture
work do not lock Horizon into a dashboard-first hierarchy.

Confirmed by this document:
- M0 and M1 production behaviour are unchanged.
- M2 scope (PR #53) and M2 implementation plan (PR #54) are not
  retroactively modified.
- The Component & Interaction Canon (PR #43) is not modified.
- The Beta 10 pre-demo runtime freeze is honoured — no runtime
  change is proposed or authorised.
- The Independence / Architecture Reset (PR #51) framing is
  preserved — no Smart Ocean X dependency framing appears in this
  document.
- No code, fixture, or Railway change is authorised by this
  document.
- The right rail will, long-term, become the action and
  coordination surface.
- The centre spine will, long-term, default to the VTS /
  spatial surface for coordination users.
- KPIs will become supporting context, not the focal point.
- Role-specific operational modes (Executive, Coordination,
  VTSO, Pilotage, Incident, Replay / Audit) will be anticipated
  in architecture before they are implemented.

**Next action:** Tony's review. Optional ChatGPT engineering
review. If approved and merged, this document becomes the
forward-looking constraint that M2 implementation honours and
M3+ scope proposals must cite.

---

## End of Operational UX Direction v0.1
