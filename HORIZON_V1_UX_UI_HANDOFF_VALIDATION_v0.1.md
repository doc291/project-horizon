# Horizon V1 — UX/UI Handoff Validation Report (v0.1)

**Status:** Validation report — planning input only
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review), AMS leadership, future Claude Design CX team
**Subject of review:** Claude Design UX/UI handoff package at `v1-handoff/`
**Authoritative inputs (merged on `main @ 6d00af3`):**
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31)
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` (PR #32)
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md` (PR #33)
- `HORIZON_V1_IMPLEMENTATION_STRATEGY_v0.1.md` (PR #34)
- `HORIZON_V1_REVIEW_PACK_v0.1.md` (PR #35)
- `HORIZON_V1_EXECUTION_PLAN_v0.1.md` (PR #36)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**Implementation status:** None. This document is a validation report.
It does **not** authorise the handoff as an implementation input,
does **not** authorise frontend build work, does **not** authorise
any change to `server.py`, the audit module, the regression gate,
the Railway configuration, or any Beta 10 production behaviour.

---

## 1. Executive Summary

The Claude Design UX/UI handoff at `v1-handoff/` is a **high-quality
visual and interaction design package** for the VTSO operational
console. It establishes a coherent "Horizon Dark" design language,
a thorough 3-column shell, a complete 5-step Decision Support
Window (DSW) flow, and a comprehensive component inventory.

**Broad alignment verdict.** The handoff aligns well with the
**single-role VTSO console** described in the V1 Screen Architecture
(§3 VTSO Console). It aligns partially with the **DSW conceptual
flow** described in the Operational Workflow Model (recommendation
lifecycle §6) and the Permission Model (decision-time authority).
It does **not** align with the broader V1 foundation in five
significant ways: (a) role-based access is not implemented (only
a presentational role switcher exists), (b) decision-commit /
audit-trail / replay surfaces are visually present but lack any
backend contract, (c) sensitive-information boundaries are not
enforced (full data is shown to any authenticated user), (d) all
non-VTSO consoles are absent (Harbour Master authority surfaces,
Port Executive dashboard, Shift Supervisor handover flow,
Stakeholder mobile, Marine Infrastructure), and (e) device strategy
(mobile, executive desktop) is absent.

**What should be accepted as authoritative design input** (once
formally authorised — see §10 and §14):
- Horizon Dark colour tokens, typography, spacing, card system
- VTSO 3-column shell layout (`360 | 1fr | 380`)
- DSW 5-step conceptual flow (Signal → Cascade → Options → Commit → Ledger)
- Component visual hierarchy (Card / Pill / CategoryPill / Dot / Icon)
- Conditions bar composition (rating / wind / swell / vis / pressure / tide / UKC / sea temp)
- Centre-tab inventory for the VTSO console (Dashboard / Berth Timeline / Shift Log / VTS Map / Pilotage / Performance)

**What should be amended before any V1.0 implementation begins:**
- "Never modify server.py" rule reconciled with the Execution Plan's
  single static-route addition for `/v1/*`
- "Replace index.html" framing reconciled with the Execution Plan's
  coexistence model
- Role switcher reframed as a **display-only preview**, not an
  RBAC mechanism
- DSW step 5 ("Audit trail preserved. Resolution applied to live
  schedule") reframed as **pending backend contract** rather than
  a delivered capability
- `/api/summary` shape assumptions explicitly flagged as **to be
  verified** against the Beta 10 endpoint (the handoff describes
  a shape that may not match what Beta 10 actually returns today)

**What should remain non-authoritative:**
- Any direct instruction about backend / API endpoints
- Any direct instruction about role permissions or authorisation
  enforcement
- Any direct instruction about audit emission or replay storage
- Any direct instruction about production deployment, Railway
  configuration, or Stage E-prod activation
- The 4-week phased timeline (this should follow the Execution
  Plan's M0–M9 governance, not the handoff's Phase 1–4 cadence)

**Recommendation.** Authorise the handoff as a **design input
(visual + interaction)** with the amendments in §9 documented
alongside it. Do **not** authorise it as an implementation
instruction set. Do **not** authorise M0 implementation yet —
M0 should only be opened after (a) these amendments are
documented, (b) ChatGPT engineering review confirms scope, and
(c) the `horizon-v1-sandbox` Railway project is provisioned per
Execution Plan §8.

---

## 2. Handoff Inventory

### 2.1 Files reviewed

| File | Path | Size | Status |
|---|---|---|---|
| Implementation prompt | `v1-handoff/V1-Implementation-Prompt.md` | 22.3 KB / 586 lines | Read in full |
| README | `v1-handoff/README.md` | 2.5 KB / 53 lines | Read in full |
| Prototype: root app | `v1-handoff/prototype/app.jsx` | 4.2 KB / 132 lines | Read in full |
| Prototype: shared components | `v1-handoff/prototype/components.jsx` | 11.3 KB | Inspected (listing + size) |
| Prototype: panels | `v1-handoff/prototype/panels.jsx` | 17.4 KB | Inspected (listing + size) |
| Prototype: centre tabs | `v1-handoff/prototype/tabs.jsx` | 23.3 KB | Inspected (listing + size) |
| Prototype: Decision Support Window | `v1-handoff/prototype/dsw.jsx` | 19.7 KB | Inspected (listing + size) |
| Prototype: layout variations | `v1-handoff/prototype/variations.jsx` | 19.1 KB | Inspected (listing + size) |
| Prototype: sample data | `v1-handoff/prototype/data.js` | 13.6 KB / 188 lines | Read in full |
| Prototype: complete CSS | `v1-handoff/prototype/Horizon V1.html` | 56.9 KB | Listed (CSS inside `<style>` block) |
| Screenshots | `v1-handoff/prototype/screenshots/` | 12 PNG files | Listed (filename inventory) |
| Zipped reference | `v1-handoff/prototype.zip` | 443 KB | Not extracted (duplicate of folder) |

### 2.2 Screenshot inventory

`screenshots/` contains 12 visual references (note: README claims
13, actual count is 12):
- `01-dashboard.png`
- `02-full.png`
- `04-default.png`
- `06-now.png`
- `07-right.png`
- `08-dsw-step1.png`
- `09-dsw-step2.png`
- `11-variations.png`
- `12-shiftlog.png`
- `13-timeline.png`
- `14-swapped.png`
- `15-swapped-right.png`

Gaps in numbering (03, 05, 10) suggest the package was pruned
during preparation. **Minor inventory discrepancy noted** —
not blocking.

### 2.3 Files NOT in the handoff (notable absences)

| Missing | Why notable |
|---|---|
| Harbour Master authority screens | Permission Model §5.2 + Screen Architecture §4 require authority surfaces |
| Port Executive dashboard | Permission Model §5.4 + Screen Architecture §6 |
| Shift Supervisor handover flow | Operational Workflow Model §4 (Shift lifecycle) |
| Stakeholder mobile surface | Permission Model §5.5 + Screen Architecture §8 |
| Marine Infrastructure console | Permission Model §5.6 + Screen Architecture §9 |
| Replay workspace | Operational Workflow Model §11 (replay) + Information Architecture §15 (replay data) |
| Escalation UI | Operational Workflow Model §8 (4 canonical escalation paths) |
| Handover acknowledgement flow | Operational Workflow Model §5.3 |
| Defer / override UI | Phase 0 reserved event types `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE` |
| Reason-code capture for HM authority actions | Screen Architecture §4.3 (HM actions require structured reason codes + secondary confirmation) |
| Audit chain integrity surface | Information Architecture §13 (chain status) |
| AI provenance display (source / scope / lineage) | Information Architecture §16 (AI prerequisites) |

These are not handoff *defects* — the handoff is scoped to V1.0
VTSO console. They are the boundary of what the handoff can
authoritatively define versus what must come from the foundation.

---

## 3. Alignment with V1 User & Permission Model

### 3.1 Roles represented

The handoff's role inventory (from `app.jsx:12`):

```js
const ROLES = ['VTSO', 'Harbour Master', 'Shift Supervisor',
               'Port Executive', 'Stakeholder', 'Marine Infra'];
```

This is a **perfect six-role match** with Permission Model §5
(VTSO, Harbour Master, Shift Supervisor, Port Executive, Port
Stakeholders, Marine Infrastructure). Naming aligns. **Strong
alignment.**

### 3.2 Role switcher assumptions — CRITICAL GAP

The role switcher in `app.jsx` is purely client-side. Selecting
a role changes a tweaks-panel state variable (`t.role`) which is
passed as a prop to `HorizonHeader`. **No API call. No auth claim.
No session re-issue. No backend permission check.**

This is acceptable for a **design prototype** demonstrating "what
the UI looks like for role X." It is **not** acceptable as an
implementation pattern for V1.0 because:

- Permission Model §3 requires server-issued role attached to the
  session cookie
- Permission Model §6 requires server-side enforcement of the
  permission matrix (25 permissions × 6 roles)
- Permission Model §11 states: "Permissions MUST be enforced at
  the API layer. Frontend role display is presentation only and
  MUST NOT be the source of authority."

If the handoff role switcher is reused unchanged in V1.0, it
creates a **frontend permission leakage** risk: a user could
switch their displayed role to Harbour Master client-side and
the UI would render HM surfaces even though the server still
considers them VTSO. The server would correctly reject privileged
API calls, but the UI would mislead the operator and any
screenshots/audit captured would show a false role.

### 3.3 Missing RBAC considerations

| Required by Permission Model | Present in handoff |
|---|---|
| Session cookie carries role claim | Not addressed |
| Permission matrix enforced server-side | Not addressed |
| Hidden destinations don't appear (not greyed-out) per Screen Architecture §2.4 | Not implemented — role switcher reveals all surfaces |
| HM authority actions require secondary confirmation + reason codes | Not present |
| Audit events carry `user_id`, `role`, `scope`, `session_id` | Not addressed (no audit emission UI) |
| Role-specific console (not just header label changes) | Not implemented — only VTSO console exists |

### 3.4 What must remain server-side

- Role assignment (issued at login, signed into session cookie)
- Permission matrix evaluation (every API call)
- Authority action gating (HM-only actions blocked at API)
- Audit emission tied to authenticated identity (not client claim)
- Role-scoped data projection (per Information Architecture §11)

### 3.5 Section verdict

**Partial alignment.** The handoff names the right six roles but
implements only a single role's console (VTSO) and uses a
client-side role switcher that must be reframed as a **display
preview, not an RBAC mechanism**, before any code is written.

---

## 4. Alignment with Operational Workflow Model

### 4.1 Shift lifecycle

The handoff includes a **single-row "handover" divider** in the
Shift Log (`data.js` line 111: `{ time: '06:00', type: 'HANDOVER',
event: 'Night → Day shift — VTSO Halvorsen', status: 'LOGGED',
divider: true }`).

Workflow Model §4 defines 8 shift substates (pre-shift brief,
shift open, in-shift, handover preparation, handover in progress,
handover accepted, shift closed, post-shift review) with audit
events `SHIFT_OPENED`, `HANDOVER_CREATED`, `HANDOVER_ACCEPTED`,
`SHIFT_CLOSED`.

**Gap:** the handoff shows handover as a single log entry, not a
lifecycle. There is no UI for preparing a handover brief, no
acknowledgement flow, no shift open/close confirmation.

### 4.2 Recommendation lifecycle

Workflow Model §6 defines an 11-stage recommendation lifecycle
(detected → generated → presented → reviewed → committed →
applied → outcome observed → audit complete, with branches for
deferred, overridden, escalated, expired).

The handoff DSW maps clearly to **stages 1–5** (signal detected →
options presented → operator commits decision). Stages 6–11
(applied to live schedule, outcome observation, audit completion,
post-decision review) are **shown in the UI** (step 5 "Decision
logged. Resolution applied to live schedule. Audit trail
preserved") but are **not backed by any API contract**.

This creates the **false sense of audit completion** risk
(restated in §13). The UI implies completion of a workflow that
the backend does not yet support.

### 4.3 Conflict ownership

Workflow Model §7 requires explicit conflict ownership — an
operator "owns" an active conflict from detection to resolution,
and ownership transfers explicitly during shift handover.

**Gap:** the handoff DecisionCard shows the conflict, the
countdown, and the resolution options, but no ownership claim,
no "transfer to incoming shift" affordance, no "owned by"
indicator.

### 4.4 Escalation

Workflow Model §8 defines 4 canonical escalation paths
(VTSO → HM, VTSO → Shift Supervisor, HM → Port Executive,
any → external authority). Event type `OPERATOR_ESCALATED` is
a V1 candidate.

**Gap:** no escalation UI in the handoff. The DSW assumes the
operator commits the decision themselves; there is no "escalate"
button, no escalation reason capture, no inbound escalation
inbox.

### 4.5 Handover

See §4.1 — partially represented (a single divider row), not
implemented as a lifecycle.

### 4.6 Defer / override

Phase 0 reserved event types `OPERATOR_DEFERRED` and
`OPERATOR_OVERRODE`. Workflow Model §6 branches include both.

**Gap:** the DSW has only "Open Full Analysis" / "Commit
decision" affordances. There is no defer affordance (acknowledge
without committing), no override affordance (commit a non-
recommended option with reason capture).

### 4.7 Replay

**Absent from prototype.** Workflow Model §11 and Information
Architecture §15 define replay as a V1.6 capability with its own
workspace, timeline, recommendation chain expansion, escalation
chain reconstruction, and hash-proof export.

This is acceptable — replay is downstream of V1.0 — but means
the handoff cannot be the source for replay design.

### 4.8 Section verdict

**Partial alignment.** Recommendation-lifecycle stages 1–5 are
well represented. Shift, escalation, ownership, defer/override,
and replay are absent or only marginally represented. Most of
these are V1.x scope, but the handoff should not be authorised
as covering them.

---

## 5. Alignment with Screen Architecture

### 5.1 VTSO console

Screen Architecture §3 (VTSO Console) specifies a 3-column shell:
- Left: alerts + active decision card
- Centre: dashboard / timeline / map / log
- Right: vessel roster + audit log

The handoff implements exactly this (`app.jsx` lines 70–88,
`hz-shell` grid). **Strong alignment.**

### 5.2 Left / centre / right panel model

| Screen Architecture spec | Handoff implementation | Verdict |
|---|---|---|
| Left: alerts + decision card | LeftPanel with AlertCard list + DecisionCard | Aligned |
| Centre: tabbed content | CenterPanel with 6 tabs (Dashboard, Berth Timeline, Shift Log, VTS Map, Pilotage, Performance) | Aligned, with rich tab set |
| Right: roster + audit log | RightPanel with Vessel Roster + Audit Log tabs | Aligned |
| Persistent left rail (per Screen Architecture §2.1) | Not present — the handoff replaces left rail with content panel | **Mismatch** |
| Top nav bar (per Screen Architecture §2.1) | `HorizonHeader` carries port + role + clock; no global nav | **Mismatch** |

The Screen Architecture's "persistent left rail" and "top bar" are
**global navigation chrome** for moving between role consoles. The
handoff's left "panel" is **content** for the VTSO console. These
are not the same. When non-VTSO consoles are designed (HM, Port
Exec, etc.), the handoff's layout will need a navigation chrome
added around it, or the handoff's "panel" model will need to be
rethought.

### 5.3 DSW flow

Screen Architecture §3.4 describes DSW as a "guided decision
surface that surfaces the operator's options with full context."
The handoff implements a polished 5-step modal:
1. Conflict detected (signal cards)
2. Downstream impact (cascade tree)
3. Resolution options (A/B/C with safety/delay/cost/confidence)
4. Commit decision (reasoning textarea)
5. Decision logged (success state)

**Strong alignment** with the conceptual flow. Authoritative as
the V1.0 DSW visual + interaction design (with §9 amendments).

### 5.4 Right panel

Vessel Roster + Audit Log split is consistent with Screen
Architecture §3.5. The audit-log tab is **presentationally
correct** but back-end audit emission is not yet wired (Stage
E-prod still paused) — see §13.

### 5.5 Stakeholder / mobile gaps

**Absent.** Screen Architecture §8 (Stakeholder mobile) is a
device-specific role console, not a degraded desktop. The handoff
contains no mobile design and no stakeholder surface.

### 5.6 Executive gaps

**Absent.** Screen Architecture §6 (Port Executive dashboard)
requires a strategic view (KPI rollups across shifts, weekly
patterns, exception summaries). The handoff's "Performance" tab
is closer to VTSO situational awareness than executive rollup.

### 5.7 Replay workspace gaps

**Absent.** Screen Architecture §10 (Replay Workspace) — not in
handoff. See §4.7.

### 5.8 Device strategy

Screen Architecture §11 establishes device-specific strategies
(desktop for VTSO/HM/Shift Supervisor, executive desktop for
Port Exec, mobile for Stakeholder, ruggedised tablet for Marine
Infra). The handoff is **single-device (1440px+ desktop)** only.

### 5.9 Section verdict

**Strong alignment for the VTSO desktop console; absent for all
other consoles and device strategies.** The handoff is correctly
scoped to "VTSO V1.0" but cannot authoritatively define the rest
of the Screen Architecture.

---

## 6. Alignment with Information Architecture

### 6.1 /api/summary data assumptions

The handoff describes a flat `/api/summary` response (V1-
Implementation-Prompt.md §2). The `data.js` sample uses
different field naming than the documented JSON shape (e.g.
`fleet` vs `vessels`, `conditions.wind.speed` vs
`conditions.wind_speed`). This is a **minor inconsistency
internal to the handoff** but flags a larger concern:

**The handoff *assumes* a particular `/api/summary` shape but has
not verified it matches what Beta 10 actually returns today.**
Required M0 spike: capture the real `/api/summary` response from
all four ports (Brisbane, Melbourne, Geelong, Darwin) and
reconcile shape differences before any component is wired to live
data. **This must happen before M0 acceptance.**

### 6.2 Source-of-truth handling

Information Architecture §4 defines source-of-truth per domain
(weather: BoM, vessels: AIS/MST/SIM, schedules: in-memory,
conflicts: derived). The handoff carries `source: 'AIS'`,
`'MST'`, or `'SIM'` per vessel — **aligned** for vessels.

**Gap:** no UI surfacing of source-of-truth for non-vessel
information (weather provenance, schedule provenance, conflict
provenance). The DSW shows options with `confidence` but no
provenance trail.

### 6.3 Role-scoped projection

Information Architecture §11 requires `/api/summary` to project
data per role (e.g., Stakeholders see public-safe fields only;
Marine Infrastructure sees berth/structural data only).

**Gap:** the handoff assumes a single summary shape served to all
authenticated users. The Implementation Strategy §17 lists this
as a V1.1+ rewrite. The handoff is **silent on projection** —
not a defect for V1.0 (single VTSO console), but the design must
not assume the same summary shape will serve every role.

### 6.4 Sensitive information boundaries

Information Architecture §13 lists 10 sensitive-info categories
(vessel commercials, draft/cargo manifests, agent details,
operator names, pilot rosters, etc.). The handoff's vessel cards
show `cargo`, `agent`, `flag` — **acceptable for VTSO** but **not
acceptable for Stakeholders or Marine Infra**.

The boundary is enforced by role-scoped projection (§6.3 above),
which is not in the handoff's scope. **No defect, but the handoff
must not be used to set the policy.**

### 6.5 Audit / replay data needs

Information Architecture §15 defines replay data needs (timeline
reconstruction, recommendation chain, escalation chain, audit
ledger integrity, hash-proof export). **The handoff does not
address replay data.**

### 6.6 AI boundaries

Information Architecture §16 requires AI-generated content to
carry source + scope + lineage. The DSW option cards carry
`confidence` (e.g. 88%) and `recommended: true` but **no source
lineage** (which model? trained on what data? when?), **no
scope** (this option is correct under what conditions?), **no
provenance trail**.

If the DSW is used as a basis for operational decisions in V1.0,
**source + scope + lineage MUST be added** to the design before
authorisation. This is in §9.

### 6.7 Section verdict

**Partial alignment.** VTSO-shaped data is well represented; role
projection, sensitive boundaries, replay data, and AI provenance
are gaps that must be closed at architecture (not handoff) level.

---

## 7. Alignment with Implementation Strategy and Execution Plan

### 7.1 Frontend-first approach

Execution Plan §1 / §5 articulates a "frontend-first replacement
strategy": React frontend rides on the existing Beta 10
`/api/summary`; backend evolution (RBAC, role-scoped projection)
is sequenced into V1.1+ once the frontend foundation is stable.

The handoff is **consistent with this approach** — it explicitly
states "you are replacing `index.html` with a React frontend; you
are NOT touching `server.py`" (V1-Implementation-Prompt.md §1
line 28). **Strong alignment in spirit.**

### 7.2 "No server.py" rule

| Source | Wording |
|---|---|
| Handoff §1 line 28 | "You are NOT touching `server.py`" |
| Handoff §6 rule 1 | "Never modify server.py." |
| Execution Plan §5.3 | "`server.py` is modified **only** to serve the React app's static build at `/v1/*` and to serve the SPA index.html for client-side routing. **No business logic in server.py changes.**" |

These are **not contradictory in intent** (both want backend
behaviour untouched in V1.0), but they are **literally
inconsistent in absolute wording**. The Execution Plan's
narrower interpretation (one static-route addition) is the
correct V1.0 posture; the handoff's absolute rule needs to be
reconciled in the §9 amendment list.

### 7.3 React at /v1/*

| Source | Position |
|---|---|
| Execution Plan §5.2 | "Both frontends ship in the same Beta 10 deploy initially. `index.html` continues to serve at `/` (Beta 10 route); React frontend serves at `/v1/` (new route prefix)" |
| Handoff §1 line 28 | "You are replacing `index.html` with a React frontend" |

**Tension.** Handoff says "replace"; Execution Plan says
"coexist". Both are defensible but only one can be the
authorised posture for V1.0. The Execution Plan's coexistence
posture is safer (it leaves Beta 10 demos working during V1
build) and should win. The handoff's "replace" framing must be
reframed in the §9 amendments.

### 7.4 Coexistence with Beta 10

The handoff is **silent on coexistence**. If the handoff is
treated as an absolute "replace" instruction, Beta 10 demos
break the day V1.0 lands. If the handoff is reframed to "coexist
at `/v1/*`", Beta 10 demos continue during V1 build. **The
reframe is required.**

### 7.5 V1 sandbox requirement

Execution Plan §8 requires a `horizon-v1-sandbox` Railway project
distinct from `horizon-prod` (Beta 10). The handoff does not
address Railway topology, deployment, or environment isolation
— this is correctly outside its scope, but means **deployment
decisions cannot be made from the handoff alone**.

### 7.6 Milestone fit

Execution Plan §11 milestones M0–M9 are organised by capability
(scaffold → app shell → dashboard → operational panels → DSW →
role-aware rendering → what-if → audit-linked actions → replay
→ hardening). The handoff's Phase 1–4 (V1-Implementation-Prompt.md
§5) is organised by surface (scaffold → core panels → remaining
tabs+DSW → integration+polish).

The two cadences are **not contradictory** but are **structured
differently**. The Execution Plan's M0–M9 cadence should be the
authoritative implementation sequence; the handoff's Phase 1–4
should be treated as a **design checklist**, not a milestone
plan.

### 7.7 M0 readiness

The Execution Plan defines M0 as "Frontend Scaffold + Design
Tokens" with `npm run build` producing `frontend/dist/` and
`server.py` adding `/v1/*` static-file routing.

**M0 is not ready to be opened.** Prerequisites:
1. UX/UI handoff formally authorised (this PR is the input to
   that decision).
2. `/api/summary` shape verified against the handoff's
   assumptions (M0 spike, see §6.1).
3. `horizon-v1-sandbox` Railway project provisioned.
4. ChatGPT engineering review of this validation report.
5. Tony's explicit M0 authorisation.

Only after all five close can M0 implementation begin.

### 7.8 Section verdict

**Aligned in spirit, with reconciliation items.** The handoff's
"never modify server.py" + "replace index.html" framings need to
be reframed to match the Execution Plan's "one static-route
addition" + "coexist at `/v1/*`" posture before M0 is opened.

---

## 8. Key Conflicts / Tensions

### 8.1 "Replace index.html" vs coexistence at /v1/*

| Side | Position |
|---|---|
| Handoff | "You are replacing `index.html` with a React frontend" |
| Execution Plan | "Both frontends ship in the same Beta 10 deploy initially; React at `/v1/*`" |

**Resolution required.** Execution Plan's coexistence model wins.
Reframe handoff before authorisation.

### 8.2 Role switcher in prototype vs real RBAC

| Side | Position |
|---|---|
| Handoff | Client-side `<select>` changing a tweaks-panel state variable |
| Permission Model | Server-issued role attached to session, enforced at API layer |

**Resolution required.** Treat the role switcher as a **display-
only preview** that does not change the user's actual session
role. Real RBAC ships in V1.1+ per Implementation Strategy §9.

### 8.3 DSW decision-logged UI vs no audit/action endpoint

| Side | Position |
|---|---|
| Handoff DSW step 5 | "Decision logged. Audit trail preserved. Resolution applied to live schedule." |
| Backend reality | No "apply decision" endpoint. Audit emission off in production (Stage E-prod paused). |

**Resolution required.** Reframe DSW step 5 as **visual mock**
until the audit-linked action endpoint exists (Execution Plan M7).
For M0–M6, step 5 should display "Decision recorded locally
(production audit pending V1.x)" or similar honest framing.

### 8.4 What-if / apply actions vs current endpoint semantics

| Side | Position |
|---|---|
| Handoff §2 | Lists `POST /api/whatif`, `apply-whatif`, `clear-whatif` as endpoints the frontend "will need" |
| Beta 10 reality | These endpoints exist (Phase 1.1 hardened them) but their semantics are scenario-overlay, not authoritative-action |

**Clarification required.** The "apply-whatif" endpoint does
**not** apply a decision to the live schedule — it overlays a
hypothetical onto the in-memory state. The DSW's "Commit
decision" must not be wired to apply-whatif (that would silently
turn hypotheticals into "committed decisions"). A separate
decision-commit endpoint must be designed (V1.x) before the DSW
commit flow can write through to the audit ledger.

### 8.5 Mobile / stakeholder surfaces not yet designed

| Side | Position |
|---|---|
| Handoff | Single 1440px+ desktop layout only |
| Permission Model + Screen Architecture | Stakeholder surface is mobile-first; Port Exec is executive desktop; Marine Infra is ruggedised tablet |

**Out of V1.0 scope.** Acknowledge in §11 (non-authoritative)
that the handoff does not define mobile / stakeholder / executive
surfaces.

### 8.6 Replay not yet present in prototype

| Side | Position |
|---|---|
| Handoff | No replay UI |
| Workflow Model §11 + Screen Architecture §10 + Information Architecture §15 | Replay workspace, timeline reconstruction, hash-proof export |

**Out of V1.0 scope.** Replay is V1.6. The handoff is correctly
silent on it but cannot define it later.

### 8.7 Audit-linked action state not yet available

| Side | Position |
|---|---|
| Handoff Audit Log tab | Right-panel chronological event list (timestamp, actor, action, reference) |
| Beta 10 reality | Audit emission disabled in production (`DATABASE_URL` unset, Stage E-prod paused) |

**Resolution required.** The Audit Log tab in V1.0 must show
**locally-generated session events** (the user's own clicks /
selections during this session) **plus** any server-emitted audit
events visible via a future `GET /api/audit` endpoint — which
does not exist today. For M0–M6, the Audit Log can show
session-only events; backend-sourced audit display is V1.x.

---

## 9. Required Amendments Before Implementation

These are mandatory amendments / clarifications. They are **not
changes to the handoff files** (those remain as design reference)
— they are clarifications recorded in the implementation
authorisation, in this validation report, and in the M0 PR
description when M0 is opened.

### 9.1 Reframe "Never modify server.py" → "No business-logic changes to server.py"

The Execution Plan §5.3 permits a single static-route addition
to serve `/v1/*`. Document this exception explicitly in the M0
authorisation.

### 9.2 Reframe "Replace index.html" → "Coexist with index.html at /v1/*"

Beta 10 `/` route continues; React mounts at `/v1/*`. Document
this in M0.

### 9.3 Reframe role switcher → "Display-only preview"

Add a visible label ("Preview role: VTSO — does not change your
permissions") to the role switcher in the V1.0 frontend. Real
RBAC ships V1.1+.

### 9.4 Reframe DSW step 5 → "Decision recorded locally (production audit pending)"

Until the audit-linked action endpoint exists (M7), DSW step 5
must not claim "audit trail preserved" or "applied to live
schedule." Honest framing required.

### 9.5 Verify /api/summary shape against handoff assumptions (M0 spike)

Capture real `/api/summary` from all 4 ports; reconcile field
names (`fleet` vs `vessels`, `conditions.wind.speed` vs
`conditions.wind_speed`); document the canonical shape in
Information Architecture v0.2 before M0 implementation begins.

### 9.6 Add AI provenance to DSW option cards

Per Information Architecture §16, option cards must carry source,
scope, and lineage — not just confidence. Add a "How was this
recommendation generated?" affordance to each option card before
the DSW is wired to a real recommendation engine. (V1.0 may
display "Demo data" placeholder until the engine ships.)

### 9.7 Adopt Execution Plan M0–M9 cadence

The handoff's Phase 1–4 timeline is reframed as a **design
checklist**; the authoritative implementation cadence is M0–M9
per Execution Plan §11.

### 9.8 Document the 12-screenshot count

Update the handoff README (if it is touched at all) to reflect
the actual 12-screenshot count, not 13. Low priority — note for
future hygiene only.

---

## 10. What Can Be Accepted As Authoritative

The following elements of the handoff **may be authorised as
design input** (with the §9 amendments documented alongside):

| Authoritative item | Source in handoff | Notes |
|---|---|---|
| Visual design tokens | §3.1 (colour, typography, spacing) | Authoritative for V1.0 frontend; do not adopt for Beta 10 |
| Component visual hierarchy | §4.8 (Card, Pill, CategoryPill, Dot, Icon) | Reusable across all V1 consoles |
| VTSO console 3-column layout | §4.1 (`360 \| 1fr \| 380`) | Authoritative for VTSO only; HM/Exec/Stakeholder/Marine Infra layouts not yet defined |
| DSW conceptual flow | §4.7 (Signal → Cascade → Options → Commit → Ledger) | Authoritative for the visual flow; step 5 framing per §9.4 |
| Horizon Dark operational design language | §3 (entire design system) | Authoritative for visual identity |
| Conditions bar composition | §4.3 | Authoritative for VTSO |
| Centre-tab inventory for VTSO console | §4.5 (6 tabs) | Authoritative for VTSO; non-VTSO tabs deferred |
| Card variants (critical / accent / selected) | §3.4 | Authoritative across V1 |
| Lucide-style icon set | §4.8 | Authoritative |

The above can be cited in V1.0 implementation PRs as the visual
specification.

---

## 11. What Must Remain Non-Authoritative

The following elements of the handoff **must NOT be treated as
authoritative**:

| Non-authoritative item | Why |
|---|---|
| Backend / API contract changes | Handoff is a frontend artefact; API contracts belong to Information Architecture and Implementation Strategy |
| RBAC / authorisation implementation | Permission Model is authoritative; handoff role switcher is display-only |
| Production deployment | Execution Plan §13 governs deployment |
| Audit activation (Stage E-prod) | Implementation Strategy §9.4 governs; remains paused |
| Role permissions | Permission Model §6 (matrix) is authoritative |
| Final workflow semantics | Operational Workflow Model is authoritative |
| Phase 1–4 timeline | Execution Plan M0–M9 is the implementation cadence |
| Replay design | Out of scope for handoff |
| Mobile / stakeholder / executive surfaces | Out of scope for handoff |
| `server.py` modifications | Execution Plan §5.3 governs (single static-route exception) |
| Decision-commit endpoint behaviour | To be designed in V1.x |
| What-if endpoint semantics | Phase 1.1-hardened; not a decision-apply path |
| Sensitive-information policy | Information Architecture §13 is authoritative |
| AI provenance policy | Information Architecture §16 is authoritative |
| Audit-log data source | Information Architecture §15 + Phase 0.7 audit module |
| Cross-port behaviour | Implementation Strategy + Beta 10 port profile logic |

---

## 12. Recommended M0 Scope If Handoff Is Accepted Later

If the handoff is authorised as a design input (with §9
amendments), the following M0 scope is recommended. **This is
not an M0 authorisation** — it is a recommendation for what M0
should look like when Tony separately authorises it.

### 12.1 Scope

- Create `frontend/` directory in the repo on an isolated branch
- Vite + React scaffold (per Execution Plan §5.1)
- Implement design tokens (CSS custom properties from
  `v1-handoff/prototype/Horizon V1.html` `<style>` block)
- Build shared components (Card, Pill, CategoryPill, Dot, Icon
  set, PageHead, SectionHd)
- Stub `App.jsx` that renders the empty 3-column shell and the
  Horizon header
- **No `/api/summary` polling yet** — use the `data.js` static
  data set as the source for the rendered shell
- **No actions** (no buttons that POST anywhere)
- **No audit emission** (no audit-related code in the frontend
  beyond stubs)
- **No RBAC claims** — the tweaks-panel role selector is labelled
  "Preview role (display only — does not change permissions)"
- **No Beta 10 replacement** — no `server.py` change in M0
- **No production deployment** — frontend lives only in
  `horizon-v1-sandbox` Railway project (or is not deployed at all
  in M0)

### 12.2 Deliverables

- `frontend/package.json`, `vite.config.js`, `index.html`
- `frontend/src/styles/tokens.css`, `base.css`, `layout.css`
- `frontend/src/components/Card.jsx`, `Pill.jsx`, etc.
- `frontend/src/layout/HorizonHeader.jsx`, `ConditionsBar.jsx`,
  `LeftPanel.jsx`, `CenterPanel.jsx`, `RightPanel.jsx`
- `frontend/src/data/sample.js` (port of `v1-handoff/prototype/data.js`)
- `frontend/dist/` build output (in `.gitignore`, not committed)

### 12.3 Out of scope for M0

- `/api/summary` polling
- Login flow
- Tab content beyond an empty Dashboard
- DSW
- VTS map
- Berth Timeline
- Shift Log
- Pilotage
- Performance
- Port switching
- Any `server.py` modification
- Any Beta 10 production change

### 12.4 Acceptance criteria for M0

- `npm run build` produces `frontend/dist/`
- The 3-column shell renders against `sample.js` data
- Design tokens visually match the handoff prototype (manual
  side-by-side check)
- Regression gate (`tests/test_beta10_regression.py`) still
  passes 46/46
- No `server.py` change in the PR
- No Beta 10 file modified

### 12.5 Rollback criteria

- M0 is reversible by deleting the `frontend/` directory and the
  PR. Beta 10 is unaffected because it never depended on M0.

---

## 13. Risk Register

### 13.1 Operational mismatch

**Risk:** the handoff designs operator behaviour (decision
commit, conflict acknowledgement, handover) before the workflow
semantics are locked. If the workflow model evolves during V1.x,
the frontend may need re-design.

**Detection:** quarterly cross-check between Workflow Model
v0.x and the implemented frontend.

**Mitigation:** keep V1.0 frontend visually faithful to the
handoff but **architecturally thin** — components consume props,
not workflow logic. Workflow logic stays in the backend (V1.x).

### 13.2 Frontend permission leakage

**Risk:** the client-side role switcher convinces operators they
have changed role; they take screenshots / actions believing
their identity is X when the server still considers them Y.

**Detection:** any V1.0 user testing where role-switching is
demonstrated; any incident response where the operator's logged
role doesn't match the perceived role.

**Mitigation:** apply §9.3 amendment — display-only label,
visible disclaimer; defer real RBAC to V1.1+.

### 13.3 False sense of audit completion

**Risk:** the DSW step 5 "Decision logged. Audit trail preserved.
Resolution applied to live schedule" creates a false belief that
the V1.0 frontend has wired through to the audit ledger and the
schedule. In reality, V1.0 backend audit is paused (Stage E-prod
deferred) and the apply-decision endpoint does not exist.

**Detection:** any audit / compliance review of V1.0; any
operator who asks "where do I find my decision log?"; any
incident response where the audit chain is expected to contain
operator decisions and does not.

**Mitigation:** apply §9.4 amendment — honest framing on step 5
until M7 wires the audit-linked action endpoint.

### 13.4 Visual design driving architecture

**Risk:** the handoff is so polished that engineering treats it
as not just visual authority but architectural authority. Backend
work gets reshaped to fit the prototype's data shape rather than
the Information Architecture's role projection model.

**Detection:** any V1.x backend PR that introduces a new
endpoint solely to match a prototype affordance; any RBAC
shortcut that bypasses Permission Model §6.

**Mitigation:** explicit §11 ("non-authoritative") boundary
documented; this validation report cited in every V1 PR
description.

### 13.5 Beta 10 contamination

**Risk:** in the rush to build V1, someone modifies Beta 10's
`index.html` or `server.py` to match handoff styling or
endpoints. Beta 10 production regresses.

**Detection:** the regression gate (`test_beta10_regression.py`)
catches API contract changes; visual regression in Beta 10 demos
catches CSS contamination.

**Mitigation:** Phase 0 protected file list remains immutable in
V1.0; V1.0 PRs touching protected files are rejected automatically
via review.

### 13.6 Deployment confusion

**Risk:** the handoff implies a single deploy ("Live production:
Railway, auto-deploy from main"). If V1.0 frontend ships to the
same `horizon-prod` Railway project, a bad V1.0 build can take
Beta 10 production down.

**Detection:** any V1.0 PR whose deploy target is `horizon-prod`.

**Mitigation:** `horizon-v1-sandbox` provisioned per Execution
Plan §8 before any frontend code is built; V1.0 deploys go there;
`horizon-prod` (Beta 10) untouched throughout V1 build.

### 13.7 Scope creep into V1.1+ workflows

**Risk:** the handoff's polished DSW invites scope creep —
"while we're building the DSW, let's also add escalation,
handover, defer, override." V1.0 bloats; ship dates slip.

**Detection:** any M0–M6 PR that introduces a non-VTSO surface
or a V1.x event type.

**Mitigation:** M0–M6 strictly bounded by §12 scope; new surfaces
require a new milestone authorisation.

---

## 14. Recommendation

### 14.1 Authorise the handoff as design input

**Yes — with the §9 amendments documented alongside the
authorisation.**

Specifically, authorise the items listed in §10 as authoritative
visual + interaction design for the V1.0 VTSO console, and
record the §9 amendments + §11 non-authoritative boundaries in
the same authorisation note.

### 14.2 Authorise M0 yet

**No — not yet.**

M0 should remain unopened until:

1. ChatGPT engineering review of this validation report
   completes
2. Tony issues a separate, explicit authorisation of the handoff
   as design input (with the §9 amendments recorded)
3. `horizon-v1-sandbox` Railway project is provisioned per
   Execution Plan §8
4. `/api/summary` shape verification spike completes (§6.1,
   §9.5) — this can be a tiny separate PR that captures the
   actual response from the four ports
5. Tony issues an explicit M0 authorisation with the §12 scope
   constraints

### 14.3 Gating conditions remaining

| Gate | Status | What closes it |
|---|---|---|
| Handoff design-input authorisation | OPEN | This validation report + Tony's authorisation |
| `/api/summary` shape verification | OPEN | Small spike PR capturing live responses |
| `horizon-v1-sandbox` provisioning | OPEN | Tony or Tony-side ops |
| M0 scope authorisation | OPEN | Tony's explicit M0 authorisation citing §12 |
| ChatGPT review of this report | OPEN | ChatGPT review |
| Stage E-prod | INTENTIONALLY PAUSED | Implementation Strategy §9.4 (deferred to V1.x) |

### 14.4 What this report does NOT authorise

- Frontend code creation
- `frontend/` directory creation
- `package.json`, Vite scaffold, or any Node tooling
- `server.py` modification
- Audit emission activation
- Stage E-prod
- Beta 10 production change
- Railway configuration change
- Test additions / modifications
- Migration creation
- Environment variable changes
- Merge of any other PR
- Touching PRs #27, #28, #29

This report is **planning input only**.

---

## End of validation report

**Status:** v0.1 validation report — planning input only
**Next action:** ChatGPT engineering review, then Tony's
decision on (a) merging this report and (b) whether to issue a
separate authorisation of the UX/UI handoff as design input
with the §9 amendments.
