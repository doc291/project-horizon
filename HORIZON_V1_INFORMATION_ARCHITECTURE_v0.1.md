# Horizon V1 — Information Architecture (v0.1)

**Status:** Design draft — V1 planning only
**Document version:** 0.1
**Date:** 2026-05-15
**Companion documents (the V1 design triad, all on `main`):**
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` (PR #30, merged at `4c4940f`)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` (PR #31, merged at `6d09b3d`)
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` (PR #32, merged at `3e747dc`)
**Implementation status:** None. **This is not implementation approval.**
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

---

## 1. Executive Summary

This document defines the **V1 information architecture** for
Project Horizon — the substance of what flows through the system,
where it originates, who owns it, how it is transformed, how it is
projected to operators, and how it is preserved for replay,
analytics, and (eventually) AI consumption.

It is the fourth foundational V1 design document and completes the
**V1 design quartet**:

| Document | Question answered | Scope |
|---|---|---|
| Permission Model (v0.1) | **WHO** | role / permission / scope contract |
| Operational Workflow Model (v0.1) | **HOW** | temporal / coordination state machines |
| Screen Architecture (v0.1) | **WHERE** | screen hierarchy, navigation, layout strategy |
| **Information Architecture (v0.1, this doc)** | **WHAT** | data domains, sources, flows, role projections, sensitivity, lineage |

This document **complements the WHO / HOW / WHERE triad** by defining
the substance the other three documents act on. Where the permission
model says "Harbour Master may view audit trail," this document says
*what an audit trail is*, *where it came from*, *what it contains*,
and *what is filtered before it reaches the HM*. Where the workflow
model says "deferred recommendations enter a defer queue," this
document defines *what a recommendation actually is as data*, *what
its decision-time snapshot contains*, and *what fields persist
across the lifecycle*.

This document **consumes the triad as authoritative** for role,
workflow, and screen contracts. It does not redefine them; it
provides the information substrate they operate on.

**This document does not authorise any implementation.** It is v0.1
of an evolving model. The Phase 0 Beta 10 baseline remains protected
at `phase-0-complete @ 4ad4aae`; no V1 data model, schema, API
shape, or integration contract will be built until this document and
its three companions have been reviewed and accepted by operational
stakeholders and (separately) by the architecture team.

---

## 2. Design Principles

Eight principles govern V1 information architecture. They sit
alongside the principles of the other three V1 documents. Where
implementation conflicts with these principles, the principle wins.

1. **Information is operationally scoped.**
   Every piece of operational data exists within a tenant, a port,
   and (usually) a shift or assignment context. Scope is not an
   afterthought applied at API boundaries; it is a first-class
   property of every record. A vessel "exists" in Brisbane port
   scope for tenant `ams-demo`; it does not float free of context.

2. **Source of truth must be explicit.**
   Every data field has exactly one authoritative source at any
   moment, and that source is named. Vessel position from AISStream
   is one source-of-truth path; vessel position from QShips is
   another; vessel name from MyShipTracking another. The system
   never silently merges sources — it picks a primary and surfaces
   the choice to the operator with a confidence indicator.

3. **Derived insight must be traceable.**
   Every conflict, every recommendation, every score is computed
   from named inputs at a named moment. The §1.4.1 decision-time
   snapshot from Phase 0.7b captures this for recommendations; V1
   extends the principle to conflicts, scores, and any future
   derived signal. If a reviewer cannot trace from "this
   recommendation" back to "these specific inputs at this specific
   time," the lineage is broken.

4. **Sensitive data should not be over-shared.**
   Role-scoped projection (§9) filters what each role receives at
   the API layer, not at the UI. Pilot identities are de-identified
   to operational capability descriptors. Commercial information
   reaches Executives only when its commercial sensitivity allows.
   Stakeholder operational data is scoped to their own assignment.
   Default is the narrowest scope; widening is deliberate.

5. **Role-specific views receive only authorised data.**
   The /api/summary response a VTSO receives is not the same
   response an Executive receives. Filtering happens server-side
   per the permission model §8.7. A Harbour Master's view of an
   incident is more detailed than an Executive's view of the same
   incident; both are correct projections of the same underlying
   audit trail.

6. **Real-time and historical information must be separated.**
   Real-time operational state (current vessel positions, active
   conflicts, current recommendations) is mutable and ephemeral.
   Historical audit record (Phase 0 hash-chained ledger) is
   append-only and immutable. They share substance but not
   persistence rules. Mutating real-time state never corrupts the
   historical record; appending to the historical record never
   degrades real-time responsiveness.

7. **Audit events are operational records, not generic logs.**
   Every audit event represents a specific operational moment with
   a defined audience and replay relevance. The audit ledger is not
   a debugging log; it is the evidentiary substrate of decision
   accountability. Application debug logs (when they exist) live
   separately and never touch the audit chain.

8. **AI can consume information only after source, scope, and
   lineage are clear.**
   No future AI feature consumes Horizon data unless that data has
   explicit source attribution, explicit scope, and explicit
   lineage. AI augmentation is downstream of information
   architecture, not a substitute for it. Information that is not
   yet trustworthy by these criteria is not yet AI-ready.

---

## 3. Information Domains

V1 organises information into **17 domains**. Each has a defined
purpose, primary source(s), and operational use. Domains are not
silos — they cross-reference extensively — but each has a single
domain owner responsible for its semantic correctness.

| # | Domain | Purpose |
|---|---|---|
| D1 | **Vessel information** | Identity, characteristics, status, and position of vessels in or approaching the port |
| D2 | **Berth information** | Static (length, depth, max LOA) and dynamic (occupied / available / maintenance) berth state |
| D3 | **Movement plans** | Scheduled, in-progress, and recently-completed vessel movements (arrivals, departures, shifts) |
| D4 | **Conflicts** | System-detected operational conflicts (berth overlap, pilot conflict, weather risk, UKC issue) |
| D5 | **Recommendations** | System-generated recommended actions for conflicts, with decision-time snapshot |
| D6 | **Actions** | Operator-initiated state changes against recommendations, conflicts, or guidance items |
| D7 | **Escalations** | Authority-flow records: source actor, target authority, reason, closure |
| D8 | **Shifts** | Bounded operational windows with supervisor identity, scope, start/end |
| D9 | **Handovers** | Structured shift-transfer records — notes, pending-item inventory, escalation queue snapshot |
| D10 | **Weather and tide** | Current and forecast environmental state |
| D11 | **Pilotage** | Pilot assignments at the *operational context* level (Horizon's narrow scope; Kyber owns roster detail) |
| D12 | **Towage** | Tug assignments and capability state |
| D13 | **Stakeholder assignments** | Per-stakeholder records of vessel-movement assignments, confirmation status, delay reports |
| D14 | **Incidents** | Coordinated sequences of operationally-significant events warranting post-hoc review |
| D15 | **Audit events** | The append-only hash-chained ledger of every operationally-significant action and state change |
| D16 | **Executive metrics** | Aggregated derived measures (throughput, dwell time, incident counts, override rates) computed from D15 plus operational state |
| D17 | **System / admin configuration** | Tenant configuration, role definitions, permission grants, scope rules, reason-code catalogues |

### Domain cross-references

- **D4 (Conflicts)** consumes **D1, D2, D3, D10** to detect; produces records consumed by **D5**
- **D5 (Recommendations)** consumes **D4** + **D1, D2, D3, D10, D11, D12** to generate; embeds the §1.4.1 decision-time snapshot of those inputs
- **D6 (Actions)** is the operator-initiated counter-flow against **D5** and **D4**
- **D7 (Escalations)** is the authority counter-flow when **D6** doesn't resolve
- **D8 (Shifts)** is the temporal scope under which most other domains operate; every action / escalation / handover carries a `shift_id`
- **D9 (Handovers)** is structured transfer of **D7** queue + pending **D5** between **D8** boundaries
- **D13 (Stakeholder assignments)** is the peripheral participant view; references **D1, D2, D3, D11, D12**
- **D14 (Incidents)** is a curated view over **D15** for replay purposes
- **D15 (Audit events)** is the immutable substrate from which **D14, D16** are derived
- **D16 (Executive metrics)** is the aggregate read of **D15** + **D6** + **D7** over time windows
- **D17 (Admin configuration)** governs all other domains (role grants, scope filters, reason-code catalogues)

---

## 4. Source-of-Truth Model

For each domain, the authoritative source, secondary fallback,
derived fields, refresh cadence, confidence handling, owner,
fallback behaviour, and audit relevance.

### D1 — Vessel information

| Aspect | Detail |
|---|---|
| Authoritative source | **AISStream** (live AIS WebSocket, real-time positions + static data) |
| Secondary source | **MyShipTracking (MST)** API cache (Phase 0 currently implemented) |
| Tertiary source | **QShips / port system feeds** for scheduled vessels not yet emitting AIS |
| Quaternary source | **Simulation** (Beta 10 demo posture; not used in production) |
| Derived fields | predicted ETA refinement, draft estimate from cargo declarations, vessel class from LOA/beam |
| Refresh cadence | AISStream: continuous; MST: per `/api/summary` poll (~30s); QShips: as the port system feeds it |
| Confidence | from source: AIS-live = high; MST-cached = medium; QShips = depends on feed; simulation = synthetic (Beta 10 demo only) |
| Owner | Port Authority (delegated to VTS operationally) |
| Fallback behaviour | AISStream stale or unavailable → MST → QShips → simulation. Each fallback degrades confidence; surfaced to operator. |
| Audit relevance | `VESSEL_STATE_OBSERVED` (Phase 0 closed-set reserved; V1+ emission decision per workflow §14.3) |

### D2 — Berth information

| Aspect | Detail |
|---|---|
| Authoritative source (static) | `port_profiles.py` (Beta 10) → `config.ports.profile_jsonb` (V1) |
| Authoritative source (dynamic) | Operator-entered state via Marine Infrastructure permission |
| Derived fields | berth availability time-series, maintenance window forecast |
| Refresh cadence | static: deploy-time; dynamic: as Marine Infrastructure changes |
| Confidence | static: high; dynamic: high when explicitly set by named actor |
| Owner | Port Authority (Marine Infrastructure stewardship) |
| Fallback behaviour | no dynamic state → assume static state |
| Audit relevance | `BERTH_AVAILABILITY_CHANGED` (V1 candidate per workflow §14.3) |

### D3 — Movement plans

| Aspect | Detail |
|---|---|
| Authoritative source | **QShips / port management system** (when integrated); operator-entered (interim) |
| Secondary source | AISStream-inferred ETA/ETD (live data may correct planned movements) |
| Derived fields | predicted handover times, pilot/towage assignment windows, berth occupancy schedule |
| Refresh cadence | QShips feed: typically per-poll; AIS-inferred: continuous |
| Confidence | QShips-confirmed = high; AIS-inferred = medium; operator-entered = high (within their authority) |
| Owner | Harbour Master (authority over schedule changes); VTS (operational changes within authority) |
| Fallback behaviour | no port-system feed → operator-entered + AIS inference; surfaced as "schedule unconfirmed" |
| Audit relevance | `SCHEDULE_RECEIVED` (Phase 0 closed-set reserved; V1 emission per integration patterns) |

### D4 — Conflicts

| Aspect | Detail |
|---|---|
| Authoritative source | **Horizon conflict engine** — computed from D1, D2, D3, D10 at each `/api/summary` poll |
| Secondary source | none — conflicts are derived, not received |
| Derived fields | severity, conflict_time, sequencing_alternatives |
| Refresh cadence | per `/api/summary` poll (~30s) |
| Confidence | high when input data is high-confidence; degrades with degraded inputs |
| Owner | Horizon system (computational); VTSO + HM operationally (resolution authority) |
| Fallback behaviour | if inputs are missing, conflict cannot be computed → no conflict detected → no false-positive guidance |
| Audit relevance | `CONFLICT_DETECTED` (Phase 0.7a live) |

### D5 — Recommendations

| Aspect | Detail |
|---|---|
| Authoritative source | **Horizon recommendation engine** — generated from a conflict + decision_support computation |
| Secondary source | none — recommendations are derived |
| Derived fields | recommended_option_id, sequencing_alternatives with cost/delay/risk, decision_deadline, confidence |
| Refresh cadence | per `/api/summary` poll; dedup by `(conflict_id, recommended_option_id)` (Phase 0.7b) |
| Confidence | structured field in the decision_support payload |
| Owner | Horizon system (generation); VTSO + HM operationally (action authority) |
| Fallback behaviour | if conflict has insufficient data to generate alternatives, no recommendation; conflict is surfaced anyway |
| Audit relevance | `RECOMMENDATION_GENERATED` (Phase 0.7b live, with full §1.4.1 snapshot); `RECOMMENDATION_PRESENTED` (Phase 0.7c live) |

### D6 — Actions

| Aspect | Detail |
|---|---|
| Authoritative source | **Operator UI** — every action is a deliberate human gesture |
| Secondary source | none — actions are not derived; they are asserted |
| Derived fields | timing relative to recommendation deadline, reason-code adherence |
| Refresh cadence | event-driven; no polling |
| Confidence | maximum (asserted by an authenticated user with audit lineage) |
| Owner | the acting user (audit captures user, role, scope, session) |
| Fallback behaviour | n/a — actions are atomic; no fallback semantics |
| Audit relevance | `OPERATOR_ACTED` (Phase 0.8a live, scope: whatif_apply / whatif_clear / send_brief); V1.1+ expands to conflict_resolve, confirm_assignment, report_delay |

### D7 — Escalations

| Aspect | Detail |
|---|---|
| Authoritative source | **Operator UI** — escalations are asserted by source actor |
| Derived fields | closure time, total chain duration, target-authority response time |
| Refresh cadence | event-driven |
| Confidence | maximum (asserted) |
| Owner | source actor (creator); target authority (handler); audit captures both |
| Fallback behaviour | escalation cannot be silently lost; if target authority is offline, system surfaces alternate target or alerts next-up authority (workflow §8.7) |
| Audit relevance | `OPERATOR_ESCALATED` (or `ESCALATION_CREATED` per workflow §17.14 naming decision); `ESCALATION_RESOLVED` (V1.3 candidates) |

### D8 — Shifts

| Aspect | Detail |
|---|---|
| Authoritative source | **Shift Supervisor action** (open/close); per-tenant shift schedule in admin configuration |
| Derived fields | shift duration, active VTSO list per shift, pending-item count at close |
| Refresh cadence | shift-boundary events |
| Confidence | maximum (asserted) |
| Owner | Shift Supervisor (operationally); Harbour Master (delegation authority) |
| Fallback behaviour | wall-clock window start without SS-asserted open → first VTSO login triggers open (workflow §4.2) |
| Audit relevance | `SHIFT_OPENED` / `SHIFT_CLOSED` (V1.0 candidates per workflow §14.3) |

### D9 — Handovers

| Aspect | Detail |
|---|---|
| Authoritative source | **Outgoing Shift Supervisor** writes; **Incoming Shift Supervisor** accepts |
| Derived fields | pending-item inventory (auto-populated from operational state at handover-create), escalation queue snapshot, time-to-accept |
| Refresh cadence | shift-boundary events |
| Confidence | maximum (asserted by named SSs) |
| Owner | outgoing SS (creation); incoming SS (acceptance) |
| Fallback behaviour | if no acceptance within timeout, HM is alerted (workflow §9.9) |
| Audit relevance | `HANDOVER_CREATED` / `HANDOVER_ACCEPTED` (V1.3 candidates) |

### D10 — Weather and tide

| Aspect | Detail |
|---|---|
| Authoritative source | **BOM** (Australian Bureau of Meteorology) — live tide forecasts + observed weather |
| Secondary source | cosine-tide approximation (fallback when BOM unavailable) |
| Derived fields | tidal window calculations, weather hold predictions, wind exposure by berth |
| Refresh cadence | per `/api/summary` poll; BOM data cached background-refresh |
| Confidence | BOM-live = high; cosine-fallback = degraded (clearly labelled) |
| Owner | external (BOM); Horizon stewards the cache and fallback |
| Fallback behaviour | BOM unreachable → cosine fallback; surfaced to operator |
| Audit relevance | `TIDAL_FORECAST_OBSERVED` / `WEATHER_FORECAST_OBSERVED` (Phase 0 reserved; V1+ emission decision) |

### D11 — Pilotage

| Aspect | Detail |
|---|---|
| Authoritative source | **Kyber** (when integrated, V1.5+) — pilot rostering, availability, sequencing |
| Interim source | port-system feed or operator-entered assignment (V1.0–V1.4) |
| Derived fields | assignment status (notified / confirmed / in-progress / complete), pilot-to-vessel mapping |
| Refresh cadence | Kyber: API/event-driven; operator: event-driven |
| Confidence | Kyber-authoritative = high (for the operational context Horizon needs); operator-entered = high within their authority |
| Owner | Pilotage provider (Kyber stewardship); Horizon stewards the operational-context view |
| Fallback behaviour | no Kyber feed → operator-entered (V1.0 default) |
| Audit relevance | assignment-confirmation actions emit `OPERATOR_ACTED` (stakeholder confirm flow) |

### D12 — Towage

| Aspect | Detail |
|---|---|
| Authoritative source | port-system feed or operator-entered (no dedicated equivalent to Kyber for towage in V1) |
| Derived fields | tug-to-vessel mapping, bollard-pull adequacy check, tug-availability window |
| Refresh cadence | per assignment event |
| Confidence | high within asserted authority |
| Owner | Towage provider (organisational); Horizon stewards the operational view |
| Fallback behaviour | no feed → operator-entered |
| Audit relevance | confirmation actions emit `OPERATOR_ACTED` |

### D13 — Stakeholder assignments

| Aspect | Detail |
|---|---|
| Authoritative source | **VTSO or HM action** creates assignments; stakeholders confirm |
| Derived fields | confirmation latency, delay reports, completion status |
| Refresh cadence | event-driven |
| Confidence | maximum (asserted) |
| Owner | VTS (operational creation); stakeholder (confirmation + delay reporting) |
| Fallback behaviour | n/a — assignments are atomic |
| Audit relevance | `OPERATOR_ACTED` (confirm_assignment, report_delay); `STAKEHOLDER_REPORTED_ISSUE` (V1.5 candidate) |

### D14 — Incidents

| Aspect | Detail |
|---|---|
| Authoritative source | **HM or Executive action** opens an incident; reference into D15 audit ledger |
| Derived fields | incident duration, actor involvement, recommendation/escalation chain |
| Refresh cadence | event-driven (incidents open / replay / close) |
| Confidence | high (HM/Executive asserted) |
| Owner | HM (creator); Executive (review) |
| Fallback behaviour | n/a |
| Audit relevance | `INCIDENT_OPENED` / `INCIDENT_REPLAYED` (V1.6 candidates) |

### D15 — Audit events

| Aspect | Detail |
|---|---|
| Authoritative source | **Horizon audit helpers** (Phase 0.7a/b/c, 0.8a live; V1 expansion per workflow §14) |
| Derived fields | none — audit events are atomic |
| Refresh cadence | event-driven; immediate emission |
| Confidence | maximum (hash-chained, per-tenant genesis, `verify_chain` proves integrity) |
| Owner | Horizon system (writes); HM + Executive (read authority); external regulator (read, V1.x) |
| Fallback behaviour | append-only by design; no row mutation, no row deletion; partition-drop only at retention boundary |
| Audit relevance | this domain *is* the audit relevance for every other domain |

### D16 — Executive metrics

| Aspect | Detail |
|---|---|
| Authoritative source | **Horizon analytics layer** — derived from D15 + D6 + D7 over time windows |
| Derived fields | trend computations, rolling averages, exception detections |
| Refresh cadence | computed on-demand (V1.4); cached for periodic dashboards |
| Confidence | as-good-as the input D15/D6/D7 confidence; aggregation does not increase certainty |
| Owner | Horizon system (computation); Executive (consumption) |
| Fallback behaviour | gaps in input data surface as gaps in trend; not interpolated silently |
| Audit relevance | `AUDIT_READ` per executive metric retrieval (V1.4 candidate) |

### D17 — System / admin configuration

| Aspect | Detail |
|---|---|
| Authoritative source | **Tenant administrator** action (V1.x); config schema seeded by Alembic migrations (V1.0) |
| Derived fields | effective permission set per user (role bundle + explicit grants) |
| Refresh cadence | configuration change events |
| Confidence | maximum (asserted by tenant admin) |
| Owner | Tenant administrator |
| Fallback behaviour | n/a — configuration always exists (default seeded) |
| Audit relevance | every config change is auditable; specific event types TBD in V1.x |

---

## 5. Operational Data Flow

The path information takes from external sources to operator
screens, audit ledger, and analytics.

### 5.1 Stage 1 — External source ingestion

External sources push or are pulled into Horizon:

- **AISStream** — WebSocket connection, continuous vessel state
  updates; cached in-memory + persisted to `_mst_cache`-equivalent
  per `/api/summary` cycle
- **MST API** — HTTP pull on-demand within `/api/summary`
- **QShips** — file/JSON feed pulled per cycle (when configured)
- **BOM tides** — HTTP pull; background cache
- **BOM weather** — HTTP pull; background cache
- **Kyber (V1.5+)** — API/event push; consumed via event listener
- **Port system feeds (V1.x)** — pull or push per integration
  pattern

Each source has its own:
- Authentication mechanism
- Refresh cadence
- Confidence level
- Fallback path

### 5.2 Stage 2 — Normalisation layer

External-source data is normalised into Horizon's canonical
operational state model:

- Vessel records normalised to `{id, name, loa, beam, draft, ata,
  atd, eta, etd, status, source, data_source, ...}` regardless of
  source
- Tide records normalised to `{current_height_m, next_high,
  next_low, source}`
- Weather records normalised to standardised fields
- Source attribution preserved (every record carries its
  authoritative source path)

The normalisation layer is the **single point** where source-specific
quirks are absorbed. Downstream consumers (conflict engine,
recommendation engine, role-scoped API projection) only ever see
normalised data.

### 5.3 Stage 3 — Operational state model

Normalised data is composed into a per-port operational state:

- Vessels list (active in or approaching the port)
- Berths state (occupied / available / maintenance)
- Pilotage assignments (current and forecast)
- Towage assignments
- Weather and tide state
- Active conflicts (computed from above)
- Active recommendations (computed from conflicts)
- Active escalations (asserted by operators)
- Active shift + supervisor identity

This is the substrate `/api/summary` returns to authenticated
operators (filtered per §9).

### 5.4 Stage 4 — Conflict engine

The conflict engine (`detect_conflicts()` in Beta 10) consumes the
operational state and produces conflict records. Each conflict
carries:

- `conflict_id` (stable derivative of input identities)
- `conflict_type` (berth_overlap, pilot_conflict, weather_risk, etc.)
- `signal_type` (CONFLICT, WARNING, ADVISORY, WEATHER)
- `severity`
- Affected vessels, berths, pilotage, towage
- Conflict time
- `sequencing_alternatives` (operator options)
- `data_source` (live / simulated)

### 5.5 Stage 5 — Recommendation engine

The recommendation engine (`_build_decision_support()` in Beta 10)
augments conflicts with structured decision support:

- `recommended_option_id`
- `recommended_reasoning`
- `confidence`
- `decision_deadline`
- Alternatives with cost / delay / risk / feasibility

The decision-time snapshot (§1.4.1, Phase 0.7b) captures the inputs
the engine used at the moment of generation.

### 5.6 Stage 6 — Role-scoped API projection

Per permission model §8 and screen architecture §4.2, the `/api/summary`
response is filtered server-side per the requesting session's role
and scope. The same underlying operational state produces:

- VTSO view: full operational density
- HM view: same as VTSO + audit-trail access
- SS view: full operational + handover/escalation
- Executive view: aggregated KPIs only
- Stakeholder view: own-assignment-scoped
- Marine Infrastructure view: berths + minimal context

### 5.7 Stage 7 — Frontend screen surfaces

The role-scoped API projection populates the screens defined in the
Screen Architecture (PR #32):

- VTSO operational console (§5 of screen architecture)
- SS console (§6)
- HM console (§7)
- Executive dashboard (§8)
- Stakeholder mobile feed (§9)

### 5.8 Stage 8 — Audit event capture

Operator actions (Stage 7 → Stage 8) emit audit events into the
hash-chained ledger (D15). Per workflow model §14:

- Live events: `SESSION_*`, `CONFLICT_DETECTED`,
  `RECOMMENDATION_GENERATED`, `RECOMMENDATION_PRESENTED`,
  `OPERATOR_ACTED`
- V1 events: `OPERATOR_ACKNOWLEDGED`, `OPERATOR_DEFERRED`,
  `OPERATOR_OVERRODE`, `OPERATOR_ESCALATED`, `HANDOVER_*`, etc.

### 5.9 Stage 9 — Replay / history layer

The audit ledger (D15) plus pre-computed snapshots (the §1.4.1
decision-time snapshots embedded in `RECOMMENDATION_GENERATED`) form
the replay substrate. The Incident Replay surface (screen
architecture §11) reconstructs operational state from this layer.

### 5.10 Stage 10 — Executive analytics layer

Aggregated derived metrics (D16) compute from D15 + D6 + D7 over
time windows. Executive Dashboard (screen architecture §8) presents
these. The aggregation is read-only against the audit ledger; it
does not produce new audit events except `AUDIT_READ`.

### 5.11 End-to-end flow summary

```
External                  Normalised               Operational State        Role Projection
sources           ──→     records          ──→     model               ──→  per role
(AISStream, BOM,          (canonical                (vessels, berths,        (VTSO sees full,
QShips, Kyber)            shape, source              conflicts,              Executive sees
                          attributed)                recommendations,        aggregated, etc.)
                                                    shift, escalations)
                                                          │
                                                          ↓
                                                    Screens (per
                                                    screen arch)
                                                          │
                                                          ↓ (operator actions)
                                                    Audit events
                                                    (D15, hash-chained)
                                                          │
                                                          ↓
                                                    Replay layer ──→ Incident review
                                                    Analytics ──→ Executive metrics
```

---

## 6. Real-Time vs Historical Data

V1 explicitly separates **mutable real-time operational state** from
**append-only historical audit record**. They share substance but
not persistence rules.

### 6.1 Real-time operational state

- **Mutability:** mutable; refreshes on every `/api/summary` poll
- **Persistence:** transient (in-memory, possibly cached); not the
  source of truth for past state
- **Examples:** current vessel positions, current weather, currently
  active conflicts, currently presented recommendations, current
  pending-action queue per VTSO
- **Safe to mutate:** yes — the operational state is supposed to
  reflect "now"
- **Lifetime:** seconds to minutes
- **Lost on restart:** acceptable; the system rebuilds from external
  sources

### 6.2 Near-real-time derived insight

- **Mutability:** mutable but slower-changing
- **Persistence:** computed on demand; may be cached briefly
- **Examples:** conflict severity scoring, recommendation
  confidence, decision deadline calculations
- **Safe to mutate:** yes (these are derived from real-time inputs)
- **Lifetime:** minutes to hours
- **Lost on restart:** acceptable (recomputed)

### 6.3 Historical audit record (D15)

- **Mutability:** **immutable; append-only; no UPDATE, no DELETE,
  no TRUNCATE** (Phase 0 baseline)
- **Persistence:** hash-chained per-tenant; partitioned by month;
  long-term retention (per `retention_class`)
- **Examples:** every audit event ever emitted; the `RECOMMENDATION_GENERATED`
  decision-time snapshot embedded in payload
- **Safe to mutate:** **NO** — mutation breaks the chain
- **Lifetime:** indefinite (subject to per-tenant retention policy)
- **Lost on restart:** never; this is the durable substrate

### 6.4 Replayable operational state

- **Mutability:** not mutable; reconstructed from D15
- **Persistence:** derived at replay time from the audit ledger
- **Examples:** "what did the VTSO see at 14:32 yesterday" —
  reconstructed from `RECOMMENDATION_GENERATED` snapshots + audit
  events in the window
- **Safe to mutate:** no — replay is read-only by definition
- **Lifetime:** as long as the underlying audit data exists

### 6.5 Analytical aggregate (D16)

- **Mutability:** computed; can be re-computed if inputs change
  (but inputs are append-only, so aggregates are stable per time
  window)
- **Persistence:** cached aggregates per time window (V1.4+)
- **Examples:** weekly throughput, monthly override rate, rolling
  incident counts
- **Safe to mutate:** the aggregate cache can be invalidated and
  recomputed; the underlying audit data cannot
- **Lifetime:** as cached; recomputed on-demand

### 6.6 Archived data

- **Mutability:** read-only; no further additions to the archive
- **Persistence:** long-term per retention class (e.g. partition
  drop after 5 years)
- **Examples:** audit events older than the tenant's hot-retention
  window
- **Safe to mutate:** no — archive must be reconstructible to its
  state at archive time
- **Lifetime:** per tenant retention policy

### 6.7 Mutation rules summary

| Layer | UPDATE allowed? | DELETE allowed? | TRUNCATE allowed? |
|---|---|---|---|
| Real-time operational state | yes | yes | yes (between polls) |
| Derived insight | yes | yes | yes |
| **Audit record (D15)** | **NO** | **NO** | **NO** |
| Replayable state | n/a (derived) | n/a | n/a |
| Analytical aggregate | yes (cache invalidate + recompute) | yes (cache only) | yes (cache only) |
| Archived | no | no (until partition drop) | no |

Phase 0's `verify_chain` is the integrity guarantee that the audit
record has not been mutated. Every chain check that returns clean
is evidence that the immutability invariant holds.

---

## 7. Shared Operational Picture Composition

The "shared operational picture" is the composed view of port state
that all operational roles consume — each as a different projection.
This section defines what composes it and how the same underlying
state produces different role views.

### 7.1 Composition inputs

The picture is composed from:

- **Vessels** (D1): identity, position, status, kinematics, ETA/ETD
- **Berths** (D2): static (capabilities) + dynamic (state)
- **Movements** (D3): scheduled, in-progress, recently-completed
  vessel movements
- **Constraints** (subset of D2 + D17): channel restrictions, depth
  limits, max-LOA per berth, weather thresholds
- **Resources** (D11, D12): pilotage and towage availability
- **Weather and tides** (D10): current and forecast
- **Conflict state** (D4): currently-detected operational conflicts
- **Recommendations** (D5): currently-active recommendations
- **Ownership** (cross-cutting): who currently owns each conflict /
  recommendation / escalation per workflow §6
- **Action status** (D6 historical + D7 active): what has been done
  in the current shift window
- **Escalation state** (D7): active escalations and their chains

### 7.2 Role projections of the same picture

The same composed picture is projected differently per role per
permission model §5 and screen architecture §4.2:

- **VTSO** sees full operational density: all vessels, all
  conflicts, all recommendations, weather/tides, pilotage,
  towage, ownership, action status, escalation state
- **Shift Supervisor** sees the same with handover overlay
  emphasised (pending items per VTSO, escalation queue)
- **Harbour Master** sees all of the above + audit-trail access
- **Port Executive** sees aggregated picture only (KPIs,
  trends, exceptions)
- **Stakeholder** sees a narrow slice of the picture relevant to
  their assignment (their vessel, their berth, weather/tide for
  their window)
- **Marine Infrastructure** sees the berth slice of the picture +
  minimal vessel context

### 7.3 Why one picture, many projections

The composition is single-sourced to ensure consistency:

- A conflict the VTSO sees is the same conflict the HM sees
  (different decoration, same underlying record)
- Ownership transfer is visible to everyone who can see the conflict
  (no one sees stale ownership)
- Action status visible to a role is filtered to that role's
  permission (but the underlying data is canonical)

This is the technical substrate of the screen architecture's
principle 3 ("ownership visibility always visible") and the
permission model's principle 1 ("see what you need, nothing more").

### 7.4 Synchronisation across operators

The shared picture is refreshed per `/api/summary` poll. Multiple
operators on the same port watch see the same picture (per-role
filtered) with the same refresh cadence. There is no concept of
"VTSO A's view" diverging from "VTSO B's view" — they share state.

V1.x may explore push-based updates (SSE / WebSocket per notification
model §12.10) for sub-second synchronisation, but V1.0 uses
poll-based composition.

---

## 8. Data Ownership and Stewardship

Ownership of a data domain is **conceptual** — the organisation or
role with semantic accountability for that data. Stewardship is
**operational** — who is responsible for the data's accuracy day to
day. Visibility (who can see it) and action authority (who can
change it) are distinct from both.

### 8.1 Owner / steward / visibility / action triangulation

| Domain | Conceptual owner | Operational steward | Default visibility | Action authority |
|---|---|---|---|---|
| D1 Vessels | Port Authority | VTS | all operational roles + Executive (aggregated) | system (state); operator (corrections) |
| D2 Berths static | Port Authority | Port engineering | all operational roles | tenant admin |
| D2 Berths dynamic | Port Authority | Marine Infrastructure | all operational roles | MI permission holders |
| D3 Movements | Port Authority | VTS (operational); HM (authority over schedule changes) | operational roles | VTSO (operational); HM (authority) |
| D4 Conflicts | Horizon system | n/a (derived) | operational roles (full); Executive (aggregated) | n/a (read-only domain) |
| D5 Recommendations | Horizon system | n/a (derived) | operational roles (full); Executive (aggregated) | VTSO + HM (action) |
| D6 Actions | acting user | the user themselves | own actions visible to self; SS + HM visible per scope | the acting user |
| D7 Escalations | source actor → target authority | escalation chain | involved actors + HM | source actor (create); target (resolve) |
| D8 Shifts | Port Authority | Shift Supervisor | operational roles | SS (open/close); HM (authority over shift def) |
| D9 Handovers | outgoing SS → incoming SS | the SSs themselves | involved SSs + HM | outgoing SS (create); incoming SS (accept) |
| D10 Weather/tides | external (BOM) | Horizon cache | all operational roles | n/a (external) |
| D11 Pilotage | Pilotage provider (Kyber stewards roster) | Pilotage provider | operational roles (operational context only) | VTSO + HM (operational); Kyber (rostering) |
| D12 Towage | Towage provider | Towage provider | operational roles | VTSO + HM (operational) |
| D13 Stakeholder assignments | VTS (creator) | stakeholder (confirmation) | involved stakeholder + VTSO | VTSO (create); stakeholder (confirm) |
| D14 Incidents | HM (or Executive) | HM | HM, Executive (aggregated) | HM (open); Executive (review) |
| D15 Audit events | Horizon system | n/a (immutable) | HM (full); SS (shift-scoped); Executive (aggregated); external regulator (V1.x) | system only |
| D16 Executive metrics | Horizon system (derived) | Horizon analytics layer | Executive | n/a (read-only) |
| D17 Admin config | Tenant administrator | Tenant administrator | tenant admin role | tenant admin only |

### 8.2 Distinctions made explicit

- **Ownership ≠ visibility.** The Port Authority *owns* vessel data
  but Executives only see vessel data in aggregate.
- **Ownership ≠ action authority.** The Port Authority owns berths
  but Marine Infrastructure permission holders take berth-state
  actions.
- **Visibility ≠ action authority.** A VTSO can see audit events
  for their own actions but cannot change them.
- **Stewardship is local.** Day-to-day stewardship of stakeholder
  assignments is the stakeholder themselves (confirming, reporting
  delays); they don't own the assignment data semantically, but
  they're the operational steward of their part of it.

### 8.3 Cross-tenant boundary

All ownership / stewardship is **per-tenant**. There is no
cross-tenant data sharing in V1 except where explicitly contracted
(e.g. shared port authority across multiple tenant organisations —
not yet a V1 use case).

---

## 9. Role-Scoped Data Projection

Per permission model §5 and screen architecture §4.2: every role
sees a different projection of the same underlying data. This
section maps each of the 17 domains to each of the 6 roles.

### 9.1 Projection legend

- **Full** — full data available, all fields, all instances within
  scope
- **Filtered** — full data available for items within the role's
  active scope (e.g. only own assignments)
- **Aggregated** — only aggregated / summary form (e.g. counts,
  trends)
- **De-identified** — full data structurally but with identifying
  fields replaced by capability descriptors (e.g. pilot identity)
- **None** — domain not visible

### 9.2 Domain × Role matrix

| Domain | VTSO | Shift Supervisor | Harbour Master | Port Executive | Stakeholder | Marine Infrastructure |
|---|---|---|---|---|---|---|
| D1 Vessels | Full | Full | Full | Aggregated | Filtered (own assignments) | Filtered (berthed only) |
| D2 Berths | Full | Full | Full | Aggregated | Filtered (own assignment) | Full |
| D3 Movements | Full | Full | Full | Aggregated | Filtered (own assignment) | Filtered (own berth) |
| D4 Conflicts | Full | Full | Full | Aggregated | None | None |
| D5 Recommendations | Full | Full | Full | Aggregated | None | None |
| D6 Actions | Own + shift-scope read | Shift-scope full | Full | Aggregated | Own only | Own only |
| D7 Escalations | Own escalations | Shift-scope full | Full | Aggregated | None | None |
| D8 Shifts | Own shift | Own shift + handover history (own) | Full | Aggregated | None | None |
| D9 Handovers | None | Own handovers + handover history (own) | Full | Aggregated (handover frequency, length) | None | None |
| D10 Weather/tides | Full | Full | Full | Aggregated | Full (read-only) | Full (read-only) |
| D11 Pilotage | Full | Full | Full | None | Filtered + De-identified | None |
| D12 Towage | Full | Full | Full | None | Filtered + De-identified | None |
| D13 Stakeholder assignments | Full | Full | Full | None | Own assignments only | None |
| D14 Incidents | None | Aggregated (shift-scope context) | Full | Aggregated | None | None |
| D15 Audit events | Own actions only | Shift-scope read | Full | Aggregated (counts, trends) | None | None |
| D16 Executive metrics | None | Operational metrics only (handover-relevant) | Full | Full | None | None |
| D17 Admin config | None | None | Read access if tenant-admin permission granted | None | None | None |

### 9.3 Per-role projection notes

#### VTSO

- Full operational density across operational domains (D1–D5,
  D10–D13)
- Own actions and shift-scope read of others' actions (D6)
- Own escalations (D7); cannot see escalations they're not party to
- Own shift only (D8); no historical shift access
- No handover access (D9 — that's SS scope)
- No audit access beyond own action history (D15)
- No incidents, executive metrics, or admin config

#### Shift Supervisor

- Same as VTSO for operational domains, plus:
- Shift-scope full read of D6 actions (per principle 4 of workflow:
  "review VTSO actions during the shift")
- Shift-scope full read of D7 escalations
- Handover history within own scope (D9)
- Shift-scope read of D15 audit events
- Aggregated incident context for current shift (D14)
- Operational metrics relevant to handover (D16: pending-queue
  depths, response times)

#### Harbour Master

- Full across every operational and audit domain
- Tenant-admin permission optionally grants D17 read

#### Port Executive

- Aggregated views only across most domains
- Full executive metrics (D16) — this is their primary domain
- Full pilotage and towage availability *aggregated* (counts,
  utilisation) but never named-individual detail

#### Stakeholder

- Filtered to own assignment scope across D1, D2, D3, D11, D12, D13
- Pilotage and towage data are de-identified (capability
  descriptors, not named identities)
- No access to conflicts, guidance, audit, incidents, or analytics
- Mobile-first surface (per screen architecture §9)

#### Marine Infrastructure

- Full access to berth data (D2) — their primary domain
- Filtered berthed-vessels view (D1) for berth context
- Filtered own-berth movement view (D3)
- Weather/tides for environmental context (D10)
- No access to conflicts, recommendations, escalations, audit, or
  metrics

### 9.4 Projection enforcement

All projection filtering happens **server-side** at the API layer
per permission model §8.7. Frontend filtering is never sufficient.
A role that does not have visibility of a domain does not receive
the data in the response payload at all.

---

## 10. Sensitive Information Boundaries

Some information must be carefully bounded across the Horizon
surface. This section defines those boundaries.

### 10.1 Pilot identity / resource identity

- **Default:** capability descriptor only (e.g. "Pilot
  qualified for this port + LOA class" rather than "Jane Smith")
- **Why:** commercial sensitivity, privacy, scope of Horizon's
  audit responsibility
- **Who sees full identity:** VTSO, SS, HM (operational necessity);
  Kyber (the system of record); Executive sees aggregated only
- **Who sees de-identified:** Stakeholders (when not themselves);
  Marine Infrastructure (not needed); audit ledger payloads
  (operator_handle is the de-identified form, e.g. "O-1")

### 10.2 Operational notes

- **Default:** notes attached to actions / handovers are scoped to
  the action's audit subject
- **Free-text constraint:** while V1 supports free-text notes for
  nuance, structured fields (reason codes) are preferred
- **Who sees:** the acting user and roles with audit access in the
  same scope
- **Who does NOT see:** Executives (aggregated only); stakeholders
  (not in their assignment scope); external regulator (V1.x;
  redaction may apply)

### 10.3 Commercial / financial impact

- **Default:** commercial impact (cost estimates, throughput value,
  delay cost) is computed in Horizon for VTSO / HM decision support
- **Aggregated for Executive:** roll-up only; not per-decision
- **Stakeholder visibility:** none of Horizon's commercial
  computations (stakeholders see their own commercial concerns
  outside Horizon's scope)
- **External regulator (V1.x):** redacted; or replaced with
  operational-impact descriptors

### 10.4 Security-sensitive incidents

- **Default:** access restricted by incident-type tag
- **Examples:** vessel boarding events, security-anomaly detection,
  ISPS-compliance events
- **Who sees:** HM, Executive, and explicitly-authorised personnel
- **NOT visible to:** general VTSO, SS, Stakeholder roles
- **Audit posture:** still emitted to the ledger, but with a
  sensitivity tag; replay filters by clearance level

### 10.5 Internal executive analysis

- **Default:** executive analytical insights stay within Executive
  scope
- **Examples:** override-rate analysis per HM, KPI projections,
  staffing pattern observations
- **NOT visible to:** the analysed subjects without explicit
  consent (per HR-style boundary)
- **Aggregation:** reported in roll-ups; not surfaced as individual
  scoring back to the operational floor

### 10.6 Detailed audit chain

- **Default:** HM has full audit access; SS has shift-scope; lower
  roles have own-action access only
- **Detail levels:**
  - Full chain (every event, full payload) → HM, audit administrator
  - Time-windowed (shift scope) → SS
  - Own actions only → VTSO, Stakeholder
  - Aggregated counts → Executive
- **Redaction:** future regulator role (V1.x) may see redacted
  payloads (e.g. de-identified actor_handle); structural events
  remain visible for replay

### 10.7 Stakeholder-specific operational commitments

- **Default:** stakeholder commercial commitments (pilot contracts,
  towage service-level agreements) are NOT in Horizon's scope
- **Horizon's responsibility:** the assignment confirmation /
  delay / issue surface
- **Outside Horizon:** the commercial relationship management;
  contract enforcement; performance penalties
- **Audit posture:** Horizon records the operational events
  (assignment, confirmation, delay); commercial consequences are
  computed elsewhere

### 10.8 Maintenance constraints

- **Default:** Marine Infrastructure's planned-maintenance schedules
  are operational data, visible to operational roles
- **Confidential elements:** maintenance vendor identity, cost
  details, root-cause analyses
- **Who sees the full detail:** Marine Infrastructure permission
  holders + HM
- **What others see:** "berth unavailable" without internal-vendor
  identification

### 10.9 Personal data

- **Default:** Horizon V1 minimises personal data:
  - User accounts: operator_handle (e.g. "O-1") + username
  - Audit payloads: handle only (not name)
  - Pilot identity: capability descriptor
- **Where personal data exists:** auth credentials (hashed); user
  display name in config (visible only within tenant admin scope)
- **GDPR-equivalent considerations:** documented separately when V1
  customer engagements arrive; not yet a V1.0 requirement

### 10.10 De-identified / capability-based resource model

For pilots and towage personnel:

- **Capability descriptor format:** `{capability_class}` (e.g.
  "Pilot — port:BRISBANE — class:LOA<250m" or "Tug operator —
  bollard_pull:75t")
- **Operational sufficiency:** capability descriptors carry enough
  information for VTSOs to plan; full identity is unnecessary for
  most decisions
- **When full identity is needed:** Kyber-side rostering; HM-level
  authority decisions involving named individuals; audit-trail
  replay where regulatory inquiry requires
- **Default in Horizon API responses:** capability descriptor;
  named identity only on explicit drill-in by authorised role

---

## 11. Integration Boundary Model

This section defines the information boundary at each integration
point — what flows in, what flows out, what is owned where.

### 11.1 AIS providers (AISStream, MyShipTracking)

**Inbound:**
- Vessel position (lat, lon, course, speed)
- Vessel identity (MMSI, callsign, name)
- Vessel static data (LOA, beam, draft, ship type)
- AIS message timestamps

**Outbound:** none. Horizon consumes; AIS providers don't read
Horizon state.

**Owner:** external providers; Horizon caches and normalises.

**Confidence handling:** stale-detection on AISStream WebSocket;
fallback to MST cache.

### 11.2 Weather / tide sources (BOM)

**Inbound:**
- Tide forecasts (heights, times) per BOM station
- Weather observations (wind, sea state, visibility)
- Weather warnings (severe weather, gale, storm)

**Outbound:** none.

**Owner:** Australian Bureau of Meteorology; Horizon caches with
explicit source-attribution.

**Confidence handling:** explicit cosine fallback for tides when
BOM is unavailable, clearly labelled.

### 11.3 Port systems (QShips, port management feeds)

**Inbound:**
- Scheduled vessel movements
- Berth assignments
- Port-issued navigation directives

**Outbound (V1.x):**
- Movement status updates
- Conflict notifications (when port systems can consume them)
- Operational state acknowledgements

**Owner:** Port Authority's port management system; Horizon
consumes the operational view.

**Confidence handling:** depends on the source system's accuracy;
Horizon reflects the source's confidence.

### 11.4 Terminal systems

**Inbound (V1.x):**
- Cargo readiness times
- Crane availability / scheduling
- Terminal access controls

**Outbound (V1.x):**
- Vessel ETA refinements
- Berth-window forecasts

**Owner:** Terminal operator; Horizon consumes the operational
intersection.

**Confidence handling:** terminal-system-specific.

### 11.5 Towage systems

**Inbound:**
- Tug roster
- Tug availability
- Towage assignments (when the towage provider has a system)

**Outbound (V1.x):**
- Assignment confirmation requests
- Vessel movement context for the tug

**Owner:** Towage provider; Horizon consumes the operational
context.

### 11.6 Kyber

**Inbound (V1.5+):**
- Pilot assignment events (assignment created, pilot accepted,
  pilot en route, pilot on board, pilot off)
- Pilot capability descriptors (de-identified)
- Pilot-side delay reports (when relevant to Horizon's
  coordination view)

**Outbound (V1.5+):**
- Operational context events (conflict detected affecting an
  assignment, weather hold, schedule change)
- De-identified actor handles only

**Owner:** Kyber (pilot roster, detailed pilot workflow); Horizon
(the broader port operational context Kyber consumes).

**Integration model:** API/event-based; federated authentication;
de-identified resource model. **Horizon V1 does not depend on
Kyber code** — boundary preserved per permission model §11 and
workflow model §13.6.

### 11.7 Future regulator / external reviewer integrations

**Inbound:** none (regulators don't write to Horizon).

**Outbound (V1.x, deferred):**
- Read-only audit ledger access, scoped to specific incidents or
  time windows
- Replay package exports (per incident, with hash-chain integrity
  proofs)
- Redacted payloads where required

**Owner:** Horizon (the audit ledger is Horizon's responsibility);
regulator consumes per authorised access pattern.

### 11.8 Future AI / ML services

**Inbound (V1.x+, deferred):**
- Normalised operational state (D1–D5, D10)
- Historical recommendations (D5)
- Action outcomes (D6, audit-linked)
- Aggregated metrics (D16)
- Reason codes (D6 / D7 closed catalogues)

**Outbound (V1.x+):**
- AI-generated recommendations (must be tagged as AI-origin)
- AI-generated trend insights for Executive consumption
- AI-flagged operational anomalies (advisory, not authoritative)

**Owner:** Horizon owns the operational substrate; AI consumes; AI
output is always tagged with provenance and confidence.

**Constraint:** per principle 8, AI must not consume any data
without explicit source / scope / lineage attribution.

### 11.9 Inbound vs outbound flow summary

| Integration | Direction | V1 phase |
|---|---|---|
| AISStream | inbound only | live (Phase 0) |
| MST | inbound only | live (Phase 0) |
| BOM | inbound only | live (Phase 0) |
| QShips | inbound only (V1.0); inbound+outbound (V1.x) | live (Phase 0); V1.x for outbound |
| Terminal systems | both | V1.x |
| Towage systems | inbound primary | V1.x |
| Kyber | both | V1.5+ |
| External regulator | outbound only | V1.x (deferred) |
| AI / ML services | both | V1.x+ (deferred) |

---

## 12. Audit Information Model

This section defines what becomes audit data vs what remains
operational state.

### 12.1 What becomes an audit event

An action or state change becomes an audit event when ANY of:

- It represents an explicit operator decision (acknowledge,
  resolve, defer, override, escalate, sign off, close port)
- It represents an authority assertion (HM approval, port closure,
  override of recommendation)
- It represents a state change with downstream consequences
  (handover acceptance, shift open/close, berth availability
  change)
- It represents an evidentiary moment for replay (recommendation
  generated, recommendation presented, incident opened)
- It represents a read against the audit ledger itself
  (`AUDIT_READ` — V1.3+)

### 12.2 What becomes audit payload

Within each audit event's payload (per workflow §14):

- **Always present:** user_id, role, scope (port, tenant, shift_id),
  session_id, observed_at
- **Per event type:**
  - `RECOMMENDATION_GENERATED`: §1.4.1 decision-time snapshot
    (Phase 0.7b — engine version, relevant vessels, relevant berths,
    eta source hierarchy, tide inputs, weather inputs, ukc inputs,
    conflict state, constraints, alternatives generated,
    recommended option, decision deadline)
  - `OPERATOR_ACTED`: action_type, summary string, surface,
    optional conflict_id, optional recommendation_id, optional
    reason_code (Phase 0.8a-shaped + V1 extensions)
  - `OPERATOR_DEFERRED`: defer_until, reason_code, notes
  - `OPERATOR_OVERRODE`: override_choice, reason_code, notes,
    original_recommendation_id
  - `OPERATOR_ESCALATED` / `ESCALATION_CREATED`: source actor +
    role, target authority, reason_code, optional bypass flag,
    reference chain
  - `HANDOVER_CREATED`: outgoing SS handle, shift_id, handover
    note hash, pending item count, escalation queue depth
  - `HANDOVER_ACCEPTED`: incoming SS handle, outgoing shift_id,
    acceptance time
  - `SHIFT_OPENED` / `SHIFT_CLOSED`: shift_id, supervisor, scope,
    timestamp

### 12.3 What remains operational state only (NOT audit)

- Current vessel positions (transient real-time)
- Current weather observation (transient)
- Currently-active conflict snapshot (recomputed each poll)
- Currently-presented recommendations (recorded as
  `RECOMMENDATION_PRESENTED` events, but the active set is
  operational)
- Currently-open notifications
- Currently-active session count
- Anything else recomputable from external sources or the audit
  ledger itself

### 12.4 What is replay-relevant

For incident replay (§13) to fully reconstruct an operational
decision, the audit ledger must contain:

- The triggering `CONFLICT_DETECTED`
- The resulting `RECOMMENDATION_GENERATED` with full §1.4.1 snapshot
- All `RECOMMENDATION_PRESENTED` events (one per surface where
  presented)
- All operator actions against the recommendation
  (`OPERATOR_ACKNOWLEDGED`, `OPERATOR_ACTED`,
  `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, `OPERATOR_ESCALATED`)
- Any escalation chain events
- The closing event (`OPERATOR_ACTED` with resolve / override; or
  `DEADLINE_PASSED`; or explicit close)
- Surrounding shift and handover events for temporal context

### 12.5 What is analytics-only

Derived metrics (D16) are computed from audit data but are not
themselves audit-emitting (except `AUDIT_READ` for the executive
read):

- Throughput aggregates
- Override-rate trends
- Escalation-closure-time distributions
- Recommendation-adoption ratios
- Shift-workload patterns

### 12.6 What must be hash-chained

The Phase 0 invariant: **every audit event is hash-chained per
tenant**. Per-tenant chains use a tenant-specific genesis
(`AUDIT_GENESIS_v1:<tenant_id>` per Phase 0.5b).

What this implies for V1:
- New V1 event types (per workflow §14.3) all chain in the same
  per-tenant chain
- Cross-tenant events do NOT exist; if multi-tenant coordination
  arises in V1.x+, it manifests as parallel events in each
  tenant's chain with reference IDs linking them
- The chain integrity test (`verify_chain` per Phase 0.5b) must
  return clean across V1 expansions

### 12.7 What can be aggregated later

Aggregation happens at read time (D16 from D15), not at write time:

- Audit events are never aggregated into compressed form at write
- Read-time aggregations cache the result for performance but do
  not replace the underlying chain
- Cache invalidation strategy: per-tenant time-window keys; new
  events in the window invalidate the cache for that window

### 12.8 Current events vs V1 expansion

| Status | Events |
|---|---|
| Phase 0 live | `SESSION_STARTED`, `SESSION_ENDED`, `CONFLICT_DETECTED`, `RECOMMENDATION_GENERATED`, `RECOMMENDATION_PRESENTED`, `OPERATOR_ACTED` |
| Phase 0 reserved (V1 emits) | `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, `DEADLINE_PASSED`, `SESSION_ENDED_WITHOUT_ACTION` |
| V1 candidates (per workflow §14.3 + screen arch §15.1) | `OPERATOR_ACKNOWLEDGED`, `OPERATOR_ESCALATED` / `ESCALATION_CREATED`, `ESCALATION_RESOLVED`, `SHIFT_OPENED`, `SHIFT_CLOSED`, `HANDOVER_CREATED`, `HANDOVER_ACCEPTED`, `INCIDENT_OPENED`, `INCIDENT_REPLAYED`, `AUDIT_READ`, `BERTH_AVAILABILITY_CHANGED`, `STAKEHOLDER_NOTIFIED`, `STAKEHOLDER_REPORTED_ISSUE`, `RECOMMENDATION_CLOSED`, `OPERATOR_SIGNED_OFF`, `DELEGATION_GRANTED`, `DELEGATION_REVOKED`, `INCIDENT_FINDING_RECORDED` |

Each V1 candidate requires explicit closed-set extension in
`audit.py` via a Phase-0.5a-style migration. The regression gate
test (`test_no_emission_event_types_outside_authorised_set`) must
be deliberately updated alongside each addition.

---

## 13. Replay Information Requirements

This section specifies the data required to reconstruct each kind
of replay artefact.

### 13.1 Replay an operational conflict

**Required:**
- `CONFLICT_DETECTED` event with full conflict object in payload
- All vessel positions, berth state, weather, tide data at the
  detection time (these need to be either in the conflict payload
  or recoverable from surrounding events)
- The decision-time snapshot in any subsequent
  `RECOMMENDATION_GENERATED`

**Reconstruction completeness:** the conflict's existence,
characteristics, and contemporaneous inputs must be reconstructible
from the ledger alone (no external lookup required).

### 13.2 Replay a recommendation

**Required:**
- `RECOMMENDATION_GENERATED` with full §1.4.1 snapshot
  (engine_version, relevant_vessels, relevant_berths,
  eta_source_hierarchy, tide_inputs, weather_inputs, ukc_inputs,
  conflict_state, constraints, alternatives_generated,
  recommended_option, decision_deadline)
- All `RECOMMENDATION_PRESENTED` events (which surface, who saw it,
  when)
- All operator actions against this recommendation_id
- All escalation events linked to this recommendation_id
- The closing event

**Reconstruction completeness:** complete lifecycle from generation
to closure, with all operator interactions reconstructible.

### 13.3 Replay an escalation

**Required:**
- The `OPERATOR_ESCALATED` event (or `ESCALATION_CREATED` per
  naming decision)
- The associated conflict or recommendation
- Source actor's session_id (for actor context)
- Target authority's actions: receipt, review, resolution
- The `ESCALATION_RESOLVED` event
- Any nested escalations (chain reconstructable)

**Reconstruction completeness:** the chain from initiation to
closure, with all actors and reasons preserved.

### 13.4 Replay a defer / override

**Required:**
- The original `RECOMMENDATION_GENERATED` with snapshot
- The `OPERATOR_DEFERRED` event (with defer_until, reason_code,
  notes) OR
- The `OPERATOR_OVERRODE` event (with override_choice, reason_code,
  notes, original_recommendation_id)
- For defer: the subsequent revisit event(s) (recommendation
  re-presented at `defer_until`) or `DEADLINE_PASSED` if missed
- For override: the operational state changes resulting from the
  override

**Reconstruction completeness:** the authority's decision, the
reasoning, and the consequences.

### 13.5 Replay a handover

**Required:**
- `HANDOVER_CREATED` event with outgoing SS handle, shift_id,
  pending-item inventory hash, escalation queue depth
- The full handover note text (stored in `shift_handover` table;
  referenced by hash in the event for integrity)
- The pending-item details (recommendations, escalations, etc.)
  at handover time
- `HANDOVER_ACCEPTED` event with incoming SS handle, time
- Any clarification exchanges (if event-emitted)
- The outgoing `SHIFT_CLOSED` and incoming `SHIFT_OPENED` events

**Reconstruction completeness:** the operational state transferred,
the actors involved, the timing of the transition.

### 13.6 Replay an incident

**Required:**
- `INCIDENT_OPENED` event with incident_id, scope, time window
- All operationally-relevant events in the time window
- All affected recommendation lifecycles (full chains per §13.2)
- All affected escalation chains (per §13.3)
- The originating event(s) that triggered the incident
  classification
- Any `INCIDENT_FINDING_RECORDED` events from prior replays
- `INCIDENT_REPLAYED` events showing review history
- Chain integrity verification across the incident window

**Reconstruction completeness:** the full operational sequence
that constituted the incident, with all decisions, escalations,
and outcomes preserved.

### 13.7 Replay a stakeholder delay / issue

**Required:**
- The originating `STAKEHOLDER_NOTIFIED` event (assignment
  creation)
- The `OPERATOR_ACTED` event for stakeholder confirmation
- The `OPERATOR_ACTED` event for delay report (with reason_code,
  expected duration)
- Any operational reactions: VTSO escalation, schedule changes,
  affected recommendations
- The closing event (delay resolved / assignment completed / etc.)

**Reconstruction completeness:** the assignment lifecycle from
notification to resolution, including delay impact.

### 13.8 Replay an executive review package

**Required (per Port Brief and incident replay):**
- The signed-off Port Brief content (referenced by `OPERATOR_SIGNED_OFF`)
- The incidents linked to the Brief (per §13.6)
- The aggregated metrics for the Brief's time window (D16
  recomputed against the audit data)
- All `AUDIT_READ` events from Executive consumption
- Chain integrity across the review window

**Reconstruction completeness:** what the Executive saw, when, and
what aggregated picture supported the review.

---

## 14. Analytics and Executive Metrics

Per permission model §11 and screen architecture §8, the Executive
Dashboard consumes derived metrics. This section specifies the
candidate metrics, their derivation source, and their cadence.

### 14.1 Operational metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Conflicts by type | count of `CONFLICT_DETECTED` events grouped by `conflict_type` | rolling 7d / 30d / 90d | from D15 |
| Conflicts by severity | same, grouped by `severity` | rolling | from D15 |
| Conflict frequency over time | hourly bucket of `CONFLICT_DETECTED` | rolling | from D15 |

### 14.2 Recommendation metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Recommendation adoption | (count of `OPERATOR_ACTED` with action_type=conflict_resolve) / (count of `RECOMMENDATION_GENERATED`) | rolling 30d | join D15 + D5 |
| Recommendation deferral rate | count of `OPERATOR_DEFERRED` / count of `RECOMMENDATION_GENERATED` | rolling | from D15 |
| Recommendation override rate | count of `OPERATOR_OVERRODE` / count of `RECOMMENDATION_GENERATED` | rolling | from D15 |
| Recommendations expired without action | count of `DEADLINE_PASSED` | rolling | from D15 |

### 14.3 Action metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Time-to-acknowledge | timestamp(`OPERATOR_ACKNOWLEDGED`) − timestamp(`RECOMMENDATION_PRESENTED`) | per-event; distribution per role | from D15 |
| Time-to-resolve | timestamp(`OPERATOR_ACTED` resolve) − timestamp(`RECOMMENDATION_PRESENTED`) | per-event; distribution | from D15 |
| Time-to-acknowledge by severity | bucket above by severity | per-event | from D15 |
| Deadline misses | count of `DEADLINE_PASSED`, optionally by recommendation type | rolling | from D15 |

### 14.4 Escalation metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Escalation rate | count of `OPERATOR_ESCALATED` per shift / per VTSO | rolling | from D15 |
| Escalation closure time | timestamp(`ESCALATION_RESOLVED`) − timestamp(`OPERATOR_ESCALATED`) | per-event | from D15 |
| Bypass-escalation rate | count of escalations with `bypass=true` flag | rolling | from D15 — operational policy signal |
| Top escalation reason codes | count by reason_code | rolling | from D15 |

### 14.5 Shift metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Shift workload | events per shift per VTSO | per-shift | from D15 |
| Handover note length | character count of handover note | per-handover | from D9 |
| Handover-to-acceptance time | timestamp(`HANDOVER_ACCEPTED`) − timestamp(`HANDOVER_CREATED`) | per-handover | from D15 |
| Pending items at handover | count from `HANDOVER_CREATED` payload | per-handover | from D15 |

### 14.6 Infrastructure metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Berth disruption duration | sum of berth-unavailable windows from `BERTH_AVAILABILITY_CHANGED` events | rolling | from D15 |
| Berth utilisation | computed from movement data + berth occupancy state | rolling | from D3 + D2 (operational state cache + audit for historic) |

### 14.7 Stakeholder metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Stakeholder delay frequency | count of `OPERATOR_ACTED` with action_type=report_delay per stakeholder type | rolling | from D15 |
| Confirmation latency | timestamp(`OPERATOR_ACTED` confirm) − timestamp(`STAKEHOLDER_NOTIFIED`) | per-assignment | from D15 |

### 14.8 Incident metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Incidents by severity | count of `INCIDENT_OPENED` grouped by severity | rolling | from D14 |
| Incident recurrence | similarity of incidents over time | rolling | derived |
| Incident replay frequency | count of `INCIDENT_REPLAYED` events | rolling | from D15 |

### 14.9 System metrics

| Metric | Derivation | Cadence | Notes |
|---|---|---|---|
| Audit chain integrity status | `verify_chain` result | continuous (hourly cron, V1.x) | from D15 directly |
| Recommendation confidence trend | average `confidence` from `decision_support` block | rolling | from D15 (recommendation snapshots) |
| Data-source uptime | tracked via fallback frequency | rolling | from operational telemetry |

### 14.10 Derivation source clarification

| Metric category | Derived from D15 | Derived from operational state |
|---|---|---|
| Conflicts | yes | partial (current snapshot) |
| Recommendations | yes | yes |
| Actions | yes (primary) | no |
| Escalations | yes | no |
| Shifts | yes | yes |
| Berth utilisation | partial | yes |
| Stakeholder | yes | no |
| Incidents | yes | yes |
| System | partial | yes |

**Most metrics are derived from D15 (audit ledger).** This is by
design: the audit ledger is the operationally-authoritative
substrate. Operational-state-only metrics (current vessel counts,
real-time weather conditions) are real-time but not historical and
do not feature in trend analytics.

### 14.11 Aggregation cadence

- **Real-time KPI tiles** (e.g. "currently active conflicts"):
  computed on dashboard load, sub-second
- **Daily roll-ups** (e.g. "yesterday's throughput"): computed
  overnight, cached
- **Weekly trends:** computed weekly, cached
- **Monthly aggregations:** computed monthly, cached
- **Custom-window queries** (e.g. "show me overrides for the last
  3 weeks"): computed on-demand from D15

---

## 15. AI / Coordination Intelligence Boundaries

V1 does NOT ship AI features. This section defines what *future* AI
may consume from Horizon's information substrate, and explicitly
what AI must NOT consume or expose without governance.

### 15.1 What future AI MAY consume

| Information | Purpose | Constraint |
|---|---|---|
| Normalised operational state (D1–D5, D10) | Pattern detection in current operations | Tenant-scoped only; never cross-tenant |
| Historical recommendations (D5 from D15) | Analyse engine calibration trends | Must include source attribution + lineage |
| Action outcomes (D6 from D15) | Learn which recommendations get accepted vs overridden | Include reason codes; pattern, not individual scoring |
| Replay packages (per §13) | Train models on full incident lifecycles | Tenant-scoped; permission-respecting |
| Reason codes (closed catalogues per workflow §8.5 / §10.6) | Categorise operational rationale | Catalogue must be stable for model training |
| Conflict timelines | Predict conflict emergence | Per-port; tenant-scoped |
| Aggregated metrics (D16) | Identify trend anomalies | Aggregation level only; not per-actor |

### 15.2 What AI MUST NOT consume or expose without governance

| Information | Why prohibited |
|---|---|
| Personal identity data (named users beyond operator_handle) | privacy; de-identification standard |
| Sensitive operational notes (free-text from operators) | privacy; unstructured, may contain incidental personal information |
| Unscoped stakeholder data (a pilot's view of other pilots' work) | scope violation |
| Raw audit payloads beyond permission boundary | reveals data the consumer cannot legitimately see |
| Cross-tenant data | per-tenant isolation; multi-tenant pollution risk |
| Security-sensitive incident detail (per §10.4) | clearance restriction |
| Commercial / financial detail beyond aggregated form | commercial sensitivity |
| Internal executive analysis (HR-style observations about operators) | analysed-subject consent boundary |

### 15.3 AI output governance

When AI eventually produces outputs (recommendations, summaries,
trend insights), the outputs must:

- Be tagged with explicit AI-origin provenance
- Carry a confidence indicator
- Never displace human authority — outputs are advisory; HM /
  authority decisions are not delegable to AI
- Be subject to audit just like operator outputs (an
  `AI_RECOMMENDATION_GENERATED` candidate event type may emerge,
  parallel to `RECOMMENDATION_GENERATED` but flagged)
- Respect role-scoped projection — AI output to an Executive view
  is aggregated; AI output to a VTSO view may be detailed

### 15.4 AI prerequisites in this document

Per principle 8: AI can consume information only after source,
scope, and lineage are clear. This document is the prerequisite
substrate. V1.0 establishes:

- Source attribution (every record carries its source)
- Scope (tenant, port, shift, role) on every record
- Lineage (audit-chained, hash-verifiable)

Once these are reliably present (V1.x), AI consumption becomes
governable — not before.

### 15.5 What this document does NOT do

- Does not authorise any AI feature
- Does not specify which AI provider, model, or technique
- Does not assume AI augmentation is mandatory at any V1 phase
- Does not define an AI roadmap

AI roadmap is a separate Phase-2-or-later conversation. This
section exists only to set the *information* preconditions so
future AI work doesn't have to be retrofitted into a substrate
that wasn't designed for it.

---

## 16. Data Quality and Confidence

V1 must surface data quality explicitly. Operators making
decisions deserve to know when the underlying data is stale,
incomplete, or in conflict.

### 16.1 Source confidence

Every record carries a source attribute (per principle 2). Source
confidence categories:

- **Authoritative live** — AISStream live, BOM live, Kyber-fed
  pilotage (V1.5+), operator-asserted operational state
- **Authoritative cached** — recent cache from authoritative
  source (e.g. MST cache <5min old)
- **Degraded fallback** — fallback source in use (cosine tides,
  simulation, stale cache >5min)
- **Synthetic** — demo/test data (Beta 10 only; not production)

### 16.2 Stale data handling

When a source goes stale (no fresh data within expected refresh
window):

- The cached value continues to be used (degraded confidence)
- The UI surfaces "stale" indicator on affected data
- If staleness exceeds threshold, fall back to next-best source
- The operator is informed before stale data is used in a decision
- Audit records the data-source attribution (so replay shows what
  was used)

### 16.3 Fallback data

When fallback paths fire:

- Surfaces continue to render with the fallback (so operations
  doesn't stop)
- Clear visual indication that fallback is in use
- Decision-support computations annotated as
  "computed with fallback data"
- Confidence in resulting recommendations is degraded

### 16.4 Missing data

If a required input is entirely missing (no source available):

- Conflict / recommendation computation that depends on it cannot
  run → no false-positive output
- The UI surfaces the gap explicitly ("UKC data unavailable")
- The operator may proceed manually with knowledge of the gap
- No silent guessing or inference from partial data

### 16.5 Conflicting source data

When two sources disagree (e.g. AIS position vs port-system
position):

- Default: the authoritative source per the source-of-truth model
  (§4) wins
- Disagreement is recorded; surfaced to operator as "sources
  disagree; using <primary>"
- Operator can override the primary explicitly (with audit trail);
  override is preserved for the rest of the operational context
- A `SOURCE_RECONCILIATION_RECORDED` candidate event captures the
  disagreement for trend analysis

### 16.6 Operator correction

Operators can correct data Horizon ingested incorrectly (V1.x):

- Override the value with an operator-asserted value
- Capture the original ingested value alongside the operator's
  value
- Audit records the correction with reason
- Subsequent ingestions from the same source may re-conflict;
  surfaced for repeat-correction or escalation

### 16.7 Confidence displayed to users

UI conventions:

- Authoritative live: no decoration (the default)
- Authoritative cached: subtle "cached" indicator
- Degraded fallback: clear "fallback" indicator
- Synthetic: explicit "demo data" indicator (Beta 10 only)
- Missing: explicit "data unavailable" placeholder, not blanks

### 16.8 Audit of data-quality changes

Significant data-quality events emit audit:

- A source falling stale beyond threshold:
  `DATA_SOURCE_STALE_DETECTED` (V1 candidate)
- A source recovering: `DATA_SOURCE_RESTORED` (V1 candidate)
- Operator correction: `OPERATOR_ACTED` with action_type
  `data_correction`
- Source disagreement: `SOURCE_RECONCILIATION_RECORDED` (V1
  candidate)

These events ensure that data-quality history is part of the
replay substrate.

---

## 17. Build Sequencing Recommendation

Information-architecture build sequence, paralleling (and
depending on) the other three V1 documents' sequences. Each phase
requires its own explicit authorisation before implementation
begins.

### V1.0 — Canonical operational state model and role-scoped projections

- Domain D1–D3, D10–D12 normalisation layer (from existing Beta 10
  sources)
- Per-record source attribution (every operational record carries
  source field)
- Role-scoped `/api/summary` projection per §9 (server-side
  filtering)
- Tenant + port scope enforcement at every projection boundary
- D17 admin configuration schema (config schema from Phase 0)
- No new audit event types yet

### V1.1 — Conflict / recommendation / action information flow

- D4 (conflicts) and D5 (recommendations) wired through to the
  role-scoped projections
- D6 (actions) audit emission for VTSO actions
  (`OPERATOR_ACKNOWLEDGED`, `OPERATOR_ACTED` action types)
- Decision-time snapshot embedded in `RECOMMENDATION_GENERATED`
  (Phase 0.7b is live; V1 confirms its consumption)
- Resolution notes capture

### V1.2 — Audit / replay linkage

- D15 closed-set extension for V1 candidates per workflow §14.3
- `OPERATOR_DEFERRED`, `OPERATOR_OVERRODE`, `DEADLINE_PASSED`,
  `SESSION_ENDED_WITHOUT_ACTION` emission (Phase 0 reserved
  becomes live)
- Audit-read meta-events (`AUDIT_READ`) for HM review surfaces

### V1.3 — Shift / handover and escalation information model

- D8 (shifts), D9 (handovers) schemas and event emission
- D7 (escalations) chain model
- `SHIFT_OPENED`, `SHIFT_CLOSED`, `HANDOVER_CREATED`,
  `HANDOVER_ACCEPTED`, `OPERATOR_ESCALATED`, `ESCALATION_RESOLVED`
  emission

### V1.4 — Executive analytics model

- D16 derivation infrastructure
- Trend computation jobs (daily / weekly / monthly aggregations)
- Cached aggregate storage with invalidation strategy
- `AUDIT_READ` emission for Executive surfaces

### V1.5 — Stakeholder feed and integration boundaries

- D13 (stakeholder assignments) schema and lifecycle
- Stakeholder notification surface
- Stakeholder action audit emission (`OPERATOR_ACTED` with
  stakeholder action types, `STAKEHOLDER_REPORTED_ISSUE`)
- Kyber integration boundary contract (API/event interface
  definition, no implementation yet)
- Federation discussion with Kyber team (deferred to V1.x if
  earlier alignment not warranted)

### V1.6 — AI-ready historical dataset and replay packages

- D14 (incidents) schema and lifecycle
- Replay package export (per §13)
- `INCIDENT_OPENED`, `INCIDENT_REPLAYED`,
  `INCIDENT_FINDING_RECORDED` emission
- AI-ready dataset documentation (which D15 events, which D1–D5
  snapshots, structured for ML consumption)
- External regulator read-access stub (V1.x — deferred unless
  contractually required)

### V1.x — Beyond the initial six phases

- AI consumption pilots (advisory output only)
- Cross-tenant coordination (when multi-tenant arrives)
- Long-term archive policy implementation
- Per-tenant retention class enforcement (partition drop
  automation)
- External regulator full integration

---

## 18. Open Questions

Surfaced for review before V1.0 design lock. Numbering reflects
domain-level concerns and questions surfaced during this draft.

1. **Authoritative source for berth schedules.** Should QShips be
   authoritative for movements, or is operator-entered the primary
   with QShips as a secondary verifier? Tenants may operate
   differently.

2. **Stale AIS handling.** What's the staleness threshold beyond
   which AISStream's data is considered unusable for conflict
   computation? Phase 0 has a default; V1 may need per-tenant
   tunability.

3. **AIS vs port-system reconciliation.** When AISStream says a
   vessel is at one position and the port system says another, who
   wins? Default: AISStream (closer to real). But port systems may
   have more accurate scheduled-position data (where the vessel
   is supposed to be vs where it currently is). Both have value.

4. **Financial impact: operational or executive-only?** Should
   commercial impact ($) appear on a VTSO's conflict detail surface,
   or only in Executive aggregate metrics? Operational decisions
   may benefit from cost awareness; commercial sensitivity may
   prefer aggregation.

5. **Pilot detail in Horizon vs Kyber.** How much detail does
   Horizon hold about individual pilots beyond capability
   descriptors? V1.5+ Kyber integration may push more identity
   data; the de-identified default should hold unless explicit
   need.

6. **Conflict reasons visible to stakeholders.** When a stakeholder
   sees a delay in their assignment, do they see *why* (which
   conflict caused it)? Operational transparency vs operational
   security.

7. **Audit payload redaction.** Some events may carry detail that
   regulators see but other consumers shouldn't (e.g.
   security-sensitive incident detail). Per-event redaction policy
   needs definition before external regulator access (V1.x).

8. **Retention requirements by information type.** Different
   domains may need different retention:
   - D15 audit events: long retention (regulatory, possibly
     5+ years)
   - D14 incidents: as-long-as-D15
   - D9 handovers: shift-history relevant for a year or two
   - D5 recommendations: included in D15
   - D3 movements: depends on regulatory requirements
   - D16 metrics: long for trend computations
   - Real-time operational state: transient

9. **Analytics freshness requirements.** What's the SLA on D16
   metrics? Real-time? 5-minute-stale? Daily? Affects caching
   strategy.

10. **AI governance boundaries.** Before any AI feature ships, who
    governs the AI's training data scope, output authority, and
    audit posture? Tenant-level governance committee? AMS-level?
    Per-customer contract?

11. **Operator correction audit.** When a VTSO corrects an
    AIS-reported vessel name, that's an `OPERATOR_ACTED`-shaped
    event with `data_correction` action type. But operator
    corrections of weather or tides probably aren't operationally
    meaningful — should those even be allowed?

12. **Source disagreement display.** When sources conflict, how
    visually prominent should the disagreement be? Subtle
    indicator on the affected field, or banner across the operational
    console?

13. **Confidence-degraded recommendation behaviour.** When
    `RECOMMENDATION_GENERATED` is produced with low-confidence
    inputs, should the recommendation be presented at all? Or held
    until confidence improves?

14. **Cross-port data correlation for Executive scope.** A
    multi-port Executive (port group) may want to compare metrics
    across ports — does that data correlation work or are
    per-port metrics fundamentally apples-and-oranges?

15. **Data quality audit cadence.** How often does the system run
    its own data-quality self-check, and when does an issue rise
    to an `AUDIT_READ`-worthy event vs a passive log?

---

## 19. Recommendations

For the V1.0 design review:

1. **Treat this document as Information Architecture v0.1.**
   Expect iteration to v0.2, v0.3, ... as operational stakeholders
   and integration partners (Kyber, port systems, terminal
   operators) push back on assumptions. The information substrate
   is foundational; structural objections surface here are
   cheaper than after V1.0 schema implementation.

2. **Do not implement V1 data models until this is reviewed.**
   The schema work for V1.0 (RBAC tables, operational tables,
   audit closed-set extensions, V1 event types) is substantial.
   Implementing against an unstable information model leads to
   migrations and reconciliation later. Lock the model first.

3. **Use this as input to API design, database schema, Claude
   Design / CX, and integration planning.** This document is the
   *substance* contract; the other three V1 documents are the
   *behaviour* contract. Together they define what V1 is and is
   not.

4. **Keep Stage E-prod paused unless separately authorised.**
   The recommendation from PR #30 §14.6, PR #31 §18, and PR #32
   §18 continues to apply. Activating production audit before V1's
   user/role identity is on each audit row means recording Beta-
   10-shaped actor attribution into the persistent ledger. This
   document reinforces that recommendation; per-event payload
   requirements (user_id, role, scope, session_id per workflow
   §14) are not yet emitted by Phase 0 helpers.

5. **Keep Beta 10 protected.** The Phase 0 regression gate
   (`tests/test_beta10_regression.py`) is the explicit guard
   against accidental drift. V1.0 implementation will necessarily
   change the regression baseline; that update is deliberate, not
   silent.

---

**End of v0.1.** Reviewer comments expected before v0.2.

The V1 design quartet (Permission Model, Workflow Model, Screen
Architecture, Information Architecture) is now complete. No V1
implementation should begin until all four documents have been
reviewed and accepted by operational stakeholders and (separately)
by the architecture team.
