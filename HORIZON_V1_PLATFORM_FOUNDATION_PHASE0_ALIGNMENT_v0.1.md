# Horizon V1 — Platform Foundation Phase 0 Alignment (v0.1)

**Document status:** Draft for review — **planning alignment only**
**Document type:** Strategic-direction alignment for Platform Foundation Phase 0
**Target stream:** Future architecture / V1 platform foundation only
  **— Beta 10 is excluded by the Immutability Rule.**
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ fd73ca8`
  (post-PR #70 — M2 Retrospective)
**Date:** 2026-05-21
**Scope of authority:** Resolves the five Tony-direction alignment
  questions identified in `HORIZON_V1_PLATFORM_FOUNDATION_v0.1.md`
  §15.1 and `HORIZON_V1_M2_RETROSPECTIVE_v0.1.md` §15.3, **before**
  Platform Foundation implementation planning begins. **Planning
  alignment only** — does **not** authorise implementation, vendor
  selection, framework lock-in, commercial commitment, deployment,
  or any runtime change. Does **not** modify any previously merged
  document. Uses architecture-planning ranges throughout; does
  **not** invent production numbers as facts; does **not** create
  hard commercial commitments.

**Authoritative inputs (all on `main`):**
- `HORIZON_V1_PLATFORM_FOUNDATION_v0.1.md` (PR #68, `0228265`)
- `HORIZON_V1_M2_RETROSPECTIVE_v0.1.md` (PR #70, `fd73ca8`)
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59)
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51)
- `HORIZON_CAPABILITY_BACKLOG.md` (HC-001 PCAM, HC-002 VTS Spatial Surface)
- Beta 10 Immutability Rule (in force)

---

## 0. Reading this document

Each of §1–§5 below resolves one of the Tony-direction questions
from PR #68 §15.1. The format is:

- **Question** (verbatim from the source document)
- **Planning assumptions** (architecture-planning ranges; not facts)
- **Options assessed** (multiple, with trade-offs)
- **Recommended position** (Claude's analysis-based recommendation;
  not a commitment)
- **Tony-side decision required** (the explicit ask)
- **Implications for platform architecture priorities**

§6 is the readiness summary tying the resolved alignment back to
the Platform Foundation Phase 0 scope-proposal decision.

This document is **alignment** — it surfaces options, recommends
where evidence supports, and identifies what still needs Tony's
strategic call. It does **not** make commercial commitments or
authorise implementation.

---

## 1. First live-client port target

### Question (PR #68 §15.1.1)
*"Which port is the first live-client target for V1 production?
Brisbane (strongest data maturity), Darwin (now demo-verified), or
another?"*

### Planning assumptions
- "First live-client" means the first port where Horizon V1 is used
  by a real port-authority customer or partner organisation, not
  internal Beta 10 demo.
- The choice affects: integration priorities (which AIS provider /
  PMS / pilot organisation systems integrate first), regulatory
  posture (state Marine Act jurisdiction), commercial framing
  (which customer relationship matures first), data-residency
  defaults, and the audit-chain genesis (per-tenant).
- The choice does **not** preclude subsequent ports — it sequences
  them.

### Options assessed

| Candidate | Strengths | Constraints / risks |
|---|---|---|
| **Darwin** | Demo-verified post-PR #56; Darwin Port Corporation relationship exists in operational data (Darwin Port Handbook 2026 cited throughout the codebase); smaller port → lower concurrent load; cyclone-season operational rules are well-documented; demo-card simulation lock proves the read-only pipeline | NT jurisdiction (different Marine Act regime to QLD/VIC); smaller commercial scale; pilot proximity to a single port-authority customer |
| **Ports Victoria / Geelong** | Larger port with operational complexity; existing fixture coverage (`melbourne-sim.json`, `geelong` profile in `port_profiles.py`); bridge restrictions and channel geometry exercised; VIC Marine Act regime well understood in Australian maritime law | Multi-stakeholder relationship (Ports Victoria, port-authority subdivisions, terminal operators); higher concurrent load; commercial relationship maturity unknown from in-session context |
| **Gladstone** | Strategic Queensland regional port; LNG / coal terminals with high-value cargoes; Queensland Marine Act regime same as Brisbane (continuity); regulatory familiarity with QLD Department of Transport and Main Roads | Not in current fixture set; integration would require new port profile + fixture capture (Option 4b; out of M2 scope); commercial relationship maturity unknown from in-session context |
| **Brisbane** (reference) | Strongest data maturity (QShips integration heritage; richest fixture); existing Beta 10 demo presence; biggest port → most operational diversity | **Risk: confusable with Beta 10.** Brisbane is the Beta 10 demo port. A Brisbane V1 production deployment risks Beta 10 / V1 conflation in customer perception. Beta 10 Immutability Rule must be visibly preserved |

### Recommended position
Claude's analysis-based recommendation: **Darwin first**, with
**Ports Victoria / Geelong second**.

Rationale:
- Darwin's read-only operational pipeline is **already demo-
  verified** post-PR #55 / PR #56. The first live-client deployment
  is the highest-risk milestone in the programme; starting from a
  surface that has already been visually validated by Tony
  minimises new-surface risk.
- Darwin's smaller scale gives the first-deployment cycle slack
  for the inevitable operational learning curve (auth, RBAC, audit
  chain, integration adapters all going live for the first time).
- The Brisbane risk (V1 / Beta 10 conflation in customer
  perception) is a real governance hazard. Starting in a non-
  Brisbane port preserves the optical separation that the
  Immutability Rule enforces architecturally.
- Ports Victoria / Geelong is the natural second deployment —
  larger port, more operational diversity, exercises the multi-
  port tenancy model and integration patterns at scale.
- Gladstone is a strong third candidate if the commercial path
  develops, but its absence from the fixture set means the first-
  deployment cycle would carry concurrent platform foundation
  work AND port-profile creation, which is higher risk.

### Tony-side decision required
- **Confirm first live-client port.** Default proposal: Darwin.
- **Confirm second-port sequencing.** Default proposal: Ports
  Victoria / Geelong.
- **Confirm Brisbane V1 deployment posture.** Specifically:
  is a Brisbane V1 deployment ever envisaged (after Beta 10
  reaches end-of-life), or is Brisbane permanently V1-excluded
  to preserve the Beta 10 reference surface?

### Implications for platform architecture priorities
- **Single-port-first architecture is acceptable for the first
  live-client deployment.** Multi-port tenancy is a structural
  property (Platform Foundation §8), but it does not have to be
  exercised in production on day one. The data model and API
  surface must be multi-port-capable; the first deployment may
  have one port in its scope.
- **NT-state regulatory posture** (Darwin) drives Marine Act
  alignment for the first deployment. Subsequent ports (Ports
  Victoria, Gladstone) bring VIC / QLD regulatory regimes; the
  platform must accommodate per-port regulatory variance from
  the start.
- **Customer-relationship architecture** — a single port-authority
  customer in the first deployment means the audit-chain genesis,
  data-residency, and secrets-store posture can be single-tenant
  initially; multi-tenant patterns are exercised in subsequent
  deployments.
- **Integration priority** — Darwin's primary integrations are
  AISStream/MST AIS (already exercised in Beta 10), BoM tides,
  and Darwin Port Corporation operational data sources. A first-
  live-client deployment scope should be one port-authority PMS
  (if any) plus existing AIS / BoM sources, not a full
  integration sweep.

---

## 2. Expected concurrent user profile

### Question (PR #68 §15.1.2)
*"What is the expected concurrent user count at first live
deployment? 5? 25? 100?"*

### Planning assumptions
- "Concurrent" means actively-authenticated, session-live users
  at peak (e.g. shift change-over). Not total user roster.
- All numbers below are **architecture-planning ranges**, not
  facts. Tony's commercial sizing is the ground truth.
- The number drives: connection-pool sizing, polling-vs-SSE
  decision (Platform Foundation §7.5), database write contention
  (audit chain), and on-call escalation thresholds.

### Options assessed (ranges only)

| Range | Plausible profile | Architecture implications |
|---|---|---|
| **1–5 concurrent** | Single shift, single port, one VTSO + one Harbour Master at peak | Single server, in-process state, polling sufficient. SSE optional. Audit chain easily synchronous. No connection-pool concerns. Suitable for the first live-client deployment. |
| **5–25 concurrent** | Single port, full shift coverage including pilotage / towage coordinators, terminal operators, and one or more executives | Single server still sufficient; polling + selective SSE for time-critical channels. Audit chain remains synchronous. Connection-pool sized in tens, not hundreds. Suitable for first deployment scale-up after stable operation. |
| **25–100 concurrent** | Multi-port or large single port with full coverage across all 8 workflow profiles (Operational Platform Workflows §3) + executives + admin | SSE for time-critical channels becomes important; polling alone strains aggregate request rate. Audit chain still synchronous but write throughput watched. Connection-pool concerns; horizontal scaling considered. Multi-tenant deployment plausible. |
| **100+ concurrent** | Multiple port-authority customers, large multi-port organisations, partner integrations | Durable event-stream substrate (Platform Foundation §11.2.3 open question), horizontal scaling, async audit-write paths with persistence guarantees, multi-tenant isolation enforced at infrastructure level. Out of scope for first live-client deployment; design for forward-compatibility only. |

### Recommended position
Plan for the **1–5 concurrent range for first live-client**, with
explicit forward-compatibility for the 5–25 range and design
patterns that do not preclude 25–100 later.

Rationale:
- A first live-client deployment is highest-risk; designing for
  100+ concurrent on day one adds complexity without operational
  need.
- The Platform Foundation §5 state-category separation and §7
  API-first / typed-contract patterns scale from 5 to 100+
  without architectural change; only deployment topology (number
  of servers, presence of durable event stream) changes.
- The 5–25 range is the natural growth path after stable single-
  shift operation in Darwin.
- The 100+ range is genuinely a different platform stance and
  should be triggered by a customer commitment, not designed
  in speculatively.

### Tony-side decision required
- **Confirm planning range for first live-client.** Default
  proposal: 1–5 concurrent.
- **Confirm growth-path range over first 12 months.** Default
  proposal: target 5–25 concurrent by end of FY27.
- **Confirm forward-compatibility target.** Default proposal:
  design for 25–100 concurrent forward-compatibility but do not
  build for it on day one.

### Implications for platform architecture priorities
- **Polling is the default** (M1 / M2 pattern continues); SSE /
  WebSocket arrive only when a specific time-critical channel
  needs it. Platform Foundation §7.5–§7.6 sequencing remains
  valid.
- **Audit chain stays synchronous** through the first deployment.
  Async write paths arrive when audit-write throughput becomes a
  measured bottleneck, not pre-emptively.
- **Single-server deployment** is acceptable for first live-
  client; horizontal scaling is a known-late-stage option.
- **Multi-tenant infrastructure** is **not** required for first
  live-client (single-tenant deployment). The data model is
  multi-tenant-capable from day one; the deployment topology
  catches up later.
- **On-call structure** — Tony plus one named technical on-call
  is sufficient for the 1–5 range (per Platform Foundation
  §15.3.5 open question). A SecOps function is appropriate from
  the 25+ range onward.

---

## 3. Capability-arrival sequence

### Question (PR #68 §15.1.3)
*"What is the desired sequence of capability arrival — auth first,
then audit, then write-path, or some other order? This is a
Tony-side strategic sequencing decision."*

### Planning assumptions
- "Capability" here is a platform capability, not a frontend tab.
- Sequencing is about which capabilities arrive in which Platform
  Foundation milestone. Each capability is its own scope
  proposal under separate authorisation.
- Hard dependencies constrain the sequence; soft dependencies
  shape it. The recommended sequence below honours both.

### Hard dependencies (cannot be reversed)
- **Auth must precede write paths.** A write path is an
  operator action; an operator action requires an authenticated
  actor; therefore auth precedes write.
- **Auth must precede RBAC enforcement.** Role-based
  authorisation requires an authenticated identity to authorise
  against.
- **Audit emission must precede operator-action endpoints.**
  An OPERATOR_ACTED event is only meaningful if it's auditable;
  introducing operator actions before the audit chain is
  operational risks misattribution and regulatory exposure.
- **Tenant isolation must precede multi-tenant production.**
  Cross-tenant data leakage is Sev-1 per Platform Foundation
  §8.6; cannot be deferred.
- **Observability must precede pen-test sign-off.** A
  pen-test relies on operational telemetry being in place.

### Soft dependencies (can flex)
- **API foundation and audit architecture** can be developed in
  parallel; both are pre-requisites for operator-action endpoints
  but neither blocks the other.
- **Replay surface** depends on the audit ledger being live but
  does not have to ship in the same milestone.
- **VTS spatial surface (HC-002)** does not block any other
  capability; it sits late in the sequence.
- **Recommendation lifecycle persistence** is part of the audit
  / event architecture; not separately sequenced.

### Recommended sequence (Platform Foundation Milestones — M3 onward)

This is a recommendation only; the milestone naming is indicative.

| Stage | Capability | Rationale |
|---|---|---|
| **PF-M1** | **Auth + RBAC foundation** | Enables every subsequent platform capability. Server-authoritative; least-privilege; port-scoped. Login flows, session/token, role catalogue, port-scope assignment. **No write paths yet.** |
| **PF-M2** | **API foundation + typed contracts + observability** | Versioned API surface; typed contracts generated from a single source of truth; structured logging, metrics, tracing, audit-chain integrity probe, on-call alert paths. **Reads only; no operator actions yet.** |
| **PF-M3** | **Audit / event architecture (append-only hash-chained ledger)** | Per-tenant genesis; synchronous emission in operator-intent path; immutable, defensible. Operator-presented events (per-recommendation), session events. **Still no operator-action write paths.** |
| **PF-M4** | **Operator-action write paths (ACK / DEFER / REJECT / ESCALATE)** | First operator-actionable surface. Each action emits an OPERATOR_ACTED row; server-authorised; port-scoped. APPLY (which produces downstream notifications) deferred to PF-M5. |
| **PF-M5** | **Recommendation lifecycle persistence + notification orchestration + APPLY action** | Decision-card lifecycle persists as a chain of audit rows; APPLY produces downstream notifications to pilot dispatcher, tug operator, terminal, agent. |
| **PF-M6** | **Replay surface + audit export workflow** | Replay snapshot derivation; Replay / Audit Mode UI surface (Canon §8 swimlane); audit export for regulatory inquiry. |
| **PF-M7** | **Ingestion hardening + integration adapters** | AIS / BoM / port-authority PMS integration adapters with retry / error isolation / degraded-mode handling per Platform Foundation §9. Source attribution and freshness visible to operators. |
| **PF-M8** | **Tenant isolation hardening + first multi-tenant deployment** | Per-tenant audit-chain genesis, per-tenant secrets, per-tenant observability scopes. Multi-port-organisation user model. Cross-port leakage tests as Sev-1 gate. |
| **PF-M9** | **VTS spatial surface (HC-002)** | After auth + RBAC + audit + operator actions are stable, the VTS surface delivers spatial value with the underlying coordination loop already real. |
| **PF-M10** | **Operational write-back integration** | Horizon updating port-authority PMS, statutory reporting, billing — only after every preceding capability is stable. |
| **PF-M11+** | **Future** | ML / analytics, additional integration partners, partner / API consumers, advanced reporting. Speculative; not sequenced. |

### Intentionally deferred
- **ML / opaque AI orchestration** — Platform Foundation §2.9
  prohibits this for operator-facing decisions in V1. ML may
  inform analytics post-PF-M5 but never replaces the deterministic
  rule layer.
- **Stage E-prod activation** — out of V1 platform scope.
- **Smart Ocean X dependency** — formally closed (Independence
  Reset); not reintroduced at any stage.
- **Kyber-boundary crossing** — out of scope; explicit
  authorisation path required if it ever approaches.
- **Beta 10 modification** — Immutability Rule; never.

### Tony-side decision required
- **Confirm the broad sequence.** Default proposal: PF-M1 →
  PF-M2 → PF-M3 → PF-M4 → PF-M5 → PF-M6 → PF-M7 → PF-M8 →
  PF-M9 → PF-M10.
- **Confirm whether PF-M1 and PF-M2 should be merged into a
  combined "platform-bootstrap" milestone.** Default proposal:
  keep separate so each is reviewable independently; merge only
  if scope creep proves they are inseparable.
- **Confirm whether replay (PF-M6) should arrive before or after
  the first multi-tenant deployment (PF-M8).** Default proposal:
  replay first — regulatory defensibility is needed for the first
  live customer.
- **Confirm whether HC-002 VTS spatial surface (PF-M9) is timed
  before first multi-tenant deployment (PF-M8) or after.** The
  recommendation above places it after, but VTSO operational
  pressure may move it forward.

### Implications for platform architecture priorities
- **PF-M1 (auth) is the highest-leverage first milestone.**
  Every subsequent capability depends on authenticated identity.
- **The first three milestones (auth, API, audit) all ship
  before any operator-action write path.** This is the
  defensible-by-design posture (Platform Foundation §6).
- **The first live-client deployment can plausibly happen at
  PF-M4** (operator actions exist; full audit chain operational;
  RBAC enforced). PF-M5 / PF-M6 follow as iterative deepening.
- **The platform reaches "deployable operational platform" status
  at PF-M5** (decision-card lifecycle persists; notifications
  fire; APPLY closes the loop).
- **Multi-tenant arrives later** (PF-M8) because first live-client
  is single-tenant.

---

## 4. Commercial framing

### Question (PR #68 §15.1.4)
*"What is the commercial framing for production — per-port
subscription, per-tenant enterprise, hybrid?"*

### Planning assumptions
- This document records the **planning positioning** to ensure
  Platform Foundation architectural decisions are commercially
  coherent. Hard commercial commitments (pricing, contracts,
  partner agreements) are out of scope and remain Tony's
  strategic call.
- Architecture must support multiple plausible commercial models
  without lock-in (Platform Foundation §12.6 — no framework
  lock-in).

### Positioning recorded

Horizon V1 is positioned as:

| Aspect | Positioning |
|---|---|
| **Primary identity** | An **operational coordination platform** for port-authority customers and their operational partners (pilot organisations, tug operators, terminal operators) |
| **Secondary identity** | A **predictive coordination layer** — the platform surfaces conflicts and recommendations before they become operational incidents |
| **NOT a VTS replacement** | Per the Pilot Proximity App note (PR #57) §5.4 and HC-002 — Horizon does not replace the official Vessel Traffic Service authority |
| **NOT a pilotage operating system** | Per PR #57 §5.4 and HC-001 — Horizon does not direct pilots, dispatch pilotage, or replace pilot organisation systems |
| **NOT a navigation system** | Per PR #57 §5.2 and Platform Foundation §2.10 — no navigation commands, no vessel control authority |

### Relationship to existing AMSG concerns

| Existing AMSG concern | Horizon's relationship |
|---|---|
| **Kyber** | Kyber boundary in force. Horizon V1 platform foundation does not cross the Kyber boundary. Any future capability that approaches the boundary requires explicit authorisation and dedicated scope proposal |
| **Smart Ocean X** | **Independence maintained** per the Horizon Independence / Architecture Reset (PR #51). Horizon V1 is not Smart Ocean X; not built on Smart Ocean X; not dependent on Smart Ocean X. Historical / legal references preserved as record but do not drive Horizon architecture |
| **Ocean Intelligence** | Out of in-session scope to characterise without Tony's input. If Ocean Intelligence is a separate AMSG product / brand / capability, its relationship to Horizon V1 should be explicitly recorded in a follow-on alignment document. **Tony-side input required.** |

### Deployment posture options

| Option | Description | Suitability |
|---|---|---|
| **Software-only deployment** | AMSG licences the platform; customer or systems integrator hosts and operates. Requires the customer to bring infrastructure, ops, on-call. | Lower margin; more partners; less operational control. Suitable for systems-integrator partnerships. |
| **Managed platform / SaaS** | AMSG hosts, operates, and updates the platform; customer accesses via login. AMSG owns the production environment, on-call, observability, secrets, deployments. | Higher margin; more operational control; recurring-revenue posture. Suitable for direct port-authority relationships. |
| **Hybrid** | AMSG hosts a multi-tenant platform with optional dedicated tenants / on-premises options for customers with regulatory or data-residency constraints. | Operationally complex but commercially flexible. Suitable mid- to long-term. |

### Recommended position
Claude's analysis-based recommendation: **plan for managed-platform
/ SaaS posture as the default**, with **software-only / on-
premises options preserved** by the architecture for customers
with strict regulatory or data-residency constraints.

Rationale:
- Managed platform aligns with the Platform Foundation §11
  production-isolation, secret-management, observability, and
  pen-test posture — these are most naturally AMSG-owned in a
  hosted environment.
- A hybrid posture (multi-tenant SaaS + dedicated-tenant or
  on-premises for strict customers) is the long-term commercial
  flexibility; the architecture must permit it but does not have
  to exercise it on day one.
- Software-only / systems-integrator-led deployments are
  feasible but cede operational governance; this is not the
  recommended primary posture.

### Tony-side decision required
- **Confirm primary deployment posture for first live-client.**
  Default proposal: managed platform / SaaS.
- **Confirm whether software-only / on-premises options are
  retained in the long term** or explicitly excluded.
- **Confirm relationship to Ocean Intelligence.** This document
  cannot characterise the relationship without Tony's input.
- **Confirm commercial-model placeholder.** Default proposal for
  internal planning only: per-port subscription with seat-based
  uplifts (this is a planning placeholder; not a pricing
  commitment).

### Implications for platform architecture priorities
- **Managed-platform posture amplifies the importance of PF-M1
  (auth) and PF-M2 (observability).** A SaaS platform's
  reputation is its uptime and its security; both depend on
  these foundations.
- **Multi-tenant readiness (PF-M8) is medium-priority** even if
  the first live-client deployment is single-tenant — the
  managed-platform business model needs multi-tenant readiness
  within ~12–18 months of first deployment.
- **Data-residency configurability** must be present from PF-M2
  (API foundation) onward so that strict-jurisdiction customers
  can be supported without re-architecture.
- **Pen-test cadence** (Platform Foundation §6.10) becomes a
  recurring commercial concern under managed-platform posture,
  not just a pre-go-live gate.

---

## 5. Target timing assumptions

### Question (PR #68 §15.1.5)
*"Is there a target date for first live-client production
deployment? Real or directional?"*

### Planning assumptions
- This section uses **directional** timing only. No hard
  commercial commitments are made.
- "FY27" means AMSG's financial year 2027 (calendar Jul 2026 –
  Jun 2027 in the typical Australian convention; Tony to
  confirm if AMSG uses a different fiscal calendar).
- Timing assumptions feed Platform Foundation milestone
  cadence, not contractual delivery dates.

### Directional timing framework

| Horizon | Period | Activity |
|---|---|---|
| **Near-term** | Now (2026-05) through end-FY26 (2026-06) | Resolve PR #68 §15.1 alignment (this document); schedule Tony Beta 10 visual check (M2 A11); optional sandbox source-branch switch (M2 A8); Platform Foundation Phase 0 scope proposal drafted (separate authorisation) |
| **FY27 H1** | 2026-07 → 2026-12 | Platform Foundation PF-M1 (auth + RBAC), PF-M2 (API + observability), PF-M3 (audit / event architecture). Internal-only operation against fixtures and sandbox data sources; no live customer |
| **FY27 H2** | 2027-01 → 2027-06 | Platform Foundation PF-M4 (operator-action write paths), PF-M5 (lifecycle persistence + notifications + APPLY). Internal-readiness end-of-FY27 H2 — platform is feature-complete for first live-client without yet having one |
| **FY28 H1** | 2027-07 → 2027-12 | **First live-client target window.** PF-M6 (replay) ships pre-live; first live-client deployment in Darwin (per §1); pre-go-live pen-test; production governance live |
| **FY28 H2 → FY29** | 2028-01 onward | PF-M7 (ingestion hardening), PF-M8 (tenant isolation + second live-client e.g. Ports Victoria), PF-M9 (HC-002 VTS spatial surface), PF-M10 (operational write-back integration) |

### Internal readiness vs external deployment readiness

| Readiness type | Definition | Target |
|---|---|---|
| **Internal readiness** | Platform is feature-complete and operationally validated against fixtures / sandbox data. Tony and AMSG team can demonstrate, test, and validate the full operational loop | End of FY27 H2 (recommendation only) |
| **External deployment readiness** | Platform has passed external pen-test, has signed-off legal / regulatory framing, has customer commercial agreement, and has on-call / runbook for live operation | FY28 H1 (recommendation only) |
| **Beta → deployment-ready transition** | The conceptual transition from Beta 10 reference-build status to V1 production-ready platform. **Note:** Beta 10 itself is permanently isolated under the Immutability Rule. The "transition" is V1 becoming production-ready, not Beta 10 evolving | FY28 H1 alongside external deployment readiness |

### Recommended position
**Plan internal readiness for end-FY27; plan first live-client
deployment for FY28 H1 (Darwin).** Earlier dates are commercially
unsafe; later dates risk drift. These are directional planning
anchors, not commitments.

### Tony-side decision required
- **Confirm AMSG fiscal calendar.** Default assumption: Australian
  calendar Jul–Jun. Tony to correct if different.
- **Confirm internal-readiness target.** Default proposal: end of
  FY27 H2.
- **Confirm first-live-client target window.** Default proposal:
  FY28 H1.
- **Confirm whether any customer-commitment constraints exist**
  that would compress these timelines. If so, the milestone
  sequence in §3 may need re-evaluation.

### Implications for platform architecture priorities
- **PF-M1 must start in FY27 H1.** This is the high-leverage
  bootstrap; delaying it cascades.
- **PF-M2 and PF-M3 can run partially in parallel** (per the soft-
  dependency note in §3); concurrency helps hit end-FY27
  internal readiness.
- **Pen-test scheduling** must be booked early in FY28 H1 against
  the platform state at end-FY27 H2.
- **Customer commercial-agreement work** is Tony-side and runs
  in parallel with platform work; both must converge by FY28 H1.

---

## 6. Platform Foundation readiness summary

### What is now sufficiently aligned

After this document is reviewed and merged, the following will be
sufficiently aligned to begin Platform Foundation Phase 0 scope-
proposal drafting:

1. **First live-client port target** — Darwin recommended;
   Ports Victoria / Geelong as second. Brisbane V1 posture
   needs Tony's explicit call.
2. **Concurrent user planning range** — 1–5 at first live-
   client; forward-compatible to 25–100. Tony confirms.
3. **Capability-arrival sequence** — PF-M1 auth → PF-M2 API +
   observability → PF-M3 audit → PF-M4 operator-action writes →
   PF-M5 lifecycle persistence + notifications + APPLY → PF-M6
   replay → PF-M7 ingestion hardening → PF-M8 multi-tenant →
   PF-M9 VTS spatial (HC-002) → PF-M10 operational write-back.
   Tony confirms broad sequence.
4. **Commercial framing** — operational coordination platform;
   predictive coordination layer; NOT VTS / pilotage OS /
   navigation system. Managed platform / SaaS posture
   recommended as default. Independence Reset framing
   maintained. Kyber boundary in force. Ocean Intelligence
   relationship needs Tony's input.
5. **Target timing** — internal readiness end of FY27 H2;
   first live-client FY28 H1. Tony confirms calendar and
   targets.

### What remains unresolved

The following items remain explicitly Tony-side and must be
resolved either in his review of this document or in a
follow-on alignment artefact, **before** Platform Foundation
Phase 0 scope proposal is drafted:

- §1: **Brisbane V1 posture** — permanently V1-excluded, or
  envisaged post-Beta 10 end-of-life?
- §3: **PF-M1 / PF-M2 separation** — keep separate or combine
  into a "platform-bootstrap" milestone?
- §3: **Replay (PF-M6) vs multi-tenant (PF-M8) timing** —
  recommendation is replay first; Tony confirms.
- §3: **VTS spatial (PF-M9) timing** — recommendation places it
  after multi-tenant; VTSO operational pressure may move it
  forward.
- §4: **Ocean Intelligence relationship** — needs Tony's input;
  cannot be characterised from in-session context.
- §4: **Long-term retention of software-only / on-premises
  options** — Tony's call.
- §4: **Commercial-model placeholder** — per-port subscription
  with seat-based uplifts is a planning placeholder, not a
  pricing commitment.
- §5: **AMSG fiscal calendar** — confirm Jul–Jun (default
  assumption) or correct.
- §5: **Customer-commitment constraints** on the timing — if any
  exist, the milestone sequence may need re-evaluation.

### Whether Platform Foundation implementation planning should proceed

**Recommended posture: Tony reviews this document; resolves the
items listed above; once resolved, Platform Foundation Phase 0
scope-proposal drafting may begin under a separate explicit
authorisation.**

This document is **alignment**, not implementation authorisation.
The next governance artefact (Platform Foundation Phase 0 scope
proposal) is a separate, later step that depends on the
resolved alignment.

### What the next governance step should be

1. **Tony's review** of this document, with focus on the "Tony-
   side decision required" markers in §§1–§5.
2. **Optional ChatGPT engineering review** of the capability-
   arrival sequence (§3) and the commercial framing (§4).
3. **Merge** this document onto `main` once review is complete
   and Tony's decisions are recorded (either inline in this
   document via a v0.2 update, or in a small follow-on
   "Platform Foundation Phase 0 Alignment — decisions" artefact).
4. **Optional second-round alignment document** if Ocean
   Intelligence relationship or other unresolved items need
   their own treatment.
5. **Tony's explicit authorisation message** of the form
   *"Authorised: draft Horizon V1 Platform Foundation Phase 0
   scope proposal"*. Until this message arrives, no scope-
   proposal drafting begins.
6. **Platform Foundation Phase 0 scope proposal PR** drafted in
   the same shape as the M2 Scope Proposal (PR #53):
   resolved scope, out-of-scope, deny-list, acceptance
   criteria, open questions, recommendations.
7. **Platform Foundation Phase 0 Implementation Plan PR**
   drafted after the scope proposal is merged, in the shape of
   the M2 Implementation Plan (PR #54): file scaffold (where
   applicable), dependencies, validation gates, acceptance
   criteria, rollback, stop conditions, phase sequencing.
8. **Tony's explicit Phase 0 authorisation message** of the
   form *"Authorised: begin Horizon V1 Platform Foundation
   Phase 0 implementation"*. Until this message arrives, no
   code is written.

---

## End of Platform Foundation Phase 0 Alignment v0.1

**Status: planning alignment only. Not authorised for
implementation.**

Confirmed by this document:
- This is an **alignment document**. No implementation, no vendor
  selection, no framework lock-in, no deployment, no commercial
  commitment, no runtime change is authorised.
- M0 / M1 / M2 are not retroactively modified.
- All previously merged documents are honoured and unmodified.
- The Beta 10 Immutability Rule is in force throughout. Beta 10
  is permanently isolated; no platform foundation work touches it.
- The Horizon Independence / Architecture Reset framing is
  preserved — no Smart Ocean X dependency framing anywhere in
  this document. Smart Ocean X independence is explicitly
  maintained.
- The Kyber boundary remains in force.
- Numbers used in this document are **architecture-planning
  ranges**, not facts. No production numbers are presented as
  facts; no hard commercial commitments are made.
- All concrete Platform Foundation work (scope proposal,
  implementation plan, implementation, deployment) is gated on
  separate explicit Tony authorisation per milestone.

**Next action:** Tony's review of §§1–§5 "Tony-side decision
required" items and the §6 readiness summary. Once Tony's
decisions are recorded, Platform Foundation Phase 0 scope-
proposal drafting may be authorised as a separate next step.
