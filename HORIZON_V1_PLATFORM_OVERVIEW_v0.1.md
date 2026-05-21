# Horizon V1 — Platform Overview (v0.1)

**Document type:** External-facing platform overview
**Classification:** Client-safe — suitable for stakeholder, prospect, and partner distribution
**Owner:** AMSG (AMS Group) — Tony Trajceski, AMSG Horizon Division
**Version:** 0.1 (initial draft)
**Date:** May 2026

---

## 1. What Horizon Is

Horizon is a predictive coordination layer for port operations, built to sit above existing vessel traffic, berth management and pilotage systems without replacing any of them. It draws together operational data that today lives in separate screens, separate teams and separate shift handovers, and presents it as a single decision-support surface — so port operators, harbour masters and coordinators can see emerging constraints earlier, coordinate responses across functions, and reduce the preventable disruptions that cost ports time, berths and operational confidence.

Horizon is wholly owned, built and operated by AMS Group, and is moving from proven demonstration to a deployable, production-grade platform designed for live port operations.

---

## 2. The Problem Horizon Solves

Port coordination today is fragmented by design. Vessel traffic services, berth planning, pilotage dispatch, towage allocation, terminal operations, and weather monitoring each operate in their own systems and organisational silos. The information that connects them — the emerging constraint that a late inbound vessel will collide with a berth-occupancy window while the tide is falling and the only available pilot is already committed — exists only in the heads of experienced operators who happen to be on shift at the right time.

When that information surfaces too late, the consequences are tangible: vessels anchor unnecessarily, berths sit empty while queues grow, pilot transfers are rushed, tugs are repositioned at cost, and terminals absorb delays they had no warning about. These are not system failures in the traditional sense — each individual system performed correctly. They are coordination failures: the right information existed, but it did not reach the right person at the right time in a form that supported a decision.

Horizon addresses this gap. It does not replace the individual operational systems that ports already rely on. It sits above them, ingesting their data, detecting emerging constraints across functional boundaries, and surfacing actionable coordination intelligence to the people who can resolve those constraints before they become disruptions.

---

## 3. Platform Architecture and Direction

Horizon is being built as a server-authoritative operational platform. The architectural commitment is that all coordination logic, conflict detection, decision-support construction, and operational state management will live on the server. The operator-facing interface — whether desktop, tablet, or future mobile companion — is a consumer of the platform's API surface, never the place where business rules execute.

This architecture is deliberate. In operational environments where decisions carry safety, commercial, and regulatory weight, the authoritative state of the port must live in one place, under one set of rules, with one audit chain. An architecture that distributes business logic to browser clients creates ambiguity about which version of the truth an operator is seeing — ambiguity that is unacceptable in a coordination platform.

**Proven in demonstration:** Horizon's Beta 10 release validates this architectural approach in a live environment. The demonstration platform ingests real-time AIS vessel data, live weather and tidal observations, and runs server-side conflict detection and decision-support generation against live port conditions. The decision-card workflow — detecting a constraint, constructing alternatives, presenting a structured coordination payload to the operator — has been demonstrated end-to-end across configured Australian port environments. Beta 10 serves as the commercial reference surface and the architectural proof point for the V1 platform programme.

**Current V1 operational surface:** The V1 programme is building the production-grade platform on top of the patterns proven in Beta 10. The current V1 milestone (M2) delivers six read-only operational views — Dashboard, Berth Timeline, Shift Log, VTS, Pilotage, and Performance — providing operators with visual access to the full coordination picture across centre-panel navigation. These views are fixture-fed and read-only by construction: they prove the operational surface layout, view-switching, and data presentation without introducing write paths, live backend calls, or authentication concerns. The read-only constraint is a deliberate engineering choice, not a limitation — it allows the operational surface to be validated independently before platform services are connected beneath it.

**Platform foundation direction:** The V1 platform foundation — currently in planning — defines the architectural properties that will underpin the production platform as it moves beyond the read-only surface:

- Every operationally meaningful surface served by a versioned, typed API. The web interface as one consumer among future surfaces (mobile companion, partner integrations, regulatory reporting).

- Authenticated, role-based access at the platform layer, with permissions scoped to actions, data, and port assignments. Identity and RBAC scaffolding has begun as foundational work; production-active enforcement is part of the platform foundation build-out.

- Server-owned operational state. The frontend renders presentation derivations; it cannot fabricate or mutate operational data.

- An append-only, cryptographically chained audit ledger capturing every operator action and every system event of operational significance. The audit-chain model has been prototyped (Phase 0.7 hash-chain precedent); production audit is part of the platform foundation build-out.

- Deterministic, documented rules behind every recommendation. No opaque model drives operator-facing decisions. This principle is already active in Beta 10's conflict detection and carries forward unchanged.

- External integrations via documented contracts, with no external system reaching into the operational store directly.

---

## 4. Operational Capabilities

Horizon's operational capability set spans the coordination lifecycle from vessel detection through post-movement review. Some capabilities are proven and active today; others are part of the platform foundation roadmap that the V1 programme is delivering.

### Proven and demonstrated (Beta 10)

**Predictive Conflict Detection** — Horizon continuously evaluates the operational state of the port against a documented rule set: berth-occupancy overlaps, under-keel clearance against tidal state, pilotage notice-window violations, towage availability gaps, bridge air-draft restrictions, weather threshold exceedances, and ETA variance beyond tolerance. When a constraint is detected — or predicted to occur within the coordination window — Horizon surfaces it as a structured conflict with severity classification, contributing factors, and time-to-impact. This capability runs against live data in the Beta 10 demonstration environment.

**Decision-Support Cards** — Each detected conflict that warrants operator attention is presented as a decision card: a structured coordination payload containing the constraint, the affected vessels and resources, sequencing alternatives with associated cost and risk metadata, a recommended option, and a deadline for action. The decision-card workflow has been demonstrated end-to-end in Beta 10, validating the pattern of server-side detection, recommendation construction, and operator-facing presentation.

**Operational Guidance** — Beyond discrete conflicts, Horizon generates ongoing operational guidance: proactive alerts about deteriorating conditions, resource availability warnings, pilotage and towage scheduling constraints, and weather-driven operational restrictions. Guidance items are categorised by operational domain (berth, weather, navigation, resources, operations) and severity.

**Weather and Environmental Intelligence** — Live weather data (wind speed and direction, swell height and period, visibility, precipitation, air pressure) and tidal observations and predictions are integrated from authoritative sources. Operational conditions are rated against documented thresholds — Excellent, Good, Moderate, Poor — providing at-a-glance awareness of the operating environment. Weather-driven operational restrictions (berthing wind limits, bridge air-draft clearance, channel swell thresholds) are evaluated continuously and surfaced as constraints when thresholds are approached or exceeded. Weather and tidal data is live in Beta 10, sourced from Open-Meteo and the Australian Bureau of Meteorology.

**Vessel Awareness** — Horizon maintains an operational picture of vessel movements within each port's scope: inbound traffic with estimated arrival times, vessels at berth with service states, outbound traffic with departure predictions, and vessels at anchor or in transit. In Beta 10, vessel data is sourced from live AIS feeds supplemented by port-authority vessel data where available.

**What-If Scenario Analysis** — Operators can model alternative coordination scenarios: "what happens if this vessel is delayed two hours?" or "what if we reassign this berth?" The scenario engine evaluates the consequences against the same rule set that drives live conflict detection, allowing operators to test decisions before committing to them. This capability is demonstrated in Beta 10.

**Port Brief Generation** — Horizon produces a structured Port Brief — a summary of current operational state, active conflicts, weather conditions, vessel movements, and coordination actions — suitable for shift handover, stakeholder communication, and operational record-keeping.

### Current V1 operational surface (M2)

**Full Centre-Panel Navigation** — The V1 operational surface provides six coordinated views accessible through centre-panel navigation: Dashboard (operational overview with conditions, vessel roster, and guidance), Berth Timeline (Gantt-style berth occupancy), Shift Log (operational event chronology), VTS (vessel traffic summary), Pilotage (pilotage queue and assignments), and Performance. These views are read-only, demonstrating the operational surface layout and information architecture that the production platform will serve through live API connections.

### Platform foundation roadmap

**Operator Decision Workflow** — The V1 platform will extend Beta 10's demonstrated decision-card pattern into a full operator action lifecycle: acknowledge, defer, apply, reject, or escalate. Each operator action will be server-authorised, port-scoped, and captured in the audit ledger. The right-rail action surface is architecturally reserved for this workflow in the V1 interface direction.

**Pilotage Coordination Awareness** — Pilotage assignments, boarding windows, transit sequencing, and pilot availability will be surfaced as coordination context. Horizon does not replace pilotage dispatch systems — it provides the shared awareness layer that connects pilotage scheduling to vessel movements, tidal windows, and berth availability, reducing the coordination overhead that currently depends on radio calls and phone conversations. The read-only Pilotage view in M2 is the surface precursor for this capability.

**Berth Coordination Timeline** — The current M2 Berth Timeline view provides the visual surface; the production platform will connect it to live berth data, enabling real-time occupancy tracking, arrival predictions, and constraint detection against berth parameters.

---

## 5. Multi-Port Operations

Horizon is designed for organisations that operate across multiple ports. The platform currently supports operational profiles for Brisbane (AUBNE), Melbourne (AUMEL), Geelong (AUGEX), and Darwin (AUDRW), with the architecture designed to accommodate additional ports without structural change.

Each port carries its own operational profile: geographic coordinates, berth catalogue with physical characteristics, tidal parameters, wind and swell thresholds, bridge restrictions, channel geometry, pilotage rules, and AIS source bindings. Port-specific rules (under-keel clearance thresholds, pilotage notice windows, towage bollard-pull requirements, cyclone-season protocols) are configurable per port rather than hard-coded, so the platform adapts to each port's regulatory and operational environment.

Beta 10 demonstrates live multi-port switching across configured Australian port environments with real weather, tidal, and AIS data per port. The V1 platform foundation extends this to production-grade multi-port tenancy: users scoped to their authorised ports, with a harbour master assigned to one port seeing only that port's data, and cross-port supervisors receiving explicit, audited port-context selection at the platform layer. Data isolation between ports will be enforced at the server, not at the interface.

---

## 6. User Roles and Workflows

Horizon serves distinct operational roles, each with a different relationship to the coordination surface. The V1 programme has documented eight workflow profiles that define how each role interacts with the platform:

**VTSO / Coordination Operator** — The primary operational user. Monitors vessel movements, manages conflict resolution, coordinates across functions. In the production platform, this role will acknowledge and act on decision cards directly. Requires spatial awareness, real-time conflict visibility, and direct access to coordination actions.

**Harbour Master** — Oversight and escalation authority. Reviews conflict state, approves or overrides coordination decisions, maintains operational governance. Requires summary-level awareness with drill-down to specifics.

**Port Authority Executive** — Strategic visibility. Monitors port performance, operational exceptions, and commercial impact. Does not interact with individual coordination decisions. Requires aggregated views and exception reporting.

**Pilotage Coordinator** — Manages pilot scheduling, boarding assignments, and transit sequencing within the broader coordination picture. Requires pilotage-specific views integrated with vessel movement and tidal context.

**Towage Coordinator** — Manages tug allocation and availability against vessel demand. Requires resource-availability views linked to vessel arrival predictions and berth assignments.

**Terminal Operator** — Manages berth-side operations. Requires visibility into arriving vessels, predicted berth windows, and coordination constraints affecting their terminal.

**Admin** — Platform configuration and user management. Does not hold operational permissions by default.

**Replay / Audit User** — Post-event review and incident investigation. Will read the historical operational state as it appeared to operators at any prior timestamp. Cannot edit history.

The current V1 surface (M2) presents a unified operational view. Role-specific modes and permission-scoped access are part of the platform foundation build-out, informed by these documented workflow profiles.

---

## 7. Data Sources and Integration

Horizon ingests operational data from multiple external sources, normalises it into a common operational model, and presents it with explicit source attribution. The operator always knows where each value came from and how fresh it is.

**AIS Vessel Data** — Real-time vessel position, heading, speed, and identity from AIS providers. Beta 10 actively ingests live AIS-derived vessel data through configured vessel-data providers, with source priority and fallback handled by Horizon. The V1 platform foundation defines an adapter architecture where multiple AIS sources can feed the same port, with documented reconciliation precedence when sources disagree.

**Weather and Tidal Data** — Live observations and forecasts from the Australian Bureau of Meteorology (tidal data) and Open-Meteo (weather conditions). These are active, live data sources in the current platform — not simulated.

**Port Authority Systems** — Berth schedules, vessel registrations, movement plans, and port-call records from existing port management systems. Integration contracts will be scoped per port engagement as the platform moves to production deployment.

**Pilotage and Towage Systems** — Pilot rostering, assignment records, and tug availability from operational dispatch systems. Integration architecture is defined; specific system connections will be established per deployment.

Each integration is designed to be independently versioned, independently monitored, and to fail gracefully. A degraded or unavailable feed does not corrupt the operational picture — it produces an explicitly signalled gap. Beta 10 already demonstrates this pattern: when a primary feed is unavailable, the platform falls through to secondary sources with visible degraded-mode indicators. The operator sees the source and freshness of every data point, not a silent best-guess.

---

## 8. Audit, Replay, and Regulatory Posture

Horizon's platform foundation defines an audit and replay architecture designed for the evidentiary requirements of maritime operations.

**Architectural direction:** Every operator action within the production platform — every acknowledgement, deferral, application, rejection, or escalation of a decision card — will be captured in an append-only, cryptographically chained audit ledger. The ledger will record the actor, timestamp, authenticated session, port scope, decision card identifier, action type, and chosen alternative. The chain will be tamper-evident: any post-hoc modification detectable.

**Precedent established:** The Phase 0.7 hash-chain model has prototyped the append-only cryptographic audit approach. The recommendation-presented audit pattern — ensuring that every recommendation surfaced to an operator is itself a recorded event — has been validated at the design level.

**Replay capability:** The audit architecture supports replay: the operational state as it appeared to an operator at any historical timestamp will be reconstructible from the ledger. This supports shift handover verification, incident investigation, training, regulatory inquiry, and operational learning. The M2 Shift Log view is the surface precursor — it demonstrates the operational event chronology layout, explicitly labelled as derived rather than audit-authoritative.

**Regulatory posture:** The audit and replay architecture is designed to satisfy the evidentiary requirements of maritime regulators, state Marine Acts, maritime insurers, and incident investigators. The separation of operator intent (what the human decided) from operational consequence (what happened as a result) is explicitly maintained in the architectural design — a distinction that regulators and insurers require for defensible incident review.

The production audit engine, replay engine, and retention policy are part of the platform foundation build-out, gated on their own scope proposals and explicit authorisation.

---

## 9. Security and Access Control

Horizon's security architecture is designed around authenticated, role-based access at the platform layer with port-scoped data isolation.

**Current state:** Identity and RBAC scaffolding has begun as foundational platform work. Beta 10 uses session-based authentication for access control. The V1 M2 operational surface is a read-only frontend with no write paths, no operator actions, and no live authentication enforcement — by deliberate construction, so the operational surface can be validated independently of the security layer.

**Platform foundation direction:** The production platform will enforce the following security properties:

- Authenticated access required for every operational endpoint. Anonymous access permitted only for clearly non-operational resources (health probes with no operational data).

- Role-based permissions scoped to actions, data, and port assignments. Users receive only the permissions their role requires.

- Port-scoped access isolation ensuring users cannot read, acknowledge, or act on data from ports outside their assignment. Cross-port data leakage is classified as a severity-one security defect.

- Server-side enforcement as the authoritative security control. The frontend may hide interface elements for UX clarity, but this is never the security boundary.

- Short-lived, scoped authentication tokens. Administrative actions audited. Re-authentication required for sensitive operations.

- Formal external security review and penetration testing prior to any live-client deployment, with findings remediated to an explicit severity bar.

- Environment separation: the Beta 10 commercial trust surface permanently isolated from V1 development and production environments; V1 sandbox separated from V1 production.

The security architecture, IdP selection, and production deployment posture are part of the platform foundation build-out, each gated on separate scope proposals and explicit authorisation.

---

## 10. What Horizon Is Not

Clarity about boundaries is as important as clarity about capabilities in the maritime operational domain.

**Horizon is not a Vessel Traffic Service.** It does not issue vessel movement instructions, provide navigational control, or replace the statutory authority of a VTS. It is a coordination layer that complements VTS operations by surfacing cross-functional constraints that VTS systems are not designed to detect.

**Horizon is not a navigation system.** It does not replace radar, ECDIS, or any bridge equipment. It does not provide navigational advice to vessels.

**Horizon is not a port management system replacement.** It does not replace existing berth booking, vessel registration, or billing systems. It sits above them, consuming their data and adding predictive coordination intelligence.

**Horizon does not act autonomously.** Every coordination action requires a human operator. Horizon recommends; humans decide. No safety-critical decision is automated.

**Horizon is not an AI black box.** Every recommendation is traceable to a documented, deterministic rule. The system is inspectable, auditable, and explainable. ML may inform future analytics layers, but the operator-facing decision-support layer remains deterministic and transparent.

---

## 11. Commercial and Delivery Model

Horizon is wholly owned, built, and operated by AMS Group through the AMSG Horizon division. There are no third-party platform dependencies, no external technology partnerships that constrain the product direction, and no inherited architectural obligations from other programmes.

The platform is positioned for delivery as an operational service — deployed, maintained, monitored, and supported by AMSG. Port authorities and operators will interact with Horizon as a managed coordination capability rather than as software they must install, configure, or maintain internally.

The delivery model supports both single-port deployments and multi-port programmes within the same platform architecture. Integration with each port's existing operational systems is scoped per engagement, respecting that every port has a different technology landscape, a different regulatory environment, and different operational priorities.

Horizon has been demonstrated to operational stakeholders across configured Australian port environments, validating the coordination model, the decision-card workflow, and the multi-port operational surface against live vessel, weather, and tidal data. The V1 programme is now building the production-grade platform — identity and access control, server-authoritative operational state, audit and replay, typed API surface, and production security posture — on top of these proven patterns.

Horizon's roadmap is driven by operational need observed in live port environments — not by technology trends, vendor partnerships, or inherited programme commitments. Capability arrives when it is operationally validated, governance-approved, and production-ready — not before.

---

*End of document.*
