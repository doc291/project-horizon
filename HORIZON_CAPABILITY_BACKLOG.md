# Horizon Capability Backlog

This document captures Project Horizon's operational capabilities, product
concepts, regulatory boundaries and coordination philosophy independently
of their implementation status. Entries may describe live features,
roadmap items, future operational models or product concepts under
consideration; presence in this document does not imply that any entry
is implemented, scheduled, or contractually committed. The purpose is to
preserve operational intent, prevent terminology drift, maintain
regulatory positioning, and provide a stable reference point across
sessions and contributors so that implementation work — when it
happens — is governed against a clearly stated capability definition
rather than reconstructed inference.

Each entry uses the same structural template: operational problem,
Horizon's role (including what Horizon explicitly is NOT), user roles,
inputs, outputs, decision authority, operational value, commercial
value, regulatory sensitivities, technical notes, and UI concepts.
Entries are versioned by an `HC-NNN` identifier; the identifier is
permanent once issued, even if the underlying capability is later
deprecated or merged into another entry.

---

# HC-001 — Pilot Coordination Awareness Module (PCAM)

## Operational Problem
Pilots currently receive encounter, sequencing and transit coordination information verbally over VHF from VTS operators. This information is often transient, memory-based and not formally structured or recorded. Operational awareness depends heavily on timing, interpretation and operator recall.

## Horizon Role
Horizon provides predictive coordination awareness and shared operational context during vessel transit operations.

Horizon does not:
- provide navigational control
- replace radar or ECDIS
- issue vessel movement instructions
- act autonomously
- replace licensed maritime authority

The module exists to improve coordination awareness and operational coherence across pilots, VTS and port stakeholders.

## User Roles
- Pilot
- VTS Operator
- Harbour Master
- Towage Coordinator
- Port Operations Coordinator

## Inputs
- AIS vessel positions
- Vessel movement plans
- Channel restrictions
- Tide state
- Weather conditions
- Pilot assignments
- Tug allocation
- Port operational rules
- Active transit sequencing

## Outputs
- Predicted encounter list
- Sequencing awareness
- Transit conflict indicators
- Coordination recommendations
- Shared operational context
- Timestamped coordination events
- Replayable coordination timeline

## Decision Authority
Horizon provides operational guidance only.

Final navigational authority and movement responsibility remain with licensed maritime officers and relevant port authorities at all times.

## Operational Value
- Reduced coordination ambiguity
- Shared situational awareness
- Reduced dependence on verbal relay and memory
- Earlier awareness of transit conflicts
- Replayable operational coordination history
- Improved operational consistency

## Commercial Value
- Enhanced operational safety
- Improved auditability
- Reduced coordination risk
- Stronger regulatory defensibility
- Foundation for future SAP-style operational support
- Differentiated operational capability for ports

## Regulatory Sensitivities
- Must remain advisory
- Must not present as autonomous navigation
- Must not replace navigational systems
- Must maintain clear human authority boundaries
- Requires strong auditability and event traceability

## Technical Notes
Future implementations should align with:
- event-driven coordination architecture
- movement-level state modelling
- immutable coordination events
- replayable operational timelines
- real-time shared operational state
- deterministic coordination logic before AI augmentation

## UI Concepts
Potential future interface concepts include:
- encounter awareness list
- shared transit view
- coordination event timeline
- conflict heatmap
- passing sequence indicators
- acknowledgement workflow

---

# HC-002 — VTS Spatial Operating Surface

## Operational Problem
VTSO users require spatial situational awareness — vessel positions,
movement vectors, proximity context and traffic relationships across
the port and approaches — to coordinate movements safely. A list-based
VTS view conveys vessel identity and conflict membership but does not
convey *where* the traffic is, *which way* it is moving, or *how close*
vessels are converging. For VTSO workflows, spatial context is
essential, not optional.

## Horizon Role
Horizon's VTS surface should evolve into a map-based / spatial
operating surface that shows the live operational vessel picture
within the operator's port scope, with vessel positions, movement
vectors, predicted paths, conflict overlays, channel geometry,
tidal / channel-depth context, and operational state. This is the
long-term centre-spine default for coordination users per the
Operational UX Direction §3.2.

Horizon does not:
- replace the official Vessel Traffic Service authority
- provide vessel-traffic command capability
- issue navigation instructions to vessels
- replace radar or ECDIS on the bridge
- act autonomously
- replace licensed maritime authority

The VTS surface exists to improve coordination awareness and shared
operational context across VTSOs, Harbour Masters and Pilotage
Coordinators within their authorised port scope.

## User Roles
- VTSO (primary)
- Harbour Master
- Shift Supervisor
- Pilotage Coordinator
- Towage Coordinator

## Inputs
- AIS vessel positions (AISStream / MST / port-authority direct feed)
- Vessel master records (name, type, dimensions)
- Movement records (inbound / outbound / shift; ETA / ETD)
- Berth / channel geometry from port profile
- Pilotage and towage assignment context
- Weather and tidal conditions
- Conflict and recommendation state from the coordination layer

## Outputs
- Spatial vessel view scoped to the operator's port context
- Vessel position markers with name / type / status
- Movement-vector indicators (heading and speed)
- Proximity / CPA overlays where computed
- Conflict overlays joining the spatial view to the right-rail
  decision-card list
- Channel and berth geometry layer
- Tidal / depth / restricted-zone overlays
- Stale-data and degraded-feed indicators (per the Platform
  Foundation §9 freshness / confidence requirements)

## Decision Authority
- Horizon presents spatial context; the operator decides
- VTS remains the authoritative traffic-management surface in the port
- COLREGS authority and pilot judgement remain authoritative for
  vessel handling

## Operational Value
- Faster shared situational awareness across VTSO, Harbour Master and
  coordinators
- Reduced VHF callout load for routine spatial-context updates
  (parallel to the value case for the Pilot Proximity App, HC-001)
- Better post-movement debrief and incident review (spatial replay
  joined to the audit ledger)
- Improved handover between shifts (the spatial picture is visible,
  not held in memory)

## Commercial Value
- Differentiated VTSO-facing surface vs. list-only platforms
- Operationally credible co-pilot for port authorities
- Foundation for future integrated coordination features (Pilot
  Proximity App integration, pilotage queue spatial view, terminal-
  berth approach overlays)

## Regulatory Sensitivities
- Must remain decision support, not vessel-traffic command
- Must not present as a VTS substitute to operators, customers or
  regulators
- Must respect each port's existing VTS provider relationship; where
  an integrated VTS provider feed is available, prefer it; where it is
  not, the AIS-derived spatial layer must be explicitly tagged as
  derived
- Must respect data-residency requirements per port / per customer
- Spatial accuracy disclaimers required (AIS position uncertainty,
  feed staleness)

## Technical Notes
- The VTS view should ultimately leverage an integrated VTS provider
  feed where available
- Where no integrated VTS provider exists, Horizon may build a
  spatial layer on top of AIS / vessel-feed data
- This is M3+ (or later) scope — see Platform Foundation (PR #68)
  open questions §15.1.3 (capability arrival sequence). The M2 VTS
  tab (PR #61) is a read-only list precursor; the M2 `vtsAdapter`
  contract is forward-compatible with a spatial replacement
- Map / charting library selection is deferred and would be a
  separate scope-proposal decision (Platform Foundation §12.6 — no
  framework lock-in from the foundation document)
- Server-authoritative for proximity / CPA / TCPA calculations
  (Platform Foundation §7.7 — no frontend business-rule ownership)
- Polling for bulk vessel state plus SSE / WebSocket for
  time-critical proximity events is a candidate pattern (Platform
  Foundation §7.5–§7.6)

## UI Concepts
- Map / spatial canvas as the centre-spine VTS tab body
- Vessel markers with hover / focus state surfacing identity, type,
  status, conflict membership
- Movement vectors (heading / SOG indicators)
- Conflict highlight joining spatial view to the right-rail decision
  card
- Channel geometry, berth markers, anchorages, pilot boarding ground
- Tidal / channel-depth overlay (toggle)
- Stale-data banner and source-attribution chip
- No map editing, no navigation instructions, no vessel commanding
- No port switcher in the VTS surface — port scope is set by login /
  user permissions (Platform Foundation §8 multi-port tenancy)
- Decision-support disclosure copy explicit on the surface

## Status
Future capability — not authorised for implementation. Captured here
to preserve operational intent. Will require its own scope proposal
+ implementation plan + Tony authorisation, mirroring the M2 / M3+
governance pattern.

---
Future Horizon capability concepts will be appended to this document progressively as the operational model evolves.
