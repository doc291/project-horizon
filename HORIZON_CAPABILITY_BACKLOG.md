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
Future Horizon capability concepts will be appended to this document progressively as the operational model evolves.
