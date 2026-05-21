# Horizon — Pilot Proximity Companion App (v0.1)

**Document status:** Draft for review — **future backlog item**
**Backlog priority:** Not yet ranked. Not authorised for implementation.
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT (pending)
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 5ca11db`
  (post-PR #56 — Darwin Decisions-panel pinned-slot fix)
**Date:** 2026-05-21
**Scope of authority:** This document is a **product note / spec sketch**
  for a future feature. It does **not** authorise implementation,
  architecture lock-in, vendor selection, contract negotiation, or any
  Beta 10 / horizon-v1-sandbox runtime change. It records the idea, the
  guardrails, and the open questions so that future authorisation can
  proceed from a written baseline rather than from memory.

**Authoritative inputs (all on `main`):**
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51, `b0d9ff2`)
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` (PR #43)
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md`
- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md`
- `HORIZON_CAPABILITY_BACKLOG.md`

---

## 1. Executive summary

The **Pilot Proximity Companion App** is a proposed future Horizon feature: a
read-only iPhone / iPad app (or responsive mobile view) for marine pilots
that lists the vessels likely to cross the pilot's path or enter
proximity during an inbound or outbound vessel movement.

Today, pilots get this information verbally from the VTS Operator (VTSO)
over VHF radio, creating operational noise, repeated transmissions, and
reliance on memory under pressure. A read-only companion app reduces
radio load, improves situational awareness, and gives the pilot the same
proximity picture the VTSO already has — **without replacing VTS, without
issuing navigation commands, and without diminishing the pilot's authority
or responsibility.**

This is a **decision-support surface, not a vessel-control surface.**
COLREGS, VTS instructions, harbour authority direction, and pilot
judgement remain authoritative. The app exists to make those decisions
better-informed, not to replace any of them.

**This document is a future backlog item.** It does not authorise any
code, vendor selection, fleet rollout, or contract change. M2 of the V1
programme remains the next implementation step under separate
authorisation.

---

## 2. Problem statement

During inbound and outbound pilotage movements, the pilot on the bridge
must maintain awareness of:

1. The vessel they are piloting (heading, speed, position, ETD/ETA at
   key waypoints).
2. Other commercial vessels in the channel and approaches that may cross
   or enter proximity.
3. Workboats, tugs, supply vessels, recreational craft, and other
   non-commercial traffic that may affect the movement.
4. Current and forecast environmental conditions affecting transit.

Today, items (2) and (3) are typically passed **verbally over VHF** from
the VTSO to the pilot. This produces several recurring problems:

- **Radio channel congestion** — proximity callouts compete with
  emergency, safety, and operational traffic on the same channel.
- **Cognitive load on the pilot** — the pilot must mentally track names,
  bearings, ranges, and intended movements of multiple vessels while
  also commanding the bridge team.
- **Cognitive load on the VTSO** — the VTSO must repeat the same
  proximity picture verbally for each pilotage movement, often multiple
  times during a single transit.
- **Reliance on memory** — once a callout has been transmitted, it is
  no longer on a screen for the pilot; subsequent crossing-vessel
  updates require additional radio traffic.
- **Inconsistent currency of information** — the verbal picture the
  pilot holds is only as fresh as the last transmission. Vessels can
  change course or speed between callouts.
- **No persistent audit of what was communicated** — if a near-miss
  occurs, the radio recording is the only record of what the pilot was
  told and when.

The result is more radio traffic than necessary, a thinner-than-ideal
shared situational picture, and a higher cognitive load on both the
pilot and the VTSO during the most safety-critical phase of a vessel
movement.

A read-only proximity app on the pilot's device, fed from the same data
sources the VTSO already uses, lets the VTSO transmit **only the items
that warrant verbal emphasis** (the genuinely unusual or safety-critical
ones) while the pilot retains continuous visual access to the
proximity picture for everything else.

---

## 3. Operational value

3.1 **Reduced VHF radio load on the pilotage channel.** Routine
proximity callouts can be replaced by the on-screen list. VTSO retains
discretion to call out any item considered worth verbal emphasis. Net
effect: fewer routine transmissions, more channel capacity for
exceptional or safety-critical communication.

3.2 **Continuous proximity picture for the pilot.** Instead of a series
of point-in-time radio updates, the pilot has an always-on, refreshing
list of the vessels relevant to the current movement.

3.3 **Lower cognitive load on the bridge.** Vessel names, bearings,
ranges, and intentions are visible at a glance rather than held in
memory between transmissions.

3.4 **Lower cognitive load on the VTSO.** The VTSO no longer has to
repeat the same proximity picture verbally for every pilotage movement.

3.5 **Consistent handover between shifts.** A new VTSO joining mid-shift,
or a relief pilot taking over, can see the same proximity picture the
outgoing operator saw — no verbal handover of every nearby vessel
required.

3.6 **Faster diagnosis when something goes wrong.** A snapshot of what
the pilot saw on screen at any given moment can be reviewed alongside
the radio recording, AIS log, and VTS log when investigating an
incident, near-miss, or operational dispute.

3.7 **Better post-movement debrief.** Pilots and harbour masters can
review the proximity picture for completed transits as part of routine
training, learning, and continuous-improvement activity.

3.8 **Compatibility with existing pilot workflow.** The app is a
companion, not a replacement. Pilots continue to use existing
navigation aids, ECDIS, paper charts, and VHF radio exactly as today.
The companion app sits alongside.

---

## 4. Safety value

4.1 **Earlier detection of crossing-vessel conflicts.** The proximity
list updates as AIS / VTS data refreshes. A converging track or
unexpected speed change becomes visible to the pilot at the next
refresh, not at the next radio transmission.

4.2 **Reduced single-point-of-failure on verbal communication.** If a
VHF transmission is missed, garbled, or the channel is occupied with
other traffic, the pilot still has the on-screen picture.

4.3 **Shared mental model with the VTSO.** Pilot and VTSO can look at
the same nearby-vessel set with the same identifiers, removing
ambiguity over which vessel is meant during any subsequent verbal
exchange ("the bulker at 270 bearing 1.4 NM" becomes a labelled row on
both screens).

4.4 **Persistent record for incident review.** A read-only log of the
proximity picture presented to the pilot at each minute of the transit
supports learning and accountability after the fact.

4.5 **Reduced fatigue impact.** Lower memory load and less radio chatter
reduce the cumulative fatigue effect on pilots and VTSOs across a
shift, which itself is a safety control.

4.6 **Explicit safety-disclosure UI.** Every screen of the app carries
a clear statement that the data is decision support only and does not
replace VTS instructions, COLREGS, pilot judgement, or harbour
authority direction. This is a deliberate safety control to keep the
authority model unambiguous in the pilot's view.

---

## 5. Architectural guardrails

These are **non-negotiable** for any implementation of this feature.
They reflect the Horizon Independence Reset (PR #51, `b0d9ff2`) and the
Horizon V1 Component & Interaction Canon (PR #43).

5.1 **Horizon is not a VTS replacement.** This feature is a coordination
and decision-support layer. The official VTS remains the authority for
traffic management in the port and approaches. The app must not present
itself, or be marketed, as a VTS.

5.2 **Horizon must not issue navigation commands.** The app shows
information. It does not direct the pilot to alter course, alter
speed, alter heading, hold, or proceed. Commands remain the prerogative
of the pilot (on advice of the bridge team and in coordination with
VTS).

5.3 **The pilot retains authority and responsibility.** The pilot is in
command of the vessel for the pilotage movement. The app is advisory
input only; the pilot is free to disregard, override, or contradict any
information the app shows. The UI must make this authority unambiguous.

5.4 **VTS remains the official traffic management authority.** Any
contradiction between VTS instruction and the app must be resolved in
favour of VTS instruction. The app must not undermine, second-guess,
or appear to override VTS direction.

5.5 **COLREGS authority is preserved.** International Regulations for
Preventing Collisions at Sea (COLREGS) and any applicable local rules
(Marine Act, Port Notices) remain the controlling rule set. The app
must not present an algorithmic recommendation as if it superseded
COLREGS.

5.6 **Read-only by construction.** The app does not allow the pilot
(or anyone) to enter, change, acknowledge, dispatch, commit, defer,
override, or escalate any operational action through it. This mirrors
the M1 / M2 read-only posture of the V1 frontend programme.

5.7 **No fake or synthetic data in production.** Every value displayed
must be sourced from an identifiable data feed (AIS, VTS, BoM, etc.)
or be marked clearly as derived/estimated. No simulated vessels in
production pilot use.

5.8 **Safety-disclosure on every screen.** A persistent banner or
footer states: *"Decision support only. Does not replace VTS
instructions, COLREGS, pilot judgement, or harbour authority
direction."*

5.9 **De-identified / resource-safe data patterns where pilot or
crew identity is involved.** Pilot identity, certificate number,
roster number, and any personally identifying information are
held only to the extent operationally necessary, encrypted in
transit and at rest, and never displayed unnecessarily.

5.10 **Independence Reset framing.** Horizon is AMSG-owned. The app
does not name, depend on, or imply Smart Ocean X.

5.11 **No write path to Beta 10 or any production backend.** The app
consumes a read API. It does not POST. It does not emit audit. It
does not mutate any port state.

5.12 **Auditable read-only log.** While the app does not mutate state,
it does retain a read-only log of what was presented to the pilot
during a movement (subject to data-retention policy and pilot /
operator consent). This is an audit / safety control, not an
operational control.

5.13 **Reduce radio load — do not bypass VTS.** The intent is to
make VTS callouts more selective, not to make them unnecessary. The
app does **not** replace any required pilot ↔ VTS communication
mandated by local regulation, port handbook, or VTS service
agreement. Mandatory radio reports (e.g. arrival at boarding ground,
pilot embarkation, port entry, berth approach) continue exactly as
today.

5.14 **Failure-safe defaults.** If the app loses data or feed
freshness, it must visibly degrade (stale-data banner, last-updated
timestamp, source indicator) rather than silently present old data
as current. If it loses all data, it must show a clear "no
proximity data — revert to VHF" state.

5.15 **No dependency on the M1 / M2 fixture pipeline for production
use.** The V1 fixture-fed pipeline is for demo and operator-facing
read-only surfaces — not for live pilot safety information. Pilot
production use requires a real live feed (which is a separate
implementation milestone).

---

## 6. Feature scope — what the app shows

This is the **proposed information set** for the pilot's view. Final
content and layout will be designed against pilot workflows during the
scoping milestone (see §13 status & next steps).

### 6.1 Subject vessel (the vessel being piloted)
- Vessel name
- MMSI / IMO (where available)
- Current position (lat/lon and human-readable bearing/range from a
  reference point, e.g. boarding ground or berth)
- Heading and course
- Speed Over Ground (SOG) and Speed Through Water (STW where available)
- Current waypoint or transit phase (e.g. "Approach", "Channel",
  "Inner harbour", "Berth approach")
- Estimated time at the next pilot-relevant waypoint

### 6.2 Nearby / crossing vessels
For each vessel on the list:
- Vessel name (and AIS-broadcast name where it differs)
- MMSI / IMO (where available)
- Vessel type / cargo class (e.g. container, bulk carrier, tug,
  workboat, recreational)
- Current position
- Heading and course
- Speed (SOG)
- **CPA** (Closest Point of Approach, distance) — or simplified
  proximity-risk band if CPA is not calculable
- **TCPA** (Time to Closest Point of Approach) — or "—" if not
  calculable
- Expected crossing or interaction point (waypoint name, lat/lon, or
  "no crossing" if tracks diverge)
- Interaction class — **ADVISORY** (no action required, awareness only),
  **CAUTION** (monitor, may require communication), or **CONFLICT**
  (immediate VTS/VHF action recommended; pilot retains authority)
- Last updated timestamp (per-vessel)
- Source of data (AIS, VTS, derived, etc.)
- Optional: a 1-line operational note (e.g. "northbound, conducting
  pilotage with PILOT 03").

### 6.3 Environmental context strip (read-only)
- Wind (speed, direction)
- Tide (height, set/drift if available)
- Visibility (if known)
- Significant weather warnings active in the port
This is informational; it does not change the proximity logic on its
own.

### 6.4 Safety disclosure (persistent)
A persistent banner or footer states (verbatim or substantively
equivalent):
> *Decision support only. Does not replace VTS instructions, COLREGS,
> pilot judgement, or harbour authority direction.*

### 6.5 Status indicators (persistent)
- Connection status (online / degraded / offline)
- Data-feed freshness (e.g. AIS last updated 12 s ago)
- Auth state (logged-in pilot identifier)
- App version

### 6.6 Explicitly excluded from the pilot's view
- Any "click to ACK / COMMIT / DEFER / OVERRIDE / ESCALATE" button
- Any free-text input or upload
- Any UI that suggests the app is issuing a command to a vessel
- Any presentation of derived recommendations as if they were COLREGS
  rules
- Any presentation as a VTS-equivalent traffic-management surface

---

## 7. Data inputs required

7.1 **AIS (Automatic Identification System) feed.**
- Source candidates: AISStream (already configured for Beta 10),
  MST AIS (already integrated), or a port-authority-owned feed.
- Required fields: MMSI, name, lat/lon, COG, SOG, heading, vessel type,
  navigational status, last-updated timestamp.

7.2 **Port operational data.**
- Berth assignments and ETA/ETD for vessels currently inbound or
  outbound — sourced from the Horizon `ViewSummary` shape (see
  Adapter Design Note §2.2) or a downstream slice of it.
- Pilotage assignments — which pilot is on which vessel, plus
  movement direction (inbound / outbound) and timing.

7.3 **VTS reference data.**
- Channel and approach segmentation (waypoint definitions, channel
  geometry).
- Port-specific speed zones, VTS sectors, exclusion zones, anchorages.

7.4 **Environmental data.**
- BoM (or equivalent local bureau) tide and wind for the active port.
- Visibility / fog observations where available.

7.5 **Derived data (computed server-side, not at the device).**
- CPA / TCPA between subject vessel and each nearby vessel.
- Expected crossing point along the planned pilotage path (using
  channel waypoints from the port profile).
- Interaction-class classification (ADVISORY / CAUTION / CONFLICT)
  using a deterministic, documented rule set — never an opaque ML
  black box.

7.6 **Identity and access data.**
- Authenticated pilot identifier (mapped to a roster).
- Active movement assignment (which pilotage event is the pilot on).

### Data sourcing principles

- Wherever possible, reuse existing Horizon backend data sources rather
  than introduce new feeds.
- Where a port-authority feed exists, prefer it over a third-party
  aggregator.
- Where derivation is required (CPA, TCPA, crossing point), the
  algorithm must be documented, deterministic, and reviewable. No
  opaque ML predictions in v1.
- All data input documentation must comply with the Independence Reset
  framing (Horizon-owned, AMSG-controlled).

---

## 8. MVP version

The MVP is the **smallest implementation that delivers operational
value to a single pilot, in a single port, under controlled
conditions.** It is intentionally narrow.

### 8.1 MVP target
A read-only iPad mobile view (responsive web view, not yet a native
app) for **one pilot at a time**, **one port at a time** (initial
candidate: **Port of Brisbane**, given the strongest Horizon data
maturity there), serving:
- Subject vessel block (§6.1)
- Nearby / crossing vessels list (§6.2) — capped at the top N closest
  vessels by range or TCPA
- Environmental context strip (§6.3) — wind, tide, visibility
- Persistent safety disclosure (§6.4)
- Status indicators (§6.5)

### 8.2 MVP constraints
- iPad responsive web view; not a native iOS app yet
- Single port (Brisbane) initially
- Single active pilot at a time per device
- Read-only end-to-end; no inputs of any kind from the pilot
- Auth gate by pilot identifier (re-uses an existing identity model if
  possible)
- Auto-refresh on a fixed interval (probably 5–10 s) plus pull-to-refresh
- Stale-data banner when the feed is older than a configurable
  threshold (e.g. 30 s)
- Data sources: existing AIS source(s), existing port profile, existing
  BoM feed; no new feed introduced in MVP
- Internal use only (pilot crew within the participating port) —
  **not a public release**

### 8.3 MVP success criteria
- A pilot can open the view at the start of a movement, identify the
  subject vessel and the relevant nearby vessels, and use the
  CPA / TCPA / interaction-class display in real or simulated
  pilotage exercises.
- A VTSO can corroborate that the proximity picture the pilot sees
  matches the VTSO's view of the same data, to within feed-freshness
  tolerance.
- Pilots and VTSOs report (qualitatively, in structured debrief) that
  the app reduced routine VHF callouts without degrading safety
  awareness.
- No incident, near-miss, or operational complaint traceable to the
  app during the MVP trial.
- Independence Reset framing honoured in all artefacts (UI, docs,
  comms).

### 8.4 MVP non-goals
- Native iOS / iPadOS app shell
- Multi-port simultaneous use
- Multi-pilot per device
- Push notifications
- Offline mode
- Apple Watch / wearable extension
- Integration with bridge ECDIS systems
- Replacement of any mandatory VHF reporting
- VTS-equivalent traffic-management features
- Any write-path or operator-action endpoint
- ML-driven proximity prediction

---

## 9. Future version (V2+)

Items below are explicitly **out of scope for MVP** and represent the
proposed evolution path. Each must be authorised separately when /
if it is taken on.

9.1 **Native iOS app shell** — App Store distribution, code signing,
MDM (Mobile Device Management) compatibility for port authorities.

9.2 **iPad split-screen / multi-window support** so the pilot can run
the app alongside ECDIS or other tools.

9.3 **Multi-port support** — Melbourne, Geelong, Darwin (the existing
Horizon port set), and beyond.

9.4 **Multi-pilot per device** — for VTS supervisory roles or training
scenarios.

9.5 **Offline / degraded-link mode** — last-known-good cache with a
clear staleness indicator; explicit "revert to VHF" state when the
cache is too stale to be safe.

9.6 **Push notifications** for new CAUTION or CONFLICT-class
interactions, gated by pilot consent and configurable per-pilot.

9.7 **Wearable extension** (Apple Watch) for a glanceable nearby-vessel
count and the most-imminent interaction. Strict no-command, no-write,
read-only design.

9.8 **Pilotage path overlay** — show the planned channel transit path
with waypoints, so the pilot can see proximity vessels relative to the
planned path as well as to current position.

9.9 **Historic playback** for post-movement debrief — replay the
proximity picture the pilot saw during the movement, with VHF audio
and AIS overlay.

9.10 **Integration with bridge systems** (ECDIS) — read-only data
out, never input. Subject to bridge-equipment certification regimes.

9.11 **VTSO companion view** — a complementary VTSO-facing app or
panel that mirrors the same proximity picture from the VTSO's
perspective, so pilot and VTSO are demonstrably looking at the same
data.

9.12 **Multi-language UI** for ports with international pilots /
crew.

9.13 **Configurable interaction-class thresholds** per port, per
pilot organisation, or per regulator — to align with local rules and
risk appetite.

9.14 **Statistical / operational reporting** — aggregate dashboards
for harbour masters showing radio-call reduction, near-miss
exposure, and operational learning over time. (Aggregated; not
pilot-identifying.)

9.15 **Open standards interop** — if a future maritime open standard
emerges for proximity-decision-support data exchange, support that
standard.

---

## 10. Risks and governance considerations

10.1 **Risk: pilot or VTSO over-reliance on the app.** If pilots come
to depend on the app, a feed outage or device failure could degrade
safety below the current radio-only baseline.
**Mitigation:** persistent safety disclosure; visible stale-data
banner; explicit "revert to VHF" state; training; the VHF channel and
mandatory radio reports are unchanged; failure-safe UI defaults
(§5.14).

10.2 **Risk: legal / regulatory uncertainty.** Pilotage is governed by
state Marine Acts, port-authority handbooks, COLREGS, and pilot
licensing regulators. A decision-support app could be construed as
inducing reliance, creating liability, or operating in a regulated
domain that requires certification.
**Mitigation:** §5 architectural guardrails explicitly preserve
existing authority models; legal review required before any MVP
deployment; engage with relevant pilotage licensing bodies; align
with local Marine Act and port handbook; document the app's
status (decision support; not a VTS; not a navigation system).

10.3 **Risk: data accuracy and freshness.** AIS data can be stale,
missing, spoofed, or wrong (e.g. AIS off, wrong vessel-type tag).
Showing inaccurate data with an apparent UI confidence could mislead
the pilot.
**Mitigation:** per-vessel last-updated timestamp; explicit data
source per vessel; stale-data thresholds; conservative
interaction-class classification; visible degraded-feed UI; the
disclosure that this is decision support only and does not replace
COLREGS / VTS / pilot judgement.

10.4 **Risk: VTS relationship.** Introducing a parallel proximity
view could be perceived by VTS providers as an erosion of their
authority or scope.
**Mitigation:** §5.4 — VTS remains the official traffic-management
authority; the app is positioned and marketed as a pilot-side
companion that reduces radio load to free VTS capacity; develop
the feature **with** VTS providers as design partners, not in
isolation; provide a companion VTSO view (future item §9.11) so
the same data is shared.

10.5 **Risk: cyber-security and data integrity.** A compromised feed
or an MITM attack on the app could feed the pilot misleading
information.
**Mitigation:** HTTPS, certificate pinning where feasible, signed
data payloads from server to device, authenticated pilot session,
and a clear failure mode if the integrity check fails. Standard
Horizon security posture extended to mobile.

10.6 **Risk: identity and personal data.** Pilot identity, roster,
and certificate number are personally identifying.
**Mitigation:** §5.9 — de-identified / resource-safe patterns;
minimum-necessary collection; encryption in transit and at rest;
no display of personally identifying data beyond what is
operationally required; data-retention policy aligned with
pilot-organisation contracts and applicable privacy law.

10.7 **Risk: device-management policy mismatch.** Port authorities
and pilot organisations may have MDM, BYOD, or device-management
policies that constrain installation, updates, and audit-log
exfiltration.
**Mitigation:** consult MDM policies before MVP rollout; support
MDM-compatible distribution paths from V2 (native app) onward;
ensure read-only logs comply with the host organisation's
retention policy.

10.8 **Risk: insurance and indemnity.** Maritime insurance contracts
may treat new decision-support tools in subtle ways.
**Mitigation:** legal review with maritime insurance specialists
before MVP deployment; align with the AMSG insurance posture for
the broader Horizon platform.

10.9 **Risk: misalignment with the Horizon Independence Reset.**
A future contributor, vendor, or partner might propose framing this
feature as Smart Ocean X-derived. That framing is forbidden under
PR #51.
**Mitigation:** all design, marketing, contractual, and code
artefacts must use neutral framing — "Horizon" / "AMSG Horizon" /
"Horizon by AMS Group" — per Independence Reset §13 / §14.

10.10 **Risk: scope creep into navigation commanding.**
A future stakeholder might request "just one button" to ACK, COMMIT,
or notify a vessel. Doing so would breach the architectural
guardrails (§5.2, §5.6) and turn the feature into something it
must not become.
**Mitigation:** any proposal to add an operator-action path
triggers a stop condition; requires a separate strategic /
governance authorisation (not just a sprint ticket); §5 guardrails
must be re-affirmed in writing before any change.

10.11 **Risk: marine-environment usability.** Pilot bridge environments
include glare, vibration, rain, salt, varying lighting, and
significant motion. UIs that work in an office can fail at sea.
**Mitigation:** field testing on real bridges in varying conditions
before any wider rollout; high-contrast UI; large touch targets;
no fine gestures; voice-friendly status indicators.

10.12 **Risk: pilot association acceptance.** Pilot organisations are
historically (and rightly) cautious about technology that affects
their work.
**Mitigation:** co-design with at least one pilot organisation as a
design partner from the start; pilot association sign-off as a
prerequisite for MVP deployment; no roll-out without that sign-off.

10.13 **Risk: aviation-style cockpit-management mistakes.** Cockpit
displays in aviation have well-documented failure modes when too
much information is shown or when displays compete for attention.
**Mitigation:** information design discipline (top N nearby
vessels; clear visual hierarchy; suppression of low-interaction-class
items); usability research with pilots before V2.

10.14 **Governance: this document does not authorise implementation.**
Implementation requires separate scoping (Pilot Proximity scope
proposal), planning (Pilot Proximity implementation plan), and
Tony's explicit authorisation, mirroring the M0 / M1 / M2 governance
pattern that has worked well to date.

10.15 **Governance: this is not a V1 milestone.** This feature is a
**future platform capability** sitting outside the current V1
programme. M2 (read-only V1 surface expansion) is the next V1
milestone and is itself not yet authorised for implementation.
The Pilot Proximity Companion App is downstream of V1 completion.

---

## 11. Open questions for maritime SMEs

The following must be answered (by pilots, harbour masters, VTSOs,
maritime regulators, and pilot organisations) before the MVP scope
is finalised:

11.1 What is the **highest-value first port** for the MVP — Brisbane,
or somewhere else? Brisbane has the strongest Horizon data maturity;
other ports may have higher safety or radio-load pain.

11.2 What is the **minimum useful refresh rate** for the proximity
view in real pilotage conditions? Every 5 s? 10 s? Per-AIS-update?

11.3 What is the **stale-data threshold** beyond which the pilot
should be told to revert to VHF only? 30 s? 60 s? Should it vary
by traffic density?

11.4 What is the **right top-N cap** for nearby vessels — 5, 10,
20? Should it vary by transit phase (approach vs inner harbour)?

11.5 What is the **right CPA / TCPA threshold** for ADVISORY,
CAUTION, and CONFLICT classes? Should it be configurable per port
or per pilot organisation?

11.6 Which **vessel classes** should the proximity list include by
default — only commercial ≥ a certain LOA, or also workboats and
recreational craft? Does it vary by port?

11.7 What **mandatory radio reports** must remain unchanged in every
port (boarding-ground arrival, embarkation, port entry, berth
approach, departure, anchor)? The app must NOT replace these.

11.8 What is the **pilot identity / authentication** model? Does the
pilot log in to a dedicated pilot account, use a port-authority SSO,
or use a pilot-organisation SSO?

11.9 What is the right **data-retention policy** for the read-only
movement log — minutes, hours, days? Who owns the log? Who can
access it? Under what legal basis (incident investigation,
training, audit)?

11.10 Is there a **pilot organisation** (or a small group) willing to
co-design the MVP and run a structured trial? If not, the feature
should not begin.

11.11 Is there a **VTS provider** willing to co-design the VTSO
companion view (§9.11) as a parallel track, or at least be a
named design partner for the pilot view?

11.12 What is the right **device baseline** — pilot-owned iPad,
pilot-organisation-issued iPad, port-authority-issued iPad? Different
devices imply different MDM and security postures.

11.13 What **bridge integration** is acceptable in V2+ — read-only
output to ECDIS via a documented protocol? Or strictly device-only?

11.14 What is the **legal framing** for the app under each
applicable jurisdiction (Australian state Marine Acts; international
COLREGS regime)? Does it qualify as a "navigation aid" requiring
certification, or as decision support sitting outside that regime?

11.15 What is the **commercial model** — per-port subscription,
per-pilot licence, port-authority enterprise deal? This is a
strategic question that affects scope, packaging, and integration
choices.

11.16 What **competing products** exist (e.g. specialist pilot apps,
maritime ECDIS extensions, VTS-vendor offerings)? Where does this
feature differ from / complement them?

11.17 Does any **safety-management system** (port-authority SMS,
pilot-organisation SMS) need to be updated before MVP deployment?

11.18 What **near-miss / incident data** could justify or contradict
the operational value claims in §3? Is there a pilot organisation
willing to share anonymised data for that assessment?

---

## 12. Relationship to current Horizon V1 work

12.1 **Out of scope for V1.** The V1 programme (M0, M1, M2) is the
operator-facing read-only dashboard for harbour masters, VTSOs,
shift supervisors, and port executives. The Pilot Proximity
Companion App is a separate downstream platform capability targeted
at pilots, not operators.

12.2 **Reuses V1 data layer concepts.** The `ViewSummary` shape and
the adapter pattern from `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md`
are conceptual reference points for how Horizon serves operational
read-only data; the Pilot Proximity feature would extend that pattern
with proximity-specific derivations rather than invent a new pattern
from scratch.

12.3 **Honours the same architectural guardrails.** Read-only;
fixture-fed during development; no Beta 10 production touch; no
audit emission; no DB dependency; no Stage E-prod; no Smart Ocean X
framing. These are the same boundaries that have produced clean M0,
M1, and demo-day fixes for Beta 10.

12.4 **Independent governance.** This feature is governed under its
own scope proposal → implementation plan → implementation →
retrospective cycle, mirroring the V1 milestone pattern. It does
not unblock or block M2.

12.5 **No M1 / M2 fixture changes.** This document does not propose
any change to `frontend/public/fixtures/*.json` or any V1 frontend
file.

12.6 **No Beta 10 runtime change.** This document does not propose
any change to `server.py`, `port_profiles.py`, the audit helpers,
the scrapers, BoM, or root `railway.toml`.

12.7 **`horizon-v1-sandbox` is the future-platform experimentation
environment** (per the post-demo state-alignment update). If, in
future, any prototype of this feature is built, it begins on a
sandbox branch — never directly on Beta 10.

12.8 **Pre-demo freeze.** Beta 10 runtime is frozen until after the
Darwin Ports demo. This document explicitly does not break that
freeze.

---

## 13. Status and next steps

**Status: Future backlog. Not authorised for implementation.**

This document is the v0.1 product note. It captures the idea, the
guardrails, and the open questions. It is **not**:
- a scope proposal
- an implementation plan
- a contract with any pilot organisation
- a vendor selection
- an architecture lock-in
- a marketing commitment

### Suggested governance sequence (when this feature is taken on)

1. **Add to `HORIZON_CAPABILITY_BACKLOG.md`** with explicit "future"
   ranking and a link to this document.
2. **Maritime SME discovery** — answer the §11 open questions with
   pilots, harbour masters, VTSOs, and pilot-organisation
   representatives.
3. **Pilot-organisation design partner** — confirm at least one
   pilot organisation willing to co-design and trial the MVP
   (§11.10).
4. **Legal and regulatory review** — assess against state Marine
   Acts, port handbooks, COLREGS framing, and maritime insurance
   posture (§10.2, §10.8).
5. **Scope proposal PR** — `HORIZON_PILOT_PROXIMITY_COMPANION_APP_SCOPE_PROPOSAL_v0.1.md`,
   mirroring the V1 M2 Scope Proposal pattern (PR #53). Includes
   resolved MVP scope, port choice, data sources, success criteria,
   risks. Requires Tony's explicit authorisation.
6. **Implementation plan PR** — concrete plan with file scaffold,
   API surface, mobile delivery mechanism (responsive web view
   vs. native), tests, and acceptance criteria. Requires separate
   explicit authorisation.
7. **Implementation** — on a dedicated branch, in `horizon-v1-sandbox`
   (or a new dedicated sandbox), never directly on Beta 10. Requires
   separate explicit authorisation.
8. **Pilot-organisation trial** — structured field trial with named
   pilots, defined success criteria, and a published debrief.
9. **Retrospective PR** — at trial close, mirroring the V1 M1
   Retrospective pattern (PR #52).

### Nothing in this document authorises any of those steps.

The next action item for this document is **review by Tony**, and
optionally by an external maritime SME for sanity-check. No further
work proceeds without explicit authorisation.

---

## End of Pilot Proximity Companion App v0.1 product note

Confirmed by this document:
- This is a **future backlog product note / spec sketch**.
- Implementation is **not** authorised by this document.
- The feature is **decision support only** — Horizon does not issue
  navigation commands, does not replace VTS, and does not erode the
  pilot's authority or responsibility.
- VTS, COLREGS, harbour authority direction, and pilot judgement
  remain authoritative.
- The feature is read-only by construction; no write path, no audit
  emission, no Beta 10 mutation.
- The Smart Ocean X architectural dependency remains **formally
  closed** under the Independence / Architecture Reset (PR #51,
  `b0d9ff2`).
- The Horizon V1 M2 milestone remains the next implementation step
  under separate authorisation. This feature sits downstream of V1
  completion.

**Next action:** ChatGPT engineering review (optional at this stage),
Tony's review, and — if approved — addition to
`HORIZON_CAPABILITY_BACKLOG.md` as a future item. Implementation
authorisation is a separate, later, explicit decision.
