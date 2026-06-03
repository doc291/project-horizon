# Terminal Readiness Perspective Architecture
## v1.0 — Governing Reference

**Classification:** Confidential
**Owner:** Tony Trajceski (AMSG Horizon Division)
**Effective date:** June 2026
**Supersedes:** None (new governing reference)
**Applies to:** All future Terminal-related work including perspective design, data contribution models, stakeholder engagement and commercial positioning
**Related documents:**
- PORT-READINESS-DISTRIBUTION-ARCHITECTURE.md
- BETA-12-READINESS-INTEGRATION-ARCHITECTURE.md
- LENS-CONTRACT.md
- Product Doctrine
- Stakeholder Value Exchange Architecture

> This document governs the Terminal stakeholder relationship with the
> Port Readiness Signal. It defines what Horizon can and cannot provide to
> terminals, how terminals contribute to and consume the Signal, what should
> be built, when, and what must be prepared now. Where a proposed terminal
> feature conflicts with this document, this document prevails until
> formally amended.

---

## 1. Executive Summary

The terminal is the most commercially important and the most operationally difficult stakeholder in the Horizon ecosystem.

It is the most commercially important because the terminal's data (predicted cargo completion time, berth-ready signal) is the single input that transforms the Port Readiness Signal's berth readiness component from schedule-based approximation to ground-truth assessment. That transformation is what makes the Signal commercially compelling for shipping lines, because berth readiness is the component shipping lines care about most. Without terminal data, the Signal carries a permanent qualification on its most important component. With terminal data, the Signal becomes the product shipping lines will pay for.

It is the most operationally difficult because the terminal's primary concerns are internal (crane productivity, labour, equipment, yard operations), its data sensitivity is the highest of any stakeholder (completion times expose operational performance), and its product pull from Horizon is the weakest (Horizon addresses a secondary concern, not the terminal's primary operational anxieties). The terminal will not be won by product alone. It will be won by ecosystem credibility, port authority advocacy, and a visible value exchange that the terminal can verify inside the product itself.

This document establishes two governing positions:

The terminal perspective is not a terminal operations product. Horizon must never attempt to replace, replicate, or compete with a Terminal Operating System. The terminal's operational world is their TOS. Horizon provides one thing the TOS cannot: live arrival confidence based on the actual vessel, not the schedule.

The terminal perspective should be prepared now but built later. The preparations (berth readiness provenance labelling, capability notes, handover gap calculation, schedule accuracy measurement) are embedded in existing readiness work at zero additional engineering cost. The build (a thin, berth-centric readiness view) should follow the successful proof of towage and pilotage integration, when ecosystem credibility makes the terminal conversation viable.

---

## 2. Core Principle

### Terminal is berth-centric

A terminal operations manager thinks in berths. Each berth has a current operation (the vessel alongside now), a predicted handover (when the current operation finishes and the berth is clear), and a queue (the next vessel, and the one after that). Their day is a sequence of handovers: finish one vessel, clear the berth, receive the next.

Every piece of intelligence Horizon provides to the terminal must be anchored to a specific berth. Fleet-wide readiness posture, port-wide conflict lists, and multi-stakeholder coordination views are not the terminal's concern. Their concern is: for each of my berths, is the next handover going to work?

### Horizon is not a TOS

A Terminal Operating System manages the terminal's internal operations: crane scheduling, gang allocation, yard management, equipment dispatch, container tracking, vessel stow plan execution, cargo documentation, and performance reporting. These are the terminal's primary operational tools and primary daily concerns.

Horizon has no visibility into any of these. It has no data about crane rates, labour availability, equipment status, yard capacity, or cargo progress. It must never attempt to simulate, estimate, or display any of these, because doing so would produce unreliable assessments that the terminal operations manager would immediately recognise as wrong, destroying trust before it is established.

Horizon's boundary is the terminal's wall. Outside the wall (vessels approaching, tidal windows, pilot and tug availability, weather), Horizon has intelligence. Inside the wall (cargo operations, equipment, labour), the TOS has intelligence. The terminal perspective is the place where outside-the-wall intelligence is delivered to the terminal in terms they understand, without pretending to see inside the wall.

### Horizon provides arrival confidence and berth handover confidence

Two assessments, both derived from the Port Readiness Signal, both expressing readiness in terminal terms.

Arrival confidence: when is the next vessel actually arriving at this berth? Based on the vessel's AIS track, the tidal window for its draft, the weather, the pilot and tug availability, and any navigational constraints. This is intelligence the TOS cannot produce because the TOS doesn't track vessels at sea, doesn't calculate tidal windows, and doesn't see pilot or tug availability.

Berth handover confidence: will the handover between the current vessel and the next vessel work? Based on the estimated berth clearance time (from the terminal's own completion forecast if contributed, or from the scheduled ETD if not) compared to the next vessel's arrival confidence. This is the gap or crunch calculation: how much margin exists between clearance and arrival?

These two assessments are the terminal's entire Horizon experience. They are narrow. They are honest. They are the things only Horizon can provide.

---

## 3. Terminal Daily Questions

The terminal operations manager asks five questions each day. Horizon can genuinely address two of them.

### Questions Horizon cannot address

**Can I work the current vessel?** Are the cranes operational? Is the labour available? Is the equipment functional? Is the yard accessible? These are entirely internal concerns. Horizon has no data, no visibility, and no intelligence to offer. This is the TOS domain. Horizon must not attempt to address it.

**Will I finish the current vessel on schedule?** The terminal tracks crane rates, moves remaining, weather stops, equipment breakdowns, and labour productivity to estimate completion. This is the terminal's core operational competence. Horizon cannot estimate cargo completion because it has no input data. Horizon can receive the terminal's own estimate (if the terminal chooses to share it) but cannot independently calculate it. The terminal's estimate is their contribution to Horizon; Horizon cannot substitute for it.

### Questions Horizon can address

**When is the next vessel actually arriving?** This is the primary WIIFM. The terminal's berth schedule shows the planned arrival time (often published days in advance, rarely updated). Horizon shows the AIS-derived arrival estimate, continuously updated, incorporating tidal window constraints, weather, and navigational factors that the schedule cannot reflect. A vessel scheduled for 0734 but tracking to 1100 on AIS is intelligence the terminal cannot get from their TOS.

**Will the handover work?** Combining the berth clearance timing (from the terminal's completion forecast or the scheduled ETD) with the vessel's actual arrival timing (from Horizon's navigation readiness assessment), Horizon can assess whether the handover will proceed smoothly (comfortable margin), be tight (narrow margin), or fail (vessel arrives before berth is clear). This handover assessment is the terminal-specific expression of the Port Readiness Signal.

### Questions Horizon partially addresses

**Do I have standby or crunch risk?** Standby (gang ready but vessel not there) results from a vessel arriving later than expected. Crunch (vessel arrives before berth is clear) results from the current operation running over. Horizon can detect the standby risk (vessel tracking late, handover gap forming) because it has the vessel's AIS track. It cannot detect crunch risk from the inside (current operation running behind) unless the terminal contributes its completion forecast. With the terminal's data, Horizon can detect both risks. Without it, Horizon can detect only standby risk from vessel-side lateness.

---

## 4. Terminal WIIFM

The honest value proposition:

> Your TOS shows you the plan. Horizon shows you whether the plan still
> matches reality.
>
> Specifically: Horizon tells you when the next vessel is actually arriving
> at your berth, using live vessel tracking, tidal predictions, weather data,
> and port service availability that your TOS does not have access to.
>
> If the vessel is tracking 3 hours late, your gang does not need to stand
> by at the scheduled time. If the vessel is tracking 2 hours early, your
> current operation may need to accelerate. Horizon gives you that
> intelligence before you would discover it by phone.

This is narrow. It does not promise operational transformation. It does not promise productivity improvement. It promises one thing: better timing intelligence for inbound vessels. That one thing reduces standby cost (don't pay a gang to wait for a late vessel) and prevents crunch surprises (know early when a vessel is arriving before you're ready).

The value is proportional to how wrong the schedule typically is. If the schedule is always right (it is not; the ACCC found only 10 percent of vessels hit their berth window in 2020-21), Horizon adds nothing. If the schedule is frequently wrong (it is), Horizon's live arrival intelligence prevents the costs that schedule inaccuracy imposes on the terminal: wasted standby and unexpected crunches.

---

## 5. Terminal Readiness Interpretation

The four readiness states translated into terminal operational language.

### READY: Handover on track

The current vessel will depart (or the berth is already clear) before the next vessel arrives, with comfortable margin. Labour and equipment planning can proceed as scheduled. No adjustment needed.

Terminal language: "Berth 3 clear by 0900. Next vessel 1100. Two-hour buffer. We're fine."

### AT RISK: Handover tight

The margin between the current vessel's departure and the next vessel's arrival is narrow. If the current operation runs over, or if the next vessel arrives early, the handover could fail. The terminal should monitor progress and prepare to adjust.

Terminal language: "Berth 3 clear by 0900. Next vessel 0945. Forty-five minutes. Tight. If we run over, we've got a crunch."

### NOT READY: Handover will fail

The next vessel will arrive before the berth is clear. The terminal faces a choice: accelerate the current operation, accept the standby cost of the incoming vessel waiting, or request the port to delay the incoming vessel.

Terminal language: "Berth 3 won't be clear until 1100. Next vessel arriving 0734. Three-plus hour gap. Gang on standby from 0734 unless we redeploy."

### UNCERTAIN: Arrival or handover confidence unreliable

Horizon cannot reliably assess the arrival timing (vessel far from port, AIS data insufficient) or the handover margin (berth clearance timing is schedule-based and unverified by the terminal). The terminal should plan from their schedule but be aware it may change.

Terminal language: "Next vessel scheduled 0734 but arrival confidence is low. Plan from the schedule but don't commit the gang until we have a firmer ETA."

---

## 6. Terminal Data Contribution Model

### What the terminal can contribute

Three data points, each narrow and each transformational.

**Predicted completion time:** the terminal's estimate of when cargo operations on the current vessel at a specific berth will be complete. This is the terminal's professional assessment based on moves remaining, crane rates, weather conditions, and labour availability. It is more accurate than the scheduled ETD because the schedule was set days ago and the terminal is assessing now.

**Berth-ready signal:** a binary confirmation that the berth is clear and available for the next vessel. This transitions berth readiness from "expected" to "confirmed."

**Departure readiness:** confirmation that the current vessel is ready to depart and is waiting only for nautical services (pilot, tug) to commence the departure. This signals to the VTSO and to the incoming vessel's planning chain that the berth clearance sequence is in motion.

### What each contribution changes

**Berth readiness transitions from schedule-based to terminal-confirmed.** The readiness provenance label changes from "based on scheduled departure" to "terminal-confirmed completion estimate" or "terminal-confirmed berth clear." Every downstream consumer of the berth readiness component sees the provenance change.

**Shipping line value increases.** The shipping line's Vessel Readiness URL shows berth readiness with terminal-confirmed provenance instead of schedule-based qualification. The "berth readiness unconfirmed" caveat disappears. The Port Readiness Signal's most important component (for the shipping line) becomes its most accurate component. This is the transformation that makes the Signal a product shipping lines will pay for.

**VTSO confidence increases.** The Readiness Tab's berth readiness assessment becomes ground-truth rather than approximate. The VTSO can make coordination decisions based on confirmed berth status rather than scheduled berth status.

**Towage dispatch confidence increases.** The towage manager's dispatch decision ("will the berth be clear when my tug arrives?") is answered with terminal-confirmed data rather than schedule-inferred data. "Berth clear (terminal-confirmed)" is a stronger dispatch signal than "berth scheduled to clear (unconfirmed)." Fewer idle tug dispatches, directly.

---

## 7. Data Contribution Visibility

### How the terminal sees their contribution improve the signal

Before contribution: every berth card in their perspective shows the berth readiness provenance as "schedule-based." The capability note reads: "Berth readiness is based on available schedule data. With terminal completion forecasts, Horizon would show whether the berth is genuinely expected to clear before this vessel arrives." The handover assessment uses the scheduled ETD, which the terminal knows may be hours wrong.

After contribution: the berth readiness provenance changes to "terminal-confirmed." The capability note disappears, replaced by the live assessment. The handover assessment uses the terminal's own completion estimate, which the terminal can verify against their own operational knowledge. If the terminal says completion at 1400 and the scheduled ETD was 1545, the handover margin improves by 105 minutes, which may transition the readiness state from NOT READY or AT RISK to READY.

The before-and-after is self-evident. The terminal does not need to be told their data matters. They can see it: the provenance label changed, the handover margin improved, the readiness state transitioned. Their data made the signal more accurate, and the improvement is visible on their own berth cards.

### How others see the improvement

Other stakeholders see the berth readiness component change provenance from "schedule-based" to "terminal-confirmed." The composite readiness state may improve (a vessel that was AT RISK because berth readiness was tight on schedule timing may become READY when terminal-confirmed timing shows a larger margin).

No stakeholder sees the terminal's raw contribution (the specific completion time). They see the assessed state (berth readiness: READY, AT RISK, NOT READY) and its provenance (schedule-based versus terminal-confirmed). The Signal is the boundary: it consumes raw data and produces assessed states. The assessed states are shared. The raw data is not. This protects the terminal's operational performance data from exposure.

### What must not happen

The terminal's non-participation must never be named, blamed, or quantified in any surface visible to other stakeholders. "Terminal has not contributed data" is blame language. "Berth readiness based on schedule data" is provenance language. The difference is critical. Provenance describes the data quality. Blame describes the stakeholder. Horizon uses provenance, never blame.

Aggregate terminal participation statistics (e.g. "0% of berth readiness assessments are terminal-confirmed at this port") must not be exposed to parties who could use them as negotiation leverage. These are internal Horizon product metrics, not external reporting dimensions.

---

## 8. Recommended Terminal Perspective

### What it is

A thin, berth-centric readiness view inside Horizon. One card per berth the terminal operates. Each card shows two layers:

**Current vessel alongside (if any):** vessel name, operation status ("working cargo" or "ready to depart"), estimated completion (if terminal has contributed; "scheduled departure" if not).

**Next vessel inbound:** vessel name, readiness state (the Port Readiness Signal composite for this vessel), AIS-derived ETA, scheduled ETA, and the handover assessment (on track, tight, will fail, or unreliable).

### What it answers

"For each of my berths, is the next handover going to work?"

This is a berth handover confidence view. Not a fleet readiness view (the VTSO has that). Not a vessel readiness view (the shipping line has that). A berth handover view, which is the terminal's specific expression of the Port Readiness Signal.

### What it must not include

Crane productivity metrics. Labour scheduling or gang management. Yard occupancy or container dwell analysis. Equipment status or maintenance tracking. Cargo documentation or customs status. Vessel stow plan analysis. Performance dashboards or KPI reporting. Any feature that belongs in a Terminal Operating System.

The terminal perspective is not a lens in the Towage or Pilotage sense (a full operational workspace with five layers). It is a readiness intelligence feed: narrow, berth-centric, and focused on the one thing Horizon provides that the TOS cannot. Building it as a full lens would violate the "Horizon is not a TOS" principle and create a product surface that the terminal operations manager would immediately compare unfavourably to their actual TOS.

### Distribution model

Primary: a thin Horizon view at a Horizon URL, accessible to terminal users with appropriate authentication.

Secondary: notifications when a vessel's arrival timing shifts beyond a threshold that affects the terminal's berth plan. "BASS STRAIT at your Berth B03 now tracking 3 hours late. Handover margin increased." The notification links to the berth card inside Horizon.

Tertiary: an API feed into the terminal's TOS, delivering the AIS-derived ETA and the handover assessment for vessels at the terminal's berths. The API delivers the signal (ETA and readiness state). The Horizon view contains the full picture (component breakdown, provenance, capability notes, expected timing). This preserves product gravity: the API is a headline, the view is the answer.

---

## 9. Build Recommendation

### Build later

The terminal perspective should not be built now. Five factors support deferral.

**The terminal is the hardest stakeholder to win.** Their data sensitivity is the highest. Their product pull is the weakest. Their primary concerns are internal and Horizon-invisible. Approaching the terminal before the ecosystem has credibility risks an early refusal that is difficult to reverse.

**Towage and pilotage proof must come first.** The terminal conversation is credible only after towage and pilotage have demonstrated that Horizon creates measurable value for participating stakeholders. Without that proof, the terminal's calculation is: "share sensitive data with an unproven platform for a secondary benefit." With proof, the calculation changes: "join a validated ecosystem where other participants are already benefiting."

**The port authority must actively advocate.** The terminal is more likely to participate when the port authority (their landlord, their regulator, or both) actively encourages participation. "The port authority that contracted Horizon expects its terminals to contribute berth readiness data" is a different conversation from "a technology vendor would like your data." The port authority's advocacy requires the port authority to be satisfied with Horizon's value first, which requires towage and pilotage integration to have been proven.

**The terminal's data contribution model requires design before engineering.** The first terminal contribution should be as low-friction as possible: one data point (predicted completion time) entered once per berth per shift. Not a system integration. Not an API. A manual entry that proves the value before the engineering investment. This model needs to be designed and agreed with the terminal partner before any engineering begins.

**Engineering effort should be concentrated on the Signal and the integrated lenses.** The readiness integration into Towage and Pilotage (Phase A, B, C) is the current engineering priority. Building a terminal perspective before the integrated lenses are complete would divert effort from the work that creates the ecosystem credibility the terminal conversation depends on.

### Prepare now

Six preparations can and should be made inside existing readiness work, at zero or minimal additional engineering cost. These preparations ensure that when the terminal conversation begins, the commercial invitation is already visible in the product and the engineering can proceed rapidly.

**Preparation 1: berth readiness provenance labelling.** The berth readiness component of the Port Readiness Signal must consistently show its provenance: "schedule-based" (using the scheduled ETD) or "terminal-confirmed" (using the terminal's completion forecast). The provenance label must be visible on every surface where berth readiness appears: the Readiness Tab, the Towage Lens, the Pilotage Lens, and the future Vessel Readiness URL. This is already partially implemented. It must be consistent and complete.

**Preparation 2: the invitational capability note.** When berth readiness is schedule-based, the expanded scorecard must show: "Berth readiness is based on available schedule data. With terminal completion forecasts, Horizon would show whether the berth is genuinely expected to clear before this vessel arrives." This capability note is the pre-positioned commercial invitation. It is visible to every audience that sees the Signal. It describes the value of terminal participation without naming or blaming the terminal. This is already implemented. It must be maintained.

**Preparation 3: berth handover gap calculation.** The time between the current vessel's expected departure and the next vessel's predicted arrival should be calculated and displayed as the handover margin. This calculation can be performed now using scheduled ETDs and AIS-derived ETAs (both available). The margin is the operational metric that makes readiness meaningful to the terminal: "45 minutes of margin" is more operationally useful than "AT RISK." This calculation should be surfaced in the Readiness Tab and in the stakeholder lenses where berth readiness is displayed.

**Preparation 4: shadow-mode schedule accuracy measurement.** Compare the scheduled ETD of current occupants with the actual departure time (observable via AIS when the vessel moves from alongside to under way). Track how wrong the schedule typically is per berth, per port, over time. This produces the evidence that makes the terminal data contribution case quantifiable: "berth readiness assessments based on schedule data have an average error of X hours at Melbourne, meaning Y standby events per month were not preventable because the schedule was too inaccurate."

This evidence is the terminal engagement instrument. "Your completion forecast would reduce that error" is a quantifiable proposition. "Please share your data" is not.

**Preparation 5: low-friction manual contribution concept.** Specify (but do not build) a manual terminal data contribution interface: a simple form where the terminal enters one number (predicted completion time) per berth, once per shift. The interface should be so simple that the terminal operations manager or supervisor can fill it in during a shift handover briefing without opening a new system. When the terminal engagement conversation begins, this specification can be deployed rapidly as the minimum viable terminal contribution mechanism.

**Preparation 6: the berth readiness transition story.** Prepare a demonstration that shows the before-and-after: what the readiness signal looks like with schedule-based berth readiness versus terminal-confirmed berth readiness. For a real vessel at Melbourne, show the readiness state with the scheduled ETD (e.g. NOT READY, 8-hour gap) and then manually override with a realistic terminal completion estimate (e.g. AT RISK, 45-minute margin). The state transition demonstrates the value of terminal participation visually and immediately. This demonstration is the most powerful tool for the terminal onboarding conversation.

---

## 10. Relationship to Shipping Line Value

The terminal's data is the gateway to the shipping line commercial product.

The Port Readiness Signal has three components. Navigation readiness is assessable from port authority data (AIS, tides, weather). Service readiness is assessable from towage and pilotage data. Berth readiness is the component that depends on the terminal.

Without terminal data, berth readiness is schedule-based. The Signal carries a permanent qualification: "berth readiness unconfirmed." Every shipping line that sees the Vessel Readiness URL sees this qualification on the component they care about most. The Signal is useful but incomplete. It can tell the shipping line "the tide is fine and the services are likely available" but it cannot tell them "the berth will actually be clear." The shipping line's primary question ("should I slow down because the berth won't be ready?") cannot be answered definitively without terminal data.

With terminal data, berth readiness becomes ground-truth. The qualification disappears. The Signal becomes authoritative on all three components. The shipping line can now receive a definitive port readiness assessment: berth clear, pilot available, tug available, tide open, proceed. Or: berth delayed, recommend speed adjustment, save X hours of anchorage. That is the product shipping lines will pay for.

The commercial consequence: the terminal's single data contribution (predicted completion time per berth) is the input that transforms the Signal from a useful coordination tool into a revenue-generating commercial product. The terminal may never pay for Horizon directly. But the terminal's participation enables the revenue stream from shipping lines. This makes the terminal the pivotal stakeholder in Horizon's commercial model, despite being the weakest in product pull.

The commercial model should reflect this asymmetry. The terminal contributes data. The shipping line pays for the Signal that the terminal's data makes possible. The terminal receives arrival confidence in return. The value flows through the terminal, not primarily to the terminal. The port authority, as the platform contracting party and the terminal's landlord, is the appropriate party to facilitate terminal participation, either through advocacy, through contractual provisions, or through funding the terminal's arrival confidence feed as part of the platform agreement.

---

## 11. Governance Boundaries

These boundaries are permanent within this architecture.

### 11.1 The terminal perspective must never become TOS-lite

Any proposed terminal feature must be evaluated against the question: "Does this belong in a TOS?" If the answer is yes, the feature must not be built. Crane scheduling, gang management, yard operations, equipment tracking, cargo documentation, performance dashboards, and any feature that requires visibility into the terminal's internal operations are permanently out of scope.

The test: if the feature requires data that only the terminal has about its own operations (beyond the three defined contribution points: completion time, berth-ready signal, departure readiness), it belongs in the TOS, not in Horizon.

### 11.2 Terminal data must not be exposed raw

The terminal's predicted completion time is operationally sensitive. It reveals when the terminal expects to finish, which could be used to assess their productivity, their reliability, or their competitive position relative to other terminals. The Port Readiness Signal consumes this data and produces an assessed berth readiness state (READY, AT RISK, NOT READY). The assessed state is shared. The raw completion time is not.

No surface (internal or external) may display the terminal's raw predicted completion time to parties other than the terminal itself and the port authority (as the platform contracting party). Other stakeholders see the berth readiness state and its provenance ("terminal-confirmed"). They do not see the specific time the terminal provided.

Exception: the berth clearance timing shown as "berth expected clear at [time]" is an assessed output that combines the terminal's completion estimate with departure logistics. It is less operationally sensitive than the raw completion forecast because it includes pilot and tug departure coordination time that the terminal does not control. This assessed timing may be shown to the VTSO and to the vessel operator but must be presented as Horizon's assessment, not as the terminal's raw data.

### 11.3 Terminal non-participation must not be blamed

The invitational capability note ("with terminal completion forecasts, Horizon would show...") is the only permitted reference to the absence of terminal data. Language such as "terminal has not connected," "terminal data missing," "terminal not participating," or "contact the terminal operator" is prohibited on every surface. The provenance label ("schedule-based") communicates the data limitation. The capability note communicates the improvement opportunity. Neither names or blames the absent party.

### 11.4 The terminal must not be treated as a primary payer too early

The terminal's product pull is too weak and its data sensitivity too high for it to be a primary buyer of Horizon. Approaching the terminal with a commercial proposal ("pay for arrival confidence") before the ecosystem credibility exists will likely produce a refusal that is difficult to reverse. The terminal's participation should be facilitated through the port authority relationship (advocacy, contractual provisions, or funded as part of the platform agreement) rather than through a direct commercial sale to the terminal.

When the terminal is approached commercially, the proposition should be: "the port authority has contracted Horizon and is encouraging terminal participation. Your arrival confidence feed is included. Your data contribution (predicted completion time) makes the Signal more accurate for the port and for the shipping lines that call at your berths." That is a facilitated inclusion, not a cold sale.

### 11.5 Berth readiness must not be overstated without terminal data

When the terminal has not contributed data, berth readiness is assessed from the scheduled ETD of the current occupant. The scheduled ETD may be significantly wrong. The berth readiness assessment must carry the "schedule-based" provenance and the readiness state must reflect the uncertainty inherent in schedule-derived timing.

The Signal must never present schedule-based berth readiness as if it were terminal-confirmed. The provenance distinction ("schedule-based" versus "terminal-confirmed") is not a technical label. It is a trust mechanism that communicates the confidence level to every consumer of the Signal. Removing, hiding, or softening this distinction to make the Signal appear more complete is a trust violation.

---

## 12. Final Recommendation

**Prepare now. Build later.**

Prepare now because the six preparations (provenance labelling, capability notes, handover gap calculation, schedule accuracy measurement, manual contribution concept, transition demonstration) cost almost nothing to implement inside existing readiness work and create the pre-conditions for a successful terminal engagement. Every time the Port Readiness Signal is shown to any audience (Ports Victoria, Shipping Australia, a towage provider, a shipping line executive), the berth readiness gap is visible and the invitation is pre-positioned. The preparation is the commercial development, happening inside the product.

Build later because the terminal is the hardest stakeholder, the product pull is the weakest, the data sensitivity is the highest, and the ecosystem credibility that makes the conversation viable does not yet exist. Towage and pilotage must be proven first. The port authority must be actively advocating. The measured outcomes must exist. The terminal engagement should begin when these preconditions are met, which the three-year commercial roadmap positions in Year 2.

The trigger for building: when a specific terminal operator at a Horizon-enabled port expresses willingness to participate (likely through port authority facilitation), the thin berth-centric view and the manual completion forecast interface should be deployable within weeks, not months. The preparation workstream ensures this is possible. The build is rapid because the specification is complete, the contribution model is defined, the readiness integration architecture is proven (through towage and pilotage Phase A), and the interface is deliberately simple (one number per berth, entered manually).

**What success looks like:** one terminal at Melbourne contributing predicted completion time for their berths, once per shift, through a simple manual entry. The berth readiness component of the Port Readiness Signal transitioning from "schedule-based" to "terminal-confirmed" for vessels at those berths. The improvement visible to the VTSO, to the towage manager, to the pilot scheduler, and to the shipping line's Vessel Readiness URL. The Signal's most important component becoming its most accurate component. That single achievement unlocks the shipping line commercial product and validates the terminal as the pivotal data contributor the entire architecture identified it to be.

---

*End of governing document. Terminal Readiness Perspective Architecture v1.0.*
