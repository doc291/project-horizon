---
title: Horizon Port Readiness Distribution Architecture
version: 1.0
status: ratified
classification: Confidential
owner: Tony Trajceski (AMSG Horizon division)
effective_date: 2026-06-03
supersedes: none (new governing reference)
applies_to: All future Port Readiness work — internal surfaces, external distribution, APIs, notifications, stakeholder onboarding
related:
  - docs/governance/LENS-CONTRACT.md
  - Port_Readiness_Signal_Product_Specification.md
---

# Horizon Port Readiness Distribution Architecture
## v1.0 — Governing Reference

> This document governs how the Port Readiness Signal is produced once and
> distributed across many surfaces and many stakeholders. It is the authority
> that future Port Readiness decisions are measured against. Where a proposed
> feature conflicts with this document, this document prevails until amended by
> the owner via a versioned entry.

---

## 1. Executive Summary

The Port Readiness Signal is Horizon's flagship product. It answers one
question — *"can this vessel proceed to berth as planned, right now, given
everything Horizon currently knows?"* — and it answers it honestly, including
when the honest answer is "not yet assessable."

Horizon's strategic position rests on a single architectural commitment:

> **One Signal, computed once, distributed through many perspectives.**

The Signal is assembled from data no single party holds alone. That horizontal
position — sitting across the port ecosystem — is the only place the Signal can
be produced, and is therefore the platform's defensible core. Everything else
(lenses, tabs, URLs, notifications, APIs) is a **perspective onto the same
Signal**, never a second source of truth.

Horizon distributes the Signal through two layers:

- **Internal distribution** — the Readiness Tab (internal flagship), and the
  Towage and Pilotage Lenses (operational workspaces) — consumed by operators
  who have Horizon accounts and live inside the platform.
- **External distribution** — the Vessel Readiness URL (external flagship),
  notifications, and (later) APIs and subscriptions — consumed by shipping
  lines, agents and terminals who may have **no Horizon account** and meet the
  Signal at the moment it matters to them.

The commercial engine is **value exchange made visible**: every place the
Signal shows UNCERTAIN is an explicit, non-accusatory invitation for the
missing stakeholder to contribute the data that would make it assessable. Trust
is earned before coverage is pursued. Horizon is advisory, never authoritative.

This document defines the principles, the two distribution layers, the
per-stakeholder consumption model, the contribution and exposure rules, the
boundaries Horizon must never cross, and the guardrails future teams must hold.

---

## 2. Core Principles

These five principles are binding. Every Port Readiness surface, present or
future, must satisfy all five.

### 2.1 One Signal
There is exactly one Port Readiness assessment for a given vessel at a given
port at a given time. It is produced by one deterministic engine. Every surface
renders **that** assessment. No surface may compute its own readiness, weight it
differently, or present a state the Signal did not produce. If two surfaces can
disagree about a vessel's readiness, the architecture has failed.

> **Engineering corollary (current state):** the assessment engine is a single
> pure module (`PRS-PURE`) consumed by the Readiness Tab and lenses. Any new
> surface — including the Vessel Readiness URL — MUST reuse that engine, not
> re-implement it. Divergence is prohibited by construction, not by discipline.

### 2.2 Multiple Perspectives
The same Signal is presented differently to different stakeholders and contexts:
a fleet picture for the VTSO, a service-and-window view for pilotage, a
demand-and-capacity view for towage, a single-vessel card for a shipping line.
Perspective changes **framing, emphasis and language** — never the underlying
state. A perspective may *withhold* detail (see §7) but may never *alter* the
verdict.

**Perspective vs Product (binding definition).** The distinction decides whether
a proposal may ship freely or requires a formal amendment:

- A **perspective** reuses the canonical Port Readiness Signal and applies
  filtering, emphasis, role language or presentation only. It has no assessment
  logic, no data pipeline and no update cycle of its own. Perspectives may be
  built within this architecture.
- A **product** has its own assessment logic, its own data pipeline, or its own
  update cycle — or can produce a **different readiness state** from the
  canonical Signal. **Any proposed product requires a formal governance
  amendment (§9.3) before any work begins.**

**Competing-signal prevention.** No surface may apply its own weighting,
filtering or reinterpretation that yields a *different composite readiness
state*. Pilotage may emphasise navigation; towage may emphasise berth and
service; a shipping line may see only its own vessel — but the **composite state
remains canonical** on every surface. Emphasis is permitted; recomputation is
not.

### 2.3 Platform Gravity
Every distributed artefact must pull the consumer **toward** Horizon, not
substitute for it. A notification states the headline and links to the answer;
the answer lives in Horizon. The Vessel Readiness URL is a doorway into the
platform, not a standalone mini-product that can be screen-scraped into
irrelevance. The test for any external artefact: *does this deepen the
relationship with Horizon, or does it let the consumer stop needing Horizon?*
Build only the former.

**URL boundary test (binding).** Every proposed Vessel Readiness URL enhancement
must pass one question: *does this make the URL more self-sufficient, or more
connected to Horizon?* Only the latter is permitted. The URL may **show the
assessment**; the **platform contains the coordination**. External surfaces are
**read-only** — they never accept input, host workflow, or accumulate the port
picture.

### 2.4 Advisory not Authoritative
The Signal supports decisions; it never issues them. It does not clear, approve,
authorise, or grant permission to proceed. Licensed officers — pilot, Harbour
Master, VTSO, master — retain all authority. Language discipline is permanent
and applies on **every** surface, internal and external: "the signal indicates",
"based on current data", "assessment" — never "cleared", "authorised",
"approved", "proceed". This protects the operators' authority and Horizon's
liability position simultaneously.

**Language controls (binding on every surface, internal and external).**

- **Prohibited:** *confirms, verifies, certifies, clears, authorises, approves,
  instructs, proceed, safe, safe to proceed.*
- **Preferred:** *indicates, assesses, based on current data, the Signal shows,
  readiness assessment, advisory only.*

The prohibited list describes authority Horizon does not hold; the preferred
list describes the advisory posture it does. New copy on any surface is checked
against this list.

### 2.5 Trust before Coverage
A Signal that is right when it says READY, that warns early when it says AT RISK,
and that is honest when it says UNCERTAIN is worth more than a Signal that
covers everything and is wrong once. Accuracy precedes completeness. The
asymmetry is absolute: the cost of false comfort (a confident READY that fails)
is catastrophic to ecosystem trust; the cost of false caution (an AT RISK that
resolves) is a minor inconvenience. Every threshold, state transition and
distribution decision is designed around that asymmetry. Missing data degrades
gracefully and visibly — it never silently defaults to green.

**Honesty / anti-greenwashing guardrails (binding).**

- **UNCERTAIN must remain visible on every readiness surface** — internal,
  external, and in any export.
- UNCERTAIN must **not be hidden for demos, sales or executive presentations.**
  The honest gap is part of the product, not a blemish to conceal.
- **READY must not be inflated** to improve appearance.
- **Threshold changes must be justified** by operational accuracy or changed
  port rules (HMD / Port Information Guide) — **never** by a desire to reduce
  AT RISK or NOT READY counts. Threshold changes are recorded.
- **Structural absence and non-participation must not be conflated.** A
  structurally-absent data source (a feed that does not yet exist) and a
  stakeholder *declining* to participate are different facts and must not be
  presented as the same thing.

### 2.6 Per-Vessel, Per-Port Scope
The canonical Signal is, and for the governed horizon remains,
**per-vessel-per-port**: one vessel, one port, one assessment. This bounds "One
Signal" and is a deliberate trust constraint, not a limitation to be quietly
relaxed.

- **Cross-port views and fleet-wide readiness views are future *governed*
  capabilities** — not assumed extensions of the current Signal.
- **Cross-port readiness aggregation requires a formal amendment (§9.3)** before
  any design or build, because aggregation changes what the Signal *means* and
  what it exposes.
- The single-active-port server model is consistent with this scope; multi-port
  concurrency is itself a governed capability.

---

## 3. Internal Distribution Layer

Consumed inside the authenticated platform by operators who work in Horizon.

### 3.1 Readiness Tab — internal flagship
The destination that answers *"is the port ready for each vessel?"* across the
whole forward picture. Level 1 is the operational summary (vessel, composite
state, readiness-first explanation, expected state-change timing, component
strip), sorted by urgency. Level 2 (expanded) is **evidence only** — the three
component assessments with reasons, confidence, provenance, missing-data notes
and stakeholder-contribution capability notes. The Readiness Tab is the
canonical full view; all other readiness surfaces are subsets or reframings of
it.

### 3.2 Towage Lens — operational workspace
The tug operations manager's world: named fleet, shift shape, capacity-clash
detection, demand-vs-capacity forward curve. It is where towage *work* is done.
Its relationship to the Signal: the Towage Lens surfaces the operational detail
*behind* the Service component of readiness, in the tug manager's own terms. It
does not re-decide readiness; it enriches the towage perspective of it.

### 3.3 Pilotage Lens — operational workspace
The pilot scheduler's world: watch shape, per-transit will-it-hold verdict, UKC
window per transit, transit sequence pressure, roster reference. It is where
pilotage *work* is done, and it surfaces the operational detail behind the
Navigation and Service components in the scheduler's terms.

### 3.4 Dashboard relationship
The Beta 10 VTSO dashboard remains the coordination and conflict surface. The
Readiness Tab sits **alongside** it as a peer destination, not a replacement:
the dashboard answers "what conflicts exist and how are they coordinated"; the
Readiness Tab answers "is the port ready for each vessel." They draw on the same
underlying port state but serve different questions. Neither overrides the
other; both defer to the licensed operator.

> **Governing rule for the internal layer:** the Readiness Tab is the single
> authoritative internal view of the Signal. Lenses are workspaces that *expand*
> a perspective of it. No internal surface may show a readiness state the Tab
> would not show for the same vessel and time.

---

## 4. External Distribution Layer

Consumed outside the platform by parties who may have no Horizon account. This
layer is how the Signal reaches the market — and how the market is invited in.

### 4.1 Vessel Readiness URL — external flagship
*One vessel. One port. One readiness assessment. One Horizon URL.* The external
embodiment of the Signal: a single-vessel card rendered by the **same engine**
as the Readiness Tab, delivered through a **signed, time-boxed, capability-
scoped link** that requires no account. It is the destination for notifications,
the target of API headlines, the artefact a shipping line or agent opens, and a
stakeholder-onboarding and demonstration tool. It must never become a port-wide
view or a second product — it is a doorway (Platform Gravity, §2.3).

### 4.2 Notifications
Notifications create urgency and carry the headline; the **answer lives in
Horizon**. A notification states "Readiness for {vessel} is now {state}" and
links to the Vessel Readiness URL. Their job is to pull the recipient to the
Signal at the moment it changes — never to be the assessment.

### 4.3 APIs
A later capability: a machine-readable **headline** of the Signal for a single
vessel, behind the same signed-token or account model. APIs distribute the
*headline and the link*, not the raw inputs, and are a perspective bound by §7.

### 4.4 Payload ceiling (binding for notifications AND APIs)
Notifications and API responses **may include only**:

- vessel
- port
- composite state
- a short one-line reason
- the Horizon URL

They **must not include**: the full component breakdown, the full readiness
explanation, expected state-change detail, capability notes, raw conflict data,
or provenance detail. **Those belong at the Horizon URL.** The API and
notifications create urgency; the Horizon URL contains the answer. This ceiling
is the mechanical guarantee of Platform Gravity (§2.3) for the headline
channels.

### 4.5 Future subscriptions
The commercial maturation of external distribution: a stakeholder subscribes to
the Signal for the vessels/berths relevant to them, receiving notifications and
URL access as a service. Subscriptions are a *distribution and commercial*
construct layered on the existing Signal — they introduce no new assessment and
no new data exposure beyond what §7 permits.

> **Governing rule for the external layer:** external surfaces are signed,
> scoped, expiring, sanitised, read-only, advisory perspectives of the Signal.
> They distribute the answer and the doorway — never the platform's interior.

---

## 5. Stakeholder Consumption Model

Each stakeholder consumes the **same Signal** through the perspective that
matches their authority and need. "Sees" below always means the assessed states
and their permitted detail — never another party's raw contributed data.

| Stakeholder | Primary surface | What they consume | Their authority |
|---|---|---|---|
| **Port Authority / VTSO** | Readiness Tab + Dashboard (internal) | The full forward picture: every relevant vessel's composite + components; coordination context | Coordination; never overrides the licensed officer |
| **Shipping Line** | Vessel Readiness URL + notifications (external) | Their own vessel's composite state, readiness explanation, expected timing, component strip, capability notes | Voyage/speed decisions; the Signal informs, does not instruct |
| **Agent** | Vessel Readiness URL (external; later: portfolio view) | Readiness for vessels in their portfolio, one vessel at a time | Coordination on behalf of principals |
| **Towage** | Towage Lens (internal, if account) + Vessel URL (external) | Service (towage) perspective: capacity, clash, fleet state — plus the composite | Dispatch decisions; the Signal informs |
| **Pilotage** | Pilotage Lens (internal, if account) + Vessel URL (external) | Navigation + Service (pilotage) perspective: windows, sequence, roster reference — plus the composite | Pilotage scheduling; the Signal informs |
| **Terminal** | Berth perspective via Tab/URL (external until integrated) | How Horizon sees arrivals for their berths; the Berth component | Berth/labour planning; the Signal informs |

**Rule:** a stakeholder sees the **assessed states** relevant to them and the
**composite**. No stakeholder sees another stakeholder's raw contributed data
through any surface. The Signal is the boundary: it consumes raw data and
produces assessed states; the states are shared, the raw data is not.

---

## 6. Data Contribution Model

The Signal becomes more trustworthy as more participants contribute. Each
contribution turns a specific component from UNCERTAIN into an assessed state.

| Contributor | Unlocks (component) | Current state without it |
|---|---|---|
| **Port Authority** (foundational) | Navigation (AIS, tides, weather, channel, bridge), berth *allocation* | Present today; the foundation the Signal is built on |
| **Pilotage provider** | Service — pilot availability / transit-window cover | UNCERTAIN (simulated); shown with an invitational capability note |
| **Towage provider** | Service — tug availability / capacity / clash | UNCERTAIN (simulated); shown with an invitational capability note |
| **Terminal operator** | Berth — confirmed completion / clearance (removes the "schedule-based" qualifier) | UNCERTAIN structural (schedule-based); shown with a capability note |
| **Shipping line** | Navigation — declared ETA / verified draft (higher confidence than AIS-derived) | Modelled/derived; nav capability note where partial |

**The contribution mechanic is the commercial engine.** Every UNCERTAIN
component is a visible, specific, non-accusatory statement of what Horizon
*would* assess if that stakeholder's data were live. The gap is the invitation.
This must always be expressed in **invitational/capability language**, never
blame — "with terminal completion forecasts, Horizon would show…", never
"terminal not connected" or "provider has not shared." Blame language is
prohibited on every surface.

**Blame-protection rules (binding).**

- **Absent stakeholders must not be named** on general readiness surfaces.
- Participation gaps are framed as **capability gaps** ("with terminal data,
  Horizon would…"), never as stakeholder failure.
- **Aggregate stakeholder-participation statistics must not be exposed
  externally** (e.g. "X% of terminals connected").
- **Assurance reporting may attribute readiness drivers to components**
  (Navigation / Service / Berth), **not to stakeholder blame.**
- In **multi-provider** environments, the Signal must not expose **comparative
  participation between providers** (no "Provider A shares data, Provider B does
  not"). The Signal assesses readiness; it does not score participants.

---

## 7. Data Exposure Rules

The single most important external-distribution control. The Signal is shared;
the platform's interior is not.

### 7.1 Safe to expose externally (per single vessel, to a scoped recipient)
- The **subject vessel**: name, and the readiness assessment for it.
- The **composite state** and the **three component states**.
- The **readiness explanation** and **expected state-change timing**.
- **Provenance / confidence / SIMULATED / ASSUMED / unavailable** labels —
  mandatory; honesty travels with the Signal.
- **Capability / contribution notes** — invitational, onboarding-positive.
- The **global caveat** (e.g. "service and terminal readiness may use simulated,
  assumed or unavailable data").
- Coarse subject-vessel context the recipient already owns (its ETA, its berth).

### 7.2 Must NOT be exposed externally
- The **port-wide picture / fleet** — external surfaces are one vessel only.
- **Raw conflict objects** or internal data-source internals/debug fields.
- **Other vessels' identities** — e.g. a berth-overlap description that names the
  occupying vessel must be de-identified externally ("berth occupied by another
  vessel until HH:MM"). Internal authenticated views may name it; external views
  may not.
- **Service-readiness internals** — pilot/tug identities, rosters (and these are
  not real data today regardless).
- **Any other stakeholder's raw contributed data.**
- **Aggregate participation statistics** or **comparative provider
  participation** (see §6 blame-protection) — never exposed externally.
- **Anything that lets the URL reconstruct the port picture** by enumeration.

### 7.3 Exposure controls (binding for any external surface)
- **Signed** (HMAC), **scoped** (one vessel, one port), **expiring** (short TTL
  matched to the operational window — a stale public link that looks live is the
  cardinal sin), **read-only**, **advisory**, **sanitised** (a single-vessel
  projection, never a filtered full summary).
- No anonymous **enumerable** access. The token is the capability; absence of a
  valid token yields an "expired/invalid" page, never data and never a blank.

---

## 8. Product Boundaries — what Horizon must never become

These are permanent. Crossing any of them damages the trust position that is the
entire moat.

1. **Never an authority.** Horizon never clears, authorises, approves, or
   instructs a movement. It informs; the licensed officer decides.
2. **Never a fabricator.** No assessed state without a traceable data source. No
   gap filled by assumption and presented as assessment. UNCERTAIN is the honest
   state and must remain available and used.
3. **Never green-by-default.** Missing data never resolves to READY. Structural
   absence is shown as UNCERTAIN with qualification; it never silently passes.
4. **Never a recommendation/cost engine on the Signal surface.** No speed advice,
   no percentage score, no dollar figures on the readiness scorecard or URL.
   (Decision-support cost modelling, where it exists internally, is a separate
   concern and is illustrative/labelled — it never bleeds into the Signal.)
5. **Never a data-broker.** It shares assessed states, never another party's raw
   contributed data.
6. **Never a disconnected mini-product.** External artefacts are doorways into
   Horizon, not substitutes for it.
7. **Never divergent.** No surface may show a readiness state the one Signal did
   not produce.

**Commercial governance clause (binding).** Commercial terms may govern *access*
to the Signal — who may see it, through which surface, under what subscription.
Commercial terms may **not** alter: Signal content, the trust principles (§2),
the data-exposure rules (§7), the product boundaries (§8), readiness states, or
advisory discipline (§2.4). Pricing and packaging sit *around* the Signal; they
never reach inside it. Any amendment to the data-exposure limits or product
boundaries requires formal recorded approval (§9.3) — **board-level where the
change is commercially material.**

---

## 9. Future Roadmap Guardrails

### 9.1 Future teams SHOULD
- Reuse the single readiness engine for every new surface; treat divergence as a
  defect.
- Extend distribution (URL, notifications, API, subscriptions) as *perspectives*
  bound by §5–§7.
- Pursue real stakeholder data feeds to convert UNCERTAIN → assessed, component
  by component, earning per-component trust independently.
- Build the **outcome/accuracy history** (was READY actually ready?) — the
  credibility engine that justifies the Signal commercially.
- Keep every surface advisory, honest, and gated/staged exactly as the readiness
  sprints were (preview first, owner-approved, trust-preserving).
- De-identify and sanitise relentlessly at every external boundary.

### 9.2 Future teams SHOULD NOT
- Re-implement the assessment in a second place, or let a surface re-weight it.
- Add a percentage Signal-confidence score, speed advice, recommendations, or
  cost figures to the Signal surfaces without a governing amendment.
- Expose the fleet, raw conflicts, or third-party data through any external
  surface.
- Ship unauthenticated enumerable vessel access, or durable/never-expiring public
  links.
- Introduce blame language anywhere, or weaken a trust principle to "look
  greener."
- Let an external artefact grow into a standalone product that erodes platform
  gravity.

### 9.3 Amendment discipline & authority
This document is amended only through a **formal, versioned, recorded
amendment**. Silent edits are prohibited. A feature that requires crossing a
boundary in §8, relaxing the per-vessel-per-port scope (§2.6), or breaching a
guardrail in §9 requires an explicit recorded amendment **before any work
begins**.

**Approved amending authority, in order:**
1. the **document owner**;
2. a **nominated successor** if the owner is unavailable;
3. the **AMS Group Board** if no owner or successor exists;
4. the **AMS Group Board** for any **commercially material** amendment
   (notably changes to data-exposure limits or product boundaries, per the
   Commercial governance clause in §8).

**Verbal instructions, email directives and commercial pressure do not
constitute amendments.** Until a change is recorded under this clause, this
document stands as written.

---

## 10. Strategic Recommendations

1. **Make the Signal the product, and the surfaces its perspectives.** Resist
   pressure to let any one surface (a lens, the URL, an API) become "the
   product." The defensible asset is the horizontal Signal; everything else is
   distribution.
2. **Lead external distribution with the Vessel Readiness URL behind a signed,
   expiring token.** It needs no accounts, reaches the parties without them, and
   is the most natural onboarding and demonstration instrument — provided it
   stays one-vessel, sanitised, and a doorway.
3. **Treat every UNCERTAIN as a sales conversation already started.** The
   contribution model is the go-to-market: the gaps are pre-qualified
   invitations. Keep them invitational, specific, and honest.
4. **Bank trust before chasing coverage.** Prioritise being demonstrably right on
   the components Horizon can already assess (navigation, tide/UKC, weather) over
   adding components it cannot yet assess well. Per-component trust compounds.
5. **Use the Shipping Australia positioning to anchor neutrality.** Horizon's
   value is that it sits across the ecosystem and favours no participant. The
   advisory-not-authoritative and no-data-broker boundaries are not constraints —
   they are the credibility that makes a neutral horizontal platform possible.
6. **Instrument accuracy from day one of external distribution.** The moment the
   Signal leaves the building, its track record becomes the asset. Measure and
   own it.
7. **Hold the line on honesty as a feature, not a limitation.** Horizon's
   competitive claim — *"Horizon tells you whether the port is ready for your
   vessel, and you can trust it"* — is only true while UNCERTAIN, provenance, and
   advisory language remain visible. They are the product, not friction.

---

---

## 11. The Moat

The Port Readiness Signal's defensibility is **not the code alone** — the
assessment engine could be re-implemented by others. The durable moat is the
combination only Horizon holds:

- **AMS port relationship and sensor position** — the right to sit horizontally
  across the port ecosystem.
- **Contractual data access** — the feeds that convert UNCERTAIN into assessed
  states, secured by agreement.
- **Per-port configuration** — the operator-ratified thresholds, towage matrices
  and rules that make the Signal correct for *each specific port*.
- **Stakeholder network effects** — each participant who contributes data makes
  the Signal more complete and more valuable to every other participant.
- **Outcome history over time** — the accumulating, audited record of "was READY
  actually ready?" that no new entrant can replicate retrospectively.

Strategic consequence: protect the relationships, the contracts, the
configuration and the outcome record as fiercely as the product. A competitor
can copy a scorecard; they cannot copy four years of trusted, port-specific,
network-fed outcome history. Every guardrail in this document exists to keep
that moat intact — honesty, neutrality and advisory discipline are not
constraints on the moat, they *are* the moat.

---

*End of governing document. Horizon Port Readiness Distribution Architecture
v1.0.*
