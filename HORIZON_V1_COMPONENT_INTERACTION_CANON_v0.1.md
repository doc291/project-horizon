# HORIZON_V1_COMPONENT_INTERACTION_CANON · v0.1

**Status** · Design governance · pre-implementation
**Audience** · Future implementers, UX maintainers, reviewers
**Scope** · The operational UX language for Horizon V1 — what the system **MUST**, **SHOULD**, **MAY**, and **MUST NOT** look and behave like, independent of how it is built.

This document is the constitution. The React implementation, when it begins, **MUST** consume this canon and **MUST NOT** invent UX patterns ad hoc from screenshots or comp inspection. Where the implementation discovers a gap, this document is amended first — code follows.

The canon is derived from the v1 design corpus: the operator workspace (`Horizon V1.html`), the Decision Support Window (`dsw.jsx`), the variation studies (`Horizon V1 — Variations.html`), and the Replay & Incident Review exploration (`Horizon V1 — Replay & Incident Review.html`). Where the canon contradicts a comp, the canon wins.

**Language convention** — MUST / MUST NOT / SHOULD / SHOULD NOT / MAY follow [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) intent. "MUST" is non-negotiable; "SHOULD" admits deliberate exceptions with rationale; "MAY" is discretionary.

---

## 0 · Preamble — what the canon serves

Horizon V1 is a port operations intelligence platform. The interface mediates safety-critical decisions for VTSOs, Harbour Masters, Shift Supervisors, Port Executives, Stakeholders and Marine Infrastructure operators. Three operational truths shape every canon entry:

- **Under-3-second findability** — operators currently use whiteboards. Critical information must be locatable in under three seconds without scrolling, scanning, or hunting.
- **Decision authority is visible** — every state on screen carries a clear answer to "who can act on this?" and "who has acted on this?".
- **The chain is the product** — recommendation → response → outcome is the artefact V1 exists to produce. Visual choices that obscure the chain are anti-patterns.

The canon protects these truths.

---

## 1 · Canonical layout primitives

### 1.1 The four-region shell

Every operational and review surface in V1 **MUST** follow a single four-region shell:

```
┌──────────────────────────────────────────────────────────────────┐
│  TOP RIBBON · identity, port, status, role, time, audit          │
├────────────────┬────────────────────────────┬────────────────────┤
│                │                            │                    │
│  LEFT RAIL     │  CENTRE OPERATIONAL SPINE  │  RIGHT STATE /     │
│  · context     │  · the primary surface     │  CONTEXT RAIL      │
│  · roster      │  · tabs or timeline        │  · decisions /     │
│  · summary     │                            │    snapshot        │
│                │                            │                    │
└────────────────┴────────────────────────────┴────────────────────┘
```

**1.1.1 Top operational ribbon** — `MUST` carry: identity (logo + port name), live status chips (vessels, movements, conflicts, conditions), role pill, data-source indicator, monotonic clock with seconds, AMS Group co-brand. In Replay, the ribbon also carries the audit-chain hash and chain-verification status.

**1.1.2 Left rail** — `MUST` host **browsing context**, never decisions. In operational mode: vessel roster + search + filter + audit log access. In Replay mode: incident summary, vessels involved, key decision index.

**1.1.3 Centre operational spine** — `MUST` host the primary work surface. In operational mode: tabbed dashboard (Dashboard · Berth Timeline · Shift Log · VTS · Pilotage · Performance). In Replay mode: the swimlane timeline. The spine is the one region that is **screen-state-defining** — its content determines what the rails surface.

**1.1.4 Right rail** — `MUST` host **active operator focus**: open Decisions and the Active Decision Card in operational mode; state-at-time-T snapshot in Replay mode. The right rail is the eye-flow destination — operators read left → centre → right and **land** on decisions.

### 1.2 Region widths

Region widths are governance-set, not stylistic preferences:

| Region | Operational mode | Replay mode | Floor |
|---|---|---|---|
| Top ribbon height | 72 px | 72 px (+ 38 px audit sub-ribbon) | 64 px |
| Left rail width | 360 px | 280 px | 240 px |
| Centre spine | 1fr (fluid) | 1fr (fluid) | 720 px |
| Right rail width | 380 px | 320 px | 280 px |

The design target is **1440 px viewport minimum**. Below 1280 px the operator workspace `SHOULD` horizontally scroll, never collapse. See §9 for the mobile carve-out.

### 1.3 Conditions ribbon

In operational mode only, a second ribbon (`64 px`) sits below the top ribbon hosting weather/tide/UKC tiles. This ribbon is **persistent across all tabs** and never collapses. It is the single canonical surfacing of environmental conditions.

### 1.4 Timeline placement

The timeline appears in **two distinct places** and **MUST NOT** appear in any third:

- **Operational mode** — as the *Berth Timeline tab* inside the centre spine. Berth-row-as-Gantt, vessel blocks as time segments.
- **Replay mode** — as the *centre spine itself*, full-width, with horizontal time axis and four swimlanes (System · Operators · Vessels · Weather).

Decision Support Window (DSW) timelines, audit-log entries, and shift-log tables are **not timelines** in the canonical sense; they are tabular or step-based and `MUST NOT` adopt the timeline visual language (axis + swimlanes + glyphs).

### 1.5 DSW placement

The Decision Support Window is a **modal full-screen overlay** anchored to the right edge:

- `MUST` open as a fixed-position panel of `min(960px, 100vw)` width, sliding in from right
- `MUST` darken and blur the underlying workspace
- `MUST` carry its own header (severity pill, signal type, decision ID, ticking deadline timer, close button)
- `MUST` walk the operator through the canonical 4-step flow (§7.2)
- `MUST NOT` be dismissable by clicking the backdrop while a deadline is < 30 minutes — only the explicit close button

---

## 2 · Canonical interaction patterns

The verbs below are the operational vocabulary of V1. Every screen, every component, every server response participates in this vocabulary or it does not belong in V1.

### 2.1 Acknowledge (ACK)

**Meaning** — the operator has seen and accepted a recommendation. ACK is "I have read this, I will act."
**Required** — a single click on the primary CTA of the recommendation card.
**Recorded** — actor + timestamp + recommendation ID.
**Not** — a commitment to apply the recommendation. ACK + COMMIT are distinct events.
**UI rule** — ACK affordances `MUST` be visually distinct from COMMIT. Green outline pill for ACK; filled green button for COMMIT.

### 2.2 Commit

**Meaning** — the operator has applied a decision to the live schedule. This is the binding act.
**Required** — explicit confirmation, never auto-fire. Inside the DSW, a single committing button after rationale capture.
**Recorded** — actor + timestamp + decision ID + selected option + audit chain hash.
**UI rule** — `MUST` use the canonical commit colour (`var(--success)`) and a shield-check icon. The commit button `MUST` be the only filled-green button in any view.

### 2.3 Defer

**Meaning** — the operator parks the recommendation deliberately, with reason.
**Required** — rationale text (minimum 8 chars, free-form) + a defer-until timestamp (optional; default = recommendation deadline).
**Recorded** — actor + timestamp + reason + defer-until.
**UI rule** — amber styling (`var(--warning)`). The defer affordance `MUST NOT` appear without a rationale field in the same UI moment — defer is never a one-click action.

### 2.4 Override

**Meaning** — the operator commits a *different* action than the system recommended.
**Required** — authority verification (role pill + permission check), rationale text, explicit selection of an alternative option.
**Recorded** — actor + timestamp + rejected recommendation + chosen alternative + rationale + authority used.
**UI rule** — purple styling (`var(--purple)`, `#A78BFA`). The override badge `MUST` be visible on the resulting decision card *and* on any downstream surfaces that reference that decision. There is no "soft override".

### 2.5 Escalate

**Meaning** — the operator routes the decision to a higher authority.
**Required** — explicit target role (HM / DPM / Port Master), rationale text.
**Recorded** — actor + timestamp + route (from-role → to-role) + rationale + downstream receiver ACK time.
**UI rule** — blue styling (`var(--blue)`, `#60A5FA`). Escalations `MUST` show *both* the originating operator's badge *and* the receiver's badge on the resulting decision. The chain forks visibly.

### 2.6 Inaction (no operator authorship)

**Meaning** — the recommendation deadline expired without ACK, DEFER, OVERRIDE or ESCALATE.
**Required** — nothing from the operator; the system records the lapse automatically.
**Recorded** — the original recommendation + deadline + the absence of any response event.
**UI rule** — muted grey on the timeline (EXPIRED); promoted to red (INACTION) only when a reviewer designates it as such post-incident. The operator-facing UI `MUST NOT` use the word "inaction"; the reviewer-facing UI `MUST`.

### 2.7 Scrub / playback (Replay-only)

**Meaning** — move the playhead through historical time.
**Required** — drag, click-to-position, keyboard arrow, jump-to-event chip.
**Recorded** — viewer interaction is *not* recorded as an audit event; entering Regulator mode *is*.
**UI rule** — the playhead is the single source of truth. Every panel that depends on time `MUST` re-bind on playhead change. There is no "live mode" inside Replay; the playhead always represents a specific historical moment.

### 2.8 Hover / detail

**Meaning** — reveal additional information without committing the screen to it.
**Required** — pointer hover (desktop), long-press (touch).
**UI rule** — hover detail `MUST` appear within 200 ms; `MUST` dismiss on pointer-out without animation latency; `MUST NOT` carry actionable affordances (no buttons inside hover popovers). Action requires a click.

### 2.9 Timeline navigation (Replay)

**Meaning** — move through events along the timeline.
**Affordances ranked by frequency of use** —
1. Drag scrubber (continuous)
2. Click event glyph on swimlane (snap-to)
3. Jump-chip click (next critical, next override, next escalation, resolved)
4. Keyboard `[` / `]` (event step), `{` / `}` (lifecycle event step)
5. Click row in left-rail key-decision list (snap-to)

**UI rule** — `SHOULD` always be possible to navigate the timeline without the mouse leaving the playback controls. The transport row is the locus.

### 2.10 Panel expansion / collapse

**Meaning** — reveal more detail within a card or row.
**Required** — chevron affordance, click anywhere on the card header.
**UI rule** — expansion `MUST` be local (push siblings, do not overlay), `MUST` animate in 150–200 ms, `MUST` remember state per session. Alert cards, decision cards, vessel rows, audit rows are all expandable.

### 2.11 Incident drill-in

**Meaning** — move from a live or recent operational view into the Replay of a specific incident.
**Required** — single click on an incident chip / row / decision ID anywhere in V1.
**UI rule** — the transition `MUST` carry the chosen incident's time window as the Replay window. The operator's workspace state (active tab, scroll position) `MUST` be preserved on return.

---

## 3 · Canonical visual semantics

### 3.1 Severity colours

Severity is **never decorative**. Each colour has exactly one operational meaning. Mis-use is an anti-pattern (§10).

| Token | Value | Meaning |
|---|---|---|
| `--critical` | `#FF4757` | Active critical signal · imminent harm · system fault |
| `--warning` | `#FFA502` | Caution · degraded condition · deferred state |
| `--success` | `#2ED573` | Acknowledged · committed · resolved · cleared |
| `--info` | `#49BCBF` (teal) | System recommendation · informational · advisory |
| `--text-muted` | `#4A6A8F` | Expired · inactive · disabled · metadata |

**Rule** — these five colours **MUST NOT** be used outside their assigned meaning. If a UI element needs a colour and none of the five fit, use a `--text-secondary` or `--bg-surface-3` neutral, or extend the palette through a canon amendment.

### 3.2 Lifecycle colours

The lifecycle palette extends the severity palette with two additional tokens used exclusively in event lifecycle visualisation:

| Token | Value | Meaning |
|---|---|---|
| `--purple` | `#A78BFA` | Override (operator chose otherwise) |
| `--blue` | `#60A5FA` | Escalation (operator routed upward) |

Purple and blue **MUST NOT** appear outside lifecycle visualisation (timeline glyphs, decision badges on override / escalated decisions, key-decision left-borders).

### 3.3 Glyph alphabet

The timeline uses a small fixed alphabet of shapes. Operators learn this alphabet on first use; it `MUST NOT` be extended without canon amendment.

| Shape | Meaning |
|---|---|
| **● Circle** | Signal · event marker · acknowledged action |
| **■ Square** | Artefact (options generated, commit, defer) |
| **◆ Diamond** | Decision point (recommendation, override) |
| **⇡ Stacked diamond** | Escalation |
| **✕ Cross** | Inaction (deadline expired with consequence) |
| **○ Empty circle** | Expired (deadline lapsed, no operator response) |

Glyph **colour** carries lifecycle state; glyph **shape** carries the kind of event. Both axes are required to disambiguate; neither alone is sufficient.

### 3.4 State semantics in pills

Pills (badges) `MUST` use one of the canonical tones (`CRITICAL`, `WARNING`, `ADVISORY`, `INFO`, `SUCCESS`) plus the lifecycle tones (`OVERRIDE`, `ESCALATED`). Pills `MUST` use uppercase + 0.08em letter-spacing + 700 weight + 10–11 px font.

**Variants** —
- `tinted` — coloured background + matching foreground + 1px border (default)
- `outline` — transparent background + coloured border + matching foreground (for low-emphasis labels, category tags)

### 3.5 Operational vs reviewer semantics

The same data is shown differently in operational vs review surfaces:

| Concept | Operational surface | Reviewer surface |
|---|---|---|
| Recommendation | Big card, ticking deadline, primary CTA | Diamond glyph on timeline, hover for detail |
| Override | Purple badge with "OVERRIDE" label | Hollow diamond on operator swimlane + thread back to system swimlane |
| Inaction | Not used — operator UI cannot label its own absence | Red cross glyph + reviewer-added annotation |
| Conditions | Persistent ribbon at top | Right-rail snapshot bound to playhead |

Reviewer-only semantics (INACTION designation, audit hash chips, chain-verification status) `MUST NOT` appear in operational mode.

---

## 4 · Canonical operational states (lifecycle)

The recommendation lifecycle has **seven states**. The state set is closed: implementations `MUST NOT` introduce new states; they `MUST` map every recommendation to exactly one of these at all times.

| # | State | Authored by | Glyph | Colour | Persists to chain? |
|---|---|---|---|---|---|
| 1 | RECOMMENDED | System | ◆ | Teal | yes |
| 2 | ACKNOWLEDGED | Operator | ● | Green | yes |
| 3 | COMMITTED | Operator | ■ | Green | yes (binding) |
| 4 | DEFERRED | Operator | ⏳ amber square | Amber | yes |
| 5 | OVERRIDDEN | Operator | ◇ hollow diamond | Purple | yes |
| 6 | ESCALATED | Operator | ⇡ stacked diamond | Blue | yes (chain forks) |
| 7 | EXPIRED | System (auto at deadline) | ○ | Muted | yes |
| 8* | INACTION | Reviewer (post-hoc only) | ✕ | Critical | yes (annotation) |

\* INACTION is a reviewer-applied designation atop an EXPIRED state. It is not authored at operational time and `MUST NOT` appear in operational surfaces.

### 4.1 Valid transitions

```
RECOMMENDED ──▶ ACKNOWLEDGED ──▶ COMMITTED      (canonical happy path)
            ──▶ ACKNOWLEDGED ──▶ DEFERRED ──▶ ACKNOWLEDGED ──▶ COMMITTED
            ──▶ ACKNOWLEDGED ──▶ OVERRIDDEN     (committed with alternative)
            ──▶ ACKNOWLEDGED ──▶ ESCALATED ──▶ (new chain under receiver)
            ──▶ EXPIRED                          (no operator engagement)
            ──▶ ACKNOWLEDGED ──▶ EXPIRED         (engagement but no decision)

EXPIRED     ──▶ INACTION                         (reviewer designation only)
```

Implementations `MUST` enforce these transitions server-side and `MUST NOT` permit out-of-order writes.

### 4.2 What each state preserves

Each state carries a minimum payload on the audit chain:

- **RECOMMENDED** — recommendation ID, inputs (which feeds), options generated, model confidence
- **ACKNOWLEDGED** — actor, role, timestamp, view-state (which UI surface acknowledged it from)
- **COMMITTED** — actor, role, timestamp, selected option, rationale (optional), resource lock applied
- **DEFERRED** — actor, role, timestamp, rationale (mandatory), defer-until
- **OVERRIDDEN** — actor, role, timestamp, rationale (mandatory), authority used, rejected recommendation, chosen alternative
- **ESCALATED** — actor, role, timestamp, rationale (optional), target role, receiver ACK time
- **EXPIRED** — original deadline, original recommendation
- **INACTION** — original EXPIRED record + reviewer ID + reviewer rationale + harm reference

---

## 5 · Canonical panel behaviour

Panels are typed by their **time-binding** and **role-binding** behaviour.

### 5.1 Binding categories

| Category | Time | Role | Examples |
|---|---|---|---|
| **Live-bound** | Now (server clock) | Current operator | Operational dashboard, conditions ribbon, vessel roster, active decision card |
| **Playhead-bound** | Replay playhead | Replay viewer + actor filter | Replay state-at-time-T panel, ownership chain, snapshot rails |
| **Incident-bound** | Incident open → close window | Reviewer | Replay left rail summary, key-decision index |
| **Static** | Set at render | None | Empty states, settings, help |

### 5.2 What rebinds on playhead change

In Replay, the playhead change is the *only* event that triggers panel state rebinding (excluding mode switches and actor filter changes). Every playhead change `MUST` rebind:

- Right-rail state-at-time-T panel (vessels, resources, conditions, open decisions)
- Active swimlane event highlight
- Audit-chain chip (hash at time-T)
- Left-rail key-decision row's "open/closed" indicator
- Any selected event detail card (re-rendered or dismissed if before-playhead)

### 5.3 What persists across playhead changes

- Selected timeline event (if still ≤ playhead)
- Reviewer bookmarks
- Mode setting (operational / executive / regulator)
- Actor filter (if any)
- Visible swimlanes (lane toggles)
- Annotation drafts in progress

### 5.4 What persists across mode changes

- Playhead position
- Selected event (if visible in the new mode)
- Annotations and bookmarks
- Audit-chain verification status

Mode changes **MUST NOT** discard time state. The reviewer can scrub in Operational mode, switch to Regulator mode, and the playhead `MUST` be at the same timestamp.

### 5.5 Filtering rules

- Filters **MUST** display a chip showing they are active.
- Filters that hide audit-chain events **MUST** display a count of hidden events ("3 events filtered").
- Filters **MUST NOT** silently remove evidence; the reviewer must be able to count what is being hidden.
- Mode-switching is not filtering — different mode = different density and emphasis, never different chain.

---

## 6 · Canonical mode system

V1 has **three modes**, applied to both operational and replay surfaces. Modes are not different products. They are framings of the same data.

### 6.1 Operational mode

**Audience** — VTSO, Harbour Master, Shift Supervisor, Marine Infrastructure
**Surfaces** — full operator workspace; full Replay detail (all swimlanes, all glyphs, all sub-second events)
**Visible** — every recommendation, every action, every weather update, every AIS ping
**Interactive** — full ACK / COMMIT / DEFER / OVERRIDE / ESCALATE in live; bookmarks + annotation in Replay
**Default for** — Operations roles

### 6.2 Executive mode

**Audience** — Port Executive, Port Stakeholder (read-only Decisions)
**Surfaces** — aggregated dashboard; collapsed timeline showing lifecycle events only
**Visible** — committed decisions, overrides, escalations, outcomes, summary KPIs
**Hidden** — AIS ping detail, sub-signal cascade refreshes, weather minor updates, hover-only diagnostic data
**Interactive** — read-only on Decisions; full read on Replay
**Default for** — Executive and Stakeholder roles

### 6.3 Regulator mode

**Audience** — external regulator (AMSA, NOPSEMA), incident review board
**Surfaces** — forensic Replay only; no live operator workspace
**Visible** — every event in tabular form, with audit hash, source feed, chain predecessor
**Hidden** — visual operational chrome (conditions ribbon decorations, role-coloured pills outside lifecycle context)
**Interactive** — view + export only; no annotation, no bookmark, no mode change
**Access** — opt-in, access-logged (entering Regulator mode is itself an audit event)
**Default for** — no role; explicitly opted into by Harbour Master or above

### 6.4 Cross-cutting mode rules

- The mode switcher **MUST** be in the top ribbon, always visible.
- Mode changes **MUST** be instantaneous (no transition longer than 150 ms).
- The audit chain **MUST** be invariant across modes — Regulator mode adds emphasis, never new data.
- Every export **MUST** declare the mode it was generated under.

---

## 7 · Canonical DSW (Decision Support Window) behaviour

### 7.1 What DSW is

The Decision Support Window is **the operator's primary act of authorship**. It is where the operator:

1. Reads what the system detected (Conflict detected)
2. Studies the downstream impact (Downstream impact)
3. Compares resolution options with trade-offs (Resolution options)
4. Commits a decision with rationale (Commit decision)
5. Sees the ledger entry confirming the commitment (Decision logged)

The five-step flow is canonical and `MUST NOT` be re-ordered.

### 7.2 What DSW is not

- **Not a wizard.** The operator may move backward freely; previous steps `MUST` remain editable until commitment.
- **Not a confirmation modal.** It carries substantive analysis — cascade modelling, resource impact, option metrics.
- **Not a notification surface.** Other alerts continue to surface in the left rail; DSW is for one decision at a time.
- **Not the only place a decision can be committed.** Simple ACKs from the right-rail decision card are also valid commit paths for low-severity recommendations.

### 7.3 DSW header invariants

The DSW header `MUST` always carry:
- Severity pill (`CRITICAL`, `WARNING`, `ADVISORY`)
- Signal type pill (`CONFLICT`, `WEATHER`, `RESOURCE`, `NAVIGATION`)
- Decision ID (monospace, `DSW-<n>` format)
- Live decision title using the canonical meaning-headline treatment (one word in teal, rest white) — this is **the** sanctioned use of `MeaningHeadline` in operational surfaces (§3)
- Decision-window countdown timer (HH:MM:SS, large mono, turns critical-red below 30:00)
- Close button (icon-only, top-right)

### 7.4 What may be simulated in M0

For implementation milestones, the following DSW behaviours `MAY` be mocked client-side without backend support:

- The five-step flow navigation (state machine in front-end)
- Static option metrics (safety / delay / cost / confidence) read from a fixture
- Rationale text capture (held in form state)
- Visual "commit" with no server write — the audit chain is not yet writable

This is acceptable for M0 demo. The operator-facing UI is correct; the persistence is deferred.

### 7.5 What requires backend truth later

The following `MUST NOT` ship without backend implementation:

- The recommendation itself — generated by the cascade model server-side
- Option metrics — derived from current resource roster, weather, DUKC, historical outcomes
- The deadline timer — computed against tide windows, pilot rosters, conditions
- The commit — must write to the audit chain with hash and signature
- The cascade visualisation — modelled from current port state, not a fixture

M0 deliverables that mock these `MUST` declare themselves as mocked in the UI (a small "DEMO" pill near the decision ID).

---

## 8 · Canonical Replay behaviour

### 8.1 Timeline-first

The Replay workspace **MUST** be organised around the timeline. The timeline is the spine; rails are views onto a moment on that spine. There is no "current view" in Replay, only "current time".

### 8.2 No glamorisation

Replay is opened on the worst days. The UI `MUST NOT`:

- Animate vessel positions smoothly across the timeline
- Play "cinematic" transitions between events
- Fade or zoom into critical moments for emphasis
- Use animation to indicate urgency

The only motion in Replay is the playhead. All other state changes are instantaneous on playhead reposition.

### 8.3 Evidence-preserving

The Replay UI **MUST**:

- Make every event sourceable to a feed at a timestamp (hover reveals source)
- Refuse to filter events without surfacing the filter count
- Refuse to re-order events for visual clarity (sequence is evidence)
- Embed the chain hash in every export, screenshot, and copy-to-clipboard action
- Show chain-verification status persistently at the top of the workspace

### 8.4 Deterministic ordering

Two reviewers opening the same incident `MUST` see identical timeline renders. Event order is:

1. Primary key — event timestamp (UTC, microsecond)
2. Secondary key — ingest sequence
3. Tertiary key — source priority (SYSTEM < OPERATOR < VESSEL < WEATHER) — for stable rendering of sub-second clusters

Display logic `MUST NOT` reorder events for any reason, including readability.

### 8.5 Chain visibility

The audit chain status `MUST` be visible at all times in Replay:

- **CHAIN VERIFIED** (green) — full chain hashes resolve; the replay is evidence
- **PARTIAL** (amber) — some events failed hash verification; affected events flagged inline
- **CHAIN BROKEN** (red) — replay is not evidence; export `MUST` be disabled

The chain status `MUST` be at the same screen position regardless of mode.

---

## 9 · Canonical mobile philosophy

Horizon V1 is a port-operator workstation product. Mobile is a **consumption** surface, never an authoring one.

### 9.1 What belongs on mobile

- Read-only Replay (collapsed lifecycle events, snapshot panel, no scrubber)
- Decision *notifications* (push alerts; tap to open in desktop)
- Conditions snapshot (current weather/tide/UKC)
- Vessel lookup (read-only roster, ETA, status)
- Shift handover briefs (read-only)

### 9.2 What never belongs on mobile

- DSW (decision support window) — too consequential for thumb-driven UI
- Commit / Override / Escalate — operator authority `MUST NOT` be exercised on mobile in V1
- Berth Timeline editing or scenario builders
- Map-based VTS interaction beyond read-only
- Regulator mode

### 9.3 Implementation rule

If a mobile screen affords an action that mobile is not allowed to author, the screen `MUST` route to a deep-link that opens the desktop workspace at the appropriate panel — not silently execute on mobile.

---

## 10 · Canonical anti-patterns (forbidden)

The following are **explicitly forbidden** in V1. New surfaces and components `MUST NOT` adopt them; existing patterns that drift into them `MUST` be amended.

### 10.1 Dashboard clutter
Adding KPI tiles, mini-charts, or "at a glance" surfaces beyond the canonical Dashboard tab. Operational density is curated, not heaped. **Anti-pattern**: a sidebar of nine new metrics added because "operators might want to see them".

### 10.2 Frontend-only RBAC assumptions
Hiding a button in the UI is not access control. Every commit, override, escalate, defer `MUST` be authority-checked server-side. The UI's role pill is a hint; the server is the gate.

### 10.3 Fake audit certainty
A "chain verified" badge that always shows green regardless of the underlying chain state. Three chain states are real (verified / partial / broken). UI `MUST NOT` collapse them for visual calmness.

### 10.4 Hidden timeline state
A timeline that filters events without showing it filtered. A scrubber that snaps without indicating it snapped. A playhead that jumps without the snapshot rebinding. Hidden state in Replay is evidence corruption.

### 10.5 Cinematic replay
Smooth-pan map animations. Vessel dots that drift across the map as the playhead moves. Easing on the playhead. Replay reads like a document, not like a film.

### 10.6 Animated AIS theatre
Live AIS pings rendered as pulsing dots on the operational map. Bigger glow for closer vessels. Spinning loaders on conditions. AIS data is dense and stable; animating it is decoration.

### 10.7 Unexplained colours
Using teal for a warning, amber for an info, or introducing a new accent colour for "design reasons". The palette is closed (§3.1, §3.2). Extensions require canon amendment.

### 10.8 Implementation-driven UX drift
Re-laying out the workspace because a component library renders differently. Replacing a canonical pill with a Material chip. The canon is the contract; library affordances follow.

### 10.9 MeaningHeadline overreach
Using the teal-meaning-word headline (`"See what's coming before it arrives"`) on operational dashboards, vessel rosters, conditions bars, or any live data surface. The treatment is reserved for the DSW header and marketing/onboarding/exploration surfaces. Operational surfaces are scannable without decorative emphasis competing with severity colours.

### 10.10 "Heads-up" overlays in Replay
Adding floating tooltips, callouts, or marketing-style annotations to historical replay screens. Replay carries only the operational chrome plus reviewer annotations as explicit audit events.

---

## 11 · Recommended implementation order

The canon is implementation-agnostic. The following is a **recommended** staging that respects dependency (audit chain underpins everything; DSW depends on cascade model; Replay depends on chain).

### 11.1 M0 — Demo-ready operator workspace (visual fidelity)

Build the canonical shell with **mocked server**. Every UI surface is wired to a fixture or stubbed endpoint.

- Top ribbon, conditions ribbon, four-region shell, all five canonical tabs (Dashboard, Berth Timeline, Shift Log, VTS, Pilotage)
- Vessel roster (left rail), Active Decision card + alerts (right rail)
- DSW open from decision card, five-step flow navigation, commit-with-no-write (UI only)
- Tweaks panel for role / scenario / conditions presets (demo scaffolding; not shipped)

**M0 declares its own mocking** — the UI carries a "DEMO" indicator when reading from fixtures.

### 11.2 M1 — Real recommendation lifecycle

The cascade model + audit chain become real.

- Recommendation generation server-side from live AIS + BoM + DUKC + roster feeds
- ACK / COMMIT / DEFER / OVERRIDE / ESCALATE write to the audit chain
- Authority verification on every write
- Decision timer computed from real deadlines (tide, pilot rotation, weather window)
- Shift Log table reads real events
- Audit log right-rail tab reads real chain events

M1 is the first milestone where the operational workspace is operationally useful.

### 11.3 M2 — Replay & Executive mode

The chain is now writable and queryable; Replay becomes viable.

- Replay workspace (the four-region shell reframed around the timeline)
- All seven lifecycle states rendered with canonical glyphs and colours
- State-at-time-T snapshot panel bound to playhead
- Three modes (Operational, Executive, Regulator) with mode-switch in top ribbon
- Audit-chain ribbon with verified / partial / broken states
- Bookmark and annotate (writes new chain events)

### 11.4 Replay-only future work (post-V1)

Deferred from V1 unless capacity allows:

- Cross-incident analytics (pattern detection across replays)
- Side-by-side replay of two incidents with synchronised playheads
- Live-to-replay handoff (continuous record visualisation)
- Mobile read-only Replay (collapsed vertical scroll)
- External evidence attachment (regulator letters, harm reports)
- Reviewer-team workflows (multi-reviewer chains, hand-off between reviewers)

### 11.5 What `MUST NOT` ship in M0

- A commit button that writes to anything other than client state without a "DEMO" indicator
- A "chain verified" badge with no chain
- Authority pills that imply server-side enforcement when the server is mocked
- Map overlays that simulate AIS feeds without a clear "SIMULATED" flag

---

## 12 · Recommendations

### 12.1 Treat this document as a living contract

When implementation discovers a gap — a behaviour the canon does not cover, or a canon entry that contradicts an operational reality — **the canon is amended first**. Code follows. The amend cycle:

1. Implementer raises a canon ticket with the gap or contradiction.
2. Design + Ops review the proposed amendment.
3. Amendment is committed to the canon (versioned).
4. Implementation proceeds against the amended canon.

The cycle `SHOULD NOT` exceed one working day in normal cases.

### 12.2 Build the canon's components first, screens second

The component vocabulary defined here (Pill, Card, Decision Card, Alert Card, Vessel Row, Conditions Tile, Timeline Glyph, Mode Switcher, Audit Chip) is a closed set. Build these as canonical components with full state coverage *before* assembling screens. Screens then become compositions; visual drift gets pinned.

### 12.3 Write the canonical empty states

Every panel needs an empty state. The empty state language is operational, not decorative:

- "No active conflicts — all clear"
- "No decisions awaiting your authority"
- "No events in the last 24h"
- "Replay window contains no audit events" (this is suspicious; flag it)

Empty states `MUST NOT` use illustrations, mascots, or generic stock UX language.

### 12.4 Reserve teal for what teal means

`#49BCBF` is the system-recommendation colour and the brand signature. The longer V1 operates, the more meaning operators will attach to teal — "the system is suggesting something". Protect this association. Do not use teal for decorative emphasis, hover states (use border lift instead), or "active" indicators outside system-authored contexts.

### 12.5 Plan the audit-chain UI before the audit-chain implementation

The chip, the ribbon, the per-event hash hover, the three chain states, the export-carries-chain rule — all of these are UI commitments. The backend `SHOULD` design its audit-chain emit format to match what the canon already commits to surfacing. UI/backend chain alignment `SHOULD` be confirmed before M1.

### 12.6 Avoid pre-emptive theming

Light mode, role-themed palettes, density modes beyond compact/regular — none are sanctioned in V1. The Horizon Dark palette is the surface. Themability is not a V1 concern.

### 12.7 Document every override of this canon

If a future implementer ships a component that deliberately departs from this canon, the departure `MUST` be documented in the component's source-level comment and in the canon's amendment log. Silent drift is the threat the canon exists to prevent.

---

## Appendix A · Source artefacts

This canon is derived from the V1 design corpus. Where the canon is silent, these artefacts may be referenced for visual specifics, but they are **examples**, not law:

- `Horizon V1.html` — main operator workspace prototype
- `panels.jsx` — left/right rail composition; alert + decision + vessel-roster patterns
- `dsw.jsx` — five-step DSW flow
- `tabs.jsx` — centre-spine tab compositions
- `components.jsx` — atom library (Icon, Pill, Card, Dot, MeaningHeadline)
- `data.js` — sample fleet, conditions, alerts, decision fixture
- `Horizon V1 — Variations.html` — decision-card layout + cascade-visualisation studies
- `Horizon V1 — Replay & Incident Review.html` — Replay UX exploration (the authoritative reference for §8)

When canon and artefact disagree, the canon wins.

---

## Appendix B · Glossary

| Term | Meaning |
|---|---|
| **ACK** | Acknowledge — operator has seen and accepted a recommendation |
| **Audit chain** | The cryptographically linked sequence of all events in V1 |
| **Canon** | This document; the operational UX contract |
| **Cascade** | The downstream effect modelling of a primary signal |
| **Chain hash** | The cryptographic hash of an audit chain segment |
| **DSW** | Decision Support Window — modal full-screen overlay for decision authoring |
| **Glyph** | A single timeline event marker shape |
| **HM** | Harbour Master |
| **Lifecycle** | The state machine of a recommendation from issue to closure |
| **Meaning-headline** | The teal-coloured "meaning word" typographic treatment, reserved for DSW header and marketing |
| **Mode** | Operational / Executive / Regulator framing of the same data |
| **Playhead** | The time cursor in Replay; the spine of the workspace |
| **Replay** | The post-incident operational reconstruction environment |
| **Right rail** | The decision/state panel on the right of the four-region shell |
| **Snapshot panel** | The right-rail content in Replay; state-at-time-T reconstruction |
| **Swimlane** | A horizontal time-band in the Replay timeline, one per actor type |
| **VTSO** | Vessel Traffic Services Operator |

---

**End of canon · v0.1**

*This document supersedes any conflicting screen capture, code comment, or oral guidance. To propose an amendment, raise a canon ticket with a specific section reference and a rationale grounded in operational reality.*
