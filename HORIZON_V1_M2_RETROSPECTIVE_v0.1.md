# Horizon V1 — M2 Retrospective (v0.1)

**Document status:** Draft for review
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT
**Execution agent:** Claude
**Effective baseline:** `origin/main @ 354e5fc`
  (post-PR #69 — M2 visual remediation + HC-002)
**Date:** 2026-05-21
**Scope of authority:** Records the full closure of Milestone M2
  of the Horizon V1 programme, captures lessons learned, governance
  observations, and recommendations for use **before** Platform
  Foundation Phase 0 begins. This document is descriptive and
  advisory — it does **not** authorise Platform Foundation
  implementation, and it does **not** change any code,
  infrastructure, or configuration.

**Cited governing documents (all on `main`):**
- `HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md` (PR #53, `8c73c55`)
- `HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md` (PR #54, `d42b9e8`)
- `HORIZON_M2_ALIGNMENT_REVIEW_v0.1.md` (PR #60, `04aed14`)
- `HORIZON_V1_OPERATIONAL_UX_DIRECTION_v0.1.md` (PR #58, `cc2f2ea`)
- `HORIZON_V1_OPERATIONAL_PLATFORM_WORKFLOWS_v0.1.md` (PR #59, `763be55`)
- `HORIZON_V1_PLATFORM_FOUNDATION_v0.1.md` (PR #68, `0228265`)
- `HORIZON_PILOT_PROXIMITY_COMPANION_APP_v0.1.md` (PR #57, `dbc7ae8`)
- `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md` (PR #51, `b0d9ff2`)
- `HORIZON_V1_M1_RETROSPECTIVE_v0.1.md` (PR #52, `dde3a84`)
- `HORIZON_CAPABILITY_BACKLOG.md` (HC-001 + HC-002 entries)
- Beta 10 Immutability Rule (in force)

---

## 1. Executive summary

Milestone M2 ("Read-only centre-spine surface expansion + Centre
Panel Navigation") is **closed** as of 2026-05-21. M2 extended the
Horizon V1 frontend operational surface from the partial Dashboard
tab (M1) to all six Canon §1.1.3 centre-spine tabs (Dashboard, VTS,
Berth Timeline, Shift Log, Pilotage, Performance placeholder),
wired by an in-memory Centre Panel Navigation primitive. A visual
remediation pass at the close of M2 moved the operator alerts /
decision card placeholder from the LEFT rail to the RIGHT rail
(per Operational UX Direction §3.1), rebalanced the conditions
strip, and captured the future VTS spatial operating surface as
backlog item HC-002.

M2 introduced **no live Beta 10 calls, no production `/api/*` calls,
no auth, no RBAC, no operator-action writes, no audit emission, no
Stage E-prod activity, no database dependency, no `server.py`
change, no root `railway.toml` change, and no production state
change.** Beta 10 was not touched throughout the entire M2 cycle —
the Beta 10 Immutability Rule was honoured by construction.

This retrospective records what M2 delivered, what worked, what
should change before Platform Foundation Phase 0, and the items
deferred to later milestones. It is descriptive and advisory —
**it does not authorise the next milestone.**

---

## 2. M2 final close status

**M2: CLOSED with notes.**

| Event | Reference | Date |
|---|---|---|
| M2 Scope Proposal merged | PR #53, `8c73c55` | 2026-05-20 |
| M2 Implementation Plan merged | PR #54, `d42b9e8` | 2026-05-20 |
| M2 Alignment Review merged | PR #60, `04aed14` | 2026-05-21 |
| M2 Phase 0 authorisation | (verbal/in-session) | 2026-05-21 |
| M2 Phase 1 — slice 1 merged (VTS) | PR #61, `5c84ab4` | 2026-05-21 |
| M2 Phase 1 — slice 2 merged (Berth Timeline) | PR #62, `5759fd5` | 2026-05-21 |
| M2 Phase 1 — hygiene slice merged (logos) | PR #63, `a0634b0` | 2026-05-21 |
| M2 Phase 1 — slice 3 merged (Shift Log) | PR #64, `e59f457` | 2026-05-21 |
| M2 Phase 1 — slice 4 merged (Pilotage) | PR #65, `888f813` | 2026-05-21 |
| M2 Phase 1 — slice 5 merged (Performance placeholder) | PR #66, `7cd53da` | 2026-05-21 |
| M2 Phase 1 — final slice merged (Centre Panel Navigation) | PR #67, `eb4d441` | 2026-05-21 |
| M2 acceptance verification | (in-session report) | 2026-05-21 |
| Visual remediation slice merged (right-rail + conditions strip + HC-002) | PR #69, `354e5fc` | 2026-05-21 |
| M2 closed | this retrospective | 2026-05-21 |

**Outstanding items** (deferred, not blocking M2 closure):
- A8 — Tony-side `horizon-v1-sandbox` source-branch switch (optional)
- A11 — Tony's Beta 10 post-M2 visual check (pattern: M1 Retrospective §2)
- A12 — Smart Ocean X independence affirmation in this retrospective
  (delivered below; see §6)

---

## 3. PR summary (PRs #61–#69)

M2 was delivered as a slice-by-slice sequence per the Phase 0
readiness report. Each slice was individually tightly scoped,
individually reviewed by Tony, and individually merged.

| PR | Title | Files | +/- | Merge SHA |
|---:|---|---:|---|---|
| **#61** | feat(v1-m2): VTS tab — adapter + read-only vessel/conflict panes (slice 1) | 7 | +726 / -0 | `5c84ab4` |
| **#62** | feat(v1-m2): Berth Timeline tab — adapter + read-only Gantt lanes (slice 2) | 8 | +1085 / -0 | `5759fd5` |
| **#63** | fix(v1-m2): use real Horizon + AMS logos in header (hygiene slice) | 5 | +111 / -17 | `a0634b0` |
| **#64** | feat(v1-m2): Shift Log tab — adapter + read-only derived event log (slice 3) | 6 | +675 / -0 | `e59f457` |
| **#65** | feat(v1-m2): Pilotage tab — adapter + read-only coordination view (slice 4) | 6 | +751 / -0 | `888f813` |
| **#66** | feat(v1-m2): Performance placeholder tab (slice 5) | 3 | +204 / -0 | `7cd53da` |
| **#67** | feat(v1-m2): Centre Panel Navigation — final M2 slice | 5 | +360 / -5 | `eb4d441` |
| **#69** | chore(v1-m2): right-rail decision-card placement + conditions strip rebalance + HC-002 | 7 | +485 / -64 | `354e5fc` |

Total M2 delta: **47 files changed, +4,397 / -86**, exclusively
under `frontend/` plus one root-level markdown addition
(`HORIZON_CAPABILITY_BACKLOG.md` HC-002 entry).

In parallel with M2 implementation, two documentation PRs landed
on `main`:
- **PR #60** — M2 Alignment Review (governance reference,
  recommended "proceed with guidance notes")
- **PR #68** — Horizon V1 Platform Foundation v0.1 (planning
  document for the next phase)

---

## 4. M2 scope completed — final acceptance criteria status

Sourced from M2 Scope Proposal §15 / M2 Implementation Plan §16,
verified in the M2 acceptance verification report (in-session,
2026-05-21).

| # | Criterion | Status | Notes |
|---|---|---|---|
| A1 | M2 fixtures reused unchanged; no production touch | **PASS** | `frontend/public/fixtures/` untouched throughout M2 |
| A2 | Tab-specific adapters per ADN §2.2; no `summaryAdapter` shape change | **PASS** | 4 new adapters (vts, berthTimeline, shiftLog, pilotage); `summaryAdapter.js` unchanged |
| A3 | `useSummary` reused unchanged | **PASS** | Polling hook + fetch wrapper unchanged |
| A4 | Tabs delivered (BT, SL, VTS, Pilotage, Performance placeholder) — read-only | **PASS** | 5 new tab components; no operator-action affordances in tab bodies |
| A5 | Centre Panel Navigation; in-memory state | **PASS** | `useState('dashboard')`; no URL routing; no persistence; no port switcher |
| A6 | Vitest tests added | **PASS** | 275 tests / 24 files (was 86 / 10 at M1 baseline) |
| A7 | `tests/test_beta10_regression.py` 46/46 throughout | **PASS** | Green at every merge |
| A8 | Railway sandbox `horizon-v1-sandbox` serves M2 from `feat/v1-m2` | **DEFERRED** | Tony-side; not blocking M2 closure |
| A9 | Bundle hash matches local build; no production URLs; only `/fixtures` as fetch target | **PASS** | Bundle inspection: `VITE_API_BASE` defaults to `/fixtures`; production-URL probe hits are benign disclosure copy only |
| A10 | PR merged to `main` with tightly scoped diff | **PASS WITH NOTE** | Delivered as 7 slice PRs (#61–#67) + 1 hygiene + 1 visual remediation per the Phase 0 plan; each slice individually tightly scoped |
| A11 | Tony's Beta 10 post-M2 visual check passes | **DEFERRED** | Tony-side post-merge step (per M1 Retrospective §2 pattern) |
| A12 | Smart Ocean X independence confirmed in M2 retrospective | **PASS** — see §6 below |
| A13 | No new auth, writes, audit emission, Stage E-prod, DB dependency, `server.py` change, root `railway.toml` change | **PASS** | Protected-files probe returned 0 hits across the entire M2 delta |

**Acceptance result: ACCEPT WITH NOTES.** Deferrals are Tony-side
governance steps, not implementation defects.

---

## 5. Visual walkthrough findings + remediation

During Tony's visual walkthrough at the close of M2 Phase 1,
three findings were surfaced. All three were addressed in PR #69
(the visual remediation slice) without breaking any M2 invariant:

### 5.1 Right-rail decision-card placement

**Finding:** the operator alerts / decision card placeholder
content was on the LEFT rail. This contradicted the merged
Operational UX Direction PR #58 §3.1, which states that the right
rail is the long-term coordination / action surface.

**Remediation (PR #69):**
- `LeftPanel.jsx` — operator alerts placeholder removed; replaced
  with a "Supporting context" placeholder (muted tone) for
  lower-priority content.
- `RightPanel.jsx` — old M0 "Vessel roster & audit log" placeholder
  replaced with the operator alerts / decision card placeholder,
  carrying an "ACTION & COORDINATION" pill and an explicit
  "Not yet implemented: ACK / DEFER / APPLY / OVERRIDE / ESCALATE —
  these require server-authoritative auth, RBAC and audit
  emission, all M3+ scope" disclosure.
- 14 new tests in `tests/layout/rails.test.jsx` pin the
  content-placement invariants on both rails.

### 5.2 Conditions strip rebalance

**Finding:** the conditions banner / strip felt visually
left-weighted and unbalanced.

**Remediation (PR #69):**
- `ConditionsBar.jsx` — wrapped into three flex regions: anchor
  (left, fixed 96 px, GOOD pill), centred tiles (flex 1,
  `justify-content: center`, six environmental tiles),
  symmetric spacer (right, fixed 96 px, aria-hidden).
- Information hierarchy unchanged — exactly six environmental
  tiles (WIND / SWELL / VIS / PRESSURE / TIDE / UKC).
- 9 new tests in `tests/layout/conditionsBar.test.jsx` pin the
  three-region markup and the six-tile invariant.

### 5.3 VTS map / spatial surface — deferred as HC-002

**Finding:** the VTS view should ultimately show a real map /
spatial vessel view. VTSOs need spatial awareness; a list-only
view conveys identity and conflict membership but not *where*
or *which way*.

**Capture (PR #69):** added a new HC-002 entry to
`HORIZON_CAPABILITY_BACKLOG.md`, mirroring the HC-001 (Pilot
Coordination Awareness Module) structure. HC-002 captures
operational problem, "Horizon does NOT" boundary (no command
capability, no navigation instructions, no replacement of
official VTS), user roles, inputs / outputs, decision authority,
operational + commercial value, regulatory sensitivities,
technical notes (cites Platform Foundation §7 / §8 / §9 / §12),
UI concepts, and explicit "not authorised for implementation"
status.

**Explicit decision:** the VTS map / spatial surface is **NOT**
implemented in M2. The M2 VTS tab (PR #61) is a read-only list
precursor; the M2 `vtsAdapter` contract is forward-compatible
with a spatial replacement. The spatial surface arrives in M3+
under a separate scope proposal.

---

## 6. Smart Ocean X independence — affirmed

Per M2 Scope Proposal §15 A12, this retrospective affirms that
the Smart Ocean X architectural dependency **remains formally
closed** under the Horizon Independence / Architecture Reset
(PR #51, `b0d9ff2`).

The full M2 delta (PRs #61–#69) was audited at close:
- The string `Smart Ocean X` appears in the M2 delta exactly
  once, in a test assertion (`expect(html).not.toMatch(/Smart Ocean X/i)`)
  inside `tests/features/performance.test.jsx` (the Performance
  placeholder test). The assertion **enforces absence**.
- No code, no copy, no documentation introduced during M2
  references Smart Ocean X as a dependency, substrate, or partner.
- The Independence Reset framing
  ("Horizon" / "AMSG Horizon" / "Horizon by AMS Group") is
  preserved throughout the M2 artefacts.

The Kyber boundary is also in force. The M2 Pilotage tab (PR #65)
explicitly does not introduce Kyber-related fields and the test
suite asserts `Kyber` appears nowhere in the rendered Pilotage
markup.

---

## 7. What M2 proved

M2 proved the following properties of the V1 frontend architecture
in production-adjacent conditions, without touching production:

1. **The fixture-fed read-only pipeline scales to all six Canon
   §1.1.3 centre-spine tabs.** From a single Dashboard tab (M1)
   to six fully composed read-only operational surfaces (M2), no
   new live backend dependency was introduced.
2. **The adapter pattern is the right seam to the future live
   API.** Four new tab-specific adapters (`vtsAdapter`,
   `berthTimelineAdapter`, `shiftLogAdapter`, `pilotageAdapter`)
   slot in beside the existing M1 adapters without modifying the
   `summaryAdapter` 20-key contract.
3. **In-memory centre-panel navigation is sufficient for M2.**
   No URL routing, no localStorage, no per-user state. The seam
   for future routing or per-role default-tab logic is the
   `useState` in `CenterPanel.jsx`.
4. **Read-only across the entire centre spine is achievable
   without operator-friction.** Every tab disclosed its read-only
   nature; no operator-action affordances leaked into any tab
   body.
5. **The Beta 10 Immutability Rule holds under heavy V1 frontend
   work.** Throughout M2's 9 PRs, zero protected files were
   touched; Beta 10 production state remained unchanged.
6. **Slice-by-slice delivery worked.** Each slice was reviewable
   in minutes, tested independently, and merged independently.
   Aggregate risk per slice was very low; aggregate progress per
   day was high.
7. **Visual disclosure copy is load-bearing.** Each placeholder
   tab carries explicit copy about what it is and is not (e.g.
   Shift Log: "DERIVED · NOT AUDIT LEDGER"; Pilotage:
   "SIMULATED · NOT A PILOTAGE OPERATING SYSTEM"; Performance:
   "PLACEHOLDER · NO ANALYTICS"). The right rail's M3+ explicit-
   deferral disclosure is the same pattern. Operators and
   reviewers can read the surface and know exactly what it is.

---

## 8. What worked well

8.1 **Phase 0 readiness report.** Surveying the worktree state,
confirming PR references, enumerating the slice sequence, and
declaring stop conditions before any code was written produced
an unambiguous Phase 1 entry point.

8.2 **Slice-by-slice authorisation.** Tony authorised each slice
explicitly. Each slice opened a PR, paused for review, and
merged with a separate "Approved for merge" message. The pattern
caught issues at the boundary (test pattern over-breadth in the
Performance and Pilotage slices) without disturbing other slices.

8.3 **The M2 Alignment Review (PR #60).** Pre-implementation,
PR #60 surfaced eight implementation-period drift risks. Each
slice was checked against those risks at commit time. Two
risks (dashboard-first creep; demo-style interaction patterns)
never materialised; the others (frontend-heavy logic; fixture
coupling; Beta 10 patterns) were actively avoided.

8.4 **react-dom/server.renderToStaticMarkup for component tests.**
M1 left no DOM-test framework in place. Using
`react-dom/server.renderToStaticMarkup` (which ships with
`react-dom`, already a dependency) let us write markup-shape
tests across all six new tabs and the centre-panel navigation
without adding `@testing-library/react`, `jsdom`, or `happy-dom`.
The test-without-DOM pattern fits the read-only M2 posture
perfectly.

8.5 **Disclosure copy as a design tool.** Every tab and every
placeholder used disclosure copy to signal what it is and is
not. This kept reviewers oriented and prevented operators (and
future maintainers) from reading the M2 surface as more than
it is.

8.6 **Tony-side visual walkthrough.** Surfaced three real issues
that automated tests could not catch (right-rail placement,
conditions strip rhythm, missing VTS spatial intent). The
remediation slice (PR #69) handled all three in a single
small, reviewable diff.

8.7 **No "while we are here" fixes.** The hygiene slice (PR #63
logo correction) was scoped narrowly and merged separately
from the feature slices. The visual remediation slice (PR #69)
combined three related layout concerns but did not extend into
adjacent work.

8.8 **Beta 10 untouched throughout.** No Railway API call from
the agent during the entire M2 cycle. The Immutability Rule was
honoured by construction (only `frontend/` files modified) and
verified by the protected-files probe at every merge.

---

## 9. Key M2 lessons

### 9.1 Test patterns must distinguish UI affordances from disclosure copy

Three slices (Performance, Pilotage, Shift Log) had test patterns
that initially matched legitimate disclosure copy as if it were
an affordance violation (e.g. `not.toMatch(/Fatigue/)` matched
the disclosure "No fatigue or competency validation"; `not.toMatch(/Filter/)`
matched the disclosure "No filters in M2"; `not.toMatch(/Brisbane|Melbourne|.../)`
matched the disclosure "captured fixtures (Brisbane / Melbourne / Geelong / Darwin)").

**Lesson:** test patterns must look for actual UI controls
(`<button>`, `<input>`, `<select>`, ARIA-tab role with selected
state, etc.), not for words in product copy. Disclosure copy is
load-bearing and the test must not punish its presence.

### 9.2 Bundle growth is the natural consequence of wiring isolated components

After the Centre Panel Navigation slice (PR #67), the JS bundle
grew from 197 kB to 225 kB (+14 %). All five previously-isolated
M2 tab components were tree-shaken out of earlier builds
because nothing imported them; once `CenterPanel` did, they
shipped. This is **expected**, **anticipated** by the M2
Implementation Plan §14.9 bundle-size soft guard (M1 + 30 %), and
well within budget.

### 9.3 Centre Panel Navigation is not port switching

The M2 cycle generated an explicit governance clarification: the
final-slice "tab switcher" is centre-panel **view** navigation,
not **port** switching. Port access is governed by login / user
permissions / role-based access in the future Platform
Foundation work. The clarification was recorded in the
authorisation message and is now enforced by the
`centerPanelNav.test.jsx` test suite (absence of port-selector
UI control, absence of port-name UI labels in nav controls).

### 9.4 "Derived" is not "fake"

The Shift Log slice (PR #64) derives events from explicit
timestamped facts already in ViewSummary (vessel arrivals,
departures, detected conflicts). It does **not** invent
operator-action events or fabricate audit-ledger semantics.
The disclosure "DERIVED · NOT AUDIT LEDGER" makes the
observational nature explicit. **Lesson:** when the
underlying data is genuinely available, deriving a presentation
view from it is appropriate; the contract is that the
derivation is transparent and forward-compatible with the
authoritative source (the real audit ledger in M3+).

### 9.5 The fixture-shape gap is real and must be handled honestly

The VTS slice (PR #61) surfaced that `heading`, `speed`, `course`
are not present in the captured fixtures. The adapter deliberately
does **not** synthesise them — it documents the gap in the
adapter header and in tab copy. This is the correct posture per
M2 Implementation Plan §7.4 ("no fabrication") and Alignment
Review §3.3 ("no frontend-heavy logic"). A future fixture
refresh (Option 4b, separately authorised) is the path forward
when these fields are operationally needed.

### 9.6 Disclosure copy must avoid the test-pattern collision

When writing future tests, prefer to match UI structure (tag
names, ARIA roles, attribute values) rather than free-form
copy strings. The Pilotage and Performance test patterns
were tightened in their respective slices after first-pass
fails.

### 9.7 The Tony-side visual walkthrough is non-substitutable

Three issues that automated tests could not catch surfaced in
the Tony walkthrough at M2 close. **Lesson:** schedule the
visual walkthrough deliberately at every milestone close; do
not assume the test suite has caught everything that matters.

---

## 10. Governance lessons

10.1 **The Beta 10 Immutability Rule held.** Beyond the rule
itself, the *enforcement mechanism* (protected-files probe at
every merge) is what made the rule operational. Without the
probe, drift would happen by accident.

10.2 **The Phase 0 readiness report is a load-bearing artefact.**
It establishes the slice sequence, the stop conditions, and the
authorisation gate before any code is written. The Phase 0 pattern
should be repeated for Platform Foundation Phase 0.

10.3 **Each slice carried explicit stop conditions.** The M2
Implementation Plan §18 stop conditions (any §4 deny-list item;
fixture-shape gap; protected-files drift; new dependency need;
Canon §10 anti-pattern; Independence Reset framing breach;
Railway behaviour deviation; Stage E-prod activity; PR diff
boundary breach; ChatGPT material finding) were not triggered
during M2. The discipline of having stop conditions written down
in advance is the value, even when they don't fire.

10.4 **ChatGPT review (if used) is a complementary signal, not a
substitute.** The Alignment Review PR (#60) is a documentation
output that reviewers can consult; per-slice ChatGPT review was
not invoked formally during M2 but the pattern from M1 (which
did invoke it for fixture-hygiene and live/read-only wording)
remains the precedent for high-risk slices.

10.5 **PRs #27 / #28 / #29 untouched throughout.** This is a
visible standing constraint, checked at every merge, that
remained in force across all 9 M2 PRs (#61–#67, #69) and the
two adjacent documentation PRs (#60, #68).

10.6 **Cross-document framing alignment held.** The Operational
UX Direction (PR #58), Operational Platform Workflows (PR #59),
M2 Alignment Review (PR #60), Pilot Proximity App note (PR #57),
and Independence Reset (PR #51) were all honoured by every M2
slice without retroactive modification.

---

## 11. Product / UX lessons

11.1 **The right-rail action-surface direction was correct.**
PR #69 demonstrated that the M2 Alignment Review §3.1 risk
("decision cards visually demoted") was real — the placeholder
content had stayed on the left during slices 1–6 simply because
it was already there. The visual walkthrough caught it; the
remediation was small and reversible. **Lesson:** when a
direction document (UX Direction §3.1) says where content
should go *long-term*, defer the move to the layout-remediation
slice rather than each functional slice — but **do not skip the
move**.

11.2 **The conditions strip is high-traffic visual real estate.**
The strip is persistent (64 px ribbon under the header) and is
in the operator's peripheral vision throughout the shift. Visual
balance matters disproportionately for surfaces in this position.

11.3 **Pilot identifier display matters.** PR #65 deliberately
renamed the fixture field `pilot_name` to `pilotId` in the
view-model, signalling visually (via `<code>` rendering) that
the value is a resource-safe code, not a person's name. This
is consistent with the Pilot Proximity App note (PR #57) §5.9
de-identified data principle.

11.4 **Placeholder copy is the M3+ scope signal.** Every M2 tab's
placeholder copy explicitly names what is deferred (audit
ledger, dispatch authority, analytics, fatigue / competency
validation, etc.). The placeholder is itself the boundary
between M2 and M3+.

11.5 **The Centre Panel Navigation default is Dashboard.** Even
though UX Direction §3.2 anticipates VTS as the long-term default
for coordination users, M2 preserved Dashboard as the M1-shipped
default. Per-role default-tab logic is explicitly M3+ scope.

---

## 12. Technical lessons

12.1 **The adapter pattern scales.** Four new adapters in M2
slot beside the existing M1 adapters without modifying the
`summaryAdapter` contract. The pattern accommodates future tab
additions in M3+ (e.g. an analytics adapter for the full
Performance tab) without re-architecture.

12.2 **`react-dom/server.renderToStaticMarkup` is the right
test-framework choice for read-only React surfaces.** Markup
snapshots are sufficient for read-only components. When M3+
adds interactive components (operator-action buttons), the
test framework choice will need re-evaluation (likely
`@testing-library/react` + DOM environment), but that is M3+
scope under separate authorisation.

12.3 **CSS-grid + absolute positioning replaces a charting
library.** The Berth Timeline slice (PR #62) implemented a
Gantt-style lane visualisation using CSS-grid and percentage-
positioned `<div>` segments. No charting library was
introduced. The same pattern likely applies to the future
VTS spatial surface (map tile layer + positioned overlays,
not a heavy mapping library at first).

12.4 **In-memory navigation state is a clean architectural seam.**
`useState` in `CenterPanel.jsx` is the boundary; routing /
persistence / per-role defaults all plug in at that seam later
without component rewrites.

12.5 **Bundle-size soft budget is operationally useful.** The M2
Plan §14.9 soft guard (M1 + 30 %) is now ground truth: M1 was
~197 kB; M2 closed at ~225 kB (+14 %), well within budget.
M3+ should retain a soft guard at the same level.

12.6 **The frozen `CENTRE_PANEL_TABS` array** is the single source
of truth for tab order. `Object.freeze()` + explicit-ordering
test prevents accidental reordering or label drift.

12.7 **No new npm dependencies were added across the entire M2
cycle.** This is a strong signal that the M1 baseline +
existing stack is sufficient for read-only surface expansion.
M3+ may need additions (DOM test framework, map library,
charting library); each should be a separate scope-proposal
decision.

---

## 13. What should change before Platform Foundation Phase 0

The following items should be addressed **before** Platform
Foundation Phase 0 work begins, or carried into the Phase 0
scope proposal as explicit actions.

13.1 **Sequence the Platform Foundation milestones.** Platform
Foundation v0.1 (PR #68) identifies open question §15.1.3 — the
desired sequence of capability arrival (auth first, then audit,
then write path, or another order). This is a Tony-side
strategic sequencing decision and must be resolved before
Phase 0 scope is drafted.

13.2 **Choose the first live-client target port.** PR #68 §15.1.1.
Brisbane (strongest data maturity), Darwin (now demo-verified),
or another. Affects integration priorities, regulatory
posture, commercial framing.

13.3 **Choose the deployment platform and storage substrate.**
PR #68 §15.2.1 / §15.2.2. These decisions feed every subsequent
infrastructure choice and must be made deliberately rather than
incrementally inherited.

13.4 **Maintain the slice-by-slice authorisation pattern.** It
worked for M2. Platform Foundation work is higher-risk than M2
(real auth, real database, real audit chain), so the discipline
should be stronger, not weaker.

13.5 **Retain the protected-files probe at every merge.** Add to
the probe any new sensitive-files surfaces that arrive during
Platform Foundation work (e.g. migration files, secret-store
configuration files, IdP integration files).

13.6 **Schedule a Tony visual walkthrough at every Platform
Foundation milestone close.** M2's walkthrough caught three
real issues; that pattern is non-substitutable.

13.7 **Re-affirm the Beta 10 Immutability Rule in every Platform
Foundation scope proposal and every implementation plan.**
Platform Foundation work is the first V1 cycle where real
backend code lands; the temptation to "while we're here" Beta
10 will be highest. Explicit re-affirmation in every artefact
is the control.

13.8 **Re-affirm the Kyber boundary in every Platform Foundation
artefact.** Platform Foundation work introduces auth, RBAC, and
audit emission — all of which approach the Kyber boundary.

---

## 14. Risks carried forward

14.1 **VTS spatial surface (HC-002) is a known operational
need.** The M2 VTS tab is a read-only list precursor. The
spatial surface arrives in M3+ under separate scope. Until
then, VTSOs in pilot deployments will receive a list-only
view, which is operationally usable but not the long-term
posture.

14.2 **No live data feeds.** M2 is fixture-fed. The first
Platform Foundation milestone that introduces live data
(AISStream / MST / BoM directly into the V1 backend, not via
Beta 10) is a high-risk integration with regulatory and
data-residency implications. Each integration is a separate
scope-proposal decision per Platform Foundation §9.

14.3 **No auth in V1 sandbox.** The sandbox is currently
unauthenticated by design (fixture-fed; read-only). The first
auth introduction (likely Platform Foundation Milestone 1
under separate scope) must be implemented server-side first
(per Platform Foundation §5.6) and tested for cross-port
leakage (Sev-1 defect class per §8.6).

14.4 **No real audit ledger.** The M2 Shift Log is derived,
not authoritative. The first audit-emission introduction must
re-use or re-implement the Phase 0.7 hash-chain model and must
not silently break the existing audit chain on Beta 10
(although Beta 10 remains immutable, the conceptual chain
needs explicit governance).

14.5 **Multi-port tenancy not yet exercised.** All M2 fixtures
are single-port. Multi-port behaviour (port-scope assignment,
port-context switching at the platform layer) is M3+ work and
will require new fixtures or live data sources to exercise.

14.6 **PRs #27 / #28 / #29 still open.** They have been
untouched since 2026-05-15 throughout M0, M1, M2. Their
disposition is out of scope for this retrospective; they will
need explicit attention at some future governance moment.

14.7 **`phase-0-complete` baseline must remain immutable.** No
M2 activity touched the tag (`bf93373`). Platform Foundation
work must not re-tag or move it.

14.8 **Bundle size will grow.** Platform Foundation work
(real auth UI, real audit-reading surfaces, real notification
surfaces) will add JS. The M2 Plan §14.9 soft guard
(M1 + 30 %) is exhausted at the M2 end-state (225 kB / +14 %).
M3+ should set a fresh soft guard and revisit it per slice.

---

## 15. Recommendations and next steps

15.1 **Close M2.** All in-session-verifiable acceptance criteria
PASS or PASS WITH NOTE. The two deferrals (A8 sandbox switch,
A11 Beta 10 visual check) are Tony-side governance steps and
do not block M2 closure.

15.2 **Begin Platform Foundation Phase 0 next.** The Platform
Foundation v0.1 document (PR #68) is the upstream reference.
The first Platform Foundation scope proposal cycle should
mirror the M2 governance pattern (scope proposal → alignment
review → implementation plan → Phase 0 readiness → slice-by-
slice authorisation → retrospective).

15.3 **Resolve PR #68 §15.1 Tony-direction open questions
before Phase 0 scope is drafted:** first live-client port,
expected concurrent user count, capability-arrival sequence,
commercial framing, target date.

15.4 **Maintain Beta 10 Immutability throughout Platform
Foundation work.** Cite it in every scope proposal, every
implementation plan, every commit message. Verify it with the
protected-files probe at every merge.

15.5 **Maintain Independence Reset framing.** No Smart Ocean X
dependency framing in any Platform Foundation artefact.

15.6 **Maintain Kyber boundary.** Each Platform Foundation
artefact must affirm Kyber-boundary compliance.

15.7 **Schedule HC-002 (VTS spatial surface) into the M3+
capability sequence**, but do not authorise implementation
ahead of platform foundation auth + RBAC + audit + replay.
The spatial surface is most valuable once decision-card
ack-then-act flows are real, not before.

15.8 **At Platform Foundation close, draft a Platform Foundation
Phase 1 Retrospective.** Mirror this document's shape.

15.9 **Do not progress automatically.** The M2 closure does
**not** authorise Platform Foundation Phase 0 work. A separate
explicit "Authorised: begin Horizon V1 Platform Foundation
Phase 0" message from Tony is required.

15.10 **Tony-side optional next steps** (outside agent scope):
- Switch `horizon-v1-sandbox` source branch to `main` (or to
  a pinned M2-final tag) if Tony wishes the sandbox to serve
  the closed M2 build. Optional.
- Run the Beta 10 post-M2 visual check (per M1 Retrospective
  §2 pattern) when convenient. Optional.

---

## 16. Status

**M2 — CLOSED with notes** (2026-05-21).

Confirmed by this document:
- All in-session-verifiable acceptance criteria PASS or PASS WITH NOTE.
- M2 introduced no live Beta 10 calls, no production `/api/*`
  calls, no auth, no RBAC, no operator-action writes, no audit
  emission, no Stage E-prod activity, no database dependency, no
  `server.py` change, no root `railway.toml` change, no production
  state change.
- Beta 10 was **not touched** throughout the entire M2 cycle.
- The Smart Ocean X architectural dependency **remains formally
  closed** under PR #51 — A12 affirmed (§6).
- The Kyber boundary is in force.
- The Centre Panel Navigation is centre-panel view navigation
  only — **not** port switching. No port selector exists in any
  M2 surface.
- The VTS map / spatial surface is **not** implemented in M2;
  captured as backlog item HC-002 in `HORIZON_CAPABILITY_BACKLOG.md`.
- Right-rail action-surface placement is in force (per UX
  Direction §3.1) following the visual remediation slice (PR #69).
- The conditions strip is visually rebalanced (three flex
  regions); information hierarchy is unchanged.
- The Beta 10 Immutability Rule was honoured by construction
  (only `frontend/` files plus root-level markdown documentation
  modified throughout M2).
- PRs #27 / #28 / #29 remain untouched.
- `phase-0-complete @ bf93373` baseline is immutable.

**Next action:** Tony's review of this retrospective. Optional
ChatGPT engineering review. If approved and merged, Tony's
Platform Foundation Phase 0 authorisation (per §15.2) is the
next governance step. Platform Foundation work does **not** begin
until that authorisation is received.

---

## End of M2 Retrospective v0.1
