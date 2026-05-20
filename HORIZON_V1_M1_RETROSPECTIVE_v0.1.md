# Horizon V1 — M1 Retrospective (v0.1)

**Document status:** Draft for review
**Owner:** AMSG (AMS Group) — Tony Trajceski
**Engineering review:** ChatGPT
**Execution agent:** Claude
**Effective baseline:** `origin/main @ b0d9ff2`
  (Merge #51 — Horizon Independence / Architecture Reset, on top of
  M1 merge `e90eb56`)
**Date:** 2026-05-20
**Scope of authority:** Records the full closure of Milestone M1
  of the Horizon V1 programme, captures lessons learned, governance
  observations, and recommendations for use **before** M2 scoping
  begins. This document is descriptive and advisory — it does **not**
  authorise M2, and it does **not** change any code, infrastructure,
  or configuration.

---

## 1. Executive summary

Milestone M1 ("Read-only Dashboard tab, fixture-backed") is **fully
closed** as of 2026-05-20. M1 delivered a fixture-fed read-only
data pipeline (adapter modules, polling hook, partial Dashboard
tab) for the Horizon V1 frontend, verified end-to-end against
captured fixtures, deployed to the `horizon-v1-sandbox` Railway
service from `feat/v1-m1`, and merged to `main` at `e90eb56`.

M1 introduced **no new authentication, no writes, no audit
emission, no Stage E-prod activity, and no live Beta 10 calls**.
Beta 10 production behaviour was visually verified unchanged by
Tony across `/`, `/login`, and the port switcher (Brisbane /
Melbourne / Geelong / Darwin).

The Horizon Independence / Architecture Reset (PR #51, `b0d9ff2`)
merged immediately after M1 closure and now governs the framing
of all future Horizon work in this repository. The Smart Ocean X
architectural dependency is **formally closed** and remains so.

This retrospective records what worked, what did not, and what
must change before M2 scoping begins. **It does not authorise
M2.** M2 scope, acceptance criteria, and implementation are
gated on a separate scope-proposal PR and Tony's explicit
authorisation.

---

## 2. M1 final close status

**M1: FULLY CLOSED.**

| Event | Reference | Date |
|---|---|---|
| M1 Execution Plan authorised | PR #36 | (earlier) |
| M1 Implementation Plan authorised | PR #48 | (earlier) |
| M1 fixtures captured locally (Option 4b) | PR #49, `fa00c6a` | pre-2026-05-20 |
| M1 Phase 1 implementation merged | PR #50, `e90eb56` | pre-2026-05-20 |
| M1 Phase 2 sandbox verification | `horizon-v1-sandbox`, commit `550add16` | pre-2026-05-20 |
| Horizon Independence / Architecture Reset merged | PR #51, `b0d9ff2` | 2026-05-20 |
| Tony's Beta 10 post-M1 visual check | (verbal/written report) | 2026-05-20 |
| M1 fully closed | this retrospective | 2026-05-20 |

**No outstanding M1 acceptance items remain.**

---

## 3. Acceptance criteria closure table

Sourced from the V1 Execution Plan §11 and the M1 Implementation
Plan §18. Each criterion lists the evidence supporting closure.

| # | Acceptance criterion | Status | Evidence |
|---|---|---|---|
| 1 | M1 fixtures captured locally via Option 4b — no production touch | CLOSED | PR #49 (`fa00c6a`); 5 fixture files under `frontend/public/fixtures/` |
| 2 | Adapter modules implemented per Adapter Design Note §2.2 | CLOSED | 10 modules under `frontend/src/api/adapters/` (summary, conditions, vessel, conflict, guidance, dashboard, etdRisk, time, status, units) |
| 3 | `useSummary` polling hook with 30 s default, tab-visibility pause, backoff `[1s, 3s, 9s]`, last-known-good cache, single-flight, isStale gate (`now > 2 × interval`) | CLOSED | PR #50, `frontend/src/hooks/useSummary.js` |
| 4 | Partial Dashboard tab (read-only, fixture-backed) — KPI tiles, ETD risk table, utilisation summary | CLOSED | PR #50, `frontend/src/features/dashboard/` |
| 5 | Unit tests added (Vitest) covering adapters and helpers | CLOSED | 86 tests / 10 files (per M1 implementation report) |
| 6 | Regression gate `tests/test_beta10_regression.py` remains green | CLOSED | 46/46 PASS (re-verified at PR #50 merge and at #51 merge) |
| 7 | Railway sandbox `horizon-v1-sandbox` serves M1 build from `feat/v1-m1` | CLOSED | `instanceStatus: RUNNING` at commit `550add16`, `/frontend/railway.json` config-path |
| 8 | Bundle hash matches local build; no production URLs in bundle; only `/fixtures` as fetch target | CLOSED | Phase 2 read-only verification on sandbox |
| 9 | PR #50 merged to `main` | CLOSED | `e90eb56` |
| 10 | Tony's Beta 10 post-M1 visual check — no production regressions | CLOSED | `/` loads, `/login` works, port switcher works across BNE / MEL / GEX / DAR, no errors, demo behaviour unchanged |

All 10 acceptance criteria are closed.

---

## 4. What M1 proved

M1 proved the following properties of the V1 architecture, in
production-adjacent conditions, without touching production:

1. **A fixture-fed read-only pipeline is sufficient** to drive the
   Horizon V1 frontend Dashboard tab end-to-end. No live Beta 10
   connection was required.
2. **The Adapter Design Note §2.2 contract holds.** The 20-key
   `ViewSummary` shape is correctly produced from the fixtures by
   the adapter modules and consumed by the Dashboard components.
3. **Polling is stable and well-behaved.** 30 s polling, tab
   visibility pause, exponential backoff `[1s, 3s, 9s]`,
   single-flight protection, last-known-good caching, and the
   `isStale` gate all work as specified.
4. **Railway sandbox provisioning works under Tony-side control.**
   The `/frontend/railway.json` config-path override + dashboard
   Config Path setting produced a clean RAILPACK build and a
   serving instance with no production cross-talk.
5. **The two-phase deploy pattern works.** Phase 1 (local
   validation) followed by Phase 2 (Railway sandbox verification
   after Tony-side source-branch switch) gave a clean, reviewable,
   reversible deployment path.
6. **The Beta 10 / V1 separation holds.** Beta 10 production was
   not touched, not impacted, and not visually changed during the
   M1 cycle. Tony's post-M1 check confirmed this.
7. **Strict change-scope governance produced clean PRs.** Each
   M1 PR (#49, #50, #51 and supporting docs) was tightly scoped,
   reviewed by ChatGPT, authorised by Tony, and merged without
   amendment of historical or unrelated content.

---

## 5. What worked well

The following practices materially contributed to the clean M1
closure and should be retained as defaults for M2 and beyond.

### 5.1 Read-only-by-default
M1 was scoped read-only from the start. No write endpoints, no
audit emission, no authentication changes, and no Stage E-prod
activity were authorised or attempted. This made every change
reviewable in isolation.

### 5.2 Fixture-fed pipeline
Using `/fixtures/*.json` as the data source — with `VITE_API_BASE`
defaulting to `/fixtures` — let the frontend exercise the full
adapter → hook → component path without any backend dependency.
Sandbox deploys served the same fixtures as local dev.

### 5.3 Option 4b local fixture capture
After ChatGPT flagged that the original Option 1 (production
`set_port` capture) would mutate Beta 10 global state visible to
customers, Tony rerouted to Option 4b: a local copy of `server.py`
on `127.0.0.1`, populated and curl-captured into `frontend/public/fixtures/`.
This was the right call. It produced production-shape fixtures
**without touching production**.

### 5.4 Tight scoping per PR
Each M1 PR was deliberately narrow:
- PR #49: fixtures only (5 files + README).
- PR #50: adapter, hook, partial Dashboard tab only.
- PR #51: Independence / Architecture Reset (single markdown).

This made every diff reviewable in minutes, and every merge
auditable in hindsight.

### 5.5 ChatGPT pre-merge review gate
ChatGPT review caught material issues before merge — including
fixture hygiene (Open-Meteo live weather caveat) and "live /
read-only" wording that risked implying a Beta 10 connection. Both
were corrected before merge.

### 5.6 Two-phase deploy pattern
Phase 1 (local validation) → Tony-side source-branch switch on
`horizon-v1-sandbox` → Phase 2 (read-only verification). Claude
never touched Railway settings; Tony performed all provisioning,
config, and source-branch operations. This separation was clean
and worked first time after the M0 config-path lesson.

### 5.7 Tony-side production checks
Beta 10 visual verification was performed by Tony, not by the
execution agent. This kept the verification outside the codebase
and outside the execution loop, which is the correct allocation.

---

## 6. Key lesson: fixture-backed / read-only pipeline

**Lesson:** A fixture-fed read-only pipeline is a viable, robust
backbone for V1 milestone delivery and should remain the default
for at least M2.

**Rationale:**
- It severs frontend milestone risk from any backend or production
  risk. M1 could not break Beta 10, by construction, because M1
  never spoke to Beta 10.
- It makes every change deterministic and replayable. Fixtures are
  static JSON checked into git; behaviour is reproducible across
  developers and CI.
- It enables sandbox deploys that look operationally realistic
  (same shapes, same magnitudes, same edge cases) without any
  customer or operator exposure.
- It defers the questions of authentication, authorisation, audit,
  and writes to a later, deliberately authorised milestone, instead
  of leaking them into UI work.

**Action for M2:** Continue read-only / fixture-fed by default.
Any deviation (e.g. a live read for non-sensitive data) must be
called out explicitly in the M2 scope proposal and authorised
separately by Tony.

---

## 7. Key lesson: Option 4b local fixture capture

**Lesson:** Production state must not be mutated to produce
fixtures. Local-server capture is the correct default.

**Background:**
The original Option 1 proposed capturing `/api/summary` from the
deployed Beta 10 instance by toggling `set_port` across Brisbane,
Melbourne, Geelong, and Darwin. ChatGPT flagged that this would
change global demo state visible to live Beta 10 customers during
the capture window.

Tony rejected Option 1 and authorised Option 4b instead: run a
local copy of `server.py` on `127.0.0.1`, drive `set_port`
locally, capture `/api/summary` via `curl`, and commit the JSON
files. PR #49 was the result.

**Why this matters:**
- The fix was a **process** fix, not a code fix. The original
  approach would have been technically functional but
  operationally unacceptable.
- Caught at review time, before any production mutation.
- Demonstrates the value of ChatGPT review and explicit Tony
  authorisation gates.

**Action for M2:** Any fixture refresh, expansion, or new fixture
capture for M2 must use Option 4b or equivalent local-only
methodology. Production capture is not authorised.

---

## 8. Key lesson: fixture hygiene and Open-Meteo live-weather caveat

**Lesson:** Fixture README claims must match runtime behaviour.

**Background:**
During PR #49 review, Tony performed a final fixture hygiene
review and surfaced that the README initially described all
fixture content as "simulated." This was not strictly accurate:
the BoM / weather path in `server.py` populates the weather cache
from Open-Meteo, a live public weather API that requires no API
key. So the weather fields in the captured fixtures reflect **real
Open-Meteo readings at capture time**, not simulated values.

Tony authorised amending the README to disclose this. The
captured fixtures are still acceptable for M1 because Open-Meteo
is a public data source with no customer-identifying or
operationally sensitive content, and the values are static once
captured.

**Why this matters:**
- "Fixtures" can silently mix simulated and live-sourced data if
  the underlying server has live external dependencies.
- README accuracy matters for downstream reviewers, customers, and
  auditors. A misleading README is a governance failure even when
  the underlying data is fine.

**Action for M2:**
- Any new fixture must come with an accurate provenance line in
  the fixtures README (simulated, live external public source,
  derived, etc.).
- Where a fixture mixes sources, the README must say so explicitly.
- If M2 introduces fixtures that touch any non-public data source,
  this must be raised in the M2 scope proposal before capture.

---

## 9. Key lesson: Railway config-path and sandbox discipline

**Lesson:** Railway service-level **Config Path** setting takes
precedence over `rootDirectory` alone, and was the root cause of
the M0 502 incident. Sandbox discipline must remain Tony-side.

**Background:**
During M0, setting `rootDirectory = /frontend` alone did **not**
make Railway read `frontend/railway.json`. The service-level
Config Path defaulted to `/railway.toml` (Beta 10's Python
config), which produced a Python build attempt and a 502 on the
serving frontend port. The fix was for Tony to update the
dashboard Config Path setting to `/frontend/railway.json`. After
that, the RAILPACK build and `npx serve -s dist -l $PORT` start
command worked correctly.

For M1 Phase 2, Tony switched the `horizon-v1-sandbox` source
branch from `feat/v1-m0` to `feat/v1-m1`, the existing
config-path setting carried over, and the M1 build deployed
cleanly on first try. The M0 lesson held.

**Why this matters:**
- `railway.json` in a subdirectory is **not** automatically read by
  Railway unless the Config Path setting points at it.
- This is a documented Railway behaviour, not a bug, but it is
  easy to miss when a repo also has a root `railway.toml`.
- The execution agent must **not** attempt to change Railway
  settings; Tony controls Config Path, source branch,
  environment variables, and project assignment.

**Action for M2:**
- Continue the strict Tony-side Railway control pattern.
- Document the Config Path setting alongside any new sandbox
  service that has a non-root config file.
- Never modify root `railway.toml` — it remains Beta 10's
  config.

---

## 10. Governance lessons

10.1 **Authorisation gates worked.** Every M1-scope change was
preceded by an explicit "Authorised: …" message from Tony. No
work was performed in the absence of authorisation. This pattern
must be retained.

10.2 **ChatGPT review surfaced material findings.** ChatGPT
flagged the Option 1 fixture-capture risk and the "live / read-only"
wording risk. Both required substantive amendment before merge.
The cost of pre-merge review was well below the cost of post-merge
remediation.

10.3 **The Independence / Architecture Reset (PR #51) was the
correct timing.** Closing the Smart Ocean X dependency on top of
M1 — rather than waiting until M2 scoping — means M2 begins from
an unambiguous framing baseline. No M2 document will need
retroactive framing correction.

10.4 **Spot-audit of historical V1 planning documents (§8.4 of
the Independence Reset) was cheap and high-value.** Confirming
that none of the 19 V1 planning documents referenced Smart Ocean
X allowed the reset to remain a single document instead of a
multi-document re-edit. This pattern (spot-audit before mass
edit) should be retained whenever a governance reset is proposed.

10.5 **Strict "do-not-touch" lists in each authorisation were
effective.** Listing horizon-prod, Beta 10, Stage E-prod, preview
Postgres, horizon-v1-sandbox (where applicable), PRs #27 / #28 /
#29, and root `railway.toml` in each authorisation made the
boundary of each task crisp and enforceable.

10.6 **Stop-for-review markers worked.** "Open PR and stop for
review" was honoured every time, with no auto-merge and no
unauthorised follow-on commits. This preserved Tony's control over
when merges occurred.

---

## 11. Product / UX lessons

11.1 **The partial Dashboard tab is a useful demo surface.** Even
without the remaining five center-panel tabs (Berth Timeline,
Shift Log, VTS, Pilotage, Performance), the KPI tiles, ETD risk
table, and utilisation summary convey the core operational
narrative.

11.2 **The fixture set covers a useful spread of states.**
Brisbane busy (9 conflicts, 13 vessels), Brisbane quiet (0
conflicts, derived), Melbourne sim (18 conflicts, 9 vessels),
null-fields (defence test), and malformed (25-key subset) gave
the adapter and UI a realistic edge-case exercise.

11.3 **Stale-data behaviour is visible to the operator.** The
`isStale` flag and stale-data banner give the operator a clear
signal when polling has fallen behind, rather than silently
showing stale numbers. This is the correct default for an
operational dashboard.

11.4 **Time-zone and unit conversion concerns are correctly
isolated in adapters.** `units.js` (nm ↔ km) and the time helpers
(date-fns-tz, DST-aware) localise these concerns away from
component code. M2 should retain this separation.

11.5 **Demo branding remained correct.** The persistent DEMO
banner and the existing brand surfaces continue to identify the
product as Horizon. The Independence Reset confirmed that no
banner or branding change was required in code.

---

## 12. Technical lessons

12.1 **Adapter Design Note §2.2 was a sound contract.** The 20
top-level `ViewSummary` keys mapped cleanly to the dashboard
needs. No mid-M1 contract amendment was required.

12.2 **`useSummary` polling hook design is robust.** Tab
visibility pause, backoff, single-flight, last-known-good, and
isStale combined to produce a hook that handles transient network
failure without thrashing the UI.

12.3 **Vitest was a low-friction choice.** 86 tests across 10
files ran fast and gave granular failure messages. One precision
issue (`units.js` rounding) was caught by `toBeCloseTo` and fixed
by moving rounding to display-time in the UI. Adapters now return
unrounded numbers; UI formats with `.toFixed()` as needed.

12.4 **`VITE_API_BASE` default to `/fixtures` is clean.** It
allows local dev, Railway sandbox, and unit tests to share the
same code path. Production / live-API behaviour will require
setting `VITE_API_BASE` explicitly at deploy time, which is the
correct opt-in stance for any future live read.

12.5 **`/frontend/railway.json` config worked first time on M1.**
RAILPACK builder, `npm install && npm run build`, and
`npx serve -s dist -l $PORT` produced a serving instance with no
mid-cycle config change.

12.6 **No bundle leakage of production URLs.** Phase 2 bundle
inspection confirmed only `/fixtures` as a fetch target and no
production Beta 10 URLs were embedded. This must remain a
release-gate for future milestones.

---

## 13. What should change before M2

The following items should be addressed **before** the M2 scope
proposal is finalised, or carried explicitly into the M2 scope
proposal as actions.

13.1 **Carry the Independence Reset framing into all M2
documents.** M2 plan / acceptance / implementation documents must
use neutral framing per Independence Reset §13 / §14. No "Horizon
is built on Smart Ocean X" framing in any form.

13.2 **Define M2 scope precisely before any code.** M2 may cover
some, all, or none of the remaining five deferred center-panel
tabs (Berth Timeline, Shift Log, VTS, Pilotage, Performance), or
may target a different surface entirely. The scope must be
explicit and authorised.

13.3 **Decide on fixture strategy for M2 up front.** Options
include reusing the M1 fixtures unchanged, expanding them, or
capturing new fixtures via Option 4b for new endpoints. This
decision belongs in the M2 scope proposal, not mid-implementation.

13.4 **Decide whether M2 introduces any live read.** The current
governance default is read-only fixture-fed. If M2 proposes a
live read of any kind (even for non-sensitive public data), this
must be called out and authorised explicitly. The default
expectation is **no live read** for M2.

13.5 **Decide on automated fixture diff tooling.** A small tool
that diffs new fixture captures against committed fixtures would
catch silent shape changes early. Optional but recommended for
M2.

13.6 **Earlier ChatGPT review on wording.** Two M1 PRs required
wording amendments after ChatGPT review (fixture README hygiene,
"live / read-only" framing). For M2, request ChatGPT review of
all wording-sensitive sections **before** opening the PR.

13.7 **Confirm regression gate scope for M2.** The
`tests/test_beta10_regression.py` gate (46/46) is still the
correct boundary for Beta 10 stability. If M2 introduces new
backend code (which is currently not anticipated), additional
regression cases should be authored before merge.

---

## 14. Risks carried forward

The following risks remain open or partially open. None of them
blocks M2 scoping, but each should be acknowledged in the M2
scope proposal.

14.1 **Live external dependency on Open-Meteo (current Beta 10
behaviour).** Not introduced by M1; pre-existing in Beta 10.
M1 fixtures captured weather values at capture time and these are
now static. No M1 runtime touches Open-Meteo. This remains a
Beta 10 concern, not an M1 concern, but is documented here for
completeness.

14.2 **Railway sandbox cost and shared dashboard.** The
`horizon-v1-sandbox` service incurs Railway usage. Tony controls
when it runs and from which branch. The execution agent must
never auto-start, auto-restart, or scale sandbox services.

14.3 **PRs #27, #28, #29 remain OPEN.** These are pre-V1 PRs
unrelated to M1 and were explicitly listed as untouched in every
M1 authorisation. They remain OPEN with no update since
2026-05-15. Their disposition is out of scope for this
retrospective.

14.4 **Stage E-prod remains paused.** This is by design and
unchanged by M1. Any future Stage E-prod activity requires its
own explicit authorisation and is not implied by M2 scoping.

14.5 **`phase-0-complete @ 4ad4aae` baseline must remain
immutable.** No M1 activity touched this tag. Any pressure to
re-tag or move it during M2 must be refused.

14.6 **Kyber boundary still in force.** No M1 activity crossed
this boundary. Any M2 activity that approaches it must be raised
in the M2 scope proposal.

14.7 **`HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md`
framing must be honoured.** Any drift back to Smart Ocean X
framing in M2 documents would re-open a dependency that is
formally closed. The retrospective records this as a governance
risk, not a current breach.

---

## 15. Recommendations

The following are the actionable recommendations arising from
this retrospective. Each is sized for a single, explicitly
authorised next step.

15.1 **Authorise an M2 scope proposal PR** (single markdown file
at root). Recommended file name pattern:
`HORIZON_V1_M2_SCOPE_PROPOSAL_v0.1.md`. The proposal should
cover:
- Candidate scope and rationale
- Acceptance criteria
- Fixture strategy
- Read-only / fixture-fed default position
- Railway sandbox plan
- Beta 10 untouched guarantee
- Closed-list of do-not-touch items
- ChatGPT review and Tony authorisation gates

15.2 **Maintain the read-only / fixture-fed default for M2.** Do
not introduce live reads, writes, audit emission, or
authentication changes in M2 without an explicit separate
authorisation.

15.3 **Maintain the two-phase deploy pattern.** Phase 1 local
validation, then Tony-side source-branch switch and Phase 2
sandbox verification.

15.4 **Maintain Tony-side Railway control.** Execution agent
performs read-only verification only.

15.5 **Maintain the ChatGPT pre-merge review gate.** Earlier in
the cycle where possible.

15.6 **Maintain strict "do-not-touch" lists in every M2
authorisation.** Include at minimum: horizon-prod, Beta 10,
Stage E-prod, preview Postgres, horizon-v1-sandbox config (Tony
controls), PRs #27 / #28 / #29, root `railway.toml`,
`phase-0-complete @ 4ad4aae`, Kyber boundary, Smart Ocean X
framing.

15.7 **Maintain the Independence Reset framing in all M2
documents.** Use "Horizon" / "AMSG Horizon" / "Horizon by AMS
Group" only. No Smart Ocean X dependency framing.

15.8 **Record M2 retrospective at M2 close.** This document is
the template. M2's retrospective should follow the same shape:
status, acceptance closure, what worked, key lessons,
governance, product, technical, what should change, risks
carried, recommendations.

15.9 **Do not auto-progress from this retrospective to M2.** A
separate explicit "Authorised: draft M2 scope proposal" message
from Tony is required.

15.10 **Confirm at M2 scope time that the Smart Ocean X
dependency remains formally closed and that no new dependency on
it has been proposed.**

---

## End of M1 retrospective

This retrospective is descriptive and advisory. It does **not**
authorise M2. M2 scope, acceptance, and implementation are
gated on a separate explicitly authorised PR.

Confirmed by this document:
- M1 is fully closed.
- M1 remained fixture-backed and read-only throughout.
- No live Beta 10 calls were introduced by M1.
- No auth changes, no writes, no audit emission, no Stage
  E-prod activity occurred during M1.
- Beta 10 was visually checked by Tony post-M1 and remained
  unchanged.
- The Smart Ocean X architectural dependency remains formally
  closed under the Independence / Architecture Reset
  (PR #51, `b0d9ff2`).
- M2 is **not** authorised by this retrospective.

**Next action:** ChatGPT engineering review of this
retrospective, then Tony's review. Once merged, M2 scope
proposal may be authorised as a separate next step.
