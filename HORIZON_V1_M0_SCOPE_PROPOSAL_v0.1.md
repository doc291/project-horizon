# Horizon V1 — M0 Scope Proposal (v0.1)

**Status:** Scope-constraining proposal — planning only
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review)
**Authoritative inputs:**
- The seven V1 foundation documents on `main @ 6d00af3`
  (Permission Model, Workflow Model, Screen Architecture,
  Information Architecture, Implementation Strategy, Review Pack,
  Execution Plan)
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` (PR #37, open)
**Pending inputs (not yet authorised):**
- Claude Design UX/UI handoff in `v1-handoff/`
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise M0 implementation.** It
proposes the **maximally constrained scope** that M0 should be
held to *if* it is later authorised, so that V1 can begin safely
without operational or governance debt.

---

## 1. Executive Summary

M0 must prove **two things and only two things**:

1. The V1 frontend architecture works — React + Vite scaffold,
   the Horizon Dark design tokens, the 3-column VTSO shell — can
   render against representative data inside the repo, on a
   developer machine, with the existing regression gate still
   green.
2. The V1 deployment model works — a `horizon-v1-sandbox` Railway
   project can host the V1 frontend artefact independently of
   the Beta 10 `horizon-prod` deployment, with zero changes to
   Beta 10 behaviour.

That is all M0 needs to prove. **Everything else is M1+.**

M0 is therefore:

- **One new directory** at the repo root: `frontend/`
- **Zero changes** to `server.py`, `index.html`, audit helpers,
  Phase 0 migrations, regression gate, deploy manifests, or any
  Beta 10 file
- **Static rendering** from a sample data file lifted from the
  handoff prototype — no live `/api/summary` call yet
- **No login flow, no auth, no role enforcement** — the V1.0
  sandbox is reachable by Tony only; a real auth path is M1+
- **No actions** — no buttons that POST anywhere
- **No DSW**, **no What-If**, **no replay**, **no escalation**,
  **no handover**, **no audit emission**
- **No production deploy** — `horizon-v1-sandbox` only
- **Reversible by deleting `frontend/`** — Beta 10 unaffected

This is intentionally tiny. The point is to **eliminate the most
common ways V1 builds fail in the first milestone**: silent
backend coupling, premature RBAC scaffolding, audit-trail
illusions, and Beta 10 production contamination.

---

## 2. What M0 IS

M0 is **a buildable, renderable, deployable React shell with the
correct visual identity, against representative static data,
isolated from Beta 10**.

Concretely:

- A new `frontend/` directory at the repository root
- A Vite + React project (`package.json`, `vite.config.js`,
  `index.html` inside `frontend/`, `src/`)
- CSS custom-property tokens lifted from
  `v1-handoff/prototype/Horizon V1.html` `<style>` block, placed
  in `frontend/src/styles/tokens.css`
- A small set of presentational components (Card, Pill,
  CategoryPill, Dot, Icon) built per the handoff's component
  inventory §4.8
- A `HorizonHeader` and `ConditionsBar` rendered against static
  sample data
- An empty 3-column shell layout (`360px | 1fr | 380px`) with
  placeholder content in each column
- A static sample-data file (`frontend/src/data/sample.js`) lifted
  from `v1-handoff/prototype/data.js`
- A `npm run build` that produces `frontend/dist/`
- A deploy of `frontend/dist/` to `horizon-v1-sandbox` Railway
  project (separate from `horizon-prod`)

That is M0 in full.

---

## 3. What M0 IS NOT

M0 is **NOT**:

- A login page
- A live `/api/summary` integration
- A role-aware UI
- A working DSW
- A What-If scenario builder
- A Port Brief downloader
- A vessel-action surface
- An audit log viewer
- A Berth Timeline, VTS Map, Pilotage, Performance, or Shift
  Log tab
- A port switcher
- A mobile / stakeholder / executive surface
- A replacement of Beta 10 at `/`
- A modification of any Beta 10 file
- A new API endpoint
- A new audit event type
- An activation of Stage E-prod
- A `server.py` change of any kind
- A merged production change

Anything resembling these in an M0 PR is **scope creep** and must
be rejected.

---

## 4. M0 Success Criteria

M0 is successful if and only if all of the following are true:

1. `cd frontend && npm install && npm run build` produces a
   `frontend/dist/` artefact on a developer machine
2. Opening that artefact in a browser shows the Horizon Dark
   3-column shell with the header, conditions bar, and three
   empty columns
3. The design tokens visually match the handoff prototype on a
   side-by-side comparison
4. `tests/test_beta10_regression.py` still passes 46/46
5. `server.py` is unchanged between Beta 10 baseline and M0 HEAD
6. `index.html` is unchanged
7. No Beta 10 file is modified
8. No new audit emission added
9. `horizon-v1-sandbox` Railway project (provisioned as an M0
   precondition) serves the V1 build at a sandbox URL distinct
   from `horizon-prod`
10. The sandbox URL is not linked from anywhere in `horizon-prod`

If any one of these fails, M0 has not closed.

---

## 5. M0 Non-Goals

M0 explicitly does **NOT** aim to:

- Connect to the Beta 10 backend
- Replace any part of `index.html`
- Demonstrate any role-specific behaviour
- Demonstrate any decision flow
- Prove the audit chain works end-to-end
- Prove RBAC works
- Replace the Beta 10 demo path
- Deliver any customer-visible value
- Be linked from `horizon-prod`
- Be discoverable to anyone other than Tony / Claude / ChatGPT
  during review

M0 is **infrastructure proof**, not capability proof. Capability
proof is M1+.

---

## 6. Protected Surfaces

The following surfaces are **immutable** during M0:

| Surface | Why |
|---|---|
| `server.py` | Beta 10 backend; any change risks regression |
| `index.html` | Beta 10 frontend; demo path |
| `audit.py` and the five audit helper modules | Phase 0.7 deliverable |
| `db.py`, `tenant.py` | Phase 0 audit DB plumbing |
| Phase 0 migrations (`alembic/`) | Schema baseline |
| `tests/test_beta10_regression.py` | Active CI gate |
| `requirements.txt` | Runtime dependency set |
| `railway.toml`, `Procfile`, `deploy/` | Beta 10 deploy manifest |
| `port_profiles.py` | Beta 10 port logic |
| Existing Beta 10 environment variables | `horizon-prod` runtime |
| `phase-0-complete @ 4ad4aae` tag | Immutable baseline |
| PRs #27, #28, #29 | DO-NOT-MERGE preview-deploy artefacts |

Any M0 PR that modifies any of the above must be rejected and
re-scoped.

---

## 7. Allowed Runtime Changes

**Runtime changes allowed in M0: ZERO.**

M0 is **fully additive at the file-system level** and **invisible
at the Beta 10 runtime level**. The Beta 10 process, the Beta 10
HTTP responses, the Beta 10 audit emission posture, and the Beta
10 deploy artefact are byte-for-byte identical before and after
M0.

The only file-system additions M0 makes are inside `frontend/`,
plus possibly:

- A single line in `.gitignore` to exclude `frontend/dist/` and
  `frontend/node_modules/`

That is the **entire allowed footprint** of M0 in the existing
repo. Everything else is inside `frontend/`.

---

## 8. Forbidden Runtime Changes

The following are **forbidden** in M0:

- Any edit to `server.py` (including the `/v1/*` static-file
  routing — that is M2+ once the V1 sandbox proves out and a
  separate authorisation is issued)
- Any edit to `index.html`
- Any edit to `audit.py` or audit helpers
- Any new audit event type definition
- Any new audit event emission call
- Any edit to `db.py`, `tenant.py`, or migrations
- Any change to `requirements.txt`, `Procfile`, `railway.toml`,
  or `deploy/`
- Any new environment variable read by Beta 10
- Any change to `tests/test_beta10_regression.py`
- Any new dependency in the Beta 10 Python runtime
- Any change to the Beta 10 Railway project (`horizon-prod`)
- Any deploy of V1 artefacts to `horizon-prod`
- Any activation of `AUDIT_EMISSION_ENABLED` in production
- Any unpause of Stage E-prod

If any of the above appears in an M0 PR diff, the PR must be
rejected, **not amended in place**.

---

## 9. Frontend Scaffold Scope

The `frontend/` directory M0 creates has the following structure
and **nothing more**:

```
frontend/
├── package.json
├── package-lock.json
├── vite.config.js
├── index.html                  ← Vite entry, NOT to be confused with Beta 10's index.html
├── .gitignore                  ← excludes node_modules/, dist/
└── src/
    ├── main.jsx
    ├── App.jsx                 ← renders shell with sample data
    ├── styles/
    │   ├── tokens.css          ← Horizon Dark design tokens
    │   ├── base.css            ← reset, typography
    │   └── layout.css          ← shell grid
    ├── components/
    │   ├── Card.jsx
    │   ├── Pill.jsx
    │   ├── CategoryPill.jsx
    │   ├── Dot.jsx
    │   └── Icon.jsx
    ├── layout/
    │   ├── HorizonHeader.jsx
    │   ├── ConditionsBar.jsx
    │   ├── LeftPanel.jsx       ← renders "(M0 placeholder)"
    │   ├── CenterPanel.jsx     ← renders "(M0 placeholder)"
    │   └── RightPanel.jsx      ← renders "(M0 placeholder)"
    └── data/
        └── sample.js           ← lifted from v1-handoff/prototype/data.js
```

**Explicitly NOT in M0:**
- `frontend/src/features/` (alerts, dashboard, timeline, vts,
  pilotage, performance, dsw, roster) — all M1+
- `frontend/src/api/` — no API client in M0
- `frontend/src/hooks/` — no polling, no clock, no countdown
- Any test framework, any test files
- Any storybook
- Any state management library
- Any routing library (no `react-router`)
- Any analytics or telemetry library

Dependency budget for M0: **React, ReactDOM, Vite, the Vite React
plugin.** That is the entire `package.json` dependencies list.

---

## 10. API Usage Scope

**M0 makes ZERO HTTP calls to the Beta 10 backend.**

The shell renders against `frontend/src/data/sample.js` — a
local module that exports the same data shape the prototype
uses. There is **no fetch, no polling, no auth header, no cookie
read** in M0.

Rationale:
- Removes risk of coupling V1 to a `/api/summary` shape that
  has not been formally captured / verified (see UX/UI Handoff
  Validation §6.1)
- Removes risk of CORS / cookie / port issues being confused with
  real architectural problems
- Makes M0 buildable and reviewable without a running backend
- Makes M0 reversible by `rm -rf frontend/`

Live API consumption is **M1**, gated on a separate
`/api/summary` shape verification spike.

---

## 11. Auth Scope

**M0 has no auth.**

- No login form
- No session cookie handling
- No `/login` POST
- No "you are logged in as X" affordance beyond a static label
  in the header

The M0 sandbox URL is reachable by anyone with the URL. Mitigation:
the URL is given only to Tony / Claude / ChatGPT during review;
it is not indexed; it carries no real data; it is hosted on a
sandbox Railway project distinct from `horizon-prod`.

A real auth path (consume the existing Beta 10 cookie flow, or
introduce a separate V1 auth) is **M1+** and requires its own
design note before implementation.

---

## 12. State Management Scope

**M0 uses React local state only.**

- No `useReducer` beyond what a single component needs
- No `useContext` for app-wide state
- No external state library (no Zustand, Redux, Jotai, Recoil,
  Valtio, MobX, etc.)
- No `react-query` / `swr` / data-fetching library

Rationale: the architectural choice between Context, Zustand, or
similar is an Execution Plan §17.1 open question. M0 must not
pre-empt that decision. M0 is **stateless enough that the choice
doesn't matter**.

---

## 13. Role Handling Scope

**M0 has a role label, not a role.**

The header may display "VTSO" as a static string. There is
**no role switcher in M0** — adding the tweaks-panel role
switcher from the prototype would (a) imply RBAC behaviour that
doesn't exist and (b) require the §9.3 amendment from the UX/UI
Handoff Validation to be implemented immediately, which expands
M0 scope.

Defer the role switcher to **M1 or later**, when:
- It is explicitly labelled as "Preview role (display only)"
- It does not change the user's session role
- It does not change visible permissions

Until then, M0's header shows the static label "VTSO" and that is
all.

---

## 14. What-If / DSW Scope

**M0 has neither.**

- No DSW modal
- No DSW stepper
- No DSW step components
- No What-If scenario builder
- No "Apply" button anywhere
- No "Commit decision" affordance
- No "Defer" / "Override" affordance
- No countdown timer

These are M3+ (DSW) and M5+ (What-If integration) per Execution
Plan §11. Premature inclusion creates the false-audit-completion
risk documented in the Handoff Validation §13.3.

---

## 15. Audit Scope

**M0 emits zero audit events.**

- No client-side telemetry
- No `console.log` of "audit-like" actions
- No "Audit Log" tab population (RightPanel right tab shows
  "(M0 placeholder)")
- No new event type defined
- No new audit emission call from backend (backend is untouched
  anyway)
- No "Decision logged" UI text anywhere
- No "Audit trail preserved" UI text anywhere

The audit ledger is **not part of M0**. Wiring the audit log
viewer is M8 per Execution Plan §11 and requires a separate
`GET /api/audit` endpoint that does not yet exist.

---

## 16. Deployment Scope

**M0 deploys to `horizon-v1-sandbox` only.**

- A Railway project named `horizon-v1-sandbox` (or equivalent),
  provisioned as an M0 precondition (see §21)
- That project deploys from the M0 branch / a dedicated V1
  branch, **not** from `main`
- Auto-deploy from `main` to `horizon-v1-sandbox` is **off** in
  M0 to avoid accidental V1 deploys when other PRs merge
- The sandbox URL is not linked from `horizon-prod`
- The sandbox URL is not indexed (no sitemap, no SEO)
- The sandbox carries no real customer data — only the static
  sample

`horizon-prod` (Beta 10) is **untouched**:
- No deploy
- No env-var change
- No build-config change
- No domain change

---

## 17. Sandbox / Environment Scope

The V1 environment topology M0 establishes:

| Project | Source | Purpose | M0 status |
|---|---|---|---|
| `horizon-prod` | `main` branch | Beta 10 production | UNCHANGED — no edit, no redeploy, no env change |
| `horizon-v1-sandbox` | V1 working branch | M0 visual proof | NEWLY PROVISIONED by Tony as a precondition |

**Only these two projects exist in M0.** `horizon-v1-preview`,
`horizon-v1-staging`, `horizon-v1-prod` (per Execution Plan §8)
are **M2+ concerns**.

The V1 working branch is a **single long-lived branch** during
M0 — branched off `main`, never merged back to `main`. Merging
V1 into `main` happens after M0 closes and a separate merge
authorisation is issued.

---

## 18. Regression Requirements

Throughout M0:

- `tests/test_beta10_regression.py` must pass 46/46 on every
  commit of the V1 branch (verifies M0 hasn't accidentally
  touched a Beta 10 file via shared tooling)
- `python -c "import server"` must succeed on Python 3.10
  (verifies the Beta 10 entry point still imports)
- A manual smoke of `horizon-prod` (load the four port pages)
  must pass after M0 deploys to sandbox (verifies Beta 10
  production unaffected)
- The Beta 10 demo flow used in customer demos must work
  identically before and after M0

If any of these regresses, M0 is rolled back immediately.

---

## 19. Rollback Requirements

M0 must be rollback-safe by a single git operation:

```
git revert <M0 merge commit>
rm -rf frontend/
```

That is the full rollback. No state migrations, no env-var
flips, no audit chain reconciliation, no customer notification.

Rollback success criteria:
- `frontend/` no longer exists in the working tree
- `git status` is clean
- Regression gate passes
- Beta 10 production unaffected (it never depended on M0)
- Sandbox URL returns 404 or the old (empty) state — either is
  acceptable

If rollback is not this simple, M0 has expanded beyond scope
and must be re-cut.

---

## 20. Exit Criteria for M0

M0 closes when **all** of the following are true:

1. M0 PR opened, reviewed by ChatGPT, authorised by Tony, merged
2. `frontend/dist/` builds locally on the developer machine
3. Sandbox URL renders the 3-column Horizon Dark shell
4. Design tokens verified against handoff prototype (side-by-side
   visual check, screenshot attached to the PR)
5. Regression gate passes 46/46
6. `server.py`, `index.html`, audit helpers, Phase 0 migrations,
   deploy manifests untouched
7. PRs #27, #28, #29 still untouched
8. Beta 10 demo flow verified working in `horizon-prod` after M0
   merge
9. No new audit event type, no audit emission call, no Stage
   E-prod activation
10. Sandbox URL not linked from `horizon-prod`

**M0 does NOT close because the UI "looks done."** M0 closes
because the **infrastructure has been proven** under all the
constraints above.

---

## 21. Preconditions Before M1

Before M1 (App Shell + Auth — per Execution Plan §11) can be
authorised, the following must close:

1. **M0 itself must be merged and verified per §20**
2. **UX/UI handoff formally authorised as design input** (per
   Handoff Validation §14 — separate Tony authorisation)
3. **`/api/summary` shape verification spike** captured (small
   PR documenting the actual response from all four ports)
4. **Auth design note** — how does V1 consume the Beta 10
   `horizon_session` cookie, given the V1 frontend will be
   served from `/v1/*` (M2+) or from a different domain
   (`horizon-v1-sandbox`)? This decision must be documented
   before M1 starts.
5. **State-library decision** — Execution Plan §17.1 open
   question must be resolved (default: React Context + local
   state; revisit later)
6. **Offline / failover decision** — Execution Plan §17.9 open
   question must be resolved before polling is introduced
7. **ChatGPT engineering review of the M0 PR** must approve
   merge

Until all seven close, M1 is **not authorised**.

---

## 22. Open Risks

### 22.1 Sandbox provisioning blocked

**Risk:** Tony does not have ready access to provision a second
Railway project; M0 cannot deploy.

**Mitigation:** M0 can close on local-build proof alone
(success criteria 1–6 of §20) with §20 items 8–10 deferred to a
small follow-up PR once the sandbox exists. Document this split
in the M0 PR description.

### 22.2 Vite tooling drift

**Risk:** Vite versions evolve; the M0 lockfile becomes the de
facto V1 build pinning. Future Vite security advisories may
require unplanned rebuilds.

**Mitigation:** capture Vite version pin in the M0 PR description;
add a calendar reminder to revisit dependencies quarterly.

### 22.3 Design-token drift from prototype

**Risk:** the handoff prototype's CSS evolves before M1; M0
tokens become stale.

**Mitigation:** `frontend/src/styles/tokens.css` carries a
header comment referencing the exact `v1-handoff/prototype/Horizon V1.html`
file SHA at the time of extraction.

### 22.4 Scope creep "while we're in there"

**Risk:** during M0 build, the temptation to add "just one tab"
or "just the DSW skeleton."

**Mitigation:** this document is cited in the M0 PR description;
any reviewer can reject scope expansion by quoting §3 / §8.

### 22.5 Sandbox URL leakage

**Risk:** the sandbox URL ends up in a screenshot / commit
message / customer-visible artefact; customers find V1 before it
is ready.

**Mitigation:** sandbox URL stays out of all commits, PR
descriptions, and screenshots. Reviewers verify the URL via
direct message, not git history.

### 22.6 Beta 10 regression via tooling

**Risk:** `npm install` running in the same repo introduces
`node_modules/` that accidentally gets bundled into a Railway
deploy of `horizon-prod`.

**Mitigation:** `frontend/node_modules/` and `frontend/dist/`
must be in `.gitignore`; `horizon-prod` deploy continues to use
`Procfile` / `railway.toml` which point only at the Python
entry point — no Node toolchain runs in `horizon-prod`.

### 22.7 Premature RBAC scaffolding

**Risk:** developer adds Context-based "currentUser" object with
a `role` field to "save time later"; an audit reviewer assumes
RBAC is partially implemented.

**Mitigation:** §13 explicitly forbids any role state structure
in M0 beyond a static header label.

---

## 23. Recommendations

### 23.1 Hold M0 until preconditions close

Per Handoff Validation §14.2 and this document §21, M0 should
not be authorised until:
- ChatGPT review of UX/UI Handoff Validation (PR #37)
- Tony's design-input authorisation of the handoff
- `/api/summary` shape verification spike
- `horizon-v1-sandbox` provisioning
- ChatGPT review of this M0 Scope Proposal

### 23.2 Adopt this document as the M0 PR description anchor

When M0 is later authorised, the M0 PR description should cite
this document and reproduce §4 (Success Criteria), §6
(Protected Surfaces), §8 (Forbidden Runtime Changes), and §20
(Exit Criteria) verbatim. Reviewers can then reject any in-PR
scope expansion by quoting the cited section.

### 23.3 Treat scope creep as the primary M0 risk

The handoff is polished enough that "build the whole thing"
will feel natural. The discipline in M0 is **not** building
what you can build; it is **not building** what M1+ should build.
Every "while I'm here" temptation must be answered with "M1+".

### 23.4 Keep the V1 working branch off `main` until M0 closes

V1 build does not flow through `main` during M0. A long-lived
V1 branch (e.g. `feat/v1-m0`) hosts the work, gets reviewed,
gets approved, then merges to `main` as a single M0 commit.
This keeps the regression gate uncomplicated and `main` clean.

### 23.5 Do not link `horizon-v1-sandbox` from `horizon-prod`

The sandbox is for review, not for users. Any link from
production introduces both a leak risk and a customer-confusion
risk. Keep them disconnected at the network and discovery layer.

### 23.6 Defer the `/v1/*` static route to M2+

The Execution Plan §5.3 permits a single `server.py` change to
serve `/v1/*`. **That change is not in M0.** M0 deploys the
V1 frontend to a **separate Railway project** so the question
"does V1 work?" is answered without touching `server.py` at
all. The `/v1/*` route is introduced only when V1 is otherwise
proven and a separate authorisation is issued (M2+).

### 23.7 Stop after M0 to assess

M0's purpose is **infrastructure proof**. After M0 closes, the
team should pause, write a brief retrospective (what worked,
what surprised us, what to adjust for M1), and then issue M1
authorisation. Sprinting M0 → M1 without a pause is how
governance debt accumulates.

---

## End of M0 Scope Proposal

**Status:** v0.1 scope-constraining proposal — planning only
**Implementation status:** None
**Next action:** ChatGPT engineering review, then Tony's
decision on (a) merging this proposal alongside the Handoff
Validation, and (b) whether — and when — to issue M0
authorisation against the constraints documented here.
