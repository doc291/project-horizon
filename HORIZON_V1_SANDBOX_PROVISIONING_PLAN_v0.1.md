# Horizon V1 — Sandbox Provisioning & Environment Readiness Plan (v0.1)

**Status:** Operational plan — planning only, no implementation
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority + Tony-side Railway ops), ChatGPT (engineering review)
**Authoritative inputs (merged on `main @ c1577a9`):**
- `HORIZON_V1_DESIGN_INPUT_AUTHORISATION_v0.1.md` (PR #41)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` (PR #39)
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38)
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` (PR #37)
- `HORIZON_V1_EXECUTION_PLAN_v0.1.md` (PR #36)
- Prior six V1 foundation documents on `main`
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document does NOT authorise implementation.** It does
**not** provision any Railway project, does **not** create any
runtime config, does **not** change `server.py`, does **not**
modify the Railway configuration, and does **not** authorise M0
implementation. It is an operational readiness plan that Tony
will use to provision `horizon-v1-sandbox` separately.

---

## 1. Executive Summary

V1 cannot safely begin engineering until `horizon-v1-sandbox`
exists as a Railway project **independent of** `horizon-prod`
(Beta 10). This document defines exactly what that sandbox is,
how it relates to Beta 10, what it must NOT do, and what
preconditions must close before M0 implementation can begin.

**Core principle:** **two Railway projects, two deployment
lifecycles, two domains, zero shared state.** `horizon-prod`
continues to serve Beta 10 to customers throughout V1 build.
`horizon-v1-sandbox` hosts the V1 React frontend artefact for
review only. They never merge. They never share. A bad deploy
to `horizon-v1-sandbox` cannot affect `horizon-prod`.

**What M0 needs from the sandbox:**
- A Railway project distinct from `horizon-prod`
- A buildable target for the V1 React static bundle
- A URL accessible to Tony / Claude / ChatGPT
- No production data
- No production audit DB
- No production auth flow
- No Beta 10 traffic

**What M0 does NOT need from the sandbox:**
- A Python runtime (V1.0 sandbox serves a static React bundle;
  no `server.py` on sandbox)
- A database (no `DATABASE_URL`)
- Custom domain (a Railway-issued `*.up.railway.app` URL is
  sufficient for V1.0 sandbox review)
- Auto-deploy from `main` (M0 deploys from the V1 working
  branch only)
- Public discovery (no search-engine indexing, no link from
  `horizon-prod`)

**Tony's operational task (after this plan merges):**
1. Create a new Railway project named `horizon-v1-sandbox`
2. Disable auto-deploy from `main`
3. Configure deploy from the V1 working branch (created at M0
   start) only
4. Confirm the sandbox URL with Claude / ChatGPT
5. Authorise M0 implementation with explicit reference to this
   plan and to M0 Scope Proposal §12

**Until those five steps close, M0 does not begin.**

---

## 2. Sandbox Purpose

The sandbox exists for exactly **three** reasons:

### 2.1 Visual verification of V1 frontend

Allow Tony / Claude / ChatGPT to load the V1 React build in a
real browser, against a real URL, with a real HTTPS certificate
— so that "does the V1 shell render the Horizon Dark design
correctly?" can be answered without spinning up a local dev
server.

### 2.2 Deployment-pipeline proof

Prove that the V1 build artefact can deploy through Railway
exactly the same way Beta 10 does, but to a separate project —
so that future V1 deploys (staging, production) inherit a
known-working pipeline.

### 2.3 Isolation from Beta 10

Provide a place where V1 work can be reviewed, screenshotted,
demonstrated, and iterated without any risk to the live Beta 10
demo path used by customers.

The sandbox is **not** for:

- Customer demos (use `horizon-prod` Beta 10 for that)
- Performance testing (V1.0 is too early)
- Load testing (no live data, no auth, no realistic traffic)
- Security testing (the sandbox carries no sensitive data)
- A second production environment

---

## 3. Separation from Beta 10

The separation is **non-negotiable** and operates at every
layer:

| Layer | Beta 10 (`horizon-prod`) | V1 sandbox (`horizon-v1-sandbox`) |
|---|---|---|
| Railway project | `horizon-prod` (existing) | `horizon-v1-sandbox` (new) |
| Source branch | `main` (auto-deploy) | V1 working branch only (manual or explicit auto-deploy from V1 branch) |
| Runtime | Python 3.10 + `server.py` | Static React build (no Python on sandbox) |
| Domain | `project-horizon-production-a03c.up.railway.app` + any custom domains | Separate Railway-issued `*.up.railway.app` URL |
| Auth | Beta 10 `horizon_session` cookie | None in M0 |
| Database | None in Beta 10 (production `DATABASE_URL` unset) | None in V1 sandbox |
| Audit emission | Off in production (Stage E-prod paused) | Off (no audit code in M0) |
| External APIs (AISStream, MST, BOM, Open-Meteo) | Live | None in M0 (static sample data only) |
| Deploy lifecycle | Auto-deploy on `main` merge | Manual deploy from V1 working branch |
| Customers | Live demos | **None** |
| Auto-rollback | Railway default | Railway default; M0 rollback is `rm -rf frontend/` + revert |

**Explicitly:**

- **`horizon-prod` remains Beta 10 only.** No V1 code ever
  deploys to it.
- **V1 work never deploys to Beta 10.** Not in M0, not in M1,
  not until V1.0 has been independently authorised for
  production cutover (a separate decision, likely V1.x / V1
  GA).
- **M0 deploys only to sandbox.** Not to `horizon-prod`. Not
  to any other project.
- **No shared deployment pipeline.** Beta 10 deploys are
  triggered by `main` merges; V1 deploys are triggered by
  pushes to the V1 working branch. Different triggers,
  different projects, different artefacts.
- **No shared release process.** Beta 10 release notes,
  changelogs, tags are independent of V1's.
- **No Stage E-prod changes.** Production `DATABASE_URL`
  remains unset; production `AUDIT_EMISSION_ENABLED` stays at
  its current state.
- **No production database use.** The sandbox carries no
  database connection.
- **No production auth changes.** Beta 10's `/login` continues
  to issue `horizon_session` cookies unchanged. The sandbox
  has no auth at all in M0.

---

## 4. Proposed Railway Project Structure

The V1.0 Railway topology at M0 close:

```
Railway organisation (existing, Tony-owned)
│
├── horizon-prod                    ← Beta 10 production
│   ├── Source: main (auto-deploy)
│   ├── Runtime: Python 3.10
│   ├── Entry: server.py via Procfile / railway.toml
│   ├── Domain: project-horizon-production-a03c.up.railway.app
│   │           + any custom domains
│   ├── ENV: existing Beta 10 vars (PORT, COOKIE_*, AIS keys,
│   │       MST keys, BOM keys, Open-Meteo keys, etc.)
│   ├── No DATABASE_URL (Stage E-prod paused)
│   └── Status: UNTOUCHED throughout V1 build
│
└── horizon-v1-sandbox              ← NEW: V1.0 frontend review
    ├── Source: V1 working branch (e.g. feat/v1-m0)
    │           Auto-deploy from V1 branch DISABLED in M0
    │           (deploy manually or enable later)
    ├── Runtime: Static-file hosting (see §9)
    ├── Entry: frontend/dist/ (built locally OR via Railway
    │          build step; see §9.2)
    ├── Domain: separate *.up.railway.app
    │           (Railway-issued; no custom domain in M0)
    ├── ENV: minimal (see §11)
    ├── No DATABASE_URL
    └── Status: NEW PROJECT, provisioned by Tony pre-M0
```

**Later milestones** (M2+, separately authorised) may add:

```
└── horizon-v1-preview               ← M2+ (per Execution Plan §8)
└── horizon-v1-staging               ← M2+
└── horizon-v1-prod                  ← V1.0 GA cutover
```

These are out of scope for this provisioning plan. Only
`horizon-v1-sandbox` is in scope for M0.

---

## 5. Environment Naming Strategy

### 5.1 Project names

| Project | Name | Status |
|---|---|---|
| Beta 10 production | `horizon-prod` | Existing |
| V1 sandbox | `horizon-v1-sandbox` | **Provisioned by Tony pre-M0** |
| V1 preview (future) | `horizon-v1-preview` | M2+ |
| V1 staging (future) | `horizon-v1-staging` | M2+ |
| V1 production (future) | `horizon-v1-prod` | V1.0 GA |

If `horizon-v1-sandbox` is already taken in the Railway org,
acceptable alternatives (in order of preference): `horizon-v1`,
`horizon-frontend-sandbox`, `horizon-v1-dev`.

### 5.2 Why not "dev"?

The sandbox is not a developer environment. It is a **review
artefact**. Developers continue to work locally
(`npm run dev` against `frontend/dist/` on a developer machine,
per M0 Scope Proposal §4 #1). The sandbox is shared review
space, not shared editing space.

### 5.3 Why explicit `v1-sandbox`?

The naming distinguishes:

- `horizon-prod` — Beta 10, customer-facing
- `horizon-v1-sandbox` — V1, review-only, never customer-facing

If a future V1 deploys to a `prod-*` project, no human reading
the project list confuses it with Beta 10.

---

## 6. Branch-to-Environment Mapping

The branch-to-deploy mapping during M0:

| Branch | Deploys to | Auto-deploy? |
|---|---|---|
| `main` | `horizon-prod` only (Beta 10) | YES (existing) |
| `main` | `horizon-v1-sandbox` | **NO — DISABLED in M0** |
| `feat/v1-m0` (V1 working branch, created at M0 start) | `horizon-v1-sandbox` | Either disabled (manual push) or enabled from this branch only |
| `feat/v1-m0` | `horizon-prod` | **NO — NEVER** |
| Any other branch | Neither | NO |
| Open PRs against `main` | Neither | NO (no preview deploys) |

**Critical rule encoded for Railway configuration:**

- `horizon-prod` listens to `main` only.
- `horizon-v1-sandbox` listens to the V1 working branch (or is
  manually triggered). **It must NOT listen to `main`** —
  because `main` carries the V1 documentation merges; we do not
  want every documentation PR triggering a sandbox redeploy.

### 6.1 V1 working branch lifecycle

- Created at M0 start (e.g. `feat/v1-m0`)
- Long-lived during V1.0 build
- Receives M0 commits, then M1, M2 commits as those milestones
  are authorised
- Merges to `main` only after V1.0 is GA-ready (a separate
  decision, late V1.0)
- The V1 working branch's existence does not change `main`'s
  behaviour at all — Beta 10 keeps deploying from `main`
  uninterrupted

---

## 7. Deployment Flow

### 7.1 Beta 10 deployment flow (existing — UNCHANGED)

```
PR opened against main
  → ChatGPT review
    → Tony authorises merge
      → gh pr merge
        → main updated
          → Railway detects push to main
            → horizon-prod auto-deploys
              → live Beta 10 update
```

This flow continues throughout V1 build with **no V1
intervention**.

### 7.2 V1 sandbox deployment flow (NEW)

```
M0 implementation work on feat/v1-m0 branch
  → local npm run build produces frontend/dist/
    → commit changes to feat/v1-m0
      → push feat/v1-m0
        → Railway detects push to feat/v1-m0
          → horizon-v1-sandbox deploys frontend/dist/
            → sandbox URL refreshed
              → Tony / Claude / ChatGPT load sandbox URL for
                review
```

**No `gh pr merge` is involved in this flow.** Sandbox
redeploys do not require Tony's per-deploy authorisation
beyond the initial M0 authorisation — because the sandbox
carries no customer traffic and no real data.

Merging the V1 working branch to `main` is a **separate**,
later decision that is **not** part of M0.

### 7.3 What does NOT trigger a sandbox deploy

- Merging documentation PRs to `main` (planning corpus)
- Merging Beta 10 fixes to `main`
- Any push to any branch other than the V1 working branch

---

## 8. Domain / URL Strategy

### 8.1 Sandbox URL

Whatever Railway issues by default — typically
`{project-name}-{random}.up.railway.app`. **No custom domain
in M0.**

A custom domain (e.g. `v1-sandbox.horizon.ams.group`) is **not
required** for M0. It can be added in M2+ if the team wants
shorter URLs for screenshots / demos, but is out of scope here.

### 8.2 The sandbox URL is NOT linked from horizon-prod

- No link in Beta 10 `index.html`
- No link in customer-facing pages
- No mention in customer-facing marketing or sales material
- No appearance in `horizon-prod` outbound traffic (e.g. CORS
  preflight)

### 8.3 The sandbox URL IS shared

- With Tony (via direct message — not commit, not PR
  description)
- With Claude (via Tony — not committed)
- With ChatGPT (via Tony — not committed)

**The URL itself never appears in a public commit, PR
description, screenshot caption, or chat log shared outside
this review group.**

### 8.4 SEO / discovery

- The sandbox responds with `X-Robots-Tag: noindex, nofollow`
  on the HTML response (if Railway's static-file host permits)
- A `robots.txt` of `User-agent: *\nDisallow: /` is shipped
  with the V1 build to discourage indexing
- No sitemap.xml in the V1 build

These are belt-and-braces measures; the primary protection is
that no one outside the review group has the URL.

---

## 9. Static Frontend Hosting Strategy

### 9.1 What the sandbox serves

The sandbox serves the static output of `npm run build` —
the contents of `frontend/dist/`:

```
frontend/dist/
├── index.html            ← React app entry (Vite-generated)
├── assets/
│   ├── *.js              ← bundled JS
│   ├── *.css             ← bundled CSS
│   └── ... fonts, etc.
└── robots.txt            ← optional, per §8.4
```

**That is the entire deploy artefact.** No Python, no
`server.py`, no `audit.py`, no migrations. The sandbox does
not run any backend code.

### 9.2 How Railway serves it

Two acceptable approaches; Tony picks one:

**Option A — Railway static site service** (preferred if
available)
- Railway's static-hosting offering serves `frontend/dist/`
  directly
- No Node, no Python runtime
- Single page application routing (all paths fall back to
  `index.html`)

**Option B — Minimal Node static server** (fallback)
- A `package.json` `start` script runs a tiny Node static
  server (e.g. `serve dist`)
- Acceptable if Railway requires a runtime entry point
- Still no Python, no `server.py`, no business logic

If Option A is available, use it. If only Option B is
available, the Node static server adds **one** Node dependency
(`serve` or equivalent) to M0's dependency budget — which
brings the M0 dependency count to: React, ReactDOM, Vite,
Vite React plugin, plus one static-server package. This is a
**small** expansion of M0's dependency budget (M0 Scope
Proposal §9 specified 4 deps); Tony's M0 authorisation should
explicitly acknowledge whichever option is taken.

### 9.3 SPA fallback routing

Whatever serving mechanism is used, **all unknown paths must
fall back to `index.html`** so that React Router (if M1 introduces
it) works. M0 does not yet introduce React Router, but the
fallback should be configured in M0 to avoid M1 re-config.

### 9.4 Build pipeline

In M0:

- Build is **local** on a developer machine (`npm run build` in
  the M0 implementer's working directory)
- `frontend/dist/` is **not** committed to git (in
  `.gitignore`)
- Railway either:
  - Re-runs the build on its side using a build command (`npm
    install && npm run build && serve dist`); OR
  - Receives a pre-built `frontend/dist/` (less common; only
    used if Railway build is unavailable)

Default expectation: **Railway builds.** This means
`horizon-v1-sandbox` needs Node 18+ available — which most
Railway buildpacks provide automatically when they see a
`package.json` with build scripts.

---

## 10. Runtime Isolation Requirements

### 10.1 No Python on the sandbox

The sandbox runtime does **not** include the Beta 10 Python
runtime. Specifically:

- No `server.py` runs on `horizon-v1-sandbox`
- No `audit.py`, no audit helpers, no `db.py`, no `tenant.py`
- No Phase 0 migrations are referenced
- No `requirements.txt` is consumed by the sandbox

This is enforced by **the sandbox's deploy artefact being
`frontend/dist/` only** — no Python files exist in
`frontend/`.

### 10.2 No database on the sandbox

- No `DATABASE_URL` environment variable set
- No Postgres add-on attached to `horizon-v1-sandbox`
- No connection to `horizon-prod`'s database (which is also
  unset in production)
- No connection to any audit DB

### 10.3 No external API calls from the sandbox

V1.0 sandbox renders static `sample.js` data per M0 Scope
Proposal §10. Therefore the sandbox:

- Does not call AISStream
- Does not call MST
- Does not call BOM
- Does not call Open-Meteo
- Does not call any AMS backend
- Does not call any third party

The sandbox is **fully offline** with respect to external data.

### 10.4 No shared resources

`horizon-v1-sandbox` does not share Railway add-ons with
`horizon-prod`:

- No shared Postgres (neither has one anyway)
- No shared Redis (Beta 10 doesn't use Redis)
- No shared object storage
- No shared environment variables

If, by Railway organisation convention, environment variables
are shared at the org level, the sandbox **ignores** them — it
does not read AIS keys, MST keys, BOM keys, etc., because the
M0 build does not call those APIs.

---

## 11. Environment Variables Policy

### 11.1 Variables the sandbox SHOULD have

Minimal set:

| Variable | Value | Reason |
|---|---|---|
| `PORT` | provided by Railway | static server binds to this |
| `NODE_ENV` | `production` | Vite production build behaviour |

That is **all** that M0 sandbox requires.

### 11.2 Variables the sandbox MUST NOT have

| Variable | Why excluded |
|---|---|
| `DATABASE_URL` | No database on sandbox; Stage E-prod paused |
| `AUDIT_EMISSION_ENABLED` | No audit code in M0 |
| `COOKIE_NAME` / `COOKIE_SECRET` / equivalent | No auth in M0 |
| `AISSTREAM_API_KEY` | No API calls in M0 |
| `MST_*` keys | No API calls in M0 |
| `BOM_*` keys | No API calls in M0 |
| `OPEN_METEO_*` keys | No API calls in M0 |
| Any production secrets | Sandbox carries no production data and must not be able to access production sources |

### 11.3 If Railway org-level variables exist

Some Railway organisations expose env vars at the org level
that all projects inherit. If `horizon-v1-sandbox` inherits
any of the variables in §11.2:

- **Override at the project level with an empty string** OR
- Disable the org-level inheritance for this project

The sandbox's container should not have any of the §11.2
variables set in its actual runtime environment.

---

## 12. Audit & Database Policy

### 12.1 The sandbox emits no audit events

- No audit code is shipped in the V1 React build
- No client-side telemetry that mimics audit semantics
- No "decision logged" UI text
- The right-panel Audit Log tab shows `(M0 placeholder)`

### 12.2 The sandbox connects to no database

- No `DATABASE_URL`
- No Postgres add-on
- No connection string for the audit DB
- No connection string for any data store

### 12.3 Stage E-prod stays paused

- `AUDIT_EMISSION_ENABLED` is not toggled
- Production `DATABASE_URL` remains unset
- Phase 0 audit module remains in its current state
- The sandbox does **not** un-pause Stage E-prod by any
  means

### 12.4 Backend audit emission

`horizon-prod` (Beta 10) audit emission is controlled by
`AUDIT_EMISSION_ENABLED` (default true, but production
`DATABASE_URL` is unset so emission is a no-op). The V1
sandbox has **no influence on this**. Backend changes to audit
emission require separate authorisation (per Implementation
Strategy §9.4) and are explicitly out of scope of V1.0.

---

## 13. Logging & Observability Expectations

### 13.1 Sandbox logs

Whatever Railway's default static-hosting logs are. Sufficient
for M0:

- Build logs (when Railway builds the React bundle)
- Access logs (which IPs hit the static URL — for sanity
  checking)
- No application logs (no application code beyond the static
  server)

### 13.2 No production-grade observability

- No Sentry / Datadog / NewRelic integration
- No custom error reporting
- No analytics (no Google Analytics, no Mixpanel, no Plausible)
- No telemetry of any kind

These are all V1.x concerns. M0 is too early.

### 13.3 What Tony / Claude / ChatGPT use to verify

- The sandbox URL loads in a browser
- Visual side-by-side comparison against handoff prototype
  screenshots
- Browser devtools to inspect the network tab (must show only
  static asset requests; zero `/api/*` calls)
- Browser console (must show no errors)

That is the observability M0 needs.

---

## 14. Rollback & Recovery Expectations

### 14.1 Sandbox rollback

If a V1 sandbox deploy is broken:

- **Railway's previous-build feature** restores the last
  working build (one click)
- The broken commit on `feat/v1-m0` can be reverted via
  `git revert` and pushed
- Worst case: the sandbox URL serves a 503 until the next
  successful build — this is acceptable because **no
  customer-facing traffic exists**

### 14.2 No Beta 10 impact during V1 rollback

By definition, V1 rollback only affects `horizon-v1-sandbox`.
`horizon-prod` is on a different project, deploys from a
different branch (`main`), and is unaware of V1 work.

### 14.3 The M0 rollback path

If M0 itself needs to be undone (e.g. dependency conflict
discovered late):

1. `git revert` the M0 commits on `feat/v1-m0`
2. `rm -rf frontend/` (if cleanup desired)
3. `feat/v1-m0` rebuilds clean
4. `horizon-v1-sandbox` redeploys the now-empty (or rolled-back)
   state
5. `main` is unchanged; Beta 10 is unaffected

### 14.4 What rollback CANNOT do

Rollback cannot **un-provision** `horizon-v1-sandbox`. Once
the Railway project exists, it exists. Tear-down is a separate
Tony action.

---

## 15. Security Expectations

### 15.1 Sandbox is not "secure" by V1.x standards

The sandbox has **no auth**, **no user data**, **no sensitive
information**. Anyone who has the URL can load it. The
mitigations are:

- The URL is not shared outside the review group
- The sandbox carries only static sample data (no real vessel
  positions, no real customer information, no real audit
  data)
- The sandbox cannot reach any backend (no API calls in M0)

### 15.2 What the sandbox does NOT protect

- The URL itself if leaked
- The static bundle's source code (the React build is
  inspectable in browser devtools; this is fine because there
  is no business logic in M0 beyond rendering sample data)

### 15.3 What is safe to put on the sandbox

- Horizon Dark visual design
- Static sample data (lifted from prototype's `data.js`,
  reshaped to `ViewSummary`)
- Placeholder text ("M0 placeholder", "coming in V1.x")

### 15.4 What is NOT safe to put on the sandbox

- Real vessel data
- Real customer names
- Real agent names
- Real port operations data
- Production API keys (none should be there — §11.2)
- Production cookies or session data
- AMS or customer internal documents

The M0 implementation PR must include a manual check: no real
data exists in the V1 bundle.

### 15.5 HTTPS

Railway issues HTTPS by default on `*.up.railway.app`
subdomains. This is sufficient for M0. No custom certificate
work required.

---

## 16. Claude Code Deployment Rules

### 16.1 What Claude (this agent) does

- **Propose** deployment changes via PR description / commit
  message
- **Implement** the V1 frontend build, scaffolding, and PR
  flow when M0 is explicitly authorised
- **Verify** via local `npm run build` that the artefact
  builds before submitting M0 PR

### 16.2 What Claude does NOT do

- **Self-deploy.** Claude never pushes to `horizon-v1-sandbox`
  directly. Railway picks up the V1 branch (or Tony triggers
  manually).
- **Provision Railway projects.** Project creation, env-var
  configuration, branch-to-project mapping is **Tony-side
  ops**. Claude documents what's needed; Tony executes.
- **Self-merge.** Every merge requires Tony's explicit
  authorisation, even within the V1 working branch (M0 is one
  merge — the M0 PR — and that merge needs the same governance
  pattern as Phase 0 / Phase 1 PRs).
- **Change `horizon-prod`.** No Claude action ever touches
  Beta 10's Railway project, env vars, or domains.
- **Authorise itself.** Claude does not declare "M0
  authorised" — Tony does.

### 16.3 Deployment governance pattern

For every V1 sandbox deploy that requires per-commit visibility
(e.g. a milestone close):

1. Claude implements the change on `feat/v1-m0`
2. Claude submits an M-series PR (M0, M1, etc.) against `main`
   — but this PR is **not for merge**, it is for review
3. ChatGPT engineering reviews
4. Tony authorises the milestone close
5. Tony merges (or Tony delegates to Claude with explicit
   per-PR authorisation, as in Phase 0 / Phase 1)

This mirrors the Phase 0 / Phase 1 governance pattern that has
been the consistent operating model since session start.

---

## 17. Protected Surfaces

The following surfaces are **immutable** throughout sandbox
provisioning and M0:

| Surface | Why |
|---|---|
| `phase-0-complete @ 4ad4aae` tag | Immutable baseline |
| `server.py` | Beta 10 backend |
| `audit.py` and the 5 audit helper modules | Phase 0.7 deliverable |
| `db.py`, `tenant.py` | Phase 0 audit DB plumbing |
| Phase 0 migrations (`alembic/`) | Schema baseline |
| `tests/test_beta10_regression.py` | Active CI gate |
| `requirements.txt` | Beta 10 runtime dependency set |
| `railway.toml`, `Procfile`, `deploy/` | **Beta 10 deploy manifest — separate from V1** |
| `port_profiles.py` | Beta 10 port logic |
| `index.html` | Beta 10 frontend |
| `horizon-prod` Railway project (existing) | Production project |
| Production `DATABASE_URL` (unset) | Stage E-prod paused |
| Production `AUDIT_EMISSION_ENABLED` | Stage E-prod paused |
| PRs #27, #28, #29 | DO-NOT-MERGE preview-deploy artefacts |
| All V1 planning documents on `main` | Reference corpus |

Any provisioning step or M0 PR that touches any of the above
must be rejected and re-scoped.

---

## 18. Preconditions Before M0 Begins

M0 implementation may not begin until **all six** of the
following close:

| # | Precondition | Status | Closes when |
|---|---|---|---|
| 1 | This sandbox provisioning plan merged | OPEN — this PR | PR #42 merges |
| 2 | `horizon-v1-sandbox` Railway project created | OPEN — Tony-side ops | Tony confirms project exists |
| 3 | Sandbox configured per §5–§11 (naming, branch mapping, env vars) | OPEN — Tony-side ops | Tony confirms configuration |
| 4 | Sandbox URL confirmed reachable (returns a placeholder 200 OK or 404 — anything other than DNS failure) | OPEN — Tony-side ops | Tony reports URL works |
| 5 | Explicit M0 authorisation message from Tony citing M0 Scope Proposal §12 + this provisioning plan | OPEN — pending | Tony's message |
| 6 | V1 working branch created (`feat/v1-m0` or equivalent) | OPEN — closes at M0 start | M0 implementation begins |

Conditions 1–5 must close in order. Condition 6 opens M0
implementation.

### 18.1 If sandbox provisioning is blocked

If Tony cannot provision the sandbox (Railway account limits,
billing, organisational constraints), M0 may proceed in
**local-build-only mode**:

- Conditions 1, 5, 6 still close as documented
- Conditions 2, 3, 4 are deferred to a follow-up
- M0 acceptance criteria 8, 9, 10 from M0 Scope Proposal §20
  are not enforced at M0 close
- M0 close requires only that `frontend/dist/` builds locally
  on a developer machine

This is the fallback per M0 Scope Proposal §22.1 (sandbox
provisioning risk). It is acceptable but discouraged — the
sandbox closes a real risk (deploy-pipeline proof) that
local-build alone cannot.

---

## 19. Risks

### 19.1 Accidental deploy to `horizon-prod`

**Risk:** a misconfigured Railway setting causes V1 commits to
push to `horizon-prod`, breaking Beta 10.

**Mitigation:** `horizon-prod` listens to `main` only. V1 work
lives on `feat/v1-m0`, never on `main`, until V1.0 GA cutover
(a separate, much later decision). As long as the V1 working
branch never merges to `main` during V1 build, this risk is
eliminated by construction.

### 19.2 Auto-deploy from `main` triggering sandbox redeploys

**Risk:** `horizon-v1-sandbox` is misconfigured to listen to
`main`, causing every documentation merge to trigger an empty
or broken sandbox redeploy.

**Mitigation:** §6 / §11.2 explicitly require sandbox to listen
to the V1 working branch only (or to be manually deployed).
Tony verifies this at provisioning time.

### 19.3 Env-var leakage from org to sandbox

**Risk:** Railway org-level env vars (production AIS keys,
BOM keys) propagate to `horizon-v1-sandbox`, allowing the
sandbox to accidentally hit production APIs.

**Mitigation:** §11.3 — override at project level with empty
strings, or disable org inheritance for this project. The
sandbox should also not have any code that reads those keys
in M0 (it's a static React build).

### 19.4 Sandbox URL leakage

**Risk:** the sandbox URL ends up in a public commit, social
media post, customer email, or marketing material; customers
find V1 before it is ready.

**Mitigation:** §8.3 — URL shared by direct message only,
never committed. §8.4 — `noindex` + `robots.txt` as
belt-and-braces.

### 19.5 Confusion about which project is which

**Risk:** Tony / Claude / ChatGPT reference the wrong
project URL in a deploy decision; a V1 change gets pushed to
Beta 10 by mistake.

**Mitigation:** §5 naming strategy (explicit `v1-sandbox` in
the name). Every M0+ PR description states which project the
deploy targets. Claude's deploy actions are scoped to the V1
working branch only — they cannot affect `main`.

### 19.6 Static-hosting pitfall — SPA routing missing

**Risk:** Railway serves only existing files; a refresh on
`/v1/dashboard` returns 404 instead of `index.html`.

**Mitigation:** §9.3 — SPA fallback configured in M0 even
though React Router is not yet used. This prevents M1 from
discovering the gap.

### 19.7 Build-tool drift

**Risk:** Node version on Railway differs from Node on the
developer machine; build succeeds locally but fails on
Railway.

**Mitigation:** `package.json` pins Node engine version
(`"engines": { "node": ">=18" }`). M0 PR includes a successful
local build screenshot + a successful Railway build log.

### 19.8 Local-build-only fallback masks real deploy issues

**Risk:** M0 closes on local-build-only (per §18.1), and
when sandbox provisioning eventually happens, a deploy bug
surfaces that should have been caught at M0.

**Mitigation:** even in local-build-only mode, the M0 PR
should include a record of attempting the sandbox deploy and
the specific failure mode. M0 close note documents which
exit-criteria items were deferred to the follow-up.

### 19.9 Custom domain temptation

**Risk:** someone adds a custom domain (e.g.
`v1-sandbox.horizon.ams.group`) to the sandbox to make URLs
shorter for screenshots, exposing the V1 work to broader
discovery.

**Mitigation:** §8.1 — no custom domain in M0. Custom domain
is M2+ if needed at all.

---

## 20. Recommendations

### 20.1 Provisioning is Tony-side

The actual Railway project creation, configuration, and
verification is **Tony-side ops**. Claude documents what is
needed (this plan); Tony executes. This split is consistent
with prior Tony-side ops (Phase 0.7 sandbox DB provisioning,
Phase 1.2 preview redeploys, Phase 1.x rollback drill prep).

### 20.2 Sandbox provisioning should be small

Following this plan, sandbox provisioning is approximately:

1. Create Railway project `horizon-v1-sandbox`
2. Disable auto-deploy from `main`
3. Configure deploy from V1 working branch (created at M0
   start) — or leave deploys manual
4. Confirm minimal env vars (§11.1) only
5. Disable org-level env-var inheritance OR override §11.2 vars
   to empty
6. Verify default `*.up.railway.app` URL responds
7. Report URL to Claude / ChatGPT via direct message

This is **~30 minutes of Tony-side ops**, not a major
engineering exercise.

### 20.3 Adopt this plan as the M0 PR's deployment-section anchor

When M0 is later authorised and the M0 PR is opened, the M0
PR description's "Deployment" section should cite this
provisioning plan and reproduce §17 (Protected Surfaces) and
§18 (Preconditions) verbatim. Reviewers can then reject any
deploy-related scope expansion by quoting the cited section.

### 20.4 Treat the sandbox as ephemeral

The sandbox is **not** a long-term environment. If V1.0
reaches a milestone close and the sandbox becomes stale, it
can be torn down and re-provisioned. There is no state to
preserve. This makes operational hygiene easy: just rebuild.

### 20.5 Stop after this plan; await sandbox provisioning

After this plan merges, the next operational step is **Tony
provisioning the sandbox**. Claude does no work between this
plan's merge and Tony's M0 authorisation.

When Tony issues the M0 authorisation (citing this plan + M0
Scope Proposal §12), Claude opens M0 implementation against
the constraints in §17 / §18 / M0 Scope Proposal §6 / §8.

### 20.6 Keep Beta 10 visible

Throughout sandbox provisioning and M0:

- `horizon-prod` continues to serve customers
- Beta 10 demos work
- Regression gate green
- No silence from Beta 10's deploy pipeline

The presence of V1 must not be discoverable from `horizon-prod`.

---

## End of provisioning plan

**Status:** v0.1 sandbox provisioning & environment readiness
plan
**Implementation status:** None
**Next action:** ChatGPT engineering review, then Tony's
decision on merging. Once merged, the operational next step is
Tony's provisioning of `horizon-v1-sandbox` per §20.2 (~30
minutes of Railway ops), followed by Tony's explicit M0
authorisation citing this plan and M0 Scope Proposal §12. M0
implementation begins only after both close.
