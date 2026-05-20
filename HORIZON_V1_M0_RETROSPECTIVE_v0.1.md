# Horizon V1 — M0 Retrospective (v0.1)

**Status:** Retrospective — documentation only
**Document version:** 0.1
**Date:** 2026-05-20
**Audience:** Tony (decision authority), ChatGPT (engineering review), future M1/M2 implementers
**M0 close commit on `main`:** `a1161cf` (merge of PR #45)
**Sandbox source:** `feat/v1-m0 @ 1f5447dc` (preserved on remote)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Untouched throughout M0.

**This document does not authorise implementation.** It records what
M0 proved and what to carry into M1.

---

## 1. Executive summary

M0 delivered the Horizon V1 frontend scaffold — a static React + Vite
shell with the Horizon Dark design system, mounted at the
`horizon-v1-sandbox` Railway project, rendering against a
`ViewSummary`-shaped static fixture with comprehensive DEMO labelling.

**The headline result of M0 is not the React shell. It is the
demonstration that V1 can evolve safely without destabilising
Beta 10.** Throughout the M0 cycle, Beta 10 production
(`horizon-prod`) stayed byte-identical at the deploy level, the
46/46 regression gate stayed green, the audit module stayed
unmodified, Stage E-prod stayed paused, and PRs #27 / #28 / #29
remained untouched. The V1 work happened entirely inside `frontend/`
and a single new Railway project (`horizon-v1-sandbox`) on a
long-lived branch (`feat/v1-m0`) — fully decoupled.

The single substantive surprise was that Railway's config-file
resolution is **not** rerouted by `rootDirectory` alone — a finding
that cost one failed deploy cycle and produced one corrective
commit (`1f5447d` adding `frontend/railway.json`) plus one Tony-
side dashboard action (pointing the service's config-file path at
that new file). The fix preserved the principle that Beta 10's
root `railway.toml` remains the canonical Beta 10 deploy config.

M0 used static data only — no backend calls, no `/api/summary`
fetch, no auth, no audit writes, no DB connection, no real
lifecycle actions. This was a deliberate constraint imposed in the
M0 Scope Proposal §10 / §11 / §15 and the Component & Interaction
Canon §7.4 + §11.5, and it held without compromise.

The recommendation for M1 (§10 of this retrospective) is to
preserve that same posture: read-only live-data integration first,
with auth / RBAC / audit attribution explicitly deferred until a
later milestone unless Tony authorises bringing them forward
individually.

---

## 2. What M0 proved

| Proof | Evidence |
|---|---|
| **V1 can evolve without touching Beta 10.** | 22 files added inside `frontend/`; zero files modified outside `frontend/`; `server.py` byte-identical between `98e562d` (pre-M0) and `a1161cf` (post-M0); regression gate 46/46 throughout |
| **`horizon-v1-sandbox` is a viable isolated deployment surface.** | Provisioned cleanly (single service, no add-ons, no DB, no production env vars, default Railway domain only); deploys auto-trigger from `feat/v1-m0` only (not `main`); zero shared state with `horizon-prod` |
| **The four-region Horizon Dark shell renders correctly.** | HTTP 200 from sandbox URL; Vite-built `dist/index.html` 540 B + 7 KB CSS + 159 KB JS (gzip ~51 KB); all 5 DEMO labels present in served bundle; `react` + `react-dom` + `serve` install successfully on Railway |
| **The Adapter Design Note's `ViewSummary` shape is consumable by components.** | `sample.js` shaped as `ViewSummary` (not prototype `data.js`, not raw `/api/summary`); all M0 components consume it without `undefined` reads; 7 lifecycle states render correctly per Canon §4 |
| **The §16.2 governance pattern scales beyond Phase 0.** | Every code change passed propose → review → authorise → implement → report → review → merge; Claude proposed; Tony authorised; ChatGPT reviewed; Tony merged. Zero unilateral Claude actions affected Railway production state |
| **Configuration-as-code at the service level works on Railway.** | `frontend/railway.json` (committed in `1f5447d`, paired with a single Tony-side dashboard config-path setting) is the canonical config source for the sandbox service; reversible by editing the file in a normal PR flow |
| **Beta 10 demo flow survives V1 evolution.** | Post-merge Tony visual check: `/` loads, `/login` works, port switcher works across all 4 ports, no production errors. The `horizon-prod` auto-redeploy from `main @ a1161cf` shipped the new `frontend/` directory but Beta 10's `server.py` never reads from it. Identical Beta 10 behaviour before, during, and after M0 |

---

## 3. What worked well

### 3.1 Aggressive scope constraint

The M0 Scope Proposal §3 + §8 + §11–§15 deliberately constrained M0
to **the smallest possible implementation slice**: a static
frontend with no API calls, no auth, no audit, no DB, no DSW, no
real actions. That discipline meant every potential M0 PR scope
expansion (e.g. "while we're in there, let's wire up `/api/summary`")
was instantly rejectable by quoting the Scope Proposal. The
constraint paid off twice: once at planning (clear shippable
target), once at the Railway config-precedence incident (no
backend coupling meant the rollback radius was just `rm -rf
frontend/` + revert, with zero Beta 10 risk).

### 3.2 The planning corpus as a contract

Sixteen V1 planning documents on `main` before any code shipped
(now 17 with this retrospective) carried real operational weight.
Every M0 decision had a citation: the dependency budget cited the
M0 Scope Proposal §9.7; `sample.js` shape cited Adapter Note §17.2;
DEMO labelling cited Canon §7.4 and §11.5; the seven lifecycle
states cited Canon §4 + Lifecycle Reconciliation §10.1; the
sandbox topology cited Sandbox Provisioning Plan §3–§11. When the
implementation report needed to defend a choice, the citation
existed. When Tony or ChatGPT asked "why this and not that," the
answer was always traceable to a merged document.

### 3.3 Two-phase deploy split

Splitting M0 implementation into **Phase 1 (local)** and **Phase
2 (Railway sandbox deploy)** turned out to be load-bearing. Phase
1 closed cleanly with all local validation gates green before
Railway was involved at all. When Phase 2 hit the config-
precedence issue, the Phase 1 work was not at risk — the
implementation itself was provably sound; the issue was Railway-
side. Without the phase split, the 502 Bad Gateway moment would
have read as "M0 broken", not "M0 implementation is right; Railway
needs a config tweak."

### 3.4 Read-only Railway verification

Every Railway interaction Claude performed before the dashboard
fixes was **read-only**: `railway list`, `railway status`,
`railway variables --json | jq 'keys'`, `railway domain`, public
HTTP probes. The pattern preserved a clean line between "Claude
verifies what Tony did" and "Tony provisions / configures."
Authoring rights stayed with Tony throughout. When Claude
encountered limits (the OAuth token was workspace-scoped, the CLI
lacked build/start command override), the response was honest
escalation rather than reaching for the GraphQL API. That
discipline cost ~15 minutes of explanation in turn but preserved
trust for the moments it really mattered.

### 3.5 Demo labelling

Nine surfaces in the M0 bundle carry explicit DEMO / DESIGN
VERIFICATION / M0 PLACEHOLDER labelling (top banner, header pill,
6 ConditionsBar tiles, DesignVerificationSwatch, 3 panel
placeholders, vessel cards). The labelling makes it operationally
impossible to mistake the sandbox for production. The fixture
(`sample.js`) carries no real customer data; the bundle source
contains zero `fetch(`, zero `XMLHttpRequest`, and only one
`/api/` string match — and that string is the comment text in
`RightPanel.jsx` explaining what's deferred to V1.x. There is no
false-state UI in M0.

### 3.6 Co-existence with Beta 10

The Execution Plan §5.2 coexistence model (Beta 10 stays at `/`;
V1 mounts at its own URL or `/v1/*` later) protected Beta 10
throughout. M0 deployed to `horizon-v1-sandbox-production.up.railway.app`,
a URL not linked from anywhere in production. Customers and
demo audiences had no path to discover V1. Beta 10's auto-deploy
from `main` triggered on the M0 merge but served byte-identical
Beta 10 because no Beta 10 file changed.

---

## 4. Key surprise — Railway config precedence over `rootDirectory`

### 4.1 What we expected

The mental model going in was: setting `rootDirectory: /frontend`
on the `horizon-v1-sandbox` service tells Railway *the entire
service context* lives inside `/frontend`. Railpack would
auto-detect Node from `frontend/package.json`; the service would
honour `frontend/package.json` scripts; Beta 10's root
`railway.toml` would be invisible to the sandbox by virtue of
being outside the service's root directory.

### 4.2 What actually happened

Railpack **did** correctly auto-detect Node from
`frontend/package.json` (deploy metadata showed
`detectedProviders: ["node"]`). `npm ci` ran successfully inside
the container, installing all 147 packages including `serve`.

But Railway's **config-file resolution is service-level, not
root-directory-level**. When the service was first connected to
the repo by Tony, Railway recorded `configFile: /railway.toml` —
the repo-root file — as the service's config source. From then
on, every deploy applied that file's build and start commands:

- Build: `pip install -r requirements.txt && playwright install chromium --with-deps || true`
- Start: `python3 server.py`

The `|| true` at the end of Beta 10's build command silently
**masked the pip failure** (no Python in the Node container) and
made the build status appear SUCCESS. Then the runtime tried to
exec `python3 server.py`, failed with `command not found`,
crash-looped 10 times against `restartPolicyMaxRetries: 10`, and
the public URL returned **502 Bad Gateway**.

### 4.3 What fixed it

A two-step correction:

1. **Code change (`1f5447d`)** — Claude added `frontend/railway.json`
   with the correct build / start commands for a Node + Vite
   static site (`npm install && npm run build` + `npx serve -s
   dist -l $PORT`)
2. **Dashboard change (Tony)** — Tony pointed the
   `horizon-v1-sandbox` service's **Config Path** setting at
   `frontend/railway.json`, overriding the default repo-root
   lookup

After both changes, the next Railway deploy (auto-triggered by
the dashboard save) resolved to:

- `configFile: /frontend/railway.json` ✓
- Build: `npm install && npm run build` ✓
- Start: `npx serve -s dist -l $PORT` ✓
- Instance: **RUNNING** ✓
- Public URL: HTTP 200 ✓

### 4.4 The lesson

**`rootDirectory` controls the build context; it does not control
the Railway config-file path.** Two separate settings. The Railway
config file is resolved at the *service* level via the dashboard
*Config Path* setting, defaulting to repo root.

For a multi-service monorepo where each service has its own
`railway.json`, **both** settings must be configured per service:

1. `rootDirectory` — for build context (where `package.json`,
   source files, build outputs live)
2. **Config Path** (dashboard, sometimes called *Railway Config
   File*) — for the deploy/build commands

Documented for future sandboxes / staging / preview environments
in §5.3 below.

### 4.5 Cost of the surprise

- 1 misleading SUCCESS build that crash-looped at runtime
- 1 corrective commit (`1f5447d`, +12 lines)
- 1 Tony-side dashboard action (Config Path setting)
- ~15 minutes of diagnostic + correction time

Beta 10 was unaffected throughout. The cost was contained to the
sandbox itself.

---

## 5. Sandbox deployment lessons

### 5.1 Two-Railway-projects model works

`horizon-prod` (Beta 10) and `horizon-v1-sandbox` (V1) are
**separate Railway projects** with completely independent deploy
lifecycles, env vars, domains, and source branches. The
separation eliminates the entire class of "an M0 mistake takes
Beta 10 down" risks. The Sandbox Provisioning Plan §3 / §4 / §6
predicted this; M0 validated it.

### 5.2 Per-service `railway.json` at the service root directory is the right pattern

After the §4 correction, `frontend/railway.json` became the
canonical sandbox config. Future changes (e.g. M1 adds a different
start command for an SSR mode, or changes restart policy) are
ordinary PR-flow edits to that file: commit on a feature branch,
push, sandbox auto-redeploys from the new commit. No more
dashboard build/start command overrides; no more divergence
between dashboard state and committed config.

### 5.3 Required configuration per Railway service in this repo

For any new Railway service in this repo that **does not** want
to inherit Beta 10's root `railway.toml`, **two** dashboard
settings must be configured at service creation:

| Setting | Value |
|---|---|
| Root Directory | service-specific subdirectory (e.g. `frontend`, or `apps/foo`, or `services/bar`) |
| Config Path (a.k.a. Railway Config File) | path inside that root directory (e.g. `frontend/railway.json`) |

Setting only Root Directory is **not sufficient** — Railway will
still read the root `railway.toml` and apply Beta 10's commands.
Document this in any future provisioning runbook.

### 5.4 The `|| true` masking effect

Beta 10's root `railway.toml` build command ends in `|| true`,
which is correct for Beta 10 (it makes `playwright install`
non-fatal because Beta 10 doesn't require it). But for any other
service consuming that build command, `|| true` masks every
preceding failure as SUCCESS. The lesson for future Beta 10
maintainers: assume the root `railway.toml` is **only** safe for
the `horizon-prod` runtime context. Any other service must
override.

### 5.5 Sandbox URL hygiene

The sandbox URL `https://horizon-v1-sandbox-production.up.railway.app`
is not linked from `horizon-prod`, not indexed
(`<meta name="robots" content="noindex, nofollow">` in the served
HTML), and carries no real customer data. The URL was shared with
Tony / Claude / ChatGPT via direct message, never committed,
never appeared in marketing material. This pattern should
continue for M1+.

### 5.6 Sandbox is ephemeral

If the sandbox accumulates cruft or the config drifts in non-
trivial ways, **delete and reprovision is acceptable**. There is
no state to preserve; the React build is rebuildable from the V1
working branch in minutes. The Sandbox Provisioning Plan §20.4
flagged this; M0 validated it remains true.

---

## 6. Governance lessons

### 6.1 §16.2 Claude / Tony split was load-bearing

Sandbox Provisioning Plan §16.2 explicitly declared **Tony does
Railway provisioning; Claude does not**. M0 stress-tested this
twice:

- During the Railway outage, when Claude proposed two paths (CLI
  vs dashboard) and Tony chose dashboard
- During the config-precedence incident, when Claude diagnosed
  the issue but stopped before applying a service-settings
  change Tony hadn't pre-authorised

In both moments, the discipline of "Claude proposes; Tony
authorises specific actions; Claude executes only those" prevented
escalating from CLI verification to GraphQL writes / dashboard
mutations without explicit go-ahead. The pattern is worth
preserving as M1 begins; M1 will introduce real write paths to
Beta 10 (or its V1 successor) and the same governance discipline
will be more valuable, not less.

### 6.2 Two-phase reporting prevents premature merge

The M0 PR opened after Phase 1 local validation only. Phase 2
deploy verification was a separately authorised step. When Phase
2 failed (the 502), the PR was not auto-merged — review continued.
Tony's M1 plan should keep the same split: local complete → PR
opened → deploy verification → merge authorisation. Don't merge
on Phase 1 alone unless Phase 2 is explicitly known to be
deferred (per Sandbox Provisioning Plan §18.1 local-build-only
fallback).

### 6.3 ChatGPT review caught a real issue pre-merge

The original v0.1 M0 plan had Tony connect Railway **before**
Claude pushed M0 code. ChatGPT review flagged that this would
make Railway try to build an empty branch. The v0.2 plan flipped
the order. The flip was small but consequential — it meant Tony's
first Railway-side action was on a branch that already had a
working local build, not on a placeholder. Future plans should
continue to expect this kind of ordering check from review.

### 6.4 Two ChatGPT review feedback items applied cleanly

- `serve` reclassified as Railway-runtime-only dependency (not an
  app framework, not part of the UI layer, removable if Railway
  static-site service supports `dist/` directly)
- `LifecycleSwatch` → `DesignVerificationSwatch` with explicit
  non-operational framing, "DEMO ONLY" header, "Example only — no
  real lifecycle state" per row, and explicit removal-in-M1+ note

Both changes survived implementation review and the served
bundle. Worth crediting ChatGPT review explicitly — these aren't
cosmetic touches.

### 6.5 Authorisation specificity matters

Tony's authorisation messages were explicit about scope: "Modify
only the V1 frontend surface", "Single new file: frontend/railway.json",
"Build Command: npm install && npm run build", "Start Command:
npx serve -s dist -l $PORT". The specificity prevented Claude
from extrapolating ("should I also add a healthcheck endpoint
while I'm here?"). When in doubt, the authorisation defaulted to
the narrowest possible interpretation. M1 should expect the
same.

---

## 7. Design / UX canon lessons

### 7.1 Closed colour / glyph / pill sets held in practice

The Canon §3 closed colour set (`--critical` / `--warning` /
`--success` / `--info` / `--text-muted`) + lifecycle palette
(`--purple` for OVERRIDE, `--blue` for ESCALATION) translated to
CSS custom properties cleanly. M0 had zero pressure to introduce
a new colour. The Canon §3.3 glyph alphabet (●, ■, ◆, ⇡, ✕, ○, ◇)
likewise held — the DesignVerificationSwatch renders each glyph
as plain Unicode in canon-coloured spans, no SVG required for
glyphs in M0.

### 7.2 `MeaningHeadline` was correctly omitted

The Canon §10.9 anti-pattern (MeaningHeadline overreach into
operational surfaces) was preserved by simply not implementing
MeaningHeadline at all in M0. The DSW (where MeaningHeadline is
sanctioned per Canon §7.3) is not in M0; nothing else gets the
teal-meaning-word treatment.

### 7.3 The `DesignVerificationSwatch` framing was the right
compromise

Rendering all seven lifecycle states inside the operator UI
would have risked the "fake audit certainty" anti-pattern (Canon
§10.3). Hiding them entirely would have made visual review
harder. The compromise — a card with explicit `DESIGN VERIFICATION
· DEMO ONLY` header and an `Example only — no real lifecycle
state` row label — is the right balance. The card is also marked
for removal in M1+ once real lifecycle surfaces ship, so the
demo artefact does not become permanent UI cruft.

### 7.4 The four-region shell is locked in

The 360 / 1fr / 380 column layout, 72 px top ribbon, 64 px
conditions ribbon — all rendered correctly at the sandbox URL
against the Horizon Dark palette. Canon §1.2 region widths are
now CSS variables. Future milestones building inside this shell
have a known surface.

### 7.5 `sample.js` shaped as `ViewSummary` was the right call

Per Adapter Design Note §17.2, `sample.js` is shaped as the
**target view model**, not the prototype `data.js` shape. This
positions M0 components to swap in M1's adapter output without
component-level rework. Validated: the M0 components all consume
`SAMPLE.portStatus.vesselsInPort`, `SAMPLE.conditions.windSpeedKts`,
`SAMPLE.dashboardMetrics.berthUtilisationPct` etc. — the exact
shape the adapter will produce in M1. No component code change
will be needed at M1 to switch from `sample.js` to
`adapter(rawSummary)`.

---

## 8. Protected Beta 10 lessons

### 8.1 The Phase 0 protected file list held

Throughout M0, the following remained unchanged:

- `server.py`, `index.html` (root)
- `audit.py` and the 5 audit helper modules
- `db.py`, `tenant.py`
- `tests/test_beta10_regression.py`
- `requirements.txt`, `railway.toml`, `Procfile`, `deploy/`,
  `port_profiles.py`
- `alembic/` migrations
- `phase-0-complete @ 4ad4aae` tag

The regression gate 46/46 was verified before every commit, after
every commit, before merge, and after merge. The protected-file
diff against PR #45 was empty by construction; the M0 PR was
mechanically inside `frontend/` only.

### 8.2 Beta 10 production behaviour was byte-identical post-M0

The post-merge Tony visual check confirmed `/` loads, `/login`
works, port switcher works across Brisbane / Melbourne / Geelong /
Darwin, demo behaviour unchanged, no production errors. The
M0 merge's auto-deploy of `horizon-prod` from `main @ a1161cf`
included the new `frontend/` directory in the deploy artefact but
Beta 10's `server.py` never reads from it, so functionally Beta
10 is unchanged.

### 8.3 No Stage E-prod activity

`AUDIT_EMISSION_ENABLED` was not toggled. Production
`DATABASE_URL` remained unset. Phase 0.7b–c audit emission paths
were not modified. The Stage E-prod pause continues per
Implementation Strategy §9.4.

### 8.4 PRs #27 / #28 / #29 timestamps stable

The DO-NOT-MERGE preview-deploy artefact PRs were observed at
every verification turn and remained unchanged. They are still
OPEN at their pre-M0 state.

### 8.5 Preview Postgres incident did not bleed into M0

The `preview-audit-activation` Postgres catatonit crash, although
operationally concurrent, was a separate Railway support thread
on a different Railway service in a different project. M0 had
zero dependency on that DB; the incident did not affect M0's
critical path at any point.

---

## 9. What should change before M1

### 9.1 Tidy stale `feat/v1-m0-placeholder` branch on remote

The placeholder branch (`feat/v1-m0-placeholder @ 98e562d`) is
left over from the initial Railway connection step before Tony
reconfigured the sandbox to use `feat/v1-m0`. It is stale and not
referenced by any service. Low priority; Tony can
`git push origin --delete feat/v1-m0-placeholder` whenever
convenient.

### 9.2 Decide the M1 sandbox source branch

Two options:

A. **Reuse `feat/v1-m0`** — extend the existing branch with M1
   commits. Sandbox auto-redeploys from each push. Simple, but
   conflates M0 and M1 history.
B. **Branch `feat/v1-m1` from `main @ a1161cf`** — fresh M1
   branch; Tony reconfigures the sandbox source from `feat/v1-m0`
   → `feat/v1-m1`. Cleaner history; one Tony-side dashboard
   action.

Recommendation: **Option B**. The dashboard action is small;
clean per-milestone branch history is worth it. M0 history stays
on `feat/v1-m0` for future reference.

### 9.3 Document the Railway Config Path setting persistence

After Tony's dashboard fix, the sandbox now reads
`frontend/railway.json`. M1 must NOT change the service's Config
Path to anything else (e.g. an M1-specific config file path) —
the path stays `frontend/railway.json`; M1's content edits go
into that file via PR. This convention should be added to the
Sandbox Provisioning Plan in a future v0.2 amendment if M1
significantly evolves the deploy pipeline.

### 9.4 Decide ACK as a write — M1, M1.5, or M2

Lifecycle Reconciliation §12.1 open question — proposed default
was M1.5 / M2 to keep M1 strictly read-only. The decision should
be made **before** the M1 plan is finalised, not during M1
implementation. Recommendation: defer ACK write path to M1.5 or
M2; keep M1 strictly read-only.

### 9.5 Decide auth model for M1 — same-origin / CORS / mock

Three options per the M0 close report:

A. **Wait until M2+** when `/v1/*` mounts on the Beta 10 domain
   via a `server.py` static-route addition — same-origin moment
   (cookie sharing trivial). M1 stays mock-auth.
B. **Add CORS to Beta 10 `server.py`** so the sandbox subdomain
   can call Beta 10's authenticated endpoints — requires
   `server.py` change, separate authorisation, and possibly cookie
   `SameSite=None; Secure` flags.
C. **Mock auth in M1** — operator pill stays static VTSO; sandbox
   stays DEMO; real auth deferred to a later milestone.

Recommendation: **Option A (wait for M2+) or Option C (mock auth
in M1)**. Option B requires modifying `server.py` and is the
most invasive of the three.

### 9.6 Sandbox URL pattern

The sandbox URL today is `horizon-v1-sandbox-production.up.railway.app`.
M1 will continue to use it (no custom domain in M1 per Sandbox
Provisioning Plan §8.1). If V1.x or M2+ wants a friendlier URL
(e.g. `v1-sandbox.horizon.ams.group`), that's a separate decision
with its own DNS / cert considerations.

### 9.7 Audit emission stays paused

No M1 work should activate `AUDIT_EMISSION_ENABLED` or set
production `DATABASE_URL`. Per Implementation Strategy §9.4,
Stage E-prod activation waits for V1.x once per-user identity is
on every audit row. M1 must not bring this forward.

---

## 10. M1 sequencing recommendation

### 10.1 M1 stays read-only

The recommended M1 scope:

- The adapter layer (`frontend/src/api/horizon.js` + adapters
  per Adapter Design Note §4) implementing the ViewSummary
  contract
- A `useSummary()` polling hook for `GET /api/summary`
- The login flow — **only if** Option A or B from §9.5 is
  authorised; otherwise mock-auth per Option C
- One operational tab content (likely Dashboard) consuming live
  adapter output
- Fixture-based adapter unit tests per Adapter Note §16
- **No** ACK / COMMIT / DEFER / OVERRIDE / ESCALATE writes
- **No** What-If integration
- **No** DSW
- **No** Replay
- **No** new audit event types
- **No** changes to `server.py` (unless Option B for auth is
  separately authorised)
- **No** Stage E-prod work

### 10.2 M1 dependency budget

App dependencies likely become: `react`, `react-dom`, `serve`,
plus **one date library** (per Adapter Note §13.1 — needs to be
DST-aware; `date-fns-tz` or `luxon` are leading candidates).
Tony decision needed on which.

### 10.3 M1 file scaffold extensions

Net new files inside `frontend/`:

```
frontend/src/api/
├── horizon.js                   ← single public entry
├── adapters/
│   ├── summaryAdapter.js
│   ├── conditionsAdapter.js
│   ├── vesselAdapter.js
│   ├── conflictAdapter.js
│   ├── guidanceAdapter.js
│   ├── dashboardAdapter.js
│   ├── etdRiskAdapter.js
│   ├── time.js
│   ├── status.js
│   └── units.js
└── fixtures/
    ├── brisbane-live.json
    ├── melbourne-sim.json
    ├── geelong-quiet.json
    ├── darwin-mixed.json
    ├── empty-conflicts.json
    ├── null-fields.json
    └── malformed.json
frontend/src/hooks/
└── useSummary.js
```

Plus likely one Dashboard tab component family (per Canon §4.5
Dashboard composition).

### 10.4 M1 plan should be written before M1 starts

Same pattern as the M0 Scope Proposal + M0 Implementation Plan
v0.2: a single markdown PR drafted, ChatGPT-reviewed, Tony-
authorised before any M1 implementation begins. The plan should
cite this retrospective.

### 10.5 Two-phase deploy for M1 too

Local first (Phase 1: adapter + tests + Dashboard content),
sandbox deploy verification second (Phase 2). The split that
worked for M0 will work for M1.

### 10.6 Don't compress M1 timeline because M0 was fast

M0 took roughly the time the M0 Scope Proposal predicted, plus
the unbudgeted Railway config-precedence incident. M1 has more
moving parts (adapter, polling, fixture tests, possibly auth) and
the dependency budget is likely larger. Plan accordingly.

---

## 11. Risks carried forward

### 11.1 Sandbox stays on `feat/v1-m0` until M1 source switch

If Tony's M1 plan elects Option B from §9.2 (new `feat/v1-m1`
branch), the sandbox needs a one-line dashboard change to point
at the new branch. Until then, the sandbox stays on `feat/v1-m0`
serving the M0 shell.

### 11.2 Workspace-scoped OAuth still in effect

The Railway CLI auth Tony refreshed during M0 is workspace-scoped
— it can reach `Project-Horizon` (Beta 10), `Project-Kyber`,
`Kyber Pulse`, `MAITS Mobilisation Manager`, and other workspace
projects. Claude's discipline of "no commands targeting non-
sandbox projects" held throughout M0 but is **discipline-only**,
not credential-scoped. For M1, consider creating a project-
scoped `RAILWAY_TOKEN` for `horizon-v1-sandbox` only and exporting
it as an env var. This was Option B from the pre-M0 capability
report and remains the more governance-aligned credential model.

### 11.3 Preview Postgres incident is still open

The `preview-audit-activation` catatonit crash is unresolved
(Tony on Railway support thread). It does not affect V1 work,
but it would block any future Phase 1.2 audit DB testing if it
ever needed to resume. Status check before any future audit DB
work.

### 11.4 Stage E-prod pause continues

Stage E-prod (production audit emission activation) remains
paused per Implementation Strategy §9.4. The recommendation is
to keep it paused until V1.x once per-user identity is on every
audit row. M1 must not bring this forward; it would require a
separate Tony authorisation independent of M1.

### 11.5 Root `railway.toml` still has `|| true` masking

Beta 10's root `railway.toml` build command will continue to
silently mask failures for any service that inadvertently
inherits it. Future Railway services in this repo (M2+
preview / staging / prod) MUST be configured with a service-
level Config Path per §5.3 to avoid the same trap M0 hit.

### 11.6 Stale `feat/v1-m0-placeholder` branch

Minor housekeeping risk: a developer rebasing or branching off
the wrong branch by accident. Low impact. Cleanup is a single
git command whenever Tony has a minute.

### 11.7 `serve` dependency might be removable in M1

If Railway's static-site offering supports `dist/` natively
without a Node runtime, `serve` can be dropped from
`dependencies`, reducing the M1 dep surface. Worth a check during
M1 planning.

---

## 12. Recommendations

### 12.1 Treat M0 as a working template for V1 milestones

The M0 cycle (Scope Proposal → Implementation Plan → ChatGPT
review → Tony authorisation → Phase 1 local → Phase 2 deploy →
post-merge verification) is the right shape for M1 and likely
M2+. Reuse it. The cycle costs ~1 day of plan-and-review per
milestone but prevents whole categories of failure.

### 12.2 Keep `frontend/railway.json` as the single sandbox config source

After the §4 correction, all sandbox build / start / restart
config lives in `frontend/railway.json`. Future changes to
sandbox behaviour go in there via PR, not via dashboard
overrides. The dashboard's Config Path setting (currently
`frontend/railway.json`) should not change.

### 12.3 Defer auth / RBAC / audit attribution unless explicitly authorised

M1 should remain read-only / live-data first. Auth, RBAC, and
audit attribution have inter-dependent design decisions (cookie
domain, role-scoped projection, per-user identity in audit
events) that touch `server.py`, the audit module, and the
Permission Model. Lumping them into M1 risks scope creep and
makes the plan unreviewable. Decompose them into V1.1 / V1.x /
post-V1.0 per Implementation Strategy §17.

### 12.4 Hold to the two-Railway-projects model

`horizon-prod` (Beta 10) and `horizon-v1-sandbox` (V1) stay
separate. Don't merge them, don't share state, don't add
`/v1/*` static routing to Beta 10's `server.py` in M1 — that's
M2+ territory per Sandbox Provisioning Plan §20.6.

### 12.5 Keep the planning corpus growing in lockstep with implementation

This retrospective brings the planning corpus to **17 documents**
on `main`. Each implementation milestone should be preceded by
at least one new planning document (scope proposal +
implementation plan) and may emit one new retrospective at
close. The corpus is the institutional memory; protect it.

### 12.6 Recommend M1 planning begins next

The M0 retrospective is the natural pause point before M1
authorisation. Tony's natural next step (when ready) is to
authorise drafting an `HORIZON_V1_M1_SCOPE_PROPOSAL_v0.1.md`
following the M0 Scope Proposal pattern. The §9 and §10 sections
of this retrospective sketch what should be in it; the M1 plan
itself is a separate document and separate authorisation.

### 12.7 Don't push back on small Railway dashboard actions

M0 demonstrated that small, well-scoped Tony-side dashboard
actions (provision project, set Config Path, override branch
source) are operationally cheap (~5 minutes each) and preserve
the governance boundary cleanly. M1 will likely have one or two
more (re-point sandbox source from `feat/v1-m0` to `feat/v1-m1`,
possibly cookie domain settings depending on §9.5 outcome). Treat
them as the natural rhythm.

---

## End of retrospective

**Status:** v0.1 retrospective — documentation only
**M0 status:** CLOSED at `main @ a1161cf`
**Sandbox status:** RUNNING from `feat/v1-m0 @ 1f5447dc`
**Next action:** Tony's decision on M1 plan authorisation. M0's
implementation, sandbox isolation, governance discipline, and
Beta 10 preservation are now the baseline for V1's next
milestone.
