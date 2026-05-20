# Horizon — Independence & Architecture Reset (v0.1)

**Status:** Strategic governance reset — documentation only
**Document version:** 0.1
**Date:** 2026-05-20
**Authoriser:** Tony Trajceski (Horizon product / commercial lead)
**Effective from:** `main @ e90eb56` (immediately after PR #50 merge)
**Scope of authority:** All future Horizon planning, architecture,
product positioning, commercial framing, and implementation work
performed under this repository.

**This document does not authorise implementation work.** It
does not change code, runtime files, Railway configuration, or
the regression gate. It establishes how Horizon is described
and reasoned about going forward.

---

## 1. Executive summary

Horizon is **an independent, AMSG-owned product and strategy**.
All future Horizon planning, architecture, product positioning,
and implementation must proceed on that basis.

Horizon is **not** built on Smart Ocean X. Horizon does not
depend on Smart Ocean X as an underlying platform, a partner
substrate, a technical dependency, or a commercial foundation.
Smart Ocean X is no longer part of Horizon.

Operationally and technically, Horizon is exactly where the
V1.0 corpus already places it: a Beta-10-derived Python +
React maritime operations intelligence system, owned and
governed by AMSG, with M0 closed, M1 substantially closed
pending Tony's post-merge Beta 10 visual check (M1 §18 item
10), and M1.5 / M2 to be authorised separately.

This reset confirms what M0 and M1 already proved
structurally: the implementation has been independent of any
external substrate from the first commit on `main`. The
codebase has never imported Smart Ocean X libraries, called
Smart Ocean X endpoints, deployed to Smart Ocean X
infrastructure, or required Smart Ocean X credentials. The
Railway projects (`horizon-prod`, `horizon-v1-sandbox`) are
AMSG-controlled. The Beta 10 backend in `server.py` and the
M1 React frontend in `frontend/` are AMSG-controlled. The
audit module, port profiles, and operational logic are
AMSG-authored.

What changes is the **language and framing** of future work,
not the bits. Future scope proposals, implementation plans,
review packs, and operational documentation must describe
Horizon as an independent AMSG product. Historical documents
that reference Smart Ocean X are not rewritten here; they
remain as historical record, but future Horizon decisions
must not lean on them as architectural premises.

---

## 2. Decision statement

**Effective immediately:**

> Horizon is an independent product owned and operated by AMS
> Group. Smart Ocean X is no longer part of Horizon. No future
> Horizon planning, architecture, code, deployment,
> commercial discussion, or stakeholder communication shall
> position Horizon as built on, partnered with, dependent on,
> or commercially derived from Smart Ocean X.

This decision is governance-level. It does not change
runtime, deployment, code, or data. It changes how Horizon
is reasoned about and described in every document, message,
PR, and stakeholder conversation from this point forward.

---

## 3. What changes

### 3.1 Strategic framing

Future Horizon work treats Horizon as an **AMSG-native
product**. The product strategy, roadmap, milestone
sequencing, customer narrative, and engagement model are
defined by AMSG's own commercial and operational priorities
— not derived from, harmonised with, or dependent on Smart
Ocean X strategy.

### 3.2 Product positioning

Horizon is positioned as **AMS Group's port operations
intelligence platform** built on:

- AMS Group's operational expertise in Australian and
  regional port operations
- AMSG's data integration approach (live AIS via AISStream
  / MST, BOM weather + tides, Open-Meteo, port-specific
  rosters and rules per `port_profiles.py`)
- AMSG's predictive coordination logic (conflict detection,
  recommendation generation, decision-time snapshot
  architecture per Phase 0.7b §1.4.1)
- AMSG's port ecosystem orchestration (multi-port profiles
  for Brisbane / Melbourne / Geelong / Darwin in V1.0;
  expandable to other ports under AMSG's commercial
  direction)
- AMSG-controlled deployment (Railway projects under AMSG
  organisation, no third-party-substrate dependency)

Commercial customers are AMSG customers. Stakeholders are
AMSG-relationship stakeholders. Pilot ports are AMSG-pilot
ports.

### 3.3 Commercial framing

Horizon's commercial model — pricing, licensing, deployment
options, customer engagement, support — is determined by
AMSG. There is no revenue share, no platform fee, no
substrate licence, no co-branded offering, no joint go-to-
market with Smart Ocean X. Customer agreements are AMSG
agreements.

### 3.4 Architecture description

Architecture documents (existing and future) describe
Horizon as:

- A **Python 3.10** monolithic backend (`server.py`) running
  on AMSG-controlled Railway infrastructure
- A **React + Vite** frontend (M0 scaffold + M1 adapter
  layer) under `frontend/`
- A **per-tenant** SHA-256-hash-chained audit ledger
  architecture (Phase 0.7, Stage E-prod paused)
- A **multi-port profile** model with simulated + live data
  fallbacks per `port_profiles.py`
- A **multi-tenant-per-deployment** posture (one tenant per
  Railway deployment in V1.0; multi-tenant-in-one-DB
  deferred to V2+ per Implementation Strategy §14.2)

**None of these architectural elements reference, integrate
with, or depend on Smart Ocean X.** Future architecture
descriptions retain this structure and continue to omit any
Smart Ocean X framing.

### 3.5 Stakeholder communication

Internal and external Horizon communications — slide decks,
demos, customer briefs, board memos, capability statements,
RFP responses, partner pitches — describe Horizon as AMSG-
owned and AMSG-built. Mentions of Smart Ocean X in future
Horizon-focused materials are inappropriate and should be
removed or reframed before circulation.

---

## 4. What does not change

### 4.1 Code on `main`

Zero. The codebase (`server.py`, `audit.py` and helpers,
`db.py`, `tenant.py`, `port_profiles.py`, `requirements.txt`,
`railway.toml`, `Procfile`, `deploy/`, `tests/`, `alembic/`,
the entire `frontend/` tree on `main @ e90eb56`) is byte-for-
byte unchanged by this reset. No file modification is
proposed.

### 4.2 Deployment topology

Zero changes. `horizon-prod` (Beta 10 production) and
`horizon-v1-sandbox` (V1 review sandbox) remain in their
current Railway configuration. No env-var changes. No
add-on changes. No domain changes. No source-branch changes.
No restart, redeploy, or reprovisioning of either Railway
project.

### 4.3 The Phase 0 baseline

`phase-0-complete @ 4ad4aae` remains the immutable Beta 10
baseline. The 46/46 regression gate
(`tests/test_beta10_regression.py`) remains the active CI
gate.

### 4.4 The V1 planning corpus

All 19 V1 planning documents merged on `main` (PRs #30–#48)
remain authoritative within their respective scopes. The M1
Scope Proposal, M1 Implementation Plan, Adapter Design Note,
Component & Interaction Canon, Lifecycle Reconciliation,
Sandbox Provisioning Plan, M0 Retrospective, and all
foundation documents continue to govern. **None of these
documents references Smart Ocean X as a dependency**, so no
amendments are required to align with this reset.

### 4.5 The Stage E-prod pause

`AUDIT_EMISSION_ENABLED` remains in its current state.
Production `DATABASE_URL` remains unset. Stage E-prod
activation remains a separately authorised, future-V1.x
decision per Implementation Strategy §9.4 — independent of
this reset.

### 4.6 The Kyber boundary

The strict separation between the Project-Horizon and
Project-Kyber codebases / Railway projects (per the
multi-project Railway workspace) remains in force. This
reset is specific to Horizon's relationship with Smart
Ocean X and does not modify the Kyber boundary.

### 4.7 PRs #27, #28, #29

The DO-NOT-MERGE preview-deploy artefact PRs remain OPEN,
untouched, and unaffected by this reset.

### 4.8 Historical references

Documents authored before this reset that may reference
Smart Ocean X (e.g. earlier business plans, board materials,
historical strategy decks, the Smart Ocean X agreement
itself) are **not rewritten** by this document. They remain
as historical artefacts. Future Horizon decisions must not
rely on the Smart Ocean X-related premises in those
documents, but the documents themselves are preserved as
record. The Smart Ocean X agreement is specifically out of
scope of this reset — it is a separately governed legal
instrument that is not touched by this document.

---

## 5. Architecture implications

### 5.1 Backend (`server.py`)

`server.py` is AMSG-authored, AMSG-owned, AMSG-deployed. It
has no external substrate dependency. All connectors it
contains (AISStream, MST, BOM, Open-Meteo, optional Postgres
audit DB) are public-API or AMSG-controlled. The Phase 0
audit chain is AMSG-implemented.

No architecture change. Future backend evolution (RBAC,
role-scoped `/api/summary` projection, apply-decision
endpoint, audit retrieval endpoint, replay backend) is
sequenced per Implementation Strategy §17 and Lifecycle
Reconciliation §11 — all under AMSG governance, with no
external substrate involvement.

### 5.2 Frontend (`frontend/`)

M0 scaffold and M1 adapter layer are AMSG-authored, AMSG-
deployed to `horizon-v1-sandbox`. The adapter consumes
captured `/api/summary` fixtures from `frontend/public/fixtures/`
(committed to the AMSG repo). No external substrate is
involved at the frontend layer.

### 5.3 Deployment

Railway projects (`horizon-prod`, `horizon-v1-sandbox`) are
AMSG-billed, AMSG-credentialled, AMSG-controlled. No third-
party-substrate infrastructure is part of Horizon. Future
environments (`horizon-v1-preview`, `horizon-v1-staging`,
`horizon-v1-prod` per Execution Plan §8) remain under AMSG
control.

### 5.4 Data sources

External data sources used by Beta 10:

- **AISStream** — public API; AMSG-credentialled if used in
  production
- **MyShipTracking (MST)** — third-party API; AMSG-
  credentialled commercial integration
- **BOM** (Bureau of Meteorology, Australia) — public free
  data
- **Open-Meteo** — public free weather data
- **QShips** — Port of Brisbane simulator / historical data

None of these are Smart Ocean X dependencies. All are
AMSG-credentialled or public-API integrations.

### 5.5 Audit ledger

The Phase 0.7 SHA-256 hash-chained audit ledger architecture
is AMSG-designed and AMSG-implemented. It is per-tenant; it
runs on AMSG-controlled Postgres (when activated under Stage
E-prod, V1.x); it produces AMSG-authoritative audit records.
No Smart Ocean X involvement in audit data, retention, or
chain integrity.

### 5.6 Identity & RBAC (future V1.1+)

When per-user identity and server-side RBAC ship (V1.1+ per
Implementation Strategy §17 + Permission Model §11), the
identity provider, role catalogue, and permission matrix are
AMSG-owned. Customer single-sign-on (SSO) integrations, if
any, are AMSG-to-customer relationships.

### 5.7 Multi-tenancy (V2+)

The V2+ multi-tenant-in-one-DB roadmap (Implementation
Strategy §14.2) is AMSG-architected. Tenant isolation,
data segregation, audit chain per-tenant genesis (already
implemented in Phase 0), and tenant-scoped configuration
are all AMSG concerns. No external multi-tenant substrate
is in scope.

---

## 6. Product positioning implications

### 6.1 Product name

**"Horizon"** (or, where ambiguity is possible, **"AMSG
Horizon"** or **"Horizon by AMS Group"**). The product is
not "Smart Ocean X Horizon", "Horizon on Smart Ocean X",
"Horizon powered by Smart Ocean X", or any variant
implying substrate dependency.

### 6.2 Category

Horizon is a **port operations intelligence platform** or
**maritime decision-support platform** in AMSG's product
category. It coordinates vessel movements, detects scheduling
conflicts, models downstream impacts, and supports
operational decisions. Internal taxonomy may evolve;
external category framing must not borrow from Smart Ocean X
positioning.

### 6.3 Target customers

Australian and regional port authorities, port operators,
shipping agents, and the operational ecosystem AMSG already
serves. Customer pipelines, pilot agreements, and engagement
roadmaps are AMSG-owned. No customer is acquired or
positioned via Smart Ocean X.

### 6.4 Roadmap positioning

Horizon's roadmap (V1.0 frontend-first replacement of Beta
10's index.html → V1.1 RBAC + role-scoped projection → V1.x
Stage E-prod audit activation → V1.6 Replay → V2 multi-tenant)
is AMSG's roadmap. It evolves on AMSG's commercial and
operational timeline.

### 6.5 Competitive positioning

Horizon competes in the maritime operations intelligence
market as an AMSG product. Competitive analysis, win/loss
review, and feature comparison are conducted against direct
maritime-software competitors — not against Smart Ocean X
positioning or in coordination with Smart Ocean X strategy.

---

## 7. Commercial implications

### 7.1 Revenue

All Horizon revenue accrues to AMSG. There is no platform
fee, substrate licence, partner share, royalty, or revenue-
share arrangement with Smart Ocean X in respect of Horizon.

### 7.2 Pricing

Horizon's pricing model — subscription, per-port, per-seat,
enterprise licence, custom — is set by AMSG independently.
Pricing does not need to be harmonised with Smart Ocean X
pricing or any other external pricing structure.

### 7.3 Customer agreements

All Horizon customer contracts, MSAs, SOWs, pilot
agreements, and support agreements are AMSG-to-customer.
Smart Ocean X is not a party to any Horizon customer
agreement.

### 7.4 Support & service

Horizon production support, incident response, escalation
paths, and SLAs are AMSG-delivered. The Railway support
relationship for `horizon-prod` and `horizon-v1-sandbox` is
AMSG-to-Railway. (The current open Railway support thread
for the preview Postgres catatonit issue is, and remains, an
AMSG-to-Railway engagement.)

### 7.5 Compliance & regulatory

Compliance with Australian privacy law, port-authority
regulation, AMSA / NOPSEMA expectations, and customer
data-handling requirements is AMSG's responsibility.
Compliance posture is documented in the V1 planning corpus
(Information Architecture §13 sensitive-information
boundaries, Permission Model §11 server-side enforcement)
and remains AMSG-owned.

### 7.6 Marketing & PR

Horizon's marketing and PR — case studies, white papers,
conference presentations, customer testimonials, press
releases — describe Horizon as AMSG's product. Future
marketing materials should not co-brand with Smart Ocean X
or reference Smart Ocean X as a platform layer.

---

## 8. Documentation implications

### 8.1 Future V1 planning documents

Every future markdown document committed to this repo
describing Horizon — scope proposals, implementation plans,
review packs, retrospectives, decision records — describes
Horizon as AMSG-owned and AMSG-built. No new document
introduces Smart Ocean X as a premise.

### 8.2 Future PR descriptions and commit messages

PR descriptions and commit messages for Horizon work must
not frame the work as Smart Ocean X-related, Smart Ocean
X-derived, or Smart Ocean X-supporting. Engineering changes
are AMSG engineering changes.

### 8.3 Future stakeholder briefs

Any document Tony prepares to brief AMSG leadership,
customers, regulators, or external partners describes
Horizon as AMSG's independent product. Smart Ocean X
context, if needed at all, is presented as separate /
historical / unrelated.

### 8.4 Existing documents

Existing documents on `main` (the 19 V1 planning documents
+ this repository's earlier history) are not rewritten.
**Spot-audit finding:** none of the 19 V1 planning documents
on `main @ e90eb56` references Smart Ocean X as a dependency,
substrate, or commercial premise. Specifically:

- `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` — AMSG roles
  only
- `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` — AMSG
  operational lifecycle only
- `HORIZON_V1_SCREEN_ARCHITECTURE_v0.1.md` — AMSG console
  surfaces only
- `HORIZON_V1_INFORMATION_ARCHITECTURE_v0.1.md` — AMSG data
  flows only
- `HORIZON_V1_IMPLEMENTATION_STRATEGY_v0.1.md` — AMSG-
  controlled implementation phases
- `HORIZON_V1_REVIEW_PACK_v0.1.md` — AMSG stakeholder
  review
- `HORIZON_V1_EXECUTION_PLAN_v0.1.md` — AMSG-controlled
  execution
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` — Claude
  Design as design provider, not platform substrate
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` — frontend
  scaffold scope
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` — `/api/summary`
  shape audit against Beta 10's `server.py`
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` — frontend
  adapter contract
- `HORIZON_V1_DESIGN_INPUT_AUTHORISATION_v0.1.md` — Claude
  Design UX/UI handoff authorisation
- `HORIZON_V1_SANDBOX_PROVISIONING_PLAN_v0.1.md` —
  AMSG Railway topology
- `HORIZON_V1_COMPONENT_INTERACTION_CANON_v0.1.md` — UX
  governance
- `HORIZON_V1_LIFECYCLE_RECONCILIATION_v0.1.md` —
  lifecycle bridge
- `HORIZON_V1_M0_RETROSPECTIVE_v0.1.md` — M0 lessons
- `HORIZON_V1_M1_SCOPE_PROPOSAL_v0.1.md` — M1 scope
- `HORIZON_V1_M1_IMPLEMENTATION_PLAN_v0.1.md` — M1 plan
- `HORIZON_V1_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md`
  (this document)

No amendment is required to any of the prior 19 documents
for Smart Ocean X reasons. If future review identifies
incidental wording that benefits from sharpening, those
amendments are tracked separately and not bundled into this
reset.

---

## 9. Codebase implications

### 9.1 No code changes

This reset triggers **zero code changes** in the repository.
The codebase is already structurally independent. No file
modification, deletion, or rename is required.

### 9.2 No commit-message rewriting

Historical commit messages on `main` are not rewritten.
Force-push rewriting is forbidden anyway (per standing
governance); this reset explicitly reaffirms that no
historical commit message requires retroactive editing.

### 9.3 No imports / dependencies to remove

A grep audit of the codebase against Smart Ocean X-related
strings — `smart_ocean`, `smartocean`, `smart-ocean`,
`sox`, or similar — should return zero matches in any
runtime file. (This audit is documentation-only; not run
during this PR. If a future audit surfaces unexpected
matches, they would be flagged as a separate cleanup PR.)

### 9.4 No env-var changes

No `SOX_*` or `SMART_OCEAN_*` env vars exist or need to be
removed. The env vars in use are all AMSG-controlled per the
Sandbox Provisioning Plan §11.

### 9.5 No deployment changes

`horizon-prod` and `horizon-v1-sandbox` Railway projects are
already under AMSG control. No reprovisioning, re-pointing,
or substrate migration is required.

### 9.6 No data migration

No customer data, audit ledger data, or operational state
needs migration. The Stage E-prod pause keeps the audit
ledger empty in production; when activated in V1.x, it will
be AMSG-controlled from the first event onwards.

---

## 10. M0 / M1 impact assessment

### 10.1 M0 status

**M0 remains valid and closed.** M0's deliverable (the M0
React shell scaffold in `frontend/` at PR #45 merge commit
`a1161cf`) was always independent of any external substrate:
React, Vite, design tokens lifted from the AMSG-internal
Horizon Dark prototype, static `sample.js` data, deployed
to AMSG's `horizon-v1-sandbox` Railway project.

No M0 artefact references Smart Ocean X. No M0 acceptance
criterion depended on Smart Ocean X. M0 closure (per M0
Retrospective §1 — "M0 proved V1 can evolve safely without
destabilising Beta 10") is not affected by this reset.

### 10.2 M1 status

**M1 remains substantially closed pending the Tony-side
Beta 10 visual check.** M1's deliverable (the adapter
layer, polling hook, partial Dashboard tab at PR #50 merge
commit `e90eb56`) is fixture-fed read-only against the
captured `/api/summary` fixtures committed in PR #49.

M1's data path is:

> sandbox URL → `serve` static-file server → `dist/fixtures/*.json` (AMSG-captured fixtures) → adapter → ViewSummary → React components

There is no external substrate anywhere in this path. M1
closure is not affected by this reset. The remaining
acceptance item (§18.10 — Beta 10 visual check post-merge)
is Tony-side and unaffected.

### 10.3 No re-validation required

Because M0 and M1 are structurally independent of any
external substrate, no re-validation pass is required to
confirm alignment with this reset. Their existing acceptance
criteria — already passed (M0) or pending only Tony's visual
check (M1) — remain unchanged.

### 10.4 PRs #27, #28, #29

The DO-NOT-MERGE preview-deploy artefact PRs are unaffected
by this reset. They are operational artefacts from Phase 1.2
audit DB activation testing, deployed to AMSG's Railway
preview environment. No Smart Ocean X involvement; no change
needed.

---

## 11. M2+ implications

### 11.1 M1.5 / M2 scope proposal

When M1.5 / M2 is authorised, its scope proposal will frame
the work as AMSG-owned. Specifically, the first-write
milestone (ACK per Lifecycle Reconciliation §12.1 — open
question default: defer ACK to M1.5 / M2) introduces a
write to the AMSG-owned audit chain, hitting an AMSG-owned
backend endpoint, authenticated against an AMSG-issued
session. No external substrate is involved.

### 11.2 V1 RBAC / role-scoped projection (V1.1)

V1.1 introduces server-side RBAC and role-scoped
`/api/summary` projection (per Implementation Strategy §17
+ Permission Model §11). The role catalogue is AMSG's; the
identity provider is AMSG-controlled; the projection logic
is AMSG-authored.

### 11.3 Stage E-prod activation (V1.x)

Stage E-prod activation (production audit emission
enablement per Implementation Strategy §9.4) remains a
future authorised step. When activated, the production
audit ledger is on AMSG-controlled Postgres, with AMSG-
controlled retention, AMSG-controlled access, and AMSG-
authoritative chain integrity.

### 11.4 V1.6 Replay

V1.6 Replay (per Canon §8, Lifecycle Reconciliation
§11.3) is AMSG-designed. The Replay storage model, the
audit chain verification UI, the export-with-hash-proof
mechanism, and the three replay modes (Operational /
Executive / Regulator) are all AMSG-controlled. No
external substrate.

### 11.5 V2+ multi-tenant

V2+ multi-tenant-in-one-DB (per Implementation Strategy
§14.2) is AMSG-architected. Tenant onboarding, isolation,
per-tenant audit chain genesis, tenant-scoped configuration,
and tenant billing are all AMSG concerns.

### 11.6 Customer SSO (when relevant)

If a customer requires SSO integration, that integration is
AMSG-to-customer, configured against AMSG's identity
provider. No external substrate sits between AMSG and the
customer.

### 11.7 New milestone authorisation pattern

Every future milestone (M1.5, M2, M2.x, V1.1, V1.x, V1.6,
V2+) follows the established Horizon governance pattern:

1. Scope proposal markdown PR → ChatGPT review → Tony
   authorisation → merge
2. Implementation plan markdown PR → ChatGPT review →
   Tony authorisation → merge
3. Implementation PR (Phase 1 local) → ChatGPT review →
   Tony authorisation
4. Phase 2 sandbox deploy verification (where applicable)
5. Tony merge authorisation
6. Optional retrospective PR

Every step under AMSG governance. No external substrate
authorisation step.

---

## 12. Documents requiring future language review

The 19 V1 planning documents on `main` were spot-audited
(§8.4 above) and **none requires amendment for Smart Ocean
X reasons**. The audit confirms that every existing
document already describes Horizon as AMSG-owned.

**Future language review** is therefore prophylactic, not
remedial:

- New planning / strategy documents authored from `main @
  e90eb56` onwards include a line in their authoritative-
  inputs header confirming Horizon's independence (one
  sentence, e.g. "Horizon is AMSG-owned per
  `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md`")
- Tony-authored stakeholder briefs / slides / decks
  prepared after this reset are reviewed against the
  forbidden-framing list in §13 before circulation
- Engineering review (ChatGPT / Claude) of future scope
  proposals and implementation plans includes a one-line
  check that no Smart Ocean X premise is reintroduced

If a previously authored document is reviewed for an
unrelated reason and Smart Ocean X language is incidentally
encountered, the language can be flagged for a separate
documentation hygiene PR — never bundled with the unrelated
substantive change.

---

## 13. Explicit forbidden framing

The following framings are **forbidden** in future Horizon
documentation, code comments, commit messages, PR
descriptions, stakeholder briefs, customer communications,
and internal AMSG strategy materials prepared in respect of
Horizon:

| Forbidden framing | Why |
|---|---|
| "Horizon is built on Smart Ocean X" | False — Horizon is built on AMSG's own Beta 10-derived stack |
| "Horizon uses Smart Ocean X as a platform" | False — there is no Smart Ocean X platform layer |
| "Horizon is a Smart Ocean X application" | False — Horizon is an AMSG product |
| "Horizon depends on Smart Ocean X for X" | False — Horizon has no Smart Ocean X dependency |
| "Smart Ocean X powers Horizon" | False — AMSG powers Horizon |
| "Horizon and Smart Ocean X share data / infrastructure / customers" | Future state: no |
| "Horizon's roadmap aligns with Smart Ocean X" | False — Horizon's roadmap is AMSG's |
| "Horizon is co-branded with Smart Ocean X" | False — Horizon is AMSG-branded |
| "Smart Ocean X is a Horizon partner / substrate / provider" | False — no such relationship in future Horizon work |
| Any future PR description, scope proposal, or implementation plan that introduces a Smart Ocean X reference as a premise | Future Horizon work is independent of Smart Ocean X |

Variants of the above ("powered by", "in partnership with",
"on top of", "integrated with", "delivered via") are
similarly forbidden in respect of any Smart Ocean X
relationship.

---

## 14. Recommended wording going forward

| Context | Recommended wording |
|---|---|
| Product short form | **"Horizon"** |
| Product long form | **"AMSG Horizon"** or **"Horizon by AMS Group"** |
| Product category | "AMS Group's port operations intelligence platform" / "AMSG's maritime decision-support system" |
| Ownership | "AMSG-owned", "AMS Group's own", "developed by AMS Group" |
| Built on | "Built on AMSG's operational expertise + Beta 10 backend + React V1 frontend" |
| Hosted on | "Hosted on AMSG-controlled Railway infrastructure" |
| Data sources | "Integrating live AIS, BOM weather, BOM tides, Open-Meteo, port-specific operational data" |
| Customers | "AMSG customers" / "AMSG pilot ports" |
| Roadmap | "Horizon's AMSG-controlled roadmap" |
| Audit chain | "AMSG-controlled SHA-256 hash-chained audit ledger" |
| Strategic context | "An independent AMSG product" / "AMSG-strategy-driven" |

These are recommendations, not rigid templates. The
principle: every Horizon mention going forward makes
AMSG ownership clear and does not imply any external
substrate.

---

## 15. Risks if old assumptions remain

If future Horizon work inadvertently retains Smart Ocean
X-related framing, the following risks materialise:

### 15.1 Strategic risk

Customer / regulator / partner conversations that imply
Smart Ocean X dependency could complicate AMSG's commercial
positioning, slow down sales cycles, or create false
expectations about the product's structure.

### 15.2 Commercial risk

Customer agreements drafted with Smart Ocean X framing
could introduce ambiguity about who owns the product,
who owns the revenue, who is responsible for support, and
which entity carries the contractual obligation.

### 15.3 Operational risk

Incident response (e.g. the current preview Postgres
catatonit issue on Tony's Railway support thread) goes
faster when AMSG controls every layer. Smart Ocean X
framing in incident communication could route response
through irrelevant intermediaries.

### 15.4 Architecture risk

New engineering work undertaken under a Smart Ocean X
premise could introduce unnecessary abstractions, dummy
integration points, or compatibility constraints — adding
cost and surface area for no gain.

### 15.5 Audit / compliance risk

The audit chain's integrity depends on a clear chain-of-
custody from AMSG-controlled backend → AMSG-controlled
storage → AMSG-controlled retrieval. Smart Ocean X
references in audit policy could introduce ambiguity that
weakens the regulatory defensibility of the chain.

### 15.6 Internal-team risk

If AMSG team members or future contractors / advisors
encounter Smart Ocean X framing in old documents and treat
it as current state, they may make planning decisions on
false premises. This reset addresses that risk by being
explicit.

### 15.7 Customer-facing copy risk

Customer-facing materials (web pages, sales decks,
demonstration scripts) that retain Smart Ocean X framing
mislead the customer about who they are buying from and
who they are bound to in contract.

---

## 16. Recommendations

### 16.1 Adopt this reset as canonical from `main @ e90eb56`

This document is the authoritative governance reset.
Future Horizon work — engineering, planning, commercial,
stakeholder — proceeds on its basis.

### 16.2 Cite this document in future authoritative-inputs blocks

New planning documents authored after this reset include a
single-line acknowledgement in their authoritative-inputs
header: e.g.

> "Horizon is AMSG-owned per
> `HORIZON_INDEPENDENCE_ARCHITECTURE_RESET_v0.1.md`
> (`main @ e90eb56`)."

This is sufficient. No further re-statement is required in
the body unless the document specifically discusses
ownership or substrate.

### 16.3 No bulk amendment of prior documents

Past 19 V1 planning documents are not amended for Smart
Ocean X reasons. Spot-audit (§8.4) confirms none require
it. Avoid bulk PR churn that would clutter `main` history
without adding clarity.

### 16.4 Continue the established governance pattern

The propose → review → authorise → implement → report →
review → merge pattern continues unchanged. The
substantive shift is in framing, not process.

### 16.5 Defer M1 retrospective until after Beta 10 visual check closes M1

Once Tony confirms the Beta 10 demo flow still works post-
M1 merge (M1 §18 item 10), the optional M1 retrospective
PR can incorporate this reset's framing from the outset.
The retrospective is not blocked by this reset; it just
arrives after.

### 16.6 New milestone authorisations cite this document if relevant

When M1.5 / M2 / V1.1 / V1.x / V1.6 / V2+ scope is
proposed, the proposal references this reset only where
substrate / ownership questions are material. Most
milestones (e.g. M1.5 ACK write, V1.6 Replay) don't need
to mention it; their implementation is structurally
unaffected.

### 16.7 Maintain the Kyber boundary

This reset addresses Smart Ocean X specifically. The
distinct Kyber-vs-Horizon separation (different Railway
projects, different codebases, different commercial
strategies) remains unaffected and remains in force.

### 16.8 Preserve historical record

Old documents that reference Smart Ocean X (legal
agreements, board memos, historical strategy decks) are
preserved as record. They are not retroactively rewritten.
This document supersedes their forward-looking
applicability to Horizon strategy and architecture, but
does not erase them.

### 16.9 Re-baseline customer / stakeholder materials at next refresh

When customer-facing decks, capability statements, or
RFP-response templates are next refreshed (in the normal
course of AMSG business), apply the §14 recommended
wording. No urgent re-issue is required; the next normal
refresh cycle is sufficient.

### 16.10 If asked, be direct

If a stakeholder, customer, partner, or team member asks
about Horizon's relationship to Smart Ocean X, the answer
is straightforward: **Horizon is an independent AMSG
product. It is not built on Smart Ocean X. Future work
proceeds on that basis.** Reference this document if a
written confirmation is needed.

---

## End of independence / architecture reset

**Status:** v0.1 strategic governance reset — documentation
only
**Effective:** immediately (from `main @ e90eb56`)
**Authoriser:** Tony Trajceski (Horizon product /
commercial lead)
**Implementation status:** None required. M0 and M1
implementation remain valid; codebase is unchanged.
**Next action:** ChatGPT engineering review of this reset,
then Tony's merge authorisation. Once merged, the document
governs framing of all subsequent Horizon work in this
repository.
