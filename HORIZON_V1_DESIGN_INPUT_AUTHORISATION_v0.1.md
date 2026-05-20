# Horizon V1 — UX/UI Handoff Design-Input Authorisation (v0.1)

**Status:** Authorisation record — formal documentation
**Document version:** 0.1
**Date:** 2026-05-20
**Authoriser:** Tony Trajceski (Horizon product/commercial lead)
**Authoritative inputs (merged on `main @ 1d02a16`):**
- `HORIZON_V1_UX_UI_HANDOFF_VALIDATION_v0.1.md` (PR #37)
- `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` (PR #38)
- `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md` (PR #39)
- `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` (PR #40)
- Prior seven V1 foundation documents on `main`
**Subject of authorisation:**
- Claude Design UX/UI handoff package at `v1-handoff/`
  - `V1-Implementation-Prompt.md`
  - `README.md`
  - `prototype/` (JSX components, `Horizon V1.html` CSS, `data.js`, screenshots)
**Beta 10 baseline:** `phase-0-complete @ 4ad4aae`. Unchanged.

**This document is the formal authorisation record.** It records
that Tony has authorised `v1-handoff/` as a **design input only**.
It does not authorise implementation. It does not authorise M0.
It does not authorise M1. It does not authorise any change to
`server.py`, the audit module, the Railway configuration, or any
Beta 10 file.

---

## 1. Executive Summary

The Claude Design UX/UI handoff package at `v1-handoff/` is
**formally authorised as a design input only**. The visual design
language, design tokens, VTSO desktop console layout, 3-column
operational shell, DSW conceptual flow, component visual
hierarchy, and screenshots may be referenced and consumed by
future V1.0 implementation work as authoritative design guidance.

**This authorisation does NOT confer implementation authority.**
Specifically, this document does **not**:

- authorise M0 implementation
- authorise M1 implementation
- authorise the creation of a `frontend/` directory
- authorise any change to `server.py`
- authorise Stage E-prod activation
- authorise production deployment
- authorise the treatment of any handoff text describing
  back-end behaviour, RBAC, audit, or actions as binding

**M0 still requires separate authorisation** per
`HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` §23 (six preconditions),
of which this design-input authorisation closes one. The
remaining five preconditions stand:

1. ChatGPT engineering review of the Handoff Validation
   (already on main per PR #37)
2. **This design-input authorisation** ← closed by this document
3. `/api/summary` shape verification spike (already on main per
   PR #39)
4. `horizon-v1-sandbox` Railway project provisioning — **open**
5. ChatGPT engineering review of the M0 Scope Proposal (already
   on main per PR #38; ChatGPT review pending)
6. Tony's explicit M0 authorisation citing the §12 scope
   constraints — **open**

Once the `horizon-v1-sandbox` Railway project exists and Tony
issues an explicit M0 authorisation, M0 may begin.

---

## 2. Authorised Design Inputs

The following elements of the `v1-handoff/` package are
authorised as **design input** for V1.0 implementation work:

| Authorised input | Source in handoff | Use in V1.0 |
|---|---|---|
| **Horizon Dark visual design language** | `V1-Implementation-Prompt.md` §3, `prototype/Horizon V1.html` | Authoritative visual identity for the V1 frontend |
| **Design tokens** (colour palette, typography, spacing, radii, shadows) | §3.1–§3.3 of the prompt; `<style>` block in `Horizon V1.html` | Extract into `frontend/src/styles/tokens.css` during M0 |
| **Component visual hierarchy** (Card, Pill, CategoryPill, Dot, Icon, PageHead, SectionHd) | §4.8 of the prompt; `prototype/components.jsx` | Authoritative for the shared component set |
| **VTSO desktop console layout direction** | §4 of the prompt; `prototype/panels.jsx`, `tabs.jsx` | Authoritative for the V1.0 VTSO console only |
| **3-column operational shell** (`360px \| 1fr \| 380px`) | §4.1 | Authoritative for VTSO desktop |
| **DSW conceptual flow** (Signal → Cascade → Options → Commit → Ledger) | §4.7; `prototype/dsw.jsx` | Authoritative for the visual flow and step structure; specific commit/ledger semantics deferred per §3.10 below |
| **Card / Pill / Conditions / Header patterns** | §4.2–§4.5 | Authoritative |
| **Screenshots** in `prototype/screenshots/` (12 files) | `prototype/screenshots/` | Authoritative as visual references for side-by-side comparison during M0 and M1 |

Implementation PRs may cite this section to justify visual /
interaction design choices. Citations must be specific (e.g.
"per Design Input Authorisation §2, line referencing `Card`
component visual hierarchy in `components.jsx`") rather than
blanket references to the handoff.

---

## 3. Non-Authoritative Elements

The following elements of `v1-handoff/` are **explicitly NOT
binding**. They may not be cited as authority for implementation
decisions:

| Non-authoritative element | Why |
|---|---|
| **Backend / API assumptions** | The handoff's documented `/api/summary` shape (V1-Implementation-Prompt.md §2) does not match the real Beta 10 response (see `HORIZON_V1_API_SUMMARY_SHAPE_SPIKE_v0.1.md`). The adapter contract in `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md` is the authoritative data contract; the handoff is not. |
| **RBAC / security model** | The prototype role switcher (`app.jsx:12`) is purely client-side. The `HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` is authoritative; role enforcement is server-side and lives in V1.1+. |
| **Production deployment instructions** | Statements like "Live production: Railway, auto-deploy from main" (V1-Implementation-Prompt.md §1) describe Beta 10's existing deploy, not V1's. V1.0 deploys to `horizon-v1-sandbox` per Execution Plan §8 and M0 Scope Proposal §16. |
| **Audit / compliance claims** | DSW step 5's "Audit trail preserved. Resolution applied to live schedule" is **not** a backed capability today. Stage E-prod is paused; no apply-decision endpoint exists. Treat as visual flow only. |
| **Role permissions** | The handoff's 6 role labels are correct (`HORIZON_V1_USER_PERMISSION_MODEL_v0.1.md` §5), but the prototype does not enforce, project, or grant any permission. Permission Model is authoritative. |
| **Workflow semantics** | The handoff implies handover, shift open/close, escalation, deferral, override. `HORIZON_V1_OPERATIONAL_WORKFLOW_MODEL_v0.1.md` is authoritative for these semantics. The handoff is not the source. |
| **Data shape** | See §3 row 1 — the `/api/summary` shape in the handoff is documentation, not contract. The Shape Spike + Adapter Design Note are the canonical source for what the frontend consumes. |
| **Write actions** | The handoff lists `POST /api/whatif`, `apply-whatif`, `clear-whatif` (V1-Implementation-Prompt.md §2). These exist in Beta 10 and are Phase 1.1-hardened, but **the DSW "Commit decision" affordance must not be wired to apply-whatif** (different semantics). A real decision-commit endpoint is V1.x. |
| **"Decision logged" claims** | DSW step 5's success state is a visual mock. Production audit emission stays no-op until Stage E-prod is separately authorised. |
| **"Replacing index.html"** | The handoff says "you are replacing `index.html` with a React frontend" (V1-Implementation-Prompt.md §1). The Execution Plan §5.2 says coexist at `/v1/*`. The Execution Plan posture wins. `index.html` is not replaced. |
| **"Never modify server.py" as an absolute** | The handoff's §6 rule 1 is absolute. The Execution Plan §5.3 permits one narrow exception (static-file serving for `/v1/*` in M2+). The narrower interpretation wins; broader interpretations are non-authoritative. |
| **Phase 1–4 timeline** | The handoff's 4-week cadence (§5) is treated as a design checklist, not a milestone plan. M0–M9 per Execution Plan §11 is the authoritative implementation cadence. |
| **Mobile / stakeholder / executive surfaces** | Not in the handoff. Permission Model §5.4–§5.6 and Screen Architecture §6/§8/§9 are authoritative for those consoles. |
| **Replay design** | Not in the handoff. V1.6 capability. Out of scope. |
| **AI provenance display** | Confidence numbers in the prototype have no source/scope/lineage. Information Architecture §16 is authoritative. AI provenance must be added before any DSW recommendation is presented as authoritative. |
| **Cascade tree (PRIMARY / +1 / +2)** | Not in the real backend response. Adapter Design Note §8.8 governs treatment (`cascade: null`, never synthesised). Handoff illustration is conceptual only. |

If a future PR cites a handoff element from this list as
authority, the PR should be rejected and re-scoped.

---

## 4. Mandatory Constraints

The following constraints are mandatory and **non-negotiable**
for any V1 implementation work flowing from this authorisation:

### 4.1 Beta 10 remains protected

- `phase-0-complete @ 4ad4aae` is the immutable Beta 10
  baseline
- `tests/test_beta10_regression.py` 46/46 must keep passing on
  every commit
- Beta 10 production (`horizon-prod`) untouched
- No edit to `server.py`, `index.html`, `audit.py`, the five
  audit helper modules, `db.py`, `tenant.py`, Phase 0
  migrations, `requirements.txt`, `railway.toml`, `Procfile`,
  `deploy/`, or `port_profiles.py`

### 4.2 `/` remains Beta 10

- The legacy `index.html` continues to serve at `/` throughout
  V1.0
- React mounts at `/v1/*` (M2+; deferred from M0 per M0 Scope
  Proposal §23.6)
- Beta 10 customer demos must continue working identically
  before, during, and after V1 build

### 4.3 V1 work must happen separately

- V1 build runs on a long-lived branch off `main`, not on
  `main` itself
- V1 deploys to `horizon-v1-sandbox` Railway project, not to
  `horizon-prod`
- V1 work merges to `main` only after milestone-by-milestone
  authorisation (per Implementation Strategy §16 governance)

### 4.4 No `server.py` changes for M0

- M0 makes zero edits to `server.py`
- The `/v1/*` static-file route addition (Execution Plan §5.3)
  is M2+ and requires its own separate authorisation
- Until then, V1 is served from `horizon-v1-sandbox`, not from
  `horizon-prod`

### 4.5 No backend writes in M0

- No `POST /api/whatif`, `apply-whatif`, `clear-whatif`,
  `set_port`, `login`, `logout` from the V1 frontend in M0
- No new endpoints
- No new audit emissions
- Read path is also forbidden in M0 (zero `/api/summary` calls);
  read path is M1, not M0

### 4.6 No audit writes in M0

- The audit module is not imported into M0 frontend code
- No client-side telemetry that mimics audit semantics
- No "Decision logged" UI text in M0
- No "Audit trail preserved" UI text in M0
- Right-panel Audit Log tab shows "(M0 placeholder)" only

### 4.7 No Stage E-prod activation

- `AUDIT_EMISSION_ENABLED` stays at its current state
- Production `DATABASE_URL` remains unset
- Stage E-prod remains paused per Implementation Strategy §9.4
- This authorisation does not advance Stage E-prod by any
  amount

### 4.8 No production deploy

- V1 artefacts do not deploy to `horizon-prod`
- V1 deploys only to `horizon-v1-sandbox` (once provisioned)
- The sandbox URL is not linked from `horizon-prod`
- The sandbox URL carries no real customer data

### 4.9 No prototype role switcher as real RBAC

- The prototype's tweaks-panel role switcher is **not**
  reproduced in V1.0 as a permission-changing affordance
- If a role switcher appears at all in V1.0, it is labelled
  "Preview role (display only — does not change your
  permissions)" or omitted entirely
- Real RBAC is V1.1+ per Implementation Strategy §17

### 4.10 No DSW "decision logged" claim unless backed by real state

- DSW step 5 text in V1.0 must reflect the actual capability
  level
- Acceptable variants: "Decision recorded locally" (with
  session-only scope clearly labelled), or step 5 is hidden
  entirely until M7
- Unacceptable: "Audit trail preserved" or "Resolution applied
  to live schedule" appearing in the UI before the
  corresponding backend endpoints exist and Stage E-prod is
  authorised (V1.x)

---

## 5. Relationship to M0

### 5.1 M0 may use handoff visuals

- Horizon Dark design tokens lifted from `prototype/Horizon V1.html`
- Shared components built per `prototype/components.jsx` visual
  hierarchy
- Layout grid matching `prototype/app.jsx` and `panels.jsx`
- Side-by-side visual comparison against
  `prototype/screenshots/` for acceptance

### 5.2 M0 must use static / mock data

- `frontend/src/data/sample.js` is the only data source in M0
- `sample.js` is shaped as `ViewSummary` (per Adapter Design
  Note §2.2 and §17.2), **not** as the prototype's `data.js`
  shape — so M0 components are positioned for M1 adapter
  output without rework
- The sample's content may be lifted from `prototype/data.js`
  but must be reshaped to match `ViewSummary`

### 5.3 M0 must not call `/api/summary`

- Zero `fetch()` calls to Beta 10 backend in M0
- No `frontend/src/api/` directory in M0
- No polling hook in M0
- No auth flow in M0
- Live API integration is M1

### 5.4 M0 must not implement real actions

- No POST endpoints called
- No "Apply", "Commit", "Cancel" buttons that actually act
- No state mutation beyond local React state used for rendering
  the static sample

### 5.5 M0 must not implement RBAC

- No role state in app-wide context
- No permission checks in components
- No "currentUser" object that anything depends on
- Static "VTSO" label in header only; no role switcher
  affordance

### 5.6 M0 must not imply audit / replay is complete

- DSW not included in M0 (M3+ per Execution Plan §11)
- Audit Log right-panel tab shows "(M0 placeholder)"
- No Shift Log tab in M0 (M1+ with explicit decision)
- No replay surface
- No "decision logged" / "audit trail" text anywhere

### 5.7 M0 must cite `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md`

- The M0 PR description must reproduce the scope-proposal §4
  (Success Criteria), §6 (Protected Surfaces), §8 (Forbidden
  Runtime Changes), and §20 (Exit Criteria) verbatim
- Any reviewer can reject in-PR scope expansion by quoting the
  cited section

---

## 6. Relationship to M1

### 6.1 M1 may introduce read-only API integration

- `frontend/src/api/horizon.js` (single public entry point)
- `frontend/src/api/adapters/` directory (internal
  normalisation modules)
- Polling hook (`useSummary()`)
- Fetch wrapper with auth-cookie handling
- One date library (DST-aware — `date-fns-tz` or `luxon`)

### 6.2 M1 must use the adapter contract from `HORIZON_V1_API_ADAPTER_DESIGN_NOTE_v0.1.md`

- The adapter implementation must honour §5–§14 of the design
  note (mapping contract, per-domain adapters, time/unit
  normalisation, severity/status normalisation)
- The adapter must respect §12 (Missing Domain Strategy) —
  explicit `null` plus `missingDomains` list for surfaces
  with no backend source; **no fabrication**
- The adapter is pure functions; no classes, no state, no RBAC,
  no audit emission, no action handling
- Fixture-based adapter tests per §16 must ship in the same M1
  PR or an immediately following M1 PR

### 6.3 M1 must not introduce write actions

- No POST / PUT / DELETE to the Beta 10 backend
- No port switching (`set_port` deferred to M4+)
- No what-if scenario builder
- No decision commit
- Read path only

### 6.4 M1 must not introduce backend changes without separate authorisation

- `server.py` byte-identical at M1 close
- No new endpoint, no field rename, no schema change
- If M1 discovers a backend gap that requires a backend change,
  the gap is documented and a separate Tony authorisation is
  requested — M1 does not extend its own scope

---

## 7. Risks Accepted

By authorising the handoff as design input, Tony explicitly
accepts the following risks documented in the Handoff Validation
report:

### 7.1 Design is VTSO-heavy

The handoff specifies a single role's console in depth and
leaves the other five roles (Harbour Master, Shift Supervisor,
Port Executive, Stakeholder, Marine Infrastructure) **without
visual design**. V1.0 ships VTSO only. Other consoles need
fresh design work in V1.1+ or via a separate handoff package.

**Accepted because:** V1.0 is intentionally scoped to the VTSO
desktop console (M0 Scope Proposal §2). Single-role first is
the deliberate strategy.

### 7.2 Executive / stakeholder / replay surfaces remain under-designed

Port Executive analytics, Stakeholder mobile, Marine
Infrastructure tablet, and the Replay Workspace have **no
handoff coverage**. They are scheduled for V1.x but no visual
design exists yet.

**Accepted because:** these are V1.x deliverables. Design for
them will be commissioned separately (likely from the same
team that produced the VTSO handoff) once the VTSO console is
implemented and stable.

### 7.3 Prototype data does not match backend exactly

The Shape Spike documented ~20 field-name differences,
several missing/added top-level keys, and at least one unit
mismatch (visibility nm vs km) between the prototype's
`data.js` and the real `/api/summary`.

**Accepted because:** the adapter design note (PR #40, now on
main) defines a normalisation layer that absorbs all gaps. No
backend change is required.

### 7.4 DSW flow is conceptual only

The handoff's 5-step DSW is a polished visual flow. The
**backend currently has no apply-decision endpoint** and the
audit ledger is paused in production. The DSW renders but
cannot truly commit a decision in V1.0.

**Accepted because:** DSW step 5 will be reframed honestly
("Decision recorded locally — production audit pending V1.x")
until M7 wires the audit-linked action endpoint, per Handoff
Validation §9.4.

### 7.5 Role switcher is visual only

The prototype role switcher does not enforce RBAC. V1.0
adopts a static role label or a clearly-labelled "preview
only" affordance per §4.9.

**Accepted because:** real RBAC is V1.1+ per Implementation
Strategy §17 and Permission Model §11.

---

## 8. Conditions Before M0

M0 may not begin until **all** of the following are true:

| Condition | Status |
|---|---|
| 1. This authorisation document merged | **Pending — this PR** |
| 2. `horizon-v1-sandbox` Railway project provisioned | **Pending — Tony-side ops** |
| 3. Explicit M0 authorisation from Tony | **Pending — separate authorisation message** |
| 4. M0 scope cites `HORIZON_V1_M0_SCOPE_PROPOSAL_v0.1.md` §12 verbatim in the M0 PR description | (closes during M0 PR creation) |
| 5. Implementation branch created separately from `main` | (closes during M0 PR creation) |
| 6. No production deployment intended in M0 scope | (closes during M0 PR creation) |

Conditions 1, 2, 3 must close in this order before M0 begins.
Conditions 4, 5, 6 close at M0 PR creation time.

### 8.1 Sandbox provisioning prerequisites

The `horizon-v1-sandbox` provisioning is Tony-side. It
requires:

- A Railway project named `horizon-v1-sandbox` (or equivalent)
  distinct from `horizon-prod`
- Auto-deploy from `main` to `horizon-v1-sandbox` **DISABLED**
  in M0 (auto-deploy from the V1 branch may be enabled later)
- The sandbox project carries no `DATABASE_URL` for the audit
  schema in M0 (Stage E-prod remains paused)
- The sandbox URL is not linked from `horizon-prod`
- The sandbox URL is shared only with Tony / Claude / ChatGPT
  during review

If sandbox provisioning is blocked (Railway access, billing,
account limits), M0 may close on the "local-build proof only"
variant per M0 Scope Proposal §22.1 — `frontend/dist/` builds
locally on a developer machine; sandbox deployment is deferred
to a small follow-up PR once provisioning closes.

---

## 9. Recommendation

### 9.1 Handoff accepted as design input

**Yes — formally accepted as documented in §2.** The visual
design language, design tokens, VTSO desktop layout, 3-column
shell, DSW conceptual flow, and component visual hierarchy may
be cited as authority in V1.0 implementation PRs.

### 9.2 Handoff NOT accepted as implementation authority

**Confirmed — explicitly NOT accepted for the items listed in
§3.** Implementation PRs must not cite the handoff for backend
behaviour, RBAC, audit, deployment, or workflow semantics.

### 9.3 M0 still not authorised

**M0 implementation is NOT authorised by this document.** The
remaining preconditions are:

- `horizon-v1-sandbox` provisioned (Tony-side ops)
- Tony's explicit M0 authorisation message citing M0 Scope
  Proposal §12

### 9.4 Next step

The recommended next step is **sandbox provisioning**, after
which Tony can issue an explicit M0 authorisation. Until
sandbox provisioning closes, M0 is paused. The planning
corpus is complete and stable; no further documents are
required before M0 can begin.

### 9.5 Authorisation scope summary

| Item | Authorised here? |
|---|---|
| Handoff visual identity as design input | **YES** |
| Horizon Dark design tokens for V1 use | **YES** |
| VTSO 3-column shell for V1 use | **YES** |
| DSW conceptual flow for V1 visual design | **YES** |
| Component visual hierarchy for V1 use | **YES** |
| Screenshots as visual reference | **YES** |
| Handoff backend / API assumptions | **NO** |
| Handoff RBAC / role implementation | **NO** |
| Handoff deployment instructions | **NO** |
| Handoff "decision logged" claims | **NO** |
| Handoff "replace index.html" framing | **NO** |
| M0 implementation | **NO — separate authorisation required** |
| M1 implementation | **NO — separate authorisation required** |
| Stage E-prod | **NO — paused** |
| Production deployment | **NO** |

---

## End of authorisation record

**Status:** v0.1 design-input authorisation
**Implementation status:** None
**Next action:** ChatGPT engineering review, then Tony's
decision on merging this authorisation. Once merged, the V1.0
preconditions stand at: sandbox provisioning (Tony-side ops)
and an explicit M0 authorisation message.
