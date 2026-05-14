# Phase 1.2 — Audit DB Activation Runbook

**Status:** Plan only — not yet executed against production
**First activation target:** **Railway preview environment**, NOT production
**Final activation target:** Railway production (AMS demo tenant)
**Authority required for each stage:** Tony's explicit "approved"
**Companion document:** `phase1-audit-rollback-runbook.md`
**Baseline protected throughout:** `phase-0-complete @ 4ad4aae`
  via `tests/test_beta10_regression.py`

---

## Hard rule

**The first real activation must happen in a Railway preview/staging
environment, not production.** Production activation only follows
after preview passes:

1. Migration completes cleanly
2. Smoke test for all 5 event types passes
3. Rollback drill completes cleanly
4. Hash-chain verification passes

If any of those four fails on preview, production activation is
blocked until the failure is understood and remedied.

---

## Stage matrix

| Stage | Where | What | Reversible by |
|---|---|---|---|
| A | main → prod | Add `psycopg[binary]` to `requirements.txt` (this PR — runtime dep only, no activation) | Revert PR |
| B-preview | preview | Provision Postgres add-on on preview; set `DATABASE_URL`; set `AUDIT_EMISSION_ENABLED=false` | Unset env vars |
| C-preview | preview | `alembic upgrade head` on preview DB | `alembic downgrade base` |
| D-preview | preview | Smoke-test all 5 event types; verify chain | n/a — read-only verification |
| Drill-preview | preview | Rollback drill on preview | n/a |
| B-prod | prod | Provision Postgres add-on on prod; set `DATABASE_URL`; `AUDIT_EMISSION_ENABLED=false` | Unset env vars |
| C-prod | prod | `alembic upgrade head` on prod DB | `alembic downgrade base` |
| D-prod | prod | Smoke-test on prod | n/a |
| E-prod | prod | Flip `AUDIT_EMISSION_ENABLED=true`; monitor | Flip back to `false` |

Each stage has a single gate. Tony must explicitly approve before the
next stage begins.

---

## Stage A — Add `psycopg` to runtime dependencies

**Owner:** Claude (PR), Tony (approve + merge + redeploy)
**Scope:** This PR. `requirements.txt` gains `psycopg[binary]==3.3.4`.

**Validation (already proven on this PR's branch):**
- `tests/test_beta10_regression.py` passes
- Full pytest passes
- Audit helpers still no-op under `DATABASE_URL` unset (the helpers
  import psycopg lazily inside `_emit_worker`; with `is_enabled()`
  returning False, that worker is never invoked)
- The production posture probe still confirms `psycopg not in
  sys.modules` after importing audit helpers — the binary is on
  disk but not loaded into the running process

**Post-merge validation Tony runs:**
- Confirm the Railway redeploy succeeds
- `pip list | grep psycopg` on the deployed image returns the
  binary
- Live `/api/summary` shape unchanged
- Live `/login` and `/logout` continue to work
- No new WARN log lines from audit helpers (because helpers are
  still gated off by `DATABASE_URL`)

**Gate to Stage B-preview:** Tony confirms post-merge probe passes.

---

## Stage B-preview — Provision Postgres add-on (preview env)

**Owner:** Tony (Railway dashboard)
**Where:** Railway preview environment for a dedicated branch
  `preview-audit-activation` (Claude can prepare that branch as a
  separate PR; this runbook does not depend on it existing yet).

**Steps:**
1. In the Railway preview environment for the activation branch:
   - Add the Postgres plugin
   - Confirm `DATABASE_URL` env var is auto-injected
2. Explicitly set `AUDIT_EMISSION_ENABLED=false` (this is the
   safety: even with `DATABASE_URL` configured, the flag keeps the
   helpers no-op)
3. Trigger preview redeploy

**Validation:**
- Preview deploy succeeds
- Preview `/api/summary` shape unchanged
- Direct shell probe: `python -c "import db; print(db.is_configured())"`
  → `True` (the URL is set)
- Direct shell probe: `python -c "import session_audit;
  print(session_audit.is_enabled())"` → `False` (the flag is off)
- No new `audit.events` rows would be created (because there is no
  schema yet, AND emission is disabled)

**Gate to Stage C-preview:** all four validations pass.

---

## Stage C-preview — Run Alembic migrations on preview DB

**Owner:** Tony (Railway shell or one-shot job)

**Steps:**
1. Open a Railway shell against the preview service
2. Run: `alembic upgrade head`
3. Verify all 4 migrations ran:
   - `0001_initial`
   - `0002_tenants`
   - `0003_seed_ams_demo`
   - `0004_audit_schema`
4. Run the activation-validation probe (Claude to ship as
   `scripts/activate_audit.py` in a separate PR after this runbook
   merges). The probe asserts:
   - `audit.events` and `audit.payloads` partitioned parent tables
     exist
   - 12 forward monthly partitions exist on each
   - CHECK constraints carry the closed sets from `audit.py`
   - `audit.tenants` has the `ams-demo` row
   - Row counts on `audit.events` and `audit.payloads` are 0

**Validation:** the probe returns "ALL CHECKS PASS" and exits 0.

**Gate to Stage D-preview:** probe passes.

---

## Stage D-preview — Smoke test (preview)

**Owner:** Tony (run the smoke script)

**Steps:**
1. Run `scripts/audit_smoke_test.py` (separate PR after this runbook):
   - Temporarily sets `AUDIT_EMISSION_ENABLED=true` for the script
     process only (does not touch the deployed env var)
   - Calls `emit_sync` once per event type for each of the 5 helpers
   - For each, queries `audit.events` and confirms exactly 1 new row
     with the expected shape
   - Runs `audit.verify_chain(conn, "ams-demo")` after each emit
   - Prints PASS/FAIL per event type and overall

**Acceptance:**
- All 5 event types produce exactly 1 row each (5 rows total)
- `verify_chain` returns clean after each insert
- Hash chain advances by 5
- Sequence numbers are 1..5

**Rollback after smoke test (do this even on success):**
```sql
DELETE FROM audit.events
WHERE tenant_id = 'ams-demo'
  AND payload->>'origin' = 'activation_smoke';
```
(The smoke script tags every emit with `origin: "activation_smoke"`
in the payload so cleanup is unambiguous.)

**Gate to Drill-preview:** smoke test passes, cleanup completes.

---

## Drill-preview — Rollback drill (preview)

**Owner:** Tony

**Steps:**
1. Set `AUDIT_EMISSION_ENABLED=true` on preview
2. Drive an authenticated `/api/summary` poll and a `/login`+`/logout`
   cycle against preview
3. Query `audit.events` — confirm rows accumulated
4. Set `AUDIT_EMISSION_ENABLED=false`
5. Drive the same operations again
6. Query `audit.events` — confirm **no new** rows since the flip
7. Optionally unset `DATABASE_URL`
8. Confirm helpers report `is_enabled() == False` and no emission

**Acceptance:** flag flip stops emission deterministically, with no
code change and no redeploy required.

**Gate to Stage B-prod:** drill passes.

---

## Stages B-prod, C-prod, D-prod — Repeat on production

Identical procedure to B-preview, C-preview, D-preview, but against
the production Railway service.

**Tony's discretionary gate at each step:** abort if preview
behaviour and production behaviour diverge in any unexpected way.

---

## Stage E-prod — Flip `AUDIT_EMISSION_ENABLED=true`

**Owner:** Tony (single env var change on production)

**Steps:**
1. Confirm Stages A through D-prod all green
2. Flip `AUDIT_EMISSION_ENABLED=true` on production
3. Trigger production redeploy (or rely on Railway env-var pickup)

**Immediate acceptance (within 5 minutes):**
- `/api/summary` byte-shape unchanged (the regression gate axioms hold)
- Production login produces a `SESSION_STARTED` row in
  `audit.events`
- A production `/api/summary` poll produces `CONFLICT_DETECTED`,
  `RECOMMENDATION_GENERATED`, and `RECOMMENDATION_PRESENTED` rows
  for the B03 + B04 demo conflicts
- One operator action (apply a what-if scenario) produces an
  `OPERATOR_ACTED` row
- Production logout produces a `SESSION_ENDED` row
- `audit.verify_chain(conn, "ams-demo")` returns clean
- WARN log rate from audit helpers is zero

**Acceptance window:** 5 minutes of monitoring with no audit-helper
WARN logs.

**Rollback (any time after Stage E):** see
`phase1-audit-rollback-runbook.md`. The rollback is always a single
env-var flip.

---

## Sign-off

Same pattern as `phase0-exit-runbook.md`. After each stage passes,
record the date, the Railway environment, and the current `main`
SHA:

```
Stage A merged on YYYY-MM-DD by <name>
  PR: #__
  Verified against main SHA: ____________
  Beta 10 regression test: PASS

Stage B-preview completed on YYYY-MM-DD
  Preview env: ____________
  DATABASE_URL set: yes
  AUDIT_EMISSION_ENABLED: false
  Probe result: db.is_configured()=True, session_audit.is_enabled()=False

Stage C-preview completed on YYYY-MM-DD
  Migrations: 0001..0004 applied clean
  Schema probe: ALL CHECKS PASS

Stage D-preview completed on YYYY-MM-DD
  5/5 event types emit and verify
  Hash chain advanced from 0 to 5
  Cleanup completed

Drill-preview completed on YYYY-MM-DD
  Flip-off stops emission: yes
  Flip-on resumes emission: yes

Stages B/C/D-prod completed on YYYY-MM-DD
  Same checks as preview, on production

Stage E-prod completed on YYYY-MM-DD
  AUDIT_EMISSION_ENABLED=true on prod
  5-minute monitoring window: clean
  WARN log rate: 0
```

After Stage E sign-off, tag the post-activation production commit
locally:

```sh
git tag -a phase-1-audit-live <SHA> -m "Phase 1 audit activation complete"
git push origin phase-1-audit-live
```

---

## What this runbook does NOT do

- Activate emission in any environment
- Touch production env vars
- Run Alembic against production
- Add a Postgres add-on
- Migrate to AWS
- Onboard customer tenants
- Enable per-tenant retention policy automation

All of the above are explicitly out of scope for Phase 1.2.
