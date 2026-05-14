# Phase 1.2 — Audit DB Rollback Runbook

**Status:** Documented but not yet exercised against production
**Companion:** `phase1-audit-activation-runbook.md`
**When to use:** any of the failure modes in §1, at any stage of the
  activation sequence

---

## §1 — When to invoke rollback

Invoke this runbook when **any** of the following is observed:

| Symptom | Rollback level |
|---|---|
| `/api/summary` byte-shape changes after activation | **Full** (Stage E → Stage A) |
| Audit-helper WARN log rate exceeds 5/hour | **Partial** (flip emission off) |
| `audit.verify_chain` returns tamper-detected | **Investigation** (do not delete data; freeze the chain and inspect) |
| Production deploy fails post-Stage A | **Full** (revert PR) |
| Alembic migration fails on production | **Partial** (downgrade migrations; keep `psycopg` dep) |
| Daemon-thread count climbs unbounded | **Partial** (flip emission off) |
| Connection pool exhaustion on Railway Postgres | **Partial** (flip emission off; investigate before re-enabling) |
| Beta 10 regression test fails on `main` after any activation stage | **Full** |
| Any unexpected change in demo UI behaviour | **Full** |

The default rollback is **always partial first** — flip emission off
— because that is the cheapest, fastest, fully reversible action. Only
if partial rollback does not restore expected behaviour should full
rollback be invoked.

---

## §2 — Partial rollback: disable emission

**The most likely rollback. Reversible with no code change.**

**Steps (Tony, Railway dashboard):**

1. Set `AUDIT_EMISSION_ENABLED=false` on the affected environment
2. Trigger a redeploy (or rely on Railway env-var pickup)
3. Wait 2 minutes for in-flight daemon threads to drain
4. Verify:
   - `python -c "import session_audit; print(session_audit.is_enabled())"`
     → `False`
   - No new rows in `audit.events` since the flip
   - `/api/summary` shape unchanged
   - WARN log rate drops to zero

**Acceptance:** no new audit rows for at least 5 minutes after the
flip, while normal demo traffic continues.

**Re-enabling later:** flip `AUDIT_EMISSION_ENABLED=true` again. No
state to restore — the audit ledger continues from wherever it left
off, with `sequence_no` and `prev_hash` advancing from the last row.

---

## §3 — Full rollback: revert to dormant audit (Stage A baseline)

Use when partial rollback is insufficient — for example, if the
production deploy itself fails after Stage A merges, or if the
regression gate fails on `main` and partial rollback does not
restore it.

**Steps:**

1. **Disable emission** (partial rollback first, per §2)
2. **Unset `DATABASE_URL`** on the production environment
   - This causes `db.is_configured()` to return False
   - All five audit helpers' `is_enabled()` returns False
   - No daemon threads spawn, no psycopg/audit modules imported
3. **(Optional) Remove the Postgres add-on**
   - Only if rolling back fully and not retrying soon
   - Causes Railway to deprovision the database; backups retained per
     Railway policy
4. **(Optional) Revert the Stage A PR**
   - If the `psycopg` dependency itself is implicated (e.g. a binary
     build failure on Railway), open a revert PR to remove the
     `psycopg[binary]==3.3.4` line from `requirements.txt`
   - After revert, `requirements.txt` returns to its Phase 0 state
     (4 lines)
   - This restores production to byte-identical Phase 0 dependency
     state

**Acceptance:**
- `tests/test_beta10_regression.py` passes on production tree
- Live `/api/summary` byte-shape matches pre-activation
- Live `/login` and `/logout` continue to work
- No new audit rows
- Daemon-thread count returns to baseline

**Recovery from full rollback:** resume the activation sequence from
Stage A, with whatever fix addresses the failure that triggered the
rollback.

---

## §4 — Investigation-only: hash-chain tamper detection

If `audit.verify_chain(conn, "ams-demo")` reports tamper:

1. **Do not delete data.** Tamper detection is the strongest signal
   the audit ledger gives. Deletion destroys evidence.
2. **Freeze the chain.** Set `AUDIT_EMISSION_ENABLED=false` to stop
   any further writes from extending a potentially corrupted chain.
3. **Inspect:**
   - Compare the failing row's stored `row_hash` against the
     re-computed hash
   - Compare the stored `payload_hash` against
     `sha256(canonical_json(payload))`
   - Read the row immediately preceding the failure point; verify
     its `row_hash` matches the failed row's `prev_hash`
   - Determine whether the corruption is at the application layer
     (canonical-JSON serialisation drift), DB-side (row mutation),
     or transport (incomplete write)
4. **Snapshot:** take an immediate `pg_dump` of `audit.events` and
   `audit.payloads` for the affected tenant
5. **Escalate:** flag to Tony for review before any further action

**Do NOT silently truncate the chain or re-seed.** The chain's
genesis-per-tenant design (`AUDIT_GENESIS_v1:<tenant_id>`) means any
"restart" creates a hash-discontinuity that is itself a permanent
record of tamper.

---

## §5 — Migration rollback (Stage C reverse)

If Alembic migrations fail mid-sequence or produce unexpected schema
state:

**Steps:**
1. Disable emission (§2)
2. Run `alembic downgrade base` against the affected DB
3. Verify both schemas (`audit`, `config`) are dropped clean
4. Verify no `audit.events_*` partitions remain
5. Confirm `alembic_version` table reports no current revision

**Acceptance:** the DB is empty of Horizon schemas. Re-running
`alembic upgrade head` produces the same state as a fresh activation.

**If `downgrade base` fails:** drop the DB entirely (preview only)
or pg_dump + restore-from-pre-migration-snapshot (production). The
pre-migration snapshot is taken in Stage B-prep per the activation
runbook.

---

## §6 — Verifying full rollback

After full rollback, run all of the following:

1. `pytest tests/test_beta10_regression.py` on the production tree
   → must pass 46/46
2. Live `curl -i https://horizon.amsgroup.com.au/api/health-data` →
   must parse as JSON with `overall` ∈ {ready, warning, issues}
3. Live `curl -i https://horizon.amsgroup.com.au/api/summary -b "$COOKIE"` →
   200 with the 26-key top-level shape
4. Railway shell probe:
   - `python -c "import db; print(db.is_configured())"` → `False`
     (if `DATABASE_URL` unset) or `True` (if only emission disabled)
   - `python -c "import session_audit, conflict_audit,
     recommendation_audit, recommendation_presented_audit,
     operator_action_audit;
     [print(m.is_enabled()) for m in [...]]"` → all `False`
5. Audit DB query (if still provisioned):
   - `SELECT count(*) FROM audit.events WHERE ts_recorded >
     <rollback_time>` → 0
6. Daemon thread count via `threading.active_count()` in Railway
   shell → unchanged from pre-activation baseline

If any of the six fails, escalate before declaring rollback complete.

---

## §7 — Rollback sign-off

Match the activation runbook's sign-off pattern. Append to this
file, do not overwrite:

```
Rollback invoked on YYYY-MM-DD HH:MM UTC by <name>
Reason: ____________
Level: partial | full | investigation-only
Verified against main SHA: ____________
Beta 10 regression test: PASS
Live /api/summary shape: unchanged
audit.events new rows since flip: 0
Acceptance time-to-stable: __ minutes
Phase 1.2 status post-rollback: paused | resumed | aborted
```

---

## §8 — Rollback drill (mandatory before Stage E-prod)

This is the rollback drill referenced in
`phase1-audit-activation-runbook.md` Drill-preview. It exercises
§2 (partial rollback) and verifies the §6 acceptance criteria.

The drill must be run **on preview only**, BEFORE production Stage
E flips emission on. If the drill fails on preview, production
activation is blocked.

The drill is read-only against the activation sequence — it tests
the rollback mechanism, not the activation outcome.

---

## What this runbook does NOT do

- Run rollback automatically on alert thresholds (manual
  invocation only in Phase 1.2)
- Cover customer-tenant rollback (only the AMS demo tenant exists)
- Provide AWS-specific rollback (defer to Phase 2)
- Cover schema-only rollback while preserving partial event data
  (the chain semantics make this unsafe; full or partial only)
