#!/usr/bin/env python3
"""
Project Horizon — audit DB activation validation probe.

Phase 1.2 Stage C-preview helper. Read-only verification that an audit
database is correctly migrated and ready for emission. Designed to be
run manually after `alembic upgrade head` against a Railway preview
DATABASE_URL, then again against production DATABASE_URL once preview
passes.

What this script does
---------------------
Reads from the target Postgres referenced by DATABASE_URL and verifies:

  • Connectivity (open a connection, run SELECT 1)
  • Current Alembic head matches the latest migration
    (0004_audit_schema)
  • config schema exists
  • config.tenant contains the ams-demo row
  • audit.events parent table exists (RANGE-partitioned)
  • audit.payloads parent table exists (RANGE-partitioned)
  • ≥ 12 monthly partitions exist on each of audit.events and
    audit.payloads (parent inherits-via-pg_inherits children count)
  • Named CHECK constraints from migration 0004 exist on both tables
  • Named indexes from migration 0004 exist on both tables
  • Initial row counts on audit.events and audit.payloads are
    reported (do not assert zero — chain may have entries from
    smoke tests or rollback drills)

What this script does NOT do
----------------------------
  • No INSERT, UPDATE, DELETE, TRUNCATE, ALTER, DROP, CREATE
  • No transaction commits (transaction is rolled back at exit)
  • No environment mutation
  • No alembic invocations
  • No emit_async / emit_sync calls
  • No psycopg connection pool (single-connection probe)
  • No external network beyond the configured DATABASE_URL host

Exit codes
----------
  0 — all checks pass; the DB is migrated and shaped as expected
  1 — one or more checks failed; output identifies which
  2 — DATABASE_URL not set or invalid; nothing was probed

Safe to run repeatedly. Idempotent. No state changes.

Usage
-----
  $ export DATABASE_URL=postgresql://user:pass@host:port/dbname
  $ python scripts/activate_audit.py

Or against the preview environment from the Railway shell:
  $ python scripts/activate_audit.py

Environment variables consulted
-------------------------------
  DATABASE_URL                  required, no default
  AUDIT_VALIDATE_TENANT_ID      optional, defaults to 'ams-demo'
  AUDIT_VALIDATE_PARTITIONS_MIN optional, defaults to 12
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Callable


# ── Configuration ─────────────────────────────────────────────────────

DEFAULT_TENANT_ID = "ams-demo"
DEFAULT_PARTITIONS_MIN = 12
EXPECTED_ALEMBIC_HEAD = "0004_audit_schema"

# Names of objects established by migrations/versions/0004_audit_schema.py.
# Updating these requires either a new migration or a deliberate edit
# here, not both — the script's job is to validate the migration, not
# negotiate with it.
AUDIT_EVENTS_INDEXES = (
    "events_ts_event_idx",
    "events_event_type_idx",
    "events_subject_idx",
)
AUDIT_PAYLOADS_INDEXES = (
    "payloads_source_idx",
    "payloads_hash_idx",
    "payloads_kind_idx",
)

AUDIT_EVENTS_CHECK_CONSTRAINTS = (
    "events_event_type_valid",
    "events_actor_type_valid",
    "events_subject_type_valid",
    "events_payload_hash_len",
    "events_prev_hash_len",
    "events_row_hash_len",
    "events_sequence_nonneg",
    "events_schema_version_valid",
)
AUDIT_PAYLOADS_CHECK_CONSTRAINTS = (
    "payloads_source_valid",
    "payloads_retention_valid",
    "payloads_hash_len",
    "payloads_size_nonneg",
)


# ── Check result type ─────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""

    def render(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        line = f"  [{mark}] {self.name}"
        if self.detail:
            line += f"  — {self.detail}"
        return line


# ── Individual checks ─────────────────────────────────────────────────


def check_connectivity(conn) -> CheckResult:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
        ok = row is not None and row[0] == 1
        return CheckResult(
            "DB connectivity",
            ok,
            "SELECT 1 returned expected row" if ok else f"unexpected: {row}",
        )
    except Exception as exc:  # pragma: no cover — exercised against live DB
        return CheckResult(
            "DB connectivity",
            False,
            f"connection or query failed: {exc!r}",
        )


def check_alembic_head(conn) -> CheckResult:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.alembic_version') IS NOT NULL"
            )
            exists = cur.fetchone()[0]
            if not exists:
                return CheckResult(
                    "Alembic head present",
                    False,
                    "alembic_version table does not exist — "
                    "have migrations been run?",
                )
            cur.execute("SELECT version_num FROM alembic_version")
            rows = cur.fetchall()
        if len(rows) != 1:
            return CheckResult(
                "Alembic head present",
                False,
                f"expected exactly 1 row in alembic_version, got {len(rows)}",
            )
        version = rows[0][0]
        ok = version == EXPECTED_ALEMBIC_HEAD
        return CheckResult(
            "Alembic head present",
            ok,
            f"version={version}"
            + ("" if ok else f"  (expected {EXPECTED_ALEMBIC_HEAD})"),
        )
    except Exception as exc:
        return CheckResult("Alembic head present", False, f"query failed: {exc!r}")


def check_config_schema_exists(conn) -> CheckResult:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_namespace WHERE nspname = 'config'"
            )
            row = cur.fetchone()
        ok = row is not None
        return CheckResult(
            "config schema exists",
            ok,
            "" if ok else "pg_namespace has no 'config' entry",
        )
    except Exception as exc:
        return CheckResult("config schema exists", False, f"query failed: {exc!r}")


def check_tenant_seeded(conn, tenant_id: str) -> CheckResult:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, display_name, tenant_type "
                "FROM config.tenant WHERE id = %s",
                (tenant_id,),
            )
            row = cur.fetchone()
        if row is None:
            return CheckResult(
                f"config.tenant has '{tenant_id}'",
                False,
                "tenant row not found — has 0003_seed_ams_demo_tenant run?",
            )
        return CheckResult(
            f"config.tenant has '{tenant_id}'",
            True,
            f"display_name={row[1]!r}  tenant_type={row[2]!r}",
        )
    except Exception as exc:
        return CheckResult(
            f"config.tenant has '{tenant_id}'",
            False,
            f"query failed: {exc!r}",
        )


def check_audit_parent_table(conn, table: str) -> CheckResult:
    """Verify the parent table exists AND is RANGE-partitioned."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relkind, pt.partstrat
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
                WHERE n.nspname = 'audit' AND c.relname = %s
                """,
                (table,),
            )
            row = cur.fetchone()
        if row is None:
            return CheckResult(
                f"audit.{table} parent table",
                False,
                "table does not exist in audit schema",
            )
        relkind, partstrat = row
        # relkind 'p' = partitioned table; partstrat 'r' = RANGE
        if relkind != "p":
            return CheckResult(
                f"audit.{table} parent table",
                False,
                f"exists but is not partitioned (relkind={relkind!r})",
            )
        if partstrat != "r":
            return CheckResult(
                f"audit.{table} parent table",
                False,
                f"partitioned but not by RANGE (partstrat={partstrat!r})",
            )
        return CheckResult(
            f"audit.{table} parent table",
            True,
            "exists, RANGE-partitioned",
        )
    except Exception as exc:
        return CheckResult(
            f"audit.{table} parent table",
            False,
            f"query failed: {exc!r}",
        )


def check_partitions_count(conn, table: str, minimum: int) -> CheckResult:
    """Count child partitions inheriting from audit.<table>."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM pg_inherits i
                JOIN pg_class p ON p.oid = i.inhparent
                JOIN pg_namespace pn ON pn.oid = p.relnamespace
                WHERE pn.nspname = 'audit' AND p.relname = %s
                """,
                (table,),
            )
            count = cur.fetchone()[0]
        ok = count >= minimum
        return CheckResult(
            f"audit.{table} ≥ {minimum} monthly partitions",
            ok,
            f"found {count}",
        )
    except Exception as exc:
        return CheckResult(
            f"audit.{table} ≥ {minimum} monthly partitions",
            False,
            f"query failed: {exc!r}",
        )


def check_constraints_present(
    conn, table: str, expected_names: tuple[str, ...]
) -> CheckResult:
    """Verify every named CHECK constraint exists on the parent table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT conname
                FROM pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'audit' AND c.relname = %s
                """,
                (table,),
            )
            found = {row[0] for row in cur.fetchall()}
        missing = sorted(set(expected_names) - found)
        ok = not missing
        return CheckResult(
            f"audit.{table} CHECK constraints",
            ok,
            f"all {len(expected_names)} present"
            if ok else f"missing: {missing}",
        )
    except Exception as exc:
        return CheckResult(
            f"audit.{table} CHECK constraints",
            False,
            f"query failed: {exc!r}",
        )


def check_indexes_present(
    conn, table: str, expected_names: tuple[str, ...]
) -> CheckResult:
    """Verify named indexes exist on the parent table (or any partition)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'audit' AND tablename = %s
                """,
                (table,),
            )
            found_on_parent = {row[0] for row in cur.fetchall()}
        # Postgres 11+ replicates partitioned-parent indexes onto each
        # partition automatically; the index name on the parent is
        # canonical for our purposes.
        missing = sorted(set(expected_names) - found_on_parent)
        ok = not missing
        return CheckResult(
            f"audit.{table} indexes",
            ok,
            f"all {len(expected_names)} present"
            if ok else f"missing: {missing}",
        )
    except Exception as exc:
        return CheckResult(
            f"audit.{table} indexes",
            False,
            f"query failed: {exc!r}",
        )


def check_initial_row_counts(conn) -> CheckResult:
    """Report row counts on audit.events and audit.payloads. Does not
    assert any specific value — the count is informational. A freshly
    migrated DB will read 0/0; a DB that has been through a smoke test
    or rollback drill may read higher."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM audit.events")
            events = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM audit.payloads")
            payloads = cur.fetchone()[0]
        return CheckResult(
            "audit row counts (informational)",
            True,
            f"events={events}  payloads={payloads}",
        )
    except Exception as exc:
        return CheckResult(
            "audit row counts (informational)",
            False,
            f"query failed: {exc!r}",
        )


# ── Driver ────────────────────────────────────────────────────────────


def _read_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(
            f"[warn] {name}={raw!r} is not an integer — using default {default}",
            file=sys.stderr,
        )
        return default


def _all_checks(conn, tenant_id: str, partitions_min: int) -> list[CheckResult]:
    """Run every check, in order. Returns the list for caller to render."""
    # Each check function is a closure binding `conn`; running them in a
    # list comprehension keeps the order deterministic and the
    # short-circuit-free semantics clear: every check runs even if an
    # earlier one fails, so the operator sees the full picture.
    checks: list[Callable[[], CheckResult]] = [
        lambda: check_connectivity(conn),
        lambda: check_alembic_head(conn),
        lambda: check_config_schema_exists(conn),
        lambda: check_tenant_seeded(conn, tenant_id),
        lambda: check_audit_parent_table(conn, "events"),
        lambda: check_audit_parent_table(conn, "payloads"),
        lambda: check_partitions_count(conn, "events", partitions_min),
        lambda: check_partitions_count(conn, "payloads", partitions_min),
        lambda: check_constraints_present(
            conn, "events", AUDIT_EVENTS_CHECK_CONSTRAINTS),
        lambda: check_constraints_present(
            conn, "payloads", AUDIT_PAYLOADS_CHECK_CONSTRAINTS),
        lambda: check_indexes_present(conn, "events", AUDIT_EVENTS_INDEXES),
        lambda: check_indexes_present(conn, "payloads", AUDIT_PAYLOADS_INDEXES),
        lambda: check_initial_row_counts(conn),
    ]
    return [fn() for fn in checks]


def main() -> int:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        print(
            "DATABASE_URL is not set — cannot probe. Set it to the target "
            "preview or production audit DB and re-run.",
            file=sys.stderr,
        )
        return 2

    tenant_id = os.environ.get(
        "AUDIT_VALIDATE_TENANT_ID", DEFAULT_TENANT_ID
    ).strip() or DEFAULT_TENANT_ID
    partitions_min = _read_int_env(
        "AUDIT_VALIDATE_PARTITIONS_MIN", DEFAULT_PARTITIONS_MIN
    )

    # Lazy-import psycopg so the script's source is importable in
    # environments where psycopg isn't installed (e.g. CI for an
    # environment that hasn't activated audit yet). The actual probe
    # absolutely requires psycopg at runtime.
    try:
        import psycopg
    except ImportError as exc:
        print(
            f"psycopg not installed in this Python environment: {exc}. "
            "Install via `pip install -r requirements.txt` and retry.",
            file=sys.stderr,
        )
        return 2

    print(
        f"Validating audit DB at host={_host_from_url(url)!r} "
        f"tenant_id={tenant_id!r} partitions_min={partitions_min}\n"
    )

    # autocommit=False so no transaction state lingers; we also
    # explicitly rollback() before close, even though we issue only
    # SELECTs.
    try:
        conn = psycopg.connect(url, connect_timeout=10, autocommit=False)
    except Exception as exc:
        print(f"[FAIL] Could not open connection: {exc!r}", file=sys.stderr)
        return 1

    try:
        results = _all_checks(conn, tenant_id, partitions_min)
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()

    for r in results:
        print(r.render())

    failed = [r for r in results if not r.passed]
    print()
    if failed:
        print(f"RESULT: FAIL — {len(failed)} of {len(results)} check(s) failed.")
        return 1
    print(f"RESULT: PASS — all {len(results)} checks passed.")
    return 0


def _host_from_url(url: str) -> str:
    """Best-effort host extraction for the banner line. Avoids printing
    credentials in the output."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.hostname:
            port = f":{parsed.port}" if parsed.port else ""
            return f"{parsed.hostname}{port}"
    except Exception:
        pass
    return "<unparseable>"


if __name__ == "__main__":
    sys.exit(main())
