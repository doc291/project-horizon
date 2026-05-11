"""
Project Horizon — Postgres connection support.

Phase 0.2 per ADR-002. This module is the canonical entry point for any
Postgres interaction in the Horizon codebase. In Phase 0.2 its only
responsibility is to verify connectivity at startup when DATABASE_URL is
set. Later Phase 0 steps add the audit writer, tenant config readers,
and the operational query paths against this same module.

Gating
------
- If DATABASE_URL is unset, every public function in this module is a
  no-op or returns False. Horizon runs in Beta-10-equivalent in-memory
  mode and never touches Postgres.
- If DATABASE_URL is set, connection verification runs at startup.
  Misconfiguration surfaces immediately rather than at first query.

psycopg is imported lazily so production deploys (which install only
requirements.txt and do not include psycopg) continue to run unchanged
when DATABASE_URL is unset. If DATABASE_URL is set but psycopg is not
installed, the application fails fast with a clear error message.
"""

import logging
import os

log = logging.getLogger("horizon.db")

# Read once at module load. The empty-string default plus strip() means
# whitespace-only values are treated as unset.
_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()


def is_configured() -> bool:
    """Return True if DATABASE_URL is set to a non-empty value."""
    return bool(_DATABASE_URL)


def get_url() -> str:
    """
    Return the configured DATABASE_URL. Used by Alembic and by future
    Phase 0 steps. Raises RuntimeError if DATABASE_URL is not set so
    misuse surfaces immediately.
    """
    if not _DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return _DATABASE_URL


def verify_connection_if_configured() -> None:
    """
    If DATABASE_URL is set, open a connection, execute SELECT 1, and
    log the result. Raises on failure so the application fails fast at
    startup.

    If DATABASE_URL is unset, this is a no-op and Horizon runs in
    Beta-10-equivalent in-memory mode.
    """
    if not is_configured():
        log.info("DATABASE_URL not set — running in legacy in-memory mode")
        return

    # Lazy import: psycopg is only required when DATABASE_URL is set.
    # This keeps production deploys (requirements.txt only, no psycopg)
    # functional in the default Beta-10-equivalent mode.
    try:
        import psycopg
    except ImportError as exc:
        log.error(
            "DATABASE_URL is set but psycopg is not installed. "
            "Install requirements-dev.txt for local development, or add "
            "psycopg to requirements.txt before enabling DATABASE_URL "
            "in production."
        )
        raise RuntimeError("psycopg is required when DATABASE_URL is set") from exc

    try:
        with psycopg.connect(_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                if row != (1,):
                    raise RuntimeError(f"Unexpected SELECT 1 result: {row!r}")
    except Exception:
        log.error("DATABASE_URL verification failed — could not connect to Postgres")
        raise

    log.info("DATABASE_URL verified — Postgres connectivity OK")
