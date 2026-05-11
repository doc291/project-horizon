"""
Alembic environment script for Project Horizon.

DATABASE_URL is read from the environment and injected into Alembic's
runtime config. The repo's db.py is the canonical source of the URL.

This script is invoked only when running migrations via the Alembic
CLI (developer-driven). It is never imported by server.py at runtime.
"""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Make repo root importable so future migrations can `from db import ...`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Inject DATABASE_URL from environment into Alembic's config.
_db_url = os.environ.get("DATABASE_URL", "").strip()
if _db_url:
    config.set_main_option("sqlalchemy.url", _db_url)

# Phase 0 migrations are hand-written; no SQLAlchemy ORM metadata yet.
target_metadata = None


def _require_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Alembic requires a target database. "
            "Set DATABASE_URL in your shell or .env file."
        )
    return url


def run_migrations_offline() -> None:
    """Generate SQL without connecting to the database."""
    context.configure(
        url=_require_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Apply migrations against the live database."""
    _require_url()
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
