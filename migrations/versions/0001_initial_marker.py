"""initial marker — establishes alembic_version table only

Revision ID: 0001_initial_marker
Revises:
Create Date: 2026-05-12

Phase 0.2 baseline migration per ADR-002. This migration creates no
application tables. Its sole purpose is to establish the
alembic_version row in the target database and confirm migration
tooling is wired correctly.

Subsequent Phase 0 migrations build on this baseline:
- Phase 0.3 creates the config.* schema (tenant configuration)
- Phase 0.5 creates the audit.* schema (audit ledger)
"""

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "0001_initial_marker"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """No-op upgrade — establishes alembic_version tracking only."""
    pass


def downgrade() -> None:
    """No-op downgrade — nothing to remove."""
    pass
