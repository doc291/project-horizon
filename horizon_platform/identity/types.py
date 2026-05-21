"""
horizon_platform/identity/types.py — PF-M1 identity domain types.

Pure dataclass + enum definitions for the PF-M1 identity model.
Per the PF-M1 Implementation Plan (PR #73) §6 conceptual shape:

  - Organisation    — a port-authority customer, terminal operator,
                      pilot organisation, tug operator, regulator, or partner
  - OrganisationType — the stakeholder class (closed enum)
  - User            — identity within exactly one Organisation
  - Role            — named permission bundle key
  - Permission      — (ActionClass, ScopeClass) pair
  - ActionClass     — what the role can do (closed enum)
  - ScopeClass      — over what scope the role can do it (closed enum)
  - PortScopeAssignment — (User, Port, Role) assignment

Scope amendment recorded in this slice (per Implementation Plan §19.19
"framework choice cannot be deferred past slice 1"):
  - Language:        Python 3.10 (continuity with the Beta 10 codebase)
  - Dependencies:    stdlib only for this scaffold slice
  - Test framework:  pytest (existing infrastructure)
  - Persistence:     NOT chosen yet — deferred to the next slice when
                     login / session handlers are implemented
  - Web framework:   NOT chosen yet — same deferral
  - API style:       NOT chosen yet — same deferral

These types are pure dataclasses + enums. They have no persistence,
no I/O, no global state, no UI awareness. They are server-side only
and importable only from server-side code. The frontend cannot reach
them.

Independence Reset framing preserved (no Smart Ocean X dependency).
Kyber boundary in force (no cryptographic primitives here).
Beta 10 untouched (this is a new module in a new path).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import FrozenSet, Optional


# ── Closed enums ─────────────────────────────────────────────────────────────


class OrganisationType(str, Enum):
    """Stakeholder class for an Organisation.

    Closed set per PF-M1 Implementation Plan §6.1. Members are
    intentionally lowercase + underscored so they round-trip through
    configuration files unchanged.
    """
    PORT_AUTHORITY     = "port_authority"
    TERMINAL_OPERATOR  = "terminal_operator"
    PILOT_ORGANISATION = "pilot_organisation"
    TUG_OPERATOR       = "tug_operator"
    REGULATOR          = "regulator"
    PARTNER            = "partner"


class ActionClass(str, Enum):
    """What a role is allowed to do.

    PF-M1 introduces read-only action classes. Write action classes
    (ACK / DEFER / APPLY / REJECT / ESCALATE) are PF-M4 scope and
    are NOT defined here. Admin actions are scoped to the admin's
    own organisation.
    """
    READ_OPERATIONAL = "read.operational"   # operational data (vessels, conflicts)
    READ_AUDIT       = "read.audit"         # audit-relevant events (PF-M3 surface)
    READ_AGGREGATED  = "read.aggregated"    # KPI / executive summaries
    READ_AUDIT_FULL  = "read.audit_full"    # audit detail incl. operator identity
    READ_TERMINAL    = "read.terminal"      # own terminal's berths (finer scope)
    ADMIN_USER       = "admin.user"         # provision / disable users (own org)
    ADMIN_ROLE       = "admin.role"         # edit role-permission mappings (own org)
    ADMIN_PORT_SCOPE = "admin.port_scope"   # edit port-scope assignments (own org)


class ScopeClass(str, Enum):
    """Over what scope a permission applies."""
    PORT         = "port"          # the user's port-scope set
    TERMINAL     = "terminal"      # the user's terminal-scope (finer than port)
    ORGANISATION = "organisation"  # the user's own organisation only
    SYSTEM       = "system"        # platform-wide (admin-only; not in PF-M1)


# ── Frozen value types ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class Permission:
    """A single permission: (action_class, scope_class)."""
    action_class: ActionClass
    scope_class:  ScopeClass


@dataclass(frozen=True)
class Role:
    """A named permission bundle.

    Roles are configuration, not code. The role key is the stable
    identifier; the permission set is the bundle. PF-M1 ships an
    initial catalogue in horizon_platform.identity.catalogue.
    """
    key:         str
    description: str
    permissions: FrozenSet[Permission]


# ── Domain entities ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Organisation:
    """A port-authority customer, terminal operator, pilot organisation,
    tug operator, regulator, or partner.

    Identity is by stable id. Organisations are the unit of tenant
    isolation (PF-M8 will exercise this; PF-M1 establishes the shape).
    """
    id:           str
    name:         str
    type:         OrganisationType
    created_at:   datetime
    disabled_at:  Optional[datetime] = None

    def is_active(self) -> bool:
        """An organisation is active iff not disabled."""
        return self.disabled_at is None


@dataclass(frozen=True)
class User:
    """A platform user.

    Belongs to exactly one Organisation. Multi-organisation membership
    is deferred to PF-M8 per Scope Proposal §4.15.

    `display_name` may be a real name (port-authority, terminal,
    regulator) or a resource-safe code (pilot organisations use codes
    like PILOT_BNE_PSP_03 per PR #57 §5.9). The choice is a per-
    organisation policy, not a global one.

    `email` is optional — sandbox test users may not have one; live
    deployments likely require it.
    """
    id:             str
    organisation_id: str
    display_name:   str
    email:          Optional[str]
    created_at:     datetime
    disabled_at:    Optional[datetime] = None
    last_login_at:  Optional[datetime] = None

    def is_active(self) -> bool:
        """A user is active iff not disabled."""
        return self.disabled_at is None


@dataclass(frozen=True)
class PortScopeAssignment:
    """A (User, Port, Role) triple recording which user has which
    role at which port.

    Effective interval semantics:
      - effective_from defaults to created_at if not set
      - effective_to == None means indefinite
      - an assignment is active iff effective_from <= now < (effective_to or +inf)

    The `as_of` parameter to is_active() permits deterministic testing
    of historical assignments (e.g. replay scenarios in PF-M6).
    """
    user_id:         str
    port_id:         str
    role_key:        str
    organisation_id: str         # denormalised for consistency-check at write time
    created_at:      datetime
    created_by:      str          # admin user id
    effective_from:  datetime
    effective_to:    Optional[datetime] = None

    def is_active(self, *, as_of: datetime) -> bool:
        """Active iff `as_of` falls within the effective interval."""
        if as_of < self.effective_from:
            return False
        if self.effective_to is not None and as_of >= self.effective_to:
            return False
        return True


# ── Convenience helpers (pure, no I/O) ───────────────────────────────────────


def make_permission(action_class: ActionClass, scope_class: ScopeClass) -> Permission:
    """Construct a Permission. Convenience wrapper around the dataclass."""
    return Permission(action_class=action_class, scope_class=scope_class)


def make_role(key: str, description: str, permissions: FrozenSet[Permission]) -> Role:
    """Construct a Role with a frozen permission set."""
    if not isinstance(permissions, frozenset):
        permissions = frozenset(permissions)
    return Role(key=key, description=description, permissions=permissions)
