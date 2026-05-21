"""
horizon_platform/access/permissions.py — PF-M1 permission resolution.

Pure functions for resolving a user's effective permissions and for
applying the server-authoritative port-scope read filter. Per the
PF-M1 Implementation Plan §10 and the Scope Proposal §3.7 / §5.3 /
§5.5:

  - Server-authoritative — every read and every write is authorised
    on the server, at the request boundary
  - No frontend-trusted permissions — the UI may hide affordances
    for clarity but never authoritative
  - Port-scoped by default — every read is filtered server-side by
    the user's port-scope set

This module is a *server-side-only* boundary. It has no I/O, no
global state, no UI awareness, and no implicit context. Every
function takes explicit arguments. The frontend cannot reach these
functions; they live in horizon_platform/ which is a server-side
package.

PF-M1 introduces READ-ONLY permissions. Write actions (ACK / DEFER /
APPLY / REJECT / ESCALATE) are PF-M4 scope and are NOT resolved here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, FrozenSet, Iterable, List, Optional, Set, TypeVar

from horizon_platform.identity.types import (
    ActionClass,
    Organisation,
    Permission,
    PortScopeAssignment,
    Role,
    ScopeClass,
    User,
)


T = TypeVar("T")


# ── Pure permission resolution ───────────────────────────────────────────────


def active_assignments_for_user(
    *,
    user: User,
    assignments: Iterable[PortScopeAssignment],
    as_of: datetime,
) -> List[PortScopeAssignment]:
    """Return the user's currently-active PortScopeAssignments.

    An assignment is included iff:
      - it belongs to this user
      - it is within its effective interval as of `as_of`
      - the user is currently active (not disabled)

    A disabled user has no active assignments, regardless of the
    assignment's effective interval.
    """
    if not user.is_active():
        return []
    return [
        a for a in assignments
        if a.user_id == user.id and a.is_active(as_of=as_of)
    ]


def effective_port_scope(
    *,
    user: User,
    assignments: Iterable[PortScopeAssignment],
    as_of: datetime,
) -> Set[str]:
    """Return the set of port_ids the user can read.

    A user with no active assignments returns the empty set — and
    by §5.5 of the Scope Proposal, must therefore see no operational
    data. The caller is responsible for filtering accordingly (see
    filter_by_port_scope below).
    """
    return {
        a.port_id
        for a in active_assignments_for_user(
            user=user, assignments=assignments, as_of=as_of,
        )
    }


def resolve_permissions(
    *,
    user: User,
    assignments: Iterable[PortScopeAssignment],
    role_catalogue: Dict[str, Role],
    as_of: datetime,
) -> FrozenSet[Permission]:
    """Return the union of all permissions the user holds across
    all their active port-scope assignments.

    Note: PF-M1 does not implement per-port permission scoping at
    this level (the per-port granularity is enforced in the read
    filter, not in the permission set). Future PF-M4+ work may
    refine this.
    """
    if not user.is_active():
        return frozenset()

    perms: Set[Permission] = set()
    for a in active_assignments_for_user(
        user=user, assignments=assignments, as_of=as_of,
    ):
        role = role_catalogue.get(a.role_key)
        if role is None:
            # Unknown role key in an assignment is a configuration
            # error; we drop it defensively and do NOT grant permissions.
            # The caller's audit-relevant logging should record this.
            continue
        perms.update(role.permissions)
    return frozenset(perms)


def has_permission(
    *,
    user: User,
    action_class: ActionClass,
    assignments: Iterable[PortScopeAssignment],
    role_catalogue: Dict[str, Role],
    as_of: datetime,
) -> bool:
    """Return True iff the user has any active assignment whose role
    grants the given action_class.

    For port-scoped action classes, callers must combine this with a
    port-scope check via effective_port_scope() — having the
    action_class is necessary but not sufficient for a specific
    port.
    """
    if not user.is_active():
        return False
    perms = resolve_permissions(
        user=user,
        assignments=assignments,
        role_catalogue=role_catalogue,
        as_of=as_of,
    )
    return any(p.action_class == action_class for p in perms)


# ── Server-authoritative port-scope read filter ──────────────────────────────


def filter_by_port_scope(
    *,
    items: Iterable[T],
    port_id_of: Callable[[T], Optional[str]],
    user: User,
    assignments: Iterable[PortScopeAssignment],
    as_of: datetime,
) -> List[T]:
    """Return only those items whose port_id is in the user's
    effective port-scope.

    `port_id_of(item)` is a caller-supplied accessor — this keeps the
    filter generic over any item shape without coupling the platform
    to a specific operational entity type.

    A user with no active port-scope returns the empty list. An item
    whose port_id is None is dropped (it has no port to scope).

    This function is the structural seam for server-side enforcement
    of Scope Proposal §3.4 / §5.5. The read handler in a future
    slice consumes the filtered list; it cannot opt out of the
    filter without bypassing this function.

    Sev-1 contract: if `assignments` is given and a vessel from a
    non-scoped port is returned, it is a security defect.
    """
    scope = effective_port_scope(
        user=user, assignments=assignments, as_of=as_of,
    )
    return [
        item for item in items
        if port_id_of(item) in scope
    ]


# ── Cross-organisation read guard ────────────────────────────────────────────


def can_read_organisation(
    *,
    actor: User,
    target_organisation_id: str,
) -> bool:
    """Return True iff `actor` is allowed to read `target_organisation_id`'s
    data.

    PF-M1 default: a user can read their own organisation only.
    No cross-organisation read permissions are configured in PF-M1
    sandbox (Scope Proposal §3.3, §4.15). Cross-stakeholder data
    sharing is a future PF-M8+ scope decision.

    This guard is invoked at the request boundary for endpoints that
    expose organisation-scoped data (e.g. pilot roster, terminal
    configuration). Per-port-scope filtering is a separate filter
    (see filter_by_port_scope above).
    """
    if not actor.is_active():
        return False
    return actor.organisation_id == target_organisation_id


# ── Server-authoritative shape invariants (referenced by tests) ──────────────
#
# These constants describe the structural properties this module
# guarantees. They are referenced by the server-authoritative shape
# tests in horizon_platform/tests/test_server_authoritative.py.

# Every public function takes its dependencies as explicit keyword-
# only arguments. There is no global state, no module-level cache, no
# implicit request context, no UI awareness.
SERVER_AUTHORITATIVE_FUNCTION_NAMES: FrozenSet[str] = frozenset({
    "active_assignments_for_user",
    "effective_port_scope",
    "resolve_permissions",
    "has_permission",
    "filter_by_port_scope",
    "can_read_organisation",
})
