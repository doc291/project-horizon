"""
horizon_platform/storage/repositories.py — PF-M1 repository contracts.

Defines the storage boundary for PF-M1 identity / RBAC data as
runtime-checkable Protocols (structural subtyping). The boundary is
deliberately minimal — it produces exactly the data the existing
slice-1a permission-resolution functions consume:

  - Organisation       — by id
  - User               — by id
  - PortScopeAssignment — by user id
  - Role catalogue     — frozen mapping

No database, no SQL, no schema, no migrations, no filesystem
persistence are introduced by this slice. The Protocols are pure
interface declarations; any class implementing the methods qualifies
without inheritance.

This slice is **the persistence boundary scaffold only**. The next
slice (under separate explicit authorisation) introduces a real
persistence implementation behind these Protocols. Until then, only
the in-memory implementation (horizon_platform.storage.in_memory)
exists, and that implementation is explicitly **non-production**.

Per the PF-M1 Implementation Plan §15 ("PF-M2 separation"), the
storage boundary is intentionally *thinner* than the API surface
boundary. Repositories return domain objects (User, Organisation,
PortScopeAssignment) — they do not return database rows, ORM
models, or JSON. The permission-resolution functions in
horizon_platform.access.permissions consume these domain objects
unchanged.

Independence Reset framing preserved.
Kyber boundary in force (no cryptographic primitives in this slice).
Beta 10 untouched (new code in a new path).
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional, Protocol, runtime_checkable

from horizon_platform.identity.types import (
    Organisation,
    PortScopeAssignment,
    Role,
    User,
)


# ── Repository contracts ─────────────────────────────────────────────────────


@runtime_checkable
class OrganisationRepository(Protocol):
    """Lookup of Organisation by id.

    `get_by_id` MUST return None for unknown ids. Returning a
    placeholder / default Organisation is forbidden — the caller
    must be able to distinguish "unknown" from "known and active"
    from "known and disabled".
    """

    def get_by_id(self, organisation_id: str) -> Optional[Organisation]: ...


@runtime_checkable
class UserRepository(Protocol):
    """Lookup of User by id.

    `get_by_id` MUST return None for unknown ids. The caller is
    responsible for checking `User.is_active()` — a disabled user
    is still resolvable here (the repository is a data-access
    layer, not an authorisation layer).

    Note: this slice does NOT introduce credential lookup
    (`get_by_credentials` etc.). Credential handling is part of
    the login / session slice and is gated behind a separate
    explicit authorisation per the Implementation Plan §20.2
    slice 2.
    """

    def get_by_id(self, user_id: str) -> Optional[User]: ...


@runtime_checkable
class PortScopeRepository(Protocol):
    """Lookup of PortScopeAssignments for a user.

    `list_for_user` MUST return an iterable (possibly empty) of all
    PortScopeAssignment rows where `assignment.user_id == user_id`.
    The repository performs no effective-interval filtering; that is
    the responsibility of `active_assignments_for_user` in the
    permissions module (which takes `as_of` explicitly). The
    repository simply produces the candidate rows.

    The iterable returned MUST NOT be a view into mutable internal
    state. Implementations should return a fresh list / tuple so
    the caller cannot mutate the repository through the return
    value.
    """

    def list_for_user(self, user_id: str) -> Iterable[PortScopeAssignment]: ...


@runtime_checkable
class RoleCatalogueRepository(Protocol):
    """Lookup of the role catalogue (frozen).

    `get_catalogue` returns a read-only Mapping of role_key -> Role.
    Implementations MUST return a mapping that cannot be mutated by
    the caller (e.g. `types.MappingProxyType` or `dict` returned
    fresh each call). The Role.permissions on each role are
    already frozensets per the slice-1a contract.

    PF-M1 sandbox ships the default catalogue from
    horizon_platform.identity.catalogue.get_default_role_catalogue.
    Production deployments override via configuration; the runtime
    loader (later slice) returns the same shape.
    """

    def get_catalogue(self) -> Mapping[str, Role]: ...


# ── Public contract constants (referenced by tests) ──────────────────────────


# The four repository contract names the storage module guarantees.
# Used by structural / no-bypass tests in horizon_platform/tests/.
STORAGE_PROTOCOL_NAMES: frozenset = frozenset({
    "OrganisationRepository",
    "UserRepository",
    "PortScopeRepository",
    "RoleCatalogueRepository",
})
