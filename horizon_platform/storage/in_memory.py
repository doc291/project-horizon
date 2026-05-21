"""
horizon_platform/storage/in_memory.py — In-memory repository implementations.

**NON-PRODUCTION.** These implementations exist for:
  - Unit and integration tests within horizon_platform/tests/
  - Sandbox bootstrap scenarios where no real persistence is yet
    configured
  - Demonstration / playground use

They are NOT a production persistence layer. The next slice (under
separate explicit authorisation) introduces a real persistence
implementation (likely relational + append-only audit table per
Platform Foundation §5.7), at which point this in-memory module
remains for tests only.

Each class implements exactly one of the Protocols defined in
horizon_platform.storage.repositories. Construction takes the
backing data as a constructor argument; the data is copied
internally so the caller cannot mutate the repository's internal
state by mutating the input list / dict afterwards.

Per the Implementation Plan §13 — these implementations have NO
I/O, NO database connection, NO filesystem persistence, and NO
network call. They are pure in-memory data structures wrapped in
the contract surface.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Iterable, List, Mapping, Optional

from horizon_platform.identity.catalogue import get_default_role_catalogue
from horizon_platform.identity.types import (
    Organisation,
    PortScopeAssignment,
    Role,
    User,
)


# ── In-memory Organisation repository ────────────────────────────────────────


class InMemoryOrganisationRepository:
    """In-memory OrganisationRepository.

    NON-PRODUCTION. Use only in tests / sandbox bootstrap.

    Construction copies the input into a private dict keyed by id;
    subsequent mutation of the constructor's argument list does
    NOT affect the repository.
    """

    def __init__(self, organisations: Iterable[Organisation] = ()):
        self._by_id: Dict[str, Organisation] = {
            o.id: o for o in organisations
        }

    def get_by_id(self, organisation_id: str) -> Optional[Organisation]:
        return self._by_id.get(organisation_id)


# ── In-memory User repository ────────────────────────────────────────────────


class InMemoryUserRepository:
    """In-memory UserRepository.

    NON-PRODUCTION. Use only in tests / sandbox bootstrap.
    """

    def __init__(self, users: Iterable[User] = ()):
        self._by_id: Dict[str, User] = {u.id: u for u in users}

    def get_by_id(self, user_id: str) -> Optional[User]:
        return self._by_id.get(user_id)


# ── In-memory PortScope repository ───────────────────────────────────────────


class InMemoryPortScopeRepository:
    """In-memory PortScopeRepository.

    NON-PRODUCTION. Use only in tests / sandbox bootstrap.

    Returns a fresh list on each `list_for_user` call so the caller
    cannot mutate internal state through the returned reference.
    """

    def __init__(self, assignments: Iterable[PortScopeAssignment] = ()):
        self._by_user_id: Dict[str, List[PortScopeAssignment]] = {}
        for a in assignments:
            self._by_user_id.setdefault(a.user_id, []).append(a)

    def list_for_user(self, user_id: str) -> Iterable[PortScopeAssignment]:
        # Return a fresh list — callers must not be able to mutate
        # the repository's internal storage.
        return list(self._by_user_id.get(user_id, ()))


# ── In-memory Role catalogue repository ──────────────────────────────────────


class InMemoryRoleCatalogueRepository:
    """In-memory RoleCatalogueRepository.

    NON-PRODUCTION. Use only in tests / sandbox bootstrap.

    Defaults to the PF-M1 default catalogue from
    horizon_platform.identity.catalogue. Returns a read-only view
    (MappingProxyType) so callers cannot mutate the catalogue.
    """

    def __init__(self, catalogue: Optional[Dict[str, Role]] = None):
        if catalogue is None:
            catalogue = get_default_role_catalogue()
        # Snapshot the input so external mutation cannot affect us.
        self._catalogue: Dict[str, Role] = dict(catalogue)

    def get_catalogue(self) -> Mapping[str, Role]:
        return MappingProxyType(self._catalogue)
