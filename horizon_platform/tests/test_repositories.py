"""
horizon_platform/tests/test_repositories.py — PF-M1 storage boundary tests.

Proves:
  - The Protocol contracts (OrganisationRepository, UserRepository,
    PortScopeRepository, RoleCatalogueRepository) are well-formed
  - The in-memory implementations satisfy the Protocols
    (runtime_checkable + isinstance test)
  - Unknown ids resolve to None (no placeholder pollution)
  - Repository data feeds the existing permission-resolution
    functions without bypass
  - Cross-port leakage is impossible through the repository boundary
  - Repository returns are isolated (caller cannot mutate internal
    state through returned references or constructor arg)
  - Structural / no-bypass guards remain in force
"""

from datetime import datetime, timedelta

import pytest

from horizon_platform.access.permissions import (
    effective_port_scope,
    filter_by_port_scope,
    has_permission,
    resolve_permissions,
)
from horizon_platform.identity.catalogue import (
    ROLE_HARBOUR_MASTER,
    ROLE_VTSO,
    get_default_role_catalogue,
)
from horizon_platform.identity.types import (
    ActionClass,
    Organisation,
    OrganisationType,
    PortScopeAssignment,
    User,
)
from horizon_platform.storage.in_memory import (
    InMemoryOrganisationRepository,
    InMemoryPortScopeRepository,
    InMemoryRoleCatalogueRepository,
    InMemoryUserRepository,
)
from horizon_platform.storage.repositories import (
    OrganisationRepository,
    PortScopeRepository,
    RoleCatalogueRepository,
    STORAGE_PROTOCOL_NAMES,
    UserRepository,
)


# ── Fixtures (pure constructors) ─────────────────────────────────────────────


NOW  = datetime(2026, 5, 21, 10, 0, 0)
PAST = NOW - timedelta(days=7)


def _org(id="org-pv", typ=OrganisationType.PORT_AUTHORITY,
         disabled: bool = False) -> Organisation:
    return Organisation(
        id=id, name=f"Org-{id}", type=typ,
        created_at=PAST,
        disabled_at=NOW - timedelta(days=1) if disabled else None,
    )


def _user(id="u1", organisation_id="org-pv", *, disabled=False) -> User:
    return User(
        id=id, organisation_id=organisation_id,
        display_name=f"User-{id}", email=f"{id}@example.com",
        created_at=PAST,
        disabled_at=NOW if disabled else None,
    )


def _assignment(*, user_id="u1", port_id="MELBOURNE",
                role_key=ROLE_VTSO, organisation_id="org-pv",
                expired=False) -> PortScopeAssignment:
    return PortScopeAssignment(
        user_id=user_id, port_id=port_id, role_key=role_key,
        organisation_id=organisation_id,
        created_at=PAST, created_by="admin-1",
        effective_from=PAST,
        effective_to=PAST + timedelta(hours=1) if expired else None,
    )


# ── Protocol contracts: structural invariants ────────────────────────────────


def test_storage_protocol_names_is_a_frozen_set():
    """STORAGE_PROTOCOL_NAMES is the published contract; it must
    be frozen so test code cannot accidentally extend it."""
    assert isinstance(STORAGE_PROTOCOL_NAMES, frozenset)


def test_storage_protocol_names_includes_all_four_repositories():
    """The four Protocols introduced by slice 1b must be in the
    published contract list."""
    assert STORAGE_PROTOCOL_NAMES == {
        "OrganisationRepository",
        "UserRepository",
        "PortScopeRepository",
        "RoleCatalogueRepository",
    }


def test_repository_protocols_are_runtime_checkable():
    """The Protocols are decorated @runtime_checkable so isinstance
    works for the in-memory implementations and any future
    persistence implementations."""
    assert isinstance(InMemoryOrganisationRepository(), OrganisationRepository)
    assert isinstance(InMemoryUserRepository(),         UserRepository)
    assert isinstance(InMemoryPortScopeRepository(),    PortScopeRepository)
    assert isinstance(InMemoryRoleCatalogueRepository(), RoleCatalogueRepository)


# ── Unknown-id handling: must return None, never a placeholder ──────────────


def test_organisation_repository_returns_none_for_unknown_id():
    repo = InMemoryOrganisationRepository(organisations=[_org("org-pv")])
    assert repo.get_by_id("org-does-not-exist") is None


def test_user_repository_returns_none_for_unknown_id():
    repo = InMemoryUserRepository(users=[_user("u1")])
    assert repo.get_by_id("u-does-not-exist") is None


def test_port_scope_repository_returns_empty_for_unknown_user():
    """Unknown user -> empty iterable. Not an exception."""
    repo = InMemoryPortScopeRepository(
        assignments=[_assignment(user_id="u1")],
    )
    assert list(repo.list_for_user("u-does-not-exist")) == []


# ── Active / disabled handling ───────────────────────────────────────────────


def test_user_repository_returns_disabled_user_unchanged():
    """The repository is a data-access layer, not an authorisation
    layer. A disabled user is still resolvable here; the caller
    is responsible for checking User.is_active()."""
    user = _user(id="u1", disabled=True)
    repo = InMemoryUserRepository(users=[user])
    got = repo.get_by_id("u1")
    assert got is not None
    assert got.is_active() is False


def test_inactive_user_resolves_to_no_permissions():
    """When repository data feeds permission resolution, an inactive
    user has no permissions and no port-scope. This is the
    integration guarantee — repository + permissions."""
    user = _user(id="u1", disabled=True)
    user_repo = InMemoryUserRepository(users=[user])
    ps_repo   = InMemoryPortScopeRepository(
        assignments=[_assignment(user_id="u1", port_id="MELBOURNE")],
    )
    catalogue_repo = InMemoryRoleCatalogueRepository()

    looked_up = user_repo.get_by_id("u1")
    assert looked_up is not None
    assignments = list(ps_repo.list_for_user("u1"))
    perms = resolve_permissions(
        user=looked_up,
        assignments=assignments,
        role_catalogue=catalogue_repo.get_catalogue(),
        as_of=NOW,
    )
    assert perms == frozenset()

    scope = effective_port_scope(
        user=looked_up, assignments=assignments, as_of=NOW,
    )
    assert scope == set()


def test_unknown_user_resolves_to_no_access():
    """An unknown user-id returns None from the repository. The
    calling code must treat that as 'no access'. This integration
    test confirms the fail-closed behaviour."""
    user_repo = InMemoryUserRepository(users=[_user("u1")])
    looked_up = user_repo.get_by_id("u-stranger")
    assert looked_up is None
    # The calling code must NOT attempt to compute permissions on
    # None. The structural contract is "no user -> no access".


# ── Organisation-membership invariant ────────────────────────────────────────


def test_user_belongs_to_exactly_one_organisation_via_repository():
    """Per Scope Proposal §4.15 (multi-org membership deferred to
    PF-M8), a user retrieved from the repository carries exactly
    one organisation_id."""
    user = _user(id="u1", organisation_id="org-pv")
    repo = InMemoryUserRepository(users=[user])
    got = repo.get_by_id("u1")
    assert got is not None
    assert isinstance(got.organisation_id, str)
    assert got.organisation_id == "org-pv"


def test_organisation_membership_required_for_meaningful_user():
    """A User without an organisation_id is structurally invalid
    (the dataclass requires it). This test confirms that
    constructing one without an org is a programming error."""
    with pytest.raises(TypeError):
        User(  # type: ignore[call-arg]
            id="u-orphan",
            display_name="Orphan",
            email=None,
            created_at=PAST,
        )


# ── Port-scope supplied by repository, NEVER by UI ───────────────────────────


def test_port_scope_assignments_come_from_repository_not_from_user_input():
    """Structural guarantee: port-scope is repository-supplied. The
    repository's list_for_user is the only legitimate source.

    This test confirms by shape that there is no public function on
    any repository that accepts a list of assignments from the
    caller and returns them — the assignments are constructor-
    provided (and intended to come from real persistence in the
    next slice), not request-provided.
    """
    import inspect

    from horizon_platform.storage import in_memory as in_memory_module

    # Every public method on the in-memory repositories must take
    # only `self` and identifiers (strings), never assignment lists.
    forbidden_param_names = {
        "assignments",
        "port_scopes",
        "scope_list",
        "from_request",
        "from_ui",
        "from_headers",
        "from_cookie",
    }
    for cls_name in (
        "InMemoryOrganisationRepository",
        "InMemoryUserRepository",
        "InMemoryPortScopeRepository",
        "InMemoryRoleCatalogueRepository",
    ):
        cls = getattr(in_memory_module, cls_name)
        for method_name, method in inspect.getmembers(
            cls, predicate=inspect.isfunction,
        ):
            if method_name.startswith("_"):
                continue
            sig = inspect.signature(method)
            for pname in sig.parameters:
                lower = pname.lower()
                for forbidden in forbidden_param_names:
                    assert forbidden not in lower, (
                        f"{cls_name}.{method_name} has parameter "
                        f"{pname!r} matching forbidden name {forbidden!r} "
                        f"— port-scope must not be request-supplied"
                    )


def test_port_scope_repository_returns_only_assignments_for_requested_user():
    """Cross-port leakage SEV-1 at the repository boundary: a
    PortScopeRepository asked for user A's assignments must NEVER
    return user B's assignments."""
    a_user_a = _assignment(user_id="user-A", port_id="MELBOURNE")
    a_user_b = _assignment(user_id="user-B", port_id="DARWIN")
    repo = InMemoryPortScopeRepository(assignments=[a_user_a, a_user_b])

    for a in repo.list_for_user("user-A"):
        assert a.user_id == "user-A"
    for a in repo.list_for_user("user-B"):
        assert a.user_id == "user-B"


# ── Cross-port leakage impossible via repository boundary ────────────────────


class _Vessel:
    """Minimal test entity carrying a port_id."""
    def __init__(self, name: str, port_id: str):
        self.name = name
        self.port_id = port_id


def _vessel_port(v: _Vessel):
    return v.port_id


def test_repository_boundary_to_filter_no_cross_port_leakage_SEV1():
    """SEV-1: User scoped to MELBOURNE via the repository must never
    see DARWIN data when filter_by_port_scope is called with the
    repository-supplied assignments.

    This is the end-to-end integration: repository -> permissions
    -> filter. The repository boundary cannot leak.
    """
    user = _user(id="vtso-mel", organisation_id="org-pv")
    user_repo = InMemoryUserRepository(users=[user])
    ps_repo = InMemoryPortScopeRepository(assignments=[
        _assignment(user_id="vtso-mel", port_id="MELBOURNE", role_key=ROLE_VTSO),
    ])

    mixed_items = [
        _Vessel("Mel-1",   "MELBOURNE"),
        _Vessel("Darwin-1", "DARWIN"),
        _Vessel("Mel-2",   "MELBOURNE"),
        _Vessel("Geelong-1", "GEELONG"),
        _Vessel("Darwin-2", "DARWIN"),
    ]

    looked_up = user_repo.get_by_id("vtso-mel")
    assert looked_up is not None
    assignments = list(ps_repo.list_for_user("vtso-mel"))

    result = filter_by_port_scope(
        items=mixed_items, port_id_of=_vessel_port,
        user=looked_up, assignments=assignments, as_of=NOW,
    )
    port_ids = {v.port_id for v in result}
    assert port_ids == {"MELBOURNE"}, (
        f"SEV-1: repository -> filter leaked cross-port: {port_ids}"
    )


def test_repository_supplied_assignments_drive_permission_resolution():
    """Repository outputs feed resolve_permissions unchanged — no
    bypass, no transformation, no UI-supplied substitution."""
    user = _user(id="hm-mel", organisation_id="org-pv")
    user_repo = InMemoryUserRepository(users=[user])
    ps_repo = InMemoryPortScopeRepository(assignments=[
        _assignment(user_id="hm-mel", port_id="MELBOURNE",
                    role_key=ROLE_HARBOUR_MASTER),
    ])
    catalogue_repo = InMemoryRoleCatalogueRepository()

    looked_up = user_repo.get_by_id("hm-mel")
    assert looked_up is not None
    assignments = list(ps_repo.list_for_user("hm-mel"))

    perms = resolve_permissions(
        user=looked_up,
        assignments=assignments,
        role_catalogue=catalogue_repo.get_catalogue(),
        as_of=NOW,
    )
    actions = {p.action_class for p in perms}
    # Harbour Master has READ_OPERATIONAL + READ_AUDIT + READ_AUDIT_FULL
    assert ActionClass.READ_OPERATIONAL in actions
    assert ActionClass.READ_AUDIT       in actions
    assert ActionClass.READ_AUDIT_FULL  in actions
    # But NOT admin (Harbour Master is operational, not admin)
    assert ActionClass.ADMIN_USER not in actions


# ── Isolation: caller cannot mutate repository internal state ────────────────


def test_organisation_repo_isolated_from_constructor_input_mutation():
    """Mutating the constructor's argument list after construction
    must NOT affect the repository's contents."""
    orgs = [_org("org-pv")]
    repo = InMemoryOrganisationRepository(organisations=orgs)
    orgs.append(_org("org-evil"))
    assert repo.get_by_id("org-evil") is None
    assert repo.get_by_id("org-pv") is not None


def test_port_scope_repo_isolated_from_constructor_input_mutation():
    assignments = [_assignment(user_id="u1", port_id="MELBOURNE")]
    repo = InMemoryPortScopeRepository(assignments=assignments)
    assignments.append(_assignment(user_id="u1", port_id="DARWIN"))
    # DARWIN must NOT appear via the repository
    rows = list(repo.list_for_user("u1"))
    port_ids = {a.port_id for a in rows}
    assert "DARWIN" not in port_ids
    assert "MELBOURNE" in port_ids


def test_port_scope_repo_returns_fresh_list():
    """Mutating the returned list MUST NOT affect the repository."""
    repo = InMemoryPortScopeRepository(assignments=[
        _assignment(user_id="u1", port_id="MELBOURNE"),
    ])
    rows = list(repo.list_for_user("u1"))
    rows.append(_assignment(user_id="u1", port_id="DARWIN"))
    # Re-fetch — DARWIN must NOT be present
    rows2 = list(repo.list_for_user("u1"))
    port_ids = {a.port_id for a in rows2}
    assert port_ids == {"MELBOURNE"}


def test_role_catalogue_repo_returns_read_only_mapping():
    """The catalogue mapping returned by get_catalogue() must be
    read-only. A caller trying to mutate it must fail."""
    repo = InMemoryRoleCatalogueRepository()
    cat = repo.get_catalogue()
    with pytest.raises((TypeError, AttributeError)):
        cat["evil_role"] = None  # type: ignore[index]


def test_role_catalogue_repo_isolated_from_input_dict_mutation():
    """Mutating the dict passed to the constructor must NOT affect
    the repository's catalogue."""
    custom = dict(get_default_role_catalogue())
    repo = InMemoryRoleCatalogueRepository(catalogue=custom)
    # Try to inject a new role via the constructor argument after the fact
    custom["evil_role"] = None  # type: ignore[assignment]
    cat = repo.get_catalogue()
    assert "evil_role" not in cat


# ── Structural / no-bypass guards ────────────────────────────────────────────


def test_storage_module_has_no_global_mutable_state():
    """The storage modules must not introduce global mutable state.
    A global dict / list / set could become a trust-leak vector if
    untrusted code mutated it between authoritative calls."""
    import inspect

    from horizon_platform.storage import in_memory, repositories

    for module in (repositories, in_memory):
        for name, value in vars(module).items():
            if name.startswith("_"):
                continue
            if (
                inspect.ismodule(value)
                or inspect.isclass(value)
                or inspect.isfunction(value)
                or callable(value) and not isinstance(value, (dict, list, set))
            ):
                continue
            if isinstance(value, (str, int, float, bool, type(None))):
                continue
            if isinstance(value, (frozenset, tuple)):
                continue
            cls_module = type(value).__module__ or ""
            if cls_module in ("typing", "__future__"):
                continue
            raise AssertionError(
                f"module {module.__name__}: symbol {name!r} of type "
                f"{type(value).__name__} is not provably immutable; "
                f"storage modules must have no global mutable state"
            )


def test_storage_module_has_no_frontend_imports():
    """The storage modules must not import any frontend concern.
    The frontend cannot reach these modules; they must not reach
    forward into the frontend either (e.g. by referring to Vite
    env vars, browser globals, etc.)."""
    import inspect

    from horizon_platform.storage import in_memory, repositories

    for module in (repositories, in_memory):
        src = inspect.getsource(module)
        for forbidden in (
            "VITE_",        # any vite env var
            "window.",      # browser global
            "document.",    # browser global
            "import vite",  # speculative
        ):
            assert forbidden not in src, (
                f"module {module.__name__} contains forbidden "
                f"frontend reference {forbidden!r}"
            )


def test_storage_module_has_no_server_py_import():
    """The storage modules must not import Beta 10's server.py.
    This is the Beta 10 Immutability shape guard: storage is new
    code in a new path; it does not reach into Beta 10."""
    import inspect

    from horizon_platform.storage import in_memory, repositories

    for module in (repositories, in_memory):
        src = inspect.getsource(module)
        forbidden_patterns = (
            "import server",
            "from server import",
            "import port_profiles",
            "from port_profiles import",
            "import audit",
            "from audit import",
            "import conflict_audit",
            "import session_audit",
        )
        for pattern in forbidden_patterns:
            assert pattern not in src, (
                f"module {module.__name__} contains forbidden Beta 10 "
                f"import pattern {pattern!r}; storage must remain "
                f"in horizon_platform/* path only"
            )


def test_storage_module_has_no_skip_bypass_unsafe_affordance():
    """The storage modules must not expose a 'skip', 'bypass',
    'unsafe', 'trust_caller', or 'force_*' affordance — common
    shapes that grow during refactor and provide accidental
    bypass routes."""
    import inspect

    from horizon_platform.storage import in_memory, repositories

    for module in (repositories, in_memory):
        src = inspect.getsource(module).lower()
        for forbidden in (
            "skip_check",
            "bypass_check",
            "bypass_port_scope",
            "trust_caller",
            "unsafe_get",
            "unsafe_list",
            "skip_filter",
            "ignore_user_id",
            "force_permit",
            "include_disabled",  # disabled-inclusion would let admins silently see disabled-user data
        ):
            assert forbidden not in src, (
                f"module {module.__name__} contains forbidden trust-leak "
                f"symbol {forbidden!r}"
            )
