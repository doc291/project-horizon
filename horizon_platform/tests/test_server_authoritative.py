"""
horizon_platform/tests/test_server_authoritative.py — PF-M1
server-authoritative shape tests.

Per the PF-M1 Scope Proposal §5.3 ("no frontend-trusted permissions")
and the Implementation Plan §12 invariants ("server-authoritative —
direct (non-UI) calls to protected endpoints are rejected"), this
suite proves *by shape* that the permission API is:

  1. Pure — no globals, no module-level cache, no implicit context
  2. Explicit-args — every function takes its dependencies as
     keyword-only arguments
  3. UI-unaware — there is no symbol in the module that touches a
     request, a session-cookie parser, or any UI concern
  4. Closed against trust-leak patterns — there is no "trust the
     frontend" / "trust the caller" / "skip the check" affordance

These are *structural* tests. They cannot prove runtime end-to-end
server-side enforcement (that requires Phase 2 sandbox HTTP tests).
They DO prove that the API surface itself has no shape into which
frontend trust could leak.

Implementation Plan §12.2 invariant #3: "Server rejects what the
UI hides — if a user's role hides an admin endpoint in the UI,
calling the same endpoint with that user's auth cookie returns
403." That end-to-end property is enforced by the resolve /
has_permission functions in this module; the structural tests
below ensure those functions cannot be bypassed.
"""

import inspect

from horizon_platform.access import permissions as perms_module
from horizon_platform.access.permissions import (
    SERVER_AUTHORITATIVE_FUNCTION_NAMES,
    active_assignments_for_user,
    can_read_organisation,
    effective_port_scope,
    filter_by_port_scope,
    has_permission,
    resolve_permissions,
)


# ── Pure function shape ──────────────────────────────────────────────────────


def test_all_authoritative_functions_are_present():
    """Every name in SERVER_AUTHORITATIVE_FUNCTION_NAMES must resolve
    to a callable in the permissions module — guards against accidental
    removal / renaming during refactor."""
    for name in SERVER_AUTHORITATIVE_FUNCTION_NAMES:
        assert hasattr(perms_module, name), (
            f"server-authoritative function {name!r} missing from "
            f"horizon_platform.access.permissions"
        )
        assert callable(getattr(perms_module, name))


def test_all_authoritative_functions_use_keyword_only_args():
    """Every public authoritative function must take its inputs as
    keyword-only arguments. Positional acceptance creates ambiguity
    about which argument is the user vs which is the actor vs which
    is the target — a real source of cross-port leakage bugs in
    real systems."""
    for name in SERVER_AUTHORITATIVE_FUNCTION_NAMES:
        fn = getattr(perms_module, name)
        sig = inspect.signature(fn)
        positional_or_keyword = [
            p for p in sig.parameters.values()
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        assert positional_or_keyword == [], (
            f"function {name!r} accepts positional / positional-or-keyword "
            f"arguments {[p.name for p in positional_or_keyword]}; PF-M1 "
            f"requires keyword-only arguments to prevent ambiguity in "
            f"authoritative calls"
        )


def test_module_has_no_global_mutable_state():
    """The permissions module must have no global mutable state. A
    global dict / list / set could become a trust-leak vector if
    untrusted code mutated it between authoritative calls."""
    for name, value in vars(perms_module).items():
        if name.startswith("_"):
            continue
        if inspect.ismodule(value) or inspect.isclass(value) or inspect.isfunction(value):
            continue
        if isinstance(value, (str, int, float, bool, type(None))):
            continue
        # Anything else must be a frozen / immutable container
        if isinstance(value, (frozenset, tuple)):
            continue
        # Read-only stdlib introspection objects (typing.Callable,
        # typing.TypeVar, __future__._Feature, etc.) are allowed.
        # They live in `typing` or `__future__` and are immutable by
        # stdlib design.
        cls_module = type(value).__module__ or ""
        if cls_module in ("typing", "__future__"):
            continue
        raise AssertionError(
            f"module-level symbol {name!r} of type {type(value).__name__} is "
            f"not provably immutable; permissions module must have no "
            f"global mutable state"
        )


# ── UI-unawareness ───────────────────────────────────────────────────────────


_FORBIDDEN_UI_HINTS = (
    "request",
    "session_cookie",
    "headers",
    "from_ui",
    "from_frontend",
    "browser",
    "tab",
    "centerpanel",
    "center_panel",
    "port_selector",
    "port_switcher",
)


def test_authoritative_functions_take_no_request_or_ui_arguments():
    """Authoritative functions must not accept arguments named like
    request / session_cookie / headers / from_ui / etc. That would
    invite "trust what the frontend sent us" patterns."""
    for name in SERVER_AUTHORITATIVE_FUNCTION_NAMES:
        fn = getattr(perms_module, name)
        sig = inspect.signature(fn)
        for pname in sig.parameters:
            lower = pname.lower()
            for hint in _FORBIDDEN_UI_HINTS:
                assert hint not in lower, (
                    f"function {name!r} has argument {pname!r} containing "
                    f"forbidden UI hint {hint!r}; permissions API must be "
                    f"UI-unaware"
                )


def test_module_source_contains_no_ui_imports():
    """The permissions module must not import any UI concern.
    Specifically: no flask request, no django request, no http
    cookies parsing, no frontend session helpers."""
    src = inspect.getsource(perms_module)
    forbidden = (
        "from flask import request",
        "from django.http",
        "import http.cookies",
        "from fastapi import Request",
        "VITE_",  # any vite env-var reference
        "window.",  # any browser global
    )
    for s in forbidden:
        assert s not in src, (
            f"permissions module contains forbidden UI-import / UI-ref "
            f"substring {s!r}"
        )


# ── Closed against trust-leak patterns ───────────────────────────────────────


def test_no_skip_check_affordance():
    """The permissions module must not expose a 'skip', 'bypass',
    'unsafe', or 'trust_caller' affordance.

    These are common shapes that grow during refactor and provide an
    accidental bypass route. PF-M1 forbids them by name."""
    src = inspect.getsource(perms_module).lower()
    for forbidden_name in (
        "skip_check",
        "bypass_check",
        "bypass_port_scope",
        "trust_caller",
        "unsafe_resolve",
        "skip_filter",
        "ignore_port_scope",
        "force_permit",
    ):
        assert forbidden_name not in src, (
            f"permissions module contains forbidden trust-leak symbol "
            f"{forbidden_name!r}"
        )


def test_filter_by_port_scope_signature_requires_explicit_assignments():
    """filter_by_port_scope must require the assignments argument
    explicitly. There must be no default that would silently skip
    the filter."""
    sig = inspect.signature(filter_by_port_scope)
    assignments_param = sig.parameters.get("assignments")
    assert assignments_param is not None, (
        "filter_by_port_scope must take an 'assignments' parameter"
    )
    assert assignments_param.default is inspect.Parameter.empty, (
        "filter_by_port_scope's 'assignments' parameter must have no default; "
        "a missing-by-default assignments list would silently disable the "
        "server-authoritative filter"
    )


def test_resolve_permissions_signature_requires_explicit_role_catalogue():
    """resolve_permissions must require role_catalogue explicitly —
    no module-level default catalogue that could be mutated to
    grant unintended permissions."""
    sig = inspect.signature(resolve_permissions)
    rc_param = sig.parameters.get("role_catalogue")
    assert rc_param is not None
    assert rc_param.default is inspect.Parameter.empty, (
        "resolve_permissions must take role_catalogue with no default"
    )


# ── Defensive constants (the public contract) ────────────────────────────────


def test_server_authoritative_function_names_is_a_frozen_set():
    """The contract list is frozen — guards against accidental
    modification at import time."""
    assert isinstance(SERVER_AUTHORITATIVE_FUNCTION_NAMES, frozenset)


def test_server_authoritative_function_names_includes_filter_and_resolve():
    """The two highest-criticality functions — the port-scope filter
    and the permission resolver — must be in the published contract."""
    assert "filter_by_port_scope" in SERVER_AUTHORITATIVE_FUNCTION_NAMES
    assert "resolve_permissions"  in SERVER_AUTHORITATIVE_FUNCTION_NAMES
    assert "has_permission"       in SERVER_AUTHORITATIVE_FUNCTION_NAMES
    assert "can_read_organisation" in SERVER_AUTHORITATIVE_FUNCTION_NAMES
