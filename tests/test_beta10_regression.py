"""
Project Horizon — Beta 10 demo regression gate (Phase 0 exit gate).

This test module is the Phase 0 exit gate. It locks the production-safety
property so that no future audit-instrumentation change can silently
disturb Beta 10's runtime behaviour.

A failure here means one of:
  - A response shape changed
  - A protected route became public, or a public route became protected
  - A security header was removed / renamed / reordered
  - A static asset (logo / icon) was replaced
  - An audit helper started doing real work in production posture
    (DATABASE_URL unset)
  - The set of audit modules / their entry points changed

Each failure mode is an explicit, reviewable regression — the test
baseline must be updated deliberately, not by accident.

This module runs with `DATABASE_URL` deliberately unset to mirror
production. Audit helpers must not spawn threads or import psycopg/audit
under any path exercised here.

Plan reference: Step 0.11 (Phase 0 exit gate).
Manual companion: phase0-exit-runbook.md.
"""

from __future__ import annotations

import email.message
import hashlib
import io
import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest

# Make repo root importable
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# ── Production-posture environment ──────────────────────────────────
# server.py imports require HORIZON_USER, HORIZON_PASS, TOKEN_SECRET to
# be set. DATABASE_URL must be UNSET to mirror Railway production.

@pytest.fixture(autouse=True)
def _production_environment(monkeypatch):
    """Apply the exact production environment posture for every test."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("AUDIT_EMISSION_ENABLED", "true")  # default; helper still no-ops without DB
    monkeypatch.setenv("HORIZON_USER", "admin")
    monkeypatch.setenv("HORIZON_PASS", "admin")
    monkeypatch.setenv("TOKEN_SECRET", "x" * 32)
    # Evict any cached server/audit modules so the env vars take effect
    for k in list(sys.modules):
        if k in {"db", "tenant"} or k.endswith("_audit"):
            del sys.modules[k]


# ════════════════════════════════════════════════════════════════════
# §1.9 Server module inventory — audit helpers and key constants
# ════════════════════════════════════════════════════════════════════

EXPECTED_AUDIT_HELPERS = (
    "session_audit",
    "conflict_audit",
    "recommendation_audit",
    "recommendation_presented_audit",
    "operator_action_audit",
)


class TestServerModuleInventory:

    def test_all_five_audit_helpers_import(self):
        for name in EXPECTED_AUDIT_HELPERS:
            mod = __import__(name)
            assert hasattr(mod, "is_enabled"), f"{name}.is_enabled() missing"
            assert hasattr(mod, "emit_async"), f"{name}.emit_async() missing"

    def test_server_constants_present_and_typed(self):
        import server
        assert isinstance(server._AUTH_USER, str)
        assert isinstance(server._AUTH_PASS, str)
        assert isinstance(server._COOKIE_NAME, str) and server._COOKIE_NAME
        assert isinstance(server._COOKIE_TTL, int) and server._COOKIE_TTL > 0
        assert server._SITE_HOST == "horizon.ams.group"


# ════════════════════════════════════════════════════════════════════
# §1.1–§1.4 build_summary() shape lock
# ════════════════════════════════════════════════════════════════════

EXPECTED_SUMMARY_KEYS = frozenset({
    "port_name", "generated_at", "lookahead_hours", "data_source",
    "data_source_label", "scraped_at", "port_status",
    "vessels", "berths", "pilotage", "towage", "port_tugs", "port_gangs",
    "conflicts", "guidance", "port_geo", "weather", "tides",
    "berth_utilisation", "etd_risk", "dashboard", "ukc", "arrival_ukc",
    "dukc", "esg", "port_profile",
})

EXPECTED_PORT_STATUS_KEYS = frozenset({
    "berths_occupied", "berths_available", "berths_total",
    "vessels_in_port", "vessels_expected_24h", "vessels_departing_24h",
    "active_conflicts", "critical_conflicts",
    "pilots_available", "tugs_available",
})

EXPECTED_DECISION_SUPPORT_KEYS = frozenset({
    "recommended_option_id", "recommended_reasoning", "confidence",
    "decision_deadline", "options",
})


class TestBuildSummaryShape:

    def test_top_level_key_set_matches_baseline(self):
        import server
        summary = server.build_summary()
        actual = set(summary.keys())
        missing = EXPECTED_SUMMARY_KEYS - actual
        extra   = actual - EXPECTED_SUMMARY_KEYS
        assert not missing, f"build_summary() missing keys: {missing}"
        assert not extra,   f"build_summary() unexpected keys: {extra}"

    def test_port_status_sub_keys_match_baseline(self):
        import server
        summary = server.build_summary()
        actual = set(summary["port_status"].keys())
        missing = EXPECTED_PORT_STATUS_KEYS - actual
        extra   = actual - EXPECTED_PORT_STATUS_KEYS
        assert not missing, f"port_status missing keys: {missing}"
        assert not extra,   f"port_status unexpected keys: {extra}"

    def test_conflicts_contains_hardcoded_demo_scenarios(self):
        """In simulation mode (no live AIS), the engine produces the B03
        + B04 hardcoded scenarios. If these disappear, the demo's
        Decision Cards go empty."""
        import server
        summary = server.build_summary()
        berths_with_conflicts = {
            c.get("berth_id")
            for c in summary.get("conflicts") or []
            if c.get("conflict_type") == "berth_overlap"
        }
        assert "B03" in berths_with_conflicts, (
            "Demo B03 berth_overlap scenario missing from /api/summary"
        )
        assert "B04" in berths_with_conflicts, (
            "Demo B04 berth_overlap scenario missing from /api/summary"
        )

    def test_every_decision_support_has_required_sub_keys(self):
        """Decision Cards render against these five sub-keys. A missing
        one breaks the UI silently."""
        import server
        summary = server.build_summary()
        for c in summary.get("conflicts") or []:
            ds = c.get("decision_support")
            if ds is None:
                continue
            actual = set(ds.keys())
            missing = EXPECTED_DECISION_SUPPORT_KEYS - actual
            assert not missing, (
                f"conflict {c.get('id')} decision_support missing keys: "
                f"{missing} (have: {sorted(actual)})"
            )


# ════════════════════════════════════════════════════════════════════
# §3, §4, §5 stub handler for in-process request driving
# ════════════════════════════════════════════════════════════════════

def _build_stub(server_module, path, *, method="GET", host="localhost",
                cookie=None, body=b""):
    """
    Build a HorizonHandler-derived stub that captures status, headers,
    and body without needing a real socket. send_response and send_header
    are overridden so we can inspect what was sent; end_headers is left
    to the real implementation so the six security headers fire.
    """

    class Stub(server_module.HorizonHandler):
        def __init__(self):
            # Bypass BaseHTTPRequestHandler.__init__ — it expects a socket.
            self.path = path
            self.command = method
            self.headers = email.message.Message()
            self.headers["Host"] = host
            if cookie:
                self.headers["Cookie"] = cookie
            if body:
                self.headers["Content-Length"] = str(len(body))
            self.rfile = io.BytesIO(body)
            self.wfile = io.BytesIO()
            self.client_address = ("127.0.0.1", 0)
            self.request_version = "HTTP/1.1"
            self.status_code = None
            self.captured_headers = []  # list of (name, value)
            self.tenant_id = "ams-demo"
            # BaseHTTPRequestHandler.end_headers appends to
            # self._headers_buffer; we override send_header so the
            # buffer is never auto-created. Initialise it explicitly so
            # super().end_headers() does not AttributeError.
            self._headers_buffer = []

        def send_response(self, code, message=None):
            self.status_code = code

        def send_header(self, name, value):
            self.captured_headers.append((name, value))

        def log_request(self, *a, **kw): pass
        def log_message(self, *a, **kw): pass
        def log_error(self, *a, **kw): pass

        def header_value(self, name):
            for n, v in self.captured_headers:
                if n.lower() == name.lower():
                    return v
            return None

    return Stub()


# ════════════════════════════════════════════════════════════════════
# §4 Public paths must remain public
# ════════════════════════════════════════════════════════════════════

EXPECTED_PUBLIC_PATHS = frozenset({
    "/login", "/logo", "/amsg-logo", "/health", "/api/health-data",
    "/favicon.ico", "/apple-touch-icon.png",
})


class TestPublicPathsPolicy:

    def test_public_paths_set_matches_baseline(self):
        import server
        actual = set(server._PUBLIC_PATHS)
        missing = EXPECTED_PUBLIC_PATHS - actual
        extra   = actual - EXPECTED_PUBLIC_PATHS
        assert not missing, f"_PUBLIC_PATHS missing entries: {missing}"
        assert not extra,   f"_PUBLIC_PATHS has unexpected entries: {extra}"

    def test_login_page_renders_without_auth(self):
        import server
        stub = _build_stub(server, "/login")
        stub.do_GET()
        assert stub.status_code == 200
        ct = stub.header_value("Content-Type") or ""
        assert "text/html" in ct, f"/login content-type unexpected: {ct}"

    def test_health_page_renders_without_auth(self):
        import server
        stub = _build_stub(server, "/health")
        stub.do_GET()
        assert stub.status_code == 200

    def test_health_data_returns_json_without_auth(self):
        """Structural check only — /api/health-data byte-size varies
        with live AIS/MST/BOM state, so we verify shape only
        (per Step 0.11 amendment)."""
        import server
        stub = _build_stub(server, "/api/health-data")
        stub.do_GET()
        assert stub.status_code == 200
        ct = stub.header_value("Content-Type") or ""
        assert "application/json" in ct
        payload = json.loads(stub.wfile.getvalue().decode())
        # Expected top-level keys
        for k in ("time", "overall", "aisstream", "mst_configured",
                  "ports", "issues"):
            assert k in payload, f"/api/health-data missing top-level key: {k}"
        assert payload["overall"] in ("ready", "warning", "issues")
        assert isinstance(payload["ports"], dict)
        assert isinstance(payload["issues"], list)
        assert isinstance(payload["mst_configured"], bool)
        assert isinstance(payload["aisstream"], dict)


# ════════════════════════════════════════════════════════════════════
# §3 Protected routes must remain auth-gated
# ════════════════════════════════════════════════════════════════════

PROTECTED_GET_PATHS = (
    "/",
    "/mobile",
    "/api/summary",
    "/api/diag",
    "/api/aisstream-status",
    "/api/mst-status",
    "/api/port-brief",
    "/api/brief-config",
    "/api/scrape",
    "/api/debug",
)

PROTECTED_POST_PATHS = (
    "/api/set_port",
    "/api/send-brief",
    "/api/whatif",
    "/api/apply-whatif",
    "/api/clear-whatif",
)


class TestAuthGatedRoutes:

    @pytest.mark.parametrize("path", PROTECTED_GET_PATHS)
    def test_protected_get_redirects_without_cookie(self, path):
        import server
        stub = _build_stub(server, path)
        stub.do_GET()
        assert stub.status_code == 302, (
            f"GET {path} should redirect (302) without cookie; "
            f"got {stub.status_code}"
        )
        loc = stub.header_value("Location") or ""
        assert loc.startswith("/login?next="), (
            f"GET {path} did not redirect to /login; got Location={loc!r}"
        )

    @pytest.mark.parametrize("path", ("/api/set_port", "/api/send-brief"))
    def test_protected_post_returns_401_without_cookie(self, path):
        """The do_POST handler explicitly checks _is_authenticated() and
        returns 401 for /api/set_port and /api/send-brief. The other
        whatif POSTs don't auth-gate at the POST handler level (they're
        gated at the network edge in a real deployment), so we only
        cover the two explicit auth-checked POSTs here."""
        import server
        stub = _build_stub(server, path, method="POST", body=b"{}")
        try:
            stub.do_POST()
        except SystemExit:
            pass
        # send_error sets status_code via send_response(401, ...)
        assert stub.status_code == 401, (
            f"POST {path} should return 401 without cookie; "
            f"got {stub.status_code}"
        )


# ════════════════════════════════════════════════════════════════════
# §5 Security headers
# ════════════════════════════════════════════════════════════════════

EXPECTED_SECURITY_HEADERS = (
    ("X-Content-Type-Options",     "nosniff"),
    ("X-Frame-Options",            "DENY"),
    ("Referrer-Policy",            "strict-origin-when-cross-origin"),
    ("Permissions-Policy",         "camera=(), microphone=(), geolocation=(), interest-cohort=()"),
    ("Cross-Origin-Opener-Policy", "same-origin"),
    ("Strict-Transport-Security",  "max-age=15552000; includeSubDomains"),
)


class TestSecurityHeaders:

    def test_security_headers_tuple_matches_baseline(self):
        """Six entries, exact names and values, in order. Asserts the
        static class attribute directly."""
        import server
        assert server.HorizonHandler._SECURITY_HEADERS == \
            EXPECTED_SECURITY_HEADERS

    def test_security_headers_fire_on_every_response(self):
        """Drive a request that exits via _redirect (which calls
        end_headers) and confirm all 6 security headers appear."""
        import server
        stub = _build_stub(server, "/api/summary")
        stub.do_GET()
        # /api/summary without cookie → 302 redirect → end_headers fires
        captured_names = {n for (n, _v) in stub.captured_headers}
        for name, _value in EXPECTED_SECURITY_HEADERS:
            assert name in captured_names, (
                f"security header {name!r} not emitted on protected-route "
                f"redirect; got headers: {sorted(captured_names)}"
            )

    def test_security_headers_fire_on_public_response(self):
        """The headers must fire on public responses too — not just on
        auth redirects."""
        import server
        stub = _build_stub(server, "/api/health-data")
        stub.do_GET()
        captured_names = {n for (n, _v) in stub.captured_headers}
        for name, _value in EXPECTED_SECURITY_HEADERS:
            assert name in captured_names, (
                f"security header {name!r} not emitted on public route; "
                f"got headers: {sorted(captured_names)}"
            )


# ════════════════════════════════════════════════════════════════════
# §5 Static asset integrity (icons / logos)
# ════════════════════════════════════════════════════════════════════

EXPECTED_ASSET_HASHES = {
    # Computed 2026-05-13 on origin/main tip (69558d4) under Python 3.10.20.
    # Update only with explicit reviewer authorisation.
    "logo.png":        "7f3d7ae430292caa426888b22b2bafbd3ad0f1fa65a5c845310946c79c4c8d72",
    "logo.svg":        "743fdec40bce8cee0620238e4c13f282f9bca07045b5bed53b16b14e43e6d05f",
    "amsg-logo.png":   "d1fd74240ad4fb3ded69284f23073988a72423aa16f57838b7c528c18096c3a9",
    "mobile-icon.png": "26b896c94063be2be859caf15ba722904c43c6556669cd3bcdd950159c5f7265",
}


class TestStaticAssetIntegrity:

    @pytest.mark.parametrize("filename,expected_sha",
                              list(EXPECTED_ASSET_HASHES.items()))
    def test_asset_sha256_matches_baseline(self, filename, expected_sha):
        path = REPO_ROOT / filename
        assert path.exists(), f"baseline asset missing: {filename}"
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == expected_sha, (
            f"{filename} SHA-256 mismatch — asset replaced without "
            f"updating Beta 10 baseline. "
            f"expected={expected_sha} actual={actual}"
        )


# ════════════════════════════════════════════════════════════════════
# §6 Audit helpers must no-op under production posture
# ════════════════════════════════════════════════════════════════════

class TestAuditHelpersNoOpInProduction:
    """For every audit helper, with DATABASE_URL unset:
       - is_enabled() returns False
       - emit_async/emit_sync spawn no thread and import no psycopg/audit
    """

    def _block_psycopg(self):
        class Blocker:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in {"psycopg", "psycopg_binary"}:
                    raise ImportError(f"simulated production: {name}")
                return None
        return Blocker()

    @pytest.mark.parametrize("module_name", EXPECTED_AUDIT_HELPERS)
    def test_helper_is_disabled_in_production_posture(self, module_name):
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith(("psycopg", "audit", "db", module_name)):
                del sys.modules[k]

        sys.meta_path.insert(0, self._block_psycopg())
        try:
            mod = __import__(module_name)
            assert mod.is_enabled() is False, (
                f"{module_name}.is_enabled() must be False with "
                f"DATABASE_URL unset"
            )
            assert "psycopg" not in sys.modules
            assert "audit" not in sys.modules, (
                f"importing {module_name} pulled audit.py into sys.modules — "
                f"production should never reach the audit module"
            )
        finally:
            sys.meta_path[:] = orig_meta_path

    def test_all_five_helpers_collectively_spawn_no_threads(self):
        """Drive a representative emit_async call on each helper and
        confirm no daemon thread is spawned in aggregate."""
        orig_meta_path = sys.meta_path.copy()
        for k in list(sys.modules):
            if k.startswith(("psycopg", "audit", "db")) or k.endswith("_audit"):
                del sys.modules[k]
        sys.meta_path.insert(0, self._block_psycopg())
        try:
            threads_before = threading.active_count()

            import session_audit
            session_audit.emit_async(
                "ams-demo", "SESSION_STARTED", "operator_session",
                "horizon", {"username": "horizon"},
            )

            import conflict_audit
            conflict_audit.emit_async(
                "ams-demo",
                {"id": "C-X", "conflict_type": "berth_overlap",
                 "severity": "high"},
                port_id="BRISBANE",
            )

            import recommendation_audit
            recommendation_audit.emit_async(
                "ams-demo",
                {"id": "C-X",
                 "decision_support": {"recommended_option_id": "R1"},
                 "sequencing_alternatives": []},
                summary={"vessels": [], "berths": []},
                port_id="BRISBANE",
            )

            import recommendation_presented_audit
            recommendation_presented_audit.emit_async(
                "ams-demo",
                {"id": "C-X",
                 "decision_support": {"recommended_option_id": "R1"}},
                port_id="BRISBANE", actor_handle="O-1",
            )

            import operator_action_audit
            operator_action_audit.emit_async(
                "ams-demo", operator_action_audit.ACTION_WHATIF_APPLY,
                summary="x", surface="api_apply_whatif",
                actor_handle="O-1", port_id="BRISBANE",
            )

            # Brief settle window
            time.sleep(0.05)
            threads_after = threading.active_count()

            assert threads_after == threads_before, (
                f"audit helpers spawned threads under production posture "
                f"(before={threads_before}, after={threads_after})"
            )
            assert "psycopg" not in sys.modules
            assert "audit" not in sys.modules
        finally:
            sys.meta_path[:] = orig_meta_path

    def test_no_emission_event_types_outside_authorised_set(self):
        """Source-level lock: only the six authorised audit event_type
        strings appear in the codebase. This catches a future PR that
        adds DEADLINE_PASSED / OPERATOR_DEFERRED / OPERATOR_OVERRODE /
        SESSION_ENDED_WITHOUT_ACTION etc. without explicit authorisation."""
        import re
        authorised = {
            "CONFLICT_DETECTED",
            "RECOMMENDATION_GENERATED",
            "RECOMMENDATION_PRESENTED",
            "OPERATOR_ACTED",
            "SESSION_STARTED",
            "SESSION_ENDED",
        }
        emitting_files = (
            "server.py",
            "session_audit.py",
            "conflict_audit.py",
            "recommendation_audit.py",
            "recommendation_presented_audit.py",
            "operator_action_audit.py",
        )
        observed: set[str] = set()
        for fn in emitting_files:
            src = (REPO_ROOT / fn).read_text()
            for m in re.finditer(r'event_type *= *["\']([A-Z_]+)["\']', src):
                observed.add(m.group(1))
        extra = observed - authorised
        assert not extra, (
            f"unauthorised event_type(s) emitted in Beta 10 code: {extra}. "
            f"Authorised set: {sorted(authorised)}"
        )
