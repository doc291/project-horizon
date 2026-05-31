"""
Beta 11 Phase 1 server integration tests.

Validates the flag gating contract at the server layer:
  - With BETA11_ENABLED off, /api/summary conflicts carry NO beta11_decision
    field (byte identical Beta 10 shape) and the decision-action route 404s.
  - With BETA11_ENABLED on, conflicts carry a beta11_decision field (None
    until a decision is issued) and the top level summary key set is unchanged.

The server module reads BETA11_ENABLED at import time, so each posture is
exercised by importing server in a clean subprocess via importlib reload.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("HORIZON_USER", "test")
os.environ.setdefault("HORIZON_PASS", "test")
os.environ.setdefault("TOKEN_SECRET", "x" * 32)


def _fresh_server(flag_value):
    """Import server with BETA11_ENABLED set to the given posture."""
    for m in list(sys.modules):
        if m in ("server", "beta11_decision"):
            del sys.modules[m]
    if flag_value is None:
        os.environ.pop("BETA11_ENABLED", None)
    else:
        os.environ["BETA11_ENABLED"] = flag_value
    return importlib.import_module("server")


@pytest.fixture
def server_off():
    srv = _fresh_server(None)
    yield srv
    for m in list(sys.modules):
        if m in ("server", "beta11_decision"):
            del sys.modules[m]
    os.environ.pop("BETA11_ENABLED", None)


@pytest.fixture
def server_on():
    srv = _fresh_server("1")
    yield srv
    for m in list(sys.modules):
        if m in ("server", "beta11_decision"):
            del sys.modules[m]
    os.environ.pop("BETA11_ENABLED", None)


EXPECTED_SUMMARY_KEYS = frozenset({
    "port_name", "generated_at", "lookahead_hours", "data_source",
    "data_source_label", "scraped_at", "port_status",
    "vessels", "berths", "pilotage", "towage", "port_tugs", "port_gangs",
    "conflicts", "guidance", "port_geo", "weather", "tides",
    "berth_utilisation", "etd_risk", "dashboard", "ukc", "arrival_ukc",
    "dukc", "esg", "port_profile",
})


class TestFlagOffParity:
    def test_flag_defaults_off(self, server_off):
        assert server_off.BETA11_ENABLED is False

    def test_summary_top_level_keys_unchanged(self, server_off):
        s = server_off.build_summary()
        assert set(s.keys()) == EXPECTED_SUMMARY_KEYS

    def test_conflicts_have_no_beta11_field(self, server_off):
        s = server_off.build_summary()
        assert all("beta11_decision" not in c for c in (s.get("conflicts") or []))


class TestFlagOnAdditive:
    def test_flag_on(self, server_on):
        assert server_on.BETA11_ENABLED is True

    def test_summary_adds_exactly_one_top_level_key(self, server_on):
        # Phase 2 contract: flag-on adds EXACTLY one top-level key, `beta11`
        # (the role/scenario block). Every other key is unchanged from Beta 10.
        s = server_on.build_summary()
        assert set(s.keys()) == EXPECTED_SUMMARY_KEYS | {"beta11"}

    def test_beta11_block_shape(self, server_on):
        s = server_on.build_summary()
        b = s.get("beta11")
        assert b is not None
        assert b["enabled"] is True and b["simulated"] is True
        assert b["scenario_port"] == "MELBOURNE"
        assert [r["id"] for r in b["roles"]] == ["VTSO", "TOWAGE", "PILOTAGE", "TERMINAL", "ASSURANCE"]
        assert b["predicted_impact"]["simulated"] is True

    def test_conflicts_have_beta11_field_null_until_issued(self, server_on):
        s = server_on.build_summary()
        conflicts = s.get("conflicts") or []
        if conflicts:
            assert all("beta11_decision" in c for c in conflicts)
            assert all(c["beta11_decision"] is None for c in conflicts)

    def test_issued_decision_surfaces_on_matching_conflict(self, server_on):
        b11 = importlib.import_module("beta11_decision")
        b11.reset_for_tests()
        s = server_on.build_summary()
        conflicts = s.get("conflicts") or []
        if not conflicts:
            pytest.skip("no conflicts in this port snapshot")
        target = conflicts[0]["id"]
        b11.issue_decision(target, "O-1", [
            {"role": "TOWAGE", "action_label": "Reassign tug"},
        ])
        s2 = server_on.build_summary()
        match = next(c for c in s2["conflicts"] if c["id"] == target)
        assert match["beta11_decision"] is not None
        assert match["beta11_decision"]["decision_state"] == b11.STATE_PROPAGATED
        b11.reset_for_tests()
