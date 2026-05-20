"""
Project Horizon — Darwin demo-card regression.

Locks in the Darwin demo-simulation-lock behaviour:

  1. Darwin's port profile carries the per-port flag
     ``demo_force_simulation: True``.
  2. No other port profile carries the flag (it defaults to falsy /
     absent for Brisbane, Melbourne, Geelong).
  3. Under simulation, Darwin's deterministic vessel / berth slot
     definitions produce at least one conflict / decision card via
     ``detect_conflicts`` — so the Darwin Ports demo reliably shows
     at least one decision card without any live-AIS dependency.

This test exercises ``detect_conflicts`` directly against the
simulated vessel / berth / pilotage / towage builders. It does not
touch the network, AISStream, MST AIS, BOM, QShips, or the database.
It deliberately runs with ``DATABASE_URL`` unset to mirror Beta 10
production posture, matching ``tests/test_beta10_regression.py``.

Rationale and design decision: see
``HORIZON_V1_M2_IMPLEMENTATION_PLAN_v0.1.md`` is *not* the source — this
is a Beta 10 demo-bias fix scoped outside the V1 milestone work. The
fix is intentionally small: a single per-port profile flag plus two
guards in ``build_summary``'s live-vessel ladder.
"""

from __future__ import annotations

import os

# Mirror the regression-gate posture: DATABASE_URL must not be set when
# this module imports server.py.
os.environ.pop("DATABASE_URL", None)

import port_profiles  # noqa: E402  (import after env scrubbing)
import server  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_darwin_world():
    """
    Build Darwin's simulated world (berths, vessels, pilotage, towage)
    by temporarily swapping ``server._ACTIVE_PORT_ID`` /
    ``server._PORT_PROFILE`` under the existing profile lock. Restores
    the original port before returning. No network, no DB.
    """
    with server._profile_lock:
        prior_id      = server._ACTIVE_PORT_ID
        prior_profile = server._PORT_PROFILE
        server._ACTIVE_PORT_ID = "DARWIN"
        server._PORT_PROFILE   = port_profiles.get_profile("DARWIN")
    try:
        now      = server.utcnow()
        berths   = server.make_berths(now)
        vessels  = server.make_vessels(now)
        pilotage = server.make_pilotage(vessels, now)
        towage   = server.make_towage(vessels, now)
        return berths, vessels, pilotage, towage, now
    finally:
        with server._profile_lock:
            server._ACTIVE_PORT_ID = prior_id
            server._PORT_PROFILE   = prior_profile


# ── Tests ────────────────────────────────────────────────────────────────────

def test_darwin_profile_carries_demo_force_simulation_flag():
    """Darwin's profile must declare the demo-simulation lock."""
    darwin = port_profiles.get_profile("DARWIN")
    assert darwin.get("demo_force_simulation") is True, (
        "Darwin profile must declare demo_force_simulation=True so "
        "build_summary skips AISStream and MST live vessel paths and "
        "the deterministic simulation drives demo cards."
    )


def test_other_ports_do_not_carry_demo_force_simulation_flag():
    """
    The flag is intentionally per-port. Brisbane, Melbourne, and Geelong
    must NOT carry it — they continue to use live AIS / MST / QShips
    data sources exactly as before.
    """
    for port_id in ("BRISBANE", "MELBOURNE", "GEELONG"):
        prof = port_profiles.get_profile(port_id)
        assert not prof.get("demo_force_simulation"), (
            f"{port_id} must NOT carry demo_force_simulation — that flag "
            f"is Darwin-only by design."
        )


def test_darwin_simulation_produces_at_least_one_conflict():
    """
    Under simulation, Darwin's deterministic vessel / berth slot table
    must produce at least one conflict so the demo always shows a card.

    The expected conflicts under the current slot table are:
      - eta_variance ADVISORY for V007 (status='at_risk')
      - berth_overlap CONFLICT for V005 / V007 at B04
    Either of these (or any future deterministic equivalent) is
    acceptable; the test asserts the minimum invariant.
    """
    berths, vessels, pilotage, towage, now = _build_darwin_world()
    conflicts = server.detect_conflicts(
        vessels, berths, pilotage, towage, now, is_live=False
    )
    assert len(conflicts) >= 1, (
        "Darwin simulation must produce at least one conflict / decision "
        "card. Got zero. The deterministic slot table (V007 at_risk and/or "
        "V005/V007 B04 overlap) has been disturbed."
    )


def test_darwin_simulation_at_risk_card_present():
    """
    Belt-and-braces: V007's hardcoded ``status='at_risk'`` should
    deterministically yield an ETA-variance ADVISORY card for Darwin.
    """
    berths, vessels, pilotage, towage, now = _build_darwin_world()
    conflicts = server.detect_conflicts(
        vessels, berths, pilotage, towage, now, is_live=False
    )
    types = {c.get("conflict_type") for c in conflicts}
    assert "eta_variance" in types or "berth_overlap" in types, (
        "Expected at least one eta_variance ADVISORY (from V007 at_risk) "
        "or berth_overlap CONFLICT (from V005/V007 at B04) in Darwin "
        "simulation output."
    )
