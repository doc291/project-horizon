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
    V007's hardcoded ``status='at_risk'`` should deterministically
    yield an ETA-variance ADVISORY card for Darwin. This is the
    Guidance / Signals invariant — separate from the Decisions panel
    invariant below.
    """
    berths, vessels, pilotage, towage, now = _build_darwin_world()
    conflicts = server.detect_conflicts(
        vessels, berths, pilotage, towage, now, is_live=False
    )
    types = {c.get("conflict_type") for c in conflicts}
    assert "eta_variance" in types, (
        "Expected an eta_variance ADVISORY from V007 (status='at_risk') "
        "in Darwin simulation output. Note: this card alone does NOT "
        "populate the Decisions panel — that requires a CONFLICT-class "
        "card with decision_support (see "
        "test_darwin_yields_decisions_panel_eligible_card)."
    )


# ── Decisions-panel invariant (PR #56) ───────────────────────────────────────
#
# The Beta 10 page-bundle filter for the right-side Decisions panel is
# (server.py:3542):
#
#   const cs = (d.conflicts || [])
#       .filter(c => c.signal_type === 'CONFLICT' && c.decision_support);
#
# Both predicates must be true. ``eta_variance`` is an ADVISORY with no
# ``decision_support`` — it does not make the panel non-empty. Only
# ``berth_overlap`` populates ``decision_support`` (and is the only
# generator that emits ``signal_type == 'CONFLICT'`` in detect_conflicts).
#
# Darwin's roster contains mostly small offshore-supply vessels (<100 m),
# and the berth_overlap generator requires ≥100 m vessels. The slot-pin
# profile field ``sim_pinned_vessels`` is what guarantees V005 (slot 4)
# and V007 (slot 6) — both at B04 — are populated with ≥100 m roster
# entries so the B04 overlap deterministically fires. The tests below
# lock that invariant in.


def test_darwin_profile_pins_v005_and_v007_to_large_roster_vessels():
    """
    Darwin's profile must pin slots 4 and 6 (V005 and V007 at B04) to
    specific ≥100 m roster vessels. This is what makes the Decisions
    panel reliably non-empty for the demo.
    """
    darwin  = port_profiles.get_profile("DARWIN")
    pinned  = darwin.get("sim_pinned_vessels") or {}
    assert pinned, (
        "Darwin profile must declare sim_pinned_vessels for slots 4 and "
        "6 (V005 / V007 at B04) so the B04 berth_overlap conflict "
        "always fires and the Decisions panel is non-empty."
    )
    # Slots 4 and 6 must be pinned (key may be int or str depending on
    # how the profile is loaded — accept both).
    keys = {int(k) for k in pinned.keys()}
    assert 4 in keys and 6 in keys, (
        "Darwin must pin both slot 4 (V005) and slot 6 (V007); "
        f"got pinned slots {sorted(keys)}."
    )
    # Pinned vessel names must exist in the Darwin roster and be ≥100 m.
    roster   = server._load_vessel_roster("DARWIN")
    by_name  = {rv["name"]: rv for rv in roster}
    for slot_idx, vessel_name in pinned.items():
        rv = by_name.get(vessel_name)
        assert rv is not None, (
            f"Pinned vessel {vessel_name!r} (slot {slot_idx}) is not in "
            f"darwin_roster.json — pinning will be a silent no-op."
        )
        assert rv["loa"] >= 100, (
            f"Pinned vessel {vessel_name!r} has LOA {rv['loa']} m — "
            f"below the 100 m berth_overlap threshold. Pin will not "
            f"produce a Decisions-panel-eligible card."
        )


def test_other_ports_do_not_carry_sim_pinned_vessels():
    """
    Slot pinning is intentionally per-port. Brisbane / Melbourne /
    Geelong must NOT carry it — they continue to use the unmodified
    daily shuffle.
    """
    for port_id in ("BRISBANE", "MELBOURNE", "GEELONG"):
        prof = port_profiles.get_profile(port_id)
        assert not prof.get("sim_pinned_vessels"), (
            f"{port_id} must NOT carry sim_pinned_vessels — that field "
            f"is Darwin-only by design."
        )


def test_darwin_yields_decisions_panel_eligible_card():
    """
    The Decisions panel invariant: Darwin simulation must produce at
    least one conflict satisfying the Beta 10 page-bundle filter
    (signal_type == 'CONFLICT' AND decision_support truthy).

    This is the test that PR #55 was missing — it cannot pass on an
    eta_variance ADVISORY alone, because eta_variance is ADVISORY and
    carries no decision_support.
    """
    berths, vessels, pilotage, towage, now = _build_darwin_world()
    conflicts = server.detect_conflicts(
        vessels, berths, pilotage, towage, now, is_live=False
    )
    eligible = [
        c for c in conflicts
        if c.get("signal_type") == "CONFLICT" and c.get("decision_support")
    ]
    assert len(eligible) >= 1, (
        "Darwin simulation must produce at least one Decisions-panel-"
        "eligible conflict (signal_type=='CONFLICT' AND "
        "decision_support truthy). Got: "
        f"{[(c.get('conflict_type'), c.get('signal_type'), bool(c.get('decision_support'))) for c in conflicts]}"
    )
    # And the decision_support payload must carry the 5 expected keys
    # that the Beta 10 page and the PDF brief consume.
    ds = eligible[0]["decision_support"]
    expected_keys = {
        "recommended_option_id",
        "recommended_reasoning",
        "confidence",
        "decision_deadline",
        "options",
    }
    missing = expected_keys - set(ds.keys())
    assert not missing, (
        f"decision_support payload missing expected keys: {missing}"
    )


def test_darwin_yields_b04_berth_overlap_card():
    """
    Belt-and-braces: the Decisions-panel-eligible card should be the
    deterministic B04 berth_overlap between the two pinned vessels.
    """
    berths, vessels, pilotage, towage, now = _build_darwin_world()
    conflicts = server.detect_conflicts(
        vessels, berths, pilotage, towage, now, is_live=False
    )
    b04_overlaps = [
        c for c in conflicts
        if c.get("conflict_type") == "berth_overlap"
        and c.get("berth_id") == "B04"
    ]
    assert len(b04_overlaps) >= 1, (
        "Expected at least one berth_overlap conflict at B04 (V005 / "
        "V007). Got conflict types: "
        f"{[c.get('conflict_type') for c in conflicts]}"
    )
    # And it must be the populated CONFLICT shape, not a stripped one.
    c = b04_overlaps[0]
    assert c["signal_type"] == "CONFLICT"
    assert c.get("decision_support") is not None
