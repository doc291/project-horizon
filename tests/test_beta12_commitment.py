"""
Beta 12 commitment-feasibility + consequence-projection unit tests.

Covers: commitment selection (only berth/time/window vessels), the four
feasibility states, option-expiry from configured planning thresholds,
neutral consequence projection, and — critically — that the surface never
emits recommendation / ranking language. Plus the flag-off parity guarantee.
"""
from datetime import datetime, timedelta, timezone

import beta12_commitment as b12

UTC = timezone.utc
NOW = datetime(2026, 7, 6, 12, 0, 0, tzinfo=UTC)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _opt(oid, strategy, feasibility, delay_mins=0, saving_h=0):
    return {
        "id": oid, "strategy": strategy, "label": f"{strategy} lever",
        "description": "desc", "affected_vessels": ["ALPHA"],
        "feasibility": feasibility, "delay_mins": delay_mins,
        "time_saving_hours": saving_h, "cascade_impact": "downstream note",
        "cost_label": "~$3,000",
    }


def _conflict(cid, ctype, severity, vessel_ids, conflict_dt, options=None,
              signal="CONFLICT"):
    return {
        "id": cid, "conflict_type": ctype, "signal_type": signal,
        "severity": severity, "vessel_ids": vessel_ids,
        "vessel_names": ["ALPHA"], "berth_id": "B1", "berth_name": "Berth 1",
        "conflict_time": _iso(conflict_dt),
        "description": f"{ctype} description",
        "decision_support": ({"options": options} if options is not None else None),
        "provenance": "simulated",
    }


def _vessels():
    return [
        {"id": "VA", "name": "ALPHA", "status": "confirmed", "berth_id": "B1",
         "eta": _iso(NOW + timedelta(hours=4)), "etd": None, "source": "sim", "loa": 200},
        {"id": "VB", "name": "BRAVO", "status": "scheduled", "berth_id": "B2",
         "eta": _iso(NOW + timedelta(hours=6)), "etd": None, "source": "sim", "loa": 180},
        {"id": "VC", "name": "CHARLIE", "status": "berthed", "berth_id": "B3",
         "eta": None, "etd": _iso(NOW + timedelta(hours=8)), "source": "sim", "loa": 150},
        {"id": "VD", "name": "DELTA", "status": "confirmed", "berth_id": "B4",
         "eta": _iso(NOW + timedelta(minutes=30)), "etd": None, "source": "sim", "loa": 210},
        {"id": "VE", "name": "ECHO", "status": "confirmed", "berth_id": "B5",
         "eta": _iso(NOW + timedelta(hours=5)), "etd": None, "source": "sim", "loa": 190},
        # Not a commitment: no berth relationship to test.
        {"id": "VF", "name": "FOXTROT", "status": "scheduled", "berth_id": None,
         "eta": _iso(NOW + timedelta(hours=3)), "etd": None, "source": "sim", "loa": 170},
    ]


def _berths():
    return [{"id": f"B{i}", "name": f"Berth {i}"} for i in range(1, 6)]


def _conflicts():
    return [
        # ALPHA — critical berth overlap, options still available (4h out) -> critical
        _conflict("C-A", "berth_overlap", "critical", ["VA"], NOW + timedelta(hours=4),
                  options=[_opt("o1", "delay_arrival", "high", delay_mins=90),
                           _opt("o2", "reassign_berth", "medium")]),
        # BRAVO — ETA variance advisory -> degrading
        _conflict("C-B", "eta_variance", "medium", ["VB"], NOW + timedelta(hours=6),
                  options=None, signal="ADVISORY"),
        # DELTA — critical overlap but options all expired (30 min out) -> broken
        _conflict("C-D", "berth_overlap", "critical", ["VD"], NOW + timedelta(minutes=30),
                  options=[_opt("o1", "delay_arrival", "high", delay_mins=90),
                           _opt("o2", "reassign_berth", "medium")]),
        # ECHO — bridge absolute limit -> broken
        _conflict("C-E", "bridge_restriction", "critical", ["VE"], NOW + timedelta(hours=5),
                  options=None),
    ]


# ── Option expiry (Q4) ─────────────────────────────────────────────────────────
def test_option_expiry_uses_configured_lead_and_flags_assumption():
    ct = NOW + timedelta(hours=4)  # 240 min out
    d = b12.compute_option_expiry("delay_arrival", ct, NOW)   # lead 90
    assert d["expires_in_mins"] == 150 and d["expired"] is False
    assert d["assumption"] is True and d["basis"] == b12.ASSUMPTION_NOTE
    r = b12.compute_option_expiry("advance_departure", ct, NOW)  # lead 180
    assert r["expires_in_mins"] == 60
    imminent = b12.compute_option_expiry("reassign_berth", NOW + timedelta(minutes=30), NOW)
    assert imminent["expired"] is True and imminent["expires_in_mins"] == 0


# ── Commitment selection + states ──────────────────────────────────────────────
def test_only_berth_time_window_vessels_are_commitments():
    cms = b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)
    ids = {c["commitment_id"] for c in cms}
    assert "VF" not in ids                      # no berth relationship -> excluded
    assert {"VA", "VB", "VC", "VD", "VE"} <= ids
    charlie = next(c for c in cms if c["commitment_id"] == "VC")
    assert charlie["movement"] == "departure"   # berthed + etd = expected release


def test_four_feasibility_states():
    cms = {c["commitment_id"]: c for c in
           b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)}
    assert cms["VC"]["feasibility_state"] == "healthy"     # no conflict
    assert cms["VB"]["feasibility_state"] == "degrading"   # advisory
    assert cms["VA"]["feasibility_state"] == "critical"    # critical + options live
    assert cms["VD"]["feasibility_state"] == "broken"      # options all expired
    assert cms["VE"]["feasibility_state"] == "broken"      # bridge absolute limit


# ── Consequence projection (Q3) ────────────────────────────────────────────────
def test_project_consequence_reshapes_shadow_neutrally():
    def stub_shadow(cid, adjustments, vessels, conflicts):
        return {"resolved": [{"id": "C-A", "description": "berth overlap description",
                              "severity": "critical"}],
                "new_conflicts": [], "cost_delta": -4000,
                "new_recommendation": "Proceed with adjusted schedule",  # must be ignored
                "new_reasoning": "..."}
    opt = _opt("o1", "delay_arrival", "high", delay_mins=90)
    out = b12.project_consequence(_conflicts()[0], opt, _vessels(), _conflicts(), stub_shadow)
    joined = " ".join(out["summary_lines"]).lower()
    assert "relieves" in joined and "net saving" in joined
    assert "proceed" not in joined and "recommend" not in joined  # no rec wording leaked
    assert out["resolved_ids"] == ["C-A"]


# ── Cards answer four questions, never recommend ───────────────────────────────
def test_cards_answer_four_questions_without_recommendation():
    def stub_shadow(cid, adjustments, vessels, conflicts):
        return {"resolved": [], "new_conflicts": [], "cost_delta": 0}
    cms = b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)
    cards = b12.build_decision_support_cards(cms, _conflicts(), _vessels(), NOW, stub_shadow)
    assert cards, "expected cards for at-risk commitments"
    # Healthy commitments never produce a card.
    assert all(c["feasibility_state"] != "healthy" for c in cards)
    for card in cards:
        assert card["why_infeasible"]           # Q1
        assert card["do_nothing"]               # Q2
        assert "recommended" not in card        # no recommendation on the card
        for o in card["options"]:
            assert "consequence" in o           # Q3
            assert "expiry" in o                # Q4
            assert "recommended" not in o       # neutral option
    # No ranking / "best" / star language anywhere in the serialised surface.
    blob = repr(cards).lower()
    for banned in ("best option", "★", "ranking", "recommended_option"):
        assert banned not in blob
    # The only permitted use of "recommend" is the explicit non-recommendation
    # disclaimer; strip it and assert nothing else recommends anything.
    assert "recommend" not in blob.replace("no option is recommended", "")


def test_block_shape_and_counts():
    def stub_shadow(cid, adjustments, vessels, conflicts):
        return {"resolved": [], "new_conflicts": [], "cost_delta": 0}
    block = b12.build_beta12_block(_vessels(), _berths(), _conflicts(), NOW, stub_shadow)
    assert set(block["state_counts"]) == {"healthy", "degrading", "critical", "broken"}
    assert block["state_counts"]["broken"] == 2
    assert block["planning_thresholds_mins"] == b12.OPTION_LEAD_TIME_MINS
    assert block["assumption_note"] == b12.ASSUMPTION_NOTE


# ── Flag-off parity ────────────────────────────────────────────────────────────
def test_flag_off_emits_no_beta12_block():
    import server
    assert server.BETA11_ENABLED is False        # default in the test environment
    assert server._beta12_summary_block(_vessels(), _berths(), _conflicts(), NOW) == {}
