"""
Beta 12 commitment-feasibility + consequence-projection unit tests
(Preview-Readiness pass).

Covers: commitment selection (only berth/time/window vessels), the THREE
operator states (On Track / Watch / Act Now), advisory-only card suppression,
option-expiry from configured planning thresholds, cost-free + feasibility-free
neutral consequence projection, evidence freshness, departure lower-confidence
note, arrival-leads ordering, operator plan correction, and the no-recommendation
guarantee. Plus flag-off parity.
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
        {"id": "VG", "name": "GOLF", "status": "confirmed", "berth_id": "B6",
         "eta": _iso(NOW + timedelta(hours=5)), "etd": None, "source": "sim", "loa": 175},
        # Not a commitment: no berth relationship to test.
        {"id": "VF", "name": "FOXTROT", "status": "scheduled", "berth_id": None,
         "eta": _iso(NOW + timedelta(hours=3)), "etd": None, "source": "sim", "loa": 170},
    ]


def _berths():
    return [{"id": f"B{i}", "name": f"Berth {i}"} for i in range(1, 7)]


def _conflicts():
    return [
        # ALPHA — critical berth overlap, options live (4h out) -> act_now, card
        _conflict("C-A", "berth_overlap", "critical", ["VA"], NOW + timedelta(hours=4),
                  options=[_opt("o1", "delay_arrival", "high", delay_mins=90),
                           _opt("o2", "reassign_berth", "medium")]),
        # BRAVO — advisory ETA variance -> watch, NO card
        _conflict("C-B", "eta_variance", "medium", ["VB"], NOW + timedelta(hours=6),
                  options=None, signal="ADVISORY"),
        # DELTA — critical overlap, options all expired (30 min out) -> act_now, card
        _conflict("C-D", "berth_overlap", "critical", ["VD"], NOW + timedelta(minutes=30),
                  options=[_opt("o1", "delay_arrival", "high", delay_mins=90),
                           _opt("o2", "reassign_berth", "medium")]),
        # ECHO — bridge absolute limit -> act_now, card (no options)
        _conflict("C-E", "bridge_restriction", "critical", ["VE"], NOW + timedelta(hours=5),
                  options=None),
        # GOLF — medium towage CONFLICT with live option -> watch, card
        _conflict("C-G", "towage_resource", "medium", ["VG"], NOW + timedelta(hours=5),
                  options=[_opt("o1", "delay_arrival", "high", delay_mins=60)]),
    ]


# ── Option expiry (Q4) ─────────────────────────────────────────────────────────
def test_option_expiry_uses_configured_lead_and_flags_assumption():
    ct = NOW + timedelta(hours=4)
    d = b12.compute_option_expiry("delay_arrival", ct, NOW)
    assert d["expires_in_mins"] == 150 and d["expired"] is False
    assert d["assumption"] is True and d["basis"] == b12.ASSUMPTION_NOTE
    imminent = b12.compute_option_expiry("reassign_berth", NOW + timedelta(minutes=30), NOW)
    assert imminent["expired"] is True and imminent["expires_in_mins"] == 0


# ── Commitment selection + three operator states ───────────────────────────────
def test_only_berth_time_window_vessels_are_commitments():
    cms = b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)
    ids = {c["commitment_id"] for c in cms}
    assert "VF" not in ids
    charlie = next(c for c in cms if c["commitment_id"] == "VC")
    assert charlie["movement"] == "departure"


def test_three_operator_states_no_four_level_vocab():
    cms = {c["commitment_id"]: c for c in
           b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)}
    assert cms["VC"]["feasibility_state"] == "on_track"
    assert cms["VB"]["feasibility_state"] == "watch"   # advisory still moves state
    assert cms["VG"]["feasibility_state"] == "watch"
    assert cms["VA"]["feasibility_state"] == "act_now"
    assert cms["VD"]["feasibility_state"] == "act_now"  # was "broken"
    assert cms["VE"]["feasibility_state"] == "act_now"  # bridge absolute
    for c in cms.values():
        assert c["feasibility_state"] in ("on_track", "watch", "act_now")
        assert c["state_label"] in ("On Track", "Watch", "Act Now")


def test_departure_carries_lower_confidence_note():
    cms = {c["commitment_id"]: c for c in
           b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)}
    assert cms["VC"]["confidence_note"] == b12.DEPARTURE_CONFIDENCE_NOTE
    assert cms["VA"]["confidence_note"] is None  # arrivals have no such note


# ── Advisory-only cards suppressed ─────────────────────────────────────────────
def test_advisory_only_commitment_gets_no_card():
    def stub(cid, adj, v, c): return {"resolved": [], "new_conflicts": []}
    cms = b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)
    cards = b12.build_decision_support_cards(cms, _conflicts(), _vessels(), NOW, stub)
    ids = {c["commitment_id"] for c in cards}
    assert "VB" not in ids                 # advisory-only -> no card
    assert "VC" not in ids                 # on track -> no card
    assert ids == {"VA", "VD", "VE", "VG"}  # operator-relevant CONFLICT threats only


# ── Consequence projection: neutral, NO cost, NO feasibility ───────────────────
def test_project_consequence_is_cost_free_and_neutral():
    def stub(cid, adjustments, vessels, conflicts):
        return {"resolved": [{"id": "C-A", "description": "berth overlap description",
                              "severity": "critical"}],
                "new_conflicts": [], "cost_delta": -4000,
                "new_recommendation": "Proceed with adjusted schedule",
                "new_reasoning": "..."}
    out = b12.project_consequence(_conflicts()[0],
                                  _opt("o1", "delay_arrival", "high", delay_mins=90),
                                  _vessels(), _conflicts(), stub)
    joined = " ".join(out["summary_lines"]).lower()
    assert "relieves" in joined
    for banned in ("a$", "cost", "saving", "$", "proceed", "recommend"):
        assert banned not in joined
    assert "cost_delta" not in out          # cost not surfaced at all


def test_cards_omit_cost_and_feasibility_and_never_recommend():
    def stub(cid, adj, v, c): return {"resolved": [], "new_conflicts": []}
    cms = b12.build_commitments(_vessels(), _berths(), _conflicts(), NOW)
    cards = b12.build_decision_support_cards(cms, _conflicts(), _vessels(), NOW, stub)
    for card in cards:
        assert "recommended" not in card
        for o in card["options"]:
            assert "feasibility" not in o          # no feasibility chip data
            assert "direct_cost_label" not in o    # no cost data
            assert "cost_label" not in o
            assert "expiry" in o and "consequence" in o
    blob = repr(cards).lower()
    # (per-option feasibility chip already asserted absent above; "feasibility"
    # as a substring legitimately appears in the "feasibility_state" field name.)
    for banned in ("best option", "optimal", "preferred", "★", "ranking",
                   "cost_label", "a$", "cost_delta"):
        assert banned not in blob
    assert "recommend" not in blob.replace("no option is recommended", "")


# ── Block: counts, ordering, freshness ─────────────────────────────────────────
def test_block_counts_ordering_and_freshness():
    def stub(cid, adj, v, c): return {"resolved": [], "new_conflicts": []}
    fresh = {"assembled_at": _iso(NOW), "ais_as_of": "2026-06-22T08:00:04Z",
             "schedule_as_of": "2026-06-22T08:00:04Z", "weather_source": "live",
             "weather_as_of": None, "tide_source": "bom", "tide_as_of": None}
    block = b12.build_beta12_block(_vessels(), _berths(), _conflicts(), NOW, stub, freshness=fresh)
    assert set(block["state_counts"]) == {"on_track", "watch", "act_now"}
    assert block["state_counts"] == {"on_track": 1, "watch": 2, "act_now": 3}
    # Arrival story leads: no departure appears before an arrival.
    seen_departure = False
    for cm in block["commitments"]:
        if cm["movement"] == "departure":
            seen_departure = True
        elif seen_departure:
            raise AssertionError("arrival appeared after a departure — ordering wrong")
    assert block["evidence_freshness"]["schedule_as_of"] == "2026-06-22T08:00:04Z"
    assert block["state_labels"] == b12.OP_LABEL


# ── Operator plan correction (retime) ──────────────────────────────────────────
def test_apply_operator_plan_is_pure_and_retimes_one_commitment():
    vessels = _vessels()
    new_eta = _iso(NOW + timedelta(hours=9))
    adjusted = b12.apply_operator_plan(vessels, "VD", new_eta=new_eta, new_berth="B1")
    orig = next(v for v in vessels if v["id"] == "VD")
    moved = next(v for v in adjusted if v["id"] == "VD")
    assert orig["eta"] != new_eta and orig["berth_id"] == "B4"   # original untouched
    assert moved["eta"] == new_eta and moved["berth_id"] == "B1"  # copy retimed
    # Retimed roster re-projects: DELTA's imminent overlap should relieve.
    def stub(cid, adj, v, c): return {"resolved": [], "new_conflicts": []}
    block = b12.build_beta12_block(adjusted, _berths(), _conflicts(), NOW, stub, working_plan=True,
                                   plan_label="Operator test plan")
    assert block["working_plan"] is True and block["plan_label"] == "Operator test plan"


# ── Flag-off parity ────────────────────────────────────────────────────────────
def test_flag_off_emits_no_beta12_block():
    import server
    assert server.BETA11_ENABLED is False
    assert server._beta12_summary_block(_vessels(), _berths(), _conflicts(), NOW) == {}
