"""
tests/test_aisstream_scoping.py — Beta 11 Slice 6A.

AISStream three-scope classification (approach / port_box / berth_box),
inclusion rule (accepted = in_port_area AND commercial; NOT gated on
stationary), status labelling, get_status funnel counts, and the health-advisory
wording change. No Decision Card / authority / conflict logic involved.
"""

import os

import aisstream_scraper as ais

# Synthetic nested cfg for precise control: berth ⊂ port ⊂ approach
CFG = {
    "berth_box": [0.0, 0.0, 1.0, 1.0],
    "port_box":  [-1.0, -1.0, 2.0, 2.0],
    "approach":  [-5.0, -5.0, 5.0, 5.0],
}

COMMERCIAL_TYPE = 70   # cargo → commercial
TUG_TYPE = 52          # port tender → not commercial
MOVING = 5.0           # sog ≥ 1.0
STILL = 0.0            # sog < 1.0


# ── 1. Box membership for each scope ─────────────────────────────────────────

def test_box_membership():
    assert ais._in_box(0.5, 0.5, CFG["berth_box"]) is True
    assert ais._in_box(1.5, 1.5, CFG["port_box"]) is True
    assert ais._in_box(1.5, 1.5, CFG["berth_box"]) is False     # in port, not berth
    assert ais._in_box(3.0, 3.0, CFG["approach"]) is True
    assert ais._in_box(3.0, 3.0, CFG["port_box"]) is False      # in approach, not port
    assert ais._in_box(9.0, 9.0, CFG["approach"]) is False


# ── 2–4. Status labels ───────────────────────────────────────────────────────

def test_berthed_in_berth_and_stationary():
    c = ais._classify(CFG, 0.5, 0.5, STILL, COMMERCIAL_TYPE, "MV TEST")
    assert c["status"] == "berthed"
    assert c["accepted"] is True


def test_anchored_in_port_stationary_outside_berth():
    c = ais._classify(CFG, 1.5, 1.5, STILL, COMMERCIAL_TYPE, "MV TEST")
    assert c["in_port_area"] is True and c["in_berth_area"] is False
    assert c["status"] == "anchored"
    assert c["accepted"] is True


def test_underway_in_port_moving():
    c = ais._classify(CFG, 1.5, 1.5, MOVING, COMMERCIAL_TYPE, "MV TEST")
    assert c["status"] == "underway"
    assert c["accepted"] is True       # accepted even though moving


# ── 5. Approach, not accepted ────────────────────────────────────────────────

def test_approach_not_accepted():
    c = ais._classify(CFG, 3.0, 3.0, MOVING, COMMERCIAL_TYPE, "MV TEST")
    assert c["in_approach"] is True and c["in_port_area"] is False
    assert c["status"] == "approach"
    assert c["accepted"] is False


# ── 6. Non-commercial in port not accepted ───────────────────────────────────

def test_tug_in_port_not_accepted():
    c = ais._classify(CFG, 1.5, 1.5, STILL, TUG_TYPE, "TUG ONE")
    assert c["commercial"] is False
    assert c["accepted"] is False      # excluded despite being in the port area


def test_named_harbour_craft_not_accepted():
    c = ais._classify(CFG, 1.5, 1.5, MOVING, COMMERCIAL_TYPE, "SVITZER MARLIN")
    assert c["commercial"] is False
    assert c["accepted"] is False


# ── 7. Commercial in port accepted even when moving ──────────────────────────

def test_commercial_moving_in_port_accepted():
    c = ais._classify(CFG, 1.5, 1.5, MOVING, COMMERCIAL_TYPE, "MV CARGO STAR")
    assert c["accepted"] is True
    assert c["stationary"] is False


# ── real port boxes are nested berth ⊂ port ⊂ approach ───────────────────────

def test_real_port_boxes_are_nested():
    for unloco, cfg in ais._PORT_BOXES.items():
        b, p, a = cfg["berth_box"], cfg["port_box"], cfg["approach"]
        # berth ⊂ port
        assert p[0] <= b[0] and p[1] <= b[1] and p[2] >= b[2] and p[3] >= b[3], f"{unloco} berth⊄port"
        # port ⊂ approach
        assert a[0] <= p[0] and a[1] <= p[1] and a[2] >= p[2] and a[3] >= p[3], f"{unloco} port⊄approach"


# ── 8. Funnel counts from get_status ─────────────────────────────────────────

def test_get_status_funnel_counts():
    # Inject 4 vessels around the real AUMEL boxes, then read the funnel.
    injected = {
        "T1": {"pos": (-37.83, 144.92, STILL),  "type": COMMERCIAL_TYPE, "name": "MV BERTHED"},   # berth, still, commercial → berthed, accepted
        "T2": {"pos": (-37.79, 144.98, MOVING), "type": COMMERCIAL_TYPE, "name": "MV UNDERWAY"},  # port not berth, moving → underway, accepted
        "T3": {"pos": (-38.20, 144.70, MOVING), "type": COMMERCIAL_TYPE, "name": "MV INBOUND"},   # approach only → approach, not accepted
        "T4": {"pos": (-37.79, 144.87, MOVING), "type": TUG_TYPE,        "name": "TUG ONE"},      # port, tug → not accepted
    }
    with ais._lock:
        for m, v in injected.items():
            lat, lon, sog = v["pos"]
            ais._positions[m] = {"lat": lat, "lon": lon, "sog": sog, "heading": 0, "ts": 0}
            ais._static_data[m] = {"type_code": v["type"], "name": v["name"]}
    try:
        f = ais.get_status()["funnel"]["AUMEL"]
        assert f["in_approach"] == 4
        assert f["in_port_area"] == 3      # T1, T2, T4
        assert f["in_berth_area"] == 1     # T1
        assert f["commercial"] == 3        # T1, T2, T3 (T4 is a tug)
        assert f["accepted"] == 2          # T1, T2 (in port AND commercial)
        assert f["stationary"] == 1        # T1
        assert f["status"]["berthed"] == 1
        # status is location/motion-based (independent of commercial): both the
        # commercial T2 and the tug T4 are in the port area and moving → underway.
        assert f["status"]["underway"] == 2
        assert f["status"]["approach"] == 1
    finally:
        with ais._lock:
            for m in injected:
                ais._positions.pop(m, None)
                ais._static_data.pop(m, None)


def test_get_status_exposes_funnel_for_all_ports():
    st = ais.get_status()
    assert "funnel" in st
    for unloco in ais._PORT_BOXES:
        assert unloco in st["funnel"]
        for k in ("in_approach", "in_port_area", "in_berth_area", "stationary", "commercial", "accepted"):
            assert k in st["funnel"][unloco]


# ── 9. Health advisory no longer says "coverage gap" ─────────────────────────

def test_health_advisory_wording_changed():
    server_py = os.path.join(os.path.dirname(os.path.dirname(__file__)), "server.py")
    with open(server_py, encoding="utf-8") as fh:
        src = fh.read()
    assert "possible coverage gap" not in src          # misleading wording removed
    assert "in port operating area" in src             # factual funnel wording present
    assert "AISStream live" in src
