"""
Project Horizon — Beta 10 decision-card recommendation selection.

Locks in the Beta 10 demo rule: the recommended option on every Decision
Card is the LOWEST-COST VIABLE option, with viability taken from the data
model's ``feasibility`` flag (``feasibility == "low"`` => not viable), and
ties broken by lower ``delay_mins`` then lower ``cascade_count``.

Covers:
  * the canonical demo scenario (Hold 82k / Accelerate 101k / Reassign 152k)
  * viability exclusion via the feasibility flag (not label text)
  * both tie-breakers (delay, then cascade)
  * backend/UI consistency: recommended_option_id matches the option whose
    ``recommended`` flag is True (requirements 6 & 7)
  * a real-path invariant over build_summary(): no card recommends the
    highest-cost option when a lower-cost viable option exists

Mirrors the Beta 10 production posture: DATABASE_URL unset, no network.
"""

from __future__ import annotations

import os

# Match the regression-gate posture: DATABASE_URL must not be set when
# server.py is imported.
os.environ.pop("DATABASE_URL", None)

import server  # noqa: E402


# ── Fixture builder ──────────────────────────────────────────────────────────

def _opt(oid, strategy, cost_usd, *, feasibility="high", delay_mins=0,
         cascade_count=0, risk="medium"):
    """Minimal option dict carrying the fields _select_recommended reads."""
    return {
        "id": oid,
        "strategy": strategy,
        "feasibility": feasibility,
        "cost_usd": cost_usd,
        "cost_label": f"~A${cost_usd:,}",
        "delay_mins": delay_mins,
        "cascade_count": cascade_count,
        "risk": risk,
        "recommended": False,
    }


def _canonical_three(hold_feasibility="high"):
    """The brief's verification scenario."""
    return [
        _opt("HOLD",     "delay_arrival",    82_000,  feasibility=hold_feasibility,
             delay_mins=120, cascade_count=1, risk="low"),
        _opt("ACCEL",    "advance_departure", 101_000, feasibility="medium",
             delay_mins=0,   cascade_count=1, risk="medium"),
        _opt("REASSIGN", "reassign_berth",    152_000, feasibility="high",
             delay_mins=30,  cascade_count=1, risk="medium"),
    ]


# ── _select_recommended ──────────────────────────────────────────────────────

def test_lowest_cost_viable_is_recommended():
    """Hold (82k) is the cheapest viable option, so it must be recommended."""
    opts = _canonical_three()
    chosen = server._select_recommended(opts)
    assert chosen["id"] == "HOLD"
    # Exactly one option flagged, and it is the chosen one.
    flagged = [o["id"] for o in opts if o["recommended"]]
    assert flagged == ["HOLD"]


def test_highest_cost_never_recommended_when_cheaper_viable_exists():
    opts = _canonical_three()
    chosen = server._select_recommended(opts)
    assert chosen["id"] != "REASSIGN", (
        "Reassign at 152k must not be recommended while Hold (82k) and "
        "Accelerate (101k) are viable."
    )


def test_non_viable_cheapest_is_excluded_by_feasibility_flag():
    """
    If the cheapest option is marked not viable (feasibility 'low' — the
    model's not-practical marker), it is excluded and the next cheapest
    VIABLE option wins. Accelerate (101k) beats Reassign (152k).
    """
    opts = _canonical_three(hold_feasibility="low")
    chosen = server._select_recommended(opts)
    assert chosen["id"] == "ACCEL", (
        "With Hold marked not viable, Accelerate (101k) is the lowest-cost "
        "viable option and must be recommended over Reassign (152k)."
    )
    assert [o["id"] for o in opts if o["recommended"]] == ["ACCEL"]


def test_tie_on_cost_breaks_to_lower_delay():
    opts = [
        _opt("A", "advance_departure", 90_000, delay_mins=90, cascade_count=1),
        _opt("B", "delay_arrival",     90_000, delay_mins=30, cascade_count=1),
    ]
    chosen = server._select_recommended(opts)
    assert chosen["id"] == "B", "Equal cost -> lower delay (30 < 90) wins."


def test_tie_on_cost_and_delay_breaks_to_lower_cascade():
    opts = [
        _opt("A", "advance_departure", 90_000, delay_mins=30, cascade_count=3),
        _opt("B", "delay_arrival",     90_000, delay_mins=30, cascade_count=1),
    ]
    chosen = server._select_recommended(opts)
    assert chosen["id"] == "B", "Equal cost+delay -> lower cascade (1 < 3) wins."


def test_all_non_viable_falls_back_to_lowest_cost_overall():
    """Never leave a card without a recommendation."""
    opts = [
        _opt("A", "delay_arrival",  70_000, feasibility="low"),
        _opt("B", "reassign_berth", 60_000, feasibility="low"),
    ]
    chosen = server._select_recommended(opts)
    assert chosen["id"] == "B", "All low -> fall back to lowest cost overall."


def test_empty_options_returns_none():
    assert server._select_recommended([]) is None


# ── _build_decision_support: id/flag consistency (requirements 6 & 7) ─────────

def test_recommended_option_id_matches_flagged_option():
    opts = _canonical_three()
    now = server.utcnow()
    ds = server._build_decision_support(opts, now + server.timedelta(hours=6), now)
    flagged = [o["id"] for o in ds["options"] if o["recommended"]]
    assert flagged == ["HOLD"]
    assert ds["recommended_option_id"] == "HOLD", (
        "recommended_option_id (read by the Recommended Action panel and the "
        "expanded option list) must equal the flagged option (read by the "
        "summary/What-If/PDF consumers)."
    )


# ── Real-path invariant over build_summary() ─────────────────────────────────

def test_no_decision_card_recommends_highest_cost_when_cheaper_viable_exists():
    """
    Drive the real engine (default Brisbane simulation, B03 + B04 demo
    cards) and assert every Decision Card recommends a lowest-cost viable
    option, and that the panel/flag agree.
    """
    summary = server.build_summary()
    cards = [
        c for c in (summary.get("conflicts") or [])
        if (c.get("decision_support") or {}).get("options")
    ]
    assert cards, "Expected at least one Decision Card in Brisbane simulation."

    for c in cards:
        ds = c["decision_support"]
        opts = ds["options"]
        rec_id = ds["recommended_option_id"]
        rec = next(o for o in opts if o["id"] == rec_id)

        # Flag and id agree.
        flagged = [o["id"] for o in opts if o.get("recommended")]
        assert flagged == [rec_id], (
            f"card {c.get('id')}: recommended flag {flagged} disagrees with "
            f"recommended_option_id {rec_id}"
        )

        # The recommendation is the lowest-cost viable option.
        viable = [o for o in opts
                  if str(o.get("feasibility", "")).lower() != "low"]
        pool = viable or opts
        min_cost = min(int(o.get("cost_usd") or 0) for o in pool)
        assert int(rec.get("cost_usd") or 0) == min_cost, (
            f"card {c.get('id')}: recommended {rec_id} costs "
            f"{rec.get('cost_usd')} but the cheapest viable option costs "
            f"{min_cost}. Options: "
            f"{[(o['id'], o.get('cost_usd'), o.get('feasibility')) for o in opts]}"
        )

        # And it is itself viable whenever any viable option exists.
        if viable:
            assert str(rec.get("feasibility", "")).lower() != "low", (
                f"card {c.get('id')}: recommended a non-viable option while "
                f"viable options existed."
            )
