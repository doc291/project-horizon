"""
tests/test_decision_card_authority.py — Beta 11 Slice 5A.

Static-content assertions (no JS runner on this line) that each Decision Card
carries a compact, read-only "Decision Authority" section wired to the existing
port-level _data.authority payload, that it hides when authority is absent, that
Decision Card actions and recommendation text are untouched, and that no new API
call is introduced. Explains decisions; never changes them.
"""

import os

_HTML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")


def _html():
    with open(_HTML_PATH, encoding="utf-8") as f:
        return f.read()


def _auth_block(h):
    return h.split("BETA11 DECISION CARD AUTHORITY START")[1].split("BETA11 DECISION CARD AUTHORITY END")[0]


# ── Section renders / is wired into each card ────────────────────────────────

def test_markers_present():
    h = _html()
    assert "BETA11 DECISION CARD AUTHORITY START" in h
    assert "BETA11 DECISION CARD AUTHORITY END" in h


def test_helper_and_call_present():
    h = _html()
    assert "function _decAuthHtml()" in h
    assert "${_decAuthHtml()}" in h          # injected into the card template


def test_section_reads_port_level_authority_payload():
    block = _auth_block(_html())
    assert "_data.authority" in block
    for field in ("overall_band", "overall_score", "feeds", "assumed_or_missing", "degraded"):
        assert field in block, f"authority section should read authority.{field}"


def test_recommended_display_language_present():
    block = _auth_block(_html())
    assert "Decision Authority" in block
    assert "Evidence" in block
    assert "Assumptions" in block
    # reinforce the principle
    assert "Horizon coordinates" in block and "you decide" in block


# ── Hides when authority missing ─────────────────────────────────────────────

def test_section_hidden_when_authority_missing():
    block = _auth_block(_html())
    # the guard returns an empty string when there is no authority payload
    assert "if(!a) return ''" in block


# ── Read-only: no API, no actions, no decision mutation ──────────────────────

def test_section_makes_no_api_call():
    block = _auth_block(_html())
    assert "fetch(" not in block
    assert "/api/" not in block


def test_section_invokes_no_decision_mutation():
    block = _auth_block(_html())
    for forbidden in ("openDSW", "openWhatIf", "commitDecision", "sigAction",
                      "setSigState", "writeAudit"):
        assert forbidden not in block, f"authority section must not invoke {forbidden!r}"


def test_forbidden_marketing_language_absent():
    block = _auth_block(_html())
    low = block.lower()
    for bad in ("ai confidence", "certainty", "guaranteed", "automated decision"):
        assert bad not in low, f"authority section must not use {bad!r}"


# ── Decision Card actions + recommendation text intact ───────────────────────

def test_decision_card_actions_still_present():
    h = _html()
    assert "onclick=\"openDSW('${c.id}')\"" in h
    assert "onclick=\"openWhatIf('${c.id}')\"" in h
    assert "dec-open-btn" in h


def test_recommendation_text_unchanged():
    h = _html()
    # recommended label + reasoning + cost rendering still present in the card
    assert "dec-rec-label" in h
    assert "recReason" in h
    assert "recommended_reasoning" in h
    assert "cost_label" in h


def test_card_template_order_authority_after_top_before_actions():
    h = _html()
    # ${_decAuthHtml()} sits after dec-card-top close and before the Open button
    i_auth = h.index("${_decAuthHtml()}")
    i_open = h.index("openDSW('${c.id}')")
    assert i_auth < i_open, "authority section must render before the action buttons"
