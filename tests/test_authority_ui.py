"""
tests/test_authority_ui.py — Beta 11 Slice 4A: Authority Diagnostics panel.

The Beta 10/11 operator frontend is a single server-rendered index.html with no
JS test runner on this line, so these are static-content assertions: the
additive, read-only Authority Diagnostics panel exists and is wired to the
existing summary.authority / summary.vessel_source payload, and the Decision
Card UI is left intact.
"""

import os

_HTML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")


def _html():
    with open(_HTML_PATH, encoding="utf-8") as f:
        return f.read()


# ── Panel exists and is clearly delimited (easy to remove) ───────────────────

def test_diagnostics_markers_present():
    h = _html()
    assert "BETA11 AUTHORITY DIAGNOSTICS START" in h
    assert "BETA11 AUTHORITY DIAGNOSTICS END" in h
    assert "BETA11 AUTHORITY DIAGNOSTICS MARKUP START" in h
    assert "BETA11 AUTHORITY DIAGNOSTICS MARKUP END" in h


def test_panel_markup_present():
    h = _html()
    assert 'id="auth-diag"' in h
    for el in ("ad-band", "ad-score", "ad-vsource", "ad-cats",
               "ad-feeds", "ad-assumed", "ad-degraded", "ad-body", "ad-toggle"):
        assert f'id="{el}"' in h, f"missing panel element id={el!r}"


# ── Wired to the existing payload (no new endpoint) ──────────────────────────

def test_render_function_and_call_present():
    h = _html()
    assert "function renderAuthorityDiagnostics(d)" in h
    assert "renderAuthorityDiagnostics(d);" in h          # called from render()
    assert "function toggleAuthDiag()" in h


def test_reads_authority_and_vessel_source_payload():
    h = _html()
    # uses the Slice-3 backend fields only
    assert "d.authority" in h
    assert "d.vessel_source" in h
    # the diagnostics function references the authority sub-fields
    block = h.split("BETA11 AUTHORITY DIAGNOSTICS START")[1].split("BETA11 AUTHORITY DIAGNOSTICS END")[0]
    for field in ("overall_band", "overall_score", "sources_by_category",
                  "feeds", "assumed_or_missing", "degraded"):
        assert field in block, f"diagnostics block should read authority.{field}"


def test_panel_does_not_call_a_new_endpoint():
    h = _html()
    block = h.split("BETA11 AUTHORITY DIAGNOSTICS START")[1].split("BETA11 AUTHORITY DIAGNOSTICS END")[0]
    # read-only: the panel must not fetch or post anything itself
    assert "fetch(" not in block
    assert "/api/" not in block


# ── Read-only: no mutation of decision/operator state ────────────────────────

def test_panel_is_read_only():
    h = _html()
    block = h.split("BETA11 AUTHORITY DIAGNOSTICS START")[1].split("BETA11 AUTHORITY DIAGNOSTICS END")[0]
    for forbidden in ("commitDecision", "acknowledge", "openDSW",
                      "set_port", "apply-whatif", "applyWhatIf"):
        assert forbidden not in block, f"diagnostics block must not invoke {forbidden!r}"


# ── Decision Card UI left intact ─────────────────────────────────────────────

def test_decision_card_ui_still_present():
    h = _html()
    # core Decision Card surface untouched / still present
    assert "function renderDecisions(" in h
    assert "dec-card sev-" in h
    assert "openDSW(" in h
    assert "Open Decision Support" in h


def test_diagnostics_markup_is_outside_app_container():
    h = _html()
    # the panel markup lives after the main </script> (body-level, additive),
    # not inside the #app layout or the decisions panel
    after_script = h.rsplit("</script>", 1)[1]
    assert 'id="auth-diag"' in after_script, "panel should be appended at body level (additive)"
