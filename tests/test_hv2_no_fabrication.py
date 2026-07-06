"""
Horizon v2 command-centre reskin — static guards.

The v2 console/wall renderer (script id="hv2-render" in index.html) must bind to
live Horizon data ONLY. This locks in:
  * no design mock/demo tokens leaked into the implementation,
  * no recommendation/ranking language in the renderer,
  * the honest unavailable / lower-confidence labels are present,
  * the surface is gated on d.beta12 (Beta 10 untouched when flag off).
"""
import re
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
HTML = (ROOT / "index.html").read_text()


def _hv2_script():
    m = re.search(r'<script id="hv2-render">(.*?)</script>', HTML, re.S)
    assert m, "hv2-render script not found in index.html"
    return m.group(1)


HV2 = _hv2_script()


def test_no_design_mock_tokens_leaked():
    # Vessel / port / operator / ledger names invented in the design package.
    banned = ["meridian", "talara", "korowai", "coorabie", "ailsa", "otira",
              "larus", "otago", "kestrel", "laurel", "sabine", "calder",
              "highland chief", "wyuna", "bauhinia", "northline", "hawkes",
              "kiriakidis", "hzn-d-", "atlas venture", "cormorant", "blue petrel",
              "sandpiper", "tern island", "ironbark"]
    low = HV2.lower()
    hits = [b for b in banned if b in low]
    assert not hits, f"design mock tokens leaked into hv2 renderer: {hits}"


def test_no_hardcoded_demo_numbers():
    # The design hardcoded 94%/98% on-time and "11 commitments"; must be data-driven.
    assert "94%" not in HV2 and "98%" not in HV2
    assert "11 commitments" not in HV2


def test_no_recommendation_language():
    low = HV2.lower()
    for w in ("recommend", "optimal", "preferred", "★"):
        assert w not in low, f"recommendation language in hv2 renderer: {w}"
    # "best" only as a whole word
    assert not re.search(r"\bbest\b", low), "recommendation word 'best' in hv2 renderer"


def test_honest_unavailable_labels_present():
    for phrase in ("confidence trend not tracked",
                   "source time unavailable",
                   "not available in this preview",
                   "On-time · now"):
        assert phrase in HV2, f"missing honest-unavailable label: {phrase!r}"


def test_surface_is_flag_gated_on_beta12():
    # renderHv2 must early-return (no hv2-active) when d.beta12 is absent.
    assert "if(!d || !d.beta12)" in HV2
    assert "classList.remove('hv2-active')" in HV2
    assert "classList.add('hv2-active')" in HV2


def test_scoped_css_root_not_global():
    # The embedded command.css must be scoped to #hv2-root, never global :root,
    # or it would override Beta 10's --bg / --font.
    m = re.search(r'<style id="hv2-command-css">(.*?)</style>', HTML, re.S)
    assert m, "hv2-command-css style block not found"
    css = m.group(1)
    assert "#hv2-root {" in css and re.search(r'(^|\})\s*:root\s*\{', css) is None
