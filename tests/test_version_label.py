"""
tests/test_version_label.py — Beta 11 housekeeping: config-driven version badge.

The version badge is no longer hardcoded; it is rendered from a substitution
token replaced at serve time by _VERSION_LABEL (env HORIZON_VERSION_LABEL,
default 'BETA 10'). Display-only — no behavioural assertions beyond the label.
"""

import os
import re

_HTML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "index.html")


def _html():
    with open(_HTML_PATH, encoding="utf-8") as f:
        return f.read()


def test_badge_uses_substitution_token_not_hardcoded():
    h = _html()
    assert '<span class="logo-badge">__HORIZON_VERSION_LABEL__</span>' in h
    # the old hardcoded badge text must be gone from the badge span
    assert '<span class="logo-badge">BETA 10</span>' not in h


def test_default_version_label_is_beta_10():
    import importlib
    import server
    importlib.reload(server)
    assert server._VERSION_LABEL == "BETA 10"


def test_default_render_shows_beta_10():
    import server
    html = _html().replace("__HORIZON_VERSION_LABEL__", server._VERSION_LABEL)
    m = re.search(r'<span class="logo-badge">([^<]*)</span>', html)
    assert m and m.group(1) == "BETA 10"
    assert "__HORIZON_VERSION_LABEL__" not in html   # token fully substituted


def test_env_override_renders_custom_label(monkeypatch):
    import importlib
    monkeypatch.setenv("HORIZON_VERSION_LABEL", "BETA 11 PREVIEW")
    import server
    importlib.reload(server)
    try:
        assert server._VERSION_LABEL == "BETA 11 PREVIEW"
        html = _html().replace("__HORIZON_VERSION_LABEL__", server._VERSION_LABEL)
        m = re.search(r'<span class="logo-badge">([^<]*)</span>', html)
        assert m and m.group(1) == "BETA 11 PREVIEW"
    finally:
        monkeypatch.delenv("HORIZON_VERSION_LABEL", raising=False)
        importlib.reload(server)   # restore default for other tests
