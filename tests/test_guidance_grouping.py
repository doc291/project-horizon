"""
Tests for guidance-rail constraint grouping (fix/beta10-guidance-constraint-grouping).

Verifies that:
  - Multiple Bolte Bridge air-draught warnings are grouped into one tile
  - Affected vessel names are retained in the grouped tile
  - A single Bolte Bridge warning still renders individually
  - Unrelated guidance warnings remain separate / unchanged
  - Berth-overlap (critical) conflicts are not affected by grouping
"""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# ── Minimal env so server.py imports without crashing ──────────────────────────
os.environ.setdefault("HORIZON_USER", "test")
os.environ.setdefault("HORIZON_PASS", "test")
os.environ.setdefault("TOKEN_SECRET", "x" * 32)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ts(offset_hours: int = 0) -> str:
    """ISO-8601 timestamp relative to a fixed base, safe for isoparse()."""
    from datetime import timedelta
    base = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(hours=offset_hours)).isoformat()


def _bridge_conflict(vessel_id: str, vessel_name: str, est_air: int = 45,
                     bridge: str = "Bolte Bridge", limit: int = 31,
                     severity: str = "high") -> dict:
    cid = hashlib.md5(f"bridge_restriction-{vessel_id}-{bridge}".encode()).hexdigest()[:8]
    return {
        "id": cid,
        "conflict_type": "bridge_restriction",
        "signal_type": "WARNING",
        "severity": severity,
        "vessel_ids": [vessel_id],
        "vessel_names": [vessel_name],
        "berth_id": None,
        "berth_name": bridge,
        "conflict_time": _ts(),
        "description": (
            f"{vessel_name} estimated air draught {est_air}m exceeds "
            f"{bridge} limit of {limit}m. "
            "Check tidal state before authorising transit."
        ),
        "resolution_options": [
            "Confirm actual air draught with master before authorising transit",
            "Check current tidal state — clearance varies with tide height",
        ],
        "sequencing_alternatives": [],
        "decision_support": None,
        "data_source": "simulated",
    }


def _berth_overlap_conflict(vessel_a: str, vessel_b: str, berth: str = "B03") -> dict:
    cid = hashlib.md5(f"berth_overlap-{vessel_a}-{berth}".encode()).hexdigest()[:8]
    return {
        "id": cid,
        "conflict_type": "berth_overlap",
        "signal_type": "CONFLICT",
        "severity": "critical",
        "vessel_ids": [vessel_a, vessel_b],
        "vessel_names": [vessel_a, vessel_b],
        "berth_id": berth,
        "berth_name": berth,
        "conflict_time": _ts(2),
        "description": f"Berth {berth}: {vessel_a} and {vessel_b} scheduled simultaneously.",
        "resolution_options": ["Delay one vessel", "Reassign to alternate berth"],
        "sequencing_alternatives": [],
        "decision_support": None,
        "data_source": "live",
    }


def _run_guidance(conflicts):
    import server
    now = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
    return server.build_guidance(conflicts, vessels=[], berths=[], pilotage=[], towage=[], now=now)


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestBridgeGrouping:

    def test_multiple_bridge_warnings_produce_one_guidance_tile(self):
        """4 vessels hitting Bolte Bridge → exactly 1 guidance tile for that bridge."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
            _bridge_conflict("V3", "Vessel Gamma"),
            _bridge_conflict("V4", "Vessel Delta"),
        ]
        items = _run_guidance(conflicts)
        bridge_items = [i for i in items if "Bolte Bridge" in i.get("message", "")]
        assert len(bridge_items) == 1, (
            f"Expected 1 grouped Bolte Bridge tile, got {len(bridge_items)}"
        )

    def test_grouped_tile_message_names_constraint_not_vessel(self):
        """Message should lead with the bridge name, not a vessel name."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        assert "Bolte Bridge" in bridge_item["message"]
        # Should NOT start with a vessel name
        assert not bridge_item["message"].startswith("Vessel Alpha")
        assert not bridge_item["message"].startswith("Vessel Beta")

    def test_grouped_tile_reports_vessel_count(self):
        """Grouped tile message includes the number of affected vessels."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
            _bridge_conflict("V3", "Vessel Gamma"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        assert "3" in bridge_item["message"]

    def test_all_affected_vessel_names_retained(self):
        """All vessel names must appear in affected_vessels."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
            _bridge_conflict("V3", "Vessel Gamma"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        affected = bridge_item.get("affected_vessels", [])
        assert "Vessel Alpha" in affected
        assert "Vessel Beta" in affected
        assert "Vessel Gamma" in affected

    def test_vessel_summary_field_present_and_non_empty(self):
        """vessel_summary should be a non-empty string listing vessel names."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        summary = bridge_item.get("vessel_summary", "")
        assert isinstance(summary, str) and summary
        assert "Vessel Alpha" in summary
        assert "Vessel Beta" in summary

    def test_vessel_summary_and_more_format_for_large_groups(self):
        """Groups >3 vessels use 'and X more' format in vessel_summary."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
            _bridge_conflict("V3", "Vessel Gamma"),
            _bridge_conflict("V4", "Vessel Delta"),
            _bridge_conflict("V5", "Vessel Epsilon"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        summary = bridge_item.get("vessel_summary", "")
        assert "and 2 more" in summary, f"Expected 'and 2 more' in summary, got: {summary!r}"

    def test_detail_contains_all_vessel_names(self):
        """Detail text must mention every affected vessel for operator reference."""
        conflicts = [
            _bridge_conflict("V1", "Nordic Star"),
            _bridge_conflict("V2", "Pacific Dawn"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        detail = bridge_item.get("detail", "")
        assert "Nordic Star" in detail
        assert "Pacific Dawn" in detail

    def test_single_bridge_warning_renders_individually(self):
        """A single Bolte Bridge warning is NOT grouped — renders as normal tile."""
        conflicts = [_bridge_conflict("V1", "Solo Vessel")]
        items = _run_guidance(conflicts)
        bridge_items = [i for i in items if "Solo Vessel" in i.get("message", "")
                        or "Bolte Bridge" in i.get("message", "")]
        assert len(bridge_items) == 1
        # Must NOT have affected_vessels (that's only for grouped tiles)
        assert "affected_vessels" not in bridge_items[0]

    def test_single_bridge_warning_message_contains_vessel_name(self):
        """Single bridge warning message should still reference the vessel."""
        conflicts = [_bridge_conflict("V1", "Solo Vessel")]
        items = _run_guidance(conflicts)
        bridge_item = items[0]
        assert "Solo Vessel" in bridge_item["message"]

    def test_unrelated_conflicts_remain_separate(self):
        """Berth-overlap, pilotage, and other conflicts are not collapsed."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
            _berth_overlap_conflict("Ship A", "Ship B", "B03"),
            _berth_overlap_conflict("Ship C", "Ship D", "B04"),
        ]
        items = _run_guidance(conflicts)
        berth_items = [i for i in items if "Berth conflict" in i.get("message", "")]
        bridge_items = [i for i in items if "Bolte Bridge" in i.get("message", "")]
        assert len(berth_items) == 2, "Two separate berth-overlap conflicts must stay separate"
        assert len(bridge_items) == 1, "Two Bolte Bridge conflicts must be grouped into one"

    def test_critical_berth_conflicts_unchanged(self):
        """Critical berth-overlap conflicts must pass through unmodified."""
        overlap = _berth_overlap_conflict("Ship A", "Ship B", "B03")
        items = _run_guidance([overlap])
        assert len(items) == 1
        item = items[0]
        assert item["priority"] == "critical"
        assert "Ship A" in item["message"] or "Ship A" in item["detail"]

    def test_different_bridges_stay_separate(self):
        """Vessels hitting different bridges are NOT merged into one tile."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha", bridge="Bolte Bridge"),
            _bridge_conflict("V2", "Vessel Beta",  bridge="West Gate Bridge"),
        ]
        items = _run_guidance(conflicts)
        assert len(items) == 2, "Two different bridges must produce two separate tiles"

    def test_multiple_vessels_same_bridge_grouped_across_severities(self):
        """Mixed high/critical severities in same bridge group — worst drives priority."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha", severity="high"),
            _bridge_conflict("V2", "Vessel Beta",  severity="critical"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        assert bridge_item["priority"] == "critical", (
            "When any vessel in the group is critical, grouped tile must be critical"
        )

    def test_grouped_tile_has_resolution_options(self):
        """Grouped tile must carry resolution_options from worst conflict."""
        conflicts = [
            _bridge_conflict("V1", "Vessel Alpha"),
            _bridge_conflict("V2", "Vessel Beta"),
        ]
        items = _run_guidance(conflicts)
        bridge_item = next(i for i in items if "Bolte Bridge" in i.get("message", ""))
        assert isinstance(bridge_item.get("resolution_options"), list)
        assert len(bridge_item["resolution_options"]) > 0

    def test_empty_conflicts_returns_empty_guidance(self):
        items = _run_guidance([])
        # May have proactive items but no bridge/conflict items
        bridge_items = [i for i in items if "Bolte Bridge" in i.get("message", "")]
        assert len(bridge_items) == 0
