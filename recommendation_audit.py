"""
Project Horizon — recommendation audit emission helper.

Phase 0.7b per ADR-002 §1.4.1.

Best-effort, feature-flagged emission of RECOMMENDATION_GENERATED events
when the conflict-detection engine produces a decision-support payload
inside `build_summary()`.

The critical Phase 0.7b deliverable is the **mandatory decision-time
snapshot** — a captured set of inputs the engine used at the moment the
recommendation was produced, sufficient for ADR-002 §1.4.1 evidence:

    relevant_vessels, relevant_berths, eta_source_hierarchy, tide_inputs,
    weather_inputs, ukc_inputs, conflict_state, constraints,
    alternatives_generated, recommended_option, decision_deadline,
    engine_version, observed_at.

The snapshot is built from the in-memory `summary` dict and the
in-memory conflict dict — the same objects the engine produced. We do
NOT re-fetch live AIS, tides, or weather from upstream sources. The
snapshot is a frozen image of what the engine saw when it computed the
recommendation.

Design properties (mirrors session_audit.py and conflict_audit.py):

- **No-ops when DATABASE_URL is unset.** db.is_configured() returns
  False in production → emit_async returns immediately, no thread
  spawned, no psycopg touched.
- **No-ops when AUDIT_EMISSION_ENABLED is false.** Emergency disable
  without code change via env var.
- **psycopg / audit lazy-imported in the worker thread.** Production
  (no psycopg in requirements.txt, DATABASE_URL unset) never reaches
  the import.
- **Daemon-thread emission.** emit_async returns within microseconds;
  the actual DB work happens off the request thread.
- **Best-effort.** All exceptions caught and logged at WARN. The
  caller (the /api/summary handler) is never affected.
- **Per-tenant, per-process deduplication.** Key is
  (conflict_id, recommended_option_id). The same recommendation
  emitted on repeated /api/summary polls is suppressed. A change of
  recommendation (e.g. the engine flips to a different option as new
  data arrives) DOES emit a new RECOMMENDATION_GENERATED event — that
  is a new decision moment.
- **Payload lifecycle safety.** Snapshot is built from in-memory
  references, then the full payload is deep-copied BEFORE the worker
  thread starts (same pattern as conflict_audit.py).
- **Phase 0.7b scope only.** This module emits RECOMMENDATION_GENERATED
  exclusively. RECOMMENDATION_PRESENTED (the operator-facing equivalent
  tied to UI render) is Phase 0.7c and is NOT emitted here. No
  operator-action events are emitted either — that is Phase 0.8.

Deduplication caveat (same as conflict_audit): the dedup set is
in-process. Multi-process deployments would each maintain their own
set. For Phase 0 (single-process Railway), this is acceptable.
"""

from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("horizon.recommendation_audit")


# ── Feature flag ──────────────────────────────────────────────────────
_AUDIT_EMISSION_ENABLED: bool = os.environ.get(
    "AUDIT_EMISSION_ENABLED", "true"
).lower() in ("true", "1", "yes", "on")


# ── Engine version pin ────────────────────────────────────────────────
# Recorded on every snapshot so a recommendation can later be traced
# back to the engine build that produced it. Hard-coded for Phase 0
# because the engine is the Beta 10 single-file server. Phase 1 will
# derive this from a build label or git tag.
ENGINE_VERSION = "horizon-beta-10"


# ── Per-tenant per-process dedup state ────────────────────────────────
# Key is (conflict_id, recommended_option_id) — a change of
# recommendation against the same conflict is a fresh decision moment
# and SHOULD re-emit.
_emitted_lock = threading.Lock()
_emitted_keys: dict[str, set[tuple[str, str]]] = {}


def is_enabled() -> bool:
    """
    Return True iff recommendation audit emission should be attempted.

    Same gating as session_audit / conflict_audit:
      1. AUDIT_EMISSION_ENABLED env var truthy (default true)
      2. DATABASE_URL configured (db.is_configured())

    In production (DATABASE_URL unset), returns False without importing
    psycopg.
    """
    if not _AUDIT_EMISSION_ENABLED:
        return False
    import db
    return db.is_configured()


# ── Snapshot builder ──────────────────────────────────────────────────

def _vessel_subset(vessels: list[dict] | None, vessel_ids: list[str]) -> list[dict]:
    """
    Extract only the vessels relevant to this conflict.

    Pure reads; never mutates the source list. Field selection is
    deliberately narrow — we capture the operational state the engine
    used (LOA, ETA, berth_id, status, source) and NOT the entire
    Beta 10 vessel record (which embeds dozens of UI hints).
    """
    if not vessels or not vessel_ids:
        return []
    wanted = set(vessel_ids)
    out: list[dict] = []
    for v in vessels:
        if v.get("id") not in wanted:
            continue
        out.append({
            "id":         v.get("id"),
            "name":       v.get("name"),
            "loa":        v.get("loa"),
            "beam":       v.get("beam"),
            "draft":      v.get("draft"),
            "status":     v.get("status"),
            "berth_id":   v.get("berth_id"),
            "eta":        v.get("eta"),
            "etd":        v.get("etd"),
            "ata":        v.get("ata"),
            "atd":        v.get("atd"),
            "source":     v.get("source"),
            "data_source": v.get("data_source"),
        })
    return out


def _berth_subset(berths: list[dict] | None, berth_id: str | None) -> dict | None:
    """Extract the berth record for this conflict (if any)."""
    if not berths or not berth_id:
        return None
    for b in berths:
        if b.get("id") == berth_id:
            return {
                "id":              b.get("id"),
                "name":            b.get("name"),
                "length_m":        b.get("length_m"),
                "depth_m":         b.get("depth_m"),
                "max_loa":         b.get("max_loa"),
                "max_draft":       b.get("max_draft"),
                "status":          b.get("status"),
                "readiness_time": b.get("readiness_time"),
            }
    return None


def _eta_source_hierarchy(summary: dict) -> dict:
    """
    Capture which upstream source produced the ETAs the engine used.

    Beta 10's hierarchy is: AISStream → MST cache → vessel scraper →
    QShips → simulation. summary.data_source already records the
    selected leg; port_profile records which feeds are live.
    """
    pp = summary.get("port_profile") or {}
    return {
        "data_source":              summary.get("data_source"),
        "data_source_label":        summary.get("data_source_label"),
        "scraped_at":               summary.get("scraped_at"),
        "using_live_vessel_data":   pp.get("using_live_vessel_data"),
        "using_live_tidal_data":    pp.get("using_live_tidal_data"),
        "using_live_weather_data":  pp.get("using_live_weather_data"),
    }


def _tide_inputs(summary: dict) -> dict:
    """Tidal state the engine saw when deciding."""
    t = summary.get("tides") or {}
    pp = summary.get("port_profile") or {}
    return {
        "current_height_m": t.get("current_height_m"),
        "next_high":        t.get("next_high"),
        "next_low":         t.get("next_low"),
        "source":           t.get("source"),
        "station_id":       pp.get("bom_station_id"),
    }


def _weather_inputs(summary: dict) -> dict:
    """Weather state the engine saw when deciding."""
    w = summary.get("weather") or {}
    return {
        "wind_kts":   w.get("wind_kts"),
        "wind_dir":   w.get("wind_dir"),
        "gust_kts":   w.get("gust_kts"),
        "sea_state":  w.get("sea_state"),
        "swell_m":    w.get("swell_m"),
        "visibility": w.get("visibility"),
        "source":     w.get("source"),
    }


def _ukc_records(ukc_block: Any) -> list[dict]:
    """
    Normalise a Beta 10 ukc/arrival_ukc block into a flat list of
    per-vessel dicts.

    Real Beta 10 shape is a dict {min_ukc_m, status, all: [...]} where
    only the `all` list holds the per-vessel records. Tests may supply
    a flat list directly. We accept both.
    """
    if not ukc_block:
        return []
    if isinstance(ukc_block, list):
        return [r for r in ukc_block if isinstance(r, dict)]
    if isinstance(ukc_block, dict):
        return [r for r in (ukc_block.get("all") or []) if isinstance(r, dict)]
    return []


def _ukc_inputs(summary: dict, vessel_ids: list[str]) -> list[dict]:
    """UKC computations for the vessels in this conflict (if any)."""
    if not vessel_ids:
        return []
    wanted = set(vessel_ids)
    out: list[dict] = []
    for u in _ukc_records(summary.get("ukc")):
        vid = u.get("vessel_id") or u.get("id")
        if vid in wanted:
            out.append({
                "vessel_id":    vid,
                "ukc_m":        u.get("ukc_m") or u.get("clearance_m"),
                "required_m":   u.get("required_m"),
                "tide_at_eta_m": u.get("tide_at_eta_m"),
                "available_depth_m": u.get("available_depth_m"),
                "vessel_draught_m":  u.get("vessel_draught_m"),
                "status":       u.get("status"),
            })
    for u in _ukc_records(summary.get("arrival_ukc")):
        vid = u.get("vessel_id") or u.get("id")
        if vid in wanted:
            out.append({
                "vessel_id":   vid,
                "phase":       "arrival",
                "ukc_m":       u.get("ukc_m") or u.get("clearance_m"),
                "required_m":  u.get("required_m"),
                "predicted_tide_m": u.get("predicted_tide_m"),
                "vessel_draught_m": u.get("vessel_draught_m"),
                "status":      u.get("status"),
            })
    return out


def _alternatives_subset(seq_alts: list[dict] | None) -> list[dict]:
    """
    Compact representation of sequencing alternatives.

    We preserve the structural decision-relevant fields (strategy,
    cost, delay, cascade count, risk, recommended-flag) but drop the
    free-text description and reasoning, which can be re-rendered from
    the alternative metadata if needed.
    """
    if not seq_alts:
        return []
    out: list[dict] = []
    for a in seq_alts:
        out.append({
            "id":           a.get("id"),
            "strategy":     a.get("strategy"),
            "headline":     a.get("headline"),
            "cost_usd":     a.get("cost_usd"),
            "cost_label":   a.get("cost_label"),
            "delay_mins":   a.get("delay_mins"),
            "cascade_count": a.get("cascade_count"),
            "risk":         a.get("risk"),
            "feasibility": a.get("feasibility"),
            "recommended": bool(a.get("recommended")),
            "affected_vessels": list(a.get("affected_vessels") or []),
        })
    return out


def _recommended_option(seq_alts: list[dict] | None,
                        recommended_id: str | None) -> dict | None:
    """Return the full recommended-option record (or None)."""
    if not seq_alts:
        return None
    rec = None
    if recommended_id:
        rec = next((a for a in seq_alts if a.get("id") == recommended_id), None)
    if rec is None:
        rec = next((a for a in seq_alts if a.get("recommended")), None)
    if rec is None:
        return None
    # Whitelist fields — include full reasoning here because the
    # recommended option is the central evidence artefact.
    return {
        "id":              rec.get("id"),
        "strategy":        rec.get("strategy"),
        "headline":        rec.get("headline"),
        "description":     rec.get("description"),
        "affected_vessels": list(rec.get("affected_vessels") or []),
        "ops_note":        rec.get("ops_note"),
        "cost_usd":        rec.get("cost_usd"),
        "cost_label":      rec.get("cost_label"),
        "delay_mins":      rec.get("delay_mins"),
        "cascade_count":   rec.get("cascade_count"),
        "feasibility":     rec.get("feasibility"),
        "risk":            rec.get("risk"),
    }


def _constraints(summary: dict, berth_record: dict | None) -> dict:
    """Capture port-level and berth-level constraints the engine respected."""
    pp = summary.get("port_profile") or {}
    return {
        "clearance_mins":     60,   # CLEARANCE_MINS in server.py — engine constant
        "berth_max_loa":      (berth_record or {}).get("max_loa"),
        "berth_max_draft":    (berth_record or {}).get("max_draft"),
        "berth_length_m":     (berth_record or {}).get("length_m"),
        "port_id":            pp.get("id"),
        "lookahead_hours":    summary.get("lookahead_hours"),
        "pilots_available":   (summary.get("port_status") or {}).get("pilots_available"),
        "tugs_available":     (summary.get("port_status") or {}).get("tugs_available"),
    }


def _conflict_state(conflict: dict) -> dict:
    """Frozen conflict descriptor at decision time."""
    return {
        "conflict_id":     conflict.get("id"),
        "conflict_type":   conflict.get("conflict_type"),
        "signal_type":     conflict.get("signal_type"),
        "severity":        conflict.get("severity"),
        "conflict_time":   conflict.get("conflict_time"),
        "description":     conflict.get("description"),
        "berth_id":        conflict.get("berth_id"),
        "berth_name":      conflict.get("berth_name"),
        "vessel_ids":      list(conflict.get("vessel_ids") or []),
        "vessel_names":    list(conflict.get("vessel_names") or []),
        "data_source":     conflict.get("data_source"),
        "safety_score":    conflict.get("safety_score"),
    }


def make_snapshot(conflict: dict, *, summary: dict) -> dict:
    """
    Build the ADR-002 §1.4.1 decision-time snapshot.

    Pure function over in-memory `conflict` and `summary`. Never
    mutates the inputs. Never re-fetches upstream data — the snapshot
    is the engine's view of the world at decision time.
    """
    ds = conflict.get("decision_support") or {}
    seq_alts = conflict.get("sequencing_alternatives") or []
    vessel_ids = list(conflict.get("vessel_ids") or [])
    berth_id = conflict.get("berth_id")
    berth_record = _berth_subset(summary.get("berths"), berth_id)

    return {
        "engine_version":         ENGINE_VERSION,
        "relevant_vessels":       _vessel_subset(summary.get("vessels"), vessel_ids),
        "relevant_berths":        [berth_record] if berth_record else [],
        "eta_source_hierarchy":   _eta_source_hierarchy(summary),
        "tide_inputs":            _tide_inputs(summary),
        "weather_inputs":         _weather_inputs(summary),
        "ukc_inputs":             _ukc_inputs(summary, vessel_ids),
        "conflict_state":         _conflict_state(conflict),
        "constraints":            _constraints(summary, berth_record),
        "alternatives_generated": _alternatives_subset(seq_alts),
        "recommended_option":     _recommended_option(seq_alts, ds.get("recommended_option_id")),
        "decision_deadline":      ds.get("decision_deadline"),
        "confidence":             ds.get("confidence"),
        "recommended_reasoning":  ds.get("recommended_reasoning"),
    }


def make_payload(
    conflict: dict,
    *,
    summary: dict,
    port_id: str | None = None,
) -> dict:
    """
    Build the RECOMMENDATION_GENERATED payload.

    The payload wraps the snapshot under `decision_snapshot` and adds
    a small recommendation-context envelope. No re-fetching from
    upstream; values are pulled from the in-memory snapshot/conflict
    objects.
    """
    ds = conflict.get("decision_support") or {}
    payload: dict[str, Any] = {
        "conflict_id":           conflict.get("id"),
        "recommendation_id":     f"{conflict.get('id')}::{ds.get('recommended_option_id')}",
        "recommended_option_id": ds.get("recommended_option_id"),
        "decision_deadline":     ds.get("decision_deadline"),
        "engine_version":        ENGINE_VERSION,
        "observed_at":           datetime.now(timezone.utc).isoformat(),
        "decision_snapshot":     make_snapshot(conflict, summary=summary),
    }
    if port_id:
        payload["port_id"] = port_id
    return payload


# ── Emission ──────────────────────────────────────────────────────────

def _dedup_key(conflict: dict) -> tuple[str, str] | None:
    """Return (conflict_id, recommended_option_id) — both required."""
    cid = conflict.get("id")
    rec_opt = (conflict.get("decision_support") or {}).get("recommended_option_id")
    if not cid or not rec_opt:
        return None
    return (str(cid), str(rec_opt))


def emit_async(
    tenant_id: str,
    conflict: dict,
    *,
    summary: dict,
    port_id: str | None = None,
) -> None:
    """
    Best-effort emission of RECOMMENDATION_GENERATED for one conflict.

    Returns immediately. Spawns a daemon thread only if:
      (a) audit is enabled, AND
      (b) the conflict carries a decision_support recommendation, AND
      (c) this (conflict_id, recommended_option_id) has not been
          emitted before for this tenant in this process.

    When DATABASE_URL is unset OR the conflict has no
    decision_support, returns without side effect.

    Lifecycle safety: payload is deep-copied here, before the worker
    thread starts.
    """
    if not is_enabled():
        return

    key = _dedup_key(conflict)
    if not key:
        return

    with _emitted_lock:
        seen = _emitted_keys.setdefault(tenant_id, set())
        if key in seen:
            return
        seen.add(key)

    payload_snapshot = copy.deepcopy(
        make_payload(conflict, summary=summary, port_id=port_id)
    )
    subject_id = payload_snapshot["recommendation_id"]

    threading.Thread(
        target=_emit_worker,
        args=(tenant_id, payload_snapshot, subject_id),
        daemon=True,
        name=f"audit-emit-RECOMMENDATION_GENERATED-{subject_id[:16]}",
    ).start()


def emit_sync(
    tenant_id: str,
    conflict: dict,
    *,
    summary: dict,
    port_id: str | None = None,
) -> bool:
    """
    Synchronous variant of emit_async. Returns True if emission
    succeeded, False if disabled, deduplicated, or failed.

    Primary use: tests. Not invoked from request handlers.
    """
    if not is_enabled():
        return False

    key = _dedup_key(conflict)
    if not key:
        return False

    with _emitted_lock:
        seen = _emitted_keys.setdefault(tenant_id, set())
        if key in seen:
            return False
        seen.add(key)

    payload_snapshot = copy.deepcopy(
        make_payload(conflict, summary=summary, port_id=port_id)
    )
    subject_id = payload_snapshot["recommendation_id"]
    return _emit_worker(tenant_id, payload_snapshot, subject_id)


def reset_dedup_state() -> None:
    """
    Clear the per-process emitted-recommendation memory. Tests only.
    """
    with _emitted_lock:
        _emitted_keys.clear()


# ── Internal worker ───────────────────────────────────────────────────

def _emit_worker(tenant_id: str, payload: dict, subject_id: str) -> bool:
    """Worker entrypoint. Returns True on success, False on failure."""
    try:
        # Lazy imports — only invoked when emission is actually attempted.
        # In production (no psycopg, no DATABASE_URL), never reached.
        import psycopg
        import audit
        import db

        url = db.get_url()
        with psycopg.connect(url, connect_timeout=5) as conn:
            audit.emit(
                conn, tenant_id,
                event_type="RECOMMENDATION_GENERATED",
                subject_type="recommendation",
                subject_id=subject_id,
                payload=payload,
                actor_handle=None,        # system-generated, no operator actor
                actor_type="system",
            )
        return True
    except Exception as exc:
        log.warning(
            "audit emission failed (best-effort): "
            "event_type=RECOMMENDATION_GENERATED tenant=%s subject=%s error=%s",
            tenant_id, subject_id, exc,
        )
        return False
