#!/usr/bin/env python3
"""
Project Horizon — audit writer performance benchmark.

Phase 0 / Step 0.5c per ADR-002. Measures throughput of the audit
writer against the target of ≥ 100 events/sec/tenant. Validates that
per-tenant advisory locks isolate tenants cleanly under concurrent
write load.

Benchmarks run:
  1. single-tenant sequential inserts (small / medium / decision-snapshot
     ~32 KB payloads)
  2. multi-tenant concurrent inserts (4 tenants × N events each, medium
     payloads)
  3. chain verification rate (rows verified per second)

Each benchmark uses a unique ephemeral tenant_id (`bench-...`) and
cleans up its rows afterwards. Safe to run repeatedly against the dev DB.

Requires: DATABASE_URL set, audit schema applied (migration 0004),
audit.py importable.

Usage:
  source .venv-step03/bin/activate
  export DATABASE_URL=postgresql://localhost:5432/horizon_dev
  python3 scripts/bench_audit.py
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

# Make repo root importable when invoked from anywhere
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import audit  # noqa: E402


DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Run with:", file=sys.stderr)
    print("  export DATABASE_URL=postgresql://localhost:5432/horizon_dev", file=sys.stderr)
    sys.exit(1)


# ── Payload templates ─────────────────────────────────────────────────

def make_small_payload() -> dict:
    """Session-event-sized payload (~150 bytes canonical)."""
    return {
        "session_id": str(uuid4()),
        "actor": "horizon",
        "client_ip": "203.0.113.1",
        "user_agent_class": "browser",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def make_medium_payload() -> dict:
    """Conflict-detected-sized payload (~1.5 KB)."""
    return {
        "conflict_id": str(uuid4()),
        "conflict_type": "berth_overlap",
        "severity": "high",
        "deadline": datetime.now(timezone.utc).isoformat(),
        "vessels": [
            {"id": f"V-{i}", "name": f"MV Bench Test {i:03d}",
             "loa": 200 + i, "draft": 11.5 + i*0.1,
             "eta": datetime.now(timezone.utc).isoformat()}
            for i in range(3)
        ],
        "berths": [
            {"id": f"B{i}", "name": f"Berth {i}", "occupied": i % 2 == 0,
             "draft_limit_m": 13.0, "loa_limit_m": 280}
            for i in range(2)
        ],
        "tide_inputs": {"hw_time": "2026-05-12T14:30:00+00:00", "hw_height_m": 2.4,
                       "source": "BOM", "freshness_min": 12},
    }


def make_decision_snapshot_payload(target_bytes: int = 32_000) -> dict:
    """
    Decision-snapshot-sized payload per ADR-002 §1.4.1 (~32 KB target).

    Builds a realistic-shaped RECOMMENDATION_GENERATED payload with
    enough vessel/berth detail to reach the target size when serialised.
    """
    base = {
        "recommendation_id": str(uuid4()),
        "conflict_id": str(uuid4()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine_version": "v1.0.0",
        "decision_snapshot": {
            "vessels": [],
            "berths": [],
            "eta_hierarchy": {},
            "tide_inputs": {"hw_time": "2026-05-12T14:30:00+00:00",
                           "hw_height_m": 2.4, "source": "BOM"},
            "weather_inputs": {"wind_kts": 12, "wind_dir": "ESE",
                              "swell_m": 0.8, "visibility_nm": 8},
            "ukc_inputs": {},
            "conflict_state": {"type": "berth_overlap", "severity": "high"},
            "constraints": {"pilotage_window": "0600-1800", "bridge_clearance_m": 38.0},
        },
        "alternatives": [],
        "recommended_option_id": str(uuid4()),
        "recommended_reasoning": "Hold for tidal window; delay vessel B by 90 min",
        "decision_deadline": datetime.now(timezone.utc).isoformat(),
        "data_provenance": {"ais_payload_ref": str(uuid4()),
                           "bom_tides_payload_ref": str(uuid4())},
    }
    # Pad with realistic vessel records until target size reached
    i = 0
    while len(audit.canonical_json(base)) < target_bytes:
        base["decision_snapshot"]["vessels"].append({
            "id": f"V-{i:04d}",
            "name": f"MV Decision Bench Vessel {i:04d}",
            "lat": -27.380 + i * 0.0001,
            "lon": 153.165 + i * 0.0001,
            "heading_deg": (i * 7) % 360,
            "speed_kts": 8.5 + (i % 5),
            "loa_m": 200 + (i % 100),
            "draft_m": 10.5 + (i % 10) * 0.1,
            "declared_eta": "2026-05-12T15:00:00+00:00",
            "ais_age_s": i % 60,
            "source": "AISSTREAM",
        })
        i += 1
    return base


# ── Benchmarks ────────────────────────────────────────────────────────

def bench_single_tenant_sequential(connect, n_events: int, payload_fn, label: str) -> dict:
    """Sequential insert benchmark — one tenant, one connection, N events.

    Payloads are pre-built before timing starts so that the measured
    throughput reflects the audit writer only, not payload construction.
    Decision-snapshot payload generation is O(n²) because it grows the
    payload byte-by-byte until the target size is met; we don't want
    that in the measured loop.
    """
    tenant_id = f"bench-seq-{uuid4().hex[:8]}"
    conn = connect()
    try:
        latencies_ms: list[float] = []

        # Pre-build all payloads OUTSIDE the timed loop.
        payloads = [payload_fn() for _ in range(n_events)]
        sample_size = len(audit.canonical_json(payloads[0]))
        total_payload_bytes = sum(len(audit.canonical_json(p)) for p in payloads)

        start = time.perf_counter()
        for i, payload in enumerate(payloads):
            t0 = time.perf_counter()
            audit.emit(
                conn, tenant_id,
                event_type="VESSEL_STATE_OBSERVED",
                subject_type="vessel", subject_id=f"V-{i}",
                payload=payload,
            )
            latencies_ms.append((time.perf_counter() - t0) * 1000)
        elapsed = time.perf_counter() - start

        rate = n_events / elapsed
        avg_payload_bytes = total_payload_bytes / n_events

        # Cleanup
        with conn.cursor() as cur:
            cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tenant_id,))
        conn.commit()

        return {
            "label": label,
            "n_events": n_events,
            "elapsed_s": round(elapsed, 3),
            "events_per_sec": round(rate, 1),
            "avg_payload_bytes": int(avg_payload_bytes),
            "sample_payload_bytes": sample_size,
            "latency_ms_p50": round(statistics.median(latencies_ms), 2),
            "latency_ms_p95": round(_percentile(latencies_ms, 0.95), 2),
            "latency_ms_p99": round(_percentile(latencies_ms, 0.99), 2),
            "latency_ms_max": round(max(latencies_ms), 2),
            "meets_target": rate >= 100,
        }
    finally:
        conn.close()


def bench_multi_tenant_concurrent(connect, n_tenants: int, events_per_tenant: int, payload_fn, label: str) -> dict:
    """
    Concurrent insert benchmark — N tenants writing in parallel, each
    with its own connection. Tests that per-tenant advisory locks do
    NOT cross-contend (tenants should scale near-linearly).
    """
    tenant_ids = [f"bench-mt-{uuid4().hex[:8]}" for _ in range(n_tenants)]
    per_tenant_results: dict[str, dict] = {}
    barrier = threading.Barrier(n_tenants)

    def writer(tid: str):
        # Pre-build payloads OUTSIDE the timed region.
        payloads = [payload_fn() for _ in range(events_per_tenant)]
        c = connect()
        try:
            # Wait until all threads are ready, then go simultaneously
            barrier.wait()
            t0 = time.perf_counter()
            for i, p in enumerate(payloads):
                audit.emit(
                    c, tid,
                    event_type="VESSEL_STATE_OBSERVED",
                    subject_type="vessel", subject_id=f"V-{i}",
                    payload=p,
                )
            elapsed = time.perf_counter() - t0
            per_tenant_results[tid] = {
                "events": events_per_tenant,
                "elapsed_s": elapsed,
                "events_per_sec": events_per_tenant / elapsed,
            }
        finally:
            c.close()

    threads = [threading.Thread(target=writer, args=(tid,)) for tid in tenant_ids]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_elapsed = time.perf_counter() - start

    total_events = n_tenants * events_per_tenant
    total_rate = total_events / total_elapsed
    per_tenant_rates = [r["events_per_sec"] for r in per_tenant_results.values()]
    min_per_tenant = min(per_tenant_rates)
    max_per_tenant = max(per_tenant_rates)
    mean_per_tenant = statistics.mean(per_tenant_rates)

    # Cleanup
    cleanup = connect()
    try:
        with cleanup.cursor() as cur:
            for tid in tenant_ids:
                cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tid,))
        cleanup.commit()
    finally:
        cleanup.close()

    # Compute "scaling factor" — if no cross-tenant contention, total
    # throughput should be roughly N × per-tenant throughput
    single_baseline_rate = 0  # we don't measure this in this function; recorded externally
    return {
        "label": label,
        "n_tenants": n_tenants,
        "events_per_tenant": events_per_tenant,
        "total_events": total_events,
        "elapsed_s": round(total_elapsed, 3),
        "total_events_per_sec": round(total_rate, 1),
        "per_tenant_min_events_per_sec": round(min_per_tenant, 1),
        "per_tenant_max_events_per_sec": round(max_per_tenant, 1),
        "per_tenant_mean_events_per_sec": round(mean_per_tenant, 1),
        "meets_target_min_tenant": min_per_tenant >= 100,
    }


def bench_chain_verification(connect, n_events: int) -> dict:
    """Verify a freshly-built N-row chain; measure rows/sec."""
    tenant_id = f"bench-verify-{uuid4().hex[:8]}"
    conn = connect()
    try:
        for i in range(n_events):
            audit.emit(
                conn, tenant_id,
                event_type="VESSEL_STATE_OBSERVED",
                subject_type="vessel", subject_id=f"V-{i}",
                payload=make_medium_payload(),
            )
        conn.commit()

        start = time.perf_counter()
        result = audit.verify_chain(conn, tenant_id)
        elapsed = time.perf_counter() - start

        # Cleanup
        with conn.cursor() as cur:
            cur.execute("DELETE FROM audit.events WHERE tenant_id = %s", (tenant_id,))
        conn.commit()

        return {
            "n_events": n_events,
            "elapsed_s": round(elapsed, 3),
            "rows_per_sec": round(result["checked"] / elapsed, 1) if elapsed > 0 else float("inf"),
            "ok": result["ok"],
            "checked": result["checked"],
        }
    finally:
        conn.close()


# ── Helpers ───────────────────────────────────────────────────────────

def _percentile(values: list[float], p: float) -> float:
    """Linear-interpolation percentile."""
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


# ── Main ──────────────────────────────────────────────────────────────

def main() -> int:
    import psycopg

    def connect():
        return psycopg.connect(DATABASE_URL)

    results: list[dict] = []

    print()
    print("=" * 70)
    print("Horizon audit writer benchmark — Phase 0.5c per ADR-002")
    print("=" * 70)
    print(f"  Target: ≥ 100 events/sec/tenant")
    print(f"  DATABASE_URL: postgresql://...{DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else DATABASE_URL.split('//')[-1]}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print()

    # ── Single-tenant sequential, three payload sizes ─────────────────
    print("── Phase 1: single-tenant sequential ──────────────────────────")
    for label, n, payload_fn in [
        ("small payload (session events)",   1000, make_small_payload),
        ("medium payload (conflicts/alerts)",  500, make_medium_payload),
        ("decision snapshot ~32 KB",            200, make_decision_snapshot_payload),
    ]:
        r = bench_single_tenant_sequential(connect, n, payload_fn, label)
        results.append({"category": "single_tenant_sequential", **r})
        ok = "✓" if r["meets_target"] else "✗"
        print(f"  {ok} {label}")
        print(f"      events={r['n_events']} elapsed={r['elapsed_s']}s "
              f"rate={r['events_per_sec']}/s payload={r['avg_payload_bytes']}B")
        print(f"      latency p50={r['latency_ms_p50']}ms p95={r['latency_ms_p95']}ms "
              f"p99={r['latency_ms_p99']}ms max={r['latency_ms_max']}ms")

    print()
    print("── Phase 2: multi-tenant concurrent ───────────────────────────")
    for label, n_tenants, per_tenant, payload_fn in [
        ("4 tenants × 200 events, medium payload",  4, 200, make_medium_payload),
        ("8 tenants × 100 events, medium payload",  8, 100, make_medium_payload),
    ]:
        r = bench_multi_tenant_concurrent(connect, n_tenants, per_tenant, payload_fn, label)
        results.append({"category": "multi_tenant_concurrent", **r})
        ok = "✓" if r["meets_target_min_tenant"] else "✗"
        print(f"  {ok} {label}")
        print(f"      total={r['total_events']} elapsed={r['elapsed_s']}s "
              f"total_rate={r['total_events_per_sec']}/s")
        print(f"      per-tenant min={r['per_tenant_min_events_per_sec']}/s "
              f"mean={r['per_tenant_mean_events_per_sec']}/s "
              f"max={r['per_tenant_max_events_per_sec']}/s")

    print()
    print("── Phase 3: chain verification ────────────────────────────────")
    for n in [100, 1000]:
        r = bench_chain_verification(connect, n)
        results.append({"category": "chain_verification", **r})
        ok = "✓" if r["ok"] else "✗"
        print(f"  {ok} verify {n}-row chain: {r['rows_per_sec']}/s "
              f"({r['elapsed_s']}s total)")

    print()
    print("─" * 70)
    print("Summary written to stdout above. JSON results below for")
    print("PERFORMANCE.md consumption:")
    print("─" * 70)
    print(json.dumps({
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "target_events_per_sec_per_tenant": 100,
        "results": results,
    }, indent=2))

    # Exit nonzero if any target missed
    failed = [r for r in results if r.get("meets_target") is False
              or r.get("meets_target_min_tenant") is False
              or r.get("ok") is False]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
