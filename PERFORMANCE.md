# Project Horizon — Performance Benchmarks

Phase 0 / Step 0.5c per ADR-002. Documents audit writer throughput,
multi-tenant scaling, and chain verification speed. Run repeatedly with
`scripts/bench_audit.py` against any Postgres 16 instance that has the
audit schema applied (migration 0004).

## Target (ADR-002 §5.1)

**≥ 100 events / second / tenant sustained.**

## Test environment (this run)

- macOS / Apple Silicon
- Python 3.10.20, psycopg 3.3.4
- Postgres 16.13 (Homebrew), local Unix socket
- Single Postgres instance, default shared buffers
- Date: 2026-05-12

## Results — all four phases meet target

### Phase 1: single-tenant sequential

| Payload | Avg size | Events | Elapsed | Events/sec | p50 latency | p95 | p99 | max | Target met |
|---|---|---|---|---|---|---|---|---|---|
| Small (session events) | 174 B | 1,000 | 0.61 s | **1,647** | 0.5 ms | 0.8 ms | 2.9 ms | 15 ms | ✓ 16× |
| Medium (conflicts/alerts) | 760 B | 500 | 0.29 s | **1,756** | 0.5 ms | 0.8 ms | 2.5 ms | 6 ms | ✓ 17× |
| Decision snapshot (~32 KB) | 32,065 B | 200 | 1.00 s | **201** | 4.3 ms | 7.7 ms | 29.6 ms | 40 ms | ✓ 2× |

**Reading:** even at the upper bound of decision-snapshot payload size,
the writer comfortably exceeds the 100/s/tenant target. The p50 latency
of 4.3 ms for 32 KB payloads indicates Postgres jsonb insert + index
maintenance dominates the cost, not Python overhead.

### Phase 2: multi-tenant concurrent (medium payloads)

| Configuration | Total events | Elapsed | Total events/sec | Per-tenant min | Per-tenant mean | Per-tenant max | Target met (min) |
|---|---|---|---|---|---|---|---|
| 4 tenants × 200 events | 800 | 0.28 s | **2,905** | 775 | 780 | 783 | ✓ 7.7× |
| 8 tenants × 100 events | 800 | 0.36 s | **2,251** | 293 | 297 | 304 | ✓ 2.9× |

**Reading:** as tenant count grows, total throughput rises but
per-tenant throughput falls due to database-resource contention
(WAL writer, shared buffers, single Postgres CPU on this host). At
8 tenants writing concurrently, each tenant still gets ~300 events/sec
— well above the 100/s target. Crucially, the per-tenant min and max
are tightly clustered (293 vs 304 at 8 tenants, 775 vs 783 at 4
tenants), confirming the advisory-lock isolation is fair across tenants.

### Phase 3: chain verification

| Chain length | Elapsed | Rows/sec | Outcome |
|---|---|---|---|
| 100 rows | 5 ms | 19,560 | ok ✓ |
| 1,000 rows | 35 ms | 28,417 | ok ✓ |

**Reading:** chain verification (recompute `payload_hash` from jsonb,
recompute `row_hash` from fields, verify `prev_hash` linkage) costs
roughly 35 µs per row at scale. A full 7-year retention chain of 1M
rows would verify in ~35 seconds. Throughput grows with chain size as
fixed connection / parse overhead amortises.

## Advisory-lock behaviour (design and observed)

The audit writer uses `pg_advisory_xact_lock(int8)` keyed by a stable
64-bit hash of `tenant_id` (`_tenant_lock_key()` in `audit.py`). Two
properties matter:

1. **Per-tenant serialisation:** all inserts for one tenant are
   serialised by the lock, so `sequence_no` assignment is gap-free
   and the hash chain is well-formed even under concurrent writers.

2. **Cross-tenant independence:** different tenants hash to different
   lock keys, so their inserts do NOT contend on the advisory lock.
   Postgres' lock manager treats them as distinct.

The observed multi-tenant scaling (Phase 2) confirms (2): per-tenant
throughput is tight across all tenants and degrades smoothly with
concurrency — the degradation pattern matches database-resource
contention (WAL, shared buffers), not lock contention.

If lock contention were the bottleneck, we would expect occasional
multi-second outliers in tenant write latency as threads serialised
on a shared lock. None observed.

## Chain integrity under concurrent load

Every multi-tenant concurrent run is followed by chain-verification
during cleanup. Across all runs in development, `verify_chain()` has
returned `ok=True` 100% of the time. Concurrent writes to different
tenants do not corrupt either chain.

## Bottleneck analysis

Per-tenant emit() cost breaks down approximately as:

| Step | Approx cost | Notes |
|---|---|---|
| Python: build canonical JSON + payload_hash | < 1 ms (small/medium); ~3 ms (32 KB) | grows with payload size |
| Postgres: acquire advisory lock | < 0.1 ms | minimal — no contention across tenants |
| Postgres: read MAX(sequence_no) | < 0.5 ms | uses (tenant_id, sequence_no) PK |
| Postgres: read prior row's row_hash | < 0.5 ms | indexed |
| Python: compute row_hash | < 0.1 ms | one SHA-256 |
| Postgres: INSERT into audit.events | ~0.5–3 ms | grows with payload size + index updates |

Total p50: ~0.5 ms (medium) → ~4.3 ms (32 KB). The dominant cost at
large payload size is Postgres' jsonb insert + index maintenance, not
Python or the advisory lock.

## Recommendation: **ACCEPTABLE AS-IS for Phase 0.6**

The audit writer comfortably meets the ADR-002 target of ≥ 100
events/sec/tenant across all payload sizes and tenant counts tested.
No optimisation work required before Phase 0.6 wires session events.

Headroom assessment:
- Realistic production write rate per port: ~10–100 events/minute
  (dozens of vessels, several decisions per shift, periodic state
  observations) — that's 0.2–1.7 events/sec/tenant
- Measured throughput at 32 KB decision-snapshot payloads: 201/sec/tenant
- **Headroom: 100×–1000× over realistic production load**

If headroom ever becomes a concern (e.g. hundreds of customers per
Postgres instance, or per-tenant write rates orders of magnitude
higher than expected), candidate optimisations:

1. **Batch inserts** — emit multiple events per transaction (sacrifices
   strict per-event durability for throughput)
2. **Connection pooling** — reduce per-emit connection overhead
   (current benchmark creates new psycopg connections per writer thread)
3. **Separate audit DB** — move audit schema to its own Postgres
   instance (ADR-002 §1.10 v1.1 trigger). This isolates audit I/O
   from operational query load.
4. **Asynchronous emission** — application-side queue + background
   writer thread (sacrifices "audit is the side-effect of work" for
   throughput)

None of these are needed now.

## How to reproduce

```bash
# Local dev setup (one-time)
brew install postgresql@16
brew services start postgresql@16
createdb horizon_dev
python3 -m venv .venv-step03
source .venv-step03/bin/activate
pip install -r requirements-dev.txt

# Apply migrations
export DATABASE_URL=postgresql://localhost:5432/horizon_dev
alembic upgrade head

# Run benchmark
python3 scripts/bench_audit.py
```

The benchmark uses ephemeral `bench-...` tenant_ids and cleans up its
rows after every phase. Safe to run repeatedly against any dev DB.

## What this benchmark does NOT measure

- Production Railway / AWS deployment performance (different I/O, CPU,
  network characteristics)
- Sustained throughput over hours / days (this is a short-duration
  benchmark; no autovacuum, WAL archival, or backup pressure)
- Failure modes (Postgres restart mid-write, network partition,
  disk full)
- Behaviour at extreme chain length (1M+ rows per tenant)
- Read query patterns (audit ledger reads from investigators or
  CrowdStrike-style scans are a separate workload)

These are appropriate concerns for production readiness audits, not
Phase 0 acceptance.

## Run history

| Date (UTC) | Notable change | Single-tenant medium events/sec |
|---|---|---|
| 2026-05-12T02:04Z | Initial Phase 0.5c run | 1,756 |
