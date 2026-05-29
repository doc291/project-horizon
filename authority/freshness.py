"""
authority/freshness.py — Freshness decay for time-stamped authority elements.

A pure function mapping (observed_at, now, half_life) → a freshness factor in
[0, 1]. Exponential half-life decay: a brand-new observation scores 1.0; at
one half-life it scores 0.5; it asymptotes toward 0 as the element ages.

`now` is always passed in explicitly — the module performs no clock reads, so
it is fully deterministic and unit-testable.
"""

from __future__ import annotations

from typing import Optional


def freshness_factor(
    observed_at: Optional[float],
    now: float,
    *,
    half_life_s: float,
) -> float:
    """Exponential-decay freshness in [0, 1].

    - observed_at is None  -> 0.0  (no observation = no freshness credit)
    - age <= 0 (future/now) -> 1.0  (clamp; clock skew never exceeds 1.0)
    - otherwise            -> 0.5 ** (age / half_life_s)

    Raises ValueError if half_life_s <= 0.
    """
    if half_life_s <= 0:
        raise ValueError("half_life_s must be > 0")
    if observed_at is None:
        return 0.0
    age = now - observed_at
    if age <= 0:
        return 1.0
    return 0.5 ** (age / half_life_s)
