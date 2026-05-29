"""
movements/normalise.py — Normalisation utilities for movement feeds.

Pure helpers:
  - normalise_name()        : canonical vessel-name key for matching
  - local_to_utc_iso()      : tz-aware local datetime -> ISO-8601 UTC string
  - parse_local_datetime()  : tolerant parse of feed date/time strings -> UTC ISO
  - parse_updated_timestamp(): feed "Updated …" string -> epoch seconds
  - classify_movement()     : table/section title -> (movement_type, is_actual)

Timezone handling uses stdlib zoneinfo (DST-correct). All functions are
deterministic; none read the clock.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional, Tuple

from movements.base import ARRIVAL, DEPARTURE, IN_PORT

try:
    from zoneinfo import ZoneInfo
    _HAVE_ZONEINFO = True
except Exception:  # pragma: no cover - zoneinfo is stdlib on 3.9+
    _HAVE_ZONEINFO = False

# Common maritime name prefixes to strip for matching.
_NAME_PREFIXES = ("MV", "M/V", "MT", "M/T", "SS", "MSC", "RV")

# Date/time formats we attempt, in order, for feed event times.
_DT_FORMATS = (
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d %b %Y %H:%M",
    "%d %B %Y %H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
    "%d/%m/%y %H:%M",
)

# Formats for the page-level "Updated …" banner.
_UPDATED_FORMATS = (
    "%A, %B, %d, %Y, %H:%M",   # "Friday, May, 29, 2026, 13:10"
    "%A, %d %B %Y, %H:%M",
    "%d %B %Y, %H:%M",
    "%d/%m/%Y %H:%M",
)


def _zone(tz_name: str):
    if _HAVE_ZONEINFO:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return None
    return None


def normalise_name(name: Optional[str]) -> str:
    """Canonical key for vessel-name matching: drop a leading maritime prefix,
    uppercase, strip all non-alphanumerics. 'MV Bass Strait' -> 'BASSSTRAIT'."""
    if not name:
        return ""
    s = name.strip()
    # Strip a single leading prefix token if present.
    parts = s.split()
    if parts and parts[0].upper().rstrip(".") in _NAME_PREFIXES:
        s = " ".join(parts[1:])
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()


def local_to_utc_iso(dt_naive: datetime, tz_name: str) -> str:
    """Attach tz_name to a naive local datetime, convert to UTC, format ISO-Z.
    Falls back to treating the value as UTC if the zone is unavailable."""
    tz = _zone(tz_name)
    if tz is None:
        aware = dt_naive.replace(tzinfo=timezone.utc)
    else:
        aware = dt_naive.replace(tzinfo=tz)
    return aware.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_local_datetime(text: Optional[str], tz_name: str) -> Optional[str]:
    """Tolerantly parse a feed date/time string in local time → UTC ISO string.
    Returns None if no format matches (caller keeps the record, time unresolved)."""
    if not text:
        return None
    raw = text.strip()
    for fmt in _DT_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return local_to_utc_iso(dt, tz_name)
        except ValueError:
            continue
    return None


def parse_updated_timestamp(text: Optional[str], tz_name: str) -> Optional[float]:
    """Parse a feed 'Updated …' banner into epoch seconds (UTC). Returns None
    if unparseable. Leading 'Updated' (any case) and surrounding whitespace are
    stripped first."""
    if not text:
        return None
    raw = re.sub(r"^\s*updated\s*", "", text.strip(), flags=re.IGNORECASE).strip()
    tz = _zone(tz_name)
    for fmt in _UPDATED_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            aware = dt.replace(tzinfo=tz or timezone.utc)
            return aware.timestamp()
        except ValueError:
            continue
    return None


def classify_movement(title: Optional[str]) -> Optional[Tuple[str, bool]]:
    """Map a PV table/section title to (movement_type, is_actual).

    'Expected Arrivals'   -> (ARRIVAL,   False)
    'Actual Arrivals'     -> (ARRIVAL,   True)
    'Expected Departures' -> (DEPARTURE, False)
    'Actual Departures'   -> (DEPARTURE, True)
    'In Port'             -> (IN_PORT,   True)
    Returns None for an unrecognised title.
    """
    if not title:
        return None
    t = title.strip().lower()
    if "in port" in t:
        return (IN_PORT, True)
    is_actual = "actual" in t   # "actual" => confirmed event; otherwise expected/estimated
    if "arriv" in t:
        return (ARRIVAL, is_actual)
    if "depart" in t:
        return (DEPARTURE, is_actual)
    return None
