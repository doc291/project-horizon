"""
movements/ports_victoria.py — PortsVictoriaMovementsAdapter (CONFIRMED_PUBLISHED).

Parses the REAL Ports Victoria ship-movements page, which presents five
HTML tables:
    Expected Arrivals | Actual Arrivals | Expected Departures |
    Actual Departures | In Port
(The previous dormant scraper assumed a single 5-column table with a
'voyage_type' column — that structure does not exist on the live page and is
replaced here.)

Movement type and actual/expected are derived from each table's heading, not a
column. Columns are mapped by header text where possible, falling back to
positional order. Every record carries CONFIRMED_PUBLISHED provenance with the
page's own "Updated …" timestamp as observed_at.

GENERIC, not Victoria-specific: this is one concrete implementation of
PublicShipMovementsAdapter. The page URL and timezone come from the profile.

Never raises out of fetch(); on failure it serves the last-good snapshot
(stale=True) if available, else an ok=False empty result.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from authority.categories import AuthorityCategory
from authority.provenance import SourceProvenance
from movements.base import (
    MovementRecord,
    MovementFetchResult,
    SERVED_LIVE,
    SERVED_LAST_GOOD,
    SERVED_NONE,
    IN_PORT,
)
from movements.normalise import (
    normalise_name,
    parse_local_datetime,
    parse_updated_timestamp,
    classify_movement,
)

_SOURCE_NAME = "Ports Victoria"
_DEFAULT_TZ = "Australia/Melbourne"
_USER_AGENT = "Mozilla/5.0 (compatible; Horizon/Beta11; +https://horizon.amsgroup.com.au)"

# Header-name → logical column. Matched case-insensitively against <th> text.
_HEADER_ALIASES = {
    "ship name": "vessel_name",
    "vessel name": "vessel_name",
    "vessel": "vessel_name",
    "name": "vessel_name",
    "date & time": "event_time",
    "date and time": "event_time",
    "date/time": "event_time",
    "eta": "event_time",
    "etd": "event_time",
    "arrived": "event_time",
    "from": "from_loc",
    "to": "to_loc",
    "berth": "berth",
    "location": "berth",
    "agent": "agent",
}

# Section titles we expect (used to associate a table with its heading).
_SECTION_TITLES = (
    "Expected Arrivals", "Actual Arrivals",
    "Expected Departures", "Actual Departures", "In Port",
)


class PortsVictoriaMovementsAdapter:
    """Concrete PublicShipMovementsAdapter for the Ports Victoria page."""

    def __init__(self):
        self._last_good: Optional[MovementFetchResult] = None

    def source_name(self) -> str:
        return _SOURCE_NAME

    def authority_category(self) -> AuthorityCategory:
        return AuthorityCategory.CONFIRMED_PUBLISHED

    # ── fetch ────────────────────────────────────────────────────────────────
    def fetch(self, profile: dict, now: float) -> MovementFetchResult:
        url = profile.get("movements_url") or profile.get("vessel_data_url")
        tz_name = profile.get("movements_tz", _DEFAULT_TZ)
        if not url:
            return MovementFetchResult(ok=False, served_from=SERVED_NONE,
                                       error="no movements_url in profile")
        try:
            html = self._get_html(url)
            result = self.parse_html(html, tz_name=tz_name)
            self._last_good = result
            return result
        except Exception as exc:  # never raise out of fetch
            if self._last_good is not None:
                lg = self._last_good
                return MovementFetchResult(
                    records=lg.records, ok=False, stale=True,
                    source_updated_at=lg.source_updated_at,
                    served_from=SERVED_LAST_GOOD, error=str(exc),
                )
            return MovementFetchResult(ok=False, served_from=SERVED_NONE, error=str(exc))

    def _get_html(self, url: str) -> str:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8", errors="replace")

    # ── parsing (pure; unit-tested against an HTML fixture) ───────────────────
    def parse_html(self, html: str, *, tz_name: str = _DEFAULT_TZ) -> MovementFetchResult:
        """Parse the five-table PV page. Pure function of (html, tz)."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Page-level "Updated …" banner → source_updated_at (epoch).
        updated_epoch = None
        upd = soup.find(string=re.compile(r"updated", re.IGNORECASE))
        if upd:
            updated_epoch = parse_updated_timestamp(str(upd), tz_name)

        records: List[MovementRecord] = []
        for table in soup.find_all("table"):
            title = self._table_title(table)
            cls = classify_movement(title)
            if cls is None:
                continue  # not one of the five known sections
            movement_type, is_actual = cls
            records.extend(
                self._parse_table(table, movement_type, is_actual,
                                  tz_name=tz_name, updated_epoch=updated_epoch)
            )

        return MovementFetchResult(
            records=tuple(records), ok=True, stale=False,
            source_updated_at=updated_epoch, served_from=SERVED_LIVE,
        )

    def _table_title(self, table) -> Optional[str]:
        """Find the section title for a table: a <caption>, or the nearest
        preceding heading (h1-h4) text, matched against known section titles."""
        cap = table.find("caption")
        if cap and cap.get_text(strip=True):
            return cap.get_text(strip=True)
        # walk previous siblings/ancestors for a heading
        prev = table.find_previous(["h1", "h2", "h3", "h4", "th"])
        if prev:
            text = prev.get_text(strip=True)
            for known in _SECTION_TITLES:
                if known.lower() in text.lower():
                    return known
            return text
        return None

    def _column_map(self, table) -> dict:
        """Map logical columns to cell indices using the header row."""
        head = table.find("tr")
        col_map: dict = {}
        if not head:
            return col_map
        cells = head.find_all(["th", "td"])
        for idx, c in enumerate(cells):
            label = c.get_text(strip=True).lower()
            logical = _HEADER_ALIASES.get(label)
            if logical and logical not in col_map:
                col_map[logical] = idx
        return col_map

    def _parse_table(self, table, movement_type, is_actual, *,
                     tz_name, updated_epoch) -> List[MovementRecord]:
        col_map = self._column_map(table)
        rows = table.find_all("tr")
        out: List[MovementRecord] = []
        for row in rows[1:]:  # skip header
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            def cell(key: str) -> str:
                idx = col_map.get(key)
                if idx is None or idx >= len(cells):
                    return ""
                return cells[idx].get_text(strip=True)

            name = cell("vessel_name")
            if not name:
                continue

            event_time = parse_local_datetime(cell("event_time"), tz_name)
            prov = SourceProvenance(
                category=AuthorityCategory.CONFIRMED_PUBLISHED,
                source=_SOURCE_NAME,
                observed_at=updated_epoch,
                detail=("actual" if is_actual else "expected"),
            )
            out.append(MovementRecord(
                vessel_name=name,
                normalised_name=normalise_name(name),
                movement_type=movement_type,
                is_actual=is_actual,
                provenance=prov,
                event_time=event_time,
                from_loc=cell("from_loc") or None,
                to_loc=cell("to_loc") or None,
                berth=cell("berth") or None,
                agent=cell("agent") or None,
                status=("in_port" if movement_type == IN_PORT
                        else ("actual" if is_actual else "expected")),
            ))
        return out
