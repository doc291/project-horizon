"""
tests/test_movements.py — Beta 11 Slice 2A: public movement adapters.

Covers Ports Victoria five-table parsing, the QShips public wrapper,
normalisation, timezone handling, expected-vs-actual classification,
vessel-name matching, unresolved schedule-only handling, authority/provenance
tagging, and failure / stale-data handling.

Pure unit tests — no network. The PV adapter is tested via its pure
`parse_html`; the QShips wrapper via an injected `rows_provider`. No /api/summary
or Decision Card wiring is exercised.
"""

import pytest

from authority.categories import AuthorityCategory
from movements.base import (
    MovementRecord, MovementFetchResult,
    PublicShipMovementsAdapter, ARRIVAL, DEPARTURE, IN_PORT,
    SERVED_LIVE, SERVED_LAST_GOOD, SERVED_NONE,
)
from movements.normalise import (
    normalise_name, parse_local_datetime, parse_updated_timestamp, classify_movement,
)
from movements.matching import (
    match_movement_to_ais, enrich_with_identifiers,
    MATCH_EXACT, MATCH_NORMALISED, MATCH_UNRESOLVED,
)
from movements.ports_victoria import PortsVictoriaMovementsAdapter
from movements.qships_public import QShipsPublicMovementsAdapter


# ── Normalisation ────────────────────────────────────────────────────────────

def test_normalise_name_strips_prefix_and_punctuation():
    assert normalise_name("MV Bass Strait") == "BASSSTRAIT"
    assert normalise_name("M/V Iron Monarch") == "IRONMONARCH"
    assert normalise_name("Pacific Topaz") == "PACIFICTOPAZ"
    assert normalise_name("SPIRIT OF TASMANIA II") == "SPIRITOFTASMANIAII"
    assert normalise_name("") == ""
    assert normalise_name(None) == ""


def test_normalise_name_does_not_strip_embedded_prefix():
    # "MViking" should not lose "MV" (not a separate token)
    assert normalise_name("MViking Star") == "MVIKINGSTAR"


# ── Timezone handling ────────────────────────────────────────────────────────

def test_parse_local_datetime_melbourne_aedt():
    # 15 Jan = AEDT (UTC+11) → 14:30 local = 03:30Z
    assert parse_local_datetime("15/01/2026 14:30", "Australia/Melbourne") == "2026-01-15T03:30:00Z"


def test_parse_local_datetime_brisbane_aest():
    # Brisbane has no DST (UTC+10) → 14:30 = 04:30Z
    assert parse_local_datetime("15/07/2026 14:30", "Australia/Brisbane") == "2026-07-15T04:30:00Z"


def test_parse_local_datetime_multiple_formats():
    assert parse_local_datetime("29 May 2026 13:10", "Australia/Melbourne") is not None
    assert parse_local_datetime("2026-05-29 13:10", "Australia/Melbourne") is not None


def test_parse_local_datetime_unparseable_returns_none():
    assert parse_local_datetime("not a date", "Australia/Melbourne") is None
    assert parse_local_datetime("", "Australia/Melbourne") is None
    assert parse_local_datetime(None, "Australia/Melbourne") is None


def test_parse_updated_timestamp_pv_banner():
    # "Updated Friday, May, 29, 2026, 13:10" (Melbourne) → epoch
    ts = parse_updated_timestamp("Updated Friday, May, 29, 2026, 13:10", "Australia/Melbourne")
    assert ts is not None and ts > 0


def test_parse_updated_timestamp_unparseable():
    assert parse_updated_timestamp("garbage", "Australia/Melbourne") is None
    assert parse_updated_timestamp(None, "Australia/Melbourne") is None


# ── Expected vs actual classification ────────────────────────────────────────

def test_classify_movement_all_five_sections():
    assert classify_movement("Expected Arrivals") == (ARRIVAL, False)
    assert classify_movement("Actual Arrivals") == (ARRIVAL, True)
    assert classify_movement("Expected Departures") == (DEPARTURE, False)
    assert classify_movement("Actual Departures") == (DEPARTURE, True)
    assert classify_movement("In Port") == (IN_PORT, True)


def test_classify_movement_unknown_returns_none():
    assert classify_movement("Random Heading") is None
    assert classify_movement("") is None
    assert classify_movement(None) is None


# ── Ports Victoria five-table parsing ────────────────────────────────────────

_PV_HTML = """
<html><body>
<h2>Expected Arrivals</h2>
<table>
  <tr><th>Ship Name</th><th>Date &amp; Time</th><th>From</th><th>To</th><th>Agent</th></tr>
  <tr><td>MV Bass Strait</td><td>29/05/2026 14:30</td><td>Sydney</td><td>Webb Dock</td><td>GAC</td></tr>
  <tr><td>Pacific Topaz</td><td>29/05/2026 16:00</td><td>Geelong</td><td>Swanson</td><td>Monson</td></tr>
</table>
<h2>Actual Arrivals</h2>
<table>
  <tr><th>Ship Name</th><th>Date &amp; Time</th><th>From</th><th>To</th><th>Agent</th></tr>
  <tr><td>Iron Monarch</td><td>29/05/2026 09:15</td><td>Newcastle</td><td>Berth 2</td><td>Wilhelmsen</td></tr>
</table>
<h2>Expected Departures</h2>
<table>
  <tr><th>Ship Name</th><th>Date &amp; Time</th><th>From</th><th>To</th><th>Agent</th></tr>
  <tr><td>Akuna</td><td>30/05/2026 06:00</td><td>Webb Dock</td><td>Singapore</td><td>Borthwick</td></tr>
</table>
<h2>Actual Departures</h2>
<table>
  <tr><th>Ship Name</th><th>Date &amp; Time</th><th>From</th><th>To</th><th>Agent</th></tr>
  <tr><td>Spirit of Tasmania II</td><td>28/05/2026 19:45</td><td>Station Pier</td><td>Devonport</td><td>TT-Line</td></tr>
</table>
<h2>In Port</h2>
<table>
  <tr><th>Ship Name</th><th>Berth</th><th>Arrived</th><th>ETD</th><th>To</th><th>Agent</th></tr>
  <tr><td>Cape Venture</td><td>Corio Quay</td><td>27/05/2026 08:00</td><td>30/05/2026 12:00</td><td>Brisbane</td><td>Inchcape</td></tr>
</table>
<p>Updated Friday, May, 29, 2026, 13:10</p>
</body></html>
"""


def _pv_parse():
    return PortsVictoriaMovementsAdapter().parse_html(_PV_HTML, tz_name="Australia/Melbourne")


def test_pv_parses_all_five_tables():
    res = _pv_parse()
    assert res.ok is True
    assert res.served_from == SERVED_LIVE
    # 2 + 1 + 1 + 1 + 1 = 6 records
    assert res.count == 6


def test_pv_movement_types_and_actual_flags():
    res = _pv_parse()
    by_name = {r.vessel_name: r for r in res.records}
    assert by_name["MV Bass Strait"].movement_type == ARRIVAL
    assert by_name["MV Bass Strait"].is_actual is False          # Expected Arrivals
    assert by_name["Iron Monarch"].is_actual is True             # Actual Arrivals
    assert by_name["Akuna"].movement_type == DEPARTURE
    assert by_name["Akuna"].is_actual is False                   # Expected Departures
    assert by_name["Spirit of Tasmania II"].is_actual is True    # Actual Departures
    assert by_name["Cape Venture"].movement_type == IN_PORT


def test_pv_fields_and_tz_conversion():
    res = _pv_parse()
    bass = next(r for r in res.records if r.vessel_name == "MV Bass Strait")
    assert bass.from_loc == "Sydney"
    assert bass.to_loc == "Webb Dock"
    assert bass.agent == "GAC"
    # 29/05/2026 14:30 Melbourne (AEST in May, UTC+10) → 04:30Z
    assert bass.event_time == "2026-05-29T04:30:00Z"


def test_pv_source_updated_at_parsed():
    res = _pv_parse()
    assert res.source_updated_at is not None and res.source_updated_at > 0


def test_pv_records_carry_confirmed_published_provenance():
    res = _pv_parse()
    for r in res.records:
        assert r.provenance.category is AuthorityCategory.CONFIRMED_PUBLISHED
        assert r.provenance.source == "Ports Victoria"
        # observed_at flows from the page "Updated …" banner
        assert r.provenance.observed_at == res.source_updated_at


def test_pv_normalised_name_populated():
    res = _pv_parse()
    bass = next(r for r in res.records if r.vessel_name == "MV Bass Strait")
    assert bass.normalised_name == "BASSSTRAIT"


# ── Ports Victoria failure / stale handling ──────────────────────────────────

class _BoomPV(PortsVictoriaMovementsAdapter):
    def _get_html(self, url):
        raise RuntimeError("network down")


def test_pv_fetch_no_url_returns_not_ok():
    res = PortsVictoriaMovementsAdapter().fetch({}, now=1000.0)
    assert res.ok is False
    assert res.served_from == SERVED_NONE


def test_pv_fetch_failure_without_last_good():
    res = _BoomPV().fetch({"movements_url": "http://x"}, now=1000.0)
    assert res.ok is False
    assert res.served_from == SERVED_NONE
    assert res.error is not None


def test_pv_fetch_failure_serves_last_good_stale():
    adapter = PortsVictoriaMovementsAdapter()
    # seed last-good via the pure parser
    adapter._last_good = adapter.parse_html(_PV_HTML, tz_name="Australia/Melbourne")
    # now force the live path to fail
    adapter._get_html = lambda url: (_ for _ in ()).throw(RuntimeError("boom"))
    res = adapter.fetch({"movements_url": "http://x"}, now=2000.0)
    assert res.ok is False
    assert res.stale is True
    assert res.served_from == SERVED_LAST_GOOD
    assert res.count == 6  # last-good records preserved


# ── QShips public wrapper ────────────────────────────────────────────────────

_QSHIPS_ROWS = [
    {"name": "HIGHLAND CHIEF", "status": "confirmed", "eta": "2026-05-29T20:00:00Z", "berth_name": "B03"},
    {"name": "DIONE LEADER", "status": "berthed", "eta": "2026-05-29T10:00:00Z",
     "etd": "2026-05-30T06:00:00Z", "berth_name": "B04"},
    {"name": "VIKING PASSAMA", "status": "sailed", "etd": "2026-05-29T08:00:00Z", "berth_name": "B05"},
    {"name": "", "status": "confirmed"},  # nameless row dropped
]


def test_qships_public_wrapper_maps_rows():
    adapter = QShipsPublicMovementsAdapter(rows_provider=lambda: _QSHIPS_ROWS)
    res = adapter.fetch({}, now=5000.0)
    assert res.ok is True
    assert res.count == 3  # nameless row dropped
    by_name = {r.vessel_name: r for r in res.records}
    assert by_name["HIGHLAND CHIEF"].movement_type == ARRIVAL
    assert by_name["HIGHLAND CHIEF"].is_actual is False          # "confirmed" = expected
    assert by_name["DIONE LEADER"].movement_type == IN_PORT      # "berthed"
    assert by_name["DIONE LEADER"].event_time == "2026-05-30T06:00:00Z"  # in_port → etd
    assert by_name["VIKING PASSAMA"].movement_type == DEPARTURE  # "sailed"


def test_qships_public_records_are_confirmed_published_not_system():
    adapter = QShipsPublicMovementsAdapter(rows_provider=lambda: _QSHIPS_ROWS)
    res = adapter.fetch({}, now=5000.0)
    for r in res.records:
        assert r.provenance.category is AuthorityCategory.CONFIRMED_PUBLISHED
        assert r.provenance.category is not AuthorityCategory.CONFIRMED_SYSTEM  # never SOAP-labelled
        assert r.provenance.source == "QShips (public)"


def test_qships_public_failure_serves_last_good_stale():
    rows = list(_QSHIPS_ROWS)
    state = {"fail": False}

    def provider():
        if state["fail"]:
            raise RuntimeError("scrape failed")
        return rows

    adapter = QShipsPublicMovementsAdapter(rows_provider=provider)
    first = adapter.fetch({}, now=5000.0)
    assert first.ok is True and first.count == 3
    state["fail"] = True
    second = adapter.fetch({}, now=6000.0)
    assert second.ok is False
    assert second.stale is True
    assert second.served_from == SERVED_LAST_GOOD
    assert second.count == 3


def test_qships_public_authority_category_method():
    adapter = QShipsPublicMovementsAdapter(rows_provider=lambda: [])
    assert adapter.authority_category() is AuthorityCategory.CONFIRMED_PUBLISHED
    assert "public" in adapter.source_name().lower()


# ── Adapters satisfy the Protocol ────────────────────────────────────────────

def test_adapters_satisfy_protocol():
    assert isinstance(PortsVictoriaMovementsAdapter(), PublicShipMovementsAdapter)
    assert isinstance(QShipsPublicMovementsAdapter(rows_provider=lambda: []),
                      PublicShipMovementsAdapter)


# ── Vessel matching ──────────────────────────────────────────────────────────

_AIS = [
    {"name": "BASS STRAIT", "loa": 189.2, "mmsi": "503001"},
    {"name": "MV Iron Monarch", "loa": 175.0, "mmsi": "503002"},
]


def _rec(name):
    return MovementRecord(
        vessel_name=name, normalised_name=normalise_name(name),
        movement_type=ARRIVAL, is_actual=False,
        provenance=__import__("authority").assumed(),
    )


def test_match_exact():
    m = match_movement_to_ais(_rec("BASS STRAIT"), _AIS)
    assert m.method == MATCH_EXACT and m.matched is True
    assert m.ais_vessel["mmsi"] == "503001"


def test_match_normalised():
    # "MV Bass Strait" vs AIS "BASS STRAIT" — differ by prefix/case
    m = match_movement_to_ais(_rec("MV Bass Strait"), _AIS)
    assert m.method == MATCH_NORMALISED and m.matched is True
    assert m.ais_vessel["mmsi"] == "503001"


def test_match_unresolved_schedule_only():
    m = match_movement_to_ais(_rec("Ghost Ship"), _AIS)
    assert m.method == MATCH_UNRESOLVED
    assert m.matched is False
    assert m.is_schedule_only is True
    assert m.ais_vessel is None


def test_enrichment_hook_is_not_implemented():
    with pytest.raises(NotImplementedError):
        enrich_with_identifiers()


# ── MovementRecord validation ────────────────────────────────────────────────

def test_movement_record_rejects_bad_type():
    import authority
    with pytest.raises(ValueError):
        MovementRecord(
            vessel_name="X", normalised_name="X",
            movement_type="teleport", is_actual=False,
            provenance=authority.assumed(),
        )
