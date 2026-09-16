"""The offline cassette inspector: it reports faults and dumps field structure.

Every test here builds a synthetic cassette or reuses the checked-in minimal one. The
inspector runs fully offline, so no network and no ``schwab-py`` are involved. The
fixtures cover the four cases the tool must handle: a quotes interaction, a chains
interaction, a bars interaction, and a gateway fault.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lake import inspect_cassette as inspect_module
from lake.cassette import Cassette, Interaction, dump_cassette
from lake.inspect_cassette import DUMPABLE_SURFACES, inspect_cassette, main

CASSETTES = Path(__file__).resolve().parents[1] / "cassettes"

QUOTES_BODY = {
    "SPY": {
        "assetMainType": "EQUITY",
        "realtime": True,
        "quote": {"bidPrice": 649.98, "quoteTime": 1787000100000},
        "fundamental": {"divYield": 1.28, "divExDate": "2026-09-18"},
        "regular": {"regularMarketTradeTime": 1787000100000},
        "extended": {"askPrice": 651.1},
        "reference": {"cusip": "111111111"},
    }
}
CHAINS_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "numberOfContracts": 1,
    "underlying": {"symbol": "SPY", "quoteTime": 1787000099000},
    "callExpDateMap": {
        "2026-09-18:25": {
            "650.0": [
                {
                    "putCall": "CALL",
                    "delta": 0.51,
                    "symbol": "SPY   260918C00650000",
                    "quoteTimeInLong": 1787000100000,
                }
            ]
        }
    },
    "putExpDateMap": {},
}
FAULT_BODY = {
    "fault": {
        "faultstring": "Body buffer overflow",
        "detail": {"errorcode": "protocol.http.TooBigBody"},
    }
}


def _cassette() -> Cassette:
    return Cassette(
        interactions=(
            Interaction("quotes", {"symbols": ["SPY"]}, 200, QUOTES_BODY, {}),
            Interaction("chains", {"symbol": "SPY"}, 200, CHAINS_BODY, {}),
            Interaction("chains", {"symbol": "BIG"}, 502, FAULT_BODY, {}),
        )
    )


def test_reports_the_fault_code_and_string():
    output = inspect_cassette(_cassette())
    assert "protocol.http.TooBigBody" in output
    assert "Body buffer overflow" in output
    assert "errorcode=" in output
    # The fault line carries the status of that interaction.
    assert "status=502" in output


def test_dumps_the_quote_block_field_names_and_time_values():
    output = inspect_cassette(_cassette())
    # Each block is named and its fields are dumped as field: type.
    assert "quote:" in output
    assert "bidPrice: float" in output
    assert "fundamental:" in output
    assert "reference:" in output
    # A time key shows its int-epoch value; a date key shows its ISO string.
    assert "quoteTime: int = 1787000100000" in output
    assert "divExDate: str = '2026-09-18'" in output


def test_dumps_exactly_one_chain_contract():
    output = inspect_cassette(_cassette())
    # The underlying block and one contract's fields appear.
    assert "underlying:" in output
    assert "putCall: str" in output
    assert "delta: float" in output
    assert "quoteTimeInLong: int = 1787000100000" in output
    # The top-level chains keys are listed.
    assert "callExpDateMap" in output


def test_output_is_extra_free():
    # No overflow bucket or debug scaffolding leaks into the diagnostics.
    assert "extra" not in inspect_cassette(_cassette())


def test_surface_filter_limits_to_one_surface():
    quotes_only = inspect_cassette(_cassette(), surface="quotes")
    assert "bidPrice: float" in quotes_only
    # Chains interactions, including the chains-endpoint fault, are excluded.
    assert "callExpDateMap" not in quotes_only
    assert "TooBigBody" not in quotes_only

    chains_only = inspect_cassette(_cassette(), surface="chains")
    assert "callExpDateMap" in chains_only
    assert "TooBigBody" in chains_only
    assert "bidPrice" not in chains_only


def test_main_reads_a_written_cassette_and_prints(capsys, tmp_path):
    path = tmp_path / "recorded.json"
    dump_cassette(_cassette(), path)
    assert main([str(path)]) == 0
    out = capsys.readouterr().out
    assert "protocol.http.TooBigBody" in out
    assert "bidPrice: float" in out


def test_main_on_the_checked_in_minimal_cassette(capsys):
    # Reuse the synthetic minimal cassette to prove the file path and both surfaces.
    assert main([str(CASSETTES / "spy_minimal.json")]) == 0
    out = capsys.readouterr().out
    assert "underlying:" in out
    assert "quote:" in out
    assert "extra" not in out


# -- the bars surface ----------------------------------------------------------
#
# The price-history payload is the one thing the recorder exists to capture, and it was
# the one payload this tool could not show. It fell through to <unknown surface: bars>,
# and --surface refused the name before any code ran.


BARS_BODY = {
    "candles": [
        {
            "open": 650.0,
            "high": 650.4,
            "low": 649.8,
            "close": 650.2,
            "volume": 1_200_000,
            "datetime": 1789054200000,
        },
        {"open": 650.2, "high": 650.3, "low": 650.0, "close": 650.1, "volume": 900_000},
    ],
    "symbol": "SPY",
    "empty": False,
}
EMPTY_BARS_BODY = {"candles": [], "symbol": "SPY", "empty": True}


def _bars_cassette(body: dict = BARS_BODY) -> Cassette:
    params = {
        "symbol": "SPY",
        "freq": "1m",
        "start": "2026-09-14T13:30:00+00:00",
        "end": "2026-09-14T20:00:00+00:00",
    }
    return Cassette(interactions=(Interaction("bars", params, 200, body, {}),))


def test_dumps_a_price_history_body_instead_of_an_unknown_surface():
    output = inspect_cassette(_bars_cassette())
    assert "unknown surface" not in output
    # The top-level keys, so an empty window is visible without reading the candles.
    assert "keys: candles, symbol, empty" in output
    assert "open: float" in output
    assert "volume: int" in output


def test_dumps_exactly_one_candle():
    output = inspect_cassette(_bars_cassette())
    # Two candles in, one dumped, and the line says which of how many.
    assert "candle [0 of 2]:" in output
    assert output.count("open: float") == 1


def test_the_candle_stamp_shows_its_value_so_an_epoch_is_visible():
    # datetime is Schwab's epoch-millisecond stamp, not an ISO string. The time-key rule
    # is what tells the two apart, and this is the field it exists for.
    assert "datetime: int = 1789054200000" in inspect_cassette(_bars_cassette())


def test_an_empty_window_is_reported_as_empty_rather_than_as_nothing():
    # Schwab answers a window it has no candles for with an empty list, never an error,
    # so the dump has to say so rather than print a bare params line.
    output = inspect_cassette(_bars_cassette(EMPTY_BARS_BODY))
    assert "candles: <empty>" in output
    assert "empty: bool" in output


def test_a_body_with_no_candle_list_is_reported_rather_than_crashing():
    output = inspect_cassette(_bars_cassette({"symbol": "SPY"}))
    assert "candles: <not a list: NoneType>" in output


def test_the_surface_filter_accepts_the_third_name():
    mixed = Cassette(interactions=_cassette().interactions + _bars_cassette().interactions)
    bars_only = inspect_cassette(mixed, surface="bars")
    assert "candle [0 of 2]:" in bars_only
    assert "callExpDateMap" not in bars_only
    assert "bidPrice" not in bars_only


def test_the_command_line_accepts_the_third_name(tmp_path, capsys):
    path = tmp_path / "bars.json"
    dump_cassette(_bars_cassette(), path)
    assert main([str(path), "--surface", "bars"]) == 0
    assert "candle [0 of 2]:" in capsys.readouterr().out


def test_the_command_line_still_refuses_a_name_the_dump_has_no_shape_for(tmp_path):
    path = tmp_path / "bars.json"
    dump_cassette(_bars_cassette(), path)
    # argparse exits 2 on a bad choice, before the file is read.
    with pytest.raises(SystemExit) as caught:
        main([str(path), "--surface", "actions"])
    assert caught.value.code == 2


def test_the_offered_surfaces_are_the_ones_the_dump_can_shape():
    # A name in the choices with no dump behind it would print <unknown surface: ...>,
    # which is exactly the failure this issue closed for bars.
    assert set(DUMPABLE_SURFACES) == {"chains", "quotes", "bars"}
    for surface in DUMPABLE_SURFACES:
        assert f"``{surface}``" in inspect_module.__doc__


def test_the_dumpable_surfaces_are_narrower_than_the_lakes_own_list():
    """``actions`` is a lake surface and never a cassette interaction.

    It is derived from sealed quotes rather than fetched, so no vendor call keys one. The
    narrower name says which surfaces this tool can dump rather than how many exist,
    following ``journal.PINNED_SURFACES`` and ``dashboard.PANEL_SURFACES``.
    """
    from lake.paths import ACTIONS, SURFACES

    assert set(DUMPABLE_SURFACES) < set(SURFACES)
    assert set(SURFACES) - set(DUMPABLE_SURFACES) == {ACTIONS}


def test_a_candle_that_is_not_a_mapping_is_reported_rather_than_crashing():
    output = inspect_cassette(_bars_cassette({"candles": ["not-a-candle"], "symbol": "SPY"}))
    assert "<not a candle: str>" in output


def test_a_string_candles_value_is_reported_rather_than_walked_character_by_character():
    # A str is a Sequence, so without the explicit exclusion its first character would be
    # dumped as if it were a candle.
    output = inspect_cassette(_bars_cassette({"candles": "nope", "symbol": "SPY"}))
    assert "<not a list: str>" in output


def test_the_candle_list_is_not_repeated_in_the_top_level_dump():
    # The top level is everything that is not a candle, so a `candles: list` line there
    # would say nothing and would contradict what the dump promises.
    output = inspect_cassette(_bars_cassette())
    assert "candles: list" not in output
    assert "keys: candles, symbol, empty" in output


def test_the_candle_fields_are_nested_under_the_candle_header():
    # A flat dump would read as if the candle's fields were top-level body fields.
    lines = inspect_cassette(_bars_cassette()).splitlines()
    header = next(i for i, line in enumerate(lines) if line.strip().startswith("candle ["))
    field = next(i for i, line in enumerate(lines) if line.strip().startswith("open:"))
    indent = lambda i: len(lines[i]) - len(lines[i].lstrip())  # noqa: E731
    assert indent(field) > indent(header)
