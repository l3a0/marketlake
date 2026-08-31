"""The offline cassette inspector: it reports faults and dumps field structure.

Every test here builds a synthetic cassette or reuses the checked-in minimal one. The
inspector runs fully offline, so no network and no ``schwab-py`` are involved. The
fixtures cover the three cases the tool must handle: a quotes interaction, a chains
interaction, and a gateway fault.
"""

from __future__ import annotations

from pathlib import Path

from lake.cassette import Cassette, Interaction, dump_cassette
from lake.inspect_cassette import inspect_cassette, main

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
