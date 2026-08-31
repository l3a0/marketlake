"""The live chain-size probe: it measures a chain through an injected fake client.

Every test here injects a fake client, so no network and no real token are involved.
The fake returns canned discovery, single-expiration, and window responses. The probe's
measurement logic runs offline against them. One test also proves the module imports
with ``schwab`` blocked, the lazy-import discipline the live path relies on.
"""

from __future__ import annotations

import datetime
import importlib
import json
import sys
from dataclasses import dataclass

from lake.probe import build_parser, measure_chain_size, render_report

# Forty weekly expirations, sorted ascending, so every window offset (4, 8, 16, 32) is
# reachable. Tests are not scanned by the enforcement seams, so naming dates is fine.
EXP_DATES = [
    (datetime.date(2026, 9, 18) + datetime.timedelta(days=7 * i)).isoformat() for i in range(40)
]


def _exp_map(dates: list[str]) -> dict:
    """A ``callExpDateMap`` keyed ``"YYYY-MM-DD:DTE"`` with one strike each."""
    return {f"{iso}:25": {"650.0": [{"putCall": "CALL"}]} for iso in dates}


DISCOVERY_BODY = {
    "symbol": "SPY",
    "status": "SUCCESS",
    "numberOfContracts": 80,
    "callExpDateMap": _exp_map(EXP_DATES),
    "putExpDateMap": {},
}
NEAREST_BODY = {"symbol": "SPY", "status": "SUCCESS", "numberOfContracts": 40}
SMALL_WINDOW_BODY = {"symbol": "SPY", "numberOfContracts": 300, "isChainTruncated": False}
FAULT_BODY = {
    "fault": {
        "faultstring": "Body buffer overflow",
        "detail": {"errorcode": "protocol.http.TooBigBody"},
    }
}


@dataclass(frozen=True)
class FakeChainResponse:
    """A stand-in for the ``httpx.Response`` the real client returns.

    ``text`` defaults to ``None`` so the byte measurement falls back to
    ``json.dumps(body)``. Set it to exercise the ``len(text)`` path with a known length.
    """

    status_code: int
    body: dict
    text: str | None = None

    def json(self) -> dict:
        return self.body


class FakeChainClient:
    """A ``schwab-py`` client stand-in that routes by the request shape.

    A ``strike_count`` call returns the discovery response. A ``from_date == to_date``
    call returns the nearest-expiration response. Any other call is a window bracket,
    keyed on its ``to_date``. Every call is recorded for assertions.
    """

    def __init__(self, *, discovery, nearest, windows) -> None:
        self._discovery = discovery
        self._nearest = nearest
        self._windows = windows
        self.calls: list[tuple] = []

    def get_option_chain(self, symbol, *, strike_count=None, from_date=None, to_date=None):
        self.calls.append((symbol, strike_count, from_date, to_date))
        if strike_count is not None:
            return self._discovery
        if from_date == to_date:
            return self._nearest
        return self._windows[to_date]


def _client(*, truncated_at: int | None = 32) -> FakeChainClient:
    """A fake whose window at ``truncated_at`` returns a 502 TooBigBody fault."""
    windows = {}
    for offset in (4, 8, 16, 32):
        to_date = datetime.date.fromisoformat(EXP_DATES[offset])
        if offset == truncated_at:
            windows[to_date] = FakeChainResponse(502, FAULT_BODY)
        else:
            windows[to_date] = FakeChainResponse(200, SMALL_WINDOW_BODY)
    return FakeChainClient(
        discovery=FakeChainResponse(200, DISCOVERY_BODY, text="x" * 1234),
        nearest=FakeChainResponse(200, NEAREST_BODY, text="y" * 567),
        windows=windows,
    )


def test_discovery_parses_the_expiration_count_and_endpoints():
    report = measure_chain_size(_client(), "SPY")
    assert report.discovery.status == 200
    assert report.discovery.n_expirations == 40
    assert report.discovery.first == EXP_DATES[0]
    assert report.discovery.last == EXP_DATES[-1]


def test_byte_sizes_use_text_length_then_fall_back_to_json():
    client = _client()
    report = measure_chain_size(client, "SPY")
    # Discovery and nearest carry text, so their byte size is the text length.
    assert report.discovery.n_bytes == 1234
    assert report.nearest is not None
    assert report.nearest.n_bytes == 567
    # The window bodies carry no text, so the size falls back to the JSON length.
    small = next(b for b in report.brackets if b.offset == 4)
    assert small.measurement.n_bytes == len(json.dumps(SMALL_WINDOW_BODY))


def test_nearest_expiration_reports_the_contract_count():
    report = measure_chain_size(_client(), "SPY")
    assert report.nearest_expiration == EXP_DATES[0]
    assert report.nearest is not None
    assert report.nearest.number_of_contracts == 40


def test_window_brackets_surface_a_502_truncation():
    report = measure_chain_size(_client(truncated_at=32), "SPY")
    assert [b.offset for b in report.brackets] == [4, 8, 16, 32]
    truncated = next(b for b in report.brackets if b.offset == 32)
    assert truncated.measurement.status == 502
    # A fault body carries neither count nor the truncation flag.
    assert truncated.measurement.number_of_contracts is None
    assert truncated.measurement.is_truncated is None
    # The healthy brackets keep their vendor counts.
    healthy = next(b for b in report.brackets if b.offset == 4)
    assert healthy.measurement.number_of_contracts == 300
    assert healthy.measurement.is_truncated is False


def test_the_probe_makes_exactly_the_expected_calls_in_order():
    client = _client()
    measure_chain_size(client, "SPY")
    first_date = datetime.date.fromisoformat(EXP_DATES[0])
    assert client.calls[0] == ("SPY", 1, None, None)
    assert client.calls[1] == ("SPY", None, first_date, first_date)
    window_targets = [call[3] for call in client.calls[2:]]
    assert window_targets == [datetime.date.fromisoformat(EXP_DATES[o]) for o in (4, 8, 16, 32)]


def test_short_chain_skips_unreachable_brackets():
    # Only five expirations, so only the +4 bracket is reachable.
    dates = EXP_DATES[:5]
    body = {"callExpDateMap": _exp_map(dates), "numberOfContracts": 10}
    to_date = datetime.date.fromisoformat(dates[4])
    client = FakeChainClient(
        discovery=FakeChainResponse(200, body),
        nearest=FakeChainResponse(200, NEAREST_BODY),
        windows={to_date: FakeChainResponse(200, SMALL_WINDOW_BODY)},
    )
    report = measure_chain_size(client, "SPY")
    assert [b.offset for b in report.brackets] == [4]


def test_render_report_names_the_truncation_and_the_expiration_count():
    text = render_report(measure_chain_size(_client(truncated_at=32), "SPY"))
    assert "expirations=40" in text
    assert "status=502" in text
    assert "window +32" in text


def test_parser_defaults_to_spy_and_collects_symbols():
    assert build_parser().parse_args([]).symbol == ["SPY"]
    args = build_parser().parse_args(["SPY", "QQQ", "--token", "/tmp/t.json"])
    assert args.symbol == ["SPY", "QQQ"]
    assert args.token == "/tmp/t.json"


def test_import_does_not_require_schwab(monkeypatch):
    # The live client is imported lazily, so importing the module must succeed even when
    # ``schwab`` cannot be imported. Blocking it in sys.modules makes any top-level
    # ``import schwab`` raise, proving the import stays out of module load.
    monkeypatch.setitem(sys.modules, "schwab", None)
    monkeypatch.setitem(sys.modules, "schwab.auth", None)
    sys.modules.pop("lake.probe", None)
    module = importlib.import_module("lake.probe")
    assert module.measure_chain_size is not None
