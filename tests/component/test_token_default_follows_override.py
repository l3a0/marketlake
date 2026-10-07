"""Every entry that falls back to the default token path follows ``MARKETLAKE_CONFIG_DIR``.

Ten entries in ``src/lake`` build a vendor or a client from a token path and fall back to
``lake.paths.default_token_path()`` when the caller names none. The token is the file a
refresh rewrites, so an entry that resolved the default from ``HOME`` alone would send a
by-hand run with the override exported to the live token. The scanner in
``tests/support/config_defaults.py`` reads only what binds at import. It cannot see a
function body that passes ``Path.home()`` to the resolver, so each entry is driven here
instead.

Each case moves ``HOME`` to one throwaway directory and ``MARKETLAKE_CONFIG_DIR`` to
another, calls the entry with no token path, and stops it at the first thing that
receives the path: the injected vendor factory, ``SchwabVendor.from_token``,
``client_from_token``, or the parsed ``--token``. The spy raises, so no vendor is built,
no lake is written, and nothing reaches the network. The path it saw must be the
override's token exactly. An entry that resolved the default from ``HOME`` would name the
home directory's token instead, and fail.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from lake import bars, capture, onboard, probe, probe_calendar, record, schwab, sweep
from lake.paths import TOKEN_FILE
from tests.support.calendar import et
from tests.support.clock import ManualClock
from tests.support.config import write_config


class Reached(Exception):
    """Raised by a spy once it has seen the token path, to stop the entry there."""


@dataclass(frozen=True)
class Rig:
    """A config, a roster and a lake the entry reads, named by explicit paths."""

    config: Path
    tickers: Path
    seen: list[Path]

    def spy(self, token_path, *args: object, **kwargs: object):
        """Stand in for whatever receives the token path, and stop the entry."""
        self.seen.append(Path(token_path))
        raise Reached


def _patch_from_token(monkeypatch, rig: Rig) -> None:
    """Replace the production vendor factory on the class every entry looks up."""
    monkeypatch.setattr(schwab.SchwabVendor, "from_token", staticmethod(rig.spy))


def _fetch_bars(rig: Rig, monkeypatch) -> None:
    bars.fetch_session_bars_from_config(
        config_path=rig.config, tickers_path=rig.tickers, vendor_factory=rig.spy
    )


def _backfill_bars(rig: Rig, monkeypatch) -> None:
    bars.backfill_bars_from_config(
        config_path=rig.config, tickers_path=rig.tickers, vendor_factory=rig.spy
    )


def _capture_cycle(rig: Rig, monkeypatch) -> None:
    _patch_from_token(monkeypatch, rig)
    capture.run_cycle_from_config(
        clock=ManualClock(et(2026, 8, 31, 10, 0)),
        config_path=rig.config,
        tickers_path=rig.tickers,
        pid=4242,
    )


def _onboard(rig: Rig, monkeypatch) -> None:
    _patch_from_token(monkeypatch, rig)
    onboard.onboard_from_config(
        "XYZ", clock=ManualClock(et(2026, 8, 31, 10, 0)), config_path=rig.config
    )


def _probe_client(rig: Rig, monkeypatch) -> None:
    monkeypatch.setattr(probe, "client_from_token", rig.spy)
    probe._client_from_token(api_key="api-key", app_secret="app-secret")


def _probe_parser(rig: Rig, monkeypatch) -> None:
    rig.spy(probe.build_parser().parse_args([]).token)


def _record_cassette(rig: Rig, monkeypatch) -> None:
    record.record_cassette("api-key", "app-secret", vendor_factory=rig.spy)


def _record_parser(rig: Rig, monkeypatch) -> None:
    rig.spy(record.build_parser().parse_args(["--out", "cassette.json"]).token)


def _sweep(rig: Rig, monkeypatch) -> None:
    # The sweep builds its vendor only when the run asks for one, so the run is replaced
    # by one that asks at once.
    def ask_for_the_vendor(*, vendor_source, **kwargs: object) -> None:
        vendor_source()

    _patch_from_token(monkeypatch, rig)
    monkeypatch.setattr(sweep, "sweep", ask_for_the_vendor)
    sweep.sweep_from_config(config_path=rig.config, tickers_path=rig.tickers)


def _probe_calendar(rig: Rig, monkeypatch) -> None:
    _patch_from_token(monkeypatch, rig)
    probe_calendar.main(["--config", str(rig.config), "--tickers", str(rig.tickers)])


ENTRIES: dict[str, Callable[[Rig, pytest.MonkeyPatch], None]] = {
    "bars.fetch_session_bars_from_config": _fetch_bars,
    "bars.backfill_bars_from_config": _backfill_bars,
    "capture.run_cycle_from_config": _capture_cycle,
    "onboard.onboard_from_config": _onboard,
    "probe._client_from_token": _probe_client,
    "probe.build_parser": _probe_parser,
    "record.record_cassette": _record_cassette,
    "record.build_parser": _record_parser,
    "sweep.sweep_from_config": _sweep,
    "probe_calendar.main": _probe_calendar,
}


@pytest.mark.parametrize("entry", ENTRIES)
def test_the_default_token_path_follows_the_override(entry, tmp_path, monkeypatch):
    home = tmp_path / "home"
    override = tmp_path / "override"
    home.mkdir()
    override.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("MARKETLAKE_CONFIG_DIR", str(override))
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: true, chain_cadence: 1m}\n")
    rig = Rig(config=write_config(tmp_path, lake_root), tickers=tickers, seen=[])

    with pytest.raises(Reached):
        ENTRIES[entry](rig, monkeypatch)

    assert rig.seen == [override / TOKEN_FILE]
