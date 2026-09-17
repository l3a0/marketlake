"""The retire command across real files.

These run the retire core against a throwaway lake and roster, with a manual clock and
no vendor. Retiring closes a ticker's open capture span and either disables the roster
entry or removes it. The span end is what the guard and the walk read later.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.capture_spans import CaptureSpans, spans_path
from lake.cassette import Cassette, Interaction
from lake.onboard import onboard
from lake.retire import RetireError, retire
from lake.security_master import SecurityMaster, master_path
from lake.tickers import load_tickers, upsert_ticker
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor


def _quote_vendor(ticker: str) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="quotes",
                    params={"symbols": [ticker]},
                    status=200,
                    body={ticker: {"realtime": True, "quote": {"bidPrice": 1.0}}},
                ),
            )
        )
    )


ONBOARD = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)  # 11:00 ET
RETIRE = datetime(2026, 8, 27, 20, 5, tzinfo=UTC)  # 16:05 ET


def _setup(lake: Path, tickers: Path, *, ticker: str = "SPY", options: bool = False) -> int:
    """Onboard-equivalent state, built directly: a master, an open span, a roster entry."""
    master = SecurityMaster()
    iid = master.register(
        kind="equity", capture_start=ONBOARD, valid_from=ONBOARD.date(), ticker=ticker
    )
    master.write(master_path(lake))
    spans = CaptureSpans()
    spans.open_span(iid, ONBOARD, options)
    spans.write(spans_path(lake))
    upsert_ticker(ticker, options=options, path=tickers)
    return iid


def test_retiring_before_the_seed_run_has_happened_refuses(tmp_path: Path):
    """The retire-side mirror of the same onboard guard: never silently lose history."""
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    master.register(kind="equity", capture_start=ONBOARD, valid_from=ONBOARD.date(), ticker="SPY")
    master.write(master_path(lake))
    # No spans file written: this lake has an instrument but was never seeded.
    upsert_ticker("SPY", options=False, path=tickers)

    with pytest.raises(RetireError, match="seed_spans"):
        retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)


def test_retire_disables_in_place_and_closes_the_span(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    report = retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    assert report.already_retired is False
    assert report.removed is False
    # The entry stays in the roster but is now disabled.
    assert load_tickers(tickers).get("SPY").enabled is False
    # The span is closed at the retire instant.
    spans = CaptureSpans.read(spans_path(lake))
    assert spans.has_open_span(iid) is False
    assert spans.spans_of(iid)[0].end == RETIRE


def test_retire_remove_deletes_the_entry_and_closes_the_span(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    report = retire(
        "SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers, remove=True
    )

    assert report.removed is True
    assert load_tickers(tickers).symbols == ()  # removing the last ticker empties the roster
    assert CaptureSpans.read(spans_path(lake)).has_open_span(iid) is False


def test_retiring_an_unknown_ticker_raises(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    _setup(lake, tickers)
    with pytest.raises(RetireError):
        retire("NOPE", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)


def test_re_running_when_already_retired_is_a_no_op(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)
    retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers, remove=True)
    end_after_first = CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].end

    later = datetime(2026, 8, 28, 20, 5, tzinfo=UTC)
    report = retire(
        "SPY", clock=ManualClock(later), lake_root=lake, tickers_path=tickers, remove=True
    )

    assert report.already_retired is True
    assert report.span_end is None
    # The already-closed span is untouched; the second run did not move its end.
    assert CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].end == end_after_first


def test_the_span_is_closed_before_the_roster_change(tmp_path: Path, monkeypatch):
    # Order matters: close the span, then touch the roster. A crash in between must leave
    # the ticker still in the roster with a closed span, never off with an open span.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    iid = _setup(lake, tickers)

    import lake.retire as retire_mod

    def boom(*args, **kwargs):
        raise RuntimeError("crash after the span write, before the roster change")

    monkeypatch.setattr(retire_mod, "set_enabled", boom)
    with pytest.raises(RuntimeError):
        retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    # The span was closed (safe, recoverable). The roster still has the ticker enabled.
    assert CaptureSpans.read(spans_path(lake)).has_open_span(iid) is False
    assert load_tickers(tickers).get("SPY").enabled is True


def test_onboard_retire_reonboard_opens_a_second_span(tmp_path: Path):
    # The #77 rejoin case, end to end: a returning ticker keeps its instrument id, opens a
    # second span, and its time away is out of scope rather than owed.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir(parents=True)  # the lake root exists before the first write, as in production
    first = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)  # 11:00 ET
    onboard(
        "SPY",
        clock=ManualClock(first),
        vendor=_quote_vendor("SPY"),
        lake_root=lake,
        tickers_path=tickers,
        options=False,
    )
    retire_at = datetime(2026, 8, 27, 20, 5, tzinfo=UTC)
    retire("SPY", clock=ManualClock(retire_at), lake_root=lake, tickers_path=tickers, remove=True)
    assert load_tickers(tickers).symbols == ()

    rejoin = datetime(2026, 9, 3, 15, 0, tzinfo=UTC)
    report = onboard(
        "SPY",
        clock=ManualClock(rejoin),
        vendor=_quote_vendor("SPY"),
        lake_root=lake,
        tickers_path=tickers,
        options=False,
    )
    assert report.already_registered is True
    assert report.instrument_id == 1  # same instrument across the rejoin
    assert report.capture_start == rejoin  # the fresh span start, not the original

    spans = CaptureSpans.read(spans_path(lake))
    windows = spans.spans_of(1)
    assert len(windows) == 2
    assert windows[0].end == retire_at
    assert windows[1].start == rejoin and windows[1].end is None
    # In scope during both live periods, out of scope for the week away.
    away = datetime(2026, 8, 30, 15, 0, tzinfo=UTC)
    assert spans.in_scope(1, first) is True
    assert spans.in_scope(1, away) is False
    assert spans.in_scope(1, rejoin) is True


# -- the spans read and the lock -----------------------------------------------------------


QQQ_ONBOARD = datetime(2026, 8, 27, 15, 30, tzinfo=UTC)  # 11:30 ET
CLOSED_AT = datetime(2026, 8, 27, 19, 0, tzinfo=UTC)  # 15:00 ET


def _racing(lake: Path, write, *, on: str = "acquire"):
    """A ``lake_lock`` that runs ``write`` as the hold is taken, or as it is released.

    The pattern is ``test_occ_mapping``'s ``racing_lock``. ``on="acquire"`` puts the other
    writer inside the hold, which is where a blocked one cannot be. ``on="release"`` puts it
    the instant the hold ends, which is where a writer blocked on the lock actually lands.
    The pair separates a read under *a* lock from a read under *the* lock the write uses.
    """
    from lake.lock import lake_lock as real_lock

    done: list[bool] = []

    @contextmanager
    def racing_lock(lake_root):
        with real_lock(lake_root) as held:
            if on == "acquire" and not done:
                done.append(True)
                write()
            yield held
        if on == "release" and not done:
            done.append(True)
            write()

    return racing_lock


def test_a_span_closed_during_the_run_is_not_reopened(tmp_path: Path, monkeypatch):
    """The spans file is rewritten whole, so a stale snapshot discards another close.

    This is the state ``retire``'s own module docstring rules out: the roster entry off and the
    span still open. Ordering keeps a crash away from it and only the lock keeps a second
    writer away.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    spy = _setup(lake, tickers)
    master = SecurityMaster.read(master_path(lake))
    qqq = master.register(
        kind="equity", capture_start=QQQ_ONBOARD, valid_from=QQQ_ONBOARD.date(), ticker="QQQ"
    )
    master.write(master_path(lake))
    spans = CaptureSpans.read(spans_path(lake))
    spans.open_span(qqq, QQQ_ONBOARD, False)
    spans.write(spans_path(lake))
    upsert_ticker("QQQ", options=False, path=tickers)

    def close_qqq():
        late = CaptureSpans.read(spans_path(lake))
        late.close_span(qqq, CLOSED_AT)
        late.write(spans_path(lake))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(lake, close_qqq))

    retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    after = CaptureSpans.read(spans_path(lake))
    assert after.spans_of(qqq)[0].end == CLOSED_AT, (
        "a concurrent retire's close was discarded, leaving QQQ off the roster with an open span"
    )
    assert after.spans_of(spy)[0].end == RETIRE, "this run's own close still landed"


def test_a_span_opened_during_the_run_survives_and_stays_in_capture_scope(
    tmp_path: Path, monkeypatch
):
    """The costliest case, because this command never writes the master.

    An instrument onboarded in the window keeps its master row and loses its span, so
    ``capture._live_roster`` resolves it and finds nothing in scope. The ticker is enabled,
    registered, and captured by nothing, and neither the watchdog nor gap marking can see it,
    because both read that state as a retirement.
    """
    from lake.capture import _live_roster

    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    _setup(lake, tickers)

    landed: list[int] = []

    def onboard_qqq():
        late = SecurityMaster.read(master_path(lake))
        iid = late.register(
            kind="equity", capture_start=QQQ_ONBOARD, valid_from=QQQ_ONBOARD.date(), ticker="QQQ"
        )
        late.write(master_path(lake))
        late_spans = CaptureSpans.read(spans_path(lake))
        late_spans.open_span(iid, QQQ_ONBOARD, False)
        late_spans.write(spans_path(lake))
        upsert_ticker("QQQ", options=False, path=tickers)
        landed.append(iid)

    monkeypatch.setattr("lake.lock.lake_lock", _racing(lake, onboard_qqq))

    retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    qqq = landed[0]
    assert CaptureSpans.read(spans_path(lake)).has_open_span(qqq), (
        "the ticker onboarded during the retire lost its span"
    )
    in_scope = [entry.ticker for entry in _live_roster(load_tickers(tickers), lake, RETIRE).enabled]
    assert "QQQ" in in_scope, (
        "QQQ is enabled and registered and capture would record nothing for it, which is minutes "
        "gone with nothing to announce it"
    )


def test_a_retire_that_only_locks_its_write_still_discards_the_close(tmp_path: Path, monkeypatch):
    """Reading under *a* lock is not reading under *the* lock the write happens in.

    A writer blocked on a read-only hold lands the instant it is released, which is before a
    separate write hold is taken.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    _setup(lake, tickers)

    landed: list[int] = []

    def onboard_qqq():
        late = SecurityMaster.read(master_path(lake))
        iid = late.register(
            kind="equity", capture_start=QQQ_ONBOARD, valid_from=QQQ_ONBOARD.date(), ticker="QQQ"
        )
        late.write(master_path(lake))
        late_spans = CaptureSpans.read(spans_path(lake))
        late_spans.open_span(iid, QQQ_ONBOARD, False)
        late_spans.write(spans_path(lake))
        landed.append(iid)

    monkeypatch.setattr("lake.lock.lake_lock", _racing(lake, onboard_qqq, on="release"))

    retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    assert CaptureSpans.read(spans_path(lake)).has_open_span(landed[0]), (
        "a writer that landed as the hold released had its span discarded"
    )


def test_an_already_retired_ticker_is_decided_inside_the_lock(tmp_path: Path, monkeypatch):
    """Whether anything is written is itself decided from the read, so it is decided in the hold.

    A close landing in the window makes this run's own work already done. Deciding that outside
    the lock writes a second close, moving the recorded end later than the instant the ticker
    actually stopped.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    spy = _setup(lake, tickers)

    def close_spy():
        late = CaptureSpans.read(spans_path(lake))
        late.close_span(spy, CLOSED_AT)
        late.write(spans_path(lake))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(lake, close_spy))

    report = retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    assert report.already_retired is True
    assert report.span_end is None
    assert CaptureSpans.read(spans_path(lake)).spans_of(spy)[0].end == CLOSED_AT, (
        "the earlier close was overwritten with this run's later instant"
    )


def test_the_report_names_the_instant_the_span_was_closed_at(tmp_path: Path):
    """``span_end`` on a retire that actually closed something, which nothing asserted.

    Every other assertion on this field is the already-retired case, where it is ``None``,
    so a report hard-coding ``None`` would satisfy all of them while telling the operator
    no span was closed on the run that closed one.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    iid = _setup(lake, tickers)

    report = retire("SPY", clock=ManualClock(RETIRE), lake_root=lake, tickers_path=tickers)

    assert report.already_retired is False
    assert report.span_end == RETIRE, "the report did not name the instant it closed the span at"
    assert CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].end == RETIRE
