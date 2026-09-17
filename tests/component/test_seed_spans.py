"""The seed command: seeding capture spans from an existing master.

These run the seed core against a throwaway lake, master, and roster, with a manual
clock. No vendor and no wall clock are crossed.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from lake.capture_spans import CaptureSpans, spans_path
from lake.security_master import KIND_EQUITY, SecurityMaster, master_path
from lake.seed_spans import seed_spans
from lake.tickers import upsert_ticker
from tests.support.clock import ManualClock

NOW = datetime(2026, 9, 2, 15, 0, tzinfo=UTC)  # 11:00 ET
SPY_START = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)  # 09:30 ET
QQQ_START = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
# A rejoin: the span opens long after the master's capture_start, which is a shape
# ``build_from_master`` cannot reproduce from the master alone.
REJOIN = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)


def test_seed_opens_one_open_span_per_instrument_from_capture_start(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    spy = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    qqq = master.register(
        kind=KIND_EQUITY, capture_start=QQQ_START, valid_from=QQQ_START.date(), ticker="QQQ"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=True, path=tickers)
    upsert_ticker("QQQ", options=False, path=tickers)

    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    assert report.already_seeded is False
    assert report.instrument_count == 2
    spans = CaptureSpans.read(spans_path(lake))
    spy_span = spans.spans_of(spy)[0]
    assert spy_span.start == SPY_START and spy_span.end is None and spy_span.options is True
    qqq_span = spans.spans_of(qqq)[0]
    assert qqq_span.start == QQQ_START and qqq_span.options is False


def test_seed_on_a_fresh_lake_writes_an_empty_spans_file(tmp_path: Path):
    # No master exists yet. The first onboarding will write its own span, so the
    # seed command's only job here is to make the spans file present rather than absent.
    lake = tmp_path / "lake"
    lake.mkdir(parents=True)
    report = seed_spans(
        clock=ManualClock(NOW), lake_root=lake, tickers_path=tmp_path / "tickers.yaml"
    )
    assert report.instrument_count == 0
    assert spans_path(lake).exists()


def test_seed_is_idempotent(tmp_path: Path):
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=False, path=tickers)

    first = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert first.already_seeded is False

    second = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert second.already_seeded is True
    assert second.instrument_count == 1


def test_seed_tolerates_a_missing_or_broken_roster(tmp_path: Path):
    # A roster that cannot be read is unknown, not empty. Unknown widens: every
    # instrument still gets an open span, with options defaulted to false, rather than
    # every one of them being skipped as though the roster had named nobody.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    iid = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    master.write(master_path(lake))
    # No tickers.yaml at all.
    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)
    assert report.instrument_count == 1
    assert CaptureSpans.read(spans_path(lake)).spans_of(iid)[0].options is False


def test_seed_skips_an_instrument_the_roster_no_longer_names(tmp_path: Path):
    # The migration-safety case: a ticker retired by hand before this code existed must
    # not come back as a permanently open span. It is skipped entirely, out of scope
    # everywhere, rather than resurrected as though it were still being captured.
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    master = SecurityMaster()
    spy = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    gone = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="GONE"
    )
    master.write(master_path(lake))
    upsert_ticker("SPY", options=False, path=tickers)  # GONE is not in the roster at all

    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    assert report.instrument_count == 1
    spans = CaptureSpans.read(spans_path(lake))
    assert spans.spans_of(spy) != ()
    assert spans.spans_of(gone) == ()


# -- the reads and the lock ---------------------------------------------------------------


def _racing_onboard(lake: Path, *, on: str = "acquire"):
    """A ``lake_lock`` that onboards SPY as the hold is taken, or as it is released.

    The pattern is ``test_occ_mapping``'s ``racing_lock``. ``on="acquire"`` puts the write
    inside the hold the seed takes, which is where a blocked writer cannot be. ``on="release"``
    puts it just after the hold ends, which is the shape a writer blocked on the lock actually
    lands in: it waits, and the kernel hands it the lock the instant the holder lets go.

    The pair is what separates a read under *a* lock from a read under *the* lock the write
    happens in. A seed that took one hold to check for the file and another to write it would
    pass the first and fail the second.
    """
    from lake.lock import lake_lock as real_lock

    landed: list[int] = []

    @contextmanager
    def racing_lock(lake_root):
        with real_lock(lake_root) as held:
            if on == "acquire" and not landed:
                landed.append(_onboard_by_hand(lake))
            yield held
        if on == "release" and not landed:
            landed.append(_onboard_by_hand(lake))

    return racing_lock, landed


def _open_span_start(lake: Path, instrument_id: int):
    """The instrument's open span start, or ``None`` when it carries no span at all.

    A discarded file leaves the instrument with no span rather than with a different one,
    so asking for element zero would raise instead of reporting. The failure this holds is
    worth a sentence rather than an ``IndexError``.
    """
    spans = CaptureSpans.read(spans_path(lake)).spans_of(instrument_id)
    return next((span.start for span in spans if span.end is None), None)


def _onboard_by_hand(lake: Path) -> int:
    """What ``lake.onboard`` writes for a rejoining ticker: a master row and a fresh span.

    The span opens at ``REJOIN`` while the master's ``capture_start`` stays at ``SPY_START``,
    which is what a ticker brought back after retirement looks like. That gap is deliberate.
    ``build_from_master`` opens a span at ``capture_start``, so a seed that discarded this file
    and rebuilt one from the master would still leave SPY holding *a* span, and an assertion
    that only asked whether SPY has one would pass without holding anything. Asking for the
    start is what separates the file that survived from the one the seed rebuilt.
    """
    master = (
        SecurityMaster.read(master_path(lake)) if master_path(lake).exists() else SecurityMaster()
    )
    iid = master.register(
        kind=KIND_EQUITY, capture_start=SPY_START, valid_from=SPY_START.date(), ticker="SPY"
    )
    master.write(master_path(lake))
    spans = CaptureSpans.read(spans_path(lake)) if spans_path(lake).exists() else CaptureSpans()
    spans.open_span(iid, REJOIN, True)
    spans.write(spans_path(lake))
    return iid


def test_the_spans_file_is_checked_for_inside_the_lock_it_is_written_under(
    tmp_path: Path, monkeypatch
):
    """An onboarding landing in the window creates the file this would otherwise overwrite.

    The existence check is what makes the seed idempotent, and taking it outside the lock is
    what lets a second writer's whole file be discarded by a run that reported seeding nothing.
    The instrument is then registered, enabled, and carried by no span, which no re-run repairs
    because the second run finds the file present and stops.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    upsert_ticker("SPY", options=True, path=tickers)
    racing_lock, landed = _racing_onboard(lake)
    monkeypatch.setattr("lake.lock.lake_lock", racing_lock)

    report = seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    spy = landed[0]
    assert report.already_seeded is True, "the file the onboarding created must stop the seed"
    assert _open_span_start(lake, spy) == REJOIN, (
        "the onboarding's span was overwritten by a seed that checked for the file outside the "
        "lock, and rebuilt a different one from the master"
    )


def test_a_seed_that_only_locks_its_write_still_discards_the_onboarding(
    tmp_path: Path, monkeypatch
):
    """Reading under *a* lock is not reading under *the* lock the write happens in.

    Splitting the two, one hold to check and another to write, is the plausible near-miss: it
    keeps the check locked and removes nothing. A writer blocked on the first hold lands the
    instant it is released, which is before the second is taken.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    upsert_ticker("SPY", options=True, path=tickers)
    racing_lock, landed = _racing_onboard(lake, on="release")
    monkeypatch.setattr("lake.lock.lake_lock", racing_lock)

    seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    spy = landed[0]
    assert _open_span_start(lake, spy) == REJOIN, (
        "a writer that landed as the hold released had its span discarded"
    )


def test_the_master_is_read_inside_the_lock_the_spans_are_written_under(
    tmp_path: Path, monkeypatch
):
    """An instrument registered in the window has to reach the file this run writes.

    The master read decides the rows rather than whether to write at all, so this is the
    second of the seed's two reads and it fails separately from the first.
    """
    lake, tickers = tmp_path / "lake", tmp_path / "tickers.yaml"
    lake.mkdir()
    qqq_master = SecurityMaster()
    qqq = qqq_master.register(
        kind=KIND_EQUITY, capture_start=QQQ_START, valid_from=QQQ_START.date(), ticker="QQQ"
    )
    qqq_master.write(master_path(lake))
    upsert_ticker("QQQ", options=False, path=tickers)
    upsert_ticker("SPY", options=True, path=tickers)

    from lake.lock import lake_lock as real_lock

    landed: list[int] = []

    @contextmanager
    def racing_lock(lake_root):
        with real_lock(lake_root) as held:
            if not landed:
                # A registration alone, with no spans file written, which is the shape
                # ``occ_mapping`` lands in. The seed is what owes this instrument its span.
                late = SecurityMaster.read(master_path(lake))
                landed.append(
                    late.register(
                        kind=KIND_EQUITY,
                        capture_start=SPY_START,
                        valid_from=SPY_START.date(),
                        ticker="SPY",
                    )
                )
                late.write(master_path(lake))
            yield held

    monkeypatch.setattr("lake.lock.lake_lock", racing_lock)

    seed_spans(clock=ManualClock(NOW), lake_root=lake, tickers_path=tickers)

    spans = CaptureSpans.read(spans_path(lake))
    assert spans.has_open_span(landed[0]), (
        "the instrument registered during the run got no span, and no re-run gives it one"
    )
    assert spans.has_open_span(qqq), "the instrument the run started from kept its span"
