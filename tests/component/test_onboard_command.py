"""The onboarding command across real files.

These run the onboarding core against a throwaway lake and a throwaway tickers.yaml,
with a cassette-backed vendor and a manual clock. No network and no token are crossed.
The three tests that drive the command line cross the real clock, because the option's
own check asks what time it is now, and each of them uses an instant far enough from now
that the answer cannot change the outcome. The tier is component: onboarding writes and
reads real files, with the clock and vendor still fake.

They cover the slice-1 contract: register with a stamped capture_start and a ticker
mapping only (the FIGI is deferred to a CUSIP-keyed backfill), verify the real-time
entitlement before trusting the ticker, write the roster entry, journal the snapshot,
and persist the master with a manifest entry so the scrub stays clean. A delayed feed is
refused before anything is written.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import gap, journal
from lake.calendar import MARKET_TZ
from lake.capture import CAPTURE_SOURCE
from lake.capture_spans import SPANS_PARTITION, CaptureSpans, spans_path
from lake.cassette import Cassette, Interaction
from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan
from lake.config import GuardConstants
from lake.manifest import append_manifest, latest_entries
from lake.onboard import MASTER_PARTITION, EntitlementError, OnboardError, main, onboard
from lake.paths import LakePaths
from lake.security_master import ID_TYPE_FIGI, ID_TYPE_TICKER, SecurityMaster, master_path
from lake.session import SessionClock
from lake.tickers import load_tickers
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.vendor import (
    CassetteVendor,
    chain_params,
    windowed_chain_cassette,
    windowed_chain_interactions,
)

# A mid-session instant: 2026-08-27 15:00 UTC is 11:00 ET, the design's "onboarded
# 11:00" case.
_MID_SESSION = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)

# The date that instant lands on, which is the date the chain fetch plans its windows
# against. It is the clock's UTC date, never the epoch's Eastern one, because it is the
# date the journaled rows land under.
_MID_SESSION_DAY = _MID_SESSION.date()


def _chain_body(*, is_delayed: bool) -> dict:
    """A minimal SPY chain body with one call and one put.

    Each contract carries its own ``expirationDate``, the field the row builder reads to
    decide which plan window fetched it. A contract without one lands with both window
    columns null whatever the fetch passed, which would leave the fetch provenance
    untested.
    """
    return {
        "symbol": "SPY",
        "isDelayed": is_delayed,
        "callExpDateMap": {
            "2026-09-18:25": {
                "650.0": [
                    {
                        "putCall": "CALL",
                        "symbol": "SPY   260918C00650000",
                        "expirationDate": "2026-09-18T20:00:00.000+00:00",
                        "bid": 4.2,
                    }
                ]
            }
        },
        "putExpDateMap": {
            "2026-09-18:25": {
                "650.0": [
                    {
                        "putCall": "PUT",
                        "symbol": "SPY   260918P00650000",
                        "expirationDate": "2026-09-18T20:00:00.000+00:00",
                        "bid": 3.8,
                    }
                ]
            }
        },
    }


def _chain_vendor(*, is_delayed: bool, day: date = _MID_SESSION_DAY) -> CassetteVendor:
    """A chain vendor recorded window by window, the way onboarding now fetches.

    ``day`` is the date the run's clock lands on, which is what the fetch plans its
    windows against. A vendor built for the wrong day matches no window at all, and a
    missed window is quiet: ``_fetch_window`` files the ``CassetteError`` as a failed
    window rather than as a short recording.
    """
    return CassetteVendor(windowed_chain_cassette("SPY", day, _chain_body(is_delayed=is_delayed)))


def _quote_vendor(ticker: str, *, realtime: bool) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="quotes",
                    params={"symbols": [ticker]},
                    status=200,
                    body={
                        ticker: {
                            "realtime": realtime,
                            "reference": {"cusip": "444444444"},
                            "quote": {"bidPrice": 1.0},
                        }
                    },
                ),
            )
        )
    )


def test_onboarding_an_existing_ticker_before_the_seed_run_has_happened_refuses(
    lake_root, tmp_path
):
    """A master with instruments and no spans file must refuse, not silently lose history.

    Onboarding a ticker the master already knows would open a fresh, empty spans file
    and read the instrument as never having had a capture span at all, discarding its
    whole recorded history. The fix is a run of the seed command, named in the error.
    """
    from lake.security_master import KIND_EQUITY

    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=_MID_SESSION, valid_from=_MID_SESSION.date(), ticker="SPY"
    )
    master.write(master_path(lake_root))
    tickers_path = tmp_path / "tickers.yaml"

    with pytest.raises(OnboardError, match="seed_spans"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_chain_vendor(is_delayed=False),
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=True,
        )


def test_onboard_registers_verifies_and_writes(lake_root, tmp_path):
    clock = ManualClock(start=_MID_SESSION)
    tickers_path = tmp_path / "tickers.yaml"

    report = onboard(
        "SPY",
        clock=clock,
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    # The report pins the day-one anchor and the stamped epoch.
    assert report.instrument_id == 1
    assert report.capture_start == _MID_SESSION
    assert report.contract_count == 2
    assert report.realtime_verified is True
    assert report.already_registered is False

    # The master persisted and resolves SPY as of the onboarding day to the same id.
    master = SecurityMaster.read(master_path(lake_root))
    onboard_day = report.capture_start.date()
    instrument_id = master.resolve("SPY", onboard_day, id_type=ID_TYPE_TICKER)
    assert instrument_id == 1
    assert master.capture_start_of(1) == _MID_SESSION

    # Registration created the ticker mapping only. The FIGI is deferred, so the master
    # has no FIGI mapping for the instrument; it backfills later from the captured CUSIP.
    assert master.symbol_at(1, onboard_day, id_type=ID_TYPE_TICKER) == "SPY"
    assert master.symbol_at(1, onboard_day, id_type=ID_TYPE_FIGI) is None
    assert all(m.id_type == ID_TYPE_TICKER for m in master)

    # The master has a manifest entry, so the reverse scrub will not flag it as orphan.
    assert MASTER_PARTITION in latest_entries(lake_root)

    # The roster entry is written and loads back with the expected settings.
    roster = load_tickers(tickers_path)
    spy = roster.get("SPY")
    assert spy.options is True
    assert spy.chain_cadence == "1m"
    assert spy.bars == ("1m", "1d")

    # The verification snapshot was journaled as the first captured cycle, not discarded.
    assert report.snapshot_surface == journal.CHAINS_SURFACE
    segment_path = lake_root / report.snapshot_segment
    assert report.snapshot_segment.startswith("journal/")
    assert segment_path.exists()

    # It round-trips through the reader with one data row per contract, the same shape a
    # capture cycle writes.
    table = journal.read_segment(segment_path).to_pylist()
    assert [row["occ_symbol"] for row in table] == [
        "SPY   260918C00650000",
        "SPY   260918P00650000",
    ]
    assert all(row["row_kind"] == journal.ROW_KIND_DATA for row in table)
    assert all(row["fetch_ts"] is not None and row["fetch_end_ts"] is not None for row in table)

    # A matching segment-keyed manifest entry was appended, sourced like a capture write.
    entry = latest_entries(lake_root)[report.snapshot_segment]
    assert entry["source"] == CAPTURE_SOURCE
    assert entry["rows"] == len(table)


def test_onboard_is_idempotent(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"

    first = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    # A later re-run reuses the id and the original capture_start rather than minting new.
    later = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    second = onboard(
        "SPY",
        clock=ManualClock(start=later),
        vendor=_chain_vendor(is_delayed=False, day=later.date()),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    assert second.already_registered is True
    assert second.instrument_id == first.instrument_id
    assert second.capture_start == first.capture_start == _MID_SESSION

    # The roster still holds exactly one SPY entry.
    roster = load_tickers(tickers_path)
    assert roster.symbols == ("SPY",)

    # Each run journaled its own snapshot at its own moment. The two segments differ and
    # both survive on disk, so a re-onboard never discards or clobbers a sample.
    assert second.snapshot_segment != first.snapshot_segment
    assert (lake_root / first.snapshot_segment).exists()
    assert (lake_root / second.snapshot_segment).exists()


def test_delayed_feed_is_refused_and_writes_nothing(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"

    with pytest.raises(EntitlementError):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_chain_vendor(is_delayed=True),
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=True,
        )

    # Nothing was trusted: no roster entry, no persisted master, and no journal segment.
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_equity_only_onboard_uses_a_quote(lake_root, tmp_path):
    tickers_path = tmp_path / "tickers.yaml"

    report = onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("QQQ", realtime=True),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=False,
    )

    # No chain snapshot, so no contract anchor.
    assert report.contract_count is None
    assert report.realtime_verified is True

    roster = load_tickers(tickers_path)
    qqq = roster.get("QQQ")
    assert qqq.options is False
    assert qqq.chain_cadence is None

    # The quotes snapshot it fetched for entitlement was journaled as the first cycle,
    # carrying the raw CUSIP for the deferred FIGI backfill.
    assert report.snapshot_surface == journal.QUOTES_SURFACE
    segment_path = lake_root / report.snapshot_segment
    assert segment_path.exists()
    row = journal.read_segment(segment_path).to_pylist()[0]
    assert row["ticker"] == "QQQ"
    assert row["realtime"] is True
    assert row["cusip"] == "444444444"
    assert row["row_kind"] == journal.ROW_KIND_DATA
    assert report.snapshot_segment in latest_entries(lake_root)


def test_equity_only_delayed_quote_is_refused(lake_root, tmp_path):
    with pytest.raises(EntitlementError):
        onboard(
            "QQQ",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_quote_vendor("QQQ", realtime=False),
            lake_root=lake_root,
            tickers_path=Path(tmp_path / "tickers.yaml"),
            options=False,
        )


# -- the capture epoch, and the split it forces --------------------------------------
#
# ``--capture-start`` lets a lake captured before its security master existed be seeded
# at the instant capture actually began. The epoch reaches the scope record and nothing else. The
# stamps that record when the command ran keep reading the clock, because threading the
# epoch into one of them would journal today's chain into a backdated partition and
# corrupt captured data. These tests are written against that split.

# A session day before the onboarding run: 2026-08-25 09:30 Eastern, the Tuesday open.
_BACKDATED = datetime(2026, 8, 25, 13, 30, tzinfo=UTC)

# The Eastern calendar date of the epoch above, which is the date the mapping opens on.
_BACKDATED_DAY = date(2026, 8, 25)

# The Monday of the week these tests live in. Its sessions run Monday through Friday, so
# the onboarding run at ``_MID_SESSION`` falls on the Thursday.
_WEEK = date(2026, 8, 24)


class _NoCallVendor:
    """A vendor that fails the test if anything reaches it."""

    def get_chain(self, *args, **kwargs):
        raise AssertionError("the vendor was called")

    def get_quotes(self, *args, **kwargs):
        raise AssertionError("the vendor was called")


def _record_one(lake_root: Path, surface: str, ticker: str, slot: datetime) -> None:
    """Put one recorded row on disk, standing for a captured cycle."""
    batch = journal.gap_rows(surface, ticker=ticker, slots=[slot], error_class="x")
    stamp = slot.strftime(gap.SEGMENT_STAMP_FORMAT)
    with journal.SegmentWriter.open(lake_root, surface, ticker, slot.date(), stamp, 1) as writer:
        writer.write_cycle(batch)


def _segment_slots(lake_root: Path, surface: str, ticker: str, day: date) -> list[str]:
    """Every minute recorded under one ticker-day, across all of its segments."""
    directory = journal.segment_dir(lake_root, surface, ticker, day)
    if not directory.is_dir():
        return []
    found: list[str] = []
    for path in sorted(directory.glob("*.arrows")):
        found += journal.read_segment(path).column("snap_ts").to_pylist()
    return sorted(found)


def _write_config(path: Path, *, lake_root: Path) -> Path:
    """A machine-local config naming a throwaway lake, for the tests that drive ``main``."""
    path.write_text(
        f"lake_root: {lake_root}\n"
        f"backup_target: {lake_root}/backup\n"
        "healthchecks_ping_key: PINGKEY\n"
        "ntfy_topic: mytopic\n"
        "schwab_api_key: SCHWABKEY\n"
        "schwab_app_secret: SCHWABSECRET\n"
    )
    return path


def test_an_explicit_epoch_reaches_the_master_the_mapping_and_the_span(lake_root, tmp_path):
    """The three call sites that take the epoch, and nothing else.

    All three are the scope record: what the master says capture began at, the date the
    ticker mapping opens on, and where the capture span starts.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        capture_start=_BACKDATED,
    )

    assert report.capture_start == _BACKDATED

    master = SecurityMaster.read(master_path(lake_root))
    assert master.capture_start_of(1) == _BACKDATED

    mapping = next(iter(master))
    assert mapping.valid_from == _BACKDATED_DAY
    assert mapping.valid_to is None

    span = CaptureSpans.read(spans_path(lake_root)).spans_of(1)[0]
    assert span.start == _BACKDATED
    assert span.end is None

    # The day before the epoch is outside the mapping's range, so the backdating is real
    # rather than a value written somewhere nothing reads.
    assert master.resolve("SPY", date(2026, 8, 24), id_type=ID_TYPE_TICKER) is None
    assert master.resolve("SPY", _BACKDATED_DAY, id_type=ID_TYPE_TICKER) == 1


def test_the_journaled_snapshot_carries_the_clock_not_the_epoch(lake_root, tmp_path):
    """The verification snapshot is a cycle that ran now, whatever epoch the scope took.

    Stamping it from the epoch would write today's chain into a backdated partition, and
    a captured minute written to the wrong day cannot be re-captured.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        capture_start=_BACKDATED,
    )

    # ``cycle_start`` decides the segment's day, so the partition says which one was used.
    assert f"date={_MID_SESSION.date().isoformat()}" in report.snapshot_segment
    assert f"date={_BACKDATED_DAY.isoformat()}" not in report.snapshot_segment

    # The journal stores its instants as ISO strings, so the comparison is made in that
    # shape rather than against a datetime, which would compare unequal whatever was
    # written and prove nothing.
    ran_at = _MID_SESSION.isoformat()
    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    for row in rows:
        assert row["snap_ts"] == ran_at
        assert row["fetch_ts"] == ran_at
        assert row["fetch_end_ts"] == ran_at


def test_the_equity_only_snapshot_carries_the_clock_too(lake_root, tmp_path):
    """The quote branch stamps its own ``fetch_end_ts``, so it owes its own test.

    Two of the five stamps are the same name on two branches. A test that drives only the
    chain branch leaves the quote branch's stamp free to take the epoch, and mutating it
    proved exactly that.
    """
    report = onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("QQQ", realtime=True),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=False,
        capture_start=_BACKDATED,
    )

    assert report.capture_start == _BACKDATED
    assert f"date={_MID_SESSION.date().isoformat()}" in report.snapshot_segment

    ran_at = _MID_SESSION.isoformat()
    row = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()[0]
    assert row["snap_ts"] == ran_at
    assert row["fetch_ts"] == ran_at
    assert row["fetch_end_ts"] == ran_at


def test_both_manifest_entries_carry_the_clock_not_the_epoch(lake_root, tmp_path):
    """``fetched_at`` records when this command ran, on the master and on the spans file."""
    onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        capture_start=_BACKDATED,
    )

    entries = latest_entries(lake_root)
    assert entries[MASTER_PARTITION]["fetched_at"] == _MID_SESSION.isoformat()
    assert entries[SPANS_PARTITION]["fetched_at"] == _MID_SESSION.isoformat()


def test_an_epoch_after_now_is_refused_and_writes_nothing(lake_root, tmp_path, capsys):
    """A span opened in the future reports out of scope, so the ticker stops being captured.

    A mistyped year is enough to do it, and nothing says so, so the refusal runs before
    anything is written.
    """
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)

    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "SPY",
                "--capture-start",
                "2099-01-04T14:30:00+00:00",
                "--config",
                str(config_path),
                "--tickers",
                str(tickers_path),
            ]
        )

    assert exit_info.value.code == 2
    assert "after now" in capsys.readouterr().err
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not spans_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_a_naive_epoch_is_refused_and_writes_nothing(lake_root, tmp_path, capsys):
    """A bare date parses as a naive midnight, and it is refused the same way.

    The line it prints is asserted here rather than the exit code alone. Deleting the
    naive branch still exits 2, because comparing a naive instant against now raises a
    ``TypeError`` that ``argparse`` turns into its own unnamed failure, so an exit code
    on its own holds nothing.
    """
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)

    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "SPY",
                "--capture-start",
                "2026-09-08",
                "--config",
                str(config_path),
                "--tickers",
                str(tickers_path),
            ]
        )

    assert exit_info.value.code == 2
    assert "naive midnight" in capsys.readouterr().err
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not spans_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_the_naive_refusal_names_the_offset_as_the_fix(tmp_path, capsys):
    """A bare date is the likely first attempt, so the line has to say what to type instead."""
    with pytest.raises(SystemExit):
        main(["SPY", "--capture-start", "2026-09-08"])

    printed = capsys.readouterr().err
    assert "offset" in printed
    assert "+00:00" in printed


def test_omitting_the_epoch_stamps_the_clock(lake_root, tmp_path):
    """The option left out reproduces the onboarding that existed before it."""
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert report.capture_start == _MID_SESSION

    master = SecurityMaster.read(master_path(lake_root))
    assert master.capture_start_of(1) == _MID_SESSION
    assert next(iter(master)).valid_from == _MID_SESSION.astimezone(MARKET_TZ).date()

    span = CaptureSpans.read(spans_path(lake_root)).spans_of(1)[0]
    assert span.start == _MID_SESSION


def test_a_second_onboarding_with_an_epoch_is_refused_whatever_it_carries(lake_root, tmp_path):
    """The epoch decides whether a run counts as a re-onboard, so a second one is refused.

    An earlier epoch resolves to nothing and registers a second instrument for the same
    ticker, which is a corrupt master. A later one resolves fine and drops the correction
    silently. Both are what a person does on noticing the first epoch was wrong.
    """
    tickers_path = tmp_path / "tickers.yaml"
    first = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=True,
    )

    later_clock = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    for epoch in (_BACKDATED, _MID_SESSION, datetime(2026, 8, 27, 16, 0, tzinfo=UTC)):
        with pytest.raises(OnboardError) as refusal:
            onboard(
                "SPY",
                clock=ManualClock(start=later_clock),
                vendor=_NoCallVendor(),
                lake_root=lake_root,
                tickers_path=tickers_path,
                options=True,
                capture_start=epoch,
            )
        # The refusal cannot tell a second attempt from a re-run after a partial one, so
        # it has to name the epoch the master holds and the way back to the idempotent
        # path. Without that the operator is left editing a parquet file.
        message = str(refusal.value)
        assert "already maps SPY" in message
        assert _MID_SESSION.isoformat() in message
        assert "--capture-start" in message

    # One instrument, one mapping, one span. A second registration would leave the ticker
    # resolving to two ids on every date the two mappings share.
    master = SecurityMaster.read(master_path(lake_root))
    assert master.instrument_ids() == {first.instrument_id}
    ticker_rows = [m for m in master if m.id_type == ID_TYPE_TICKER and m.id_value == "SPY"]
    assert len(ticker_rows) == 1
    assert master.resolve("SPY", _BACKDATED_DAY, id_type=ID_TYPE_TICKER) is None

    spans = CaptureSpans.read(spans_path(lake_root))
    assert len(spans.spans_of(first.instrument_id)) == 1


def test_a_backdated_span_marks_nothing_into_a_sealed_date(lake_root, tmp_path):
    """The startup walk skips a sealed date and stops below the span.

    Opening a span over a week the lake already sealed invites the worry that the walk
    floods that history with markers. It does not. A date the manifest has sealed is
    skipped, and a day no span covers ends the walk. What does change is that the walk
    starts working at all, because a lake with no spans places no ticker in scope.
    """
    surface = journal.QUOTES_SURFACE
    sealed_day = date(2026, 8, 26)
    tickers_path = tmp_path / "tickers.yaml"

    # A day already sealed: rows on disk and a manifest entry naming its partition.
    sealed_slot = et(2026, 8, 26, 15, 0)
    _record_one(lake_root, surface, "QQQ", sealed_slot)
    sealed_key = (
        LakePaths(lake_root).partition_path(surface, "QQQ", sealed_day).relative_to(lake_root)
    ).as_posix()
    append_manifest(
        lake_root,
        partition=sealed_key,
        source="compaction",
        sha256="0" * 64,
        rows=1,
        fetched_at=_MID_SESSION.isoformat(),
    )
    before = _segment_slots(lake_root, surface, "QQQ", sealed_day)
    assert before == [sealed_slot.isoformat()]

    onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("QQQ", realtime=True),
        lake_root=lake_root,
        tickers_path=tickers_path,
        options=False,
        capture_start=_BACKDATED,
    )

    marker = gap.GapMarker(
        lake_root=lake_root,
        roster=lambda: load_tickers(tickers_path),
        session_clock=SessionClock(
            clock=ManualClock(start=_MID_SESSION), calendar=weekday_sessions(_WEEK)
        ),
        master=lambda: SecurityMaster.read(master_path(lake_root)),
        spans=lambda: CaptureSpans.read(spans_path(lake_root)),
        pid=4242,
    )
    report = marker.on_start()

    assert report.problems == ()
    assert sealed_key in report.sealed
    assert _segment_slots(lake_root, surface, "QQQ", sealed_day) == before

    # The walk marked the backdated session day, which is what arming it was for, and
    # stopped at the day below the span rather than walking into a history nobody meant
    # to capture.
    marked = {span.day for span in report.spans}
    assert _BACKDATED_DAY in marked
    assert sealed_day not in marked
    assert date(2026, 8, 24) not in marked


# -- fetching the chain by its date-window plan ---------------------------------------
#
# A whole chain in one request exceeds Schwab's gateway body limit, which comes back as a
# 502. Onboarding used to make exactly that request, so neither anchor ticker could be
# onboarded against a live chain at all. These run against that.

# The default plan's windows on the onboarding day, the concrete ranges the fetch asks
# for. Derived rather than typed, so a plan change moves the fixtures with it.
_WINDOWS = DEFAULT_CHAIN_PLAN.windows_for(_MID_SESSION_DAY)

# The window holding the fixture body's one expiration, 2026-09-18. It is the second, the
# ten-to-thirty-day range, so failing it is what makes a fetch lose every contract while
# the other four still answer.
_CONTRACT_WINDOW = _WINDOWS[1]


def _too_big() -> Interaction:
    """The 502 a bare whole-chain request comes back with, the fault this issue names.

    ``errorcode`` carries ``TooBigBody`` at the top level, the shape the offline fakes
    use for the gateway's size fault.
    """
    return Interaction(
        endpoint="chains",
        params=chain_params("SPY"),
        status=502,
        body={"errorcode": "protocol.http.TooBigBody"},
    )


def _failed_window(window: tuple[date, date | None], status: int = 502) -> Interaction:
    """A recorded failure for one window, so the fetch gives it up under ``http_<status>``.

    The status is deliberately not the ``TooBigBody`` fault, which the fetcher would
    split and refetch instead of giving up on.
    """
    from_date, to_date = window
    return Interaction(
        endpoint="chains",
        params=chain_params("SPY", from_date=from_date, to_date=to_date),
        status=status,
        body={"error": "refused"},
    )


class _SlowVendor:
    """Advances the manual clock by a fixed span on every chain call.

    The manual clock only moves when told to, so without this every window's fetch is
    instantaneous and a round trip spanning five windows reads the same as one spanning
    a single call.
    """

    def __init__(self, inner, clock, *, seconds: float) -> None:
        self._inner = inner
        self._clock = clock
        self._seconds = seconds

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        self._clock.advance(self._seconds)
        return self._inner.get_chain(
            symbol, from_date=from_date, to_date=to_date, strike_count=strike_count
        )

    def get_quotes(self, symbols):
        return self._inner.get_quotes(symbols)


def test_a_chain_too_big_for_one_request_still_onboards(lake_root, tmp_path):
    """The case that failed live: the bare whole-chain request 502s, the windows do not.

    Both anchor tickers came back ``HTTP 502`` with errorcode ``protocol.http.TooBigBody``
    on every onboarding attempt, because the command asked for the whole chain in one
    request. This vendor answers that request the same way and answers each planned
    window normally, so a run that still made the bare request cannot pass.
    """
    vendor = CassetteVendor(
        windowed_chain_cassette(
            "SPY",
            _MID_SESSION_DAY,
            _chain_body(is_delayed=False),
            extra=(_too_big(),),
        )
    )

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=vendor,
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert report.contract_count == 2
    assert report.partial_chain is False
    assert report.realtime_verified is True
    assert (lake_root / report.snapshot_segment).exists()


def test_the_journaled_segment_carries_the_windows_the_fetch_ran(lake_root, tmp_path):
    """The plan's concrete ranges reach the rows, rather than the empty tuple of before.

    A chains row records which window fetched it. Onboarding passed empty tuples while it
    fetched bare, so its segments said nothing about how the rows were collected and read
    differently from every segment the loop writes.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    from_date, to_date = _CONTRACT_WINDOW
    for row in rows:
        assert row["window_start"] == from_date.isoformat()
        assert row["window_end"] == (None if to_date is None else to_date.isoformat())


def test_a_partial_chain_proves_the_entitlement_and_says_it_was_partial(lake_root, tmp_path):
    """One successful window is enough to prove real-time, and the anchor says it is partial.

    A count taken off part of a chain reads exactly like a whole one in the sign-off
    block, and the anchor is what every later median-relative check measures against. The
    failed window's absence markers ride the same segment, so the shortfall is on disk
    as well as in the report.
    """
    body = _chain_body(is_delayed=False)
    # Two expirations, one in each of the first two windows, so one window can fail while
    # the other still returns a contract.
    near = _WINDOWS[0][0].isoformat()
    body["callExpDateMap"][f"{near}:0"] = {
        "640.0": [
            {
                "putCall": "CALL",
                "symbol": "SPY   260827C00640000",
                "expirationDate": f"{near}T20:00:00.000+00:00",
                "bid": 1.5,
            }
        ]
    }
    vendor = CassetteVendor(
        windowed_chain_cassette(
            "SPY", _MID_SESSION_DAY, body, extra=(_failed_window(_CONTRACT_WINDOW),)
        )
    )

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=vendor,
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    # The near window's one contract is what came back, not the three the body describes.
    assert report.realtime_verified is True
    assert report.contract_count == 1
    assert report.partial_chain is True
    assert "(partial chain: a window failed)" in report.render()

    # The failed window's absence marker landed in the same segment, so the segment says
    # what it missed rather than reading as a whole chain of one contract.
    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    markers = [row for row in rows if row["row_kind"] != journal.ROW_KIND_DATA]
    assert [row["error_class"] for row in markers] == ["http_502"]
    assert markers[0]["window_start"] == _CONTRACT_WINDOW[0].isoformat()


def test_every_window_failing_refuses_and_writes_nothing(lake_root, tmp_path):
    """The whole-chain failure the bare 502 used to be, now named by its error class.

    There is no single response left to read a status off, so the refusal names the first
    failed window's class. Nothing is written, because the refusal runs ahead of the
    roster, the master, and the journal, exactly where the bare status check ran.
    """
    tickers_path = tmp_path / "tickers.yaml"
    vendor = CassetteVendor(
        Cassette(interactions=tuple(_failed_window(window, 401) for window in _WINDOWS))
    )

    with pytest.raises(OnboardError) as refusal:
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=vendor,
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=True,
        )

    message = str(refusal.value)
    assert "first chain snapshot for SPY failed" in message
    assert "http_401" in message

    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_a_chain_with_no_contract_refuses_rather_than_pinning_a_zero_anchor(lake_root, tmp_path):
    """Every window answers 200 and carries nothing, which is not the same as no body.

    The sign-off pins the first snapshot's contract count as the day-one plausibility
    anchor, and the median-relative battery has nothing else to measure against until
    history accrues. An anchor of zero is worse than no anchor. The refusal runs before
    the write, so no zero-row segment and no ``rows=0`` manifest entry is left behind.
    """
    tickers_path = tmp_path / "tickers.yaml"
    empty = {"symbol": "SPY", "isDelayed": False, "callExpDateMap": {}, "putExpDateMap": {}}

    with pytest.raises(OnboardError) as refusal:
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=CassetteVendor(windowed_chain_cassette("SPY", _MID_SESSION_DAY, empty)),
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=True,
        )

    assert "carried no contract" in str(refusal.value)
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not (lake_root / "journal").exists()


def test_a_backdated_epoch_plans_the_same_windows_as_a_run_without_one(lake_root, tmp_path):
    """The windows come off the clock's date, never the epoch's.

    ``--capture-start`` can put the epoch a week back, and it is an Eastern date besides,
    while the rows land under the clock's own UTC date. Planning the windows against the
    epoch would ask for ranges nothing in the day being captured falls in, and the fetch
    would answer that as a chain whose every window failed.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        capture_start=_BACKDATED,
    )

    # The epoch still reaches the scope record, and it reaches the windows nowhere.
    assert report.capture_start == _BACKDATED
    assert report.contract_count == 2
    assert report.partial_chain is False

    from_date, to_date = _CONTRACT_WINDOW
    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    for row in rows:
        assert row["window_start"] == from_date.isoformat()
        assert row["window_end"] == (None if to_date is None else to_date.isoformat())


def test_the_journaled_round_trip_spans_every_window(lake_root, tmp_path):
    """``fetch_ts`` and ``fetch_end_ts`` come off the fetch, so they cover the whole fetch.

    Onboarding stamped its own pair around one call. Keeping that pair would journal a
    round trip around a request that no longer happens, and the five windows the fetch
    really spends would read as an instantaneous fetch.
    """
    clock = ManualClock(start=_MID_SESSION)
    seconds = 3
    vendor = _SlowVendor(_chain_vendor(is_delayed=False), clock, seconds=seconds)

    report = onboard(
        "SPY",
        clock=clock,
        vendor=vendor,
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    for row in rows:
        fetch_ts = datetime.fromisoformat(row["fetch_ts"])
        fetch_end_ts = datetime.fromisoformat(row["fetch_end_ts"])
        # One span per window of the plan, which is more than any single call's.
        assert (fetch_end_ts - fetch_ts).total_seconds() == seconds * len(_WINDOWS)
        assert (fetch_end_ts - fetch_ts).total_seconds() > seconds


def test_the_equity_only_branch_still_stamps_its_own_round_trip(lake_root, tmp_path):
    """``--no-options`` is untouched: one quote, its own pair, and no chain call at all.

    The chain branch takes its stamps from the fetch now, so the two branches stamp
    differently. This is what holds the quote branch where it was.
    """
    clock = ManualClock(start=_MID_SESSION)
    quote_vendor = _quote_vendor("QQQ", realtime=True)

    class _NoChain:
        def get_chain(self, *args, **kwargs):
            raise AssertionError("the equity-only path fetched a chain")

        def get_quotes(self, symbols):
            clock.advance(5)
            return quote_vendor.get_quotes(symbols)

    report = onboard(
        "QQQ",
        clock=clock,
        vendor=_NoChain(),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=False,
    )

    assert report.contract_count is None
    assert report.partial_chain is False
    assert "day-one anchor" not in report.render()

    row = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()[0]
    assert row["snap_ts"] == _MID_SESSION.isoformat()
    assert row["fetch_ts"] == _MID_SESSION.isoformat()
    assert row["fetch_end_ts"] == (_MID_SESSION + timedelta(seconds=5)).isoformat()
    # A quote row has no window columns at all, so the quote branch has nothing to name.
    assert "window_start" not in row


def test_the_fixture_builder_refuses_an_expiration_no_window_holds(lake_root):
    """A short recording reads as a failed fetch, so the builder refuses to make one.

    ``_fetch_window`` catches every raised exception and files it as a failed window, so
    a ``CassetteError`` for a window a fixture forgot is indistinguishable from a window
    the vendor refused. An expiration dated before the session date falls in no window,
    and dropping it quietly would leave a recording holding fewer contracts than the body
    it was built from.
    """
    body = _chain_body(is_delayed=False)
    body["putExpDateMap"]["2026-08-01:0"] = {
        "600.0": [
            {
                "putCall": "PUT",
                "symbol": "old",
                "expirationDate": "2026-08-01T20:00:00.000+00:00",
            }
        ]
    }

    with pytest.raises(ValueError, match="falls in no window"):
        windowed_chain_interactions("SPY", _MID_SESSION_DAY, body)


def test_the_fixture_builder_refuses_a_contract_dated_off_its_map_key(lake_root):
    """The recording places by map key, production stamps by the contract's own field.

    A body where the two disagree records a contract under one window and journals it
    under another, and every window assertion in this file would then be checking that
    the fixture author kept two dates in step by hand.
    """
    body = _chain_body(is_delayed=False)
    body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]["expirationDate"] = (
        "2026-12-18T21:00:00.000+00:00"
    )

    with pytest.raises(ValueError, match="not the window it would be journalled under"):
        windowed_chain_interactions("SPY", _MID_SESSION_DAY, body)


def test_the_fixture_builder_refuses_a_contract_with_no_expiration_date(lake_root):
    """A contract with no ``expirationDate`` journals null window columns, silently.

    The row still lands and the count is still right, so nothing fails. Only the fetch
    provenance goes missing, which is the column that tells a windowed fetch from the bare
    one this issue replaced. An integration fixture omitted the field exactly this way.
    """
    body = _chain_body(is_delayed=False)
    del body["callExpDateMap"]["2026-09-18:25"]["650.0"][0]["expirationDate"]

    with pytest.raises(ValueError, match="no expirationDate"):
        windowed_chain_interactions("SPY", _MID_SESSION_DAY, body)


def test_onboard_reads_no_plan_file(lake_root, tmp_path, monkeypatch):
    """The core takes everything injected, so it never reaches the config directory.

    ``load_chain_plan`` reads the operator's own config directory. ``fill_option_close``
    defaults its plan that way, which is right for a function the daemon reaches
    directly, and copying it here would make onboarding's own claim false: every
    dependency is injected, so the whole flow runs offline. The wrapper is where the real
    plan is read, so the core falls back to the built-in plan instead.
    """
    import lake.onboard

    def _refuse(*args, **kwargs):
        raise AssertionError("onboard read the machine's chain plan file")

    monkeypatch.setattr(lake.onboard, "load_chain_plan", _refuse)

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    # The built-in plan is what it fell back to, so the fixture's windows still matched.
    assert report.contract_count == 2
    assert report.partial_chain is False


def test_a_named_plan_overrides_the_built_in_one(lake_root, tmp_path):
    """The parameter is a real seam, not a value nothing reads.

    A single-window plan asks for one open-ended range instead of the default's five, so a
    recording made for that plan matches only if the named plan reached the fetch.
    """
    one_window = ChainPlan(((0, None),))
    vendor = CassetteVendor(
        windowed_chain_cassette(
            "SPY", _MID_SESSION_DAY, _chain_body(is_delayed=False), plan=one_window
        )
    )

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=vendor,
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        plan=one_window,
    )

    assert report.contract_count == 2
    assert report.partial_chain is False

    row = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()[0]
    assert row["window_start"] == _MID_SESSION_DAY.isoformat()
    assert row["window_end"] is None


def test_a_recalibrated_split_depth_reaches_the_fetch(lake_root, tmp_path):
    """The ``guards`` parameter is a real seam, the way ``plan`` is.

    ``chain_chunk_max_split_depth`` decides how many midpoint splits a too-big window is
    worth before the range is given up. A machine that tuned it down must get a fetch that
    gives up where it stands rather than one that halves the window and asks for ranges
    the vendor was never asked about.
    """
    one_window = ChainPlan(((0, None),))
    too_big = Interaction(
        endpoint="chains",
        params=chain_params("SPY", from_date=_MID_SESSION_DAY),
        status=502,
        body={"errorcode": "protocol.http.TooBigBody"},
    )

    with pytest.raises(OnboardError) as refusal:
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=CassetteVendor(Cassette(interactions=(too_big,))),
            lake_root=lake_root,
            tickers_path=tmp_path / "tickers.yaml",
            options=True,
            plan=one_window,
            guards=GuardConstants(chain_chunk_max_split_depth=0),
        )

    # Depth 0 gives the one window up where it stands, under the size class. A depth that
    # split would ask for halves this cassette has never heard of, and the raised
    # ``CassetteError`` would come back as ``cassette_error`` instead.
    assert "chain_chunk_failed" in str(refusal.value)


def test_the_wrapper_loads_the_plan_and_passes_the_config_s_guards(
    lake_root, tmp_path, monkeypatch
):
    """``onboard_from_config`` is the only reader of the machine's plan file.

    The core falls back to the built-in plan rather than to a file, so a wrapper that
    stopped loading the plan would leave a machine whose nightly job re-sized its windows
    onboarding by the built-in default, and nothing would say so. The guards travel the
    same way, which is how ``fill_option_close_from_config`` wires its own pair.
    """
    import lake.onboard
    import lake.schwab

    tuned = ChainPlan(((0, None),))
    vendor = CassetteVendor(
        windowed_chain_cassette("SPY", _MID_SESSION_DAY, _chain_body(is_delayed=False), plan=tuned)
    )
    seen: list[GuardConstants] = []

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            return vendor

    real_fetch_chain = lake.onboard.capture.fetch_chain

    def _record(*args, **kwargs):
        seen.append(kwargs["guards"])
        return real_fetch_chain(*args, **kwargs)

    monkeypatch.setattr(lake.schwab, "SchwabVendor", _Stub)
    monkeypatch.setattr(lake.onboard, "load_chain_plan", lambda: tuned)
    monkeypatch.setattr(lake.onboard.capture, "fetch_chain", _record)

    config = write_config(tmp_path, lake_root, guards={"chain_chunk_max_split_depth": 0})
    report = lake.onboard.onboard_from_config(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        config_path=str(config),
        tickers_path=tmp_path / "tickers.yaml",
        token_path=tmp_path / "token.json",
    )

    # The loaded plan reached the fetch: this cassette records one open-ended window, and
    # the built-in plan's five windows would miss every one of them.
    assert report.contract_count == 2
    assert report.partial_chain is False

    # The config's own recalibrated guard reached it too, rather than the pinned default.
    assert [g.chain_chunk_max_split_depth for g in seen] == [0]
