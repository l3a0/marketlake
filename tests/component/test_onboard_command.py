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

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake import gap, journal
from lake.calendar import MARKET_TZ
from lake.capture import CAPTURE_SOURCE
from lake.capture_spans import SPANS_PARTITION, CaptureSpans, spans_path
from lake.cassette import Cassette, Interaction
from lake.manifest import append_manifest, latest_entries
from lake.onboard import MASTER_PARTITION, EntitlementError, OnboardError, main, onboard
from lake.paths import LakePaths
from lake.security_master import ID_TYPE_FIGI, ID_TYPE_TICKER, SecurityMaster, master_path
from lake.session import SessionClock
from lake.tickers import load_tickers
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.vendor import CassetteVendor

# A mid-session instant: 2026-08-27 15:00 UTC is 11:00 ET, the design's "onboarded
# 11:00" case.
_MID_SESSION = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)


def _chain_body(*, is_delayed: bool) -> dict:
    """A minimal SPY chain body with one call and one put."""
    return {
        "symbol": "SPY",
        "isDelayed": is_delayed,
        "callExpDateMap": {
            "2026-09-18:25": {
                "650.0": [{"putCall": "CALL", "symbol": "SPY   260918C00650000", "bid": 4.2}]
            }
        },
        "putExpDateMap": {
            "2026-09-18:25": {
                "650.0": [{"putCall": "PUT", "symbol": "SPY   260918P00650000", "bid": 3.8}]
            }
        },
    }


def _chain_vendor(*, is_delayed: bool) -> CassetteVendor:
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="chains",
                    params={"symbol": "SPY"},
                    status=200,
                    body=_chain_body(is_delayed=is_delayed),
                ),
            )
        )
    )


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
        vendor=_chain_vendor(is_delayed=False),
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
