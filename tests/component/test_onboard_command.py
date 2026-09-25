"""The onboarding command across real files.

These run the onboarding core against a throwaway lake and a throwaway tickers.yaml,
with a cassette-backed vendor and a manual clock. No network and no token are crossed.
The tests that drive the command line cross the real clock, because the command builds its
own rather than taking one. Three of them pass ``--capture-start`` and use an instant far
enough from now that the answer cannot change the outcome. The four that drive a refusal
or a sign-off through ``main`` assert nothing that a date decides, and they reach the
recorded vendor through the one seam the command has, which is where it builds the
Schwab-backed one. The tier is component: onboarding writes and reads real files, with the
clock and vendor still fake.

They cover the slice-1 contract: register with a stamped capture_start and a ticker
mapping only (the FIGI is deferred to a CUSIP-keyed backfill), verify the real-time
entitlement before trusting the ticker, write the roster entry, journal the snapshot,
and persist the master with a manifest entry so the scrub stays clean. A delayed feed is
refused before anything is written.

The last group covers the snapshot's own schema-drift signature, on both surfaces, and
where onboarding sends it. The capture loop pages a phone for a vendor retype. Onboarding
runs in its own process with no alarm behind it, so the sign-off report is its channel.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import gap, journal, schema_drift
from lake.calendar import MARKET_TZ
from lake.capture import CAPTURE_SOURCE, CHAIN_SCHEMA_DRIFT
from lake.capture_spans import SPANS_PARTITION, CaptureSpans, CaptureSpansError, spans_path
from lake.cassette import Cassette, Interaction
from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan
from lake.config import GuardConstants
from lake.manifest import ManifestError, append_manifest, latest_entries
from lake.onboard import MASTER_PARTITION, EntitlementError, OnboardError, main, onboard
from lake.paths import LakePaths
from lake.security_master import (
    ID_TYPE_FIGI,
    ID_TYPE_TICKER,
    KIND_EQUITY,
    SecurityMaster,
    SecurityMasterError,
    master_path,
)
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
    # The windows are fired concurrently, 50 ms apart on the manual clock, so the fetch ends
    # once the last window has been submitted. That is still the clock's day, not the epoch's.
    ended_at = (_MID_SESSION + timedelta(milliseconds=50) * (len(_WINDOWS) - 1)).isoformat()
    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    for row in rows:
        assert row["snap_ts"] == ran_at
        assert row["fetch_ts"] == ran_at
        assert row["fetch_end_ts"] == ended_at


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


def _drifted_window(window: tuple[date, date | None]) -> Interaction:
    """A 200 whose strikes hold a Mapping where the vendor sends a list of contracts.

    This is the payload shape marketlake #305 is about. It answers, so nothing in the status
    or the body's own flags says anything is wrong, and the merge is what refuses it.
    """
    body = _chain_body(is_delayed=False)
    for map_key, put_call in (("callExpDateMap", "C"), ("putExpDateMap", "P")):
        body[map_key]["2026-09-18:25"]["650.0"] = {
            "putCall": put_call,
            "symbol": f"SPY   260918{put_call}00650000",
        }
    from_date, to_date = window
    return Interaction(
        endpoint="chains",
        params=chain_params("SPY", from_date=from_date, to_date=to_date),
        status=200,
        body=body,
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


def test_a_chain_whose_payload_drifted_refuses_before_anything_commits(lake_root, tmp_path):
    """A vendor payload shape change reaches the operator as a named line, not a traceback.

    Every window answers 200 carrying a strike that holds a Mapping where a list of contracts
    belongs. The merge refuses each one by type, so every window is given up under
    ``chain_schema_drift``, no body comes back, and the refusal runs ahead of every durable
    write.

    Before marketlake #305 that shape merged, because ``list.extend`` takes any iterable, and
    the Mapping's keys reached the row builder as contracts. The ``AttributeError`` came out of
    ``journal_snapshot``, which onboarding calls last, so the operator got a traceback after
    the roster, the master, the spans and their manifest entries had all committed. ``main``
    catches ``OnboardError`` and nothing wider on purpose, and a vendor payload change is
    neither of the two categories that decision names.
    """
    tickers_path = tmp_path / "tickers.yaml"
    vendor = CassetteVendor(
        Cassette(interactions=tuple(_drifted_window(window) for window in _WINDOWS))
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
    assert CHAIN_SCHEMA_DRIFT in message

    # Nothing committed: not the roster, not the master, not the spans, not a segment.
    assert not tickers_path.exists()
    assert not master_path(lake_root).exists()
    assert not spans_path(lake_root).exists()
    assert not (lake_root / "journal").exists()
    assert latest_entries(lake_root) == {}


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

    # The vendor advances the manual clock inside each call, which is only well defined
    # when one call runs at a time, so this runs at a cap of 1 and the span is the sum.
    report = onboard(
        "SPY",
        clock=clock,
        vendor=vendor,
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
        guards=GuardConstants(capture_max_concurrency=1),
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


def _stub_the_vendor(monkeypatch, vendor) -> None:
    """Hand ``onboard_from_config`` a recorded vendor instead of the Schwab-backed one.

    ``main`` builds its own vendor from the token file, so a test that drives ``main``
    has this one seam and no other.

    The chain plan is pinned to the built-in default at the same time. That pin changes
    nothing for the callers below, because every one of them passes ``--no-options`` and
    the plan is read only on the options branch. It is here for the first options-path
    test that drives ``main``, since the wrapper is the one reader of the machine's plan
    file and a test must not depend on what that machine happens to hold.
    """
    import lake.onboard
    import lake.schwab

    class _Stub:
        @staticmethod
        def from_token(token_path, *, api_key, app_secret):
            return vendor

    monkeypatch.setattr(lake.schwab, "SchwabVendor", _Stub)
    monkeypatch.setattr(lake.onboard, "load_chain_plan", lambda: DEFAULT_CHAIN_PLAN)


def test_the_seed_spans_refusal_reaches_the_operator_as_one_line(
    lake_root, tmp_path, monkeypatch, capsys
):
    """The refusal that names the next command to run must not arrive under a stack.

    This message is the one that tells an operator to run ``python -m lake.seed_spans``.
    A traceback puts the frames of this module between that sentence and the prompt, so
    the line that matters is the line read past.
    """
    from lake.security_master import KIND_EQUITY

    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=_MID_SESSION, valid_from=_MID_SESSION.date(), ticker="SPY"
    )
    master.write(master_path(lake_root))
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=True))

    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "SPY",
                "--no-options",
                "--config",
                str(config_path),
                "--tickers",
                str(tickers_path),
                "--token",
                str(tmp_path / "token.json"),
            ]
        )

    # The code is compared by type as well as by value. ``SystemExit(2.0)`` satisfies
    # ``== 2`` and exits the real process 1, printing a stray ``2.0``, so the value alone
    # holds nothing here.
    assert exit_info.value.code == 2
    assert type(exit_info.value.code) is int
    captured = capsys.readouterr()
    # The whole line, rather than a prefix and a substring. This is the message the issue
    # exists for, and an exact compare is what refuses a repr-wrapped exception, a missing
    # newline, and a second line printed beside it.
    assert captured.err == (
        "onboard: capture spans are missing but the security master already has "
        "instruments; run `python -m lake.seed_spans` before onboarding\n"
    )
    assert captured.out == ""
    assert not tickers_path.exists()


def test_a_delayed_feed_refused_through_the_command_prints_the_not_trusted_line(
    lake_root, tmp_path, monkeypatch, capsys
):
    """``EntitlementError`` is an ``OnboardError``, so the subclass exits the same way.

    A catch written against ``EntitlementError`` alone would pass this test and fail the
    one above, and a catch written against the base class passes both. This is the one
    that says the subclass is covered.
    """
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=False))

    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "SPY",
                "--no-options",
                "--config",
                str(config_path),
                "--tickers",
                str(tickers_path),
                "--token",
                str(tmp_path / "token.json"),
            ]
        )

    assert exit_info.value.code == 2
    assert type(exit_info.value.code) is int
    captured = capsys.readouterr()
    assert captured.err == (
        "onboard: quote for SPY is not real-time: realtime=False; ticker not trusted\n"
    )
    assert captured.out == ""
    assert not tickers_path.exists()


def test_an_onboarding_that_succeeds_still_returns_zero(lake_root, tmp_path, monkeypatch, capsys):
    """The catch must not swallow the happy path, which is what a bare ``except`` would do."""
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=True))

    code = main(
        [
            "SPY",
            "--no-options",
            "--config",
            str(config_path),
            "--tickers",
            str(tickers_path),
            "--token",
            str(tmp_path / "token.json"),
        ]
    )

    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    # The rendered sign-off, not merely a string with the ticker in it. Printing
    # ``report.ticker`` alone would satisfy a substring check and lose the whole block.
    assert captured.out.startswith("Onboarded SPY\n")
    assert f"  tickers.yaml:    {tickers_path}" in captured.out
    assert load_tickers(tickers_path).get("SPY").options is False


def _drive_main(tmp_path, config_path, tickers_path):
    """Run the command the way the boundary tests below run it."""
    return main(
        [
            "SPY",
            "--no-options",
            "--config",
            str(config_path),
            "--tickers",
            str(tickers_path),
            "--token",
            str(tmp_path / "token.json"),
        ]
    )


def test_a_corrupt_master_still_reaches_the_operator_as_a_traceback(
    lake_root, tmp_path, monkeypatch, capsys
):
    """The catch is bounded to ``OnboardError``, and this covers the boundary from outside.

    A refused onboarding is a normal outcome of the command, so it gets a line. A master
    that cannot be read is a corrupt lake, which is a bug, and the stack names where the
    corruption was found. Widening the catch to the lake-state errors would pass every
    other test in this file and turn that stack into one line that hides the frame.

    The empty stderr is asserted beside the exception. An added handler that prints the
    refusal line and re-raises would satisfy the exception check on its own, and it would
    put a line reading exactly like an operator refusal above a corruption stack.
    """
    path = master_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"this is not a parquet file")
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=True))

    with pytest.raises(SecurityMasterError):
        _drive_main(tmp_path, config_path, tickers_path)

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_an_unreadable_spans_file_still_reaches_the_operator_as_a_traceback(
    lake_root, tmp_path, monkeypatch, capsys
):
    """The second of the three classes the boundary names, covered the same way.

    The design doc's considered-and-rejected entry names a master that cannot be read, a
    spans file that will not parse, and a manifest that refuses an append. One test for
    the master leaves the other two free: widening the catch to ``CaptureSpansError``
    passes a suite that covers only the first.
    """
    path = spans_path(lake_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"this is not a parquet file")
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=True))

    with pytest.raises(CaptureSpansError):
        _drive_main(tmp_path, config_path, tickers_path)

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_a_refused_manifest_append_still_reaches_the_operator_as_a_traceback(
    lake_root, tmp_path, monkeypatch, capsys
):
    """The third class the boundary names. A manifest that will not take an append.

    The manifest is reached from ``record_partition``, past every refusal, so the seam is
    that call rather than a file written ahead of the run. A lake whose manifest refuses
    an append is a lake that cannot record what it just wrote, and the frame that failed
    is what a reader needs.
    """
    import lake.onboard

    def _refuse(*args, **kwargs):
        raise ManifestError("manifest append refused")

    monkeypatch.setattr(lake.onboard, "record_partition", _refuse)
    tickers_path = tmp_path / "tickers.yaml"
    config_path = _write_config(tmp_path / "config.yaml", lake_root=lake_root)
    _stub_the_vendor(monkeypatch, _quote_vendor("SPY", realtime=True))

    with pytest.raises(ManifestError):
        _drive_main(tmp_path, config_path, tickers_path)

    assert capsys.readouterr().err == ""


# -- the snapshot's schema-drift signature, on both surfaces ---------------------------

# A vendor field sent at a type its pinned column refuses. ``openInterest`` is ``int64``
# on chains and ``quote.totalVolume`` is ``int64`` on quotes, so a float refuses on both.
# Onboarding reaches both surfaces, because ``--no-options`` onboards through the same
# ``journal_snapshot`` on quotes, while the close+5 fill is chains alone.
_RETYPED_OPEN_INTEREST = 1234.7
_RETYPED_TOTAL_VOLUME = 7.5


def _drifting_chain_vendor(day: date = _MID_SESSION_DAY) -> CassetteVendor:
    """A chain vendor whose contracts send ``openInterest`` at a refused type."""
    body = _chain_body(is_delayed=False)
    for exp_map in (body["callExpDateMap"], body["putExpDateMap"]):
        for strikes in exp_map.values():
            for contracts in strikes.values():
                for contract in contracts:
                    contract["openInterest"] = _RETYPED_OPEN_INTEREST
    return CassetteVendor(windowed_chain_cassette("SPY", day, body))


def _drifting_quote_vendor(ticker: str) -> CassetteVendor:
    """A quote vendor whose quote block sends ``totalVolume`` at a refused type."""
    return CassetteVendor(
        Cassette(
            interactions=(
                Interaction(
                    endpoint="quotes",
                    params={"symbols": [ticker]},
                    status=200,
                    body={
                        ticker: {
                            "realtime": True,
                            "reference": {"cusip": "444444444"},
                            "quote": {"bidPrice": 1.0, "totalVolume": _RETYPED_TOTAL_VOLUME},
                        }
                    },
                ),
            )
        )
    )


def test_an_onboarding_chain_snapshot_on_a_drifting_field_says_so(lake_root, tmp_path):
    """A ticker onboarded onto a drifting field, and where the finding goes.

    The capture loop pages a phone for this. Onboarding runs in its own process with no
    alarm behind it, so the sign-off report is the operator channel it has, and the
    deferred half of this was that a snapshot journaled a retype and reported nothing at
    all. How long that silence lasts is not a minute: #297 seeded the live lake at 03:25
    UTC, outside any session, so the next ordinary cycle was the following morning's open.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_drifting_chain_vendor(),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert report.routed_columns == ("open_interest",)
    rendered = report.render()
    assert "schema drift:    chains open_interest arrived at a type the column refused" in rendered

    # The signature is read off the batch, and the segment is what carries the evidence.
    row = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()[0]
    assert row["open_interest"] is None
    assert "openInterest" in row["extra"]


def test_an_onboarding_quote_snapshot_on_a_drifting_field_says_so(lake_root, tmp_path):
    """The equity-only half, which nothing else here would reach.

    ``journal.routed_columns`` takes the surface and matches against that surface's own
    ``extra_paths``, so it is not chains-only. ``--no-options`` onboards through the same
    ``journal_snapshot`` on quotes, and a test that covered the chains half alone would
    leave the quotes one free to be wired to the wrong surface's paths.
    """
    report = onboard(
        "QQQ",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_drifting_quote_vendor("QQQ"),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=False,
    )

    assert report.snapshot_surface == journal.QUOTES_SURFACE
    assert report.routed_columns == ("total_volume",)
    assert "schema drift:    quotes total_volume" in report.render()

    row = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()[0]
    assert row["total_volume"] is None
    assert "totalVolume" in row["extra"]


def test_an_onboarding_snapshot_that_meets_no_drift_says_nothing(lake_root, tmp_path):
    """The ordinary onboarding, which is every one this lake has run.

    The report stays silent, so the line an operator has to read means something when it
    is there. The snapshot itself is unchanged, which is the other half of the claim: the
    scan is a read of the built batch and must not touch what lands.
    """
    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert report.routed_columns == ()
    assert "schema drift" not in report.render()
    rows = journal.read_segment(lake_root / report.snapshot_segment).to_pylist()
    assert rows
    assert {row["extra"] for row in rows} == {None}


def test_onboarding_on_a_drifting_field_reaches_no_pager(lake_root, tmp_path, monkeypatch):
    """Why the report is the channel, held by what runs rather than by what is imported.

    The daemon's drift page needs a publisher, and a publisher is built from the two
    secrets in the machine-local config. Onboarding is a hand-run command in its own
    process with no alarm behind it, so wiring a page in here would mean a second place
    that reveals those secrets and a pager whose per-session cap nothing else counts
    against.

    The recorder goes on ``lake.schema_drift.page`` itself, which is the one object any
    route to a page has to reach. An earlier version of this asserted three names were
    absent from ``lake.onboard``, and a function-local import under a fourth name walked
    straight past it.
    """
    called: list[tuple] = []
    monkeypatch.setattr(schema_drift, "page", lambda *args, **kwargs: called.append(args))

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_drifting_chain_vendor(),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert report.routed_columns == ("open_interest",), "the drifting case did not arise"
    assert called == [], "onboarding paged"


# -- the reference reads and the lock ------------------------------------------------------


_RACER_START = datetime(2026, 8, 20, 13, 30, tzinfo=UTC)


def _racing(write, *, on: str = "acquire"):
    """A ``lake_lock`` that runs ``write`` as the hold is taken, or as it is released.

    The pattern is ``test_occ_mapping``'s ``racing_lock``. The ``on="release"`` half is what
    separates a read under *a* lock from a read under *the* lock the write happens in: a
    writer blocked on a read-only hold lands the instant that hold ends, which is before a
    separate write hold is taken. ``onboard`` imports the lock inside the function, so
    patching the module attribute is what the call resolves against.
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


def _onboard_iwm(lake_root: Path, landed: list[int]):
    """What a concurrent ``lake.onboard`` writes: a master row and an open span."""

    def write():
        master = (
            SecurityMaster.read(master_path(lake_root))
            if master_path(lake_root).exists()
            else SecurityMaster()
        )
        iid = master.register(
            kind=KIND_EQUITY,
            capture_start=_RACER_START,
            valid_from=_RACER_START.date(),
            ticker="IWM",
        )
        master.write(master_path(lake_root))
        spans = (
            CaptureSpans.read(spans_path(lake_root))
            if spans_path(lake_root).exists()
            else CaptureSpans()
        )
        spans.open_span(iid, _RACER_START, False)
        spans.write(spans_path(lake_root))
        landed.append(iid)

    return write


def test_an_onboarding_during_the_fetch_is_not_discarded_and_its_id_is_not_reissued(
    lake_root, tmp_path, monkeypatch
):
    """The window holds a whole vendor round trip, and the write is a whole-file rewrite.

    A stale master does not merely drop the other registration. ``SecurityMaster.register``
    takes its id from ``next_instrument_id()`` on the snapshot it is handed, so this ticker is
    issued the id that instrument already holds, and the master's promise that ids never
    change is what breaks.
    """
    landed: list[int] = []
    monkeypatch.setattr("lake.lock.lake_lock", _racing(_onboard_iwm(lake_root, landed)))

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    iwm = landed[0]
    master = SecurityMaster.read(master_path(lake_root))
    assert master.resolve("IWM", _MID_SESSION.date(), id_type=ID_TYPE_TICKER) == iwm, (
        "the ticker onboarded during the fetch was discarded by a stale snapshot"
    )
    assert report.instrument_id != iwm, "two tickers were issued one instrument_id"
    assert CaptureSpans.read(spans_path(lake_root)).has_open_span(iwm), (
        "the ticker onboarded during the fetch lost its capture span"
    )


def test_an_onboarding_that_only_locks_its_write_still_discards_the_other(
    lake_root, tmp_path, monkeypatch
):
    """Reading under *a* lock is not reading under *the* lock the write happens in."""
    landed: list[int] = []
    monkeypatch.setattr(
        "lake.lock.lake_lock", _racing(_onboard_iwm(lake_root, landed), on="release")
    )

    onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    iwm = landed[0]
    assert (
        SecurityMaster.read(master_path(lake_root)).resolve(
            "IWM", _MID_SESSION.date(), id_type=ID_TYPE_TICKER
        )
        == iwm
    ), "a writer that landed as the hold released was discarded"


def test_a_span_closed_during_the_fetch_is_not_reopened(lake_root, tmp_path, monkeypatch):
    """A retire landing in the window has its close discarded by a stale spans snapshot.

    The instrument is a different one from the ticker being onboarded, so nothing here is the
    rejoin path. A reopened span is a retired ticker captured again with nothing saying so.
    """
    master = SecurityMaster()
    iwm = master.register(
        kind=KIND_EQUITY, capture_start=_RACER_START, valid_from=_RACER_START.date(), ticker="IWM"
    )
    master.write(master_path(lake_root))
    spans = CaptureSpans()
    spans.open_span(iwm, _RACER_START, False)
    spans.write(spans_path(lake_root))
    closed_at = _MID_SESSION

    def retire_iwm():
        late = CaptureSpans.read(spans_path(lake_root))
        late.close_span(iwm, closed_at)
        late.write(spans_path(lake_root))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(retire_iwm))

    onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_chain_vendor(is_delayed=False),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=True,
    )

    assert CaptureSpans.read(spans_path(lake_root)).spans_of(iwm)[0].end == closed_at, (
        "a concurrent retire's close was discarded by a stale spans snapshot"
    )


def test_an_explicit_epoch_is_refused_against_a_registration_that_landed_in_the_window(
    lake_root, tmp_path, monkeypatch
):
    """The refusal is asked again under the lock, and this is the case that needs it.

    A ticker another onboarding registered during the fetch resolves inside the hold, so the
    idempotent branch would reuse that instrument and drop this caller's epoch without a word.
    That is the second of the two failures the refusal exists for, so it refuses instead. It
    reaches the operator as one named line and exit 2 the same way the preflight asking does.
    """

    def onboard_spy_first():
        master = (
            SecurityMaster.read(master_path(lake_root))
            if master_path(lake_root).exists()
            else SecurityMaster()
        )
        iid = master.register(
            kind=KIND_EQUITY,
            capture_start=_RACER_START,
            valid_from=_RACER_START.date(),
            ticker="SPY",
        )
        master.write(master_path(lake_root))
        spans = CaptureSpans()
        spans.open_span(iid, _RACER_START, True)
        spans.write(spans_path(lake_root))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(onboard_spy_first))

    with pytest.raises(OnboardError, match="already"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_chain_vendor(is_delayed=False),
            lake_root=lake_root,
            tickers_path=tmp_path / "tickers.yaml",
            options=True,
            capture_start=_MID_SESSION,
        )

    span = CaptureSpans.read(spans_path(lake_root)).spans_of(1)[0]
    assert span.start == _RACER_START, "the other onboarding's epoch was silently replaced"


# -- what the preflight still owes, and what the re-derivation must not invent ------------


class _CountingVendor:
    """A vendor that counts what reached it, so a refusal can be shown to cost no request."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def get_quotes(self, symbols):
        self.calls += 1
        return self.inner.get_quotes(symbols)

    def __getattr__(self, name):
        return getattr(self.inner, name)


def test_an_unreadable_spans_file_refuses_before_the_request_is_spent(lake_root, tmp_path):
    """Moving a read into the lock must not move the refusal it raises past the fetch.

    ``CaptureSpans.read`` raises ``SpansUnreadable`` on a file that will not parse, and an
    operator is owed that for the price of no request and no roster entry. Inside the lock
    alone it arrives after both, and the half-onboarded ticker left in the roster is then
    captured anyway, because ``capture._live_roster`` widens when the spans cannot be read.
    So the spans keep a preflight read whose answer is thrown away.
    """
    master = SecurityMaster()
    master.register(
        kind=KIND_EQUITY, capture_start=_MID_SESSION, valid_from=_MID_SESSION.date(), ticker="SPY"
    )
    master.write(master_path(lake_root))
    spans_path(lake_root).write_bytes(b"not a parquet file at all")
    tickers_path = tmp_path / "tickers.yaml"
    vendor = _CountingVendor(_quote_vendor("QQQ", realtime=True))

    with pytest.raises(CaptureSpansError):
        onboard(
            "QQQ",
            clock=ManualClock(start=_MID_SESSION),
            vendor=vendor,
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=False,
        )

    assert vendor.calls == 0, "the refusal cost a vendor request"
    assert not tickers_path.exists(), "the refusal left an enabled roster entry behind"


def test_a_naive_epoch_refuses_before_the_request_is_spent(lake_root, tmp_path):
    """``register`` and ``open_span`` both refuse it, and both now run inside the lock.

    The command line's own hook catches this first, so only a library caller reaches here.
    Leaving it to those two would spend a request and write the roster before refusing.
    """
    SecurityMaster().write(master_path(lake_root))
    CaptureSpans().write(spans_path(lake_root))
    tickers_path = tmp_path / "tickers.yaml"
    vendor = _CountingVendor(_quote_vendor("SPY", realtime=True))

    with pytest.raises(ValueError, match="timezone-aware"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=vendor,
            lake_root=lake_root,
            tickers_path=tickers_path,
            options=False,
            capture_start=datetime(2026, 8, 20, 0, 0),
        )

    assert vendor.calls == 0
    assert not tickers_path.exists()


def test_a_rejoin_refuses_rather_than_opening_a_span_inside_one_just_closed(
    lake_root, tmp_path, monkeypatch
):
    """The decision is re-derived from the fresh read and the instant must be too.

    A retire landing in the window closes the span at an instant later than the ``epoch``
    this run read before the fetch. The fresh read then shows no open span, so the rejoin
    branch is taken, and opening at that stale instant starts a second span *inside* the
    one just closed. ``open_span`` guards only against an open span, so it accepts the
    overlap, and ``in_scope`` is a union, which reads the recorded retirement away.
    """
    master = SecurityMaster()
    spy = master.register(
        kind=KIND_EQUITY, capture_start=_RACER_START, valid_from=_RACER_START.date(), ticker="SPY"
    )
    master.write(master_path(lake_root))
    spans = CaptureSpans()
    spans.open_span(spy, _RACER_START, False)
    spans.write(spans_path(lake_root))
    closed_at = _MID_SESSION + timedelta(minutes=10)

    def retire_spy():
        late = CaptureSpans.read(spans_path(lake_root))
        late.close_span(spy, closed_at)
        late.write(spans_path(lake_root))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(retire_spy))

    with pytest.raises(OnboardError, match="was retired at"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_quote_vendor("SPY", realtime=True),
            lake_root=lake_root,
            tickers_path=tmp_path / "tickers.yaml",
            options=False,
        )

    after = CaptureSpans.read(spans_path(lake_root)).spans_of(spy)
    assert len(after) == 1, "a second span was opened inside the one the retire closed"
    assert after[0].end == closed_at, "the recorded retirement was read away by an overlap"


def test_a_registration_under_a_later_date_is_refused_rather_than_made_ambiguous(
    lake_root, tmp_path, monkeypatch
):
    """``resolve`` answers as of one date and the master has to be right on every date.

    An onboarding landing in the window under a later market date leaves a mapping this
    run's resolution cannot see. ``register`` guards the duplicate id and never the
    duplicate symbol, so registering on top gives one ticker two instruments over
    overlapping open ranges, and ``resolve`` then raises ``AmbiguousSymbol`` on every date
    they share. No re-run repairs that, which is why it refuses instead.
    """
    SecurityMaster().write(master_path(lake_root))
    CaptureSpans().write(spans_path(lake_root))
    later = _MID_SESSION + timedelta(days=1)

    def onboard_spy_later():
        late = SecurityMaster.read(master_path(lake_root))
        late.register(kind=KIND_EQUITY, capture_start=later, valid_from=later.date(), ticker="SPY")
        late.write(master_path(lake_root))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(onboard_spy_later))

    with pytest.raises(OnboardError, match="already maps it"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_quote_vendor("SPY", realtime=True),
            lake_root=lake_root,
            tickers_path=tmp_path / "tickers.yaml",
            options=False,
        )

    master = SecurityMaster.read(master_path(lake_root))
    assert len([m for m in master.mappings if m.id_value == "SPY"]) == 1
    assert master.resolve("SPY", later.date(), id_type=ID_TYPE_TICKER) is not None, (
        "the master was left ambiguous, which no re-run repairs"
    )


def test_a_second_onboarding_of_the_same_ticker_in_the_window_takes_the_idempotent_path(
    lake_root, tmp_path, monkeypatch
):
    """The decision has to come from the fresh master, not only the bytes written.

    Deriving `existing_id` from the preflight snapshot leaves every write fresh and gets
    the idempotence lookup wrong, which no assertion about file contents can see. The
    earlier races here onboard a *different* ticker, so `existing_id` is `None` either way
    and only `register`'s id assignment is exercised. This one races the same ticker with
    no explicit epoch, which is the case that reaches the lookup itself.
    """
    landed: list[int] = []

    def onboard_spy_first():
        master = (
            SecurityMaster.read(master_path(lake_root))
            if master_path(lake_root).exists()
            else SecurityMaster()
        )
        iid = master.register(
            kind=KIND_EQUITY,
            capture_start=_MID_SESSION,
            valid_from=_MID_SESSION.date(),
            ticker="SPY",
        )
        master.write(master_path(lake_root))
        spans = CaptureSpans()
        spans.open_span(iid, _MID_SESSION, False)
        spans.write(spans_path(lake_root))
        landed.append(iid)

    monkeypatch.setattr("lake.lock.lake_lock", _racing(onboard_spy_first))

    report = onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("SPY", realtime=True),
        lake_root=lake_root,
        tickers_path=tmp_path / "tickers.yaml",
        options=False,
    )

    assert report.already_registered is True, "the ticker registered in the window was not reused"
    assert report.instrument_id == landed[0]
    master = SecurityMaster.read(master_path(lake_root))
    open_ticker_rows = [
        m
        for m in master.mappings
        if m.id_type == ID_TYPE_TICKER and m.id_value == "SPY" and m.valid_to is None
    ]
    assert len(open_ticker_rows) == 1, (
        "one ticker gained two open mappings, which makes resolve raise AmbiguousSymbol forever"
    )


def test_the_unseeded_refusal_is_asked_again_against_the_master_the_lock_writes(
    lake_root, tmp_path, monkeypatch
):
    """A registration landing in the window is what makes a clean lake an unseeded one.

    The preflight asking sees a lake with no instruments and no spans file, which is the
    brand-new lake this guard deliberately lets through. A writer that registers into the
    master and writes no spans file, the shape `occ_mapping` lands in, turns it into the
    lake the guard exists to refuse. Without the second asking the run opens a fresh spans
    file holding only its own ticker, and `seed_spans` will then never give the other one a
    span, because the file exists.
    """

    def register_only():
        master = (
            SecurityMaster.read(master_path(lake_root))
            if master_path(lake_root).exists()
            else SecurityMaster()
        )
        master.register(
            kind=KIND_EQUITY,
            capture_start=_RACER_START,
            valid_from=_RACER_START.date(),
            ticker="IWM",
        )
        master.write(master_path(lake_root))

    monkeypatch.setattr("lake.lock.lake_lock", _racing(register_only))

    with pytest.raises(OnboardError, match="capture spans are missing"):
        onboard(
            "SPY",
            clock=ManualClock(start=_MID_SESSION),
            vendor=_quote_vendor("SPY", realtime=True),
            lake_root=lake_root,
            tickers_path=tmp_path / "tickers.yaml",
            options=False,
        )

    assert not spans_path(lake_root).exists(), (
        "a spans file was opened that leaves the other instrument never-captured"
    )


def test_an_idempotent_re_onboard_of_a_live_ticker_records_no_second_spans_entry(
    lake_root, tmp_path
):
    """The written claim beside the write, which nothing asserted.

    "Write the spans file only when a span was opened, so an idempotent re-onboard of a
    live ticker adds no manifest entry." The bytes would be identical either way, so only
    the entry count says whether the guard is there.
    """
    tickers_path = tmp_path / "tickers.yaml"
    kwargs = dict(lake_root=lake_root, tickers_path=tickers_path, options=False)
    # Distinct pids, because the segment name carries the writer session and both runs
    # share a clock. The same minute from the same pid collides on ``O_CREAT|O_EXCL``.
    onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("SPY", realtime=True),
        pid=1,
        **kwargs,
    )
    before = _spans_entry_count(lake_root)

    onboard(
        "SPY",
        clock=ManualClock(start=_MID_SESSION),
        vendor=_quote_vendor("SPY", realtime=True),
        pid=2,
        **kwargs,
    )

    assert _spans_entry_count(lake_root) == before, (
        "a re-onboard that opened no span still recorded a manifest entry for the spans file"
    )


def _spans_entry_count(lake_root: Path) -> int:
    """Every manifest line keyed to the spans file, not just the latest one."""
    import json

    lines = (lake_root / "manifest.jsonl").read_text().splitlines()
    return sum(
        1 for line in lines if line.strip() and json.loads(line).get("partition") == SPANS_PARTITION
    )
