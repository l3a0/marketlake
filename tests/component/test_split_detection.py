"""The split detector: reading an OCC re-symboling out of rows the lake already sealed.

Nothing here fetches. Every test builds a lake on disk whose chains partitions carry the
vendor's ``optionRoot`` and its deliverable columns, runs the detection against it with a
manual clock, and reads the ledger back off the file the way a reader would.

The lake holds no split in its captured window, so every transition these tests drive is a
fixture. What the live lake supplied instead is the shape of the rows and the traps. SPY and
QQQ carry one root and one deliverable across all 19,799,808 data rows of 2026-09-14 and
2026-09-15, so the two sessions the fixtures below start from are what an ordinary day looks
like, and the three readings that fail on real data are each driven here:

1. A symbol the lake has not seen before is not the signal. The two ordinary sessions gained
   454 new ``occ_symbol`` values on SPY and 542 on QQQ under the unchanged root, because new
   strikes and new expiries list daily.
2. Every symbol changing at once is not the signal either. ``occ_symbol`` is 23 characters on
   2026-09-02 and 21 on the later partitions, because the vendor narrowed an eight-digit
   expiry to six.
3. The root survives both.

**This file carries its own chains schema, and that is deliberate.** The scale guard reads
``strike_price`` and ``underlying_price``, and ``FIXTURE_CHAINS_SCHEMA`` carries neither.
``tests/component/test_oi_view.py`` gives the reasons and this takes the second of them.
Adding these two to the shared schema would break nothing, unlike ``volume``, which
``test_load_chain.test_a_promoted_value_is_lifted_out_of_the_overflow`` asserts is absent from
``_chains(rows)``. What the local schema buys is keeping the fourteen other files
that import the shared one out of this change. ``test_settlement_view.py`` and
``test_continuity_view.py`` do the same.

**Every session here carries a ladder and a spot**, because the scale guard staying silent on
an ordinary day is the claim most of this file's detections exercise. A default row that left
the two columns null would make the guard report every pair uncomparable and assert that
silence nowhere.
"""

from __future__ import annotations

import json
import zlib
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import actions, report
from lake.actions import (
    CHECK_INSTRUMENT_RESOLUTION,
    PROVENANCE_OBSERVED,
    TYPE_SPLIT,
)
from lake.manifest import record_partition, scrub
from lake.occ_mapping import CHECK_OCC_MAPPING
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.security_master import (
    ID_TYPE_OCC,
    ID_TYPE_TICKER,
    KIND_EQUITY,
    MASTER_FILENAME,
    REFERENCE_DIR,
    Mapping,
    MasterUnreadable,
    SecurityMaster,
    master_path,
)
from lake.splits import (
    CHECK_SPLIT_BOUNDARY,
    CHECK_SPLIT_CONSISTENCY,
    CHECK_SPLIT_DELIVERABLE,
    CHECK_SPLIT_PAYLOAD,
    CHECK_STRIKE_SCALE,
    REASON_DELIVERABLE_UNCHANGED,
    REASON_INSTRUMENT_CHANGED,
    REASON_NO_LADDER,
    REASON_NO_OPTION_CLOSE,
    REASON_NO_UNDERLYING,
    REASON_NOT_SEALED,
    REASON_OUT_OF_SCOPE,
    REASON_PARTIAL_READ,
    REASON_PARTITION_ABSENT,
    REASON_QUARANTINED,
    REASON_ROOT_RETURNED,
    REASON_SCALE_WINDOW,
    REASON_STANDARD_SERIES,
    REASON_THIN,
    detect_splits,
)
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

# The shared fixture schema plus the scale guard's two columns, in the shared one's own order.
# Everything else matches it, so the loader's rules apply unchanged.
SPLIT_CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("occ_symbol", pa.string()),
        ("ssid", pa.int64()),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("last", pa.float64()),
        ("open_interest", pa.int64()),
        ("option_root", pa.string()),
        ("multiplier", pa.float64()),
        ("non_standard", pa.bool_()),
        ("mini", pa.bool_()),
        ("deliverable_note", pa.string()),
        ("option_deliverables_list", pa.string()),
        ("is_chain_truncated", pa.bool_()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
        ("strike_price", pa.float64()),
        ("underlying_price", pa.float64()),
    ]
)


def _chains(rows: list[dict] | None = None, schema: pa.Schema | None = None) -> pa.Table:
    """A chains table in this file's schema, filling a column no row names with nulls."""
    schema = SPLIT_CHAINS_SCHEMA if schema is None else schema
    rows = [] if rows is None else rows
    return pa.table({name: [row.get(name) for row in rows] for name in schema.names}, schema=schema)


# Three consecutive sessions. The lake's own 2026-09-14 and 2026-09-15 are the pair that
# carry data, and a third sits after them so a boundary has a session on each side of it.
DAY_ONE = date(2026, 9, 14)
DAY_TWO = date(2026, 9, 15)
DAY_THREE = date(2026, 9, 16)

# The week those three sit in, Monday 2026-09-14 through Friday 2026-09-18. A real calendar
# would answer the same for every date this file names, and the fake is used anyway because
# the seam is what the detection reads and a test decides what a session is. The detection
# asks it which sessions sit between two sealed days, so a fixture pair that is consecutive
# here has nothing between it and one that skips a day has exactly the skipped session.
CALENDAR = weekday_sessions(date(2026, 9, 14))

FIRST_NIGHT = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)  # 20:00 ET on 2026-09-15
# A minute later in the day rather than the same minute. A withheld file is named by its ET
# time of day and filed under the ticker-day its rows belong to, so two runs at the same time
# of day holding the same finding for one ticker-day would collide on one name.
SECOND_NIGHT = datetime(2026, 9, 17, 0, 1, tzinfo=UTC)

RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# The root and the deliverable an ordinary SPY session carries, as the live lake writes them.
ROOT = "SPY"
# The root the OCC issues for an adjusted contract. Schwab returns it in the same column.
ADJUSTED_ROOT = "SPY1"
NOTE = "100 SPY"
ADJUSTED_NOTE = "150 SPY"


def _deliverables(units: float, symbol: str = "SPY", currency: str | None = None) -> str:
    """``option_deliverables_list`` as the vendor encodes it, JSON in a string column."""
    entries = [
        {
            "assetType": "STOCK",
            "currencyType": currency,
            "deliverableUnits": units,
            "symbol": symbol,
        }
    ]
    return json.dumps(entries, sort_keys=True)


def _with_cash(units: float, cash: float) -> str:
    """A deliverable of shares plus cash, which is #136's own example of a non-scalar one."""
    entries = [
        {
            "assetType": "STOCK",
            "currencyType": None,
            "deliverableUnits": units,
            "symbol": "SPY",
        },
        {
            "assetType": "CURRENCY",
            "currencyType": "USD",
            "deliverableUnits": cash,
            "symbol": "USD",
        },
    ]
    return json.dumps(entries, sort_keys=True)


STANDARD = _deliverables(100.0)
ADJUSTED = _deliverables(150.0)


# The default contract, and a second standard one that lists beside it. An adjustment
# re-symbols the open contracts while newly listed standard ones keep the original root, so a
# session on the far side of a boundary carries both and they are different contracts.
DEFAULT_OCC = "SPY   260918C00650000"
CARRIED_OCC = "SPY   260918C00700000"
QQQ_OCC = "QQQ   260918C00650000"


def _ssid(occ_symbol: str) -> int:
    """A fixture's stable contract id for one symbol, the way Schwab's ``ssid`` is stable.

    Derived from the symbol so two rows in one session never share an id by accident, which
    the live lake bears out: every ``ssid`` is distinct within each of SPY's and QQQ's
    2026-09-14, 09-15 and 09-16 sessions. A re-symboling is the one case where the id has to
    be carried across a symbol change by hand, which is what ``_adjusted_row`` does.
    """
    return zlib.crc32(occ_symbol.encode())


_DERIVE = object()


def _strike(occ_symbol: str | None) -> float | None:
    """The strike the OCC symbol itself spells, so a fixture row cannot contradict its symbol.

    The last eight digits are the strike in thousandths, which is the vendor's own encoding and
    what ``ADJUSTED_OCC``'s ``00433330`` means. Deriving it rather than defaulting to one number
    is what gives a session a ladder with more than one rung, and the scale guard reads the
    ladder. A row carrying no symbol at all names no strike either, which is the shape a row
    with nothing on it has.
    """
    return None if occ_symbol is None else int(occ_symbol[-8:]) / 1000


def _row(
    day: date,
    *,
    occ_symbol: str = DEFAULT_OCC,
    ssid: int | None | object = _DERIVE,
    option_root: str | None = ROOT,
    deliverables: str | None = STANDARD,
    note: str | None = NOTE,
    multiplier: float | None = 100.0,
    non_standard: bool | None = False,
    mini: bool | None = False,
    close_tag: str | None = "option_close",
    row_kind: str = "data",
    suspect: bool = False,
    truncated: bool = False,
    ticker: str = "SPY",
    strike: float | None | object = _DERIVE,
    underlying: float | None = 655.0,
) -> dict:
    """One chains row at the session's option close, carrying the deliverable columns.

    ``ssid`` defaults to the one ``_ssid`` derives from the symbol, so a contract keeps its id
    across sessions and two symbols never collide. Pass it to say that a row is the same
    contract as one spelled differently, which is what a re-symboling is.
    """
    if ssid is _DERIVE:
        ssid = _ssid(occ_symbol)
    if strike is _DERIVE:
        strike = _strike(occ_symbol)
    return {
        "snap_ts": f"{day.isoformat()}T20:15:00+00:00",
        "fetch_ts": f"{day.isoformat()}T20:15:00.400+00:00",
        "vendor_quote_ts": f"{day.isoformat()}T20:15:00+00:00",
        "ticker": ticker,
        "occ_symbol": occ_symbol,
        "ssid": ssid,
        "bid": 4.20,
        "ask": 4.25,
        "last": 4.22,
        "open_interest": 1234,
        "option_root": option_root,
        "multiplier": multiplier,
        "non_standard": non_standard,
        "mini": mini,
        "deliverable_note": note,
        "option_deliverables_list": deliverables,
        "is_chain_truncated": truncated,
        "row_kind": row_kind,
        "error_class": None if row_kind == "data" else "vendor_auth_error",
        "suspect": suspect,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
        "strike_price": strike,
        "underlying_price": underlying,
    }


def _gap_day_row(day: date, ticker: str = "SPY") -> dict:
    """One gap row: a minute the cycle attempted and missed, every vendor column null.

    Four days of these per ticker is what the lake's 2026-09-08 through 2026-09-11 hold, from
    a real auth outage, and it is what makes ``load_chain`` raise ``NoOptionClose``.
    """
    return _row(
        day,
        row_kind="gap",
        ssid=None,
        option_root=None,
        deliverables=None,
        note=None,
        multiplier=None,
        non_standard=None,
        mini=None,
        ticker=ticker,
        strike=None,
        underlying=None,
    )


def _adjusted_row(day: date, **kwargs) -> dict:
    """One row of the re-symboled contracts: the gained root and the moved deliverable.

    It carries the default contract's ``ssid``, so it is that contract wearing a new symbol.
    That is what a re-symboling is, and it is what ``lake.occ_mapping`` pairs on.
    """
    defaults = {
        "ssid": _ssid(DEFAULT_OCC),
        "occ_symbol": "SPY1  260918C00433330",
        "option_root": ADJUSTED_ROOT,
        "deliverables": ADJUSTED,
        "note": ADJUSTED_NOTE,
        "non_standard": True,
    }
    return _row(day, **{**defaults, **kwargs})


def _ledger_table():
    """The schema-version ledger recording version 1 at the shape the running code writes."""
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def _mapping(
    instrument_id: int,
    ticker: str,
    *,
    valid_from: date = date(2026, 9, 8),
    valid_to: date | None = None,
) -> Mapping:
    """One master row, built directly.

    ``register`` opens an unbounded mapping and ``remap`` closes the one it replaces, so
    neither can express a symbol two instruments hold over different ranges, nor one
    instrument holding two symbols at once. Both are shapes the walk has to answer for.
    """
    return Mapping(
        instrument_id=instrument_id,
        id_type=ID_TYPE_TICKER,
        id_value=ticker,
        valid_from=valid_from,
        valid_to=valid_to,
        kind=KIND_EQUITY,
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
    )


def _master(*, valid_from: date = date(2026, 9, 8), tickers: tuple[str, ...] = ("SPY",)):
    """A master holding each ticker from ``valid_from``, the way the live lake's does."""
    master = SecurityMaster()
    for ticker in tickers:
        master.register(
            kind=KIND_EQUITY,
            capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
            valid_from=valid_from,
            ticker=ticker,
        )
    return master


def _lake(
    fixture_lake: FixtureLake,
    sessions: dict[tuple[str, date], list[dict]],
    *,
    master: SecurityMaster | None = None,
    quarantine: list[dict] | None = None,
    quotes: tuple[str, date] | None = None,
) -> Path:
    """A lake holding one chains partition per session, plus the ledger and the master.

    ``quotes`` seals a quotes partition beside them, for the test that asks which surface the
    walk enumerates.
    """
    for (ticker, day), rows in sessions.items():
        fixture_lake.with_chains(ticker, day, _chains(rows))
    if quotes is not None:
        fixture_lake.with_partition("quotes", quotes[0], quotes[1], _chains([_row(quotes[1])]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    for entry in quarantine or []:
        fixture_lake.with_quarantine(entry)
    root = fixture_lake.build()
    (master if master is not None else _master()).write(master_path(root))
    return root


def _two_sessions(fixture_lake: FixtureLake, **kwargs) -> Path:
    """The ordinary session, then one that gained the adjusted root beside it.

    Both roots are present on the second day, because an OCC adjustment re-symbols the open
    contracts while newly listed standard contracts keep the original root.
    """
    return _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_TWO, **kwargs),
            ],
        },
    )


def _entries(root: Path) -> list[dict]:
    return actions.read(root)


def _findings(root: Path, day: date) -> list[dict]:
    """Every withheld finding filed for one ticker-day, read back off the files."""
    directory = report.withheld_dir(root, day)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


def _reasons(report_out) -> list[str]:
    return sorted(skip.reason for skip in report_out.skipped)


def _not_splits(report_out) -> list[str]:
    """Why each root change the run met had no corporate action behind it."""
    return sorted(mark.reason for mark in report_out.not_adjustments)


# -- the scale guard's fixtures ---------------------------------------------------------

# A ladder wide enough that a coincidence and a real rescaling are different numbers. Measured
# on the live lake, a stationary SPY ladder confirms at most 0.287785 when spot moves by a whole
# ratio anyway, and a real adjustment confirms at 1.000000. A one-rung ladder cannot tell those
# apart, because one rung either maps or does not.
LADDER = (600.0, 650.0, 700.0, 750.0, 800.0, 810.0, 820.0)


def _occ(strike: float, ticker: str = "SPY") -> str:
    """One contract's symbol at a strike, spelled the way the vendor spells it."""
    return f"{ticker:<6}260918C{int(round(strike * 1000)):08d}"


def _ladder_rows(day: date, strikes, spot: float, ticker: str = "SPY", **kwargs) -> list[dict]:
    """One session listing a contract at each strike, all naming one underlying price."""
    return [
        _row(day, occ_symbol=_occ(strike, ticker), underlying=spot, ticker=ticker, **kwargs)
        for strike in strikes
    ]


def _scale_lake(
    fixture_lake: FixtureLake,
    *,
    before=LADDER,
    after=LADDER,
    spot_before: float = 700.0,
    spot_after: float = 700.0,
    **kwargs,
) -> Path:
    """Two adjacent sessions of one ticker, each with its own ladder and its own spot."""
    return _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, before, spot_before),
            ("SPY", DAY_TWO): _ladder_rows(DAY_TWO, after, spot_after),
        },
        **kwargs,
    )


def _scale_unread(report_out) -> list[str]:
    return sorted(unread.reason for unread in report_out.scale_unread)


# -- the boundary and the ratio ---------------------------------------------------------


def test_a_gained_root_with_a_moved_deliverable_lands_one_split(fixture_lake: FixtureLake):
    """The whole deliverable, end to end.

    The second session carries a root the first did not, its contracts deliver 150 shares
    where the first's delivered 100, and the ratio the ledger records is 1.5.
    """
    root = _two_sessions(fixture_lake)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["type"] == TYPE_SPLIT
    assert entry["split_ratio"] == 1.5
    assert entry["provenance"] == PROVENANCE_OBSERVED
    assert report_out.held == ()
    assert len(report_out.appended) == 1


def test_the_ratio_comes_from_deliverable_units_and_not_from_the_note(
    fixture_lake: FixtureLake,
):
    """``deliverableUnits`` is a typed number and ``deliverable_note`` is free text.

    The gate reads both and they have to agree, and what lands is the typed one. Driving them
    apart by a hair the tolerance admits is what says which of the two the ledger got.
    """
    root = _two_sessions(fixture_lake, deliverables=_deliverables(150.00000001))

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == pytest.approx(1.5000000001, rel=1e-15)
    assert entry["split_ratio"] != 1.5, "the note's 150/100 landed instead of the typed count"


def test_a_split_pays_nothing_and_announces_nothing(fixture_lake: FixtureLake):
    """The convention ``actions.append``'s docstring fixes for this module.

    A split fills ``split_ratio`` and leaves ``cash_amount`` null, and both remaining vendor
    dates are null because a split pays nothing and Schwab carries no announcement date.
    """
    root = _two_sessions(fixture_lake)

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["cash_amount"] is None
    assert entry["pay_date"] is None
    assert entry["declared_date"] is None


def test_both_dates_are_the_boundary_session(fixture_lake: FixtureLake):
    """``observed_on`` and ``ex_date`` are the boundary day and never the night of the run.

    A split detected from a root change has no vendor date at all, so the boundary session is
    the only honest answer for either. The run's own clock reads 2026-09-15 in market time,
    which is the same day here, so the entry is also checked against the day before to say
    the two are not being confused.
    """
    root = _two_sessions(fixture_lake)

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["observed_on"] == DAY_TWO.isoformat()
    assert entry["ex_date"] == DAY_TWO.isoformat()
    assert entry["ex_date"] != DAY_ONE.isoformat()


def test_a_second_night_appends_nothing(fixture_lake: FixtureLake):
    """A split stays visible in sealed chains forever, so the second run has to be inert.

    ``observed_on`` is the boundary session rather than the night the walk ran, which is what
    makes ``same_but_for_recorded_at`` match. A detector stamping the night would append the
    same split every night forever.
    """
    root = _two_sessions(fixture_lake)
    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    second = detect_splits(lake_root=root, clock=ManualClock(SECOND_NIGHT), calendar=CALENDAR)

    assert len(_entries(root)) == 1
    assert second.appended == () and second.held == ()
    assert second.unchanged == 1


# -- what is not a boundary -------------------------------------------------------------


def test_new_strikes_under_the_unchanged_root_are_not_a_boundary(fixture_lake: FixtureLake):
    """The reading the live lake refutes: a symbol the lake has not seen before.

    SPY gained 454 ``occ_symbol`` values it had never carried across the two ordinary
    sessions of 2026-09-14 and 2026-09-15, every one under the unchanged root, because new
    strikes and new expiries list daily. That reading files about a thousand splits a day.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, occ_symbol="SPY   260918C00650000")],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol="SPY   260918C00650000"),
                _row(DAY_TWO, occ_symbol="SPY   260918C00655000"),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    assert report_out.appended == () and report_out.held == ()
    assert report_out.not_adjustments == (), "a new strike was read as a root change"


def test_the_vendor_respelling_every_symbol_is_not_a_boundary(fixture_lake: FixtureLake):
    """The harder version of the same trap, which the lake already holds.

    ``occ_symbol`` is 23 characters on 2026-09-02 and 21 on the later partitions, because the
    vendor narrowed an eight-digit expiry to six. Every symbol changed and no split happened.
    The root slice returns ``SPY`` under both spellings, so root-keying survives a change that
    symbol-keying reads as total churn. ``option_root`` is null here on purpose, which is what
    it is on the lake's own 2026-09-02 partition, so the fallback is what answers.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [
                _row(DAY_ONE, occ_symbol="SPY   20260918C00650000", option_root=None)
            ],
            ("SPY", DAY_TWO): [_row(DAY_TWO, occ_symbol="SPY   260918C00650000", option_root=None)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    assert report_out.appended == () and report_out.held == ()
    assert report_out.not_adjustments == (), (
        "the root came off the symbol whole, so every respelled contract was a new root"
    )


def test_the_same_deliverable_under_a_new_root_is_a_rename_and_not_a_split(
    fixture_lake: FixtureLake,
):
    """``SecurityMaster.remap`` says a rename and an OCC re-symboling are one operation.

    The deliverable is the only thing that separates them, and ``actions.append`` would take
    a ``split_ratio`` of ``1.0`` without complaint. A no-op factor in the ledger is one every
    adjusted view then reads as a real corporate action.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO, option_root=ADJUSTED_ROOT, non_standard=True)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a rename landed a no-op factor"
    assert report_out.held == ()
    assert _not_splits(report_out) == [REASON_DELIVERABLE_UNCHANGED]


# -- the five skips ---------------------------------------------------------------------


def test_a_gap_day_is_skipped(fixture_lake: FixtureLake):
    """A session with no option close is no observation rather than an error.

    ``load_chain`` raises ``NoOptionClose`` on 8 of the lake's 13 sealed chains partitions,
    which is SPY and QQQ across 2026-09-08 to 2026-09-11.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_NO_OPTION_CLOSE]
    assert report_out.held == ()


def test_a_quarantined_partition_is_skipped_and_the_walk_goes_on(fixture_lake: FixtureLake):
    """``lake.oi`` is the precedent: catch it by name rather than losing the walk to it.

    A walk that does not catch ``PartitionQuarantined`` loses every remaining ticker on the
    first quarantined partition. The third session here is the one that says the walk
    continued.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
        },
        quarantine=[
            {"partition": f"chains/ticker=SPY/date={DAY_TWO.isoformat()}.parquet", "verdict": "bad"}
        ],
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_QUARANTINED]
    assert report_out.ticker_days == 3


def test_a_partial_read_is_skipped_rather_than_read_incomplete(fixture_lake: FixtureLake):
    """A table the overflow projection could not present whole cannot bound a boundary.

    An absent schema-version ledger produces the condition for every version at once, and the
    exception refuses a bypass: the projection rides on it as diagnosis rather than a second
    way to get the table. So a comparison made across it would be a comparison against
    contents nobody saw in full.
    """
    fixture_lake.with_chains("SPY", DAY_ONE, _chains([_row(DAY_ONE)]))
    root = fixture_lake.build()
    _master().write(master_path(root))

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_PARTIAL_READ]


@pytest.mark.parametrize("flag", ["suspect", "truncated"])
def test_a_thin_snapshot_cannot_bound_a_boundary(fixture_lake: FixtureLake, flag: str):
    """A thin chain carries a thin root set, so the next ordinary session looks like a gain.

    A response far under its trailing-median contract count is journaled anyway and tagged,
    and ``load_chain`` does not filter on ``suspect`` at all, which is the right division of
    labour: the battery judges a suspect response and the loader does not pre-empt it. So
    this walk is what refuses it.

    The count of times this has happened is zero. ``is_chain_truncated`` and ``suspect`` are
    ``False`` on all 19,799,808 data rows in the lake.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO, **{flag: True})],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_THIN]
    assert _entries(root) == []


def test_a_ticker_day_before_the_master_knows_the_symbol_is_out_of_scope(
    fixture_lake: FixtureLake,
):
    """The first run files a finding that is not a fault, unless the scope test is there.

    The live lake's ``chains/`` holds a SPY 2026-09-02 partition with 2 data rows while the
    master's mappings both begin on 2026-09-08. ``resolve_instrument`` raises
    ``UnresolvedSymbol`` there, and that exception's docstring calls the condition a
    reference-data fault. ``capture_spans.py`` has already decided what such a day is:
    before an instrument's first span is out of scope, never a gap.
    """
    early = date(2026, 9, 2)
    root = _lake(
        fixture_lake,
        {("SPY", early): [_row(early)], ("SPY", DAY_ONE): [_row(DAY_ONE)]},
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_OUT_OF_SCOPE]
    assert report_out.held == (), "an out-of-scope day filed a reference-data fault"
    assert _findings(root, early) == []


def test_a_symbol_the_master_does_not_carry_files_one_finding_for_the_ticker(
    fixture_lake: FixtureLake,
):
    """The other half of the same test: the fault that is real.

    A symbol the master does not carry at all is what ``UnresolvedSymbol`` describes. No day
    of that ticker will resolve, so it files once rather than once per ticker-day, which is
    the same reason ``by_ticker`` lets the instrument enter one level down.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ")],
            ("QQQ", DAY_TWO): [_row(DAY_TWO, ticker="QQQ")],
        },
        master=_master(tickers=("SPY",)),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (held,) = report_out.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION
    assert held.finding.event == TYPE_SPLIT
    (filed,) = _findings(root, DAY_ONE)
    assert filed["exception"] == "QQQ: UnresolvedSymbol"


def test_a_skipped_session_holds_the_boundary_rather_than_guessing_its_date(
    fixture_lake: FixtureLake,
):
    """``ex_date`` sits in the key, so a date the detector gets wrong cannot be repaired.

    A corrected entry lands under a second key rather than superseding, and every adjusted
    price then applies the split twice. The lake's own 2026-09-08 through 2026-09-11 hold gap
    rows and no data, so a root change anywhere in that hole would otherwise surface at the
    next data day and be attributed to it.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a boundary landed under a date nothing bounded"
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_BOUNDARY
    (filed,) = _findings(root, DAY_THREE)
    assert filed["exception"] == "SPY: BoundaryUnbounded"


# -- the gate ---------------------------------------------------------------------------


def test_a_note_that_disagrees_with_the_typed_count_holds_the_split(
    fixture_lake: FixtureLake,
):
    """The gate compares the vendor against itself, and a drifted field is what it catches.

    ``deliverableUnits`` and ``deliverable_note`` are two spellings of one fact. One moving
    while the other does not is the shape that would put a wrong ratio in the ledger while
    looking well-formed.
    """
    root = _two_sessions(fixture_lake, note="200 SPY")

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_CONSISTENCY
    assert held.finding.computed == 1.5
    assert held.finding.against == 2.0


def test_a_note_that_names_no_plain_share_count_holds_the_split(fixture_lake: FixtureLake):
    """A gate missing an input has not agreed, which is what makes it fail closed.

    The live lake's note is ``100 SPY`` on every row of both tickers. A note this module
    cannot read as a plain share count is not guessed at.
    """
    root = _two_sessions(fixture_lake, note="150 SPY plus 25.00 USD")

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_CONSISTENCY
    assert held.finding.against is None


# -- what one float cannot say ----------------------------------------------------------


def test_a_deliverable_carrying_cash_is_held_rather_than_flattened(
    fixture_lake: FixtureLake,
):
    """#136's own example. A contract delivering shares plus cash has no valid multiplier.

    ``actions.append`` carries one ``split_ratio`` float and would take ``1.5`` here without
    objecting, which is the number that reads like a whole-ratio split and is not one. #136
    asks for the event to be surfaced instead of faked, so it is held.
    """
    root = _two_sessions(fixture_lake, deliverables=_deliverables(150.0, currency="USD"))

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_DELIVERABLE
    assert "cash" in str(held.finding.exception)
    (filed,) = _findings(root, DAY_TWO)
    assert filed["exception"] == "SPY: NonScalarDeliverable"


def test_a_deliverable_naming_a_different_security_is_held(fixture_lake: FixtureLake):
    """The same count of a different security is not a split at all.

    A ratio scales what a contract delivers. It cannot say that what is delivered changed.
    """
    root = _two_sessions(fixture_lake, deliverables=_deliverables(150.0, symbol="XYZ"))

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_DELIVERABLE


def test_a_moved_contract_multiplier_is_held(fixture_lake: FixtureLake):
    """A ratio scales what the contract delivers. A moved multiplier scales what it is."""
    root = _two_sessions(fixture_lake, multiplier=150.0)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_DELIVERABLE


def test_a_gained_root_whose_contracts_are_standard_is_a_new_series(
    fixture_lake: FixtureLake,
):
    """An OCC adjustment turns standard contracts into non-standard ones.

    So a chain that begins listing a fresh standard series under a second root has gained a
    root and had no corporate action. This is the ordinary state of a ticker in the weeks
    after a real adjustment, when a new standard series lists beside the adjusted one, and
    reading it as a boundary lands the first split's own ratio inverted.
    """
    root = _two_sessions(fixture_lake, non_standard=False)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a newly listed standard series landed as a split"
    assert report_out.held == ()
    assert _not_splits(report_out) == [REASON_STANDARD_SERIES]


def test_a_gained_root_that_does_not_say_whether_it_is_standard_is_held(
    fixture_lake: FixtureLake,
):
    """Unknown is read as neither, because the two want opposite treatments.

    Without the flag nothing separates a newly listed series from an adjustment, so this
    fails closed the way every other unanswerable question here does. Landing is the one
    outcome the ledger's key cannot take back.
    """
    root = _two_sessions(fixture_lake, non_standard=None)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_PAYLOAD


# -- payloads the record rules refuse ---------------------------------------------------


def test_contracts_that_disagree_about_the_deliverable_hold_rather_than_end_the_run(
    fixture_lake: FixtureLake,
):
    """Taking the first row would let the file's own order decide what the ledger gets.

    That is the rule ``actions._observation`` already states for its own close of record, and
    a run that died on it would lose every ticker it had not reached yet.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO),
                _adjusted_row(DAY_TWO),
                _adjusted_row(DAY_TWO, deliverables=_deliverables(200.0), note="200 SPY"),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_PAYLOAD
    (filed,) = _findings(root, DAY_TWO)
    assert filed["exception"] == "SPY: DeliverableUnreadable"


def test_a_deliverables_column_that_is_not_json_holds_rather_than_ends_the_run(
    fixture_lake: FixtureLake,
):
    """A vendor payload this module cannot parse is not this run's to repair."""
    root = _two_sessions(fixture_lake, deliverables="{not json")

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_PAYLOAD


# -- what the walk enumerates -----------------------------------------------------------


def test_the_walk_reads_chains_and_passes_over_every_other_surface(
    fixture_lake: FixtureLake,
):
    """The dividend extraction reads quotes and this reads chains.

    A key naming any other surface is passed over, along with both ledgers and every
    reference table, which is what ``surface_ticker_days`` takes its surface argument for.
    """
    root = _two_sessions(fixture_lake)
    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.ticker_days == 2, "a non-chains key was counted as a ticker-day"


def test_a_quotes_partition_beside_the_chains_is_not_walked(fixture_lake: FixtureLake):
    """The same rule from the other side: a sealed quotes ticker-day is not enumerated."""
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)]},
        quotes=("SPY", DAY_TWO),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.ticker_days == 1


# -- the command ------------------------------------------------------------------------


def _config(tmp_path: Path, root: Path) -> Path:
    return write_config(tmp_path, lake_root=root)


def test_the_splits_subcommand_runs_the_detection(fixture_lake: FixtureLake, tmp_path: Path):
    """``python -m lake.actions splits`` is this module's invocation."""
    root = _two_sessions(fixture_lake)
    config = _config(tmp_path, root)

    code = actions.main(["splits", "--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    (entry,) = _entries(root)
    assert entry["type"] == TYPE_SPLIT


def test_config_is_accepted_before_the_subcommand_too(fixture_lake: FixtureLake, tmp_path: Path):
    """The subparser declares ``--config`` with ``SUPPRESS``, so the top-level value survives.

    Without that, a subparser's own ``None`` default would overwrite a ``--config`` written
    before the subcommand, and the run would read the machine's configured lake instead.
    """
    root = _two_sessions(fixture_lake)
    config = _config(tmp_path, root)

    code = actions.main(["--config", str(config), "splits"], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    assert len(_entries(root)) == 1


def test_the_bare_command_still_runs_the_dividend_extraction(
    fixture_lake: FixtureLake, tmp_path: Path
):
    """The default did not move when the subcommands landed.

    Seven component call sites and a sentence in ``docs/design.md`` document the extraction as
    running as ``python -m lake.actions``, and making it the default is what leaves all of
    them true. A lake holding chains and no quotes gives the extraction nothing to read, so a
    bare run that landed a split would be the detector running under the wrong name.
    """
    root = _two_sessions(fixture_lake)
    config = _config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    assert _entries(root) == [], "the bare command ran the split detection"


def test_the_dividends_subcommand_names_the_default_out_loud(
    fixture_lake: FixtureLake, tmp_path: Path
):
    """``dividends`` is a name for what the bare command already does, not a second thing."""
    root = _two_sessions(fixture_lake)
    config = _config(tmp_path, root)

    code = actions.main(["dividends", "--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    assert _entries(root) == []


def test_the_render_says_splits_and_names_the_ratio(fixture_lake: FixtureLake):
    """``ExtractionReport.render`` prints "Dividend extraction" and "cash None" here.

    Both are wrong for a split, which is why this is a second form rather than the same one.
    """
    root = _two_sessions(fixture_lake)

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    assert rendered.startswith("Split detection over 2 sealed chains ticker-day(s)")
    assert "ratio 1.5" in rendered
    assert "cash" not in rendered


# -- the strike-vs-spot scale guard -----------------------------------------------------


def test_an_ordinary_pair_names_no_ratio_and_files_nothing(fixture_lake: FixtureLake):
    """The claim the live lake bears out, and the one the guard must not break.

    The four adjacent pairs the lake holds move spot between 0.999745 and 1.006586 and never
    move the ladder at all. Nothing there is within reach of a whole ratio, so no candidate is
    even formed and the confirmation is never computed.
    """
    root = _scale_lake(fixture_lake, spot_before=700.0, spot_after=696.5)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.scale_pairs == 1
    assert report_out.held == ()
    assert _findings(root, DAY_TWO) == []


def test_a_whole_ratio_split_the_root_signal_cannot_see_is_filed(fixture_lake: FixtureLake):
    """The gap this guard exists for.

    A whole-ratio split leaves the deliverable exactly where it was, so ``Deliverable.same_as``
    is true on all six fields and the root signal records nothing. Every strike halves and so
    does spot, and that pair is the only evidence the lake holds.
    """
    root = _scale_lake(
        fixture_lake,
        before=LADDER,
        after=tuple(strike / 2 for strike in LADDER),
        spot_before=700.0,
        spot_after=349.5,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "the root signal cannot land a whole-ratio split"
    (held,) = report_out.held
    assert held.finding.check == CHECK_STRIKE_SCALE
    assert held.finding.computed == 2.0
    assert held.finding.against == pytest.approx(700.0 / 349.5)
    (filed,) = _findings(root, DAY_TWO)
    assert filed["check"] == CHECK_STRIKE_SCALE
    assert filed["computed"] == 2.0
    assert filed["day"] == DAY_TWO.isoformat()


def test_a_reverse_split_is_the_same_test_run_the_other_way(fixture_lake: FixtureLake):
    """``round`` picks the candidate off the reciprocal, so the ratio comes back below one."""
    root = _scale_lake(
        fixture_lake,
        before=LADDER,
        after=tuple(strike * 2 for strike in LADDER),
        spot_before=700.0,
        spot_after=1398.0,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (held,) = report_out.held
    assert held.finding.check == CHECK_STRIKE_SCALE
    assert held.finding.computed == 0.5


def test_a_crash_moves_spot_and_leaves_the_ladder_where_it_was(fixture_lake: FixtureLake):
    """Spot halving alone is not a split, and the ladder is what says so.

    Measured on the live lake, a stationary ladder confirms at most 0.287785 when spot moves by
    a whole ratio anyway. That is what the floor sits above, and it is the crash this must not
    read as a corporate action. The price check for a move like this is #280's, in
    ``bars.CHECK_BAR_CLOSE``.
    """
    root = _scale_lake(fixture_lake, spot_before=700.0, spot_after=349.5)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.scale_pairs == 1
    assert report_out.held == (), "a crash was recorded as a split"


def test_a_split_the_ledger_already_holds_is_counted_and_not_filed(fixture_lake: FixtureLake):
    """A held finding never clears, so a resolved split must stop being filed.

    marketlake #286's manual entry is the only resolution ``write_withheld`` names, and such an
    entry never reaches ``_examine`` at all, because no root appeared. So the ledger is read
    directly, through the snapshot the run already takes.
    """
    root = _scale_lake(
        fixture_lake,
        before=LADDER,
        after=tuple(strike / 2 for strike in LADDER),
        spot_before=700.0,
        spot_after=349.5,
    )
    actions.append(
        root,
        instrument_id=1,
        observed_on=DAY_TWO,
        ex_date=DAY_TWO,
        recorded_at=RECORDED_AT,
        type=TYPE_SPLIT,
        pay_date=None,
        declared_date=None,
        split_ratio=2.0,
        provenance=PROVENANCE_OBSERVED,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.scale_covered == 1
    assert report_out.held == (), "a split the ledger already describes was filed again"


def test_a_landed_uneven_split_does_not_file_a_scale_finding_beside_it(
    fixture_lake: FixtureLake,
):
    """Both signals can fire on one adjustment, and the ledger entry is the whole answer.

    Without the suppression the same line files every night forever for an event the ledger
    already holds, because nothing in ``reports/`` is ever pruned.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [
                *_ladder_rows(DAY_TWO, LADDER, 349.5),
                *[
                    _row(
                        DAY_TWO,
                        occ_symbol=_occ(strike / 2, ADJUSTED_ROOT),
                        # The contract's own id, carried across the symbol change. That is
                        # what a re-symboling is, and ``lake.occ_mapping`` refuses a boundary
                        # where no contract pairs.
                        ssid=_ssid(_occ(strike)),
                        underlying=349.5,
                        option_root=ADJUSTED_ROOT,
                        deliverables=ADJUSTED,
                        note=ADJUSTED_NOTE,
                        non_standard=True,
                    )
                    for strike in LADDER
                ],
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert len(report_out.appended) == 1, "the root signal did not land its own entry"
    assert report_out.scale_covered == 1
    assert [held.finding.check for held in report_out.held] == []


def test_a_skipped_session_between_the_pair_is_not_judged(fixture_lake: FixtureLake):
    """Ladder attrition across a wider window is unmeasured, and a wrong finding never clears.

    ``CHECK_SPLIT_BOUNDARY`` refuses a wide window for its own reason, which is the ledger key.
    This one refuses it because the comparison itself has no measurement behind it.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): _ladder_rows(
                DAY_THREE, tuple(strike / 2 for strike in LADDER), 349.5
            ),
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.scale_pairs == 0
    assert _scale_unread(report_out) == [REASON_SCALE_WINDOW]
    assert report_out.held == ()


def test_a_session_naming_no_single_spot_is_not_judged(fixture_lake: FixtureLake):
    """Two rows disagreeing about the underlying leave no ratio to round.

    Taking the first would let the file's own order decide what the guard compares, which is the
    refusal ``deliverable_of`` already makes about its own reading.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=_occ(650.0), underlying=349.5),
                _row(DAY_TWO, occ_symbol=_occ(700.0), underlying=350.5),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _scale_unread(report_out) == [REASON_NO_UNDERLYING]
    assert report_out.scale_pairs == 0


def test_a_null_underlying_beside_a_named_one_is_not_a_disagreement(fixture_lake: FixtureLake):
    """A row saying nothing does not contradict a row that names the price.

    ``_column`` already treats a column a partition never carried as a session with nothing to
    say, and one null row inside a snapshot is the same thing one row at a time.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [
                *_ladder_rows(DAY_TWO, tuple(s / 2 for s in LADDER), 349.5),
                _row(DAY_TWO, occ_symbol=_occ(300.0), underlying=None),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.scale_unread == ()
    assert [held.finding.check for held in report_out.held] == [CHECK_STRIKE_SCALE]


def test_a_session_listing_no_strike_is_not_judged(fixture_lake: FixtureLake):
    """A ladder with nothing in it is a denominator of zero rather than a confirmation."""
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [_row(DAY_TWO, strike=None, underlying=349.5)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _scale_unread(report_out) == [REASON_NO_LADDER]
    assert report_out.held == ()


def test_a_pair_spanning_two_instruments_is_not_a_pair(fixture_lake: FixtureLake):
    """Two securities' ladders are not comparable, which ``_examine`` already decides.

    The master hands the ticker from one instrument to another between the two sessions, so the
    sessions describe different things and their strikes say nothing about each other.
    """
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_to=DAY_TWO),
            _mapping(2, "SPY", valid_from=DAY_TWO),
        ]
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): _ladder_rows(DAY_TWO, tuple(strike / 2 for strike in LADDER), 349.5),
        },
        master=master,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _scale_unread(report_out) == [REASON_INSTRUMENT_CHANGED]
    assert report_out.held == ()


def test_a_finding_about_an_unreadable_row_does_not_hide_a_real_split(
    fixture_lake: FixtureLake,
):
    """The suppression asks what the walk filed, not whether it filed.

    A gained root whose ``non_standard`` flag is null files ``split_payload``, which says a row
    could not be read rather than that a corporate action was described. Reading any finding as
    an answer would hide a genuine 2:1 behind an unrelated unreadable row, every night, for as
    long as that row survives.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): _ladder_rows(
                DAY_TWO,
                tuple(strike / 2 for strike in LADDER),
                349.5,
                option_root=ADJUSTED_ROOT,
                non_standard=None,
            ),
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    filed = sorted(held.finding.check for held in report_out.held)
    assert filed == [CHECK_SPLIT_PAYLOAD, CHECK_STRIKE_SCALE]
    assert report_out.scale_covered == 0


def test_a_gate_refusing_this_split_does_suppress_the_scale_finding(
    fixture_lake: FixtureLake,
):
    """The other side of the same rule: a refusal *about* this adjustment is an answer.

    The consistency gate holds a ratio the vendor's two spellings disagree about. That names the
    same event, so filing a scale finding beside it would put two lines in front of an operator
    for one split and neither would ever clear.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [
                _row(
                    DAY_TWO,
                    occ_symbol=_occ(strike / 2, ADJUSTED_ROOT),
                    ssid=_ssid(_occ(strike)),
                    underlying=349.5,
                    option_root=ADJUSTED_ROOT,
                    deliverables=_deliverables(200.0),
                    note="150 SPY",
                    non_standard=True,
                )
                for strike in LADDER
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert [held.finding.check for held in report_out.held] == [CHECK_SPLIT_CONSISTENCY]
    assert report_out.scale_covered == 1


def test_a_finding_on_another_ticker_does_not_suppress_this_one(fixture_lake: FixtureLake):
    """The comparison is against what this session filed, not against the run so far.

    ``by_ticker`` walks QQQ before SPY, so a boundary QQQ could not date leaves a finding on the
    list before SPY's pair is judged. Counting from the start of the run rather than from this
    session would read that as SPY's split being described already.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", occ_symbol=QQQ_OCC)],
            ("QQQ", DAY_TWO): [_gap_day_row(DAY_TWO, ticker="QQQ")],
            ("QQQ", DAY_THREE): [
                _row(DAY_THREE, ticker="QQQ", occ_symbol=QQQ_OCC),
                _row(
                    DAY_THREE,
                    ticker="QQQ",
                    occ_symbol="QQQ1  260918C00433330",
                    ssid=_ssid(QQQ_OCC),
                    option_root="QQQ1",
                    deliverables=ADJUSTED,
                    note=ADJUSTED_NOTE,
                    non_standard=True,
                ),
            ],
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): _ladder_rows(DAY_TWO, tuple(strike / 2 for strike in LADDER), 349.5),
        },
        master=SecurityMaster([_mapping(1, "SPY"), _mapping(2, "QQQ")]),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    filed = sorted(held.finding.check for held in report_out.held)
    assert CHECK_SPLIT_BOUNDARY in filed, "the QQQ boundary should still be held"
    assert CHECK_STRIKE_SCALE in filed, "SPY's split was hidden behind QQQ's finding"


def test_the_render_names_each_reason_a_pair_was_not_compared(fixture_lake: FixtureLake):
    """A count with no breakdown says how many and never which, which is half a sign-off."""
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): _ladder_rows(DAY_THREE, LADDER, 700.0),
        },
    )

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    assert "  scale not compared: 1" in rendered
    assert f"    - {REASON_SCALE_WINDOW}: 1" in rendered


def test_the_render_says_what_the_scale_guard_did(fixture_lake: FixtureLake):
    """A run that compared pairs must not read like a run that compared none."""
    root = _scale_lake(fixture_lake, spot_before=700.0, spot_after=696.5)

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    assert "  scale compared: 1" in rendered
    assert "  scale already recorded: 0" in rendered
    assert "  scale not compared: 0" in rendered


def test_the_render_names_each_skip_and_each_non_adjustment_under_its_own_heading(
    fixture_lake: FixtureLake,
):
    """The two by-reason blocks are a shared renderer now, so both surfaces need holding.

    ``by_reason`` moved out of this module into ``lake.actions`` when the dividend walk grew
    the same record, which means a change made for that walk's block silently changes this
    one. This walk asserted its skips and non-adjustments as objects and never as the text an
    operator reads, so the two blocks could swap contents or print under one heading and
    nothing here would notice.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_TWO, non_standard=False),
            ],
            ("SPY", DAY_THREE): [_gap_day_row(DAY_THREE)],
        },
    )

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    not_a_split = rendered.index("  not a split: 1")
    skipped = rendered.index("  skipped:   1")
    assert not_a_split < skipped, "the two blocks are in the order render declares"
    assert f"    - {REASON_STANDARD_SERIES}: 1" in rendered
    assert f"    - {REASON_NO_OPTION_CLOSE}: 1" in rendered
    # Each line sits under its own heading rather than both under one.
    assert rendered.index(f"    - {REASON_STANDARD_SERIES}: 1") < skipped
    assert rendered.index(f"    - {REASON_NO_OPTION_CLOSE}: 1") > skipped


def test_the_subcommand_inherits_the_three_code_contract(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """A refusal reaches the operator as one line rather than a stack.

    ``main`` already has the contract to follow rather than reinvent: 0 when the run landed
    what it found, 1 when something was held and filed, and 2 for an operator mistake with a
    fix behind it. #294 and #300 were this class of defect on the onboarding command, so a
    refusal let out here as a stack trace repeats a fixed bug.
    """
    fixture_lake.with_chains("SPY", DAY_ONE, _chains([_row(DAY_ONE)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    config = _config(tmp_path, root)

    code = actions.main(["splits", "--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 2
    printed = capsys.readouterr()
    assert printed.err.startswith("actions: no security master at")
    assert "Traceback" not in printed.err


def test_a_finding_that_could_not_be_filed_exits_one(
    fixture_lake: FixtureLake, tmp_path: Path, monkeypatch, capsys
):
    """A run that held something and filed nothing reads exactly like a run that found nothing.

    That silence is what the middle exit code exists to break. One unwritable file does not
    cost the other tickers their splits either, so the walk carries the failure to the report
    rather than raising out of it.
    """
    root = _two_sessions(fixture_lake, note="200 SPY")
    config = _config(tmp_path, root)

    def refuse(*args, **kwargs):
        raise OSError("read-only file system")

    monkeypatch.setattr("lake.splits.write_withheld", refuse)

    code = actions.main(["splits", "--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 1
    assert "NOT filed: OSError" in capsys.readouterr().out


def test_the_detection_reads_no_config(fixture_lake: FixtureLake, monkeypatch):
    """Every dependency is injected, the way ``seed_spans`` and the extraction are.

    ``detect_splits`` takes its lake root, its clock and its calendar, so nothing under it
    reaches for the machine's own configuration.
    """
    root = _two_sessions(fixture_lake)

    def refuse(*args, **kwargs):
        raise AssertionError("the detection read a config file")

    monkeypatch.setattr("lake.config.load_config", refuse)

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert len(_entries(root)) == 1


# -- the three other things that change a root set --------------------------------------


def test_a_mini_option_listing_is_not_a_corporate_action(fixture_lake: FixtureLake):
    """A mini contract is a tenth-size contract under its own root.

    So the first one to list gains a root and delivers a tenth of what a standard contract
    does, which is the exact shape of an adjustment. Reading the rows lands a fabricated
    ten-for-one reverse split in an append-only ledger. ``mini`` is ``False`` on all
    19,799,808 data rows the lake holds, so excluding them drops nothing today.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO),
                _row(
                    DAY_TWO,
                    occ_symbol="SPY7  260918C00650000",
                    option_root="SPY7",
                    deliverables=_deliverables(10.0),
                    note="10 SPY",
                    non_standard=True,
                    mini=True,
                ),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a mini listing landed a ten-for-one reverse split"
    assert report_out.held == ()
    assert report_out.not_adjustments == ()


def test_a_root_returning_to_the_chain_is_not_a_gain(fixture_lake: FixtureLake):
    """A set difference has no direction, and that is how the ratio gets computed backwards.

    A root whose contracts all expire out of one session and list again in the next reads as
    an adjustment. Every guard can be armed and the entry still lands, under a date the
    ledger's key cannot correct.
    """
    adjusted = dict(
        option_root=ADJUSTED_ROOT,
        occ_symbol="SPY1  260918C00433330",
        deliverables=ADJUSTED,
        note=ADJUSTED_NOTE,
        non_standard=True,
    )
    root = _lake(
        fixture_lake,
        {
            # Day one carries both. Day two's response omits the adjusted expiry. Day three
            # carries it again, which is not a second adjustment.
            ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **adjusted)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE), _row(DAY_THREE, **adjusted)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a returning root landed a phantom inverse split"
    assert report_out.held == ()
    assert _not_splits(report_out) == [REASON_ROOT_RETURNED]


def test_a_real_split_still_lands_when_the_previous_session_carries_two_roots(
    fixture_lake: FixtureLake,
):
    """The ordinary state of a ticker from the day after any adjustment onwards.

    Reading the previous session whole raises the moment it carries two roots, because its
    contracts disagree about what they deliver. The prior side reads the standard contracts
    instead, which is what the adjustment was made from.
    """
    legacy = dict(
        option_root=ADJUSTED_ROOT,
        occ_symbol="SPY1  260918C00433330",
        deliverables=ADJUSTED,
        note=ADJUSTED_NOTE,
        non_standard=True,
    )
    second = dict(
        option_root="SPY2",
        # The standard contract, re-symboled again. An adjustment renames a contract the
        # chain already carried rather than listing one out of nowhere.
        ssid=_ssid(DEFAULT_OCC),
        occ_symbol="SPY2  260918C00325000",
        deliverables=_deliverables(200.0),
        note="200 SPY",
        non_standard=True,
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **legacy)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _row(DAY_TWO, **legacy),
                _row(DAY_TWO, **second),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 2.0, "the prior side was not read off the standard series"
    assert report_out.held == ()


def test_the_prior_side_falls_back_to_the_previous_session(fixture_lake: FixtureLake):
    """The boundary where every contract was re-symboled at once.

    The boundary session then carries no standard series, so there is no contract still
    naming the old count beside one naming the new, and the previous session is what has it.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_adjusted_row(DAY_TWO)],
        },
    )

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 1.5


def test_a_split_and_then_a_fresh_standard_series_files_nothing_false(
    fixture_lake: FixtureLake,
):
    """The whole post-adjustment lifecycle, which is where the false findings used to be.

    Day two adjusts every contract. Day three lists a standard series beside the adjusted
    one, which is a gained root with no corporate action. A held finding never clears, so one
    false finding here is one file a night forever.

    The root day three gains is ``SPY``, which day one carried, so it is the returning-root
    rule that answers rather than the standard-series one. Both reach the same place. The
    other is driven by :func:`test_a_gained_root_whose_contracts_are_standard_is_a_new_series`,
    where the gained root is one the ticker never carried.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_adjusted_row(DAY_TWO)],
            ("SPY", DAY_THREE): [
                _adjusted_row(DAY_THREE),
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 1.5 and entry["ex_date"] == DAY_TWO.isoformat()
    assert report_out.held == (), "the re-listed standard series filed a finding"
    assert _not_splits(report_out) == [REASON_ROOT_RETURNED]


# -- the run survives what it cannot read -----------------------------------------------


def test_a_note_of_zero_shares_holds_and_the_next_ticker_still_lands_its_split(
    fixture_lake: FixtureLake,
):
    """The gate divides by the note ratio, so a zero on the new side reaches the division.

    ``ZeroDivisionError`` is not a ``SplitError``, so it escapes the walk and ends the run.
    QQQ sorts before SPY, so the second ticker's genuine split is what a dying run costs.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ")],
            ("QQQ", DAY_TWO): [
                _row(DAY_TWO, ticker="QQQ"),
                _adjusted_row(DAY_TWO, note="0 SPY", ticker="QQQ"),
            ],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_TWO),
            ],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 1.5, "the dying run cost the second ticker its split"
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_CONSISTENCY
    assert held.finding.against is None


def test_a_manifested_partition_whose_file_is_gone_is_skipped(fixture_lake: FixtureLake):
    """The manifest records what the lake sealed, not that the file is still on disk."""
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
    )
    fixture_lake.partition_path("chains", "SPY", DAY_TWO).unlink()

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_PARTITION_ABSENT]
    assert report_out.held == ()


def test_a_partition_missing_a_column_reads_it_as_null_rather_than_raising(
    fixture_lake: FixtureLake,
):
    """A session sealed before a column existed has nothing to say about it.

    ``actions._observation`` reads its own columns the same way and for the same reason. A
    ``KeyError`` here would end the whole run rather than skipping one session.
    """
    narrow = pa.schema([f for f in SPLIT_CHAINS_SCHEMA if f.name not in {"option_root", "mini"}])
    fixture_lake.with_chains("SPY", DAY_ONE, _chains([_row(DAY_ONE)], schema=narrow))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    _master().write(master_path(root))

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.ticker_days == 1
    assert report_out.held == () and report_out.skipped == ()


def test_contracts_naming_no_root_at_all_are_held(fixture_lake: FixtureLake):
    """A row with neither root column is a damaged row, not a root of its own.

    Left alone it contributes an empty-string root that the boundary rule reads as a gain,
    and a split lands under a root that names nothing.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO),
                _adjusted_row(DAY_TWO, option_root=None, occ_symbol=None),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_PAYLOAD


# -- the walk's own state ---------------------------------------------------------------


# -- the session the lake never captured ------------------------------------------------


def test_a_session_the_lake_never_captured_is_not_a_pair(fixture_lake: FixtureLake):
    """The reproduction marketlake #431 was filed on.

    The lake holds 2026-09-14 and 2026-09-16 and nothing at all for 2026-09-15, and both the
    ladder and the spot halve across the gap. Before the calendar reached this walk, the
    enumeration came off the manifest alone, so the uncaptured session incremented nothing and
    the pair read as consecutive: the guard judged it, found a whole ratio and filed a finding
    dated to 2026-09-16 while the split's real ex-date is 2026-09-15.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_THREE): _ladder_rows(
                DAY_THREE, tuple(strike / 2 for strike in LADDER), 349.5
            ),
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_NOT_SEALED]
    assert [skip.day for skip in report_out.skipped] == [DAY_TWO]
    assert _scale_unread(report_out) == [REASON_SCALE_WINDOW]
    assert report_out.scale_pairs == 0
    assert _findings(root, DAY_THREE) == [], "a finding was filed under a date nobody can clear"


def test_the_finding_the_gap_used_to_file_does_not_return_on_a_second_night(
    fixture_lake: FixtureLake,
):
    """Nothing filed means nothing to re-file, which is the half of #431 that was permanent.

    ``write_withheld`` files a held finding again every night and nothing prunes ``reports/``,
    so the finding this used to raise was re-filed under 2026-09-16 for ever while an operator
    landing the split at its true 2026-09-15 could not clear it. A pair nobody judged files
    nothing on either night.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_THREE): _ladder_rows(
                DAY_THREE, tuple(strike / 2 for strike in LADDER), 349.5
            ),
        },
    )

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)
    detect_splits(lake_root=root, clock=ManualClock(SECOND_NIGHT), calendar=CALENDAR)

    assert _findings(root, DAY_THREE) == []
    assert _entries(root) == []


def test_a_boundary_across_a_session_the_lake_never_captured_is_held(fixture_lake: FixtureLake):
    """The expensive half. ``ex_date`` sits in the ledger's key and cannot be superseded.

    A root gained across an uncaptured session could be that session's adjustment or the
    boundary session's own, and the walk has no way to tell. Landing a guess writes an entry a
    corrected one cannot replace, only join, and every adjusted price then applies both.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (filed,) = _findings(root, DAY_THREE)
    assert filed["check"] == CHECK_SPLIT_BOUNDARY
    assert report_out.held[0].finding.check == CHECK_SPLIT_BOUNDARY


def test_the_held_boundary_names_both_ends_of_the_window(fixture_lake: FixtureLake):
    """A count and a start leave an operator to work out which session the boundary day was.

    Both dates are what say which sessions to go and look at. They reach the run's own render
    and not the filed record, because ``report.redacted`` keeps two fields and drops whatever
    the exception chose to say, which for an ``OSError`` would be a path on the capture
    machine. ``_finding`` already names that split: "The run's own render prints the whole
    string, so the message reaches the operator there and the class alone reaches the file."
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    (line,) = [line for line in rendered.splitlines() if "BoundaryUnbounded" in line]
    assert f"between {DAY_ONE.isoformat()} and {DAY_THREE.isoformat()}" in line
    assert "1 of them" in line


def test_a_weekend_between_two_sessions_is_not_a_gap(fixture_lake: FixtureLake):
    """Friday and the Monday after it are consecutive sessions, and a split across them lands.

    This is the case that fails if the walk counts days rather than asking the calendar. Two
    calendar days sit between them and no session does, so refusing the pair here would refuse
    one ordinary pair in five for ever.
    """
    friday = date(2026, 9, 18)
    monday = date(2026, 9, 21)
    root = _lake(
        fixture_lake,
        {
            ("SPY", friday): [_row(friday)],
            ("SPY", monday): [
                _row(monday, occ_symbol=CARRIED_OCC),
                _adjusted_row(monday),
            ],
        },
    )

    report_out = detect_splits(
        lake_root=root,
        clock=ManualClock(FIRST_NIGHT),
        calendar=weekday_sessions(date(2026, 9, 14), date(2026, 9, 21)),
    )

    assert _reasons(report_out) == []
    (entry,) = _entries(root)
    assert entry["ex_date"] == monday.isoformat()


def test_a_holiday_between_two_sessions_is_not_a_gap(fixture_lake: FixtureLake):
    """A closed market is no session, so the sessions either side of it are adjacent.

    The live lake's own 2026-09-07 is Labor Day, and it sits inside the one manifest gap SPY
    holds. A walk that treated every absent weekday as uncaptured would report it.
    """
    holiday = date(2026, 9, 15)
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    report_out = detect_splits(
        lake_root=root,
        clock=ManualClock(FIRST_NIGHT),
        calendar=weekday_sessions(date(2026, 9, 14), holidays=[holiday]),
    )

    assert _reasons(report_out) == []
    (entry,) = _entries(root)
    assert entry["ex_date"] == DAY_THREE.isoformat()


def test_a_stretch_below_the_first_readable_session_is_not_reported_as_uncaptured(
    fixture_lake: FixtureLake,
):
    """The live lake's own shape, and why the enumeration waits for a session to be read.

    SPY's manifested chains days run 2026-09-02, then 2026-09-08 onward. 2026-09-02 predates
    the master's ``capture_start``, so it is out of scope rather than a gap, and every session
    between it and the first readable day is out of scope for the same reason. Reporting those
    as uncaptured would file skips every night for a window no pair spans.
    """
    early = date(2026, 9, 2)
    root = _lake(
        fixture_lake,
        {("SPY", early): [_row(early)], ("SPY", DAY_ONE): [_row(DAY_ONE)]},
    )

    report_out = detect_splits(
        lake_root=root,
        clock=ManualClock(FIRST_NIGHT),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7), date(2026, 9, 14)),
    )

    assert _reasons(report_out) == [REASON_OUT_OF_SCOPE]


def test_a_session_the_master_places_out_of_scope_is_not_reported_as_unsealed(
    fixture_lake: FixtureLake,
):
    """Step 1 already decides this for a manifested day, and a day with no partition is owed it.

    A ticker handed from one instrument to another leaves the sessions below the second one's
    first valid mapping placed outside it. The master calls such a day out of scope rather than
    a gap, and without the same question here the walk reports three capture shortfalls a night
    on a window the instrument change already refuses to judge.
    """
    early, late = date(2026, 9, 15), date(2026, 9, 21)
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_to=date(2026, 9, 16)),
            _mapping(2, "SPY", valid_from=late),
        ]
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", early): _ladder_rows(early, LADDER, 700.0),
            ("SPY", late): _ladder_rows(late, LADDER, 700.0),
        },
        master=master,
    )

    report_out = detect_splits(
        lake_root=root,
        clock=ManualClock(FIRST_NIGHT),
        calendar=weekday_sessions(date(2026, 9, 14), date(2026, 9, 21)),
    )

    assert {skip.reason for skip in report_out.skipped} == {REASON_OUT_OF_SCOPE}
    assert [skip.day for skip in report_out.skipped] == [
        date(2026, 9, 16),
        date(2026, 9, 17),
        date(2026, 9, 18),
    ]


def test_a_session_out_of_scope_still_widens_the_window(fixture_lake: FixtureLake):
    """What the master decides is the label an operator reads, never whether the pair is judged.

    The manifested out-of-scope branch increments the same counter, and it has to: a ticker
    retired and brought back has an away period the lake holds nothing across, and a split
    inside it is exactly the boundary whose ``ex_date`` cannot be guessed. So a day the master
    places out of scope is reported as out of scope and refuses the pair all the same.
    """
    away = date(2026, 9, 16)
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_to=away),
            _mapping(1, "SPY", valid_from=date(2026, 9, 17)),
        ]
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", date(2026, 9, 17)): [
                _row(date(2026, 9, 17), occ_symbol=CARRIED_OCC),
                _adjusted_row(date(2026, 9, 17)),
            ],
        },
        master=master,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _reasons(report_out) == [REASON_OUT_OF_SCOPE]
    assert _entries(root) == [], "a boundary landed across a window nothing read"
    assert [entry.finding.check for entry in report_out.held] == [CHECK_SPLIT_BOUNDARY]


def test_an_uncaptured_session_does_not_deafen_the_ticker_for_the_rest_of_the_walk(
    fixture_lake: FixtureLake,
):
    """``unread_since`` resets on each readable session, whatever widened it.

    Left unreset by the new reason, one day the machine was off would hold every later boundary
    of that ticker for ever.
    """
    day_four = date(2026, 9, 17)
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
            ("SPY", day_four): [
                _row(day_four, occ_symbol=CARRIED_OCC),
                _adjusted_row(day_four),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["ex_date"] == day_four.isoformat()
    assert report_out.held == (), "an earlier gap held a boundary nothing had widened"


def test_a_session_the_lake_never_captured_is_reported_by_its_own_reason(
    fixture_lake: FixtureLake,
):
    """The render is where an operator reads how wide the lake's windows are.

    ``sweep._ledger_outcome`` counts ``skipped`` and does not read ``scale_unread``, so a pair
    refused for a window reaches the nightly report through this list and through nothing else.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
        },
    )

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    assert f"    - {REASON_NOT_SEALED}: 1" in rendered
    assert "  skipped:   1" in rendered


def test_a_skip_does_not_deafen_the_ticker_for_the_rest_of_the_walk(
    fixture_lake: FixtureLake,
):
    """``unread_since`` counts sessions since the last readable one, and resets on each.

    Left unreset, one skipped session holds every later boundary of that ticker forever. The
    live lake's SPY has an out-of-scope day and four gap days before its first data day, so
    that ticker would never land a split again.
    """
    day_four = date(2026, 9, 17)
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
            ("SPY", day_four): [
                _row(day_four, occ_symbol=CARRIED_OCC),
                _adjusted_row(day_four),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["ex_date"] == day_four.isoformat()
    assert report_out.held == (), "an earlier skip held a boundary nothing had widened"


def test_a_shrinking_root_set_is_not_a_boundary(fixture_lake: FixtureLake):
    """Contracts under an old root all expiring is an ordinary session, not an event.

    A symmetric difference would read it as a boundary and then file a payload finding an
    operator has to triage, because the lost root has no rows to read a deliverable from.
    """
    adjusted = dict(
        option_root=ADJUSTED_ROOT,
        occ_symbol="SPY1  260918C00433330",
        deliverables=ADJUSTED,
        note=ADJUSTED_NOTE,
        non_standard=True,
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **adjusted)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    assert report_out.held == () and report_out.not_adjustments == ()


def test_a_symbol_handed_between_two_instruments_starts_fresh(fixture_lake: FixtureLake):
    """Two sessions describing different securities have incomparable root sets.

    Diffing them would compute a ratio across two unrelated companies.
    """
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_to=DAY_TWO),
            _mapping(2, "SPY", valid_from=DAY_TWO),
        ]
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_adjusted_row(DAY_TWO)],
        },
        master=master,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a ratio was computed across two instruments"
    assert report_out.held == () and report_out.not_adjustments == ()


def test_two_tickers_on_one_instrument_hold_the_second_boundary(fixture_lake: FixtureLake):
    """A master mapping two symbols to one instrument is one the master calls corrupt.

    The second boundary is refused rather than passed over, because the two can disagree
    about the ratio and a run that dropped one in silence would report a ledger it does not
    describe.
    """
    master = SecurityMaster([_mapping(1, "SPY"), _mapping(1, "SPZ")])
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_TWO),
            ],
            ("SPZ", DAY_ONE): [_row(DAY_ONE, ticker="SPZ")],
            ("SPZ", DAY_TWO): [
                _row(DAY_TWO, ticker="SPZ"),
                _adjusted_row(
                    DAY_TWO, deliverables=_deliverables(400.0), note="400 SPY", ticker="SPZ"
                ),
            ],
        },
        master=master,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert len(_entries(root)) == 1
    assert len(report_out.appended) == 1
    (held,) = report_out.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION


def test_an_ambiguous_master_holds_once_and_does_not_lose_the_ticker(
    fixture_lake: FixtureLake,
):
    """``AmbiguousSymbol`` says the master is corrupt, and a corrupt master is one condition.

    One finding per ticker rather than one per ticker-day, the same reason ``by_ticker`` lets
    the instrument enter one level down.
    """
    master = SecurityMaster()
    for _ in range(2):
        master.register(
            kind=KIND_EQUITY,
            capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
            valid_from=date(2026, 9, 8),
            ticker="SPY",
        )
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
        master=master,
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (held,) = report_out.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION
    (filed,) = _findings(root, DAY_ONE)
    assert filed["exception"] == "SPY: AmbiguousSymbol"
    assert filed["instrument_ids"] == [1, 2]


def test_the_prior_side_reads_the_standard_series_of_the_boundary_session(
    fixture_lake: FixtureLake,
):
    """Not the previous session's, which can be missing the standard series entirely.

    The two agree whenever both sessions carry the standard root, which is almost always. They
    part when the standard series was absent from the session before and lists again on the
    boundary day, and then only the boundary session names what a standard contract delivers.
    Reading the previous session instead takes the ratio off the *adjusted* series, which is
    the wrong denominator and lands a wrong factor rather than a finding.
    """
    day_four = date(2026, 9, 17)
    legacy = dict(
        option_root=ADJUSTED_ROOT,
        occ_symbol="SPY1  260918C00433330",
        deliverables=ADJUSTED,
        note=ADJUSTED_NOTE,
        non_standard=True,
    )
    second = dict(
        option_root="SPY2",
        # The standard contract, re-symboled again, the same as the fixture above.
        ssid=_ssid(DEFAULT_OCC),
        occ_symbol="SPY2  260918C00216660",
        deliverables=_deliverables(300.0),
        note="300 SPY",
        non_standard=True,
    )
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_TWO): [_row(DAY_TWO), _row(DAY_TWO, **legacy)],
            # The standard series is not listed this session, so it cannot answer from here.
            ("SPY", DAY_THREE): [_row(DAY_THREE, **legacy)],
            ("SPY", day_four): [
                _row(day_four, **legacy),
                _row(day_four, occ_symbol=CARRIED_OCC),
                _row(day_four, **second),
            ],
        },
    )

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 3.0, (
        "the ratio was taken against the adjusted series rather than the standard one"
    )


# -- the OCC mapping rows the boundary writes -------------------------------------------


def _two_tickers(fixture_lake: FixtureLake) -> Path:
    """Two tickers, each with its own boundary and its own contract symbols.

    The symbols have to differ. Two tickers whose contracts share an ``occ_symbol`` would
    have the second boundary skipped as already written, which reads exactly like the first
    ticker's failure having cost the second its mapping.
    """
    return _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", occ_symbol=QQQ_OCC)],
            ("QQQ", DAY_TWO): [
                _row(DAY_TWO, ticker="QQQ", occ_symbol="QQQ   260918C00700000"),
                _adjusted_row(
                    DAY_TWO,
                    ticker="QQQ",
                    ssid=_ssid(QQQ_OCC),
                    occ_symbol="QQQ1  260918C00433330",
                ),
            ],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_TWO)],
        },
        master=_master(tickers=("QQQ", "SPY")),
    )


def _mappings(root: Path) -> list[tuple[str, date, date | None]]:
    """Every OCC mapping row the master carries, as symbol and half-open range."""
    master = SecurityMaster.read(master_path(root))
    return [
        (m.id_value, m.valid_from, m.valid_to) for m in master.mappings if m.id_type == ID_TYPE_OCC
    ]


def test_a_landed_split_also_threads_its_contracts_through_the_master(
    fixture_lake: FixtureLake,
):
    """The two outputs share a trigger and nothing else.

    The ledger records the ratio and the master records which contract is which, so one run
    writes both and neither is derivable from the other.
    """
    root = _two_sessions(fixture_lake)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 1.5
    (remap,) = report_out.mapped
    assert (remap.old_symbol, remap.new_symbol) == (DEFAULT_OCC, "SPY1  260918C00433330")
    assert _mappings(root) == [
        (DEFAULT_OCC, DAY_ONE, DAY_TWO),
        ("SPY1  260918C00433330", DAY_TWO, None),
    ]
    master = SecurityMaster.read(master_path(root))
    assert master.resolve(DEFAULT_OCC, DAY_ONE, id_type=ID_TYPE_OCC) == remap.instrument_id
    assert (
        master.resolve("SPY1  260918C00433330", DAY_TWO, id_type=ID_TYPE_OCC) == remap.instrument_id
    )


def test_a_rename_writes_the_mapping_and_appends_no_ledger_entry(fixture_lake: FixtureLake):
    """``remap``'s docstring says a rename and an OCC re-symboling are one operation.

    The deliverable is what separates a rename from a split for the *ledger*, and it separates
    nothing for the master, which records no ratio at all. So this is the case where the two
    writes come apart completely.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(DAY_TWO, occ_symbol=CARRIED_OCC),
                _row(
                    DAY_TWO,
                    ssid=_ssid(DEFAULT_OCC),
                    occ_symbol="SPY1  260918C00650000",
                    option_root=ADJUSTED_ROOT,
                    non_standard=True,
                ),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "a rename landed a no-op factor"
    assert _not_splits(report_out) == [REASON_DELIVERABLE_UNCHANGED]
    assert [remap.new_symbol for remap in report_out.mapped] == ["SPY1  260918C00650000"]


def test_an_adjustment_no_float_describes_is_held_and_still_mapped(
    fixture_lake: FixtureLake,
):
    """#136 says such an event is surfaced rather than faked, and identity is not the ratio.

    The contracts were re-symboled whether or not the ledger can carry what they now deliver,
    and both consumers of these rows break on a re-symboling the master does not record.
    """
    root = _two_sessions(fixture_lake, deliverables=_with_cash(150.0, 25.0), note="150 SPY")

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_DELIVERABLE
    assert len(report_out.mapped) == 1


def test_a_ratio_the_gate_refuses_is_still_mapped(fixture_lake: FixtureLake):
    """The gate judges the vendor's two spellings of the deliverable against each other.

    A disagreement makes the *ratio* untrustworthy. It says nothing about which contract is
    which, and the pairing does not read either field.
    """
    root = _two_sessions(fixture_lake, note="175 SPY")

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_CONSISTENCY
    assert len(report_out.mapped) == 1


def test_a_boundary_the_walk_cannot_date_writes_no_mapping(fixture_lake: FixtureLake):
    """``effective`` is the field a guessed boundary would get wrong.

    A mapping dated to the wrong session threads a contract's history across a day it did not
    change on, and every read through the master then answers from the wrong side.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE),
            ],
        },
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (held,) = report_out.held
    assert held.finding.check == CHECK_SPLIT_BOUNDARY
    assert report_out.mapped == ()
    assert _mappings(root) == []


def test_a_second_night_rewrites_neither_the_ledger_nor_the_master(
    fixture_lake: FixtureLake,
):
    """Sealed chains never change, so a second night re-derives the same boundary.

    The mapping write cannot ride the ledger's guard, because a run that landed the entry and
    failed the master write would never come back to it.
    """
    root = _two_sessions(fixture_lake)
    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)
    before = master_path(root).read_bytes()
    entries_before = _entries(root)

    report_out = detect_splits(lake_root=root, clock=ManualClock(SECOND_NIGHT), calendar=CALENDAR)

    assert _entries(root) == entries_before
    assert report_out.unchanged == 1
    assert report_out.mapped == ()
    assert master_path(root).read_bytes() == before


def test_a_refused_mapping_files_a_finding_and_the_split_still_lands(
    fixture_lake: FixtureLake,
):
    """A refusal here costs this boundary its mapping and not the run.

    The adjusted contract carries an ``ssid`` the walk has never read, which is what a vendor
    dropping the identifier through a re-symboling looks like. The ledger entry is a separate
    record and lands anyway.
    """
    root = _two_sessions(fixture_lake, ssid=999_000_001)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (entry,) = _entries(root)
    assert entry["split_ratio"] == 1.5
    (held,) = report_out.held
    assert held.finding.check == CHECK_OCC_MAPPING
    assert "MappingRefused" in held.finding.exception
    assert report_out.mapped == ()
    (filed,) = _findings(root, DAY_TWO)
    assert filed["check"] == CHECK_OCC_MAPPING


def test_a_newly_listed_standard_series_maps_nothing(fixture_lake: FixtureLake):
    """An adjustment turns standard contracts into non-standard ones.

    A gained root the vendor still calls standard has no adjustment behind it, so it never
    reaches the mapping write at all.
    """
    root = _two_sessions(fixture_lake, non_standard=False, deliverables=STANDARD, note=NOTE)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _not_splits(report_out) == [REASON_STANDARD_SERIES]
    assert report_out.mapped == () and _mappings(root) == []


def test_the_render_names_the_mapping_it_wrote(fixture_lake: FixtureLake):
    """A run that rewrote the reference table every consumer resolves through says so."""
    root = _two_sessions(fixture_lake)

    rendered = detect_splits(
        lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR
    ).render()

    assert "  mapped:    1" in rendered
    assert f"{DEFAULT_OCC!r} [{DAY_ONE.isoformat()} -> {DAY_TWO.isoformat()})" in rendered
    assert "becomes 'SPY1  260918C00433330'" in rendered


def test_the_master_the_write_touches_stays_manifested(fixture_lake: FixtureLake):
    """A rewrite that records no entry fails the integrity scrub's forward pass."""
    root = _two_sessions(fixture_lake)
    record_partition(
        root,
        f"{REFERENCE_DIR}/{MASTER_FILENAME}",
        source="reference",
        rows=2,
        fetched_at=RECORDED_AT.isoformat(),
    )
    assert scrub(root).ok

    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert scrub(root).ok, "the master was rewritten without recording its new sha"


def test_a_master_torn_during_the_walk_ends_the_run_rather_than_filing_per_boundary(
    fixture_lake: FixtureLake, monkeypatch
):
    """The walk's opening read covers a master torn before it. This is one torn during it.

    Held per boundary instead, every remaining ticker files an ``occ_mapping`` finding, the
    run reports success, splits keep landing with no mapping row behind any of them, and the
    one condition a single command fixes is buried as noise. ``main`` has a line for it.
    """
    root = _two_tickers(fixture_lake)
    import lake.actions

    # ``lake.splits`` binds ``read_master`` at module scope for its own opening read, and
    # ``occ_mapping`` imports it inside the write. So this tears the master for the write
    # alone, which is the mid-walk case: the run has already read a whole master once.
    def tear(lake_root):
        raise MasterUnreadable(master_path(lake_root))

    monkeypatch.setattr(lake.actions, "read_master", tear)

    with pytest.raises(MasterUnreadable):
        detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert _entries(root) == [], "the run reached the second ticker instead of ending"


def test_a_row_count_regression_files_a_finding_and_the_walk_goes_on(
    fixture_lake: FixtureLake, monkeypatch
):
    """A bare ``Exception`` outside ``SecurityMasterError``, which a narrow catch would miss.

    This write can never cause one, because registering and remapping only grow the row list.
    A restore from backup makes it reachable, and uncaught it would end the run and cost every
    ticker the walk had not yet reached.
    """
    root = _two_tickers(fixture_lake)
    import lake.manifest

    real = lake.manifest.record_partition

    def refuse_once(lake_root, partition, **kwargs):
        if partition.startswith(REFERENCE_DIR) and not getattr(refuse_once, "fired", False):
            refuse_once.fired = True
            raise lake.manifest.RowCountRegression(partition, 99, 1)
        return real(lake_root, partition, **kwargs)

    monkeypatch.setattr(lake.manifest, "record_partition", refuse_once)

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    (held,) = report_out.held
    assert held.finding.check == CHECK_OCC_MAPPING and held.finding.symbol == "QQQ"
    assert "were written and the manifest entry was not" in held.finding.exception, (
        "the finding says the mapping failed when the rows are on disk"
    )
    assert [remap.ticker for remap in report_out.mapped] == ["SPY"], (
        "the second ticker lost its mapping to the first ticker's refusal"
    )
    assert len(_entries(root)) == 2


def test_a_symbol_handed_between_two_instruments_maps_nothing(fixture_lake: FixtureLake):
    """The symbol history resets with the root history, and for the same reason.

    Without it the incoming instrument keeps the retired security's contracts. The third
    session is what makes that reachable: the changeover session itself returns early, so a
    test that stops there passes whether or not the reset ran. Here instrument 2 gains a root
    a session later, carrying a contract instrument 1 held and instrument 2 never did. With
    the reset that contract is one the walk cannot place and the boundary is refused. Without
    it, one company's old symbol is paired to another company's new one and an option
    instrument is registered under it, which is the orphaning the master exists to prevent.
    """
    handed = "SPY   260918C00800000"
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO, occ_symbol=handed)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=handed),
                _adjusted_row(DAY_THREE),
            ],
        },
        master=SecurityMaster(
            [
                _mapping(1, "SPY", valid_from=DAY_ONE, valid_to=DAY_TWO),
                _mapping(2, "SPY", valid_from=DAY_TWO),
            ]
        ),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert report_out.mapped == (), "the incoming instrument inherited the outgoing one's history"
    assert _mappings(root) == []
    (held,) = report_out.held
    assert held.finding.check == CHECK_OCC_MAPPING
    assert "cannot place 1 of them" in held.finding.exception


def test_a_boundary_the_ledger_refuses_for_a_duplicate_instrument_is_still_mapped(
    fixture_lake: FixtureLake,
):
    """The least obvious of the seven outcomes that reach the write, so it is executed here.

    Two tickers resolving to one instrument is a reference-data fault, and it is a fault about
    which instrument a *ticker* names. The mapping rows name contracts by their own symbols
    under their own fresh instrument ids, so holding them would lose a real identity change
    over a defect in a different row of the same table.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", occ_symbol=QQQ_OCC)],
            ("QQQ", DAY_TWO): [
                _row(DAY_TWO, ticker="QQQ", occ_symbol="QQQ   260918C00700000"),
                _adjusted_row(
                    DAY_TWO,
                    ticker="QQQ",
                    ssid=_ssid(QQQ_OCC),
                    occ_symbol="QQQ1  260918C00433330",
                ),
            ],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_TWO)],
        },
        # Both tickers on one instrument, which is what the ledger's key guard refuses.
        master=SecurityMaster([_mapping(1, "QQQ"), _mapping(1, "SPY")]),
    )

    report_out = detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)

    assert [held.finding.check for held in report_out.held] == [CHECK_INSTRUMENT_RESOLUTION]
    assert len(_entries(root)) == 1, "the second boundary's ledger entry was refused"
    assert sorted(remap.ticker for remap in report_out.mapped) == ["QQQ", "SPY"], (
        "a fault about which instrument a ticker names lost both contracts their mapping"
    )


def test_a_run_that_landed_the_entry_and_lost_the_master_writes_the_mapping_next_night(
    fixture_lake: FixtureLake,
):
    """The mapping write cannot ride the ledger's guard, and this is the case that proves it.

    ``same_but_for_recorded_at`` suppresses the second append, so the ledger reports the split
    unchanged. The mapping is written on its own terms, which is what makes the two writes
    independently recoverable rather than one silently depending on the other.
    """
    root = _two_sessions(fixture_lake)
    detect_splits(lake_root=root, clock=ManualClock(FIRST_NIGHT), calendar=CALENDAR)
    entries = _entries(root)
    # The ledger keeps its entry and the master loses its rows, which is what a first run that
    # appended and then failed its master write leaves behind.
    _master().write(master_path(root))
    assert _mappings(root) == []

    report_out = detect_splits(lake_root=root, clock=ManualClock(SECOND_NIGHT), calendar=CALENDAR)

    assert _entries(root) == entries and report_out.unchanged == 1
    assert len(report_out.mapped) == 1
    assert _mappings(root) == [
        (DEFAULT_OCC, DAY_ONE, DAY_TWO),
        ("SPY1  260918C00433330", DAY_TWO, None),
    ]
