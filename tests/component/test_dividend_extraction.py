"""The dividend extraction: reading a corporate action out of rows the lake already sealed.

Nothing here fetches. Every test builds a lake on disk whose quotes partitions carry the
vendor's ``fundamental`` block, runs the extraction against it with a manual clock, and reads
the ledger back off the file the way a reader would.

Marketlake #284 names nine behaviours and they are the specification. Three more sit beside
them, each pinning something the issue argues at length and none of the nine reaches.

1. The amount that lands is the per-event ``div_pay_amount`` and never ``div_amount``, the
   annualized trailing figure. That is the trap the issue exists to avoid, and the two differ
   by exactly the payout frequency, so reading the wrong one inflates every total-return
   factor fourfold for a quarterly payer.
2. The date the entry records and the date the instrument resolved at are one date. Resolving
   at one and recording another attributes the action to whoever held the ticker on a
   different day, silently.
3. One date written two ways is not a transition. Schwab supplies ``div_ex_date`` as a
   timestamp spelling of a date, and the live lake has already produced more than one
   spelling of one instant.

The detection logic has no live example to run against. Both tickers in the lake carry
exactly one distinct ``div_ex_date`` across every data row, so the transition these tests
drive is a fixture rather than something captured.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake import actions, report
from lake.actions import (
    CHECK_DIVIDEND_CONSISTENCY,
    CHECK_DIVIDEND_PAYLOAD,
    CHECK_INSTRUMENT_RESOLUTION,
    DIVIDEND_CONSISTENCY_TOLERANCE,
    PROVENANCE_OBSERVED,
    PROVENANCE_VENDOR_REPORTED,
    REASON_NO_SPOT_CLOSE,
    REASON_PARTIAL_READ,
    REASON_PARTITION_ABSENT,
    REASON_QUARANTINED,
    TYPE_DIVIDEND,
    check_dividend_consistency,
    extract_dividends,
)
from lake.paths import QUOTES, LakePaths
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.security_master import ID_TYPE_TICKER, KIND_EQUITY, SecurityMaster, master_path
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake

# The two sessions the fixture lake holds data for, and the evening the extraction ran. The
# second night is a day later, which is what test 3 needs to mean two runs.
DAY_ONE = date(2026, 9, 14)
DAY_TWO = date(2026, 9, 15)
# A third session, for the walk that has to step over the middle one.
DAY_THREE = date(2026, 9, 16)
FIRST_NIGHT = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)  # 20:00 ET on 2026-09-14
# 20:01 ET on 2026-09-15, a minute later in the day rather than the same minute. A withheld
# file is named by its ET time of day and filed under the ticker-day its rows belong to, so
# two runs at the same time of day holding the same finding for one ticker-day collide on one
# name. A real clock separates them by microseconds and a manual one does not.
SECOND_NIGHT = datetime(2026, 9, 16, 0, 1, tzinfo=UTC)

# SPY's dividend as the live lake reports it. The annualized figure is four times the
# per-event amount less 0.00002, which is the vendor's own rounding and what the gate's
# tolerance was measured from.
PAY_AMOUNT = 1.90352
ANNUALIZED = 7.61406
FREQ = 4
EX_DATE = "2026-06-18T00:00:00Z"
PAY_DATE = "2026-07-31T00:00:00Z"
DECLARED = "2026-01-02T00:00:00Z"

# The next quarter's dividend, which is the transition the detection logic needs and the live
# lake does not carry.
NEXT_EX_DATE = "2026-09-17T00:00:00Z"
NEXT_PAY_AMOUNT = 1.95
NEXT_ANNUALIZED = 7.8

# When the schema-version ledger recorded version 1. Any instant does, since the loader reads
# the recorded shape and never the time it was recorded.
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# The quotes columns the loader needs plus the six a dividend is read out of. This is a
# fixture schema rather than the pinned capture schema, the way ``tests/support/lake.py``'s
# is, carrying what these tests read and no more.
QUOTES_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("ticker", pa.string()),
        ("last", pa.float64()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("close_tag", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
        ("div_pay_amount", pa.float64()),
        ("div_ex_date", pa.string()),
        ("div_amount", pa.float64()),
        ("div_freq", pa.int64()),
        ("div_pay_date", pa.string()),
        ("declaration_date", pa.string()),
    ]
)


def _row(
    day: date,
    *,
    close_tag: str | None = "spot_close",
    row_kind: str = "data",
    ex_date: str | None = EX_DATE,
    pay_amount: float | None = PAY_AMOUNT,
    amount: float | None = ANNUALIZED,
    freq: int | None = FREQ,
    pay_date: str | None = PAY_DATE,
    declared: str | None = DECLARED,
    ticker: str = "SPY",
) -> dict:
    """One quotes row at the session's equity close, carrying the fundamental block."""
    snap = f"{day.isoformat()}T20:00:00+00:00"
    return {
        "snap_ts": snap,
        "fetch_ts": f"{day.isoformat()}T20:00:00.300+00:00",
        "ticker": ticker,
        "last": 650.0,
        "row_kind": row_kind,
        "error_class": None if row_kind == "data" else "vendor_auth_error",
        "close_tag": close_tag,
        "schema_version": 1,
        "extra": None,
        "div_pay_amount": pay_amount,
        "div_ex_date": ex_date,
        "div_amount": amount,
        "div_freq": freq,
        "div_pay_date": pay_date,
        "declaration_date": declared,
    }


def _gap_day_row(day: date, ticker: str = "SPY") -> dict:
    """One gap row: a minute the cycle attempted and missed, every vendor column null.

    A day of these is what the lake's 2026-09-08 through 2026-09-11 actually hold, from a real
    auth outage, and it is what makes ``load_quotes`` raise ``NoSpotClose``.
    """
    return _row(
        day,
        row_kind="gap",
        ex_date=None,
        pay_amount=None,
        amount=None,
        freq=None,
        pay_date=None,
        declared=None,
        ticker=ticker,
    )


def _table(rows: list[dict]) -> pa.Table:
    columns = {name: [row.get(name) for row in rows] for name in QUOTES_SCHEMA.names}
    return pa.table(columns, schema=QUOTES_SCHEMA)


def _ledger_table() -> pa.Table:
    """The schema-version ledger recording version 1 at the shape the running code writes."""
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


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
    chains: tuple[str, date] | None = None,
    quarantined: tuple[str, date] | Sequence[tuple[str, date]] | None = None,
    ledger: bool = True,
) -> Path:
    """A lake holding one quotes partition per session, plus the ledger and the master.

    ``chains`` seals a chains partition beside them, for the test that asks which surface the
    walk enumerates. ``quarantined`` writes the verdict ledger's entry for one quotes
    ticker-day, which is what the validation battery will append. ``ledger=False`` leaves out
    the schema-version ledger, which is what makes the overflow projection refuse every read.
    """
    for (ticker, day), rows in sessions.items():
        fixture_lake.with_quotes(ticker, day, _table(rows))
    if chains is not None:
        fixture_lake.with_partition("chains", chains[0], chains[1], _table([_row(chains[1])]))
    if ledger:
        fixture_lake.with_reference("schema_versions", _ledger_table())
    if quarantined is not None:
        withheld = [quarantined] if isinstance(quarantined[0], str) else list(quarantined)
        for ticker, day in withheld:
            partition = (
                LakePaths(fixture_lake.root)
                .partition_path(QUOTES, ticker, day)
                .relative_to(fixture_lake.root)
                .as_posix()
            )
            fixture_lake.with_quarantine(
                {"partition": partition, "verdict": "suspect", "check": "delayed_feed"}
            )
    root = fixture_lake.build()
    (master if master is not None else _master()).write(master_path(root))
    return root


def _entries(root: Path) -> list[dict]:
    return actions.read(root)


def _clear_quarantine(root: Path, ticker: str, day: date) -> None:
    """Append the sign-off row that clears one partition's verdict.

    Resolution reads the last entry on a partition in file order, so a clearing row supersedes
    the withholding one rather than replacing it.
    """
    partition = LakePaths(root).partition_path(QUOTES, ticker, day).relative_to(root).as_posix()
    line = json.dumps(
        {"partition": partition, "verdict": "clean", "check": "delayed_feed"}, sort_keys=True
    )
    path = root / "quarantine.jsonl"
    path.write_text(path.read_text() + line + "\n")


def _findings(root: Path, day: date) -> list[dict]:
    """Every withheld finding filed for one ticker-day, read back off the files."""
    directory = report.withheld_dir(root, day)
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]


# -- 1 and 2. the two provenances -------------------------------------------------------


def test_a_value_changing_between_two_observations_emits_one_observed_entry(
    fixture_lake: FixtureLake,
):
    """#284 test 1.

    Two sessions, the second reporting a new ex-date. The lake saw the change, so the entry
    for the new value is ``observed``. The first session's value was already there when the
    lake started looking, so it is ``vendor_reported``.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(
                    DAY_TWO,
                    ex_date=NEXT_EX_DATE,
                    pay_amount=NEXT_PAY_AMOUNT,
                    amount=NEXT_ANNUALIZED,
                )
            ],
        },
    )

    report_out = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    first, second = _entries(root)
    assert first["ex_date"] == "2026-06-18"
    assert first["provenance"] == PROVENANCE_VENDOR_REPORTED
    assert second["ex_date"] == "2026-09-17"
    assert second["provenance"] == PROVENANCE_OBSERVED, (
        "a change the lake watched happen is observed, not vendor_reported"
    )
    assert len(report_out.appended) == 2 and report_out.held == ()


def test_a_value_present_on_the_first_observation_emits_one_vendor_reported_entry(
    fixture_lake: FixtureLake,
):
    """#284 test 2.

    SPY's ex-date sits nearly three months before the lake's first data row, so it is Schwab's
    trailing report rather than something this lake observed. It still lands, because a
    total-return factor spanning June needs June's dividend, and ``provenance`` is what keeps
    that honest.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})

    extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["provenance"] == PROVENANCE_VENDOR_REPORTED
    assert entry["type"] == TYPE_DIVIDEND


# -- 3. idempotence ---------------------------------------------------------------------


def test_an_unchanged_value_across_two_nights_emits_nothing_on_the_second(
    fixture_lake: FixtureLake,
):
    """#284 test 3.

    The comparison is every field but ``recorded_at``. Comparing whole entries would append
    the same dividend every night forever, since the clock's answer moves while nothing else
    does, and the ledger every adjusted price reads would grow without learning anything.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
    )

    first = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))
    second = extract_dividends(lake_root=root, clock=ManualClock(SECOND_NIGHT))

    assert len(first.appended) == 1
    assert second.appended == (), "the second night re-appended a dividend the ledger held"
    assert second.unchanged == 1
    assert len(_entries(root)) == 1


def test_a_second_session_repeating_the_value_is_not_a_second_entry(fixture_lake: FixtureLake):
    """Within one run, too. A value is emitted at the observation that first carried it."""
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["observed_on"] == DAY_ONE.isoformat()
    assert len(result.appended) == 1


# -- 4. the two ways a ticker-day carries no observation ----------------------------------


def test_a_ticker_day_with_no_equity_close_emits_nothing(fixture_lake: FixtureLake):
    """#284 test 4, first half.

    A gap day has nothing to read and reads as no observation rather than as an error. Four
    of the lake's days are exactly this, from an auth outage that took the whole session.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_gap_day_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
        },
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["observed_on"] == DAY_TWO.isoformat(), "the gap day was read as an observation"
    assert entry["provenance"] == PROVENANCE_VENDOR_REPORTED, (
        "a gap day is no observation, so the day after it is still the first one"
    )
    assert result.ticker_days == 2 and result.held == ()
    assert [(skip.ticker, skip.day, skip.reason) for skip in result.skipped] == [
        ("SPY", DAY_ONE, REASON_NO_SPOT_CLOSE)
    ], "the gap day was skipped in silence"


def test_a_data_row_with_null_dividend_fields_emits_nothing(fixture_lake: FixtureLake):
    """#284 test 4, second half.

    A non-paying instrument reports nothing, and an entry of nulls is not an event.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [
                _row(
                    DAY_ONE,
                    ex_date=None,
                    pay_amount=None,
                    amount=None,
                    freq=None,
                    pay_date=None,
                    declared=None,
                )
            ]
        },
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    assert result.appended == () and result.held == ()


# -- 5 and 6. the gate ------------------------------------------------------------------


def test_a_payload_failing_self_consistency_is_held_and_filed(fixture_lake: FixtureLake):
    """#284 test 5.

    A drifted fundamental is one figure moving while the other does not. Here the annualized
    figure is the per-event amount itself, which is what a reader taking ``div_amount`` for
    the payout would produce, and four times what the payer actually pays.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE, amount=PAY_AMOUNT)]})

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == [], "a dividend the gate refused reached the ledger"
    (held,) = result.held
    assert held.finding.check == CHECK_DIVIDEND_CONSISTENCY
    (finding,) = _findings(root, DAY_ONE)
    assert finding["check"] == CHECK_DIVIDEND_CONSISTENCY
    assert finding["computed"] == FREQ * PAY_AMOUNT
    assert finding["against"] == PAY_AMOUNT
    assert finding["symbol"] == "SPY" and finding["event"] == TYPE_DIVIDEND
    # The id is what a human resolving the finding acts on, and it was known here.
    assert finding["instrument_id"] == 1
    assert held.filed_at.exists()


def test_a_payload_inside_the_tolerance_lands(fixture_lake: FixtureLake):
    """#284 test 6.

    This is the live payload. SPY reports 7.61406 against four times 1.90352, which is
    7.61408, so the vendor's own arithmetic is off by 0.00002. A gate that refused it would
    hold every SPY dividend the lake will ever see.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert result.held == ()
    (entry,) = _entries(root)
    assert entry["cash_amount"] == PAY_AMOUNT
    (landed,) = result.appended
    assert landed.symbol == "SPY" and landed.entry == entry


def test_the_walk_hands_the_gate_the_vendor_frequency_it_read(fixture_lake: FixtureLake):
    """Both live tickers are quarterly, so nothing else pins that the walk reads `div_freq`.

    A monthly payer is self-consistent at twelve and nowhere near it at four, so an extraction
    that assumed a quarterly payer would hold every dividend a monthly one ever reports while
    the gate itself stayed correct.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE, freq=12, pay_amount=0.5, amount=6.0)]},
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert result.held == (), "a monthly payer was judged against a quarterly product"
    (entry,) = _entries(root)
    assert entry["cash_amount"] == 0.5


def test_a_payload_missing_a_figure_the_check_needs_is_held_rather_than_landed(
    fixture_lake: FixtureLake,
):
    """A check with nothing to compare has not agreed, and a factor lands only after it does.

    Landing on a payload the gate could not judge would make the fail-closed rule mean
    "unless the vendor left the figure out", which is the shape a stale fundamental takes.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE, amount=None)]})

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    (held,) = result.held
    assert held.finding.check == CHECK_DIVIDEND_CONSISTENCY
    (finding,) = _findings(root, DAY_ONE)
    assert "against" not in finding, "a figure the payload did not carry was filed as a number"
    assert finding["computed"] == FREQ * PAY_AMOUNT


# -- 7. both ways the resolution fails closed --------------------------------------------


def test_an_unresolvable_symbol_holds_the_action_and_files(fixture_lake: FixtureLake):
    """#284 test 7, first half.

    A master that starts after the session means the master and the capture spans disagree
    about the ticker. An action held out can be landed later. One landed under a guessed
    instrument corrupts every factor that instrument's prices feed.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)]},
        master=_master(valid_from=date(2026, 9, 16)),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    (held,) = result.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION
    (finding,) = _findings(root, DAY_ONE)
    assert finding["exception"] == "SPY: UnresolvedSymbol", (
        "the class is what files, and the message, which can name a path, is what drops"
    )
    assert "instrument_id" not in finding, "an id was filed for the finding that has none"


def test_an_ambiguous_symbol_holds_the_action_and_files_the_several_instruments(
    fixture_lake: FixtureLake,
):
    """#284 test 7, second half.

    ``AmbiguousSymbol`` is a corrupt master: one symbol mapping to several instruments on one
    date. The plural is the finding rather than a detail to fold into a singular field.
    """
    master = _master()
    master.register(
        kind=KIND_EQUITY,
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
        valid_from=date(2026, 9, 8),
        ticker="SPY",
    )
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]}, master=master)

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    (held,) = result.held
    assert held.finding.check == CHECK_INSTRUMENT_RESOLUTION
    (finding,) = _findings(root, DAY_ONE)
    assert finding["exception"] == "SPY: AmbiguousSymbol"
    assert finding["instrument_ids"] == [1, 2]


def test_a_held_action_never_reaches_the_ledger_while_another_ticker_lands(
    fixture_lake: FixtureLake,
):
    """One ticker's refusal is one ticker's, and the run goes on.

    A gate that stopped the walk would cost every other ticker's dividend to one bad payload.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, amount=PAY_AMOUNT)],
            ("QQQ", DAY_ONE): [
                _row(DAY_ONE, ticker="QQQ", pay_amount=0.81349, amount=3.25396, freq=4)
            ],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["cash_amount"] == 0.81349
    assert len(result.held) == 1 and result.held[0].finding.symbol == "SPY"


# -- the three the issue argues and the nine do not reach ---------------------------------


# -- the gate's two stated properties, each held from both sides ------------------------


def test_the_tolerance_admits_what_the_vendor_rounds_and_no_more():
    """The constant is pinned from both sides, not just named in prose.

    "Three parts in a million and no more" is a claim about a number, and a test that only
    exercises the live payload and a 300% disagreement leaves the whole band between them
    free. A tolerance of five percent would admit every stale fundamental that drifted by
    less than a twentieth and pass such a suite.
    """
    # Absolute figures rather than ones derived from the constant. A test written as
    # "half the tolerance" and "twice the tolerance" moves with the number it is meant to
    # hold, so every value of it passes and nothing is pinned at all.
    assert _at_relative_error(2.63e-6).agrees is True, (
        "the gate refused the drift SPY's own payload carries"
    )
    assert _at_relative_error(5e-6).agrees is False, (
        "the gate admitted a drift larger than any the vendor's rounding explains"
    )
    assert DIVIDEND_CONSISTENCY_TOLERANCE == 3e-6


def test_the_tolerance_is_relative_rather_than_absolute():
    """A penny payer and a large one carry the same absolute rounding and different meaning.

    The 0.00002 SPY reports is 2.6 parts in a million of its annualized figure and a fifth of
    a percent of a penny dividend. An absolute tolerance would read those two as the same
    disagreement, which is why the comparison divides.
    """
    # A penny payer off by the same absolute 0.00002 an absolute tolerance would admit.
    penny = check_dividend_consistency(div_freq=4, div_pay_amount=0.0025, div_amount=0.00998)

    assert penny.agrees is False, (
        "an absolute tolerance was applied, so a penny payer's real drift passed"
    )
    assert abs(penny.computed - penny.against) < 1e-4, (
        "the fixture no longer carries the small absolute difference the test is about"
    )


def test_the_gate_reads_the_vendor_frequency_rather_than_assuming_a_quarterly_payer():
    """Every live payer in the lake is quarterly, so nothing else pins that `div_freq` is read.

    A gate that hardcoded four would judge a monthly payer against a quarterly product and
    hold every dividend it ever reports.
    """
    monthly = check_dividend_consistency(div_freq=12, div_pay_amount=0.5, div_amount=6.0)
    as_quarterly = check_dividend_consistency(div_freq=4, div_pay_amount=0.5, div_amount=6.0)

    assert monthly.agrees is True
    assert as_quarterly.agrees is False
    assert monthly.computed == 6.0


def test_a_zero_annualized_figure_agrees_only_when_nothing_is_paid():
    """A figure of zero has no relative scale, so the branch decides it rather than a division.

    A vendor zeroing the annualized figure while a real per-event amount survives is the
    stale-fundamental shape the gate exists to catch, and reading zero as agreement would
    land it ungated.
    """
    non_payer = check_dividend_consistency(div_freq=4, div_pay_amount=0.0, div_amount=0.0)
    stale = check_dividend_consistency(div_freq=4, div_pay_amount=PAY_AMOUNT, div_amount=0.0)

    assert non_payer.agrees is True
    assert stale.agrees is False, "a stale per-event amount passed beside a zeroed annual one"


def test_a_frequency_of_zero_does_not_let_the_amount_out_of_the_comparison():
    """Multiplying by zero drops `div_pay_amount` out of the equation entirely.

    A vendor reporting a stale amount beside a zeroed frequency and a zeroed annual figure
    would otherwise be judged on arithmetic that no longer mentions the amount, and the gate
    would agree to anything.
    """
    zeroed = check_dividend_consistency(div_freq=0, div_pay_amount=PAY_AMOUNT, div_amount=0.0)

    assert zeroed.agrees is False


def _at_relative_error(relative: float):
    """A quarterly payload whose annualized figure sits a chosen relative distance off."""
    annualized = FREQ * PAY_AMOUNT
    return check_dividend_consistency(
        div_freq=FREQ,
        div_pay_amount=PAY_AMOUNT,
        div_amount=annualized * (1 + relative),
    )


def test_the_amount_that_lands_is_the_per_event_figure_and_never_the_annualized_one():
    """The trap the whole deliverable exists to avoid, asked of the gate directly.

    ``div_amount`` is four times ``div_pay_amount`` for a quarterly payer, so an extraction
    reading it would inflate every total-return factor fourfold. The gate is what catches a
    swap, because the two figures compared against each other is exactly the check.
    """
    swapped = actions.check_dividend_consistency(
        div_freq=FREQ, div_pay_amount=ANNUALIZED, div_amount=ANNUALIZED
    )
    assert swapped.agrees is False, "the annualized figure read as the per-event one passed"

    live = actions.check_dividend_consistency(
        div_freq=FREQ, div_pay_amount=PAY_AMOUNT, div_amount=ANNUALIZED
    )
    assert live.agrees is True


def test_the_entry_records_the_date_the_instrument_resolved_at(fixture_lake: FixtureLake):
    """The observation date is one date, in ``observed_on`` and in the resolver both.

    The master here opens the day the lake first captured SPY, so resolving at the ex-date,
    which is June, would answer nothing. The entry still records June as ``ex_date``, because
    that is when the event happened.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})

    extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["observed_on"] == DAY_ONE.isoformat()
    assert entry["ex_date"] == "2026-06-18"
    assert entry["instrument_id"] == 1
    # The other two vendor dates, read back off the entry. Each answers a different question,
    # so one going null or the two swapping is a silent loss in the ledger every adjusted
    # price is computed through.
    assert entry["pay_date"] == "2026-07-31", "the pay date did not survive the write"
    assert entry["declared_date"] == "2026-01-02", "the declaration date did not survive"


def test_one_date_written_two_ways_is_not_a_transition(fixture_lake: FixtureLake):
    """The live lake has already produced more than one spelling of one instant.

    A walk comparing the vendor's raw text would read the respelling as a new event and append
    a second entry for a dividend that never happened.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, ex_date="2026-06-18T00:00:00Z")],
            ("SPY", DAY_TWO): [_row(DAY_TWO, ex_date="2026-06-18")],
        },
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert len(_entries(root)) == 1, "one date written two ways was read as two events"
    assert len(result.appended) == 1


def test_one_date_written_two_ways_is_not_a_second_finding_either(fixture_lake: FixtureLake):
    """The normalization holds the held path as well as the landing path.

    A dividend that lands is caught by the key it was already emitted under, so nothing there
    notices a raw comparison. A dividend held out reaches no key at all, so the respelling
    reads as a second event and files the same condition twice in one run. The repetition
    under ``reports/`` is supposed to mean another night, not another spelling.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, ex_date="2026-06-18T00:00:00Z")],
            ("SPY", DAY_TWO): [_row(DAY_TWO, ex_date="2026-06-18")],
        },
        master=_master(valid_from=date(2026, 9, 16)),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert len(result.held) == 1, "one date written two ways was held as two findings"
    assert result.held[0].finding.check == CHECK_INSTRUMENT_RESOLUTION


def test_the_ticker_days_come_from_the_manifest_and_name_the_quotes_surface(
    fixture_lake: FixtureLake,
):
    """A chains partition is not a session this walk reads, and is not counted as one.

    The manifest is the lake's own record of what it holds, which is what keeps the walk from
    asking for a session the lake never captured. It bounds what a refusal can be and does not
    remove refusals, which marketlake #352 corrected: the manifest records what was sealed
    rather than what is still on disk, and four of the loader's refusals reach this walk.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)]},
        chains=("SPY", DAY_TWO),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert result.ticker_days == 1
    assert len(_entries(root)) == 1


def test_the_append_refreshes_the_manifest_entry(fixture_lake: FixtureLake):
    """The ledger is manifested like any lake file.

    The reverse scrub names ``manifest.jsonl``, ``journal/`` and ``reports/`` alone, so a write
    that skipped its manifest entry would be reported as an orphan.
    """
    from lake.manifest import latest_entries

    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})

    extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    entry = latest_entries(root)[actions.ACTIONS_PARTITION]
    assert entry["source"] == actions.SWEEP_SOURCE
    assert entry["rows"] == 1


# -- what a run that holds more than one thing does --------------------------------------


def test_two_findings_in_one_run_are_two_files_under_one_clock(fixture_lake: FixtureLake):
    """The sequence in the file name is what tells them apart.

    One run files under one injected clock and one pid, and the clock does not advance
    between findings, so without a per-finding sequence the second would land on the first's
    name. A human reading the directory would then see one of two.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE, amount=PAY_AMOUNT)],
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", amount=PAY_AMOUNT)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert len(result.held) == 2, "a run that held two things reported fewer"
    paths = {held.filed_at for held in result.held}
    assert len(paths) == 2, "two findings in one run collided on one file name"
    assert all(path.exists() for path in paths)
    assert len(_findings(root, DAY_ONE)) == 2


def test_a_finding_that_cannot_be_filed_leaves_the_walk_running_and_says_so(
    fixture_lake: FixtureLake, tmp_path: Path, capsys, monkeypatch
):
    """``write_withheld`` raises and says the containment belongs to this caller.

    A raise out of the filing costs the rest of the walk, and the rest of the walk is other
    tickers' dividends. What the failure costs instead is an exit code, because a finding held
    and never written down reads exactly like a run that found nothing.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", amount=PAY_AMOUNT)],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )

    def refuse(*args, **kwargs):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr("lake.actions.write_withheld", refuse)
    config = write_config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 1, "an unwritable finding was reported as a clean run"
    printed = capsys.readouterr()
    assert "could not be filed: PermissionError" in printed.err
    assert "NOT filed: PermissionError" in printed.out
    assert "Traceback" not in printed.err


def test_a_ticker_after_the_unwritable_finding_still_lands(fixture_lake: FixtureLake, monkeypatch):
    """The walk goes on, because one unwritable file is not the other tickers' dividends."""
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", amount=PAY_AMOUNT)],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )
    monkeypatch.setattr(
        "lake.actions.write_withheld",
        lambda *a, **k: (_ for _ in ()).throw(PermissionError(13, "Permission denied")),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["cash_amount"] == PAY_AMOUNT, "SPY was lost to QQQ's unwritable finding"
    assert len(result.unfiled) == 1


# -- a payload the ledger's own record rules refuse ---------------------------------------


def test_a_vendor_date_that_names_no_date_is_held_rather_than_ending_the_run(
    fixture_lake: FixtureLake,
):
    """A timestamp carrying a time of day does not name a date, and ``append`` refuses it.

    Before this was contained, one such value ended the run where it met it, so every ticker
    the walk had not reached yet lost its dividend, and nothing was written down.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", pay_date="2026-07-31T12:00:00Z")],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (landed,) = result.appended
    assert landed.symbol == "SPY", "the ticker after the bad payload never landed"
    (held,) = result.held
    assert held.finding.check == CHECK_DIVIDEND_PAYLOAD
    (finding,) = _findings(root, DAY_ONE)
    assert finding["exception"] == "QQQ: ValueError", (
        "the class is what files, and the message, which can name a path, is what drops"
    )


def test_a_negative_amount_passes_the_gate_and_is_still_held(fixture_lake: FixtureLake):
    """Nothing pays a negative dividend, and a self-consistent one clears the gate.

    ``append`` refuses it on the ledger's own record rules, so without containment a payload
    the gate agreed to would end the run instead of being held.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE, pay_amount=-1.0, amount=-4.0)]},
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    (held,) = result.held
    assert held.finding.check == CHECK_DIVIDEND_PAYLOAD
    assert held.finding.instrument_id == 1, "the id was known and was not carried"


def test_a_close_whose_rows_disagree_about_the_dividend_is_held(fixture_lake: FixtureLake):
    """A partition can hold two spellings of one instant, and both rows come back.

    Taking the first would let the file's own order decide which dividend the ledger gets,
    silently, on the surface every adjusted price is computed through.
    """
    first = _row(DAY_ONE)
    second = {**_row(DAY_ONE), "snap_ts": f"{DAY_ONE.isoformat()}T20:00:00Z"}
    second["div_ex_date"] = NEXT_EX_DATE
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [first, second]})

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == [], "one of two disagreeing rows was picked and landed"
    (held,) = result.held
    assert held.finding.check == CHECK_DIVIDEND_PAYLOAD


def test_a_partition_carrying_no_dividend_columns_is_a_ticker_day_with_no_event(
    fixture_lake: FixtureLake,
):
    """A session sealed before a column existed reads as null rather than ending the run."""
    from tests.support.lake import sample_quotes_table

    fixture_lake.with_quotes("SPY", DAY_ONE, sample_quotes_table())
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    _master().write(master_path(root))

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert result.ticker_days == 1
    assert result.appended == () and result.held == ()


# -- the ledger never grows on a value it already holds -----------------------------------


def test_an_ex_date_that_returns_to_an_earlier_value_does_not_append_forever(
    fixture_lake: FixtureLake,
):
    """A key is emitted once per run, at the first observation carrying its value.

    An ex-date that moves away and comes back is not a second first. Emitting it twice puts
    two provenances on one key, so neither matches what ``latest`` resolves, and the ledger
    grows by two lines every night for as long as the lake holds those sessions.
    """
    third = date(2026, 9, 16)
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(
                    DAY_TWO,
                    ex_date=NEXT_EX_DATE,
                    pay_amount=NEXT_PAY_AMOUNT,
                    amount=NEXT_ANNUALIZED,
                )
            ],
            ("SPY", third): [_row(third)],
        },
    )

    first = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))
    second = extract_dividends(lake_root=root, clock=ManualClock(SECOND_NIGHT))

    assert len(first.appended) == 2, "the returning value was emitted as a third event"
    assert second.appended == (), "the second night appended a dividend the ledger held"
    assert len(_entries(root)) == 2


# -- a symbol handed from one instrument to another ---------------------------------------


def test_a_ticker_handed_to_a_second_instrument_lands_that_instrument_its_own_entry(
    fixture_lake: FixtureLake,
):
    """The comparison is the instrument and the date together, which is what step 4 asks for.

    The master exists because tickers move. A walk comparing the ex-date alone reads the
    second instrument's first observation as a repeat of the first instrument's, and that
    instrument's dividend is silently absent from the ledger every factor is computed
    through.
    """
    master = SecurityMaster()
    first = master.register(
        kind=KIND_EQUITY,
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
        valid_from=date(2026, 9, 8),
        ticker="SPY",
    )
    master.remap(first, ID_TYPE_TICKER, "OLD", DAY_TWO)
    second = master.register(
        kind=KIND_EQUITY,
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
        valid_from=DAY_TWO,
        ticker="SPY",
    )
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
        master=master,
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    landed = {entry["instrument_id"] for entry in _entries(root)}
    assert landed == {first, second}, "the incoming instrument's dividend never landed"
    assert len(result.appended) == 2
    # Neither instrument watched the value change, so neither entry claims it did.
    assert {entry["provenance"] for entry in _entries(root)} == {PROVENANCE_VENDOR_REPORTED}


# -- the three refusals beyond the gap day --------------------------------------------


def test_a_quarantined_partition_costs_that_ticker_day_and_not_the_run(
    fixture_lake: FixtureLake,
):
    """Marketlake #352.

    Before this was contained, ``load_quotes`` raised ``PartitionQuarantined`` out of
    ``_observation`` and the run ended where it met it. QQQ sorts before SPY in ``by_ticker``,
    so SPY's dividend never landed and nothing was written down, which reads exactly like a
    night with no dividends in it.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ")],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
        },
        master=_master(tickers=("SPY", "QQQ")),
        quarantined=("QQQ", DAY_ONE),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["instrument_id"] == 1, "SPY was lost to QQQ's quarantined partition"
    assert [(skip.ticker, skip.day, skip.reason) for skip in result.skipped] == [
        ("QQQ", DAY_ONE, REASON_QUARANTINED)
    ]
    assert result.held == (), "a withheld verdict is not a finding this walk files"


def test_a_manifested_partition_whose_file_is_gone_costs_that_ticker_day(
    fixture_lake: FixtureLake,
):
    """The manifest records what the lake sealed, not what is still on disk.

    ``splits.read_session`` gives exactly that as its reason for catching ``PartitionAbsent``,
    and this walk's docstring used to argue the opposite.
    """
    root = _lake(
        fixture_lake,
        {
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ")],
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )
    LakePaths(root).partition_path(QUOTES, "QQQ", DAY_ONE).unlink()

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    (entry,) = _entries(root)
    assert entry["instrument_id"] == 1, "SPY was lost to QQQ's missing file"
    assert [skip.reason for skip in result.skipped] == [REASON_PARTITION_ABSENT]


def test_a_partition_the_projection_cannot_present_whole_costs_that_ticker_day(
    fixture_lake: FixtureLake,
):
    """An absent schema-version ledger makes the overflow projection refuse every read.

    Reading on would compare against contents nobody saw in full, so ``PartialRead`` refuses
    the bypass. Every ticker-day is refused here, which is what makes the count the only thing
    separating this run from one that read nothing at all.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]}, ledger=False)

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _entries(root) == []
    assert [skip.reason for skip in result.skipped] == [REASON_PARTIAL_READ]
    assert result.ticker_days == 1, "the ticker-day was enumerated and then refused"


def test_a_quarantined_partition_reaches_the_operator_as_a_counted_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """A caught refusal nothing counts is one silence traded for another.

    ``actions.main`` has no catch for a ``LoadError``, so before this the command died on a
    traceback before ``render`` ran, which cost the operator the block for every ticker that
    did land. The reason is on the line because the count alone cannot tell a gap day from a
    partition the battery withheld.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)]},
        quarantined=("SPY", DAY_ONE),
    )
    config = write_config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    printed = capsys.readouterr()
    assert "Traceback" not in printed.err
    assert "skipped:   1" in printed.out
    assert f"- {REASON_QUARANTINED}: 1" in printed.out


def test_a_verdict_that_clears_lands_the_entry_the_skip_delayed(fixture_lake: FixtureLake):
    """The skip is repairable, which is what settles it against holding a finding.

    ``ex_date`` is read off the row rather than derived from a boundary, so what a skip moves
    is ``observed_on``, which is no part of the ledger's key. The run after the verdict clears
    appends the corrected entry once, and it supersedes rather than landing beside the first.
    """
    root = _lake(
        fixture_lake,
        {("SPY", DAY_ONE): [_row(DAY_ONE)], ("SPY", DAY_TWO): [_row(DAY_TWO)]},
        quarantined=("SPY", DAY_ONE),
    )

    extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))
    (delayed,) = _entries(root)
    assert delayed["observed_on"] == DAY_TWO.isoformat()

    # The sign-off tool clears the verdict, the way marketlake #139 will.
    _clear_quarantine(root, "SPY", DAY_ONE)

    second = extract_dividends(lake_root=root, clock=ManualClock(SECOND_NIGHT))

    assert second.skipped == ()
    corrected = _entries(root)
    assert len(corrected) == 2, "the corrected entry did not land"
    assert corrected[-1]["observed_on"] == DAY_ONE.isoformat()
    assert corrected[-1]["ex_date"] == corrected[0]["ex_date"], (
        "a second key would leave both entries resolving separately"
    )

    # A third run learns nothing new, so the correction lands once rather than every night.
    third = extract_dividends(lake_root=root, clock=ManualClock(SECOND_NIGHT))
    assert len(_entries(root)) == 2 and third.unchanged == 1


def test_a_skipped_session_can_lose_a_transition_rather_than_delay_it(
    fixture_lake: FixtureLake,
):
    """The cost the count exists to make visible.

    A value that moves and moves back across the skipped session leaves ``previous`` matching
    the session after it, so the middle value is never recorded at all. This is the shape a
    gap day has always had, and it is why a run that skipped something must not read like a
    quiet night.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [
                _row(
                    DAY_TWO,
                    ex_date=NEXT_EX_DATE,
                    pay_amount=NEXT_PAY_AMOUNT,
                    amount=NEXT_ANNUALIZED,
                )
            ],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
        },
        quarantined=("SPY", DAY_TWO),
    )

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    landed = [entry["ex_date"] for entry in _entries(root)]
    assert landed == ["2026-06-18"], "only the value either side of the skip landed"
    assert [skip.reason for skip in result.skipped] == [REASON_QUARANTINED]


def test_several_skips_are_counted_each_and_grouped_by_reason(fixture_lake: FixtureLake):
    """The count is the deliverable, so more than one of it has to survive.

    Every other test here refuses exactly one ticker-day, which leaves a walk that recorded
    only the first skip, or counted every reason as the whole total, indistinguishable from
    this one. Two reasons with different counts is what separates them, and the rendered block
    is where an operator reads both.

    The reasons sort, so the lines arrive in one order rather than the order the walk met
    them. A run whose reasons moved between nights would read as a changed lake.
    """
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
        },
        quarantined=[("SPY", DAY_ONE), ("SPY", DAY_TWO)],
    )
    LakePaths(root).partition_path(QUOTES, "SPY", DAY_THREE).unlink()

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert [(skip.day, skip.reason) for skip in result.skipped] == [
        (DAY_ONE, REASON_QUARANTINED),
        (DAY_TWO, REASON_QUARANTINED),
        (DAY_THREE, REASON_PARTITION_ABSENT),
    ], "the walk recorded fewer skips than it made, or lost their order"

    rendered = result.render()
    assert "  skipped:   3" in rendered
    absent = rendered.index("    - manifested partition absent: 1")
    quarantined = rendered.index("    - quarantined: 2")
    assert absent < quarantined, "the reasons are sorted, so this pair has one order"


def test_the_reason_an_operator_reads_is_the_text_and_not_the_constant(fixture_lake: FixtureLake):
    """Every other assertion compares the walk's answer against the imported constant.

    That holds the walk against itself and holds the words against nothing, so renaming a
    reason changes the nightly sign-off block and the digest an operator reads while the
    suite stays green. These are the strings, written out once.
    """
    assert REASON_NO_SPOT_CLOSE == "no spot close"
    assert REASON_QUARANTINED == "quarantined"
    assert REASON_PARTIAL_READ == "partial read"
    assert REASON_PARTITION_ABSENT == "manifested partition absent"


def test_both_walks_spell_a_shared_reason_one_way(fixture_lake: FixtureLake):
    """The reason ``Skip`` and three constants moved into ``lake.actions``.

    ``lake.splits`` imports them rather than declaring its own, so one reason has one
    spelling. A local re-declaration in either module would shadow the import and drift
    silently, which is the failure the move exists to prevent, so identity is what is
    asserted rather than equality.
    """
    from lake import splits

    assert splits.REASON_QUARANTINED is REASON_QUARANTINED
    assert splits.REASON_PARTIAL_READ is REASON_PARTIAL_READ
    assert splits.REASON_PARTITION_ABSENT is REASON_PARTITION_ABSENT
    assert splits.Skip is actions.Skip
    # The close-of-record reason is per surface, because each names its own tag.
    assert splits.REASON_NO_OPTION_CLOSE == "no option close"
    assert splits.REASON_NO_OPTION_CLOSE != REASON_NO_SPOT_CLOSE


# -- 8 and 9. the entry point -------------------------------------------------------------


def test_the_command_runs_the_extraction_and_reports_what_it_did(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#284 test 8. ``lake.sweep`` drives the same walk from its 18:30 job."""
    root = _lake(
        fixture_lake,
        {
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", amount=PAY_AMOUNT)],
        },
        master=_master(tickers=("SPY", "QQQ")),
    )
    config = write_config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 0
    printed = capsys.readouterr().out
    assert "appended:  1" in printed
    assert "held:      1" in printed
    assert "unchanged: 0" in printed
    assert str(PAY_AMOUNT) in printed
    assert CHECK_DIVIDEND_CONSISTENCY in printed
    assert "filed at" in printed, "the report names no file for a finding a human has to read"
    # The entry carries no ticker, so the line an operator reads has to name one. Otherwise
    # reading the run means looking up who instrument 1 was on the day it was observed.
    assert "SPY (instrument 1)" in printed
    assert "QQQ" in printed
    # Provenance is what separates a value the lake watched change from one it only ever
    # found sitting there, and the sign-off block is where an operator reads it.
    assert PROVENANCE_VENDOR_REPORTED in printed

    # A second run over the same lake learns nothing new, and the count is what says so.
    # Without it a run that appended nothing reads the same as one that read nothing.
    assert actions.main(["--config", str(config)], clock=ManualClock(SECOND_NIGHT)) == 0
    again = capsys.readouterr().out
    assert "appended:  0" in again
    assert "unchanged: 1" in again, "the unchanged count does not move with the run"


def test_the_command_against_a_lake_with_no_master_exits_two_with_a_named_line(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#284 test 9, first half.

    An unseeded lake is an operator mistake with a one-command fix, and a traceback names the
    wrong thing. The line names the command that fixes it.
    """
    fixture_lake.with_quotes("SPY", DAY_ONE, _table([_row(DAY_ONE)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    config = write_config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 2
    printed = capsys.readouterr()
    assert printed.out == ""
    assert printed.err.startswith("actions: no security master at")
    assert "python -m lake.onboard" in printed.err
    assert "Traceback" not in printed.err


def test_the_command_against_a_torn_master_says_something_different(
    fixture_lake: FixtureLake, tmp_path: Path, capsys
):
    """#284 test 9, second half.

    An absent master is not a corrupt one, and the two want different things done. An operator
    who reads "cannot read the master" and runs the onboarding command against a corrupt file
    is being told the wrong thing.
    """
    fixture_lake.with_quotes("SPY", DAY_ONE, _table([_row(DAY_ONE)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()
    master_path(root).write_bytes(b"not parquet at all")
    config = write_config(tmp_path, root)

    code = actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT))

    assert code == 2
    printed = capsys.readouterr().err
    assert "is not readable parquet" in printed
    assert "Restore it from the backup." in printed
    assert "python -m lake.onboard" not in printed, (
        "a torn master was answered with the command for an absent one"
    )


def test_the_command_takes_its_clock_rather_than_reading_one(
    fixture_lake: FixtureLake, tmp_path: Path
):
    """A wall clock never reaches past this process, which is what makes two nights testable."""
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})
    config = write_config(tmp_path, root)

    assert actions.main(["--config", str(config)], clock=ManualClock(FIRST_NIGHT)) == 0

    (entry,) = _entries(root)
    assert entry["recorded_at"] == FIRST_NIGHT.isoformat()


def test_the_extraction_reads_no_config(fixture_lake: FixtureLake, monkeypatch):
    """Every dependency is injected, the way ``seed_spans`` states the rule.

    A lake root that came from config would mean the nightly job and a test could not point
    the extraction at two different lakes, and it is what the ``..._from_config`` wrapper is
    for.
    """
    root = _lake(fixture_lake, {("SPY", DAY_ONE): [_row(DAY_ONE)]})

    def refuse(*args, **kwargs):
        raise AssertionError("the extraction loaded a config file")

    monkeypatch.setattr("lake.config.load_config", refuse)

    result = extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert len(result.appended) == 1


def test_an_absent_master_raises_rather_than_holding_one_finding_per_ticker_day(
    fixture_lake: FixtureLake,
):
    """One condition with one command behind it is not a per-action finding.

    Holding it the way ``UnresolvedSymbol`` is held would file one finding per ticker-day for
    something a single onboarding run fixes, and the pile would say nothing the first line
    does not.
    """
    fixture_lake.with_quotes("SPY", DAY_ONE, _table([_row(DAY_ONE)]))
    fixture_lake.with_reference("schema_versions", _ledger_table())
    root = fixture_lake.build()

    with pytest.raises(actions.MasterAbsent):
        extract_dividends(lake_root=root, clock=ManualClock(FIRST_NIGHT))

    assert _findings(root, DAY_ONE) == []
