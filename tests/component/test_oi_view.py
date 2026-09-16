"""``oi_view`` against a fixture lake on disk.

Component tier, because every test crosses files: two sealed chains partitions, the two
reference tables, and the schema-version ledger. No vendor, no network, no wall clock, and
the real lake is never touched.

**This file carries its own chains schema, and that is deliberate.** The comparable set
ranks on ``volume`` and filters on ``expiration_date``, and ``FIXTURE_CHAINS_SCHEMA``
carries neither. ``expiration_date`` could simply be added to the shared schema.
``volume`` could not: ``test_load_chain.test_a_promoted_value_is_lifted_out_of_the_overflow``
asserts ``"volume" not in sample_chains_table(rows).column_names`` and its docstring says
why, which is that a column the table already has is filled in place, so the projection
adding a column is only visible on one the table lacks. Adding ``volume`` there would leave
that test green while it proved nothing. So this deliverable brings its own schema and the
shared one is left exactly as it was, which also keeps the nine other files that import it
out of this change.

The sessions are real exchange sessions, so the calendar under test is the real one rather
than a stand-in that could agree with a wrong implementation. 2026-09-14 and 2026-09-15 are
consecutive sessions. 2026-09-04 and 2026-09-08 are consecutive sessions with Labor Day
between them, which is what separates stepping over a holiday from stepping over a capture
gap.

The tests carry the numbering from #137, so a mutation that issue names points at the test
that issue names.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from lake.calendar import DEFAULT_CALENDAR, MARKET_TZ, ExchangeCalendar
from lake.capture_spans import CaptureSpans
from lake.config import GuardConstants
from lake.oi import (
    REASON_EXPIRED_OUT,
    REASON_NO_CYCLE_PASSED,
    REASON_NO_DATA_CYCLES,
    REASON_NOT_YET_CAPTURED,
    REASON_PARTITION_ABSENT,
    REASON_PARTITION_QUARANTINED,
    REASON_SET_UNDER_FLOOR,
    VERDICT_ABSENT,
    VERDICT_INDETERMINATE,
    VERDICT_PENDING,
    VERDICT_SETTLED,
    BaselineAbsent,
    ScopeUnreadable,
    SessionOutOfScope,
    oi_view,
)
from lake.schema_versions import RecordedVersion, SchemaVersionLedger, running_fingerprints
from lake.security_master import SecurityMaster
from tests.support.lake import FixtureLake

CALENDAR = ExchangeCalendar(DEFAULT_CALENDAR)

SESSION = "2026-09-14"
FOLLOWING = "2026-09-15"
THIRD = "2026-09-16"

# Before the holiday, and the session after it. 2026-09-07 is Labor Day.
BEFORE_HOLIDAY = "2026-09-04"
AFTER_HOLIDAY = "2026-09-08"

# The capture epoch these fixtures use. Well before every session above, so a span opened
# here covers all of them and a session out of scope is out of scope on purpose.
EPOCH = datetime(2026, 8, 1, 13, 30, tzinfo=UTC)
RECORDED_AT = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

# A chains schema with the two columns the comparable set needs. Everything else matches
# the shared fixture schema, so the loader's own rules apply unchanged.
OI_CHAINS_SCHEMA = pa.schema(
    [
        ("snap_ts", pa.string()),
        ("fetch_ts", pa.string()),
        ("vendor_quote_ts", pa.string()),
        ("ticker", pa.string()),
        ("occ_symbol", pa.string()),
        ("open_interest", pa.int64()),
        ("volume", pa.int64()),
        ("expiration_date", pa.string()),
        ("row_kind", pa.string()),
        ("error_class", pa.string()),
        ("suspect", pa.bool_()),
        ("close_tag", pa.string()),
        ("session_phase", pa.string()),
        ("schema_version", pa.int64()),
        ("extra", pa.string()),
    ]
)

# An expiration well past every session here, so a contract carrying it survives into the
# next chain and can be a voter.
LATER = "2026-10-16"


def occ(index: int, expiration: str = LATER) -> str:
    """One OCC symbol. Six-character padded root, then YYMMDD, then the right and strike."""
    stamp = date.fromisoformat(expiration).strftime("%y%m%d")
    return f"SPY   {stamp}C{index:08d}"


def row(
    snap: str,
    symbol: str,
    open_interest: int | None,
    *,
    volume: int = 0,
    expiration: str = LATER,
    close_tag: str | None = None,
    row_kind: str = "data",
) -> dict:
    """One chains row. ``fetch_ts`` sits off the slot, the way a real one does."""
    return {
        "snap_ts": snap,
        "fetch_ts": snap,
        "vendor_quote_ts": snap,
        "ticker": "SPY",
        "occ_symbol": symbol,
        "open_interest": open_interest,
        "volume": volume,
        "expiration_date": expiration,
        "row_kind": row_kind,
        "error_class": None,
        "suspect": False,
        "close_tag": close_tag,
        "session_phase": None,
        "schema_version": 1,
        "extra": None,
    }


def table(rows: list[dict]) -> pa.Table:
    columns = {name: [entry.get(name) for entry in rows] for name in OI_CHAINS_SCHEMA.names}
    return pa.table(columns, schema=OI_CHAINS_SCHEMA)


def et(day: str, hour: int, minute: int) -> str:
    """An Eastern wall-clock stamp, spelled with its own offset the way capture spells it."""
    moment = datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}:00")
    return moment.replace(tzinfo=MARKET_TZ).isoformat()


def close_rows(day: str, contracts: dict[str, int], **kwargs) -> list[dict]:
    """A whole close-of-record cycle, tagged ``option_close``."""
    stamp = CALENDAR.option_close(date.fromisoformat(day)).isoformat()
    return [
        row(stamp, symbol, value, close_tag="option_close", **_per(symbol, kwargs))
        for symbol, value in contracts.items()
    ]


def cycle_rows(day: str, hour: int, minute: int, contracts: dict[str, int], **kwargs) -> list[dict]:
    """One intraday cycle, untagged."""
    stamp = et(day, hour, minute)
    return [
        row(stamp, symbol, value, **_per(symbol, kwargs)) for symbol, value in contracts.items()
    ]


def _per(symbol: str, kwargs: dict) -> dict:
    """Per-contract keyword overrides, so one call can vary volume across the set."""
    resolved = dict(kwargs)
    volumes = resolved.pop("volumes", None)
    expirations = resolved.pop("expirations", None)
    if volumes is not None:
        resolved["volume"] = volumes[symbol]
    if expirations is not None:
        resolved["expiration"] = expirations[symbol]
    return resolved


def ledger_table() -> pa.Table:
    entry = RecordedVersion(version=1, recorded_at=RECORDED_AT, fingerprints=running_fingerprints())
    return SchemaVersionLedger([entry]).to_table()


def reference(
    lake: FixtureLake,
    *,
    options: bool = True,
    span_start: datetime = EPOCH,
    span_end: datetime | None = None,
) -> None:
    """The security master and the capture spans, which together answer scope."""
    master = SecurityMaster()
    instrument = master.register(
        kind="equity",
        capture_start=span_start,
        valid_from=span_start.date(),
        ticker="SPY",
    )
    spans = CaptureSpans()
    spans.open_span(instrument, span_start, options)
    if span_end is not None:
        spans.close_span(instrument, span_end)
    lake.with_reference("security_master", master.to_table())
    lake.with_reference("capture_spans", spans.to_table())
    lake.with_reference("schema_versions", ledger_table())


def constants(**overrides) -> GuardConstants:
    """Small guard constants, so a fixture stays readable rather than realistic in size."""
    defaults = {
        "oi_comparable_set_floor": 4,
        "oi_refresh_quorum": 0.50,
        "oi_plateau_cycles": 1,
        "oi_comparable_set_size": 10,
    }
    return GuardConstants(**{**defaults, **overrides})


def verdicts(answer: pa.Table) -> set[tuple[str, str | None]]:
    return set(
        zip(answer.column("verdict").to_pylist(), answer.column("reason").to_pylist(), strict=True)
    )


def view(root: Path, **kwargs) -> pa.Table:
    return oi_view("SPY", SESSION, lake_root=root, calendar=CALENDAR, **kwargs)


# A set of eight contracts, all surviving past both sessions, with descending volume so
# the ranking has something to rank.
SET = {occ(index): 1000 + index for index in range(8)}
VOLUMES = {symbol: 900 - position for position, symbol in enumerate(SET)}
REFRESHED = {symbol: value + 500 for symbol, value in SET.items()}


def settled_lake(fixture_lake: FixtureLake, following_cycles: list[list[dict]]) -> Path:
    """S's close plus the following session's cycles, in order."""
    fixture_lake.with_chains(
        "SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)), source="capture"
    )
    rows: list[dict] = []
    for entry in following_cycles:
        rows.extend(entry)
    fixture_lake.with_chains("SPY", FOLLOWING, table(rows), source="capture")
    reference(fixture_lake)
    return fixture_lake.build()


# -- 1. the baseline resolves through close_tag -------------------------------


def test_1_a_later_untagged_cycle_does_not_become_the_baseline(fixture_lake: FixtureLake):
    """#137 test 1. Onboarding can journal a cycle after a session's own close.

    The session holds its tagged close and then an untagged 22:00 cycle carrying the
    already-refreshed numbers. A baseline resolved off the largest ``snap_ts`` would take
    the late cycle, find the next session identical to it, and report no refresh. A
    baseline resolved off ``close_tag`` takes the close and sees every contract change.
    """
    fixture_lake.with_chains(
        "SPY",
        SESSION,
        table(
            close_rows(SESSION, SET, volumes=VOLUMES)
            + cycle_rows(SESSION, 22, 0, REFRESHED, volumes=VOLUMES)
        ),
    )
    fixture_lake.with_chains(
        "SPY",
        FOLLOWING,
        table(
            cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert set(answer.column("open_interest").to_pylist()) == set(REFRESHED.values())


# -- 2. the quorum ------------------------------------------------------------


def test_2_a_change_under_the_quorum_declares_no_refresh(fixture_lake: FixtureLake):
    """#137 test 2. One contract moving is noise, not a settlement."""
    one_moved = dict(SET)
    one_moved[occ(0)] = SET[occ(0)] + 500
    root = settled_lake(
        fixture_lake,
        [
            cycle_rows(FOLLOWING, 9, 30, one_moved, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 32, REFRESHED, volumes=VOLUMES),
        ],
    )

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert answer.column("source_snap_ts").to_pylist()[0] == et(FOLLOWING, 9, 31)


# -- 3. the plateau -----------------------------------------------------------


def test_3_a_cycle_that_moves_again_loses_to_the_one_that_settles(fixture_lake: FixtureLake):
    """#137 test 3. A snapshot straddling the vendor's load is not the settled figure."""
    torn = {symbol: 0 for symbol in SET}
    root = settled_lake(
        fixture_lake,
        [
            cycle_rows(FOLLOWING, 9, 30, torn, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 32, REFRESHED, volumes=VOLUMES),
        ],
    )

    answer = view(root, constants=constants())

    assert answer.column("source_snap_ts").to_pylist()[0] == et(FOLLOWING, 9, 31)
    assert set(answer.column("open_interest").to_pylist()) == set(REFRESHED.values())


# -- 4. a gap session stops the walk ------------------------------------------


def test_4_a_following_session_of_gap_rows_makes_s_absent(fixture_lake: FixtureLake):
    """#137 test 4. The walk stops rather than reading the session after the gap.

    The third session holds a clean refresh. A walk that looked past the gap would find it
    and return a settled figure that belongs to a different session's trading.
    """
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    stamp = CALENDAR.option_close(date.fromisoformat(FOLLOWING)).isoformat()
    fixture_lake.with_chains(
        "SPY",
        FOLLOWING,
        table([row(stamp, occ(0), None, row_kind="gap", close_tag="option_close")]),
    )
    fixture_lake.with_chains(
        "SPY",
        THIRD,
        table(
            cycle_rows(THIRD, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(THIRD, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_ABSENT, REASON_NO_DATA_CYCLES)}


# -- 5 and 6. pending, and what stops it being pending ------------------------


def test_5_an_unsealed_following_session_is_pending(fixture_lake: FixtureLake):
    """#137 test 5. Last night's session is not permanently unknown."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_PENDING, REASON_NOT_YET_CAPTURED)}


def test_5_a_newer_sealed_session_makes_the_gap_absent(fixture_lake: FixtureLake):
    """#137 test 5, the other half. Nothing goes back to seal a session already passed."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    fixture_lake.with_chains(
        "SPY", THIRD, table(cycle_rows(THIRD, 9, 30, REFRESHED, volumes=VOLUMES))
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_ABSENT, REASON_PARTITION_ABSENT)}


def test_6_a_retired_instrument_is_absent_rather_than_pending(fixture_lake: FixtureLake):
    """#137 test 6. A closed span means nothing will ever capture the next session."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    reference(fixture_lake, span_end=datetime(2026, 9, 14, 21, 0, tzinfo=UTC))
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_ABSENT, REASON_PARTITION_ABSENT)}


def test_6_a_quotes_only_instrument_has_no_view_at_all(fixture_lake: FixtureLake):
    """#137 test 6, the other half. A span that captures no chains answers no session.

    This refuses rather than marking, and earlier than the walk. A quotes-only instrument
    seals no chains partition ever, so every session of it would be pending forever under
    a newest-sealed test on its own.
    """
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    reference(fixture_lake, options=False)
    root = fixture_lake.build()

    with pytest.raises(SessionOutOfScope, match="option chains"):
        view(root, constants=constants())


# -- 7. the floor -------------------------------------------------------------


def test_7_a_set_under_the_floor_is_indeterminate(fixture_lake: FixtureLake):
    """#137 test 7. Too few voters cannot tell a stale feed from quiet contracts."""
    root = settled_lake(
        fixture_lake,
        [
            cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES),
        ],
    )

    answer = view(root, constants=constants(oi_comparable_set_floor=len(SET) + 1))

    assert verdicts(answer) == {(VERDICT_INDETERMINATE, REASON_SET_UNDER_FLOOR)}


# -- 8. a contract that expires out --------------------------------------------


def test_8_a_contract_expiring_on_s_is_marked_rather_than_carried(fixture_lake: FixtureLake):
    """#137 test 8. Expiry-day final OI is unobservable, so it gets a marker.

    The expiring contract is in S's close roster and gone from the next chain. Carrying
    its S-close figure forward would report a settled number nothing observed.
    """
    expiring = occ(99, SESSION)
    roster = {**SET, expiring: 4242}
    volumes = {**VOLUMES, expiring: 5000}
    expirations = {symbol: LATER for symbol in SET} | {expiring: SESSION}
    fixture_lake.with_chains(
        "SPY",
        SESSION,
        table(close_rows(SESSION, roster, volumes=volumes, expirations=expirations)),
    )
    fixture_lake.with_chains(
        "SPY",
        FOLLOWING,
        table(
            cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    rows = dict(
        zip(
            answer.column("occ_symbol").to_pylist(),
            zip(
                answer.column("verdict").to_pylist(),
                answer.column("reason").to_pylist(),
                answer.column("open_interest").to_pylist(),
                strict=True,
            ),
            strict=True,
        )
    )
    assert rows[expiring] == (VERDICT_ABSENT, REASON_EXPIRED_OUT, None)
    assert rows[occ(0)] == (VERDICT_SETTLED, None, REFRESHED[occ(0)])


# -- 9. quarantine -------------------------------------------------------------


def test_9_a_quarantined_following_partition_makes_s_absent(fixture_lake: FixtureLake):
    """#137 test 9. Fail closed, and never step past a withheld partition."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    fixture_lake.with_chains(
        "SPY",
        FOLLOWING,
        table(
            cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    fixture_lake.with_quarantine(
        {
            "partition": f"chains/ticker=SPY/date={FOLLOWING}.parquet",
            "quarantined": True,
            "reason": "row count out of band",
        }
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_ABSENT, REASON_PARTITION_QUARANTINED)}


def test_9_the_opt_in_reads_the_quarantined_partition(fixture_lake: FixtureLake):
    """The explicit opt-in still reads it, the way every other read in the layer does."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    fixture_lake.with_chains(
        "SPY",
        FOLLOWING,
        table(
            cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    fixture_lake.with_quarantine(
        {
            "partition": f"chains/ticker=SPY/date={FOLLOWING}.parquet",
            "quarantined": True,
            "reason": "row count out of band",
        }
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = view(root, constants=constants(), include_quarantined=True)

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}


# -- 10. the set is fixed at S -------------------------------------------------


def test_10_a_partial_cycle_votes_with_what_it_carries(fixture_lake: FixtureLake):
    """#137 test 10. The set is S's, and the candidate supplies voters rather than members.

    The next session's cycle carries four of S's eight set members, all refreshed, plus
    eight contracts S never ranked whose OI never moves. Voting over the set members the
    cycle carries makes that four of four. Re-ranking off the candidate would make it four
    of twelve, a third, which is under the quorum and answers that nothing refreshed.
    """
    partial = {symbol: REFRESHED[symbol] for symbol in list(SET)[:4]}
    strangers = {occ(50 + index): 77 for index in range(8)}
    stranger_volumes = {symbol: 10_000 for symbol in strangers}
    first = cycle_rows(FOLLOWING, 9, 30, partial, volumes=VOLUMES) + cycle_rows(
        FOLLOWING, 9, 30, strangers, volumes=stranger_volumes
    )
    second = cycle_rows(FOLLOWING, 9, 31, partial, volumes=VOLUMES) + cycle_rows(
        FOLLOWING, 9, 31, strangers, volumes=stranger_volumes
    )
    root = settled_lake(fixture_lake, [first, second])

    answer = view(root, constants=constants())

    assert (VERDICT_SETTLED, None) in verdicts(answer)
    assert answer.column("source_snap_ts").to_pylist()[0] == et(FOLLOWING, 9, 30)


# -- 12. the plateau window at a session's end ---------------------------------


def test_12_a_refresh_inside_the_final_k_yields_no_verdict(fixture_lake: FixtureLake):
    """#137 test 12. A cycle with nothing behind it cannot show a plateau.

    The session holds two cycles. The first matches the baseline and the second is fully
    refreshed, and the second is the last, so with a plateau of one it is not a candidate.
    Selecting it would report a figure nothing confirmed.
    """
    root = settled_lake(
        fixture_lake,
        [
            cycle_rows(FOLLOWING, 9, 30, SET, volumes=VOLUMES),
            cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES),
        ],
    )

    answer = view(root, constants=constants())

    assert verdicts(answer) == {(VERDICT_ABSENT, REASON_NO_CYCLE_PASSED)}


# -- 13. the reasons are told apart --------------------------------------------


def test_13_each_withheld_reason_is_distinguishable(tmp_path: Path):
    """#137 test 13. Six session-wide reasons, each produced and each its own code.

    A single undifferentiated marker would pass every other test in this file, because
    every other test asserts one scenario at a time. This runs six scenarios that all
    withhold a value and asserts they do not collapse onto one another.
    """
    produced = {name: _reason_for(tmp_path / name, name) for name in _WITHHOLDING}

    assert len(set(produced.values())) == len(produced), produced
    assert produced == {
        "no_data_cycles": REASON_NO_DATA_CYCLES,
        "not_yet_captured": REASON_NOT_YET_CAPTURED,
        "partition_absent": REASON_PARTITION_ABSENT,
        "partition_quarantined": REASON_PARTITION_QUARANTINED,
        "set_under_floor": REASON_SET_UNDER_FLOOR,
        "no_cycle_passed": REASON_NO_CYCLE_PASSED,
    }


_WITHHOLDING = (
    "no_data_cycles",
    "not_yet_captured",
    "partition_absent",
    "partition_quarantined",
    "set_under_floor",
    "no_cycle_passed",
)


def _reason_for(root: Path, scenario: str) -> str | None:
    """Build the lake one scenario needs, read it, and hand back the reason it produced."""
    lake = FixtureLake(root)
    lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    guards = constants()

    if scenario == "no_data_cycles":
        stamp = CALENDAR.option_close(date.fromisoformat(FOLLOWING)).isoformat()
        lake.with_chains("SPY", FOLLOWING, table([row(stamp, occ(0), None, row_kind="gap")]))
    elif scenario == "partition_absent":
        lake.with_chains("SPY", THIRD, table(cycle_rows(THIRD, 9, 30, SET, volumes=VOLUMES)))
    elif scenario == "set_under_floor":
        guards = constants(oi_comparable_set_floor=len(SET) + 1)
    elif scenario == "no_cycle_passed":
        lake.with_chains(
            "SPY",
            FOLLOWING,
            table(
                cycle_rows(FOLLOWING, 9, 30, SET, volumes=VOLUMES)
                + cycle_rows(FOLLOWING, 9, 31, SET, volumes=VOLUMES)
            ),
        )
    elif scenario == "partition_quarantined":
        lake.with_chains(
            "SPY",
            FOLLOWING,
            table(
                cycle_rows(FOLLOWING, 9, 30, REFRESHED, volumes=VOLUMES)
                + cycle_rows(FOLLOWING, 9, 31, REFRESHED, volumes=VOLUMES)
            ),
        )
        lake.with_quarantine(
            {
                "partition": f"chains/ticker=SPY/date={FOLLOWING}.parquet",
                "quarantined": True,
                "reason": "row count out of band",
            }
        )
    reference(lake)
    answer = oi_view("SPY", SESSION, lake_root=lake.build(), calendar=CALENDAR, constants=guards)
    reasons = set(answer.column("reason").to_pylist())
    assert len(reasons) == 1, (scenario, reasons)
    return reasons.pop()


# -- scope, and the refusals that are not markers ------------------------------


def test_a_session_before_the_capture_epoch_gets_no_verdict(fixture_lake: FixtureLake):
    """A partition that predates the span is not a session this view can judge.

    The live lake holds exactly this shape: ``chains/ticker=SPY/date=2026-09-02.parquet``
    with two data rows, from before the epoch the spans record.
    """
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    reference(fixture_lake, span_start=datetime(2026, 9, 15, 13, 30, tzinfo=UTC))
    root = fixture_lake.build()

    with pytest.raises(SessionOutOfScope, match="no capture span"):
        view(root, constants=constants())


def test_a_day_that_is_not_a_session_is_refused(fixture_lake: FixtureLake):
    """2026-09-12 is a Saturday. It has no close for a span to contain."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    reference(fixture_lake)
    root = fixture_lake.build()

    with pytest.raises(SessionOutOfScope, match="not a trading session"):
        oi_view("SPY", "2026-09-12", lake_root=root, calendar=CALENDAR, constants=constants())


def test_an_absent_reference_table_refuses_rather_than_answering_out_of_scope(
    fixture_lake: FixtureLake,
):
    """Silently answering out of scope would look exactly like an empty lake."""
    fixture_lake.with_chains("SPY", SESSION, table(close_rows(SESSION, SET, volumes=VOLUMES)))
    fixture_lake.with_reference("schema_versions", ledger_table())
    root = fixture_lake.build()

    with pytest.raises(ScopeUnreadable, match="security master"):
        view(root, constants=constants())


def test_a_session_whose_own_close_is_missing_refuses_rather_than_returning_nothing(
    fixture_lake: FixtureLake,
):
    """The answer's rows are S's close roster, so no close means no rows to mark.

    An empty table would make "S has no baseline" and "S's close held no contracts" the
    same answer.
    """
    fixture_lake.with_chains(
        "SPY", SESSION, table(cycle_rows(SESSION, 9, 30, SET, volumes=VOLUMES))
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    with pytest.raises(BaselineAbsent) as caught:
        view(root, constants=constants())
    assert caught.value.reason == "no_close_of_record"


# -- a holiday is stepped over, a capture gap is not --------------------------


def test_a_holiday_is_stepped_over_by_the_calendar(fixture_lake: FixtureLake):
    """2026-09-07 is Labor Day, so 2026-09-04's calendar-next session is 2026-09-08.

    Nothing traded in between, so nothing settled in between, and Friday's OI is what
    Tuesday's chains carry. This is the case a capture gap looks like and is not.
    """
    fixture_lake.with_chains(
        "SPY", BEFORE_HOLIDAY, table(close_rows(BEFORE_HOLIDAY, SET, volumes=VOLUMES))
    )
    fixture_lake.with_chains(
        "SPY",
        AFTER_HOLIDAY,
        table(
            cycle_rows(AFTER_HOLIDAY, 9, 30, REFRESHED, volumes=VOLUMES)
            + cycle_rows(AFTER_HOLIDAY, 9, 31, REFRESHED, volumes=VOLUMES)
        ),
    )
    reference(fixture_lake)
    root = fixture_lake.build()

    answer = oi_view(
        "SPY", BEFORE_HOLIDAY, lake_root=root, calendar=CALENDAR, constants=constants()
    )

    assert verdicts(answer) == {(VERDICT_SETTLED, None)}
    assert answer.column("source_session").to_pylist()[0] == AFTER_HOLIDAY
