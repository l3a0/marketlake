"""The split walk resuming from a saved state, and finding each ticker's cutoff in one pass.

marketlake #755 trims old chains partitions, and a walk rebuilt from the first session left on
disk lands permanent phantom splits. marketlake #783 lets the walk report each ticker's state at
a cutoff and resume from it later. These tests hold it to one standard: a resumed walk answers
exactly what a walk over the whole lake answers.

**How a test stops a run at a cutoff.** It hides the chains days after that day from the
manifest, so the run meets the real end of the ticker's days, and restores them afterwards. A
trim deletes the days at or before the cutoff, file and manifest line both. Every run here goes
to a copy of one built lake, so the walk from the start and the resumed walk never see each
other's ledger, master or findings.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from lake.paths import CHAINS, LakePaths, parse_partition_rel
from lake.security_master import Mapping, SecurityMaster, master_path
from lake.splits import (
    CHECK_SPLIT_BOUNDARY,
    CHECK_SPLIT_CONSISTENCY,
    CHECK_STRIKE_SCALE,
    REASON_NOT_SEALED,
    REASON_OUT_OF_SCOPE,
    REASON_PARTIAL_READ,
    REASON_PARTITION_ABSENT,
    REASON_QUARANTINED,
    REASON_THIN,
    SplitReport,
    WalkState,
    detect_splits,
)
from tests.component.test_split_detection import (
    ADJUSTED,
    ADJUSTED_NOTE,
    ADJUSTED_ROOT,
    CALENDAR,
    CARRIED_OCC,
    DAY_ONE,
    DAY_THREE,
    DAY_TWO,
    FIRST_NIGHT,
    LADDER,
    QQQ_OCC,
    SECOND_NIGHT,
    _adjusted_row,
    _chains,
    _entries,
    _gap_day_row,
    _ladder_rows,
    _lake,
    _mapping,
    _mappings,
    _master,
    _row,
)
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake

DAY_FOUR = date(2026, 9, 17)
DAY_FIVE = date(2026, 9, 18)

# The re-symboled contracts of ``test_a_root_returning_to_the_chain_is_not_a_gain``, carried
# under the vendor's default ssid derivation rather than the default contract's, so each row
# is its own contract.
RETURNING = dict(
    option_root=ADJUSTED_ROOT,
    occ_symbol="SPY1  260918C00433330",
    deliverables=ADJUSTED,
    note=ADJUSTED_NOTE,
    non_standard=True,
)


# -- stopping, trimming and resuming a run ---------------------------------------------------


def _chains_lines(root: Path, drop) -> list[str]:
    """Rewrite the manifest without the chains lines ``drop`` names, and return those lines."""
    path = root / "manifest.jsonl"
    kept, dropped = [], []
    for line in path.read_text().splitlines():
        reference = parse_partition_rel(json.loads(line)["partition"])
        if reference is not None and reference.surface == CHAINS and drop(reference):
            dropped.append(line)
        else:
            kept.append(line)
    path.write_text("".join(f"{line}\n" for line in kept))
    return dropped


def _restore(root: Path, lines: list[str]) -> None:
    with (root / "manifest.jsonl").open("a") as manifest:
        manifest.writelines(f"{line}\n" for line in lines)


def _trim(root: Path, states: tuple[WalkState, ...]) -> None:
    """Delete each ticker's chains days at or before its cutoff, file and manifest line both."""
    cutoffs = {state.ticker: state.cutoff for state in states}
    for line in _chains_lines(
        root, lambda ref: ref.ticker in cutoffs and ref.day <= cutoffs[ref.ticker]
    ):
        reference = parse_partition_rel(json.loads(line)["partition"])
        LakePaths(root).chains_partition_path(reference.ticker, reference.day).unlink()


def _clear(root: Path, partitions: tuple[str, ...]) -> None:
    """A sign-off clearing each partition's quarantine, the way ``lake.signoff`` writes it."""
    with (root / "quarantine.jsonl").open("a") as ledger:
        for partition in partitions:
            ledger.write(json.dumps({"partition": partition, "verdict": "clean"}) + "\n")


def _copy(base: Path, name: str) -> Path:
    """A fresh copy of the lake beside it, numbered when the name is already taken."""
    target, number = base.parent / name, 1
    while target.exists():
        number += 1
        target = base.parent / f"{name}-{number}"
    shutil.copytree(base, target)
    return target


def _run(root: Path, *, night=FIRST_NIGHT, calendar=CALENDAR, **kwargs) -> SplitReport:
    return detect_splits(lake_root=root, clock=ManualClock(night), calendar=calendar, **kwargs)


def _stopped_through(base: Path, name: str, through: date, **kwargs) -> SplitReport:
    """A run over a copy of the lake that holds no chains day after ``through``."""
    root = _copy(base, name)
    _chains_lines(root, lambda ref: ref.day > through)
    return _run(root, **kwargs)


def _states(report_out: SplitReport) -> dict[str, WalkState]:
    return {state.ticker: state for state in report_out.states}


# -- what two walks are compared on ----------------------------------------------------------


def _ledger(root: Path) -> list[str]:
    """Every ledger line but ``recorded_at``, which names the night that wrote it."""
    return sorted(
        json.dumps({k: v for k, v in entry.items() if k != "recorded_at"}, sort_keys=True)
        for entry in _entries(root)
    )


def _answers(reports: list[tuple[SplitReport, dict[str, date] | None]]) -> dict[str, list]:
    """The held findings, marks and skips several runs reported, each run up to its own bound.

    Each run is paired with a bound per ticker, or with ``None`` to count it in full. A run
    with bounds counts for each ticker's days up to its bound, and not at all for a ticker it
    has no bound for. A first night that reported no state for a ticker leaves the second
    night to walk that ticker from the start, so the first night's answers for it count for
    nothing.
    """

    def counts(ticker: str, day: date, bounds: dict[str, date] | None) -> bool:
        return bounds is None or (ticker in bounds and day <= bounds[ticker])

    held, marks, skipped = [], [], []
    for report_out, bounds in reports:
        held += [
            repr(entry.finding)
            for entry in report_out.held
            if counts(entry.finding.symbol, entry.finding.observed_on, bounds)
        ]
        marks += [
            (mark.ticker, mark.day, mark.reason)
            for mark in report_out.not_adjustments
            if counts(mark.ticker, mark.day, bounds)
        ]
        skipped += [
            (skip.ticker, skip.day, skip.reason)
            for skip in report_out.skipped
            if counts(skip.ticker, skip.day, bounds)
        ]
    return {"held": sorted(held), "marks": sorted(marks), "skipped": sorted(skipped)}


# -- the scenarios ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """A lake as it stands on the second night, and what the first night saw of it.

    ``night_one`` is the last chains day the lake held on the first night. ``quarantine`` is
    withheld on the first night and ``cleared`` is signed off before the second. Each of SPY's
    ``unsealed`` days sat in the journal on the first night and is sealed before the second.
    ``master`` is the master both nights start from, and ``edited`` replaces it before the
    second. ``exposes`` names the state variable whose loss this lake shows, where it shows one.
    """

    sessions: dict
    night_one: date
    exposes: str | None = None
    quarantine: tuple[str, ...] = ()
    cleared: tuple[str, ...] = ()
    unsealed: tuple[date, ...] = ()
    master: tuple[Mapping, ...] | None = None
    edited: tuple[Mapping, ...] | None = None


def _partition(day: date, ticker: str = "SPY") -> str:
    return f"chains/ticker={ticker}/date={day.isoformat()}.parquet"


SCENARIOS = {
    # A whole-ratio split the day after the cutoff, which only the scale guard sees. It needs
    # the previous session, and the ladder and spot on it.
    "a split the day after the cutoff": Scenario(
        sessions={
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): _ladder_rows(DAY_TWO, LADDER, 700.0),
            ("SPY", DAY_THREE): _ladder_rows(
                DAY_THREE, tuple(strike / 2 for strike in LADDER), 349.5
            ),
        },
        night_one=DAY_TWO,
        exposes="previous",
    ),
    # A root that falls out of the chain on the cutoff day and returns the day after.
    "a root returning after the cutoff": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **RETURNING)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE), _row(DAY_THREE, **RETURNING)],
        },
        night_one=DAY_TWO,
        exposes="seen",
    ),
    # A contract read since day one and re-symboled on day four. Its old mapping opens on day
    # one, and only the saved history knows that once days one and two are gone.
    "a re-symboling two days after the cutoff": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
            ("SPY", DAY_FOUR): [_row(DAY_FOUR, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_FOUR)],
        },
        night_one=DAY_TWO,
        exposes="history",
    ),
    # A gap day at the cutoff, so the boundary after it is a day wider than it looks.
    "a gap day at the cutoff before a split": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_gap_day_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_THREE)],
        },
        night_one=DAY_TWO,
        exposes="unread_since",
    ),
    # A session the lake never captured between the cutoff and a split.
    "an uncaptured session after the cutoff before a split": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_FOUR): [_row(DAY_FOUR, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_FOUR)],
        },
        night_one=DAY_TWO,
        exposes="last_day",
    ),
    # A quarantine signed off after the first night. A walk from the start reads the day and
    # lands the split after it, so the resumed walk has to read it too.
    "a quarantine cleared after the first night": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_THREE)],
        },
        night_one=DAY_TWO,
        quarantine=(_partition(DAY_TWO),),
        cleared=(_partition(DAY_TWO),),
    ),
    # A split the gate refuses on day three. A held split never clears, so it files again
    # every night, and the resumed walk has to file it too.
    "a refused split before the first night ends": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [
                _row(DAY_THREE, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_THREE, note="200 SPY"),
            ],
            ("SPY", DAY_FOUR): [
                _row(DAY_FOUR, occ_symbol=CARRIED_OCC),
                _adjusted_row(DAY_FOUR, note="200 SPY"),
            ],
        },
        night_one=DAY_FOUR,
    ),
    # The root of "a root returning after the cutoff", with day one still in the journal on
    # the first night, as a seal ``compact`` refused would leave it while day two sealed. Read
    # from day two alone, day three's returning root is a gain.
    "an unsealed first day before the first night": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE), _row(DAY_ONE, **RETURNING)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE), _row(DAY_THREE, **RETURNING)],
        },
        night_one=DAY_TWO,
        unsealed=(DAY_ONE,),
    ),
    # Day one resolves and is thin, and day two is out of scope until a master edit closes the
    # hole. A walk from the start then reads day two, so the old mapping of day four's
    # re-symboling opens there, and the resumed walk has to read it too.
    "a master hole after a thin first day, closed after the first night": Scenario(
        sessions={
            ("SPY", DAY_ONE): [_row(DAY_ONE, suspect=True)],
            ("SPY", DAY_TWO): [_row(DAY_TWO)],
            ("SPY", DAY_THREE): [_row(DAY_THREE)],
            ("SPY", DAY_FOUR): [_row(DAY_FOUR, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_FOUR)],
        },
        night_one=DAY_THREE,
        master=(_mapping(1, "SPY", valid_to=DAY_TWO), _mapping(1, "SPY", valid_from=DAY_THREE)),
        edited=(_mapping(1, "SPY"),),
    ),
}

# Each variable set back to what a walk from scratch starts with. The ladder is the
# previous session kept with its strikes and spot stripped, so a state that saved a slimmer
# session than the walk holds fails too.
DROPS = {
    "previous": lambda state: replace(state, previous=None),
    "ladder": lambda state: replace(
        state, previous=replace(state.previous, strikes=frozenset(), spot=None)
    ),
    "seen": lambda state: replace(state, seen=frozenset()),
    "history": lambda state: replace(state, history=()),
    "unread_since": lambda state: replace(state, unread_since=0),
    "last_day": lambda state: replace(state, last_day=None),
}
EXPOSED_BY = {"ladder": "previous"}


def _base(fixture_lake: FixtureLake, scenario: Scenario) -> Path:
    return _lake(
        fixture_lake,
        scenario.sessions,
        quarantine=[{"partition": p, "verdict": "bad"} for p in scenario.quarantine],
        master=None if scenario.master is None else SecurityMaster(scenario.master),
    )


def _edit(root: Path, scenario: Scenario) -> None:
    """The changes made between the two nights: a sign-off and a master edit."""
    _clear(root, scenario.cleared)
    if scenario.edited is not None:
        SecurityMaster(scenario.edited).write(master_path(root))


def _from_the_start(base: Path, scenario: Scenario) -> dict[str, list]:
    """The second night's walk over the whole lake."""
    root = _copy(base, "whole")
    _edit(root, scenario)
    report_out = _run(root, night=SECOND_NIGHT)
    return {
        "ledger": _ledger(root),
        "mappings": sorted(_mappings(root)),
        **_answers([(report_out, None)]),
    }


def _resumed(base: Path, scenario: Scenario, *, trim: bool = True, drop=None) -> dict[str, list]:
    """The first night stopped at ``night_one``, then a trim, then a resumed second night."""
    root = _copy(base, "resumed")
    later = _chains_lines(
        root, lambda ref: ref.day > scenario.night_one or ref.day in scenario.unsealed
    )
    for day in scenario.unsealed:
        _segment(FixtureLake(root), "SPY", day, CARRIED_OCC)
    first = _run(root)
    states = first.states
    _edit(root, scenario)
    _restore(root, later)
    for day in scenario.unsealed:
        shutil.rmtree(LakePaths(root).segment_dir(CHAINS, "SPY", day))
    if trim:
        _trim(root, states)
    passed = tuple(drop(state) for state in states) if drop else states
    second = _run(root, night=SECOND_NIGHT, resume=passed)
    cutoffs = {state.ticker: state.cutoff for state in states}
    return {
        "ledger": _ledger(root),
        "mappings": sorted(_mappings(root)),
        **_answers([(first, cutoffs), (second, None)]),
    }


@pytest.mark.parametrize("trim", [True, False], ids=["trimmed", "untrimmed"])
@pytest.mark.parametrize("name", list(SCENARIOS))
def test_a_resumed_walk_answers_what_a_walk_from_the_start_answers(
    fixture_lake: FixtureLake, name: str, trim: bool
):
    """Ledger lines, OCC mappings, held findings, marks and skips all match.

    Untrimmed, the days at or before the cutoff are still on disk, and the resumed walk still
    does not revisit them.
    """
    scenario = SCENARIOS[name]
    base = _base(fixture_lake, scenario)

    assert _resumed(base, scenario, trim=trim) == _from_the_start(base, scenario)


@pytest.mark.parametrize("dropped", list(DROPS))
def test_a_state_missing_any_one_variable_answers_differently(
    fixture_lake: FixtureLake, dropped: str
):
    """Each of the five variables is needed, and the previous session is needed whole."""
    exposed = EXPOSED_BY.get(dropped, dropped)
    (scenario,) = [s for s in SCENARIOS.values() if s.exposes == exposed]
    base = _base(fixture_lake, scenario)

    whole = _from_the_start(base, scenario)

    assert _resumed(base, scenario) == whole, "the full state must match before a drop means"
    assert _resumed(base, scenario, drop=DROPS[dropped]) != whole


def test_each_scenario_shows_what_it_says(fixture_lake: FixtureLake):
    """What each lake does on a walk from the start, so no scenario matches by doing nothing."""
    found = {}
    for name, scenario in SCENARIOS.items():
        lake = FixtureLake(fixture_lake.root.parent / name.replace(" ", "-") / "lake")
        found[name] = _from_the_start(_base(lake, scenario), scenario)

    split = found["a split the day after the cutoff"]["held"]
    assert len(split) == 1 and CHECK_STRIKE_SCALE in split[0]
    assert found["a root returning after the cutoff"]["ledger"] == []
    ((_, valid_from, valid_to), _) = found["a re-symboling two days after the cutoff"]["mappings"]
    assert valid_from == DAY_ONE and valid_to == DAY_FOUR
    for name in (
        "a gap day at the cutoff before a split",
        "an uncaptured session after the cutoff before a split",
    ):
        assert found[name]["ledger"] == []
        assert CHECK_SPLIT_BOUNDARY in found[name]["held"][0]
    assert len(found["a quarantine cleared after the first night"]["ledger"]) == 1
    refused = found["a refused split before the first night ends"]["held"]
    assert len(refused) == 1 and CHECK_SPLIT_CONSISTENCY in refused[0]
    assert found["an unsealed first day before the first night"]["ledger"] == []
    hole = found["a master hole after a thin first day, closed after the first night"]
    ((_, valid_from, valid_to), _) = hole["mappings"]
    assert valid_from == DAY_TWO and valid_to == DAY_FOUR


# -- the cutoff one pass finds ---------------------------------------------------------------


def _one_pass_equals_a_stopped_run(
    base: Path, ticker: str, expected: date | None, **kwargs
) -> WalkState | None:
    """The state one pass reports, checked against a run that met its end at the same day."""
    state = _states(_run(_copy(base, "one-pass"), **kwargs)).get(ticker)
    if expected is None:
        assert state is None
        return None
    assert state is not None and state.cutoff == expected
    stopped = _states(_stopped_through(base, "stopped", expected, **kwargs))[ticker]
    assert state == stopped
    return state


def _days(*days: date, **rows) -> dict:
    return {("SPY", day): rows.get(day.isoformat(), [_row(day)]) for day in days}


def test_a_day_past_the_window_edge_stops_the_cutoff(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE))

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO, edge=DAY_TWO)


def test_without_an_edge_or_any_event_the_cutoff_is_the_last_day(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE))

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_THREE)


REVERSIBLE = {
    "quarantined": REASON_QUARANTINED,
    "absent": REASON_PARTITION_ABSENT,
    "out of scope": REASON_OUT_OF_SCOPE,
    "partial read": REASON_PARTIAL_READ,
}


@pytest.mark.parametrize("reason", list(REVERSIBLE))
def test_a_day_skipped_for_a_reason_that_can_reverse_stops_the_cutoff(
    fixture_lake: FixtureLake, reason: str
):
    """The cutoff is the day before it, so a resume reads it once it can be read."""
    rows = {}
    quarantine = None
    master = None
    if reason == "quarantined":
        quarantine = [{"partition": _partition(DAY_THREE), "verdict": "bad"}]
    if reason == "out of scope":
        master = SecurityMaster(
            [_mapping(1, "SPY", valid_to=DAY_THREE), _mapping(1, "SPY", valid_from=DAY_FOUR)]
        )
    if reason == "partial read":
        rows[DAY_THREE.isoformat()] = [{**_row(DAY_THREE), "schema_version": 2}]
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_TWO, DAY_THREE, DAY_FOUR, **rows),
        quarantine=quarantine,
        master=master,
    )
    if reason == "absent":
        LakePaths(base).chains_partition_path("SPY", DAY_THREE).unlink()

    report_out = _run(_copy(base, "reasons"))

    assert [(skip.day, skip.reason) for skip in report_out.skipped] == [
        (DAY_THREE, REVERSIBLE[reason])
    ]
    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)


@pytest.mark.parametrize("kind", ["gap", "thin"])
def test_a_day_skipped_for_a_reason_that_cannot_reverse_does_not_stop_it(
    fixture_lake: FixtureLake, kind: str
):
    row = _gap_day_row(DAY_THREE) if kind == "gap" else _row(DAY_THREE, suspect=True)
    base = _lake(
        fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE, DAY_FOUR, **{DAY_THREE.isoformat(): [row]})
    )

    state = _one_pass_equals_a_stopped_run(base, "SPY", DAY_FOUR)

    assert state.unread_since == 0 and state.previous.day == DAY_FOUR


def _segment(lake: FixtureLake, ticker: str, day: date, occ: str) -> None:
    lake.with_journal_segment(
        CHAINS,
        ticker,
        day,
        _chains([_row(day, ticker=ticker, occ_symbol=occ)]),
        start_ts="20260916T133000Z",
        pid=4242,
    )


def test_an_unsealed_session_among_the_uncaptured_stops_the_cutoff_before_them(
    fixture_lake: FixtureLake,
):
    """The state is the one before the uncaptured sessions were counted.

    Day two was never captured and day three's rows sit in the journal unsealed. A state taken
    while counting them would carry one or two of them, and a resume from day one counts them
    again.
    """
    _segment(fixture_lake, "SPY", DAY_THREE, CARRIED_OCC)
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_FOUR))

    state = _one_pass_equals_a_stopped_run(base, "SPY", DAY_ONE)

    assert state.unread_since == 0


def test_an_unsealed_session_before_the_first_manifested_day_leaves_no_cutoff(
    fixture_lake: FixtureLake,
):
    """Once day one seals it is SPY's first day, and a resume past day two would never read it."""
    _segment(fixture_lake, "SPY", DAY_ONE, CARRIED_OCC)
    base = _lake(fixture_lake, _days(DAY_TWO, DAY_THREE))

    _one_pass_equals_a_stopped_run(base, "SPY", None)


def test_another_tickers_unsealed_session_before_this_tickers_first_day_does_not_stop_it(
    fixture_lake: FixtureLake,
):
    """The journal is read per ticker before the first day too, so QQQ's day one is QQQ's."""
    _segment(fixture_lake, "QQQ", DAY_ONE, QQQ_OCC)
    base = _lake(fixture_lake, _days(DAY_TWO, DAY_THREE))

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_THREE)


def test_segments_beside_a_sealed_first_day_do_not_stop_it(fixture_lake: FixtureLake):
    """A manifested day is judged by its partition, the first one as much as any later one."""
    _segment(fixture_lake, "SPY", DAY_ONE, CARRIED_OCC)
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO))

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)


def test_journal_entries_that_name_no_date_do_not_stop_the_cutoff(fixture_lake: FixtureLake):
    """The journal also holds the timing directory, and a date it cannot parse names no day."""
    journal = LakePaths(fixture_lake.root).journal_dir
    (journal / "timing").mkdir(parents=True)
    (journal / "date=unreadable").mkdir()
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO))

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)


def test_a_journal_that_cannot_be_listed_leaves_no_cutoff(fixture_lake: FixtureLake):
    """Nothing says no unsealed session sits before the first day, so no day is passed."""
    journal = LakePaths(fixture_lake.root).journal_dir
    journal.mkdir(parents=True)
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO))
    journal.chmod(0)
    try:
        states = _states(_run(base))
    finally:
        journal.chmod(0o755)

    assert "SPY" not in states


def test_an_unsealed_session_before_the_first_read_session_stops_the_cutoff(
    fixture_lake: FixtureLake,
):
    """Day one is thin, so no session has been read when day two's unsealed rows are met.

    Day two is not reported as uncaptured, since no session has been read before it. It still
    stops the cutoff, because once it seals a resume past day three would never read it.
    """
    _segment(fixture_lake, "SPY", DAY_TWO, CARRIED_OCC)
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_THREE, **{DAY_ONE.isoformat(): [_row(DAY_ONE, suspect=True)]}),
    )

    report_out = _run(_copy(base, "skips"))

    assert [(skip.day, skip.reason) for skip in report_out.skipped] == [(DAY_ONE, REASON_THIN)]
    _one_pass_equals_a_stopped_run(base, "SPY", DAY_ONE)


def test_a_session_the_lake_never_captured_does_not_stop_the_cutoff(fixture_lake: FixtureLake):
    """A file compaction would never seal is not a segment, so day three counts as uncaptured."""
    directory = LakePaths(fixture_lake.root).segment_dir(CHAINS, "SPY", DAY_THREE)
    directory.mkdir(parents=True)
    (directory / "stray.arrows").write_bytes(b"")
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_FOUR))

    report_out = _run(_copy(base, "one-pass"))

    assert [skip.reason for skip in report_out.skipped] == [REASON_NOT_SEALED] * 2
    assert _states(report_out)["SPY"].cutoff == DAY_FOUR


def test_a_journal_directory_that_cannot_be_listed_stops_the_cutoff(fixture_lake: FixtureLake):
    """Nothing says the rows are gone, so the cutoff stays before the day rather than past it."""
    directory = LakePaths(fixture_lake.root).segment_dir(CHAINS, "SPY", DAY_THREE)
    directory.mkdir(parents=True)
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_FOUR))
    directory.chmod(0)
    try:
        state = _states(_run(base))["SPY"]
    finally:
        directory.chmod(0o755)

    assert state.cutoff == DAY_ONE


def test_another_tickers_unsealed_session_does_not_stop_this_tickers_cutoff(
    fixture_lake: FixtureLake,
):
    """The journal is read per ticker, so QQQ's unsealed day stops QQQ and not SPY."""
    _segment(fixture_lake, "QQQ", DAY_THREE, QQQ_OCC)
    sessions = {
        **_days(DAY_ONE, DAY_FOUR),
        ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", occ_symbol=QQQ_OCC)],
        ("QQQ", DAY_FOUR): [_row(DAY_FOUR, ticker="QQQ", occ_symbol=QQQ_OCC)],
    }
    base = _lake(fixture_lake, sessions, master=_master(tickers=("QQQ", "SPY")))

    states = _states(_run(_copy(base, "one-pass")))

    assert states["SPY"].cutoff == DAY_FOUR
    assert states["QQQ"].cutoff == DAY_ONE


def test_a_symbol_the_master_cannot_place_stops_the_cutoff(fixture_lake: FixtureLake):
    """A second instrument claiming SPY from day three makes the master ambiguous there."""
    master = _master()
    master.register(
        kind="equity",
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
        valid_from=DAY_THREE,
        ticker="SPY",
    )
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE, DAY_FOUR), master=master)

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)


def test_a_symbol_the_master_never_carried_leaves_no_cutoff(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO), master=_master(tickers=("QQQ",)))

    _one_pass_equals_a_stopped_run(base, "SPY", None)


@pytest.mark.parametrize("by", ["the gate", "the scale guard"])
def test_a_held_split_stops_the_cutoff_before_its_day(fixture_lake: FixtureLake, by: str):
    if by == "the gate":
        rows = [_row(DAY_THREE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_THREE, note="200 SPY")]
        sessions = _days(DAY_ONE, DAY_TWO, DAY_THREE, **{DAY_THREE.isoformat(): rows})
    else:
        sessions = {
            ("SPY", DAY_ONE): _ladder_rows(DAY_ONE, LADDER, 700.0),
            ("SPY", DAY_TWO): _ladder_rows(DAY_TWO, LADDER, 700.0),
            ("SPY", DAY_THREE): _ladder_rows(
                DAY_THREE, tuple(strike / 2 for strike in LADDER), 349.5
            ),
        }
    base = _lake(fixture_lake, sessions)

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)


def test_an_out_of_scope_day_before_the_first_read_session_does_not_stop_it(
    fixture_lake: FixtureLake,
):
    """The live lake's SPY 2026-09-02 predates the master, and would stop SPY's cutoff forever."""
    base = _lake(
        fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE), master=_master(valid_from=DAY_TWO)
    )

    report_out = _run(_copy(base, "one-pass"))

    assert [(skip.day, skip.reason) for skip in report_out.skipped] == [
        (DAY_ONE, REASON_OUT_OF_SCOPE)
    ]
    _one_pass_equals_a_stopped_run(base, "SPY", DAY_THREE)


def test_an_out_of_scope_day_after_a_day_that_resolved_unread_stops_it(
    fixture_lake: FixtureLake,
):
    """Day one resolves and is thin, so day two is out of scope after a day in scope."""
    master = SecurityMaster(
        [_mapping(1, "SPY", valid_to=DAY_TWO), _mapping(1, "SPY", valid_from=DAY_THREE)]
    )
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_TWO, DAY_THREE, **{DAY_ONE.isoformat(): [_row(DAY_ONE, suspect=True)]}),
        master=master,
    )

    _one_pass_equals_a_stopped_run(base, "SPY", DAY_ONE)


def test_a_resume_after_a_day_that_resolved_unread_stops_at_an_out_of_scope_day(
    fixture_lake: FixtureLake,
):
    """The saved state read no session, so only the master says day one resolved.

    SPY's mapping opens on day one itself, the cutoff, which still counts as resolved there.
    """
    master = SecurityMaster(
        [
            _mapping(1, "SPY", valid_from=DAY_ONE, valid_to=DAY_TWO),
            _mapping(1, "SPY", valid_from=DAY_THREE),
        ]
    )
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_TWO, DAY_THREE, **{DAY_ONE.isoformat(): [_row(DAY_ONE, suspect=True)]}),
        master=master,
    )
    (saved,) = _stopped_through(base, "first", DAY_ONE).states
    assert saved.previous is None

    (state,) = _run(_copy(base, "second"), night=SECOND_NIGHT, resume=[saved]).states

    assert state == saved


def test_a_resume_before_the_ticker_ever_resolved_passes_an_out_of_scope_day(
    fixture_lake: FixtureLake,
):
    """SPY resolves only from day three, so days one and two precede its first in-scope day.

    Another ticker resolving before the cutoff says nothing about SPY.
    """
    master = SecurityMaster([_mapping(1, "SPY", valid_from=DAY_THREE), _mapping(2, "QQQ")])
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE), master=master)
    (saved,) = _stopped_through(base, "first", DAY_ONE).states
    assert saved.cutoff == DAY_ONE

    resumed = _states(_run(_copy(base, "second"), night=SECOND_NIGHT, resume=[saved]))["SPY"]
    whole = _states(_run(_copy(base, "whole"), night=SECOND_NIGHT))["SPY"]

    assert resumed == whole
    assert resumed.cutoff == DAY_THREE


def test_an_uncaptured_day_out_of_scope_does_not_stop_it(fixture_lake: FixtureLake):
    """No partition exists there for a master edit to make readable."""
    early, late = date(2026, 9, 15), date(2026, 9, 21)
    master = SecurityMaster(
        [_mapping(1, "SPY", valid_to=date(2026, 9, 16)), _mapping(2, "SPY", valid_from=late)]
    )
    base = _lake(
        fixture_lake,
        {("SPY", early): [_row(early)], ("SPY", late): [_row(late)]},
        master=master,
    )
    calendar = weekday_sessions(date(2026, 9, 14), date(2026, 9, 21))

    report_out = _run(_copy(base, "one-pass"), calendar=calendar)

    assert {skip.reason for skip in report_out.skipped} == {REASON_OUT_OF_SCOPE}
    assert _states(report_out)["SPY"].cutoff == late


def test_an_unsealed_day_out_of_scope_still_stops_it(fixture_lake: FixtureLake):
    """Sealed, its rows become a manifested out-of-scope day, which a master edit can open."""
    early, late = date(2026, 9, 15), date(2026, 9, 21)
    away = date(2026, 9, 17)
    master = SecurityMaster(
        [_mapping(1, "SPY", valid_to=date(2026, 9, 16)), _mapping(2, "SPY", valid_from=late)]
    )
    _segment(fixture_lake, "SPY", away, CARRIED_OCC)
    base = _lake(
        fixture_lake,
        {("SPY", early): [_row(early)], ("SPY", late): [_row(late)]},
        master=master,
    )
    calendar = weekday_sessions(date(2026, 9, 14), date(2026, 9, 21))

    report_out = _run(_copy(base, "one-pass"), calendar=calendar)

    assert (away, REASON_OUT_OF_SCOPE) in [(skip.day, skip.reason) for skip in report_out.skipped]
    assert _states(report_out)["SPY"].cutoff == early


def test_a_retired_ticker_gets_a_cutoff_at_its_last_day(fixture_lake: FixtureLake):
    """QQQ stops being captured after day two while SPY goes on."""
    sessions = {
        **_days(DAY_ONE, DAY_TWO, DAY_THREE),
        ("QQQ", DAY_ONE): [_row(DAY_ONE, ticker="QQQ", occ_symbol=QQQ_OCC)],
        ("QQQ", DAY_TWO): [_row(DAY_TWO, ticker="QQQ", occ_symbol=QQQ_OCC)],
    }
    base = _lake(fixture_lake, sessions, master=_master(tickers=("QQQ", "SPY")))

    state = _one_pass_equals_a_stopped_run(base, "QQQ", DAY_TWO)

    assert state.previous.day == DAY_TWO


def test_a_split_past_the_window_edge_is_still_judged(fixture_lake: FixtureLake):
    """The edge sets the cutoff and nothing else, so a split past it is still filed."""
    rows = [_row(DAY_THREE, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_THREE, note="200 SPY")]
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_TWO, DAY_THREE, DAY_FOUR, **{DAY_THREE.isoformat(): rows}),
    )

    edged = _run(_copy(base, "edged"), edge=DAY_ONE)
    whole = _run(_copy(base, "whole"))

    assert _states(edged)["SPY"].cutoff == DAY_ONE
    assert _answers([(edged, None)]) == _answers([(whole, None)])
    assert len(edged.held) == 1


def test_a_cutoff_after_an_unread_day_keeps_the_count_of_unread_sessions(
    fixture_lake: FixtureLake,
):
    """Day two is a gap day and day three is quarantined, so the state carries one unread."""
    base = _lake(
        fixture_lake,
        _days(
            DAY_ONE, DAY_TWO, DAY_THREE, DAY_FOUR, **{DAY_TWO.isoformat(): [_gap_day_row(DAY_TWO)]}
        ),
        quarantine=[{"partition": _partition(DAY_THREE), "verdict": "bad"}],
    )

    state = _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)

    assert state.unread_since == 1


def test_a_held_split_after_an_uncaptured_session_stops_the_cutoff_before_counting_it(
    fixture_lake: FixtureLake,
):
    """Day three was never captured, so a state counting it would count it again on a resume."""
    rows = [_row(DAY_FOUR, occ_symbol=CARRIED_OCC), _adjusted_row(DAY_FOUR, note="200 SPY")]
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_FOUR, **{DAY_FOUR.isoformat(): rows}))

    state = _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)

    assert state.unread_since == 0


def test_an_ambiguous_day_after_an_uncaptured_session_stops_the_cutoff_before_counting_it(
    fixture_lake: FixtureLake,
):
    master = _master()
    master.register(
        kind="equity",
        capture_start=datetime(2026, 9, 8, 17, 7, tzinfo=UTC),
        valid_from=DAY_FOUR,
        ticker="SPY",
    )
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_FOUR), master=master)

    state = _one_pass_equals_a_stopped_run(base, "SPY", DAY_TWO)

    assert state.unread_since == 0


# -- what a resume carries over --------------------------------------------------------------


def test_a_state_for_a_ticker_the_manifest_no_longer_lists_comes_back_unchanged(
    fixture_lake: FixtureLake,
):
    """A ticker whose days are all trimmed keeps its history for the day it returns."""
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO))
    gone = WalkState(
        ticker="IWM",
        cutoff=DAY_ONE,
        previous=None,
        seen=frozenset({"IWM"}),
        history=((7, "IWM   260918C00200000", DAY_ONE),),
        unread_since=0,
        last_day=DAY_ONE,
    )

    states = _states(_run(_copy(base, "one-pass"), resume=[gone]))

    assert states["IWM"] == gone
    assert states["SPY"].cutoff == DAY_TWO


def test_a_resume_whose_first_day_stops_keeps_the_state_it_started_from(
    fixture_lake: FixtureLake,
):
    base = _lake(
        fixture_lake,
        _days(DAY_ONE, DAY_TWO, DAY_THREE),
        quarantine=[{"partition": _partition(DAY_THREE), "verdict": "bad"}],
    )
    (saved,) = _stopped_through(base, "first", DAY_TWO).states

    (state,) = _run(_copy(base, "second"), night=SECOND_NIGHT, resume=[saved]).states

    assert state == saved


def test_two_saved_states_for_one_ticker_are_refused(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE))
    (saved,) = _run(_copy(base, "first")).states

    with pytest.raises(ValueError, match="two saved states name SPY"):
        _run(_copy(base, "second"), resume=[saved, saved])


def test_a_resume_meeting_an_unknown_symbol_keeps_its_saved_state(fixture_lake: FixtureLake):
    """The master no longer carries SPY, after a session the lake never captured."""
    known = _lake(FixtureLake(fixture_lake.root.parent / "known" / "lake"), _days(DAY_ONE, DAY_TWO))
    (saved,) = _run(_copy(known, "first")).states
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_FOUR), master=_master(tickers=("QQQ",)))

    (state,) = _run(_copy(base, "second"), night=SECOND_NIGHT, resume=[saved]).states

    assert state == saved


def test_a_resumed_state_whose_first_day_stops_before_any_day_is_processed_comes_back_unchanged(
    fixture_lake: FixtureLake,
):
    """The edge stops the resume at its first day, so the run hands back what it was passed."""
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO, DAY_THREE))
    (saved,) = _stopped_through(base, "first", DAY_ONE).states
    blank = replace(saved, last_day=None)

    (state,) = _run(_copy(base, "second"), resume=[blank], edge=DAY_ONE).states

    assert state == blank


def test_states_come_back_in_ticker_order(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE, DAY_TWO))
    (spy,) = _stopped_through(base, "first", DAY_ONE).states
    gone = WalkState(
        ticker="IWM",
        cutoff=DAY_ONE,
        previous=None,
        seen=frozenset(),
        history=(),
        unread_since=0,
        last_day=DAY_ONE,
    )

    report_out = _run(_copy(base, "second"), resume=[spy, gone])

    assert [state.ticker for state in report_out.states] == ["IWM", "SPY"]


def test_two_different_saved_states_for_one_ticker_are_refused(fixture_lake: FixtureLake):
    base = _lake(fixture_lake, _days(DAY_ONE))
    (saved,) = _run(_copy(base, "first")).states

    with pytest.raises(ValueError, match="two saved states name SPY"):
        _run(_copy(base, "second"), resume=[saved, replace(saved, unread_since=1)])
