"""The lake window: its floor, how a stored value is judged, and the edge it sets.

marketlake #786 adds ``lake_window_sessions``, the number of recent sessions a host keeps on its
lake volume, and marketlake #953 adds a key per surface beside it. These are the value-only rules
behind them. The render, the sweep and the trim that apply them are tested in
``tests/component``. Every key name and every new message is written out as a literal rather
than read from the code, because every per-surface key name contains ``lake_window_sessions``,
so a check against the code's constant would pass a message naming the wrong key.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from lake.config import GuardConstants
from lake.window import (
    EDGE_SEARCH_DAYS,
    KEEP_EVERY_SESSION,
    WINDOW_KEY,
    EdgeNotFound,
    WindowRefused,
    edge_search_days,
    most_sessions_in,
    surface_windows,
    window_edge,
    window_floor,
    window_sessions,
)
from tests.support.calendar import weekday_sessions

MONDAY = date(2026, 9, 14)
FRIDAY = date(2026, 9, 18)
SATURDAY = date(2026, 9, 19)
THREE_WEEKS = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7), MONDAY)


# -- the floor -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("days", "sessions"),
    [(1, 1), (5, 5), (6, 5), (7, 5), (8, 6), (14, 10), (29, 21), (30, 22), (31, 23)],
)
def test_a_span_of_calendar_days_holds_at_most_its_weekdays(days: int, sessions: int):
    """Five sessions a whole week, and the days left over count until they reach five."""
    assert most_sessions_in(days) == sessions


def test_the_default_floor_is_22_sessions():
    """The History panel's 30 calendar days hold at most 22 sessions, which binds today."""
    assert window_floor(GuardConstants()) == 22


@pytest.mark.parametrize("trailing", [21, 30, 45])
def test_the_floor_follows_a_trailing_median_override(trailing: int):
    """A ``guards:`` override that widens the battery's trailing median raises the floor.

    The battery reads ``trailing_median_sessions`` sessions before the judged day, so it needs
    that many plus the day itself. Today's 20 sits under the History panel's 22, so a test that
    read today's value could not tell a derived floor from a literal one.
    """
    guards = GuardConstants(trailing_median_sessions=trailing)

    assert window_floor(guards) == trailing + 1


def test_a_narrower_trailing_median_does_not_lower_the_floor():
    """The History panel and the runway still need their 22 sessions."""
    assert window_floor(GuardConstants(trailing_median_sessions=5)) == 22


# -- judging a stored value ------------------------------------------------------------------


def test_an_absent_key_is_no_window():
    assert window_sessions(None, GuardConstants()) is None


@pytest.mark.parametrize("value", [22, 23, 40])
def test_a_whole_number_at_or_over_the_floor_is_the_window(value: int):
    assert window_sessions(value, GuardConstants()) == value


def test_a_value_under_the_floor_is_refused_naming_the_key_and_the_floor():
    with pytest.raises(WindowRefused) as raised:
        window_sessions(21, GuardConstants())

    message = str(raised.value)
    assert WINDOW_KEY in message
    assert "22 sessions" in message
    # The sweep files this in its report, and ``report.redacted`` keeps a line only up to its
    # second ``": "``.
    assert ": " not in message


@pytest.mark.parametrize("value", ["'22'", "True", "None", "22.0", "[22]"])
def test_a_stored_value_that_is_not_an_int_is_refused(value: str):
    """``Config`` stores any value but an ``int`` as its ``repr``, and none of them is a window."""
    with pytest.raises(WindowRefused, match="not a whole number"):
        window_sessions(value, GuardConstants())


def test_a_bool_is_refused_although_python_counts_it_an_int():
    """Refused as not a number, rather than as a window of one session under the floor."""
    with pytest.raises(WindowRefused, match="not a whole number"):
        window_sessions(True, GuardConstants())


def test_the_floor_judges_against_the_guards_it_is_given():
    """A value fine under the default floor is refused under a raised one."""
    with pytest.raises(WindowRefused, match="31 sessions"):
        window_sessions(22, GuardConstants(trailing_median_sessions=30))


# -- the edge --------------------------------------------------------------------------------


def test_the_edge_counts_tonight_as_the_first_session():
    """With tonight first, a window of five ending on a Friday starts on its Monday.

    Counting from the session before tonight would put the edge a week back, and after the
    next trim and seal the lake would hold one session more than the window.
    """
    assert window_edge(THREE_WEEKS, FRIDAY, 5) == MONDAY


def test_a_window_of_one_is_tonight():
    assert window_edge(THREE_WEEKS, FRIDAY, 1) == FRIDAY


def test_a_session_the_lake_never_captured_still_takes_a_slot():
    """The count is the calendar's, whatever the lake holds, as ``trailing_medians`` counts."""
    assert window_edge(THREE_WEEKS, FRIDAY, 6) == date(2026, 9, 11)


def test_a_holiday_takes_no_slot():
    calendar = weekday_sessions(date(2026, 9, 7), MONDAY, holidays=(date(2026, 9, 7),))

    assert window_edge(calendar, FRIDAY, 9) == date(2026, 9, 8)


def test_a_night_that_is_not_a_session_takes_no_slot():
    assert window_edge(THREE_WEEKS, SATURDAY, 1) == FRIDAY


class _CountingCalendar:
    """A calendar that answers no session, and fails a test that asks it too often."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.asked = 0

    def is_session(self, day: date) -> bool:
        self.asked += 1
        assert self.asked <= self.limit, "the edge kept counting past its stop"
        return False


def test_the_edge_stops_counting_rather_than_looping_on_a_calendar_with_no_session():
    """A calendar that answers no session refuses the edge instead of hanging the sweep."""
    calendar = _CountingCalendar(limit=400)

    with pytest.raises(EdgeNotFound, match="answers no session"):
        window_edge(calendar, FRIDAY, 22)

    # A literal year rather than ``EDGE_SEARCH_DAYS``, so a shorter stop cannot move the
    # expectation with it.
    assert calendar.asked == 366


def test_the_stop_reaches_back_a_year():
    """A session a year back is still found, so the stop is not too short.

    Written with a literal 365 days, so a stop shortened in the code cannot move the fixture
    with it.
    """
    far = FRIDAY - timedelta(days=365)

    class _OneSession:
        def is_session(self, day: date) -> bool:
            return day in (FRIDAY, far)

    assert window_edge(_OneSession(), FRIDAY, 2) == far


# -- review fixes ----------------------------------------------------------------------------


def test_the_history_width_matches_the_dashboards():
    """``lake.window`` restates the History panel's width so the sweep need not import DuckDB.

    The two must not drift, or the floor would protect a panel of a different width.
    """
    from lake import dashboard, window

    assert window.HISTORY_WINDOW_DAYS == dashboard.HISTORY_WINDOW_DAYS


def test_the_floor_keeps_more_nightly_files_than_the_history_panel_reads():
    """A rebuilt lake keeps one nightly report per night after its newest trimmed day.

    Marketlake #796. The History panel reads the last ``HISTORY_REPORTS`` nightly files and
    needs one older still, or it calls the oldest the first the sweep ever filed. The floor is
    the fewest sessions a trimmed lake keeps, so it has to exceed that count.
    """
    from lake import dashboard, window

    assert window.window_floor(GuardConstants()) > dashboard.HISTORY_REPORTS


def test_the_window_module_does_not_import_the_dashboard():
    """The render and the sweep load ``lake.window``, and DuckDB is about 17.5 MiB of import."""
    import subprocess
    import sys

    probe = (
        "import sys, lake.window, lake.config; "
        "from lake.window import window_floor; "
        "window_floor(lake.config.GuardConstants()); "
        "print('lake.dashboard' in sys.modules, 'duckdb' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout

    assert out.split() == ["False", "False"]


def test_a_year_long_window_finds_its_edge_on_the_real_calendar():
    """260 sessions span more than 366 days, so a fixed one-year search would always refuse."""
    from lake.calendar import ExchangeCalendar

    tonight = date(2026, 10, 8)

    edge = window_edge(ExchangeCalendar(), tonight, 260)

    assert tonight - edge > timedelta(days=EDGE_SEARCH_DAYS)
    assert edge_search_days(260) > EDGE_SEARCH_DAYS


def test_a_window_longer_than_the_search_says_so():
    """Too few sessions found is a window the search cannot reach, not an empty calendar."""
    with pytest.raises(EdgeNotFound, match="only 15 of 22 sessions") as raised:
        window_edge(THREE_WEEKS, FRIDAY, 22)

    assert "answers no session" not in str(raised.value)


def test_a_window_of_no_session_is_refused():
    with pytest.raises(ValueError):
        window_edge(THREE_WEEKS, FRIDAY, 0)


@pytest.mark.parametrize("span", ["HISTORY_WINDOW_DAYS", "GROWTH_WINDOW_DAYS"])
def test_the_floor_reads_each_calendar_span(monkeypatch, span: str):
    """Widen either span to 60 days and the floor follows, to the 44 weekdays it holds."""
    monkeypatch.setattr(f"lake.window.{span}", 60)

    assert window_floor(GuardConstants()) == 44


def test_a_window_reaching_past_the_real_calendars_first_session_is_refused():
    """``exchange_calendars`` refuses a day before its first session with a ``ValueError``.

    That session lies 20 years before today and moves daily, so no fixed date names it.

    6,000 sessions reach back further than that. Escaping as ``DateOutOfBounds`` would cost the
    sweep its report, digest and ping, so the count refuses as ``EdgeNotFound`` instead.
    """
    from lake.calendar import ExchangeCalendar

    with pytest.raises(EdgeNotFound, match="reaches back past it"):
        window_edge(ExchangeCalendar(), date(2026, 10, 8), 6000)


def test_a_huge_window_on_a_calendar_with_no_session_stops_at_the_first_date():
    """The search outruns ``date.min`` before its own bound, and stepping past it overflows."""
    with pytest.raises(EdgeNotFound, match="first day a date can name"):
        window_edge(_CountingCalendar(limit=10**7), FRIDAY, 300_000)


# -- a window per surface, marketlake #953 ------------------------------------------------------

GUARDS = GuardConstants()


def _floor_message(key: str) -> str:
    """Today's floor message, which chains and quotes keep with their own key in it."""
    return (
        f"{key} is under the floor of 22 sessions that the readers of sealed chains and quotes "
        "need, which are the battery's trailing median, the History panel and the disk runway. "
        "Raise it in config/vm.yaml"
    )


def _keeps_message(key: str) -> str:
    return (
        f"{key} keeps every session, which only lake_window_sessions_bars and "
        "lake_window_sessions_quotes may, because the split checkpoint is counted from the "
        "chains window. Set a whole number in config/vm.yaml"
    )


BARS_FLOOR = (
    "lake_window_sessions_bars is under the floor of 22 sessions that the chains and quotes "
    "windows share. Raise it in config/vm.yaml"
)
BARS_NOT_A_NUMBER = "lake_window_sessions_bars is not a whole number of sessions or all"


def _refusal(value: object, key: str) -> str:
    with pytest.raises(WindowRefused) as raised:
        window_sessions(value, GUARDS, key=key)
    message = str(raised.value)
    # ``report.redacted`` keeps a sweep line only up to its second ``": "``.
    assert ": " not in message
    return message


@pytest.mark.parametrize("key", ["lake_window_sessions_bars", "lake_window_sessions_quotes"])
def test_all_on_the_bars_or_quotes_key_keeps_every_session(key: str):
    """``config._window_value`` stores ``all`` as its ``repr``, which is what the judge reads."""
    assert window_sessions("'all'", GUARDS, key=key) is KEEP_EVERY_SESSION


@pytest.mark.parametrize("key", ["lake_window_sessions", "lake_window_sessions_chains"])
def test_all_on_the_chains_key_or_the_base_key_is_refused(key: str):
    """Refusal 11.2 on #953. A chains window of every session counts no checkpoint edge."""
    assert _refusal("'all'", key) == _keeps_message(key)


@pytest.mark.parametrize("value", ["'ALL'", "'All'", "None", "'22'", "True", "[22]"])
@pytest.mark.parametrize("key", ["lake_window_sessions_bars", "lake_window_sessions_quotes"])
def test_a_bars_or_quotes_value_that_is_neither_a_number_nor_all_is_refused(key: str, value: str):
    """Refusal 11.1. Only lower-case ``all`` keeps every session, and a blank key stores
    ``"None"``. The message says ``or all``, which a prefix check could not see go missing."""
    assert _refusal(value, key) == f"{key} is not a whole number of sessions or all"


@pytest.mark.parametrize("key", ["lake_window_sessions", "lake_window_sessions_chains"])
def test_a_chains_value_that_is_not_a_number_keeps_todays_text_with_its_key(key: str):
    assert _refusal("'many'", key) == f"{key} is not a whole number of sessions"


@pytest.mark.parametrize("key", ["lake_window_sessions_chains", "lake_window_sessions_quotes"])
def test_a_chains_or_quotes_window_under_the_floor_names_its_own_key(key: str):
    assert _refusal(21, key) == _floor_message(key)


def test_a_bars_window_under_the_floor_does_not_claim_a_reader_needs_it():
    """Refusal 11.3. No reader of sealed bars needs the floor, so the message says the floor is
    the one the chains and quotes windows share."""
    assert _refusal(21, "lake_window_sessions_bars") == BARS_FLOOR


@pytest.mark.parametrize("key", ["lake_window_sessions_bars", "lake_window_sessions_quotes"])
def test_a_number_on_a_per_surface_key_is_its_window(key: str):
    assert window_sessions(25, GUARDS, key=key) == 25


def test_the_base_floor_message_is_unchanged():
    """A host with ``lake_window_sessions`` alone files the line it filed before #953."""
    assert _refusal(21, "lake_window_sessions") == _floor_message("lake_window_sessions")


# -- inheritance -------------------------------------------------------------------------------


def _stored(**keys: object) -> dict[str, object]:
    """The four stored values, each absent unless named, keyed by the literal key names."""
    names = {
        "base": "lake_window_sessions",
        "chains": "lake_window_sessions_chains",
        "quotes": "lake_window_sessions_quotes",
        "bars": "lake_window_sessions_bars",
    }
    return {names[name]: value for name, value in keys.items()}


@pytest.mark.parametrize(
    "keys",
    [{}, {"bars": "'all'"}, {"bars": "'ALL'"}, {"chains": 25, "quotes": 3}],
    ids=["nothing", "bars-all", "bars-ALL", "bad-and-good"],
)
def test_without_the_base_key_nothing_is_judged_and_nothing_trims(keys):
    """Only ``lake_window_sessions`` turns trimming on, so a per-surface key alone, even one the
    judge would refuse, is no window and files nothing. The render refuses it instead."""
    assert surface_windows(_stored(**keys), GUARDS) is None


def test_the_base_key_alone_sets_every_surface():
    resolved = surface_windows(_stored(base=22), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22, "bars": 22}
    assert resolved.refused == {}
    assert resolved.run_refusal is None


def test_an_absent_key_inherits_the_base_key_never_the_chains_key():
    """Mutation this catches: an absent quotes or bars key taking the chains window."""
    resolved = surface_windows(_stored(base=22, chains=25), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 25, "quotes": 22, "bars": 22}
    assert resolved.refused == {}
    assert resolved.run_refusal is None
    assert resolved.sources == {
        "chains": "lake_window_sessions_chains",
        "quotes": "lake_window_sessions",
        "bars": "lake_window_sessions",
    }


def test_bars_kept_is_none_beside_numeric_windows():
    """The VM's own setting, ``lake_window_sessions: 22`` and ``lake_window_sessions_bars: all``."""
    resolved = surface_windows(_stored(base=22, bars="'all'"), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22, "bars": None}
    assert resolved.refused == {}
    assert resolved.run_refusal is None


def test_a_refused_bars_key_refuses_bars_alone():
    """A refused surface is absent from ``windows``, so ``get`` cannot pass it off as kept."""
    resolved = surface_windows(_stored(base=22, bars="'ALL'"), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22}
    assert resolved.refused == {"lake_window_sessions_bars": BARS_NOT_A_NUMBER}
    assert resolved.run_refusal is None


def test_a_blank_key_is_present_and_does_not_inherit():
    """A key written with no value stores ``"None"``. It is present, so it is refused rather than
    given ``lake_window_sessions``'s window."""
    resolved = surface_windows(_stored(base=22, bars="None"), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22}
    assert resolved.refused == {"lake_window_sessions_bars": BARS_NOT_A_NUMBER}
    assert resolved.run_refusal is None


def test_two_refused_keys_are_two_entries_in_key_order():
    resolved = surface_windows(_stored(base=22, bars="'All'", quotes=21), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22}
    assert resolved.refused == {
        "lake_window_sessions_quotes": _floor_message("lake_window_sessions_quotes"),
        "lake_window_sessions_bars": BARS_NOT_A_NUMBER,
    }
    assert list(resolved.refused) == ["lake_window_sessions_quotes", "lake_window_sessions_bars"]
    assert resolved.run_refusal is None


def test_a_refused_base_key_is_one_entry_and_refuses_the_run():
    """An absent key inherits the base key's verdict and adds no entry, so one bad base key is
    one line in the sweep's report."""
    resolved = surface_windows(_stored(base=21), GUARDS)

    assert resolved is not None
    assert resolved.windows == {}
    assert resolved.refused == {"lake_window_sessions": _floor_message("lake_window_sessions")}
    assert resolved.run_refusal == _floor_message("lake_window_sessions")


def test_a_refused_base_key_refuses_the_run_beside_every_good_surface_key():
    resolved = surface_windows(_stored(base=21, chains=22, quotes=22, bars="'all'"), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"chains": 22, "quotes": 22, "bars": None}
    assert resolved.refused == {"lake_window_sessions": _floor_message("lake_window_sessions")}
    assert resolved.run_refusal == _floor_message("lake_window_sessions")


def test_a_refused_chains_key_refuses_the_run():
    resolved = surface_windows(_stored(base=22, chains="'all'"), GUARDS)

    assert resolved is not None
    assert resolved.windows == {"quotes": 22, "bars": 22}
    assert resolved.refused == {
        "lake_window_sessions_chains": _keeps_message("lake_window_sessions_chains")
    }
    assert resolved.run_refusal == _keeps_message("lake_window_sessions_chains")


def test_the_run_refusal_names_the_base_key_first():
    resolved = surface_windows(_stored(base="'all'", chains=21), GUARDS)

    assert resolved is not None
    assert resolved.windows == {}
    assert resolved.refused == {
        "lake_window_sessions": _keeps_message("lake_window_sessions"),
        "lake_window_sessions_chains": _floor_message("lake_window_sessions_chains"),
    }
    assert resolved.run_refusal == _keeps_message("lake_window_sessions")


def test_a_per_surface_key_is_judged_by_the_merged_guards():
    """A guard that raises the floor to 31 refuses a quotes window of 30, which the default floor
    of 22 would pass."""
    guards = GuardConstants(trailing_median_sessions=30)
    resolved = surface_windows(_stored(base=31, quotes=30), guards)

    assert resolved is not None
    assert resolved.windows == {"chains": 31, "bars": 31}
    assert resolved.refused == {
        "lake_window_sessions_quotes": _floor_message("lake_window_sessions_quotes").replace(
            "floor of 22", "floor of 31"
        )
    }
    assert resolved.run_refusal is None
