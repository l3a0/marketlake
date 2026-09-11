"""The close+5 guard: the last chance to record each day's close of record.

Two cycles a session day carry a close tag, and the guard runs five minutes after the
option close to check that both landed. It is per-tag and asymmetric, because the two
closes are not equally recoverable.

An ``option_close`` that never landed can be refetched. Option quotes freeze at the
option close, so a fetch at close+5 still observes the closing marks. The fill is
written as its own segment and carries the close slot in ``snap_ts``, never its fetch
minute, so a reader asking for the close gets the close.

A ``spot_close`` that never landed is unrecoverable by construction. The 16:00 moment
cannot be re-observed at 16:20. A post-close fetch would carry frozen option marks
against an extended-hours underlying, which is the moment-mixing the tag exists to
prevent. So it is recorded as an explicit absent-marker and nothing is fetched.

The five-minute limit is pinned in code rather than in config, because it defines what
an option close means and not how loudly to complain. Past it the fill is refused
outright.

The guard is the sole writer of the ``spot_close`` absent-marker. On a post-close
restart it runs before startup gap marking, so the minutes it owns are already recorded
when the marker walks the day and are not marked a second time.

Which tickers it checks is a rule of its own, because every row it writes names one. A
ticker is checked for a close when the roster still carries it and its ``capture_start``
is at or before that close. Both facts are read when the guard runs, never held from
daemon start. ``run`` says why.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from lake import journal
from lake.security_master import (
    SecurityMaster,
    capture_start_in_market_time,
    is_in_scope,
)
from lake.session import OPTION_CLOSE, SPOT_CLOSE, SessionClock
from lake.tickers import Roster

# The reason on a marker for a close that was never observed. The equity close is one
# moment and it passed unwitnessed, so nothing names a failure to retry.
SPOT_CLOSE_UNOBSERVED = "spot_close_unobserved"

# The reason on a marker for an expiration the intraday chain carried and the close+5
# fill did not. The fill still stands for the series it does hold, so this names the
# shortfall rather than voiding the fill.
OPTION_CLOSE_SERIES_ABSENT = "option_close_series_absent"

_STAMP = "%Y%m%dT%H%M%S%f"


@dataclass(frozen=True)
class GuardOutcome:
    """What the guard did for one session day, and what the nightly report should say.

    Three of the design's rules end in "flags the nightly report", and no report exists
    yet. Everything it would say is here, so the report reads it rather than the guard
    guessing where to write. Until then the daemon prints it.
    """

    day: date
    filled: tuple[str, ...] = ()
    unobserved: tuple[str, ...] = ()
    baseline_less: tuple[str, ...] = ()
    shortfalls: tuple[str, ...] = ()
    refused: tuple[str, ...] = ()
    problems: tuple[str, ...] = ()

    @property
    def reportable(self) -> bool:
        """Whether anything here belongs in the nightly report."""
        return bool(
            self.unobserved
            or self.baseline_less
            or self.shortfalls
            or self.refused
            or self.problems
        )


@dataclass
class _Findings:
    filled: list[str] = field(default_factory=list)
    unobserved: list[str] = field(default_factory=list)
    baseline_less: list[str] = field(default_factory=list)
    shortfalls: list[str] = field(default_factory=list)
    refused: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


class CloseGuard:
    """Checks both close tags landed, fills the recoverable one, marks the other.

    ``fill`` is the injected fetch. It is handed a ticker and the close slot and returns
    the expirations it captured, or ``None`` when it could not fetch. Leaving it unset
    makes the guard marker-only, which is what a test wants and what a daemon with no
    vendor client falls back to.

    ``roster`` and ``master`` are both readers, not values. Between them they answer the
    only question the guard asks before it writes: did this ticker owe a close at this
    moment. Both are read when the guard runs, because onboarding writes both files while
    the daemon runs and a copy from daemon start answers for the wrong moment. See ``run``.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        roster: Callable[[], Roster],
        session_clock: SessionClock,
        master: Callable[[], SecurityMaster | None] | None = None,
        fill=None,
        pid: int | None = None,
    ) -> None:
        self._root = Path(lake_root)
        self._roster = roster
        self._session_clock = session_clock
        self._master = master
        self._fill = fill
        self._pid = os.getpid() if pid is None else pid

    def run(self, day: date) -> GuardOutcome:
        """Check the day's two closes, once, at or after close+5.

        What the guard writes is a claim that a named ticker owed a close and nothing
        observed it. Absence cannot be read off the lake, because a ticker that captured
        nothing looks the same as one that was never owed anything. So the guard asks
        two sources outside the data, one for each end of a ticker's scope.

        ``tickers.yaml`` says whether the ticker is still captured, and it is read here
        rather than held from daemon start. The roster is the only statement of that,
        and a copy hours old answers for the wrong moment. A ticker onboarded mid-session
        owes both of that day's closes and a frozen copy never checks it. A retired one
        owes neither and a frozen copy marks it anyway, on a surface no cycle writes to
        again.

        ``capture_start`` says when the ticker came into scope, and each close is checked
        against its own moment. A ticker onboarded between the two closes owes the option
        close and not the equity close, so one clamp for both would be wrong either way.
        The master is read here for the same reason the roster is. Onboarding writes it
        while the daemon runs, so a copy from daemon start cannot place the one ticker the
        clamp exists for, and the clamp would do nothing for exactly that case. One read
        per run, so every ticker in a run is judged against one master.

        One end has no source. There is no ``capture_end`` epoch, so a ticker retired
        between the equity close and this run loses a marker it did owe. That window is
        twenty minutes on a live daemon and longer on a late restart. The doc names the
        same limit for gap marking, and closing it needs an epoch neither has.
        """
        bounds = self._session_clock.bounds(day)
        found = _Findings()
        master = self._master() if self._master is not None else None
        for entry in self._roster():
            epoch = capture_start_in_market_time(master, entry.ticker, day)
            if epoch is None or is_in_scope(bounds.equity_close, epoch):
                self._check_spot_close(entry.ticker, bounds.equity_close, found)
            if entry.options and (epoch is None or is_in_scope(bounds.option_close, epoch)):
                self._check_option_close(entry.ticker, bounds, found)
        return GuardOutcome(
            day,
            tuple(found.filled),
            tuple(found.unobserved),
            tuple(found.baseline_less),
            tuple(found.shortfalls),
            tuple(found.refused),
            tuple(found.problems),
        )

    # -- the unrecoverable half ------------------------------------------------

    def _check_spot_close(self, ticker: str, slot: datetime, found: _Findings) -> None:
        """Mark an equity close nobody observed. Never fetch one.

        Quotes carry the underlying, so this is asked of the quotes surface. A data row
        under the tag means the cycle landed. A gap row means it ran and failed, which
        is already recorded, so nothing is added.
        """
        data, gaps = journal.close_tag_rows(
            self._root, journal.QUOTES_SURFACE, ticker, slot.date(), SPOT_CLOSE
        )
        if data or gaps:
            return
        try:
            self._marker(journal.QUOTES_SURFACE, ticker, slot, SPOT_CLOSE, SPOT_CLOSE_UNOBSERVED)
        except OSError as exc:
            found.problems.append(f"quotes/{ticker} spot_close: {type(exc).__name__}")
            return
        found.unobserved.append(ticker)

    # -- the recoverable half --------------------------------------------------

    def _check_option_close(self, ticker: str, bounds, found: _Findings) -> None:
        """Refetch an option close whose marks are missing, inside the window.

        The trigger is missing marks rather than a missing cycle. A chain that failed at
        the option close leaves a tagged gap row, and that row records the attempt while
        holding nothing a reader can price against. The close+5 refetch is exactly what
        rescues it, so a failed cycle qualifies the same as one that never ran.
        """
        day = bounds.day
        data, _ = journal.close_tag_rows(
            self._root, journal.CHAINS_SURFACE, ticker, day, OPTION_CLOSE
        )
        if data:
            return
        if self._fill is None:
            found.refused.append(f"{ticker}: no fill fetcher")
            return
        now = self._session_clock.snap_slot()
        if now > bounds.option_close_deadline:
            # Past close+5 the marks are no longer the close's. Refusing is the rule,
            # and it is pinned here rather than in config because it defines what an
            # option close means.
            found.refused.append(f"{ticker}: past close+5")
            return
        try:
            captured = self._fill(ticker, bounds.option_close)
        except Exception as exc:  # noqa: BLE001 - a vendor failure must not stop the guard
            found.problems.append(f"chains/{ticker} option_close: {type(exc).__name__}")
            return
        if captured is None:
            found.refused.append(f"{ticker}: fill fetch returned nothing")
            return
        found.filled.append(ticker)

        baseline = journal.latest_expirations(self._root, ticker)
        if baseline is None:
            # No same-day cycle to compare against. The fill still stands, and the
            # battery is told to judge it rather than the guard voiding it.
            found.baseline_less.append(ticker)
            return
        missing = sorted(set(baseline) - set(captured))
        if missing:
            found.shortfalls.append(f"{ticker}: {len(missing)} expirations")

    # -- the marker write ------------------------------------------------------

    def _marker(
        self, surface: str, ticker: str, slot: datetime, close_tag: str, error_class: str
    ) -> None:
        """One tagged marker row in its own segment, like the option-close fill."""
        stamp = slot.strftime(_STAMP)
        with journal.SegmentWriter.open(
            self._root, surface, ticker, slot.date(), stamp, self._pid
        ) as writer:
            writer.write_cycle(
                journal.gap_batch(
                    surface,
                    ticker=ticker,
                    snap_ts=slot,
                    error_class=error_class,
                    close_tag=close_tag,
                )
            )


__all__ = [
    "OPTION_CLOSE_SERIES_ABSENT",
    "SPOT_CLOSE_UNOBSERVED",
    "CloseGuard",
    "GuardOutcome",
]
