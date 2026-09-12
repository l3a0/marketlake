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
ticker is checked for a close when a capture span covers that close's minute. The guard
reads the capture-spans file, not the roster, so a ticker retired between the equity
close and this run still gets its owed marker: its span still covers 16:00 even though it
has left the roster. The span also carries whether options were captured, so the guard
knows whether the option close was owed. The master turns each span's ``instrument_id``
back into the ticker symbol that names the row. Both are read when the guard runs, never
held from daemon start. ``run`` says why.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from lake import journal
from lake.capture_spans import CaptureSpans
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.security_master import SecurityMaster
from lake.session import OPTION_CLOSE, SPOT_CLOSE, SessionClock

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

    ``spans`` and ``master`` are both readers, not values. Between them they answer the
    only question the guard asks before it writes: did this ticker owe a close at this
    moment. The spans file says which instruments were in scope at a close's minute, and
    the master turns an ``instrument_id`` into the ticker symbol. Both are read when the
    guard runs, because onboarding and retiring write these files while the daemon runs
    and a copy from daemon start answers for the wrong moment. See ``run``.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        spans: Callable[[], CaptureSpans | None],
        session_clock: SessionClock,
        master: Callable[[], SecurityMaster | None],
        fill=None,
        pid: int | None = None,
    ) -> None:
        self._root = Path(lake_root)
        self._spans = spans
        self._session_clock = session_clock
        self._master = master
        self._fill = fill
        self._pid = os.getpid() if pid is None else pid

    def run(self, day: date) -> GuardOutcome:
        """Check the day's two closes, once, at or after close+5.

        What the guard writes is a claim that a named ticker owed a close and nothing
        observed it. Absence cannot be read off the lake, because a ticker that captured
        nothing looks the same as one that was never owed anything. So the guard reads a
        source outside the data: the capture spans.

        Each close is checked against its own minute. A ticker owes the equity close when
        a span covers 16:00, and the option close when a span covers 16:15 and captured
        options. The two are separate because a ticker onboarded between the closes owes
        only the later one, and a ticker retired between them owes only the earlier one.
        A span covering 16:00 but ending before 16:15 is exactly the retired-at-16:02
        case, and it still gets its spot-close marker because the guard reads spans rather
        than the live roster.

        The spans and the master are read here, not held from daemon start, because
        onboarding and retiring write them while the daemon runs. A copy from daemon start
        would answer for the wrong moment: it would miss a ticker onboarded mid-session
        and mark one retired mid-session. One read per run, so every ticker in a run is
        judged against one snapshot.

        Nothing here raises. The guard runs from a hook ``run_loop`` does not wrap, so a
        raise would exit the process, and under ``KeepAlive`` the successor would reach
        the same minute and raise again. Failures resolve into ``problems`` at one of two
        grains. A prologue failure stops the run, because those reads decide who is owed a
        marker and which days are already sealed. A per-ticker failure costs that ticker
        alone and the run carries on, because the rest still owe their markers. The
        dispatcher wraps this call too, for whatever a later edit adds that neither grain
        foresees.
        """
        bounds = self._session_clock.bounds(day)
        found = _Findings()
        master = self._master() if self._master is not None else None
        spans = self._spans() if self._spans is not None else None
        try:
            # Read once for the run, like the spans and the master above, so every ticker
            # is judged against one snapshot of what compaction has already sealed, and
            # against one snapshot of who was in scope at each close.
            sealed = latest_entries(self._root)
            spot_owed = self._covering(spans, master, bounds.equity_close, day)
            option_owed = self._covering(spans, master, bounds.option_close, day)
        except Exception as exc:  # noqa: BLE001 - the run stops, the daemon does not
            # The prologue answers two questions the run cannot proceed without: which
            # ticker-days are already sealed, and who owed each close. A failure here is
            # answered by writing nothing, never by widening. An empty ledger would call
            # every partition unsealed, which is how this writer makes a false claim
            # about a day compaction already sealed and the next run deletes as debris.
            # An empty scope would write no marker anyway, so both failures resolve the
            # same way: say what broke and write nothing.
            #
            # What that costs is named in #102 rather than hidden here. The startup walk
            # does not pick the day up afterwards, because compaction seals it ten minutes
            # later and the walk skips a sealed date. So the minute this run owed stays a
            # hole with no row naming it. That is a smaller loss than the session's
            # capture, which is what the alternative costs, and it is still a loss.
            found.problems.append(f"prologue: {type(exc).__name__}: {exc}")
            return GuardOutcome(day, problems=tuple(found.problems))
        for ticker, _ in spot_owed:
            # Per ticker, because one unreadable file is one ticker's loss and not the
            # run's. Marking is the record completeness is counted from, so a drifted
            # segment under the third ticker must not cost the fourth its marker.
            try:
                if self._is_sealed(sealed, journal.QUOTES_SURFACE, ticker, day):
                    continue
                self._check_spot_close(ticker, bounds.equity_close, found)
            except Exception as exc:  # noqa: BLE001 - one ticker, not the run
                found.problems.append(f"quotes/{ticker}: {type(exc).__name__}: {exc}")
        for ticker, options in option_owed:
            if not options:
                continue
            try:
                if self._is_sealed(sealed, journal.CHAINS_SURFACE, ticker, day):
                    continue
                self._check_option_close(ticker, bounds, found)
            except Exception as exc:  # noqa: BLE001 - one ticker, not the run
                found.problems.append(f"chains/{ticker}: {type(exc).__name__}: {exc}")
        return GuardOutcome(
            day,
            tuple(found.filled),
            tuple(found.unobserved),
            tuple(found.baseline_less),
            tuple(found.shortfalls),
            tuple(found.refused),
            tuple(found.problems),
        )

    def _is_sealed(self, sealed: dict, surface: str, ticker: str, day: date) -> bool:
        """Whether compaction has already sealed this ticker-day's partition.

        A sealed day is one this guard must say nothing about. Compaction unlinks a day's
        segments once its partition is manifested, so ``close_tag_rows`` reads the empty
        directory and reports a close nobody observed, for a close that was captured and
        is sitting in the partition. The marker that follows is a false claim, and the
        next run deletes it as debris, so a row a live writer wrote is silently dropped.

        Gap-marking learned this first and skips a sealed date for the same reason. The
        guard's own window closes before compaction's opens, so a sealed day is always a
        day this guard's work is finished with. Only a restart after the seal reaches
        here at all.
        """
        partition = LakePaths(self._root).partition_path(surface, ticker, day)
        return partition.relative_to(self._root).as_posix() in sealed

    def _covering(
        self,
        spans: CaptureSpans | None,
        master: SecurityMaster | None,
        instant: datetime,
        day: date,
    ) -> list[tuple[str, bool]]:
        """The (ticker, options) pairs whose capture span covers ``instant``.

        Returns nothing when the spans file or the master is missing, which widens to
        checking no ticker rather than raising. That is the safe direction on the daemon's
        unguarded hooks: a missing source records nothing rather than a false marker.
        """
        if spans is None or master is None:
            return []
        out: list[tuple[str, bool]] = []
        for span in spans.spans_covering(instant):
            ticker = master.symbol_at(span.instrument_id, day)
            if ticker is not None:
                out.append((ticker, span.options))
        return out

    # -- the unrecoverable half ------------------------------------------------

    def _check_spot_close(self, ticker: str, slot: datetime, found: _Findings) -> None:
        """Mark an equity close nobody observed. Never fetch one.

        Quotes carry the underlying, so this is asked of the quotes surface. A data row
        under the tag means the cycle landed. A gap row means it ran and failed, which
        is already recorded, so nothing is added.
        """
        rows = journal.close_tag_rows(
            self._root, journal.QUOTES_SURFACE, ticker, slot.date(), SPOT_CLOSE
        )
        if rows.data or rows.gaps:
            return
        if rows.unreadable:
            # The marker this would write is a claim that nothing observed the close. A
            # segment that will not read might hold the very row that refutes it, and the
            # next run deletes a false marker as debris, so a row a live writer wrote goes
            # with it. Saying so and writing nothing is the honest answer.
            #
            # This withholds a marker for a minute nothing else records either, because
            # the startup walk refuses the same pair. The day then reads short with no row
            # naming why, which is the loss #102 tracks. It is the right side to err on
            # only because the alternative is a false claim sealed into the record.
            found.problems.append(f"quotes/{ticker}: {len(rows.unreadable)} unreadable")
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
        rows = journal.close_tag_rows(self._root, journal.CHAINS_SURFACE, ticker, day, OPTION_CLOSE)
        if rows.data:
            return
        if rows.unreadable:
            # Named, and then the fill runs anyway. The two closes are not symmetric and
            # this is where that bites. Withholding the equity close's marker withholds a
            # claim, which is cheap to be wrong about. Withholding the option close's fill
            # withholds the sample, and the window shuts five minutes later, so being
            # wrong here is permanent.
            #
            # The risk taken instead is a duplicate. If the file that will not read does
            # hold the close, the fill writes a second close-tagged segment beside it and
            # compaction merges both. That is a row count the battery can see and argue
            # with. A close nobody fetched is a row that does not exist and cannot be
            # bought back, which is the loss the whole lake is built to avoid.
            #
            # A zero-byte segment is the case that decides it. ``SegmentWriter`` creates
            # the file and fsyncs its directory entry before any schema bytes land, so a
            # process killed in between leaves one durably empty. That is precisely what
            # the crash loop this issue fixes used to produce, and such a file holds
            # nothing to duplicate.
            found.problems.append(f"chains/{ticker}: {len(rows.unreadable)} unreadable")
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
