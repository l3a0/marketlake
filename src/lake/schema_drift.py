"""The parser's schema-drift page: a vendor retype, seen the minute it arrives.

The design's schema policy says a retyped known field pages, since it can zero a
downstream surface. This is the producer for that sentence.

The evidence is the one the parser already writes. When a vendor sends a known field at a
type its column refuses, ``journal._routed_column`` nulls the column for that row and
parks the raw value in ``extra`` under the vendor's own name for it. A known field's name
in the overflow can arrive no other way, so its presence is the drift signature, and
``journal.routed_columns`` reads it back off the built batch.

This is not the page compaction sends. Compaction compares a ticker-day's merged segment
schema against the pinned one and catches a rotation this project's own release caused,
at night. This one is the vendor's payload changing shape, seen the minute the response is
parsed. Different producer, different evidence, and hours earlier.

Two rules keep one vendor change to one page, and both are arithmetic rather than taste.

1. **Once when the drift starts, not once per cycle it persists.** The model is the
   watchdog's own once-on-transition rule. Capture runs a cycle a minute against
   ``alert.DEFAULT_DAILY_CAP`` of forty pages a day, so a drift that persists would spend
   the whole cap in forty minutes, and the page it swallowed could be the auth-death page.
   A column that stops routing re-arms, so a vendor that drifts, is fixed, and drifts
   again pages twice.
2. **One page naming every column that moved, not one per column or per ticker.** The
   model is ``compact._page_drift``, and the reasoning carries over exactly. A vendor
   retype reaches every ticker on the same cycle, so paging per finding would scale the
   page count with the roster while the fact stayed one fact. The page carries how many
   tickers each column drifted on, and the tickers themselves go to stderr.

Not every observation is a whole cycle, which is where that second rule needs care. The
close+5 fill writes one ticker's segment through ``capture.journal_snapshot``, outside the
loop, and it carries the same signature. ``observe_partial`` is its way in. The fold still
holds, because one vendor fact is still one page. What does not hold is the ticker count,
since a writer that read one ticker can only ever report one, so a partial finding prints
no count and the page says which ticker was read instead.

An onboarding snapshot writes that same shape of segment and never arrives here at all. It
runs in its own process with no alarm behind it, so its finding rides its sign-off report,
at ``onboard.OnboardReport``.

Evidence comes from data segments alone, and it is counted per ticker rather than per
surface. A gap segment carries no vendor observation, so a ticker that gapped says nothing
about the payload's shape and a column it was drifting stays drifting until that same
ticker lands a clean data row.

Counting the evidence per surface instead is the shape that fails, and it fails quietly.
One ticker retyped against a roster that is otherwise healthy means the surface produces
data every cycle, so the surface looks like evidence while the only ticker that could
speak is the one that gapped. Every ordinary transient gap on that ticker then reads as
the drift clearing, and its return reads as a new drift. A ticker flapping against an
unchanging vendor fact pages every other minute, which spends the forty-a-day cap before
11:00 and swallows whatever the watchdog owed after that.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from lake.alert import REFUSED, Message, Publisher
from lake.capture import CycleResult
from lake.journal import ROW_KIND_DATA

# The event and title on the parser's schema-drift page. ``compact.SCHEMA_DRIFT_EVENT`` is
# ``compaction_schema_drift`` and ``battery_drift.SCHEMA_DRIFT_EVENT`` is
# ``battery_schema_drift``, so the convention is the producer's name in front of the
# condition, and a reader of ``reports/alerts/`` can tell them apart without opening a
# file. The title says where the drift was seen, which is the other half of the same
# distinction: two events that read alike on a phone would be worse than one.
SCHEMA_DRIFT_EVENT = "parser_schema_drift"
SCHEMA_DRIFT_TITLE = "Schema drift in the vendor payload"

# How many column names the page prints per surface before it stops and says how many are
# left. The bound is the design's own, which pins every page body at plain text under 1,000
# bytes. ``extra_paths`` enumerates every column that can ever reach one of these bodies,
# 119 of them across the two journaled surfaces today, so the widest drift is computable
# rather than hypothetical. On the 115-ticker roster the design sizes for, every one of them
# drifting at once runs to 3,867 bytes uncapped and 866 capped. The roster size is part of
# the measurement, because the body prints a ticker count per column.
#
# ``extra_paths`` also covers a third pinned surface, ``bars``, and its five columns are not
# in that 119. They cannot reach a body either, because this page is built from journal
# segments and a bar never reaches one.
#
# What makes the cap load-bearing rather than defensive is the shape of those two numbers.
# The capped body is bounded by the cap, and the uncapped one grows with the column count,
# which grows every time a field is promoted out of ``extra`` into a column of its own.
# ntfy's own default body limit of 4096 bytes, which it answers an oversize POST with a 400
# for and ``NtfyTransport`` does not retry, sits further out and is not what decides this.
#
# The count survives the cut, because it is what separates one moved column from a
# wholesale retype, and stderr names every column either way.
PAGE_COLUMN_CAP = 12


@dataclass(frozen=True)
class ColumnDrift:
    """One column that started drifting, and the tickers that carried it.

    ``tickers`` is in cycle order, which is roster order, and it is what stderr prints.
    The page prints its length instead, per the collapsing rule.

    ``partial`` says the finding came from an observation of part of the roster rather
    than a whole cycle, which is what ``observe_partial`` produces. It is what ``_body``
    reads to decide whether printing a ticker count would be honest.
    """

    surface: str
    column: str
    tickers: tuple[str, ...]
    partial: bool = False


class SchemaDriftObserver:
    """Which columns are drifting right now, per surface, and which ones are new.

    It is handed each cycle's outcome and returns what should page. It sends nothing and
    reads no clock, so the caller decides both when to ask and where a page goes, which is
    the shape the watchdog already has.

    The state is, per surface, each drifting column and the tickers known to be drifting
    it. A column entering its surface's state is the transition that pages. A column
    leaving re-arms it, and a column leaves only on positive evidence: every ticker that
    was drifting it has since produced a data row that did not.

    Remembering the tickers is what makes the reset honest, and it is the difference
    between one page and a page every other minute. Their absence is also what clears a
    retired ticker, since a ticker the cycle no longer names at all is off the roster and
    can never produce the evidence that would clear it.

    There are two ways in, and which one a caller takes is decided by whether it holds a
    roster. ``observe`` takes a whole cycle and runs both halves, the transitions and the
    clearance. ``observe_partial`` takes one ticker's segment and runs the transitions
    alone. A caller holding one ticker must take the second, for the reason that method
    gives.
    """

    def __init__(self) -> None:
        self._routing: dict[str, dict[str, frozenset[str]]] = {}

    def observe(self, result: CycleResult) -> tuple[ColumnDrift, ...]:
        """Take one cycle's outcome and return the columns that started drifting in it."""
        # Three readings of the cycle, because the three answer different questions. The
        # roster is who the cycle still names, which is what tells a retired ticker from a
        # gapped one. The data rows are the only evidence about the vendor's payload. The
        # drifting columns are this cycle's finding.
        roster: dict[str, set[str]] = {}
        landed: dict[str, set[str]] = {}
        drifting: dict[str, dict[str, list[str]]] = {}
        for segment in result.segments:
            roster.setdefault(segment.surface, set()).add(segment.ticker)
            if segment.row_kind != ROW_KIND_DATA:
                continue
            landed.setdefault(segment.surface, set()).add(segment.ticker)
            for column in segment.routed_columns:
                columns = drifting.setdefault(segment.surface, {})
                columns.setdefault(column, []).append(segment.ticker)
        # A ticker whose segment could not be written at all is still on the roster. It
        # produced no observation, so it is not evidence, and dropping it here would read
        # as a retirement and clear a drift it says nothing about.
        for error in result.errors:
            roster.setdefault(error.surface, set()).add(error.ticker)

        started: list[ColumnDrift] = []
        # Only the surfaces this cycle touched. A surface it named at all, even to gap, is
        # a surface the roster still carries. One it did not name at all is no evidence of
        # anything, so its state stands untouched rather than being read as a clearance.
        for surface in sorted(roster):
            named = roster[surface]
            observed = landed.get(surface, set())
            now = drifting.get(surface, {})
            was = self._routing.get(surface, {})
            held: dict[str, frozenset[str]] = {}
            for column in sorted(set(now) | set(was)):
                reported = now.get(column, [])
                if reported and column not in was:
                    started.append(ColumnDrift(surface, column, tuple(reported)))
                # A ticker that was drifting this column and has not landed a data row
                # since is unresolved rather than clean. It keeps the column drifting, so
                # its next data row is not a fresh transition. A ticker the roster no
                # longer names drops out here, which is what lets a retirement clear.
                unresolved = (was.get(column, frozenset()) - observed) & named
                remembered = frozenset(reported) | unresolved
                if remembered:
                    held[column] = remembered
            if held:
                self._routing[surface] = held
            else:
                self._routing.pop(surface, None)
        return tuple(started)

    def observe_partial(
        self, surface: str, ticker: str, columns: Sequence[str]
    ) -> tuple[ColumnDrift, ...]:
        """Take one ticker's finding and return the columns that started drifting in it.

        This is the entry for the close+5 fill, which is the one writer that is not a
        cycle and still reaches a pager. It writes one segment for one ticker through
        ``capture.journal_snapshot``, and that segment carries the same drift signature a
        cycle's segment carries. An onboarding snapshot writes the same shape and comes
        nowhere near here, because it has no publisher to page through.

        It runs the transition half and not the clearance half, which is the whole reason
        it exists rather than a one-segment ``CycleResult``. Such a result type-checks and
        reads as a cycle, and the damage is silent. Clearance subtracts the roster, at
        ``unresolved = (was - observed) & named``, so a roster of one names every other
        ticker as retired and drops a column they are still drifting. Nothing would be
        lost on disk and no page would be missed. The cost is the duplicate the state
        exists to prevent: the next ordinary cycle would find the column absent, read it
        as a fresh transition, and page a second time for one vendor fact.

        So a partial observation only ever adds. A column it names that the surface was
        not already drifting is a transition and pages, marked ``partial`` so the page
        does not present one ticker as the retype's reach. A column it names that was
        already drifting pages nothing and remembers this ticker beside the others, which
        is what keeps the next ordinary cycle from re-reading it as new. A partial
        observation that finds nothing touches no state at all, because absence of
        evidence from one ticker is not evidence about the rest.

        Two prices come with that, and neither is hidden.

        1. A partial page says the reach is unmeasured, and no later page measures it. The
           state this writes is what silences the next whole cycle, which is the one
           observation that could have counted the roster. So the operator's one page for
           the drift is the one that could not say how far it went, where before this
           existed the next morning's cycle paged the count. The reach is still on stderr
           and in the nightly report, and which channel should carry it is marketlake #318.
        2. ``observe`` prunes only the surfaces a cycle's roster names, so a surface
           reached by partial observations alone would remember a ticker per call and
           never drop one. Nothing reaches that today, because the only caller names the
           chains surface and every cycle names it too. A second caller on another surface
           is what would make it real.
        """
        was = dict(self._routing.get(surface, {}))
        started: list[ColumnDrift] = []
        for column in sorted(set(columns)):
            if column not in was:
                started.append(ColumnDrift(surface, column, (ticker,), partial=True))
            held = self._routing.setdefault(surface, {})
            held[column] = was.get(column, frozenset()) | {ticker}
        return tuple(started)


def _body(drifted: Sequence[ColumnDrift]) -> str:
    """The page's text: what happened, then each surface's columns and their reach.

    A reach is printed only where one was measured. A finding from a whole cycle carries
    the roster's own answer, so the page prints how many tickers drifted the column. A
    partial finding carries whatever its one writer looked at, so the count would be one
    however far the retype actually reaches, and an operator reading "on 1 ticker(s)" at
    16:20 would take a vendor-wide retype for an isolated one. The count comes off those,
    and a closing sentence names what was read instead.

    One page's findings all come from one observation, because each caller passes what one
    call to ``observe`` or ``observe_partial`` returned, and neither ever returns both
    kinds. So the list is all partial or none of it is. The closing sentence is still
    written to be true of a mixed list, since what it names is the reach of a column that
    printed no count.
    """
    named: dict[str, list[str]] = {}
    for drift in drifted:
        if drift.partial:
            named.setdefault(drift.surface, []).append(drift.column)
        else:
            count = len(drift.tickers)
            named.setdefault(drift.surface, []).append(f"{drift.column} on {count} ticker(s)")
    parts = []
    for surface in sorted(named):
        columns = named[surface]
        shown = columns[:PAGE_COLUMN_CAP]
        if len(columns) > len(shown):
            shown.append(f"and {len(columns) - len(shown)} more")
        parts.append(f"{surface}: {', '.join(shown)}")
    body = (
        "The vendor sent a known field at a type its column refused, so the column is null "
        f"on the rows that carried it and the raw value is in extra. {'. '.join(parts)}."
    )
    looked_at = sorted({ticker for drift in drifted if drift.partial for ticker in drift.tickers})
    if looked_at:
        body += (
            f" Only {', '.join(looked_at)} was read, not a whole cycle, so the reach of a "
            "column printed without a count is unmeasured."
        )
    return body


def page(publisher: Publisher, drifted: Sequence[ColumnDrift], *, now: datetime) -> None:
    """Page once for the cycle, naming every column that started drifting in it.

    The finding reaches stderr as well as the phone, which is what compaction's drift page
    and the daemon's assertion page both already do. launchd captures that stream and the
    restart script sends the operator to it, so the per-ticker detail the page folds away
    is still recoverable without opening a segment.

    A publisher that refused the page found one of its own secrets in the body, and it
    redacted its record for that reason, so stderr must not undo the redaction. That is the
    one case where the body stops here.

    ``Publisher.publish`` never raises, so this cannot cost the cycle that produced the
    finding. A page that did not reach the phone is written down under ``reports/alerts/``
    by the publisher itself, and the reason is named on stderr too.

    The publisher is required rather than optional. ``compact._page_drift`` takes one that
    may be ``None`` because ``recompact_ticker_day`` is a hand run that passes none. Both
    callers here are the daemon's, its cycle hook and its close+5 fill, and ``_alarm``
    always builds a publisher, so an optional one here would be a branch nothing reaches.
    Onboarding is the producer that has no publisher, and it reaches none of this: it runs
    in its own process with no alarm behind it, so its finding rides its sign-off report.
    """
    if not drifted:
        return
    body = _body(drifted)
    delivery = publisher.publish(
        Message(event=SCHEMA_DRIFT_EVENT, title=SCHEMA_DRIFT_TITLE, body=body), now=now
    )
    if delivery.reason == REFUSED:
        print("capture: schema-drift page refused: it carried a secret", file=sys.stderr)
        return
    print(f"capture: {SCHEMA_DRIFT_TITLE}: {body}", file=sys.stderr)
    for drift in drifted:
        reach = ", ".join(drift.tickers)
        print(f"capture: schema drift: {drift.surface}.{drift.column} on {reach}", file=sys.stderr)
    if not delivery.sent:
        kept = "written down" if delivery.recorded else "lost"
        print(
            f"capture: schema-drift page not sent: {delivery.reason}, {kept}",
            file=sys.stderr,
        )
