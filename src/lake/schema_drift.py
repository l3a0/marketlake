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

Evidence comes from data segments alone. A gap segment carries no vendor observation, so
a cycle whose every fetch failed says nothing about the payload's shape and leaves the
state where it stood. Without that rule a whole-roster outage would re-arm every column
and page again the minute capture came back.
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
# ``compaction_schema_drift``, so the convention is the producer's name in front of the
# condition, and a reader of ``reports/alerts/`` can tell the two apart without opening a
# file. The title says where the drift was seen, which is the other half of the same
# distinction: two events that read alike on a phone would be worse than one.
SCHEMA_DRIFT_EVENT = "parser_schema_drift"
SCHEMA_DRIFT_TITLE = "Schema drift in the vendor payload"

# How many column names the page prints per surface before it stops and says how many are
# left. The bound is the design's own, which pins every page body at plain text under 1,000
# bytes. ``extra_paths`` enumerates every column that can ever reach one of these bodies,
# 119 of them across the two surfaces today, so the widest drift is computable rather than
# hypothetical: uncapped it runs to 3,867 bytes, and capped it runs to 866.
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
    """One column that started drifting this cycle, and the tickers that carried it.

    ``tickers`` is in cycle order, which is roster order, and it is what stderr prints.
    The page prints its length instead, per the collapsing rule.
    """

    surface: str
    column: str
    tickers: tuple[str, ...]


class SchemaDriftObserver:
    """Which columns are drifting right now, per surface, and which ones are new.

    It is handed each cycle's outcome and returns what should page. It sends nothing and
    reads no clock, so the caller decides both when to ask and where a page goes, which is
    the shape the watchdog already has.

    The state is one set of routing columns per surface. A column entering its surface's
    set is the transition that pages. A column leaving re-arms it. A surface the cycle
    produced no data for is not evidence and is left alone.
    """

    def __init__(self) -> None:
        self._routing: dict[str, frozenset[str]] = {}

    def observe(self, result: CycleResult) -> tuple[ColumnDrift, ...]:
        """Take one cycle's outcome and return the columns that started drifting in it."""
        carried: dict[str, dict[str, list[str]]] = {}
        for segment in result.segments:
            if segment.row_kind != ROW_KIND_DATA:
                continue
            columns = carried.setdefault(segment.surface, {})
            for column in segment.routed_columns:
                columns.setdefault(column, []).append(segment.ticker)
        drifted: list[ColumnDrift] = []
        for surface in sorted(carried):
            columns = carried[surface]
            was = self._routing.get(surface, frozenset())
            self._routing[surface] = frozenset(columns)
            drifted.extend(
                ColumnDrift(surface, column, tuple(columns[column]))
                for column in sorted(columns)
                if column not in was
            )
        return tuple(drifted)


def _body(drifted: Sequence[ColumnDrift]) -> str:
    """The page's text: what happened, then each surface's columns and their reach."""
    named: dict[str, list[str]] = {}
    for drift in drifted:
        count = len(drift.tickers)
        named.setdefault(drift.surface, []).append(f"{drift.column} on {count} ticker(s)")
    parts = []
    for surface in sorted(named):
        columns = named[surface]
        shown = columns[:PAGE_COLUMN_CAP]
        if len(columns) > len(shown):
            shown.append(f"and {len(columns) - len(shown)} more")
        parts.append(f"{surface}: {', '.join(shown)}")
    return (
        "The vendor sent a known field at a type its column refused, so the column is null "
        f"on the rows that carried it and the raw value is in extra. {'; '.join(parts)}."
    )


def page(publisher: Publisher | None, drifted: Sequence[ColumnDrift], *, now: datetime) -> None:
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
    """
    if not drifted:
        return
    body = _body(drifted)
    delivery = None
    if publisher is not None:
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
    if delivery is not None and not delivery.sent:
        kept = "written down" if delivery.recorded else "lost"
        print(
            f"capture: schema-drift page not sent: {delivery.reason}, {kept}",
            file=sys.stderr,
        )
