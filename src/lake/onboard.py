"""The onboarding command: any ticker, one command.

``python -m lake.onboard TICKER`` brings one instrument into the lake. This is the
slice-1 version of the flow the design pins. It does exactly the steps that stand on
their own with the market closed and the network faked, and it defers the two steps
that belong to later slices with clear notes instead of faking them.

The slice-1 steps, in order.

1. *Register the instrument in the security master.* The security master is the
   reference table that assigns every instrument a stable internal ``instrument_id``
   and maps it to the external symbols. Registration stamps a ``capture_start`` epoch,
   the instant capture begins, and creates the ticker mapping. The epoch defaults to the
   injected clock's now, and ``--capture-start`` supplies it outright. It does not
   resolve the FIGI. The FIGI is not capture-critical, and Schwab's own CUSIP is captured
   raw on the equity quotes, so the FIGI backfills later from that CUSIP in a deferred
   enrichment pass. Day-one onboarding is kept to the two facts that
   cannot be redone later: the ``instrument_id`` and the ``capture_start`` epoch. All
   coverage and gap accounting clamp to that epoch, so onboarding day reads "onboarded
   11:00," never 40 percent missing.
2. *Take the first snapshot and assert the real-time entitlement.* For an options
   ticker this fetches the chain by its date-window plan, through the same
   ``capture.fetch_chain`` the capture loop and the close+5 fill use, and asserts the
   vendor's ``isDelayed`` flag is false. A whole chain in one request exceeds Schwab's
   gateway body limit, which Schwab answers with a 502, so the plan's windows are what
   make a real chain fetchable at all. For an equity-only ticker it fetches one quote
   and asserts the ``realtime`` flag is true. Real-time entitlement is a verified
   precondition, not an assumption. A delayed feed fails onboarding before the ticker is
   trusted, written, or journaled. The fetch is stamped like a capture cycle:
   ``snap_ts`` the onboarding minute slot, and a ``fetch_ts`` / ``fetch_end_ts`` pair
   around the request. The chain branch takes that pair from ``fetch_chain``, which
   stamps it around the whole windowed fetch, so the journaled round trip covers every
   window. The quote branch stamps its own pair around its one call. Both read the
   injected clock.
3. *Write the roster entry.* The command writes the ``tickers.yaml`` entry itself. The
   roster lives in ``~/.config/marketlake/``, outside the repo, so no machine path or
   secret ever lands in a tracked file.
4. *Persist the master and journal the snapshot.* The master is written under the
   lake-root lock and given a manifest entry, so the integrity scrub stays clean in both
   directions. Then, once the ticker is trusted, the same snapshot fetched for
   verification is journaled as the ticker's first captured cycle rather than discarded.
   No second fetch is made. It is written through the capture primitive's own durable
   path, so the segment is indistinguishable in shape from one the loop writes. A
   perishable sample is never thrown away. A re-onboard is a new writer session, so its
   segment name differs and journaling another snapshot is fine: each is a real cycle at
   its own moment.
5. *Print a sign-off report.* The report pins the first snapshot's contract count as
   the day-one plausibility anchor. The median-relative battery checks have no anchor
   until history accrues, so this count is the one early sanity number. It also names any
   column of that snapshot whose vendor field arrived at a type the column refused. The
   capture loop pages a phone for that, and this command has no phone behind it, so the
   report is where a ticker onboarded onto a drifting field says so.

A windowed fetch can come back partial. ``fetch_chain`` never raises, so it returns
whatever the windows that succeeded carried, with absence markers naming the rest. The
loop journals that partial snapshot as data, and the close+5 fill refuses to land a body
with no contract in it. Onboarding is a third caller making a third claim, that a ticker
is trustworthy, so it owes three answers of its own.

1. *A partial chain still proves the entitlement.* One successful window carries the
   ``isDelayed`` flag, and a real-time flag is real-time whatever else failed. The
   contract count is then measured on part of the chain, so the report marks the anchor
   as partial rather than letting a partial chain read as a whole one.
2. *Every window failing refuses before the entitlement is asked.* There is no body to
   ask it of. The refusal names the fetch's error class, which is what there is left to
   name once no single response carries a status.
3. *A chain that answers and carries no contract refuses too.* This one runs after the
   entitlement assertion, because a body did come back and it can still prove the feed is
   real-time. What it cannot do is anchor the sign-off. That count is the day-one
   plausibility anchor, the one early sanity number, because the median-relative battery
   checks have nothing to measure against until history accrues. An anchor of zero is
   worse than no anchor, since every later check measures against it.

The epoch and the stamps are two different facts, and ``--capture-start`` is what
separates them. A lake can be captured before its master exists, and seeding one
honestly means recording the instant capture actually began rather than the instant the
command runs. So the epoch reaches three places and nothing else: the master's
``capture_start``, the ticker mapping's ``valid_from``, and the capture span's start.

Five stamps keep reading the clock, because each records when this command ran.

1. The journaled snapshot's ``cycle_start``.
2. Its ``fetch_ts``.
3. Its ``fetch_end_ts``.
4. The ``fetched_at`` on the master's manifest entry.
5. The ``fetched_at`` on the spans file's manifest entry.

Threading the epoch into any of the five would write today's chain into a backdated
partition and corrupt captured data that can never be captured again.

Three refusals guard the option. All three are asked ahead of the vendor call and ahead of
every write, so a typo spends neither a request nor a rewritten roster, and the third is
then asked a second time under the lock, where it can refuse later. The paragraph below
the list carries that second asking.

1. An instant after the clock's own now is refused. A capture span starting in the
   future reports out of scope, so the ticker would not be captured until that instant
   arrived, with nothing said about it. A mistyped year is enough to do that, and a lost
   minute is gone forever.
2. A naive instant is refused, and the refusal names the fix. ``datetime.fromisoformat``
   reads a bare date as a naive midnight, and a bare date is what a person types first.
3. An explicit epoch is refused outright when the master already maps the ticker on any
   date. The idempotence check resolves the ticker as of the epoch, so the epoch's own
   value decides whether a run counts as a re-onboard at all. Running the command again
   with a corrected epoch is exactly what a person does on noticing the first one was
   wrong, and that is the run this refusal catches.

The first two are ``argparse``'s, raised from the option's own ``type`` hook, so the
operator gets one named line and exit 2 with no stack to read past. That is the code and
the shape ``argparse`` already uses for a bad argument in these entries. The third cannot
go there, because asking what the master already holds means reading the master, which
means the lake root from config. It raises ``OnboardError`` from ``onboard`` instead, and
``main`` turns that into the same named line and the same exit 2, so all three read alike.
Every other ``OnboardError`` the flow raises arrives the same way.

The third refusal is asked *twice*, and only the first asking is free. It runs before the
vendor call, which is what keeps the guarantee above that a typo spends no request. It
then runs again against the master read inside the lock, because a ticker another
onboarding registered during the fetch resolves there, and reusing that instrument would
drop this caller's epoch without a word. So that second asking can raise after the request
is spent and after the roster entry is written. That is the price of deciding from what
the lake holds at the moment of the write rather than from a snapshot taken before a
network round trip, and it is paid deliberately. Marketlake #481.

Deferred, not faked. Each is a later slice, and this command is structured so each
becomes an added step here without reshaping the flow.

- The FIGI resolution is a deferred enrichment. Schwab's CUSIP is captured raw on the
  equity quotes, and the FIGI backfills from it later, because a CUSIP is an unambiguous
  OpenFIGI lookup key. Day-one onboarding leaves the master's FIGI unset.
- The corporate-actions history fetch is slice 3 (D16). Onboarding will later fetch and
  land splits and dividends for the ticker. It is skipped here.
- The full validation battery is slice 5 (D20). Onboarding will later run the battery
  and fold its verdict into the report. Here only the single real-time entitlement
  precondition is checked, which is the one gate the design names for slice 1.

Every dependency is injected: the clock, the vendor, the chain plan, and the guard
constants. The plan and the guards default to their own in-memory constants rather than
to a file read, so a caller naming only the clock and the vendor still runs offline. The
whole flow therefore needs no network, no real token, and no wall-clock read. The one
exception is the command line's own ``--capture-start`` check, which asks what time it is
now and so reads the real clock before any of this is reached. The thin ``onboard_from_config``
wires the real config and the Schwab-backed vendor around the same core, keeping every
real construction lazy.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from lake import capture, capture_spans, journal, security_master
from lake.calendar import MARKET_TZ
from lake.chain_plan import DEFAULT_CHAIN_PLAN, ChainPlan, load_chain_plan
from lake.clock import Clock, SystemClock
from lake.config import GuardConstants, input_errors_exit, load_config
from lake.manifest import record_partition
from lake.security_master import ID_TYPE_TICKER, KIND_EQUITY, SecurityMaster
from lake.tickers import upsert_ticker
from lake.timing import RequestRecord
from lake.vendor import Vendor

# The manifest ``source`` for the security master's reference entry.
REFERENCE_SOURCE = "reference"

# The lake-relative path of the security master, the key its manifest entry uses.
MASTER_PARTITION = f"{security_master.REFERENCE_DIR}/{security_master.MASTER_FILENAME}"

# The default roster fields for an options ticker. SPY and QQQ are the anchor tickers,
# so options-on at one-minute cadence is the default. ``chain_cadence`` is a cadence
# string, not a time of day.
DEFAULT_CHAIN_CADENCE = "1m"
DEFAULT_BARS: tuple[str, ...] = ("1m", "1d")


class OnboardError(Exception):
    """Base class for every onboarding failure."""


class EntitlementError(OnboardError):
    """Raised when the first snapshot does not prove a real-time entitlement.

    Real-time entitlement is a verified precondition. A delayed feed corrupts every
    row silently, so the ticker is refused before it is trusted or written.
    """


@dataclass(frozen=True)
class OnboardReport:
    """The sign-off report for one onboarded ticker.

    ``contract_count`` is the day-one plausibility anchor: the number of contracts in
    the first chain snapshot. It is ``None`` for an equity-only ticker, which takes no
    chain snapshot. ``partial_chain`` is true when a window of that chain fetch failed,
    so the count was measured on part of the chain rather than all of it, and the
    rendered anchor says so. ``already_registered`` is true when the ticker was already
    in the master, so a re-run reuses its id and capture_start rather than minting new
    ones.
    ``snapshot_surface`` is the surface journaled as the first cycle (``chains`` or
    ``quotes``), and ``snapshot_segment`` is that segment's lake-relative path. The
    master's FIGI is deliberately unset here; it backfills later from the captured CUSIP.
    ``deferred`` names the later-slice steps this command does not yet do.

    ``routed_columns`` names the snapshot's own columns whose vendor field arrived at a
    type the column refused, which is the schema-drift signature the capture cycle pages
    on. This report is where it goes, because onboarding runs in its own process with no
    alarm behind it. It reaches neither the daemon nor the daemon's drift observer. The
    field is empty on every ordinary onboarding, and the rendered block stays silent when
    it is. The column name is the whole finding here. This command looks at one ticker,
    so there is no reach to report.
    """

    ticker: str
    instrument_id: int
    capture_start: datetime
    options: bool
    contract_count: int | None
    realtime_verified: bool
    tickers_path: Path
    master_path: Path
    already_registered: bool
    snapshot_surface: str
    snapshot_segment: str
    partial_chain: bool = False
    routed_columns: tuple[str, ...] = ()
    deferred: tuple[str, ...] = (
        "FIGI resolution from the captured CUSIP (deferred enrichment)",
        "corporate-actions history fetch (slice 3, D16)",
        "full validation battery (slice 5, D20)",
    )

    def render(self) -> str:
        """A human-readable sign-off block."""
        lines = [
            f"Onboarded {self.ticker}",
            f"  instrument_id:   {self.instrument_id}"
            + (" (already registered)" if self.already_registered else ""),
            f"  capture_start:   {self.capture_start.isoformat()}",
            f"  options:         {self.options}",
            f"  realtime:        {'verified' if self.realtime_verified else 'not checked'}",
        ]
        if self.contract_count is not None:
            # A partial fetch's count describes part of a chain, so the line says which
            # it is. This takes the same conditional decoration the instrument id above
            # takes for an already-registered ticker.
            lines.append(
                f"  day-one anchor:  {self.contract_count} contracts in first snapshot"
                + (" (partial chain: a window failed)" if self.partial_chain else "")
            )
        lines.append(f"  first cycle:     {self.snapshot_surface} segment {self.snapshot_segment}")
        if self.routed_columns:
            # Named rather than counted. The count is the honest number on a cycle's page,
            # where it says how far a retype reached. Here it would always be one, and the
            # operator can act on the column name and on nothing else.
            lines.append(
                f"  schema drift:    {self.snapshot_surface} "
                f"{', '.join(self.routed_columns)} arrived at a type the column refused, "
                "so the column is null and the raw value is in extra"
            )
        lines.append(f"  tickers.yaml:    {self.tickers_path}")
        lines.append(f"  security master: {self.master_path}")
        lines.append("  deferred to later slices:")
        lines.extend(f"    - {item}" for item in self.deferred)
        return "\n".join(lines)


def _ok(status: int) -> bool:
    """Whether an HTTP status is a success."""
    return 200 <= status < 300


def _count_contracts(body: Mapping[str, object]) -> int:
    """The number of contracts in a chain body, calls plus puts.

    This walks the same ``expDateMap`` nesting the journal's row builder walks, so the
    anchor count matches what capture would journal. It never reads the vendor's own
    ``numberOfContracts`` field, which is not guaranteed present or accurate.
    """
    count = 0
    for map_key in ("callExpDateMap", "putExpDateMap"):
        exp_map = body.get(map_key) or {}
        if isinstance(exp_map, Mapping):
            for strikes in exp_map.values():
                if isinstance(strikes, Mapping):
                    for contract_list in strikes.values():
                        count += len(contract_list)
    return count


def _assert_chain_realtime(body: Mapping[str, object]) -> None:
    """Assert a chain response proves a real-time entitlement.

    The vendor's ``isDelayed`` flag must be present and false. A missing flag cannot
    prove real-time, so it is refused too.
    """
    is_delayed = body.get("isDelayed")
    if is_delayed is not False:
        raise EntitlementError(
            f"chain response is not real-time: isDelayed={is_delayed!r}; ticker not trusted"
        )


def _assert_quote_realtime(ticker: str, body: Mapping[str, object]) -> None:
    """Assert a quote response proves a real-time entitlement.

    The envelope's ``realtime`` flag must be present and true.
    """
    envelope = body.get(ticker)
    realtime = envelope.get("realtime") if isinstance(envelope, Mapping) else None
    if realtime is not True:
        raise EntitlementError(
            f"quote for {ticker} is not real-time: realtime={realtime!r}; ticker not trusted"
        )


def _market_date(instant: datetime) -> date:
    """The Eastern-time calendar date of an instant, the market's notion of today.

    The security master's validity range is dated, and a mapping begins on the market
    day capture starts. Converting to the market zone keeps that date correct near the
    UTC midnight boundary. This reads no clock; the instant is the caller's.
    """
    return instant.astimezone(MARKET_TZ).date()


def _parse_capture_start(text: str) -> datetime:
    """Parse and check the ``--capture-start`` argument. ``argparse`` calls this.

    Three things are refused here, at parse time, so a typo costs neither a vendor call
    nor a rewritten roster.

    1. A string that does not parse as an ISO-8601 instant.
    2. A naive instant. The refusal names the fix, because ``datetime.fromisoformat``
       reads a bare date as a naive midnight and a bare date is what a person types
       first.
    3. An instant after now. A span starting in the future reports out of scope, so the
       ticker is not captured until that instant arrives and nothing says so.

    ``register`` and ``open_span`` both refuse a naive instant themselves, so nothing
    reaches the master unchecked, and both now run inside the lake-root lock, which is
    after the live fetch and after the roster upsert. So ``onboard`` refuses a naive epoch
    of its own accord before either, and refusing here as well is what turns it into one
    named line and exit 2 rather than a ``ValueError``.

    Every refusal raises ``ArgumentTypeError``, so the operator gets one named line and
    exit 2 rather than a stack. The clock is the real one, because this runs before any
    caller has built a clock and the question is what time it is now.
    """
    import argparse

    try:
        instant = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{text!r} is not an ISO-8601 instant, like 2026-09-08T17:07:00+00:00"
        ) from None
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            f"{text!r} carries no UTC offset, so it reads as a naive midnight. Pass an "
            "offset, like 2026-09-08T17:07:00+00:00 or 2026-09-08T13:07:00-04:00."
        )
    now = SystemClock().now()
    if instant > now:
        raise argparse.ArgumentTypeError(
            f"{text!r} is after now ({now.isoformat()}). A capture span starting in the "
            "future reports out of scope, so the ticker is not captured until that "
            "instant arrives and nothing says so."
        )
    return instant


def _read_master(master_path: Path) -> SecurityMaster:
    """The master on disk, or an empty one when the lake has no master yet.

    Named rather than inlined because it is read twice on purpose: once before the vendor
    call, for the refusals that must answer without spending a request, and once inside
    the lock, for every value the write is derived from. Marketlake #481.
    """
    return SecurityMaster.read(master_path) if master_path.exists() else SecurityMaster()


def _read_spans(spans_file: Path) -> capture_spans.CaptureSpans:
    """The capture spans on disk, or an empty set when the lake has no spans file yet."""
    return (
        capture_spans.CaptureSpans.read(spans_file)
        if spans_file.exists()
        else capture_spans.CaptureSpans()
    )


def _refuse_an_unseeded_lake(master: SecurityMaster, spans_file: Path) -> None:
    """Refuse a lake whose master has instruments and whose spans file does not exist.

    The master already has instruments, registered under the old capture_start-only
    scheme, and the seed run that gives them their spans has not happened. Opening a
    fresh, empty spans file here would silently discard every one of their capture
    histories: an already-registered ticker would look never-captured to every span
    reader. Refuse instead of guessing, and name the fix. A brand-new lake, with no
    instruments yet, never trips this, so a fresh onboarding needs no seed run first.

    Asked twice, like the epoch refusal beside it: once before the vendor call and once
    against the master the lock actually writes.
    """
    if not spans_file.exists() and master.instrument_ids():
        raise OnboardError(
            "capture spans are missing but the security master already has instruments; "
            "run `python -m lake.seed_spans` before onboarding"
        )


def _refuse_a_second_ticker_mapping(master: SecurityMaster, ticker: str) -> None:
    """Refuse registering a ticker the master already maps on some other date.

    ``resolve`` answers as of one date, so a mapping opened under a later market date is
    invisible to it and the caller would register a second instrument for one ticker over
    overlapping open ranges. ``resolve`` then raises ``AmbiguousSymbol`` on every date they
    share, which is the state the master calls corrupt, and nothing repairs it by re-running.
    Marketlake #481.
    """
    held = [m for m in master.mappings if m.id_type == ID_TYPE_TICKER and m.id_value == ticker]
    if not held:
        return
    ranges = ", ".join(
        f"instrument_id {m.instrument_id} from {m.valid_from.isoformat()}"
        for m in sorted(held, key=lambda m: (m.instrument_id, m.valid_from))
    )
    raise OnboardError(
        f"{ticker} resolves to no instrument on this run's date and the master already maps "
        f"it ({ranges}), which another onboarding landed while this one fetched. Registering "
        f"on top of that would give one ticker two instruments over overlapping ranges. "
        f"Re-run to take the idempotent path"
    )


def _refuse_a_span_that_overlaps_a_close(
    spans: capture_spans.CaptureSpans, instrument_id: int, start: datetime, ticker: str
) -> None:
    """Refuse opening a span that would begin inside one already closed.

    ``CaptureSpans.open_span`` refuses a second *open* span and nothing else, so a start
    earlier than an existing close is accepted and the two overlap. ``in_scope`` is a union
    over the spans, so the overlap reads the recorded retirement away, and
    ``close_guard`` selects by span rather than by roster and would mark the ticker owed
    for ever. Marketlake #481.
    """
    latest = max((s.end for s in spans.spans_of(instrument_id) if s.end is not None), default=None)
    if latest is None or start > latest:
        return
    raise OnboardError(
        f"{ticker} was retired at {latest.isoformat()} while this onboarding ran, which is "
        f"after the {start.isoformat()} this run would open its new span at. Opening it "
        f"would overlap the span that retirement closed. Re-run to rejoin the ticker"
    )


def _refuse_a_second_epoch(master: SecurityMaster, ticker: str) -> None:
    """Refuse an explicit epoch for a ticker the master already maps, on any date.

    The idempotence check resolves the ticker as of the epoch, so the epoch decides
    whether a run counts as a re-onboard. An epoch earlier than the first run's resolves
    to nothing, because the first run's mapping begins later. Registration then runs a
    second time, and ``register`` guards only against a duplicate ``instrument_id``,
    never against a duplicate symbol. The master would hold two instruments mapping one
    ticker over overlapping open ranges, and ``resolve`` raises ``AmbiguousSymbol`` on
    every date they share, which is the signal of a corrupt master. A second span opens
    at the earlier start too, so the spans file disagrees with itself.

    An epoch later than the first run's fails the other way. Resolution succeeds, nothing
    is registered, the open span is kept, and the run reports an already-registered
    ticker while the correction is silently dropped.

    Asking the master what it already holds, rather than what it holds on one date, is
    what catches both. The refusal cannot tell a second attempt at a different epoch from
    a re-run after a partial one, so it names the epoch the master already holds and both
    ways forward. An operator whose epoch matches that value re-runs without the option
    and takes the idempotent path. One whose epoch differs repairs the recorded value
    deliberately, which is a decision rather than a second command.
    """
    held = [m for m in master.mappings if m.id_type == ID_TYPE_TICKER and m.id_value == ticker]
    if not held:
        return
    ranges = ", ".join(
        f"instrument_id {m.instrument_id} from {m.valid_from.isoformat()} "
        + ("(open)" if m.valid_to is None else f"to {m.valid_to.isoformat()}")
        for m in sorted(held, key=lambda m: (m.instrument_id, m.valid_from))
    )
    recorded = sorted({m.capture_start.isoformat() for m in held})
    raise OnboardError(
        f"the security master already maps {ticker}, so an explicit capture_start is "
        f"refused. It holds {', '.join(recorded)} as the capture_start, on {ranges}. "
        "Re-run without --capture-start if that epoch is the one you wanted, which "
        "reuses the instrument and its recorded epoch. Repair the recorded epoch "
        "deliberately if it is not."
    )


def onboard(
    ticker: str,
    *,
    clock: Clock,
    vendor: Vendor,
    lake_root: Path | str,
    tickers_path: str | Path | None = None,
    tickers_env: Mapping[str, str] | None = None,
    options: bool = True,
    chain_cadence: str | None = DEFAULT_CHAIN_CADENCE,
    bars: Sequence[str] = DEFAULT_BARS,
    capture_start: datetime | None = None,
    plan: ChainPlan | None = None,
    guards: GuardConstants | None = None,
    pid: int | None = None,
) -> OnboardReport:
    """Onboard one ticker into the lake and return its sign-off report.

    Every dependency is injected, so this runs offline. The steps follow the module
    docstring. The security master is read from disk if it exists, updated, and written
    back under the lake-root lock with a fresh manifest entry. The roster entry and the
    master persist to disk. The first snapshot is fetched to verify the real-time
    entitlement and count contracts, and then, once the ticker is trusted, that same
    snapshot is journaled as the ticker's first captured cycle through the capture
    primitive's durable path. No second fetch is made. ``pid`` sets the journal segment's
    writer-session id, defaulting to this process, so a caller can force a deterministic
    segment name.

    ``capture_start`` is the epoch capture began at, defaulting to the clock's now. It
    reaches the scope record alone, and the module docstring carries the split and the
    three refusals it owes. The command line checks its shape before calling this, and
    the one refusal that needs the master is here.

    ``plan`` and ``guards`` belong to the chain fetch, and each defaults to its in-memory
    constant, the built-in ``DEFAULT_CHAIN_PLAN`` and the pinned ``GuardConstants``.
    Neither default reads anything, which is what keeps the claim above true: every
    dependency is injected and this runs offline. ``fill_option_close`` defaults its plan
    by reading the machine's plan file instead, and that is right for a function the
    daemon reaches directly. Doing it here would put a config-directory read inside the
    one function built to need nothing. ``onboard_from_config`` is where the real plan is
    loaded and passed in.
    """
    lake_root = Path(lake_root)
    now = clock.now()
    # The epoch the master, the mapping, and the span take. Every other value below is
    # the clock's.
    epoch = now if capture_start is None else capture_start
    valid_from = _market_date(epoch)

    master_path = security_master.master_path(lake_root)
    spans_file = capture_spans.spans_path(lake_root)

    # A preflight read, for the refusals alone. Both of them have to answer before the
    # vendor call, so a typo spends neither a request nor a rewritten roster, which is
    # what the module docstring pins. Nothing is decided from this snapshot: every value
    # the write needs is re-derived from a fresh read inside the lock below, because a
    # reference table is rewritten whole and a stale snapshot discards whatever landed in
    # the window rather than superseding it. Marketlake #481.
    if capture_start is not None and (
        capture_start.tzinfo is None or capture_start.utcoffset() is None
    ):
        # ``register`` and ``open_span`` both refuse this themselves, and both now run
        # inside the lock, so leaving it to them spends a request and rewrites the roster
        # first. ``ValueError`` is what they raise and what a library caller already
        # handles, so the type is kept and only the moment moves.
        raise ValueError("capture_start must be timezone-aware")

    preflight = _read_master(master_path)
    _refuse_an_unseeded_lake(preflight, spans_file)
    # The spans are read here too, and what they say is deliberately thrown away. No
    # decision is taken from this call. It is the read itself that has to happen before
    # the vendor call, because a spans file that will not parse raises ``SpansUnreadable``,
    # and an operator is owed that refusal for the price of no request and no roster
    # entry. Moving it inside the lock alone put it after both, and the half-onboarded
    # ticker left in the roster is then captured anyway, since ``capture._live_roster``
    # widens when the spans file cannot be read.
    _read_spans(spans_file)
    if capture_start is not None:
        # An explicit epoch is what makes the resolution below depend on the caller's
        # value, so it is what owes the duplicate-symbol guard. An omitted epoch resolves
        # at the clock's own date and leaves the idempotent re-onboard exactly as it was.
        _refuse_a_second_epoch(preflight, ticker)

    # The first snapshot proves the real-time entitlement before the ticker is trusted.
    # It is stamped like a capture cycle so it can be journaled as the first cycle:
    # cycle_start floors to snap_ts, and each branch stamps its own round trip from the
    # injected clock.
    cycle_start = clock.now()
    contract_count: int | None = None
    partial_chain = False
    windows: tuple[tuple[date, date | None], ...] = ()
    absent_markers: tuple[journal.AbsentMarker, ...] = ()
    requests: tuple[RequestRecord, ...] = ()
    if options:
        # The chain goes through the capture loop's own windowed fetch. One bare request
        # for a whole chain exceeds Schwab's gateway body limit, which is a 502, so the
        # anchor tickers could not be onboarded at all until this fetched by the plan.
        #
        # ``day`` is the clock's date, never the epoch's. It decides which windows are
        # planned, and the rows land under ``cycle_start``'s own date because that is
        # what ``journal_snapshot`` places the segment by. A backdated ``--capture-start``
        # must not move the windows away from the day being captured.
        fetched = capture.fetch_chain(
            clock,
            vendor,
            ticker,
            day=cycle_start.date(),
            lake_root=lake_root,
            plan=plan if plan is not None else DEFAULT_CHAIN_PLAN,
            guards=guards if guards is not None else GuardConstants(),
        )
        # The fetch stamps its own pair around every window, so the journaled round trip
        # covers the whole fetch rather than one call that no longer happens.
        fetch_ts = fetched.fetch_ts
        fetch_end_ts = fetched.fetch_end_ts
        if fetched.body is None:
            # Every window failed, so nothing came back to prove anything with. There is
            # no single response left to name a status from, so the refusal names the
            # first failed window's class instead.
            raise OnboardError(f"first chain snapshot for {ticker} failed: {fetched.error_class}")
        body = fetched.body
        _assert_chain_realtime(body)
        contract_count = _count_contracts(body)
        if contract_count == 0:
            # A body that answered and carried no contract is the second failure shape.
            # The sign-off pins this count as the day-one plausibility anchor, the one
            # early sanity number the median-relative battery has no substitute for until
            # history accrues, and an anchor of zero is worse than no anchor. The check
            # runs before any write, so no zero-row segment is created to clean up.
            raise OnboardError(
                f"first chain snapshot for {ticker} carried no contract, so the day-one "
                "anchor would be pinned at zero; ticker not trusted"
            )
        # A fetch that gave up a window still proves the entitlement, because one
        # successful window carries the flag. Its count describes part of the chain, so
        # the report says which it is.
        partial_chain = fetched.error_class is not None
        windows = fetched.windows
        absent_markers = fetched.absent_markers
        requests = fetched.requests
        snapshot_surface = journal.CHAINS_SURFACE
    else:
        fetch_ts = clock.now()
        response = vendor.get_quotes([ticker])
        fetch_end_ts = clock.now()
        if not _ok(response.status):
            raise OnboardError(f"first quote for {ticker} failed: HTTP {response.status}")
        _assert_quote_realtime(ticker, response.body)
        body = response.body
        snapshot_surface = journal.QUOTES_SURFACE

    # Only now, past the entitlement gate, write the roster entry.
    written_tickers_path = upsert_ticker(
        ticker,
        options=options,
        chain_cadence=chain_cadence if options else None,
        bars=bars,
        path=tickers_path,
        env=tickers_env,
    )

    # Persist the master and record it, under the lake-root lock. The manifest entry
    # keeps the reverse integrity scrub from flagging the master as an orphan. The lock
    # import is local to keep this module free of the lock unless it writes.
    from lake.lock import lake_lock

    with lake_lock(lake_root):
        # Read both reference tables inside the hold that rewrites them, and re-derive
        # every decision from what they say now rather than from the preflight snapshot.
        # The window above holds a whole vendor round trip, and the write is a whole-file
        # rewrite, so anything another writer landed in it would be discarded rather than
        # superseded. ``SecurityMaster.register`` takes its id from
        # ``next_instrument_id()`` on the snapshot it is handed, so a stale one does not
        # merely drop a registration: it hands that instrument's id to this ticker, and
        # the master's promise that ids never change is what breaks.
        #
        # Holding across the fetch instead is rejected. ``lake_lock`` is a blocking
        # exclusive ``flock`` on the manifest, and a vendor round trip inside it would put
        # a network call in front of capture's per-minute append, which is the same reason
        # ``lake.bars`` fetches outside its own hold.
        master = _read_master(master_path)
        spans = _read_spans(spans_file)
        _refuse_an_unseeded_lake(master, spans_file)
        if capture_start is not None:
            # Asked again, and not only for symmetry. A ticker another onboarding
            # registered in the window resolves here, so the branch below would reuse that
            # instrument and drop this caller's epoch without a word, which is the second
            # of the two failures this refusal exists for. The price is that it can now
            # raise after the request is spent and after the roster entry is written, and
            # the module docstring above says so rather than leaving it to be met here.
            _refuse_a_second_epoch(master, ticker)

        # Idempotent-friendly: reuse the existing instrument if the ticker is already
        # known.
        existing_id = master.resolve(ticker, valid_from, id_type=ID_TYPE_TICKER)
        if existing_id is None:
            # Resolution is as of one date and the guard is over every date. An onboarding
            # that landed in the window under a *later* market date leaves a mapping this
            # resolution cannot see, and registering on top of it gives one ticker two
            # instruments over overlapping open ranges. ``register`` guards the duplicate
            # id and never the duplicate symbol, so ``resolve`` would raise
            # ``AmbiguousSymbol`` on every date they share, which the master calls corrupt
            # and which no re-run repairs.
            _refuse_a_second_ticker_mapping(master, ticker)
        already_registered = existing_id is not None
        if already_registered:
            instrument_id = existing_id
        else:
            # Register with the ticker mapping only. The FIGI is left unset here and
            # backfills later from the captured CUSIP. The two facts that cannot be
            # redone, the instrument_id and the capture_start epoch, are what onboarding
            # pins now.
            instrument_id = master.register(
                kind=KIND_EQUITY,
                capture_start=epoch,
                valid_from=valid_from,
                ticker=ticker,
            )

        # Open a capture span. A new instrument opens its first. A ticker brought back
        # after retirement, its spans all closed, opens a fresh one at the clock's now,
        # because retiring leaves the ticker mapping open and the refusal above turns away
        # a rejoin carrying an explicit epoch. A ticker already capturing keeps its open
        # span, so re-onboarding is idempotent for scope too.
        if spans.has_open_span(instrument_id):
            opened_span = False
        else:
            # A brand-new instrument opens at the epoch, which is the caller's value or
            # the clock's now. A rejoin reads the clock again here instead, because the
            # first reading was taken before the vendor round trip and a retire landing in
            # that window closes the span at an instant later than it. Opening at the
            # stale reading then starts a second span inside the one just closed.
            # ``open_span`` guards only against an *open* span, so it accepts that overlap
            # and ``in_scope`` is a union, which reads the recorded retirement away.
            start_at = clock.now() if already_registered else epoch
            _refuse_a_span_that_overlaps_a_close(spans, instrument_id, start_at, ticker)
            spans.open_span(instrument_id, start_at, options)
            opened_span = True
        # The report's capture_start is the current span's start: the epoch for a new or
        # rejoined ticker, and the existing open span's start for one already capturing.
        # It takes its own name, because the parameter above is what the caller asked for
        # and this is what the lake now holds. The branch above leaves an open span either
        # way, so this needs no guard of its own.
        span_start = next(s.start for s in spans.spans_of(instrument_id) if s.end is None)

        # Persist the master and record it. The manifest entry keeps the reverse
        # integrity scrub from flagging the master as an orphan.
        master.write(master_path)
        record_partition(
            lake_root,
            MASTER_PARTITION,
            source=REFERENCE_SOURCE,
            rows=len(master),
            fetched_at=now.isoformat(),
        )
        # Write the spans file only when a span was opened, so an idempotent re-onboard
        # of a live ticker adds no manifest entry. The write is atomic, the same as the
        # master's, and both live under this one lock.
        if opened_span:
            spans.write(spans_file)
            record_partition(
                lake_root,
                capture_spans.SPANS_PARTITION,
                source=REFERENCE_SOURCE,
                rows=len(spans),
                fetched_at=now.isoformat(),
            )

    # Journal the same verification snapshot as the ticker's first captured cycle,
    # through the capture primitive's own durable path. This runs after the master lock
    # is released, since journal_snapshot takes the lock itself for its manifest append.
    snapshot = capture.journal_snapshot(
        lake_root,
        snapshot_surface,
        ticker,
        body=body,
        cycle_start=cycle_start,
        fetch_ts=fetch_ts,
        fetch_end_ts=fetch_end_ts,
        pid=pid,
        windows=windows,
        absent_markers=absent_markers,
    )
    # The first snapshot's requests go to the timing file under the minute the snapshot
    # landed at, the same as a loop cycle's. Onboarding runs in its own process, and the
    # file takes one ``O_APPEND`` write per line, so this never interleaves with the
    # daemon's lines. It never raises either.
    first_slot = cycle_start.replace(second=0, microsecond=0)
    capture.record_requests(
        lake_root,
        snap_ts=first_slot,
        day=first_slot.date(),
        records=requests,
        where=f"onboarding {ticker}",
    )

    return OnboardReport(
        ticker=ticker,
        instrument_id=instrument_id,
        capture_start=span_start,
        options=options,
        contract_count=contract_count,
        realtime_verified=True,
        tickers_path=written_tickers_path,
        master_path=master_path,
        already_registered=already_registered,
        snapshot_surface=snapshot_surface,
        snapshot_segment=snapshot.partition,
        partial_chain=partial_chain,
        routed_columns=snapshot.routed_columns,
    )


def onboard_from_config(
    ticker: str,
    *,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    tickers_path: str | Path | None = None,
    token_path: str | Path | None = None,
    options: bool = True,
    chain_cadence: str | None = DEFAULT_CHAIN_CADENCE,
    bars: Sequence[str] = DEFAULT_BARS,
    capture_start: datetime | None = None,
) -> OnboardReport:
    """Onboard one ticker wired from the real config and the Schwab-backed vendor.

    This is the entry ``python -m lake.onboard`` calls. It loads the machine-local
    config and builds the Schwab-backed vendor from the token file. The Schwab client is
    built lazily, so importing this module and running the offline suite touch neither it
    nor the network. Most tests drive ``onboard`` directly with a fake vendor and never
    reach here. A test that needs the command itself, and so needs this wrapper, replaces
    ``lake.schwab.SchwabVendor`` instead. That is the one seam, because the import below
    is function-local and there is no name in this module to patch.

    It reads the chain plan and passes the config's guards, the way
    ``fill_option_close_from_config`` does, so a nightly plan rewrite reaches the next
    onboarding rather than the next restart. This is the only place the plan file is
    read, because ``onboard`` itself takes everything injected and falls back to the
    built-in plan rather than to a file. ``load_chain_plan`` never raises, so a missing,
    unreadable, or invalid file lands on that same built-in default.
    """
    from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor

    config = load_config(config_path)
    resolved_clock = clock if clock is not None else SystemClock()
    vendor = SchwabVendor.from_token(
        token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
        clock=resolved_clock,
    )
    return onboard(
        ticker,
        clock=resolved_clock,
        vendor=vendor,
        lake_root=config.lake_root,
        tickers_path=tickers_path,
        options=options,
        chain_cadence=chain_cadence,
        bars=bars,
        capture_start=capture_start,
        plan=load_chain_plan(),
        guards=config.guards,
    )


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m lake.onboard",
        description="Onboard one ticker into the lake: register, verify, and write the roster.",
    )
    parser.add_argument("ticker", help="The ticker to onboard, like SPY.")
    parser.add_argument(
        "--no-options",
        dest="options",
        action="store_false",
        help="Onboard as an equity-only ticker (no option chain).",
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    parser.add_argument("--tickers", help="Path to tickers.yaml (defaults to the standard place).")
    parser.add_argument("--token", help="Path to token.json (defaults to the standard location).")
    parser.add_argument(
        "--chain-cadence",
        default=DEFAULT_CHAIN_CADENCE,
        help="Chain capture cadence for an options ticker, like 1m.",
    )
    parser.add_argument(
        "--capture-start",
        type=_parse_capture_start,
        help=(
            "The instant capture began, as an ISO-8601 instant carrying an offset, like "
            "2026-09-08T17:07:00+00:00. Defaults to now. Pass it to seed a lake that was "
            "captured before its security master existed."
        ),
    )
    parser.add_argument(
        "--bars",
        nargs="*",
        default=list(DEFAULT_BARS),
        help="Bar frequencies to capture, like 1m 1d.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.onboard`` entry. Returns a process exit code.

    A refusal prints one line and exits 2, the same shape ``input_errors_exit`` uses and
    the code the sibling commands use for an operator mistake. Every refusal message here
    is written for a person, and one of them names the exact command to run next, so a
    stack above it is a stack to read past. The catch sits beside ``input_errors_exit``
    rather than inside it, the way ``retire`` and ``reauth`` both do, because that helper
    means the three operator-editable files in the config directory and a refused
    precondition is a different category.

    It catches ``OnboardError`` and nothing wider. The master's own errors, the spans
    file's, and the manifest's keep their tracebacks on purpose. A refused onboarding is a
    normal outcome of the command, while a master that fails to resolve is a bug or a
    corrupt lake, and there the stack is what a reader needs.
    """
    args = _build_parser().parse_args(argv)
    try:
        with input_errors_exit("onboard"):
            report = onboard_from_config(
                args.ticker,
                config_path=args.config,
                tickers_path=args.tickers,
                token_path=args.token,
                options=args.options,
                chain_cadence=args.chain_cadence,
                bars=args.bars,
                capture_start=args.capture_start,
            )
    except OnboardError as exc:
        print(f"onboard: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    print(report.render())
    return 0


__all__ = [
    "DEFAULT_BARS",
    "DEFAULT_CHAIN_CADENCE",
    "MASTER_PARTITION",
    "EntitlementError",
    "OnboardError",
    "OnboardReport",
    "main",
    "onboard",
    "onboard_from_config",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
