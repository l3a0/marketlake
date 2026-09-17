"""The cassette recorder.

A cassette is a saved vendor response replayed offline, so a test never touches the
network. Its format lives in ``lake.cassette``. This module is the by-hand tool that
captures a real Schwab call into that format. The captured cassette then replays
through ``CassetteVendor`` in the offline suite.

The recorder resolves credentials, builds the real vendor from them, asks it for each
requested chain, quote batch and price-history window, and writes each reply into an
``Interaction`` keyed exactly the way ``CassetteVendor`` looks it up. Two injection points
keep it testable offline.

1. The credentials arrive as plain-string arguments, never fetched inside the record
   logic. So a test passes fake strings.
2. The vendor is built through an injected factory that defaults to the real
   ``SchwabVendor.from_token``. So a test injects a factory returning a fake-client
   vendor, and the shaping runs with no network and no real token.

Run it by hand to record from the real vendor::

    uv run python -m lake.record --out spy.json --chain SPY --chain QQQ --quotes SPY,QQQ
    uv run python -m lake.record --out bars.json \
        --bars SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00
    uv run python -m lake.record --out flagged.json \
        --bars SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00,extended_hours=false

That path builds the real client and reads credentials from ``config.yaml``, so it is
a live check, never a continuous-integration step. Credentials come from D1's config
loader, the one source shared with the daemon's auth. They never live in the repo or
the environment. Any cassette committed to the repo must be synthetic or sanitized. A
recording from a real account carries real market data and must not be checked in.

**An existing ``--out`` is refused rather than overwritten.** A recording costs a live
token and a moment of market hours that does not come back, so silently replacing one
with a second run's output is a loss nothing can undo. Pass ``--force`` to overwrite on
purpose. This is a decision rather than the default that was there before it.

**One window is what a recording is for.** A price-history fixture set wants more windows
than a live session is worth burning requests on, so record one and build the rest with
``tests.support.vendor.bars_interactions``. That is the same division the chain recording
already uses, where one bare-symbol body feeds ``windowed_chain_interactions``.

The second and third examples are one window asked twice, differing only in ``extended_hours``,
so the pair reads what Schwab picks when the flag is left unset and what it sends when the flag
says the regular session. Two answers about one window, which is what a recording is for, rather
than a fixture set.

**They are two runs on purpose, and two ``--out`` paths.** ``record_cassette`` accumulates in
memory and ``main`` writes only after the last request returns, so a vendor failure on the second
of two windows in one run throws away the first, which is already paid for. #443 carries that.
Until it is settled, the pair is recorded as two invocations.

Every example carries ``uv run`` because a bare ``python`` is not on the path this is run from. An
example that cannot be pasted is not an example.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from lake.cassette import Cassette, Interaction, dump_cassette
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor
from lake.vendor import (
    BAR_FREQS,
    BARS_ENDPOINT,
    MINUTE_FREQ,
    Vendor,
    VendorError,
    VendorResponse,
    bars_params,
    require_bar_freq,
    require_utc_bound,
)

# How the recorder builds a vendor from a token path and resolved credentials. The
# default is the real factory. A test injects one that returns a fake-client vendor.
VendorFactory = Callable[..., Vendor]

# The one named field ``--bars`` reads after its four positional ones, spelled the way the vendor
# seam spells the parameter. ``previous_close`` is the seam's other flag and is deliberately not
# here, because nothing asks the recorder for it.
BAR_FLAG_NAME = "extended_hours"

# The only two value spellings. The seam forwards the value to ``schwab-py`` unchanged, which puts
# it straight into ``params["needExtendedHoursData"]``, so ``1`` and ``True`` leave as different
# query values. The cassette key cannot tell them apart, because Python reads
# ``{"extended_hours": 1} == {"extended_hours": True}`` as equal. A recording keyed that way is
# found, and answers a request it never made. Reading only the two bools is what prevents it.
BAR_FLAG_VALUES = {"true": True, "false": False}


def _interaction(endpoint: str, params: dict, response: VendorResponse) -> Interaction:
    """Shape one verbatim ``VendorResponse`` into a recorded ``Interaction``.

    The ``params`` key must match what ``CassetteVendor`` queries: ``{"symbol": s}``
    for a chain, ``{"symbols": [...]}`` for a quote batch, and whatever
    ``lake.vendor.bars_params`` builds for a price-history window. The third is built by
    that shared function rather than spelled again here, because its key carries a
    timezone normalization a second spelling would get wrong. The body and headers are
    copied into plain dicts so the recording does not alias live state. Nothing in the
    body is inspected.
    """
    return Interaction(
        endpoint=endpoint,
        params=params,
        status=response.status,
        body=dict(response.body),
        headers=dict(response.headers),
    )


@dataclass(frozen=True)
class BarRequest:
    """One price-history window to record.

    ``freq`` is ``"1m"`` or ``"1d"``, the two the vendor seam has a call for. ``start``
    and ``end`` are both required and both timezone-aware, because ``schwab-py``
    substitutes a fifty-five year window for a missing bound and reads a naive one in the
    host's local zone.

    ``extended_hours`` decides whether the response covers the regular session or the whole
    extended one. Left ``None`` it is omitted from both the request and the cassette key, and
    Schwab picks, which is the third state rather than a false. It is refused unless it is
    exactly a ``bool`` or ``None``, for the reason ``BAR_FLAG_VALUES`` carries: the seam forwards
    it unchanged while the key cannot tell ``1`` from ``True``.
    """

    symbol: str
    freq: str
    start: datetime
    end: datetime
    extended_hours: bool | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol is empty")
        require_bar_freq(self.freq)
        if self.extended_hours is not None and not isinstance(self.extended_hours, bool):
            raise ValueError(f"extended_hours {self.extended_hours!r} is not True, False or None")
        start = require_utc_bound(self.start, "start")
        end = require_utc_bound(self.end, "end")
        if start >= end:
            # Schwab answers a backwards window with the empty shape rather than an error,
            # so a transposed pair would spend a live request and record "no candles" as
            # the answer for a window nobody asked for.
            raise ValueError(f"start {start.isoformat()} is not before end {end.isoformat()}")


def _bar_request_key(request: BarRequest) -> dict:
    """The cassette key one ``BarRequest`` records under.

    Written once and used twice, by the recorder that writes the key and by the duplicate
    refusal that compares keys. A second spelling could drift from the first and let the guard
    pass a pair the recording then cannot tell apart.

    It is a key and not the request. ``_key_instant`` truncates each bound to the millisecond
    ``schwab-py`` puts on the wire, on purpose, so two ``BarRequest`` objects that differ below
    that are unequal objects with one key.
    """
    return bars_params(
        request.symbol,
        request.freq,
        start=request.start,
        end=request.end,
        extended_hours=request.extended_hours,
    )


def record_cassette(
    api_key: str,
    app_secret: str,
    chain_symbols: Sequence[str] = (),
    quote_batches: Sequence[Sequence[str]] = (),
    bar_requests: Sequence[BarRequest] = (),
    *,
    token_path: str | Path = DEFAULT_TOKEN_PATH,
    vendor_factory: VendorFactory = SchwabVendor.from_token,
) -> Cassette:
    """Record a cassette by building a vendor and calling it for each interaction.

    ``api_key`` and ``app_secret`` are the resolved credentials, injected as plain
    strings. ``token_path`` is the token file to authenticate with, the fixed
    convention by default. ``vendor_factory`` builds the vendor from those three, so
    a test injects a factory that returns a fake-client vendor and never touches the
    network. ``chain_symbols`` are the underlyings to record full chains for. Each
    ``quote_batches`` entry is one batched quote request, a list of symbols recorded
    together the way the shared sampler batches them. Each ``bar_requests`` entry is one
    price-history window, recorded through whichever per-frequency vendor method its
    ``freq`` names, carrying whatever ``extended_hours`` it names.

    The recorder makes exactly the calls requested, in order, and never reaches past
    them. So the resulting cassette replays deterministically. The token mint time is
    read off the built vendor and stamped in, so the replayed fake reports it. A
    vendor with no mint time simply omits it.
    """
    vendor = vendor_factory(token_path, api_key=api_key, app_secret=app_secret)

    interactions: list[Interaction] = []
    for symbol in chain_symbols:
        response = vendor.get_chain(symbol)
        interactions.append(_interaction("chains", {"symbol": symbol}, response))
    for batch in quote_batches:
        symbols = list(batch)
        response = vendor.get_quotes(symbols)
        interactions.append(_interaction("quotes", {"symbols": symbols}, response))
    for request in bar_requests:
        fetch = vendor.get_minute_bars if request.freq == MINUTE_FREQ else vendor.get_daily_bars
        response = fetch(
            request.symbol,
            start=request.start,
            end=request.end,
            extended_hours=request.extended_hours,
        )
        interactions.append(
            _interaction(
                BARS_ENDPOINT,
                _bar_request_key(request),
                response,
            )
        )

    try:
        mint = vendor.token_mint_time().isoformat()
    except VendorError:
        mint = None
    return Cassette(interactions=tuple(interactions), token_mint_time=mint)


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for the by-hand recorder.

    Factored out so the argument shaping is unit-testable without running a real
    fetch. ``--chain`` repeats per underlying. ``--quotes`` repeats per batch, each a
    comma-separated symbol list. ``--bars`` repeats per window.

    ``--bars`` carries four fields where the other two carry one, because a price-history
    request needs a symbol, a frequency and two bounds, and neither bound may be omitted.
    The convention the other flags set, one repeatable flag per endpoint whose value
    encodes the request, extends by making the value comma-separated the way ``--quotes``
    already is.

    One ISO spelling collides with that separator. ISO 8601 allows a comma as the
    fractional-second marker and ``datetime.fromisoformat`` accepts it, so
    ``2026-09-14T09:30:00,500-04:00`` splits into two fields. The refusal names that case
    rather than only counting fields, because a bound written that way is a real spelling
    and not a typo. A session bound carries no fractional seconds, so nothing on the
    intended path meets it.

    A vendor flag rides after the four as a named field, ``extended_hours=false``, rather than
    as a bare fifth one. A field is taken as a flag only when it carries an ``=`` and does not
    read as an instant, so a bound that split on its own fractional-second comma reaches the
    four-field refusal with the sentence naming the case.

    Both halves of that test are load-bearing, and review is what found the second one. An ``=``
    alone does not mean a flag, because ``datetime.fromisoformat`` accepts
    ``2026-09-14T16:00:00=-04:00``: 3.12 takes any non-digit as the fractional-second separator
    when no fractional digits follow and an offset comes next. So the separation rests on asking
    the bound reader, not on a claim about which characters an instant can hold.

    A bare fifth field read as the flag whatever it said would lose it. The ISO case would
    arrive as a bad flag value naming the end bound, and the fractional-second sentence would
    never be reached. A bare fifth field popped only when it spells ``true`` or ``false`` would
    in fact keep it, because an ISO fractional part is digits and never spells either word. But
    that rests the guard on a coincidence about the data, and a bare value says nothing at the
    terminal about what it means. So a bare fifth field is refused, and a test covers it.

    The named field is read in trailing position only, so the four positional fields stay
    positional. The ``metavar`` shows that position, because a flag written in the middle falls
    to the four-field refusal and collects the fractional-second sentence, which blames the
    wrong thing.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.record",
        description="Record Schwab responses into a replayable cassette (by-hand live tool).",
    )
    parser.add_argument("--out", required=True, help="Path to write the cassette JSON to.")
    parser.add_argument(
        "--chain",
        action="append",
        default=[],
        dest="chains",
        metavar="SYMBOL",
        help="Record the full option chain for this underlying. Repeatable.",
    )
    parser.add_argument(
        "--quotes",
        action="append",
        default=[],
        dest="quote_batches",
        metavar="SYM1,SYM2",
        help="Record one batched quote request for this comma-separated list. Repeatable.",
    )
    parser.add_argument(
        "--bars",
        action="append",
        default=[],
        dest="bar_requests",
        metavar=f"SYMBOL,FREQ,START,END[,{BAR_FLAG_NAME}=true|false]",
        help=(
            "Record one price-history window. FREQ is "
            f"{' or '.join(BAR_FREQS)}. START and END are timezone-aware ISO instants, "
            f"both required. A trailing {BAR_FLAG_NAME}=true or {BAR_FLAG_NAME}=false sets the "
            "vendor flag and keys the recording on it. Left off, the flag is omitted from both "
            "and Schwab picks. Repeatable."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing --out path. Without it an existing path is refused.",
    )
    parser.add_argument(
        "--token",
        default=str(DEFAULT_TOKEN_PATH),
        help="Path to the Schwab token file. Defaults to the design's standard location.",
    )
    return parser


def _parse_quote_batches(raw_batches: Sequence[str]) -> list[list[str]]:
    """Split each ``--quotes`` value into its symbol list."""
    return [
        [symbol.strip() for symbol in batch.split(",") if symbol.strip()] for batch in raw_batches
    ]


def _splits_an_instant(fields: list[str]) -> bool:
    """Whether two adjacent fields rejoin into one readable instant.

    ISO 8601 allows a comma as the fractional-second marker, so ``2026-09-14T09:30:00,500-04:00``
    arrives here as two fields. This demonstrates that case rather than guessing at it: the two
    halves are joined back with the comma that split them and handed to the same parser that reads
    a bound. Nothing else in a ``--bars`` value does that.

    Counting fields cannot do this job any more. Before the flag existed, more than four fields
    almost always meant the marker. Now the likelier cause is a mistyped flag, and three of those
    are realistic: ``extended_hours: false`` with a colon, a bare ``false``, and a trailing comma.
    Each of them used to collect a sentence telling the operator to go and fix a fractional second
    they never wrote.

    A pattern match cannot do it either. The obvious one, seconds followed by a comma and a digit,
    matches every ordinary value, because the field separator is itself a comma and a bound ends
    in ``:00`` right before it.
    """
    for first, second in zip(fields, fields[1:], strict=False):
        try:
            datetime.fromisoformat(f"{first},{second}")
        except ValueError:
            continue
        return True
    return False


def _names_a_flag(field: str) -> bool:
    """Whether one field is a ``name=value`` flag rather than a bound.

    An ``=`` alone does not settle it. ``datetime.fromisoformat`` in 3.12 takes any non-digit as
    the fractional-second separator when no fractional digits follow and an offset comes next, so
    ``2026-09-14T16:00:00=-04:00`` is a bound this tool reads. Popping that as a flag would refuse
    a real window while blaming a named field. Asking the bound reader first is the same move the
    fractional-second refusal makes: demonstrate what a field is rather than guess from a
    character it happens to carry.
    """
    if "=" not in field:
        return False
    try:
        datetime.fromisoformat(field)
    except ValueError:
        return True
    return False


def _take_bar_flag(raw: str, fields: list[str]) -> bool | None:
    """Take the named fields off the end of one ``--bars`` value and read the flag.

    ``fields`` is shortened in place, so the caller counts positional fields afterwards and its
    four-field refusal keeps naming the ISO comma case. Only the trailing run of ``name=value``
    fields is taken, which is what leaves the four positional fields positional.
    """
    named: list[str] = []
    while fields and _names_a_flag(fields[-1]):
        named.insert(0, fields.pop())

    flag: bool | None = None
    for field in named:
        name, _, value = field.partition("=")
        name, value = name.strip().lower(), value.strip().lower()
        if name != BAR_FLAG_NAME:
            raise ValueError(
                f"--bars {raw!r} carries a named field {name!r}. The one name it reads is "
                f"{BAR_FLAG_NAME}."
            )
        if flag is not None:
            raise ValueError(f"--bars {raw!r} carries {BAR_FLAG_NAME} more than once")
        if value not in BAR_FLAG_VALUES:
            raise ValueError(
                f"--bars {raw!r} spells {BAR_FLAG_NAME} as {value!r}. It spells "
                f"{' or '.join(BAR_FLAG_VALUES)} and nothing else, because the value reaches "
                "Schwab exactly as written."
            )
        flag = BAR_FLAG_VALUES[value]
    return flag


def _parse_bar_requests(raw_requests: Sequence[str]) -> list[BarRequest]:
    """Split each ``--bars`` value into a ``BarRequest``.

    Every refusal here raises ``ValueError`` with a line naming the value and what was
    wrong with it. ``main`` hands that line to ``argparse``, so an operator who mistypes a
    bound reads one sentence and an exit code rather than a stack trace, and reads it
    before any live request goes out. That is also why the duplicate refusal below lives here
    rather than in ``record_cassette``, which would cover every caller and reach the operator as
    a stack trace, because ``main`` wraps only this function and ``check_out_path``.
    """
    requests: list[BarRequest] = []
    keys: list[dict] = []
    for raw in raw_requests:
        fields = [field.strip() for field in raw.split(",")]
        extended_hours = _take_bar_flag(raw, fields)
        if len(fields) != 4:
            hint = (
                ". An ISO instant may use a comma for fractional seconds, which splits "
                "here. Write it with a period, like 2026-09-14T09:30:00.500-04:00."
                if _splits_an_instant(fields)
                else ""
            )
            raise ValueError(
                f"--bars {raw!r} needs four comma-separated fields, SYMBOL,FREQ,START,END, "
                f"optionally followed by {BAR_FLAG_NAME}=true or {BAR_FLAG_NAME}=false, "
                f"and carries {len(fields)}{hint}"
            )
        symbol, freq, start_text, end_text = fields
        try:
            start = datetime.fromisoformat(start_text)
            end = datetime.fromisoformat(end_text)
        except ValueError as exc:
            raise ValueError(f"--bars {raw!r} has an unreadable instant: {exc}") from exc
        try:
            request = BarRequest(
                symbol=symbol,
                freq=freq,
                start=start,
                end=end,
                extended_hours=extended_hours,
            )
        except ValueError as exc:
            raise ValueError(f"--bars {raw!r}: {exc}") from exc
        key = _bar_request_key(request)
        if key in keys:
            # ``Cassette.find`` matches on params and returns the first hit, so a second
            # interaction keyed alike is reachable only by index and never by the replay. The
            # flag is what makes that the shape of the intended run: one window recorded twice
            # differing in one field, where an operator who writes the field once has written two
            # identical values. Recording one window twice on purpose is a real thing to want, so
            # the message says how, rather than the refusal pretending nobody could mean it.
            #
            # Chains and quotes can be duplicated the same way and are not refused here. #442
            # carries them, because neither reaches a parse step inside the ``try`` that turns a
            # refusal into one line.
            raise ValueError(
                f"--bars {raw!r} asks for a window an earlier --bars already asked for. Two "
                "requests keyed alike record two interactions the replay cannot tell apart, and "
                f"only the first is ever found. Vary the window or the {BAR_FLAG_NAME} flag, "
                "drop one, or record the pair as two runs with two --out paths."
            )
        keys.append(key)
        requests.append(request)
    return requests


def check_out_path(path: str | Path, *, force: bool = False) -> Path:
    """Refuse an ``--out`` that already holds a recording, unless overwriting is asked for.

    A recording costs a live token and a moment of market hours that does not come back.
    ``dump_cassette`` writes whatever path it is given, so a second run against the same
    ``--out`` used to replace the first silently. Refusing by default makes overwriting a
    thing the operator asks for, and ``--force`` is how they ask.

    The directory is checked for the same reason, and it is the half that costs more.
    ``dump_cassette`` is a plain ``write_text``, so a missing parent raises only once the
    fetch has already happened. That loses the recording the request just paid for, which
    is the exact loss the overwrite refusal exists to prevent, arriving through a door the
    overwrite check does not cover.
    """
    resolved = Path(path)
    parent = resolved.parent
    if not parent.is_dir():
        raise ValueError(
            f"--out {resolved} names a directory that does not exist, {parent}. "
            "Create it first, because a recording that cannot be written is a live "
            "request already spent."
        )
    if resolved.exists() and not force:
        raise ValueError(
            f"--out {resolved} already exists. A recording costs a live request, so it is "
            "not overwritten by default. Pass --force to replace it, or name another path."
        )
    return resolved


def main(argv: Sequence[str] | None = None) -> int:
    """Record a cassette from the real vendor and write it to disk.

    This path builds the real ``schwab-py`` client, so it needs a token and
    credentials and only runs by hand. Credentials come from ``config.yaml`` through
    D1's config loader, never the repo or the environment. The import is lazy, the
    same discipline ``SchwabVendor.from_token`` uses for ``schwab-py``, so this module
    imports and the whole suite runs even where ``lake.config`` is absent. The
    recorded cassette is for local diagnosis. Do not commit a recording from a real
    account.

    Both argument refusals run before the credentials are loaded and before any request
    goes out, so a mistyped window or an ``--out`` that already holds a recording costs
    nothing.
    """
    from lake.config import input_errors_exit, load_config  # lazy: D1 dependency, live only

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        bar_requests = _parse_bar_requests(args.bar_requests)
        out = check_out_path(args.out, force=args.force)
    except ValueError as exc:
        parser.error(str(exc))
    with input_errors_exit("record"):
        cfg = load_config()
    api_key = cfg.schwab_api_key.reveal()
    app_secret = cfg.schwab_app_secret.reveal()

    cassette = record_cassette(
        api_key,
        app_secret,
        chain_symbols=args.chains,
        quote_batches=_parse_quote_batches(args.quote_batches),
        bar_requests=bar_requests,
        token_path=args.token,
    )
    dump_cassette(cassette, out)
    print(f"wrote {len(cassette.interactions)} interactions to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
