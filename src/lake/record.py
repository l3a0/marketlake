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

    python -m lake.record --out spy.json --chain SPY --chain QQQ --quotes SPY,QQQ
    python -m lake.record --out bars.json \
        --bars SPY,1m,2026-09-14T09:30:00-04:00,2026-09-14T16:00:00-04:00

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
    """

    symbol: str
    freq: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol is empty")
        require_bar_freq(self.freq)
        start = require_utc_bound(self.start, "start")
        end = require_utc_bound(self.end, "end")
        if start >= end:
            # Schwab answers a backwards window with the empty shape rather than an error,
            # so a transposed pair would spend a live request and record "no candles" as
            # the answer for a window nobody asked for.
            raise ValueError(f"start {start.isoformat()} is not before end {end.isoformat()}")


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
    ``freq`` names.

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
        response = fetch(request.symbol, start=request.start, end=request.end)
        interactions.append(
            _interaction(
                BARS_ENDPOINT,
                bars_params(request.symbol, request.freq, start=request.start, end=request.end),
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
        metavar="SYMBOL,FREQ,START,END",
        help=(
            "Record one price-history window. FREQ is "
            f"{' or '.join(BAR_FREQS)}. START and END are timezone-aware ISO instants, "
            "both required. Repeatable."
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


def _parse_bar_requests(raw_requests: Sequence[str]) -> list[BarRequest]:
    """Split each ``--bars`` value into a ``BarRequest``.

    Every refusal here raises ``ValueError`` with a line naming the value and what was
    wrong with it. ``main`` hands that line to ``argparse``, so an operator who mistypes a
    bound reads one sentence and an exit code rather than a stack trace, and reads it
    before any live request goes out.
    """
    requests: list[BarRequest] = []
    for raw in raw_requests:
        fields = [field.strip() for field in raw.split(",")]
        if len(fields) != 4:
            hint = (
                ". An ISO instant may use a comma for fractional seconds, which splits "
                "here. Write it with a period, like 2026-09-14T09:30:00.500-04:00."
                if len(fields) > 4
                else ""
            )
            raise ValueError(
                f"--bars {raw!r} needs four comma-separated fields, SYMBOL,FREQ,START,END, "
                f"and carries {len(fields)}{hint}"
            )
        symbol, freq, start_text, end_text = fields
        try:
            start = datetime.fromisoformat(start_text)
            end = datetime.fromisoformat(end_text)
        except ValueError as exc:
            raise ValueError(f"--bars {raw!r} has an unreadable instant: {exc}") from exc
        try:
            requests.append(BarRequest(symbol=symbol, freq=freq, start=start, end=end))
        except ValueError as exc:
            raise ValueError(f"--bars {raw!r}: {exc}") from exc
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
