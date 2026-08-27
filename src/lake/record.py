"""The cassette recorder.

A cassette is a saved vendor response replayed offline, so a test never touches the
network. Its format lives in ``lake.cassette``. This module is the by-hand tool that
captures a real Schwab call into that format. The captured cassette then replays
through ``CassetteVendor`` in the offline suite.

The recorder resolves credentials, builds the real vendor from them, asks it for each
requested chain and quote batch, and writes each reply into an ``Interaction`` keyed
exactly the way ``CassetteVendor`` looks it up. Two injection points keep it testable
offline.

1. The credentials arrive as plain-string arguments, never fetched inside the record
   logic. So a test passes fake strings.
2. The vendor is built through an injected factory that defaults to the real
   ``SchwabVendor.from_token``. So a test injects a factory returning a fake-client
   vendor, and the shaping runs with no network and no real token.

Run it by hand to record from the real vendor::

    python -m lake.record --out spy.json --chain SPY --chain QQQ --quotes SPY,QQQ

That path builds the real client and reads credentials from ``config.yaml``, so it is
a live check, never a continuous-integration step. Credentials come from D1's config
loader, the one source shared with the daemon's auth. They never live in the repo or
the environment. Any cassette committed to the repo must be synthetic or sanitized. A
recording from a real account carries real market data and must not be checked in.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from pathlib import Path

from lake.cassette import Cassette, Interaction, dump_cassette
from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor
from lake.vendor import Vendor, VendorError, VendorResponse

# How the recorder builds a vendor from a token path and resolved credentials. The
# default is the real factory. A test injects one that returns a fake-client vendor.
VendorFactory = Callable[..., Vendor]


def _interaction(endpoint: str, params: dict, response: VendorResponse) -> Interaction:
    """Shape one verbatim ``VendorResponse`` into a recorded ``Interaction``.

    The ``params`` key must match what ``CassetteVendor`` queries: ``{"symbol": s}``
    for a chain and ``{"symbols": [...]}`` for a quote batch. The body and headers
    are copied into plain dicts so the recording does not alias live state. Nothing
    in the body is inspected.
    """
    return Interaction(
        endpoint=endpoint,
        params=params,
        status=response.status,
        body=dict(response.body),
        headers=dict(response.headers),
    )


def record_cassette(
    api_key: str,
    app_secret: str,
    chain_symbols: Sequence[str] = (),
    quote_batches: Sequence[Sequence[str]] = (),
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
    together the way the shared sampler batches them.

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

    try:
        mint = vendor.token_mint_time().isoformat()
    except VendorError:
        mint = None
    return Cassette(interactions=tuple(interactions), token_mint_time=mint)


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for the by-hand recorder.

    Factored out so the argument shaping is unit-testable without running a real
    fetch. ``--chain`` repeats per underlying. ``--quotes`` repeats per batch, each a
    comma-separated symbol list.
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


def main(argv: Sequence[str] | None = None) -> int:
    """Record a cassette from the real vendor and write it to disk.

    This path builds the real ``schwab-py`` client, so it needs a token and
    credentials and only runs by hand. Credentials come from ``config.yaml`` through
    D1's config loader, never the repo or the environment. The import is lazy, the
    same discipline ``SchwabVendor.from_token`` uses for ``schwab-py``, so this module
    imports and the whole suite runs even where ``lake.config`` is absent. The
    recorded cassette is for local diagnosis. Do not commit a recording from a real
    account.
    """
    from lake.config import load_config  # lazy: D1 dependency, live only

    args = build_parser().parse_args(argv)
    cfg = load_config()
    api_key = cfg.schwab_api_key.reveal()
    app_secret = cfg.schwab_app_secret.reveal()

    cassette = record_cassette(
        api_key,
        app_secret,
        chain_symbols=args.chains,
        quote_batches=_parse_quote_batches(args.quote_batches),
        token_path=args.token,
    )
    dump_cassette(cassette, args.out)
    print(f"wrote {len(cassette.interactions)} interactions to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
