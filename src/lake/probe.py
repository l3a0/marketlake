"""The live chain-size probe.

This is a by-hand live tool. It measures how big an option chain is, so a human can
size the chunk constant. That constant is the number of expirations the design's
chunk-by-expiration fallback fetches per request when one whole-chain request is too
big. The probe makes real Schwab calls, so it never runs in continuous integration.

It follows ``record.py``'s discipline exactly. Two things make that true.

1. Credentials come from ``load_config`` at runtime, never from the repo or the
   environment. The import of the config loader is lazy, inside ``main``.
2. The real ``schwab-py`` client is built lazily in a factory that runs live only. The
   measurement logic takes an already-built client as an argument, so a test injects a
   fake client and the whole thing runs offline with no network and no token.

For a given underlying the probe prints three measurements.

1. Discovery: ``get_option_chain(sym, strike_count=1)``. It reports the status, the
   response byte size, the number of distinct expirations, and the first and last
   expiration. Expirations are parsed from the ``callExpDateMap`` keys, which Schwab
   formats as ``"YYYY-MM-DD:DTE"``.
2. One expiration, full strikes: ``get_option_chain(sym, from_date=d, to_date=d)`` for
   the nearest expiration. It reports the status, the bytes, and ``numberOfContracts``.
3. Window brackets: for each offset in ``(4, 8, 16, 32)`` it fetches from the first
   expiration through the offset-th one. It reports the status, the bytes,
   ``numberOfContracts``, and ``isChainTruncated``. A ``502`` with a
   ``protocol.http.TooBigBody`` fault then shows exactly where the body limit bites.

Run it by hand against the real vendor::

    python -m lake.probe SPY

That path builds the real client and reads credentials from ``config.yaml``. It is a
live check, never a continuous-integration step. The client is built with
``enforce_enums=False``, the same setting the recorder uses, so plain strings pass
through where ``schwab-py`` would otherwise validate its own enums.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake.schwab import DEFAULT_TOKEN_PATH

# The window brackets, in expirations from the front of the chain. Each names how many
# expirations past the first the bracket reaches, so the body-size growth is visible.
WINDOW_OFFSETS = (4, 8, 16, 32)


@runtime_checkable
class ChainResponse(Protocol):
    """The slice of a ``schwab-py`` reply the probe reads.

    The real client hands back an ``httpx.Response``. Only these three members matter
    here: the status code, the raw response text for the byte measurement, and the
    parsed JSON body. A test fake implements the same three.
    """

    @property
    def status_code(self) -> int:
        """The HTTP status code."""
        ...

    @property
    def text(self) -> str:
        """The raw response text, used for the byte measurement."""
        ...

    def json(self) -> Mapping[str, object]:
        """The parsed JSON body, exactly as the vendor sent it."""
        ...


@runtime_checkable
class ChainClient(Protocol):
    """What the probe needs from a ``schwab-py`` client.

    One endpoint method is the whole contract. The real ``schwab.client.Client``
    satisfies it, and so does a test fake. The probe only ever passes ``strike_count``,
    ``from_date``, and ``to_date``, so those are the only parameters pinned here.
    """

    def get_option_chain(
        self,
        symbol: str,
        *,
        strike_count: int | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> ChainResponse:
        """One option-chain request for the underlying."""
        ...


# How the probe builds a client from a token path and resolved credentials. The default
# is the real factory. A test never uses it; it injects a fake client into the
# measurement function directly.
ClientFactory = Callable[..., ChainClient]


@dataclass(frozen=True)
class Discovery:
    """The discovery measurement: how many expirations the chain spans."""

    status: int
    n_bytes: int
    expirations: tuple[str, ...]

    @property
    def n_expirations(self) -> int:
        """The number of distinct expirations found."""
        return len(self.expirations)

    @property
    def first(self) -> str | None:
        """The nearest expiration, or ``None`` when the chain is empty."""
        return self.expirations[0] if self.expirations else None

    @property
    def last(self) -> str | None:
        """The farthest expiration, or ``None`` when the chain is empty."""
        return self.expirations[-1] if self.expirations else None


@dataclass(frozen=True)
class Measurement:
    """One chain fetch's size: status, byte size, and the vendor's own counts.

    ``number_of_contracts`` is the vendor's ``numberOfContracts``. ``is_truncated`` is
    its ``isChainTruncated``. Both are ``None`` on an error body, such as the ``502``
    ``TooBigBody`` fault, because such a body carries neither field.
    """

    status: int
    n_bytes: int
    number_of_contracts: int | None
    is_truncated: bool | None


@dataclass(frozen=True)
class Bracket:
    """One window bracket: the offset it reached and the fetch it produced."""

    offset: int
    to_expiration: str
    measurement: Measurement


@dataclass(frozen=True)
class ChainSizeReport:
    """The whole probe result for one underlying."""

    symbol: str
    discovery: Discovery
    nearest_expiration: str | None
    nearest: Measurement | None
    brackets: tuple[Bracket, ...]


def _response_bytes(response: ChainResponse) -> int:
    """The response byte size.

    It reads ``len(response.text)``. If the fake or the reply carries no text, it falls
    back to ``len(json.dumps(response.json()))``.
    """
    text = getattr(response, "text", None)
    if isinstance(text, str):
        return len(text)
    return len(json.dumps(response.json()))


def _expirations(body: Mapping[str, object]) -> list[str]:
    """The sorted distinct expiration dates parsed from ``callExpDateMap``.

    Each key is ``"YYYY-MM-DD:DTE"``, so the date part is everything before the first
    colon. Duplicate dates across strikes collapse to one.
    """
    exp_map = body.get("callExpDateMap")
    if not isinstance(exp_map, Mapping):
        return []
    return sorted({str(key).split(":", 1)[0] for key in exp_map})


def _number_of_contracts(body: Mapping[str, object]) -> int | None:
    """The vendor's ``numberOfContracts``, or ``None`` when it is absent or non-int."""
    value = body.get("numberOfContracts")
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _is_truncated(body: Mapping[str, object]) -> bool | None:
    """The vendor's ``isChainTruncated``, or ``None`` when it is absent or non-bool."""
    value = body.get("isChainTruncated")
    return value if isinstance(value, bool) else None


def _measure(response: ChainResponse) -> Measurement:
    """Shape one chain response into a ``Measurement``."""
    body = response.json()
    mapping: Mapping[str, object] = body if isinstance(body, Mapping) else {}
    return Measurement(
        status=response.status_code,
        n_bytes=_response_bytes(response),
        number_of_contracts=_number_of_contracts(mapping),
        is_truncated=_is_truncated(mapping),
    )


def measure_chain_size(client: ChainClient, symbol: str) -> ChainSizeReport:
    """Measure one underlying's chain size through an injected client.

    The client is already built. In production ``main`` builds the real one. In a test
    it is a fake with the same method shape, so this runs offline. The three
    measurements fire in order: discovery, then the nearest full expiration, then each
    window bracket that the chain is long enough to reach.
    """
    discovery_response = client.get_option_chain(symbol, strike_count=1)
    discovery_body = discovery_response.json()
    body: Mapping[str, object] = discovery_body if isinstance(discovery_body, Mapping) else {}
    expirations = _expirations(body)
    discovery = Discovery(
        status=discovery_response.status_code,
        n_bytes=_response_bytes(discovery_response),
        expirations=tuple(expirations),
    )

    nearest_expiration = expirations[0] if expirations else None
    nearest: Measurement | None = None
    if nearest_expiration is not None:
        nearest_date = date.fromisoformat(nearest_expiration)
        nearest = _measure(
            client.get_option_chain(symbol, from_date=nearest_date, to_date=nearest_date)
        )

    brackets: list[Bracket] = []
    if expirations:
        first_date = date.fromisoformat(expirations[0])
        for offset in WINDOW_OFFSETS:
            if offset >= len(expirations):
                continue
            to_expiration = expirations[offset]
            measurement = _measure(
                client.get_option_chain(
                    symbol, from_date=first_date, to_date=date.fromisoformat(to_expiration)
                )
            )
            brackets.append(
                Bracket(offset=offset, to_expiration=to_expiration, measurement=measurement)
            )

    return ChainSizeReport(
        symbol=symbol,
        discovery=discovery,
        nearest_expiration=nearest_expiration,
        nearest=nearest,
        brackets=tuple(brackets),
    )


def render_report(report: ChainSizeReport) -> str:
    """Render a report as human-readable lines for the console."""
    lines = [f"{report.symbol}:"]
    discovery = report.discovery
    lines.append(
        f"  discovery: status={discovery.status} bytes={discovery.n_bytes} "
        f"expirations={discovery.n_expirations} "
        f"first={discovery.first} last={discovery.last}"
    )
    if report.nearest is not None:
        nearest = report.nearest
        lines.append(
            f"  nearest {report.nearest_expiration}: status={nearest.status} "
            f"bytes={nearest.n_bytes} contracts={nearest.number_of_contracts}"
        )
    for bracket in report.brackets:
        measurement = bracket.measurement
        lines.append(
            f"  window +{bracket.offset} (-> {bracket.to_expiration}): "
            f"status={measurement.status} bytes={measurement.n_bytes} "
            f"contracts={measurement.number_of_contracts} "
            f"truncated={measurement.is_truncated}"
        )
    return "\n".join(lines)


def _client_from_token(
    token_path: str | Path = DEFAULT_TOKEN_PATH,
    *,
    api_key: str,
    app_secret: str,
) -> ChainClient:
    """Build the real client from a token file.

    This is the one place ``schwab-py`` is imported, and it is imported lazily. So
    ``import lake.probe`` and the whole unit suite run without the library. The client
    is built with ``enforce_enums=False`` so plain strings pass through, matching the
    recorder. This factory runs only in the by-hand live check.
    """
    from schwab.auth import client_from_token_file  # lazy: real dep, live only

    return client_from_token_file(str(token_path), api_key, app_secret, enforce_enums=False)


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for the by-hand probe.

    Factored out so the argument shaping is unit-testable without a real fetch. The
    positional ``symbol`` repeats and defaults to ``SPY``.
    """
    parser = argparse.ArgumentParser(
        prog="python -m lake.probe",
        description="Measure live option-chain size to size the chunk constant (by-hand tool).",
    )
    parser.add_argument(
        "symbol",
        nargs="*",
        default=["SPY"],
        help="Underlying symbol(s) to probe. Defaults to SPY.",
    )
    parser.add_argument(
        "--token",
        default=str(DEFAULT_TOKEN_PATH),
        help="Path to the Schwab token file. Defaults to the design's standard location.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Probe each underlying against the real vendor and print its report.

    This path builds the real ``schwab-py`` client, so it needs a token and credentials
    and only runs by hand. Credentials come from ``config.yaml`` through the config
    loader, never the repo or the environment. The import is lazy, the same discipline
    the recorder uses, so this module imports and the suite runs even where
    ``lake.config`` is absent.
    """
    from lake.config import load_config  # lazy: D1 dependency, live only

    args = build_parser().parse_args(argv)
    cfg = load_config()
    api_key = cfg.schwab_api_key.reveal()
    app_secret = cfg.schwab_app_secret.reveal()

    client = _client_from_token(args.token, api_key=api_key, app_secret=app_secret)
    for symbol in args.symbol:
        print(render_report(measure_chain_size(client, symbol)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
