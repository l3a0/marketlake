"""The onboarding command: any ticker, one command.

``python -m lake.onboard TICKER`` brings one instrument into the lake. This is the
slice-1 version of the flow the design pins. It does exactly the steps that stand on
their own with the market closed and the network faked, and it defers the two steps
that belong to later slices with clear notes instead of faking them.

The slice-1 steps, in order.

1. *Register the instrument in the security master.* The security master is the
   reference table that assigns every instrument a stable internal ``instrument_id``
   and maps it to the external symbols. Registration stamps a ``capture_start`` epoch,
   the instant capture begins, taken from the injected clock. All coverage and gap
   accounting clamp to it, so onboarding day reads "onboarded 11:00," never 40 percent
   missing.
2. *Resolve the FIGI.* A FIGI is the one globally-standard, free security identifier.
   It is resolved through an injected resolver seam. The real one queries OpenFIGI over
   HTTP; a test injects a fake. An unresolved FIGI does not block onboarding, since the
   ticker mapping alone identifies the instrument. It is noted in the report.
3. *Take the first snapshot and assert the real-time entitlement.* For an options
   ticker this fetches the full chain and asserts the vendor's ``isDelayed`` flag is
   false. For an equity-only ticker it fetches one quote and asserts the ``realtime``
   flag is true. Real-time entitlement is a verified precondition, not an assumption. A
   delayed feed fails onboarding before the ticker is trusted or written.
4. *Write the roster entry.* The command writes the ``tickers.yaml`` entry itself. The
   roster lives in ``~/.config/marketlake/``, outside the repo, so no machine path or
   secret ever lands in a tracked file.
5. *Persist the master and record it.* The master is written under the lake-root lock
   and given a manifest entry, so the integrity scrub stays clean in both directions.
6. *Print a sign-off report.* The report pins the first snapshot's contract count as
   the day-one plausibility anchor. The median-relative battery checks have no anchor
   until history accrues, so this count is the one early sanity number.

Deferred, not faked. Each is a later slice, and this command is structured so each
becomes an added step here without reshaping the flow.

- The corporate-actions history fetch is slice 3 (D16). Onboarding will later fetch and
  land splits and dividends for the ticker. It is skipped here.
- The full validation battery is slice 5 (D20). Onboarding will later run the battery
  and fold its verdict into the report. Here only the single real-time entitlement
  precondition is checked, which is the one gate the design names for slice 1.

Every dependency is injected: the clock, the vendor, and the FIGI resolver. So the
whole flow runs offline with no network, no real token, and no wall-clock read. The
thin ``onboard_from_config`` wires the real config, the Schwab-backed vendor, and the
OpenFIGI resolver around the same core, keeping every real construction lazy.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from lake import security_master
from lake.calendar import MARKET_TZ
from lake.clock import Clock, SystemClock
from lake.config import load_config
from lake.manifest import record_partition
from lake.security_master import ID_TYPE_TICKER, KIND_EQUITY, SecurityMaster
from lake.tickers import upsert_ticker
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


@runtime_checkable
class FigiResolver(Protocol):
    """Resolves a ticker to its FIGI. The real one queries OpenFIGI over HTTP."""

    def resolve(self, ticker: str) -> str | None:
        """The instrument's FIGI, or ``None`` when it cannot be resolved."""
        ...


class OpenFigiResolver:
    """The real FIGI resolver: a POST to the OpenFIGI mapping API.

    OpenFIGI maps a ticker to its FIGI for free. The HTTP request runs only in the
    by-hand live check, never in continuous integration. A test injects a fake. An
    optional API key raises the rate limit; it is a secret passed by the caller, never
    read from or written to the repo.
    """

    MAPPING_URL = "https://api.openfigi.com/v3/mapping"

    def __init__(self, api_key: str | None = None, timeout_seconds: float = 10.0) -> None:
        self._api_key = api_key
        self._timeout = timeout_seconds

    def resolve(self, ticker: str) -> str | None:
        import json
        import urllib.request  # lazy: only the live check makes a real request

        payload = json.dumps([{"idType": "TICKER", "idValue": ticker, "exchCode": "US"}]).encode()
        headers = {"Content-Type": "application/json"}
        if self._api_key is not None:
            headers["X-OPENFIGI-APIKEY"] = self._api_key
        request = urllib.request.Request(self.MAPPING_URL, data=payload, headers=headers)
        with urllib.request.urlopen(request, timeout=self._timeout) as response:
            body = json.loads(response.read())
        # The response is a list, one result per query. A match carries ``data`` with
        # one or more instruments, each with a ``figi``. No match carries a ``warning``.
        try:
            return body[0]["data"][0]["figi"]
        except (KeyError, IndexError, TypeError):
            return None


@dataclass(frozen=True)
class OnboardReport:
    """The sign-off report for one onboarded ticker.

    ``contract_count`` is the day-one plausibility anchor: the number of contracts in
    the first chain snapshot. It is ``None`` for an equity-only ticker, which takes no
    chain snapshot. ``already_registered`` is true when the ticker was already in the
    master, so a re-run reuses its id and capture_start rather than minting new ones.
    ``deferred`` names the later-slice steps this command does not yet do.
    """

    ticker: str
    instrument_id: int
    figi: str | None
    capture_start: datetime
    options: bool
    contract_count: int | None
    realtime_verified: bool
    tickers_path: Path
    master_path: Path
    already_registered: bool
    deferred: tuple[str, ...] = (
        "corporate-actions history fetch (slice 3, D16)",
        "full validation battery (slice 5, D20)",
    )

    def render(self) -> str:
        """A human-readable sign-off block."""
        lines = [
            f"Onboarded {self.ticker}",
            f"  instrument_id:   {self.instrument_id}"
            + (" (already registered)" if self.already_registered else ""),
            f"  figi:            {self.figi if self.figi is not None else 'unresolved'}",
            f"  capture_start:   {self.capture_start.isoformat()}",
            f"  options:         {self.options}",
            f"  realtime:        {'verified' if self.realtime_verified else 'not checked'}",
        ]
        if self.contract_count is not None:
            lines.append(f"  day-one anchor:  {self.contract_count} contracts in first snapshot")
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


def onboard(
    ticker: str,
    *,
    clock: Clock,
    vendor: Vendor,
    figi_resolver: FigiResolver,
    lake_root: Path | str,
    tickers_path: str | Path | None = None,
    tickers_env: Mapping[str, str] | None = None,
    options: bool = True,
    chain_cadence: str | None = DEFAULT_CHAIN_CADENCE,
    bars: Sequence[str] = DEFAULT_BARS,
) -> OnboardReport:
    """Onboard one ticker into the lake and return its sign-off report.

    Every dependency is injected, so this runs offline. The steps follow the module
    docstring. The security master is read from disk if it exists, updated, and written
    back under the lake-root lock with a fresh manifest entry. The roster entry and the
    master persist to disk; the first snapshot is fetched to verify entitlement and
    count contracts, and is not itself journaled, since the capture loop journals going
    forward.
    """
    lake_root = Path(lake_root)
    now = clock.now()
    valid_from = _market_date(now)

    master_path = security_master.master_path(lake_root)
    master = SecurityMaster.read(master_path) if master_path.exists() else SecurityMaster()

    # Idempotent-friendly: reuse the existing instrument if the ticker is already known.
    existing_id = master.resolve(ticker, valid_from, id_type=ID_TYPE_TICKER)
    already_registered = existing_id is not None

    # Resolve the FIGI up front so a new registration can carry it.
    figi = figi_resolver.resolve(ticker)

    if already_registered:
        instrument_id = existing_id
        capture_start = master.capture_start_of(instrument_id)
    else:
        instrument_id = master.register(
            kind=KIND_EQUITY,
            capture_start=now,
            valid_from=valid_from,
            ticker=ticker,
            figi=figi,
        )
        capture_start = master.capture_start_of(instrument_id)

    # The first snapshot proves the real-time entitlement before the ticker is trusted.
    contract_count: int | None = None
    if options:
        response = vendor.get_chain(ticker)
        if not _ok(response.status):
            raise OnboardError(f"first chain snapshot for {ticker} failed: HTTP {response.status}")
        _assert_chain_realtime(response.body)
        contract_count = _count_contracts(response.body)
    else:
        response = vendor.get_quotes([ticker])
        if not _ok(response.status):
            raise OnboardError(f"first quote for {ticker} failed: HTTP {response.status}")
        _assert_quote_realtime(ticker, response.body)

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
        master.write(master_path)
        record_partition(
            lake_root,
            MASTER_PARTITION,
            source=REFERENCE_SOURCE,
            rows=len(master),
            fetched_at=now.isoformat(),
        )

    return OnboardReport(
        ticker=ticker,
        instrument_id=instrument_id,
        figi=figi,
        capture_start=capture_start,
        options=options,
        contract_count=contract_count,
        realtime_verified=True,
        tickers_path=written_tickers_path,
        master_path=master_path,
        already_registered=already_registered,
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
    figi_resolver: FigiResolver | None = None,
) -> OnboardReport:
    """Onboard one ticker wired from the real config, vendor, and OpenFIGI resolver.

    This is the entry ``python -m lake.onboard`` calls. It loads the machine-local
    config, builds the Schwab-backed vendor from the token file, and defaults the FIGI
    resolver to the real OpenFIGI one. The Schwab client and the OpenFIGI request are
    built lazily, so importing this module and running the offline suite touch neither.
    A test drives ``onboard`` directly with fakes instead.
    """
    from lake.schwab import DEFAULT_TOKEN_PATH, SchwabVendor

    config = load_config(config_path)
    vendor = SchwabVendor.from_token(
        token_path if token_path is not None else DEFAULT_TOKEN_PATH,
        api_key=config.schwab_api_key.reveal(),
        app_secret=config.schwab_app_secret.reveal(),
    )
    return onboard(
        ticker,
        clock=clock if clock is not None else SystemClock(),
        vendor=vendor,
        figi_resolver=figi_resolver if figi_resolver is not None else OpenFigiResolver(),
        lake_root=config.lake_root,
        tickers_path=tickers_path,
        options=options,
        chain_cadence=chain_cadence,
        bars=bars,
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
        "--bars",
        nargs="*",
        default=list(DEFAULT_BARS),
        help="Bar frequencies to capture, like 1m 1d.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.onboard`` entry. Returns a process exit code."""
    args = _build_parser().parse_args(argv)
    report = onboard_from_config(
        args.ticker,
        config_path=args.config,
        tickers_path=args.tickers,
        token_path=args.token,
        options=args.options,
        chain_cadence=args.chain_cadence,
        bars=args.bars,
    )
    print(report.render())
    return 0


__all__ = [
    "DEFAULT_BARS",
    "DEFAULT_CHAIN_CADENCE",
    "MASTER_PARTITION",
    "EntitlementError",
    "FigiResolver",
    "OnboardError",
    "OnboardReport",
    "OpenFigiResolver",
    "main",
    "onboard",
    "onboard_from_config",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
