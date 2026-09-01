"""The chunked chain fetch, over the real filesystem.

A full SPY option chain exceeds Schwab's gateway body limit, so the capture cycle
fetches a chain in expiration chunks and reassembles it into one snapshot. These run one
whole cycle over real journal segments with a programmable fake vendor and a manual
clock. No network and no wall clock are crossed. So the tier is component: one subsystem,
capture, over real files, with the vendor and clock still fake.

They pin the chunker's contract:

1. A discovery response plus per-chunk responses reassemble to the full contract set in
   one chains segment, with the chain-level header taken from discovery.
2. A chunk that returns a 502 (or flags ``isChainTruncated``) is split in half and
   refetched until it succeeds.
3. A chunk that fails past the split bound becomes absent-expiration gap-marker rows
   inside a tagged partial snapshot, while the rest of the chain journals normally.
4. A failed discovery, and a chain where every chunk fails, are whole-chain gaps.
5. The split recursion honours the depth bound.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from lake import capture, journal
from lake.config import GuardConstants
from lake.manifest import latest_entries
from lake.tickers import Roster
from lake.vendor import VendorResponse
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)

# Four synthetic expirations, a week apart. ISO date strings sort chronologically.
E1, E2, E3, E4 = "2026-09-04", "2026-09-11", "2026-09-18", "2026-09-25"

# A 502 the way Schwab's gateway rejects an over-large chain body.
_TOO_BIG = VendorResponse(status=502, body={"errorcode": "protocol.http.TooBigBody"})

# A minimal batched-quote response so the cycle's quote leg lands cleanly beside the chain.
_QUOTES = VendorResponse(
    status=200,
    body={
        "SPY": {
            "assetMainType": "EQUITY",
            "realtime": True,
            "quote": {
                "bidPrice": 649.98,
                "askPrice": 650.02,
                "lastPrice": 650.0,
                "quoteTime": 1787000100000,
            },
        }
    },
)


def _roster() -> Roster:
    return Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}})


def _contract(exp_iso: str, put_call: str, *, bid: float, oi: int) -> dict:
    """One synthetic contract, enough fields for the calibrated row builder to read."""
    letter = "C" if put_call == "CALL" else "P"
    return {
        "symbol": f"SPY   {exp_iso.replace('-', '')}{letter}00650000",
        "putCall": put_call,
        "strikePrice": 650.0,
        "expirationDate": f"{exp_iso}T20:00:00.000+00:00",
        "quoteTimeInLong": 1787000099000,
        "bid": bid,
        "openInterest": oi,
    }


def _chain_body(
    expirations: list[str],
    *,
    truncated: bool = False,
    underlying_price: float = 650.0,
) -> dict:
    """A chain body carrying one call and one put per named expiration."""
    call_map: dict[str, dict[str, list]] = {}
    put_map: dict[str, dict[str, list]] = {}
    count = 0
    for exp_iso in expirations:
        key = f"{exp_iso}:7"
        call_map[key] = {"650.0": [_contract(exp_iso, "CALL", bid=1.0, oi=100)]}
        put_map[key] = {"650.0": [_contract(exp_iso, "PUT", bid=0.9, oi=90)]}
        count += 2
    return {
        "status": "SUCCESS",
        "underlying": None,
        "underlyingPrice": underlying_price,
        "interestRate": 4.25,
        "dividendYield": 1.28,
        "isDelayed": False,
        "isChainTruncated": truncated,
        "numberOfContracts": count,
        "callExpDateMap": call_map,
        "putExpDateMap": put_map,
    }


def _chain_response(expirations: list[str], **kwargs) -> VendorResponse:
    return VendorResponse(status=200, body=_chain_body(expirations, **kwargs))


class _ChunkVendor:
    """A programmable ``Vendor`` for the chunker.

    ``discovery`` answers the ``strike_count`` probe. ``chunks`` maps an
    ``(from_iso, to_iso)`` expiration range to its response, so the split tree is driven
    exactly. A requested range with no mapping raises, so a test never silently reaches
    past its setup. Every chain call is recorded as ``(symbol, from_iso, to_iso,
    strike_count)``.
    """

    def __init__(
        self,
        *,
        discovery: VendorResponse,
        chunks: dict[tuple[str, str], VendorResponse] | None = None,
    ) -> None:
        self._discovery = discovery
        self._chunks = chunks or {}
        self.chain_calls: list[tuple[str, str | None, str | None, int | None]] = []

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        f = from_date.isoformat() if from_date is not None else None
        t = to_date.isoformat() if to_date is not None else None
        self.chain_calls.append((symbol, f, t, strike_count))
        if strike_count is not None:
            return self._discovery
        key = (f, t)
        if key not in self._chunks:
            raise AssertionError(f"no canned chunk for range {key}")
        return self._chunks[key]

    def get_quotes(self, symbols):
        return _QUOTES

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _run(vendor: _ChunkVendor, lake_root: Path, *, guards: GuardConstants | None = None):
    return capture.run_cycle(
        ManualClock(start=_CLOCK_START), vendor, _roster(), lake_root, pid=4242, guards=guards
    )


def _chain_rows(result, ticker: str = "SPY") -> list[dict]:
    return journal.read_segment(result.segment(CHAINS, ticker).path).to_pylist()


# -- 1. discovery + per-chunk responses reassemble to one segment ------------------------


def test_chunks_reassemble_to_the_full_contract_set_in_one_segment(lake_root):
    # Three expirations, grouped two per chunk, so the cycle fetches two chunks and
    # reassembles both into one snapshot. The header comes from discovery, not the chunks,
    # so discovery's underlying price wins over the chunks' deliberately different one.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1, E2, E3], underlying_price=650.0),
        chunks={
            (E1, E2): _chain_response([E1, E2], underlying_price=999.0),
            (E3, E3): _chain_response([E3], underlying_price=999.0),
        },
    )
    result = _run(vendor, lake_root, guards=GuardConstants(chain_chunk_expirations=2))

    # Exactly one chains segment for SPY, and it is a data segment.
    chain_segs = [s for s in result.segments if s.surface == CHAINS]
    assert len(chain_segs) == 1
    assert chain_segs[0].row_kind == journal.ROW_KIND_DATA
    assert chain_segs[0].error_class is None

    rows = _chain_rows(result)
    # All six contracts across the three expirations survived, none duplicated.
    assert len(rows) == 6
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    assert {r["occ_symbol"] for r in rows} == {
        _contract(exp, side, bid=0, oi=0)["symbol"]
        for exp in (E1, E2, E3)
        for side in ("CALL", "PUT")
    }
    assert sorted(r["expiration_date"] for r in rows) == sorted(
        f"{exp}T20:00:00.000+00:00" for exp in (E1, E2, E3) for _ in ("call", "put")
    )
    # The header is discovery's, repeated on every row.
    assert {r["underlying_price"] for r in rows} == {650.0}
    assert {r["interest_rate"] for r in rows} == {4.25}
    # The count is the six reassembled contracts, not any single chunk's, and nothing was
    # given up, so the chain reads untruncated.
    assert {r["number_of_contracts"] for r in rows} == {6}
    assert {r["is_chain_truncated"] for r in rows} == {False}

    # The request order proves discovery-then-group: one probe, then one fetch per group.
    assert vendor.chain_calls == [
        ("SPY", None, None, 1),
        ("SPY", E1, E2, None),
        ("SPY", E3, E3, None),
    ]


# -- 2. a 502 chunk is split in half and refetched --------------------------------------


def test_a_too_big_chunk_splits_and_then_succeeds(lake_root):
    # One group of four expirations. The whole-range fetch 502s, so it is halved and each
    # half succeeds. All eight contracts land, with no gaps.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1, E2, E3, E4]),
        chunks={
            (E1, E4): _TOO_BIG,
            (E1, E2): _chain_response([E1, E2]),
            (E3, E4): _chain_response([E3, E4]),
        },
    )
    result = _run(vendor, lake_root)

    rows = _chain_rows(result)
    assert len(rows) == 8
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)
    assert result.segment(CHAINS, "SPY").error_class is None
    # The split is visible in the request trace: the full range, then its two halves.
    assert vendor.chain_calls == [
        ("SPY", None, None, 1),
        ("SPY", E1, E4, None),
        ("SPY", E1, E2, None),
        ("SPY", E3, E4, None),
    ]


def test_an_is_chain_truncated_200_also_splits(lake_root):
    # A 200 flagged isChainTruncated is just as "too big" as a 502, so it splits too.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1, E2]),
        chunks={
            (E1, E2): _chain_response([E1, E2], truncated=True),
            (E1, E1): _chain_response([E1]),
            (E2, E2): _chain_response([E2]),
        },
    )
    result = _run(vendor, lake_root)

    rows = _chain_rows(result)
    assert len(rows) == 4
    assert all(r["row_kind"] == journal.ROW_KIND_DATA for r in rows)


# -- 3. a permanently failing chunk becomes a tagged partial snapshot --------------------


def test_a_permanently_failing_chunk_yields_a_tagged_partial(lake_root):
    # The E1-E2 range 502s and splits. E1 alone succeeds, E2 alone keeps failing down to a
    # single expiration, so E2 is given up on. The snapshot journals E1's contracts and an
    # absent-marker for E2. It stays a data segment, tagged partial, not a whole gap.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1, E2]),
        chunks={
            (E1, E2): _TOO_BIG,
            (E1, E1): _chain_response([E1]),
            (E2, E2): _TOO_BIG,
        },
    )
    result = _run(vendor, lake_root)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_DATA
    assert outcome.error_class == capture.CHAIN_CHUNK_FAILED

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]

    # E1's call and put journaled normally.
    assert len(data_rows) == 2
    assert {r["expiration_date"] for r in data_rows} == {f"{E1}T20:00:00.000+00:00"}
    assert all(r["error_class"] is None for r in data_rows)
    assert all(r["bid"] is not None for r in data_rows)

    # The chain-level fields are recomputed from the reassembly, not the discovery probe:
    # two captured contracts, not discovery's four, and truncation true because E2 was
    # given up.
    assert all(r["number_of_contracts"] == 2 for r in data_rows)
    assert all(r["is_chain_truncated"] is True for r in data_rows)

    # E2 is one absent-marker, tagged, naming the missing expiration, holding no data.
    assert len(gap_rows) == 1
    gap = gap_rows[0]
    assert gap["error_class"] == capture.CHAIN_CHUNK_FAILED
    assert gap["expiration_date"] == E2
    assert gap["bid"] is None and gap["open_interest"] is None

    # A partial snapshot is still a durable, manifested segment.
    assert outcome.partition in latest_entries(lake_root)


def test_a_bounded_split_depth_gives_up_on_the_deeper_ranges(lake_root):
    # With a split-depth bound of 1, the full range 502s and splits once. Each half is now
    # at the bound, so a half that is still too big is given up on wholesale rather than
    # split to singles. Here the first half fails and the second succeeds.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1, E2, E3, E4]),
        chunks={
            (E1, E4): _TOO_BIG,
            (E1, E2): _TOO_BIG,
            (E3, E4): _chain_response([E3, E4]),
        },
    )
    result = _run(
        vendor,
        lake_root,
        guards=GuardConstants(chain_chunk_expirations=8, chain_chunk_max_split_depth=1),
    )

    rows = _chain_rows(result)
    data_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_DATA]
    gap_rows = [r for r in rows if r["row_kind"] == journal.ROW_KIND_GAP]
    # E3 and E4 survived as four data rows; E1 and E2 are absent-markers.
    assert {r["expiration_date"] for r in data_rows} == {
        f"{E3}T20:00:00.000+00:00",
        f"{E4}T20:00:00.000+00:00",
    }
    assert {r["expiration_date"] for r in gap_rows} == {E1, E2}
    assert all(r["error_class"] == capture.CHAIN_CHUNK_FAILED for r in gap_rows)


# -- 4. whole-chain gaps: discovery failure, and every chunk failing ---------------------


def test_discovery_failure_yields_a_whole_chain_gap(lake_root):
    vendor = _ChunkVendor(discovery=VendorResponse(status=500, body={"error": "boom"}))
    result = _run(vendor, lake_root)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == "http_500"
    assert outcome.rows == 1
    gap = _chain_rows(result)[0]
    assert gap["row_kind"] == journal.ROW_KIND_GAP
    assert gap["bid"] is None
    # A failed discovery never fetches a chunk.
    assert vendor.chain_calls == [("SPY", None, None, 1)]


def test_every_chunk_failing_yields_a_whole_chain_gap(lake_root):
    # One expiration whose only chunk always 502s. Nothing survives, so the whole chain is
    # a gap, tagged with the chunk-failure class rather than becoming an empty snapshot.
    vendor = _ChunkVendor(
        discovery=_chain_response([E1]),
        chunks={(E1, E1): _TOO_BIG},
    )
    result = _run(vendor, lake_root)

    outcome = result.segment(CHAINS, "SPY")
    assert outcome.row_kind == journal.ROW_KIND_GAP
    assert outcome.error_class == capture.CHAIN_CHUNK_FAILED
    assert outcome.rows == 1
