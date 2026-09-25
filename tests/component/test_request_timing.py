"""The request timing file, written by a real capture cycle over the real filesystem.

A capture cycle appends one line per vendor request to ``journal/timing/date=D.jsonl``, so
a slow minute can be split into its requests and each request into Schwab's time and the
network's (marketlake #531). These run whole cycles with a programmable vendor and a manual
clock. The vendor advances the clock by a different amount inside each call, so every
stamp a line carries is an exact instant a test can name, and no real time is read.

Advancing the clock from inside a vendor call is only allowed on the one-at-a-time path.
Marketlake #532 fires a cycle's requests through a pool whose threads must never advance
the clock, so these cycles run at a concurrency cap of 1, the path that restores fetching
one request at a time. One test at the end runs the pool itself, with a vendor that leaves
the clock alone, and checks what the lines must say whatever order the responses land in.

What they cover:

1. One line per request, carrying the caller's own start and end stamps, the transport's
   stamps as the vendor returned them, and the keys that join it to its rows.
2. A request that raised still has a start and an end, which is the shape a timeout takes.
3. A 429's sub-code is found wherever it sits, never changes the error class the watchdog
   pages on, and a failed reply keeps a bounded copy without its cookie.
4. A chain rejected on every window keeps each window's sub-code, although its rows are
   one whole-chain gap.
5. A too-big window that is split records the split request and both halves.
6. The file never costs a minute: a write that fails still leaves every segment and
   manifest entry, and says so on stderr, and so does a record that came out incomplete.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from lake import capture, journal
from lake.chain_plan import ChainPlan
from lake.config import GuardConstants
from lake.manifest import latest_entries
from lake.paths import LakePaths
from lake.tickers import Roster
from lake.vendor import RequestTiming, VendorResponse
from tests.support.clock import ManualClock

CHAINS = journal.CHAINS_SURFACE
QUOTES = journal.QUOTES_SURFACE

# A clock with non-zero seconds, so the floor to the minute is observable. The session date
# is this instant's date, and the plan's day offsets add to it.
_CLOCK_START = datetime(2026, 8, 24, 13, 30, 45, tzinfo=UTC)
_SNAP = datetime(2026, 8, 24, 13, 30, tzinfo=UTC)
SESSION = date(2026, 8, 24)
TWO_WINDOWS = ChainPlan(((0, 9), (10, None)))


def _d(offset: int) -> str:
    return (SESSION + timedelta(days=offset)).isoformat()


NEAR = (_d(0), _d(9))
TAIL = (_d(10), None)


def _at(seconds: float) -> datetime:
    """The instant ``seconds`` after the clock's start, which is how every stamp is named."""
    return _CLOCK_START + timedelta(seconds=seconds)


def _contract(exp_iso: str, put_call: str) -> dict:
    letter = "C" if put_call == "CALL" else "P"
    return {
        "symbol": f"SPY   {exp_iso.replace('-', '')}{letter}00650000",
        "putCall": put_call,
        "strikePrice": 650.0,
        "expirationDate": f"{exp_iso}T20:00:00.000+00:00",
        "quoteTimeInLong": 1787000099000,
        "bid": 1.0,
        "openInterest": 100,
    }


def _chain_body(expirations: list[str]) -> dict:
    call_map = {f"{e}:7": {"650.0": [_contract(e, "CALL")]} for e in expirations}
    put_map = {f"{e}:7": {"650.0": [_contract(e, "PUT")]} for e in expirations}
    return {
        "status": "SUCCESS",
        "underlying": None,
        "underlyingPrice": 650.0,
        "interestRate": 4.25,
        "dividendYield": 1.28,
        "isDelayed": False,
        "isChainTruncated": False,
        "numberOfContracts": 2 * len(expirations),
        "callExpDateMap": call_map,
        "putExpDateMap": put_map,
    }


_QUOTE_BODY = {
    "SPY": {
        "assetMainType": "EQUITY",
        "realtime": True,
        "quote": {"bidPrice": 649.98, "askPrice": 650.02, "quoteTime": 1787000100000},
    }
}


class _TimedVendor:
    """A vendor that takes a set time for each call, by advancing the manual clock.

    ``windows`` maps a ``(from_iso, to_iso)`` range to ``(seconds, answer)``. The call
    advances the clock by ``seconds`` and then returns ``answer``, or raises it when it is
    an exception. ``quotes`` is the same pair for the batched quote request.
    """

    def __init__(self, clock: ManualClock, windows: dict, quotes: tuple) -> None:
        self._clock = clock
        self._windows = windows
        self._quotes = quotes
        self.calls: list[tuple[str | None, str | None]] = []

    def _answer(self, pair):
        seconds, answer = pair
        self._clock.advance(seconds)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        key = (from_date.isoformat(), to_date.isoformat() if to_date is not None else None)
        self.calls.append(key)
        return self._answer(self._windows[key])

    def get_quotes(self, symbols):
        return self._answer(self._quotes)

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def _run(vendor: _TimedVendor, clock: ManualClock, lake_root: Path, plan=TWO_WINDOWS, guards=None):
    return capture.run_cycle(
        clock,
        vendor,
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=4242,
        plan=plan,
        guards=guards if guards is not None else GuardConstants(capture_max_concurrency=1),
    )


def _lines(lake_root: Path, day: date = SESSION) -> list[dict]:
    path = LakePaths(lake_root).timing_path(day)
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _chain_lines(lake_root: Path) -> list[dict]:
    return [line for line in _lines(lake_root) if line["surface"] == CHAINS]


def _timing(
    sent: float, headers: float, body: float, *, size: int, connected: float | None = None
) -> RequestTiming:
    """Transport stamps as literals, never derived from anything the code computes."""
    return RequestTiming(
        sent=_at(sent),
        connected=None if connected is None else _at(connected),
        headers=_at(headers),
        body=_at(body),
        bytes=size,
    )


def _iso(seconds: float) -> str:
    return _at(seconds).isoformat()


# -- 1. one line per request ---------------------------------------------------------------


def test_every_request_writes_one_line_that_joins_to_its_rows(lake_root):
    clock = ManualClock(start=_CLOCK_START)
    near = VendorResponse(
        200,
        _chain_body(["2026-08-28"]),
        timing=_timing(0.25, 2.5, 2.75, size=81_000, connected=0.5),
    )
    tail = VendorResponse(
        200,
        _chain_body(["2026-09-18"]),
        timing=_timing(3.5, 7.0, 8.5, size=52_000),
    )
    quotes = VendorResponse(200, _QUOTE_BODY, timing=_timing(9.25, 9.5, 9.75, size=900))
    vendor = _TimedVendor(
        clock,
        windows={NEAR: (3.0, near), TAIL: (6.0, tail)},
        quotes=(1.0, quotes),
    )

    result = _run(vendor, clock, lake_root)

    # The file sits where runway, compaction and the scrub were each checked to expect it.
    assert LakePaths(lake_root).timing_path(SESSION) == (
        lake_root / "journal" / "timing" / "date=2026-08-24.jsonl"
    )
    lines = _lines(lake_root)
    assert [(line["surface"], line["window_start"], line["window_end"]) for line in lines] == [
        (CHAINS, NEAR[0], NEAR[1]),
        (CHAINS, TAIL[0], TAIL[1]),
        (QUOTES, None, None),
    ]
    first, second, batch = lines
    # The caller's stamps bracket each call exactly: each window's end is its start plus
    # the time the vendor took, and the next request starts where the last one ended.
    assert (first["request_start_ts"], first["request_end_ts"]) == (_iso(0), _iso(3))
    assert (second["request_start_ts"], second["request_end_ts"]) == (_iso(3), _iso(9))
    assert (batch["request_start_ts"], batch["request_end_ts"]) == (_iso(9), _iso(10))
    # The transport's stamps are the ones the vendor returned, untouched.
    assert first["request_sent_ts"] == _iso(0.25)
    assert first["request_headers_ts"] == _iso(2.5)
    assert first["request_body_ts"] == _iso(2.75)
    assert first["request_connected_ts"] == _iso(0.5)
    assert second["request_connected_ts"] is None
    assert (first["request_bytes"], second["request_bytes"], batch["request_bytes"]) == (
        81_000,
        52_000,
        900,
    )
    assert second["request_headers_ts"] == _iso(7.0)
    # A chain request names its ticker, the quote batch the symbols it served.
    assert (first["ticker"], first["symbols"]) == ("SPY", [])
    assert (batch["ticker"], batch["symbols"]) == (None, ["SPY"])
    for line in lines:
        assert line["v"] == 1
        assert line["kind"] == "request"
        assert line["snap_ts"] == _SNAP.isoformat()
        assert (line["status"], line["error_class"]) == (200, None)
        assert (line["request_subcode"], line["request_error_detail"]) == (None, None)
        assert line["request_failure"] is None

    # The keys join every chain row to its request's line.
    rows = journal.read_segment(result.segment(CHAINS, "SPY").path).to_pylist()
    windows = {(line["window_start"], line["window_end"]) for line in _chain_lines(lake_root)}
    assert {(row["window_start"], row["window_end"]) for row in rows} == windows
    assert {row["snap_ts"] for row in rows} == {_SNAP.isoformat()}


def test_a_request_that_raised_keeps_its_start_and_end(lake_root):
    # A timeout is the shape #534 needs timed: no response came back, so the transport
    # stamps are null, and the caller's two stamps say how long the wait was.
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (2.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (30.0, TimeoutError("read timed out")),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    tail = _chain_lines(lake_root)[1]
    assert (tail["request_start_ts"], tail["request_end_ts"]) == (_iso(2), _iso(32))
    assert (tail["status"], tail["error_class"]) == (None, "timeout_error")
    for field in (
        "request_sent_ts",
        "request_connected_ts",
        "request_headers_ts",
        "request_body_ts",
        "request_bytes",
    ):
        assert tail[field] is None


# -- 2. rejections ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "headers", "expected"),
    [
        (
            {"errors": [{"status": "429", "detail": "Too many requests: 429-005 burst"}]},
            {"content-type": "application/json"},
            "429-005",
        ),
        ({"message": "slow down"}, {"x-schwab-error": "429-001"}, "429-001"),
        ({"message": "slow down"}, {"retry-after": "1"}, None),
    ],
    ids=["in-the-body", "in-a-header", "absent"],
)
def test_a_429_sub_code_is_found_where_it_sits(lake_root, body, headers, expected):
    clock = ManualClock(start=_CLOCK_START)
    rejected = VendorResponse(429, body, headers={**headers, "Set-Cookie": "session=abc"})
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, rejected),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    tail = _chain_lines(lake_root)[1]
    # The class the watchdog pages on is untouched by whatever the sub-code search found.
    assert (tail["status"], tail["error_class"]) == (429, "http_429")
    assert tail["request_subcode"] == expected
    detail = json.loads(tail["request_error_detail"])
    assert json.loads(detail["body"]) == body
    assert {name.lower() for name in detail["headers"]} == set(headers)


def test_a_sub_code_is_read_only_off_a_429(lake_root):
    # A 400 that echoes the pattern back is not a rate limit, and a 200 has nothing to say.
    clock = ManualClock(start=_CLOCK_START)
    echoed = {"error": "bad parameter 429-005", **_chain_body(["2026-08-28"])}
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, echoed)),
            TAIL: (1.0, VendorResponse(400, {"error": "bad parameter 429-005"})),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    near, tail = _chain_lines(lake_root)
    assert (near["request_subcode"], near["request_error_detail"]) == (None, None)
    assert tail["request_subcode"] is None
    assert tail["error_class"] == "http_400"
    assert tail["request_error_detail"] is not None


def test_the_error_copy_keeps_only_the_first_4096_bytes_of_the_body(lake_root):
    clock = ManualClock(start=_CLOCK_START)
    long_body = {"detail": "x" * 10_000}
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(503, long_body)),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    kept = json.loads(_chain_lines(lake_root)[1]["request_error_detail"])["body"]
    assert len(kept.encode("utf-8")) == 4096
    assert json.dumps(long_body, sort_keys=True).startswith(kept)


def test_a_chain_rejected_on_every_window_keeps_each_windows_sub_code(lake_root):
    # The likeliest shape of a burst rejection. The rows collapse to one whole-chain gap,
    # and the lines keep what that one row cannot: every window's own sub-code.
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(429, {"detail": "429-005"})),
            TAIL: (1.0, VendorResponse(429, {"detail": "429-001"})),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    result = _run(vendor, clock, lake_root)

    rows = journal.read_segment(result.segment(CHAINS, "SPY").path).to_pylist()
    assert [(row["row_kind"], row["error_class"]) for row in rows] == [
        (journal.ROW_KIND_GAP, "http_429")
    ]
    assert [line["request_subcode"] for line in _chain_lines(lake_root)] == [
        "429-005",
        "429-001",
    ]


# -- 3. a split -----------------------------------------------------------------------------


def test_a_split_window_records_the_split_request_and_both_halves(lake_root):
    clock = ManualClock(start=_CLOCK_START)
    plan = ChainPlan(((0, 9), (10, None)))
    first_half = (_d(0), _d(4))
    second_half = (_d(5), _d(9))
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.5, VendorResponse(502, {"errorcode": "protocol.http.TooBigBody"})),
            first_half: (1.0, VendorResponse(200, _chain_body(["2026-08-26"]))),
            second_half: (1.0, VendorResponse(200, _chain_body(["2026-08-31"]))),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root, plan=plan)

    lines = _chain_lines(lake_root)
    assert [(line["window_start"], line["window_end"], line["status"]) for line in lines] == [
        (NEAR[0], NEAR[1], 502),
        (first_half[0], first_half[1], 200),
        (second_half[0], second_half[1], 200),
        (TAIL[0], TAIL[1], 200),
    ]
    # The split request failed nothing yet, so it carries no class of its own.
    assert [line["error_class"] for line in lines] == [None, None, None, None]


# -- 4. the file never costs a minute --------------------------------------------------------


def test_a_timing_file_that_cannot_be_written_costs_only_its_lines(lake_root, capsys):
    # A file where the timing directory belongs makes every append fail.
    (lake_root / "journal").mkdir()
    (lake_root / "journal" / "timing").write_text("in the way", encoding="utf-8")
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    result = _run(vendor, clock, lake_root)

    assert {(s.surface, s.ticker) for s in result.segments} == {(CHAINS, "SPY"), (QUOTES, "SPY")}
    assert not result.errors
    manifested = latest_entries(lake_root)
    for outcome in result.segments:
        assert outcome.partition in manifested
    err = capsys.readouterr().err
    assert err.count("capture: request timing not written for") == 1


def test_an_incomplete_record_says_so_once_per_cycle(lake_root, capsys):
    clock = ManualClock(start=_CLOCK_START)
    broken = RequestTiming(sent=_at(0.1), failure="RuntimeError: hook broke")
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]), timing=broken)),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]), timing=broken)),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    err = capsys.readouterr().err
    # One line for the whole cycle, naming the reason once however many records share it.
    assert err.splitlines() == [
        f"capture: request timing incomplete for the {_SNAP.isoformat()} cycle: "
        "RuntimeError: hook broke"
    ]
    # The lines still land, carrying what was recorded and naming what was not, so a null
    # stamp that failed reads differently from one nobody observed.
    lines = _chain_lines(lake_root)
    assert [line["request_sent_ts"] for line in lines] == [_iso(0.1)] * 2
    assert [line["request_failure"] for line in lines] == ["RuntimeError: hook broke"] * 2
    quotes = [line for line in _lines(lake_root) if line["surface"] == QUOTES]
    assert [line["request_failure"] for line in quotes] == [None]


def test_a_record_that_cannot_be_written_costs_only_its_own_line(lake_root, capsys):
    # A vendor fake whose timing is not a ``RequestTiming`` makes that one line fail to
    # build. The lines before and after it still land.
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]), timing=object())),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    assert [(line["surface"], line["window_start"]) for line in _lines(lake_root)] == [
        (CHAINS, TAIL[0]),
        (QUOTES, None),
    ]
    assert capsys.readouterr().err.count("capture: request timing not written for") == 1


@pytest.mark.parametrize(
    ("answer", "status", "error_class", "subcode"),
    [
        (ConnectionError("reset"), None, "connection_error", None),
        (VendorResponse(429, {"detail": "429-001 sustained"}), 429, "http_429", "429-001"),
    ],
    ids=["raised", "rejected"],
)
def test_a_failed_quote_batch_still_writes_its_line(
    lake_root, answer, status, error_class, subcode
):
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(4.0, answer),
    )

    _run(vendor, clock, lake_root)

    (batch,) = [line for line in _lines(lake_root) if line["surface"] == QUOTES]
    assert (batch["status"], batch["error_class"], batch["request_subcode"]) == (
        status,
        error_class,
        subcode,
    )
    assert (batch["request_start_ts"], batch["request_end_ts"]) == (_iso(2), _iso(6))


def test_each_failed_window_records_the_class_capture_gave_it(lake_root):
    # A body the merge cannot read is given up as drift, and a too-big window past the
    # depth bound as the size class. Each line carries the class its own branch recorded.
    clock = ManualClock(start=_CLOCK_START)
    drifted = _chain_body(["2026-08-28"])
    drifted["callExpDateMap"]["2026-08-28:7"]["655.0"] = "XY"
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, drifted)),
            TAIL: (1.0, VendorResponse(502, {"errorcode": "protocol.http.TooBigBody"})),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(
        vendor,
        clock,
        lake_root,
        guards=GuardConstants(chain_chunk_max_split_depth=0, capture_max_concurrency=1),
    )

    assert [(line["status"], line["error_class"]) for line in _chain_lines(lake_root)] == [
        (200, capture.CHAIN_SCHEMA_DRIFT),
        (502, capture.CHAIN_CHUNK_FAILED),
    ]


def test_the_lines_are_written_only_after_the_minute_is_durable(lake_root, monkeypatch):
    # At the moment the append runs, every segment is manifested and the cycle's metadata
    # stamp is on disk, so a slow or failed append can never sit in front of either.
    seen: list[tuple[bool, bool]] = []
    real = capture.append_requests

    def spy(root, **kwargs):
        manifested = latest_entries(root)
        segments = [p.relative_to(root).as_posix() for p in (root / "journal").rglob("*.arrows")]
        seen.append(
            (
                bool(segments) and all(seg in manifested for seg in segments),
                (root / "journal" / "metadata.json").exists(),
            )
        )
        return real(root, **kwargs)

    monkeypatch.setattr(capture, "append_requests", spy)
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    assert seen and all(entry == (True, True) for entry in seen)


def test_a_reply_the_rejection_parser_cannot_read_costs_only_its_copy(lake_root, capsys):
    # A fake reply with no headers mapping makes the parser fail. The cycle lands, the line
    # names the failure, and stderr says so once.
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(503, {}, headers=None)),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    result = _run(vendor, clock, lake_root)

    assert not result.errors
    tail = _chain_lines(lake_root)[1]
    assert (tail["error_class"], tail["request_error_detail"]) == ("http_503", None)
    assert tail["request_failure"].startswith("AttributeError")
    assert capsys.readouterr().err.count("capture: request timing incomplete for") == 1


class _Refusing:
    """A stderr that refuses every write, the way a full disk refuses a launchd log."""

    def write(self, text: str) -> int:
        raise OSError(28, "No space left on device")

    def flush(self) -> None:
        raise OSError(28, "No space left on device")


def test_nothing_in_the_timing_path_can_raise_out_of_a_cycle(lake_root, monkeypatch):
    # The append raises something that is not an ``OSError``, the failure count raises,
    # and stderr refuses the report. The cycle still returns with every segment landed.
    import sys

    def broken_append(*args, **kwargs):
        raise RuntimeError("append broke")

    def broken_failures(records):
        raise RuntimeError("count broke")

    monkeypatch.setattr(capture, "append_requests", broken_append)
    monkeypatch.setattr(capture, "failures", broken_failures)
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (1.0, VendorResponse(200, _chain_body(["2026-09-18"]))),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )
    monkeypatch.setattr(sys, "stderr", _Refusing())

    result = _run(vendor, clock, lake_root)

    assert {(s.surface, s.ticker) for s in result.segments} == {(CHAINS, "SPY"), (QUOTES, "SPY")}
    assert not result.errors


def test_the_body_is_searched_before_the_headers(lake_root):
    clock = ManualClock(start=_CLOCK_START)
    vendor = _TimedVendor(
        clock,
        windows={
            NEAR: (1.0, VendorResponse(200, _chain_body(["2026-08-28"]))),
            TAIL: (
                1.0,
                VendorResponse(429, {"detail": "429-005"}, headers={"x-code": "429-001"}),
            ),
        },
        quotes=(0.5, VendorResponse(200, _QUOTE_BODY)),
    )

    _run(vendor, clock, lake_root)

    assert _chain_lines(lake_root)[1]["request_subcode"] == "429-005"


class _StillVendor:
    """A vendor that answers from a map and never touches the clock, safe for pool threads."""

    def __init__(self, windows: dict, quotes: VendorResponse) -> None:
        self._windows = windows
        self._quotes = quotes

    def get_chain(self, symbol, *, from_date=None, to_date=None, strike_count=None):
        key = (from_date.isoformat(), to_date.isoformat() if to_date is not None else None)
        answer = self._windows[key]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get_quotes(self, symbols):
        return self._quotes

    def token_mint_time(self):
        return datetime(2026, 8, 23, tzinfo=UTC)


def test_the_pool_writes_one_line_per_request_in_plan_order(lake_root):
    # Through #532's pool the requests run concurrently and land in any order. The chain's
    # lines still follow the plan, each line keeps its own window's outcome, and the quote
    # batch gets its line, all written once by the cycle after the minute is durable.
    clock = ManualClock(start=_CLOCK_START)
    vendor = _StillVendor(
        windows={
            NEAR: VendorResponse(200, _chain_body(["2026-08-28"]), timing=_timing(1, 2, 3, size=7)),
            TAIL: VendorResponse(429, {"detail": "429-005"}),
        },
        quotes=VendorResponse(200, _QUOTE_BODY),
    )

    capture.run_cycle(
        clock,
        vendor,
        Roster.from_mapping({"SPY": {"options": True, "chain_cadence": "1m"}}),
        lake_root,
        pid=4242,
        plan=TWO_WINDOWS,
        guards=GuardConstants(capture_max_concurrency=4, capture_stagger_ms=0),
    )

    lines = _lines(lake_root)
    chains = [line for line in lines if line["surface"] == CHAINS]
    assert [(line["window_start"], line["status"], line["request_subcode"]) for line in chains] == [
        (NEAR[0], 200, None),
        (TAIL[0], 429, "429-005"),
    ]
    assert chains[0]["request_headers_ts"] == _iso(2)
    assert chains[0]["request_bytes"] == 7
    (batch,) = [line for line in lines if line["surface"] == QUOTES]
    assert (batch["status"], batch["symbols"]) == (200, ["SPY"])
    assert {line["snap_ts"] for line in lines} == {_SNAP.isoformat()}
