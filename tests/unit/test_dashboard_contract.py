"""The dashboard's fixed-query contract, decided from values alone.

These pin the boundary rules without a connection or a socket in the path, so the tier is
unit. The parameter validators, the Host check, the route and registry shape, the status
vocabulary, the slot denominator, the tab icon, and the command-line contract are each a
pure function, a table, or bytes shipped inside the package.

Two cases touch the filesystem, and neither leaves the package.

1. The page and icon cases read bytes shipped inside it.
2. The icon renderer's command-line case writes one file to a temporary directory.

Neither crosses a subsystem boundary, so the tier holds.

The command-line cases reach ``main`` with every seam it wires replaced: the clock, the
calendar, the service, the config reader, and the server factory. The factory raises the
port-in-use error, so ``main`` records its choices and returns before anything binds or
is read. So the tier holds even though the function under test is the process entry.
"""

from __future__ import annotations

import errno
import re
import struct
import zlib
from datetime import date, datetime
from importlib import resources
from pathlib import Path
from types import SimpleNamespace

import pytest

from lake import dashboard, favicon
from lake.calendar import MARKET_TZ, OPTION_CLOSE_OFFSET
from lake.config import GuardConstants
from lake.dashboard import (
    NAMED_QUERIES,
    ROUTES,
    STATUSES,
    QueryParameterError,
    host_allowed,
    parse_date,
    session_slots,
    validate_parameters,
    validate_ticker,
)
from lake.session import COMPACTION_DELAY, OPTION_CLOSE_GUARD, SessionBounds

ROSTER = {"QQQ": ("chains", "quotes"), "SPY": ("chains", "quotes")}


# -- the validators ----------------------------------------------------------


def test_parse_date_accepts_strict_iso():
    assert parse_date("2026-08-24") == date(2026, 8, 24)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "20260824",
        "2026-8-24",
        "2026-08-24T00:00",
        "2026-13-01",
        "2026-02-30",
        "x",
        "2026-08-24 ",
    ],
)
def test_parse_date_rejects_everything_else(text: str):
    with pytest.raises(QueryParameterError):
        parse_date(text)


def test_validate_ticker_accepts_only_roster_members():
    assert validate_ticker("SPY", ROSTER) == "SPY"
    for text in ("spy", "NOPE", "SPY ", "", "SPY;DROP"):
        with pytest.raises(QueryParameterError):
            validate_ticker(text, ROSTER)


def test_validate_parameters_types_the_fields_and_refuses_unknown_ones():
    today = NAMED_QUERIES["today"]
    assert validate_parameters(today, {"date": "2026-08-24", "ticker": "SPY"}, ROSTER) == {
        "day": date(2026, 8, 24),
        "ticker": "SPY",
    }
    assert validate_parameters(today, {}, ROSTER) == {}
    with pytest.raises(QueryParameterError):
        validate_parameters(today, {"sql": "SELECT 1"}, ROSTER)
    with pytest.raises(QueryParameterError):
        validate_parameters(NAMED_QUERIES["now"], {"ticker": "SPY"}, ROSTER)


def test_parameter_errors_never_echo_the_value():
    with pytest.raises(QueryParameterError) as caught:
        validate_ticker("EVIL<script>", ROSTER)
    assert "EVIL" not in str(caught.value)
    with pytest.raises(QueryParameterError) as caught:
        parse_date("2026-99-99")
    assert "99" not in str(caught.value)


# -- the Host check ----------------------------------------------------------


@pytest.mark.parametrize(
    ("host", "allowed"),
    [
        ("localhost", True),
        ("LOCALHOST", True),
        ("localhost:8765", True),
        ("127.0.0.1", True),
        ("127.0.0.1:80", True),
        (" 127.0.0.1:8765 ", True),
        (None, False),
        ("", False),
        ("localhost:", False),
        ("localhost:abc", False),
        ("localhost.evil.example", False),
        ("evil.localhost", False),
        ("evil.localhost:8765", False),
        ("xlocalhost", False),
        ("xlocalhost:8765", False),
        ("evil.example", False),
        ("127.0.0.1.evil.example", False),
        ("0127.0.0.1", False),
        ("[::1]", False),
        ("[::1]:8765", False),
        ("localhost:8765:1", False),
        ("0.0.0.0", False),
    ],
)
def test_host_allowed(host: str | None, allowed: bool):
    assert host_allowed(host) is allowed


# -- the registry and the routes ---------------------------------------------


def test_the_registry_is_exactly_the_two_panels():
    assert set(NAMED_QUERIES) == {"now", "today"}
    assert NAMED_QUERIES["now"].parameters == frozenset()
    assert NAMED_QUERIES["today"].parameters == frozenset({"date", "ticker"})
    for name, query in NAMED_QUERIES.items():
        assert query.name == name
        assert callable(query.run)


def test_every_route_maps_to_a_registered_query():
    assert ROUTES == {"/api/now": "now", "/api/today": "today"}
    assert set(ROUTES.values()) <= set(NAMED_QUERIES)


def test_the_bind_is_loopback_and_not_configurable():
    assert dashboard.BIND_HOST == "127.0.0.1"
    parser = dashboard.build_parser()
    for flag in ("--host", "--bind", "--address"):
        with pytest.raises(SystemExit):
            parser.parse_args([flag, "0.0.0.0"])


# -- the slot denominator ----------------------------------------------------


def _bounds(day: date, open_h: int, open_m: int, close_h: int, close_m: int, early: bool):
    """Session bounds built from values, the way the session clock derives them."""
    opened = datetime(day.year, day.month, day.day, open_h, open_m, tzinfo=MARKET_TZ)
    equity_close = datetime(day.year, day.month, day.day, close_h, close_m, tzinfo=MARKET_TZ)
    option_close = equity_close + OPTION_CLOSE_OFFSET
    return SessionBounds(
        day=day,
        open=opened,
        equity_close=equity_close,
        option_close=option_close,
        option_close_deadline=option_close + OPTION_CLOSE_GUARD,
        compaction=option_close + COMPACTION_DELAY,
        early_close=early,
    )


def test_session_slots_run_from_the_open_through_the_option_close():
    bounds = _bounds(date(2026, 8, 24), 9, 30, 16, 0, early=False)
    slots = session_slots(bounds)
    assert len(slots) == 406
    assert slots[0] == bounds.open
    assert slots[-1] == bounds.option_close
    steps = zip(slots, slots[1:], strict=False)
    assert all((later - earlier) == dashboard.SLOT for earlier, later in steps)


def test_session_slots_shrink_on_an_early_close():
    bounds = _bounds(date(2026, 11, 27), 9, 30, 13, 0, early=True)
    slots = session_slots(bounds)
    assert len(slots) == 226
    assert slots[-1] == bounds.option_close


# -- the command line and the page -------------------------------------------


def test_build_parser_takes_a_port_and_a_lake_root():
    parser = dashboard.build_parser()
    args = parser.parse_args([])
    assert args.port == dashboard.DEFAULT_PORT
    assert args.lake_root is None
    assert args.config is None
    args = parser.parse_args(["--port", "9001", "--lake-root", "/lake", "--config", "c.yaml"])
    assert (args.port, args.lake_root, args.config) == (9001, "/lake", "c.yaml")


# The one line the page is allowed to carry that names a resource. It is pinned whole,
# so a link that changed its target, grew an attribute, or gained a sibling fails to
# match and is left for the marker sweep below to catch. The ``sizes`` value is pinned
# to the shape a size list takes, because ``[^"]*`` there would let a URL ride inside
# the one line the sweep never sees.
ICON_LINK = re.compile(
    rb'^<link rel="icon" href="/favicon\.ico" sizes="(?P<sizes>[0-9x ]*)">\n', re.MULTILINE
)


def test_the_status_page_ships_in_the_package_and_is_self_contained():
    page = dashboard.load_status_page()
    assert b"<title>" in page
    assert b"/api/now" in page
    assert b"/api/today" in page
    # The page declares exactly one resource: its own tab icon, on its own origin.
    rest, found = ICON_LINK.subn(b"", page)
    assert found == 1, "the page declares the tab icon exactly once"
    # No external resource: the page must work offline and inside the same-origin policy.
    # Every marker still runs, over everything except that one pinned line. Removing the
    # line rather than relaxing the markers is what keeps this guard from going slack: a
    # second ``<link``, an ``<img``, or any absolute URL still fails.
    for marker in (b"http://", b"https://", b"<link", b"<img", b"src="):
        assert marker not in rest


def test_the_declared_icon_sizes_match_the_sizes_the_icon_carries():
    # The ``sizes`` attribute is a claim about a binary the HTML cannot see. Pinning it
    # here means a size added to the renderer without updating the page fails a test
    # rather than shipping a page that misdescribes its own icon.
    match = ICON_LINK.search(dashboard.load_status_page())
    assert match is not None
    declared = match.group("sizes").decode().split()
    assert declared == [f"{side}x{side}" for side in favicon.SIZES]


# -- the tab icon ------------------------------------------------------------


def test_the_favicon_ships_in_the_package_as_a_three_size_ico():
    icon = dashboard.load_favicon()
    # The ICO directory header: two reserved zero bytes, type 1 for an icon, then the
    # image count. Reading it here is what makes this a test of the container and not
    # just of the file's length.
    reserved, kind, count = struct.unpack_from("<HHH", icon, 0)
    assert (reserved, kind) == (0, 1)
    assert count == len(favicon.SIZES)
    sizes = []
    for index in range(count):
        width, height, colours, pad = struct.unpack_from("<BBBB", icon, 6 + 16 * index)
        assert (colours, pad) == (0, 0), "a true-colour entry counts no palette"
        assert width == height, "the mark is square at every size"
        sizes.append(width)
    assert tuple(sizes) == favicon.SIZES
    # Every sub-image is a PNG. The directory entry never says so, because a bit count of
    # 32 describes a BMP sub-image just as well, so the form is read off the payload. The
    # payload's own dimensions are read too. Without that the entry is an unchecked
    # promise, and a container holding three 16-pixel frames under 16, 32 and 48 entries
    # would pass.
    for index, side in enumerate(sizes):
        length, offset = struct.unpack_from("<II", icon, 6 + 16 * index + 8)
        assert icon[offset : offset + 8] == b"\x89PNG\r\n\x1a\n"
        assert offset + length <= len(icon)
        declared = struct.unpack_from(">II", icon, offset + 16)
        assert declared == (side, side), "the payload is the size its entry claims"


def test_the_shipped_favicon_is_exactly_what_the_renderer_produces():
    # The golden pin. The checked-in binary is not the only record of the icon: a reader
    # who cannot diff 411 bytes can read ``MARK`` instead and trust that it is the same
    # thing. A mark edited without regenerating the file fails here.
    assert dashboard.load_favicon() == favicon.render()


def test_the_renderer_is_deterministic():
    assert favicon.render() == favicon.render()


def test_every_icon_size_is_a_whole_multiple_of_the_grid():
    # Integer scaling is what keeps the larger sizes crisp. A size off the grid would
    # land an edge on a fraction of a pixel, so the constraint is load-bearing, not tidy.
    assert favicon.GRID == len(favicon.MARK) == 16
    # Pinned as literals. Every other size assertion compares against ``SIZES``, so
    # without this the whole set could drift and the suite would still agree with it.
    assert favicon.SIZES == (16, 32, 48)
    assert all(len(row) == favicon.GRID for row in favicon.MARK)
    for side in favicon.SIZES:
        assert side % favicon.GRID == 0


def test_render_refuses_a_size_the_container_cannot_carry(monkeypatch):
    # ``render`` refuses two shapes of size.
    # 1. A size off the grid, which would put an edge part-way through a pixel.
    # 2. A size past 256, which would wrap in the entry's single width byte and ship a
    #    container describing a smaller image than it holds.
    monkeypatch.setattr(favicon, "SIZES", (24,))
    with pytest.raises(ValueError, match="whole multiple"):
        favicon.render()
    monkeypatch.setattr(favicon, "SIZES", (512,))
    with pytest.raises(ValueError, match="range an ICO entry"):
        favicon.render()


def test_the_renderer_cli_writes_the_same_bytes_it_ships(tmp_path):
    out = tmp_path / "favicon.ico"
    assert favicon.main(["--out", str(out)]) == 0
    assert out.read_bytes() == dashboard.load_favicon()


def test_the_cli_defaults_to_the_shipped_file():
    # The default output path is what makes ``python -m lake.favicon`` a regeneration
    # rather than a scratch render. A default pointing elsewhere would let the mark and
    # the shipped bytes drift apart without anyone running a second command.
    loaded = resources.files("lake").joinpath("static").joinpath(dashboard.FAVICON)
    assert favicon.packaged_path() == Path(str(loaded))
    # With no ``--out`` the parser leaves the choice to ``main``, which is what makes the
    # packaged path the default rather than a value argparse happens to hold.
    assert favicon.build_parser().parse_args([]).out is None


def test_the_cli_writes_its_default_path_when_given_no_out(tmp_path, monkeypatch):
    # Drives the branch the test above only reasons about. The default is redirected, so
    # the run proves ``main`` writes wherever ``packaged_path`` points without touching
    # the file the package ships.
    target = tmp_path / "favicon.ico"
    monkeypatch.setattr(favicon, "packaged_path", lambda: target)
    assert favicon.main([]) == 0
    assert target.read_bytes() == favicon.render()


def test_the_ink_is_the_pages_captured_colour():
    # The module docstring claims the ink is ``--captured``'s light-scheme value. That is
    # a claim about another file, so it is pinned the way the ``sizes`` attribute is. The
    # light value is the one on bare ``:root``. That rule is indented two spaces, and the
    # dark-scheme override nests four deep inside its media query, so the block is
    # anchored on the indentation. Matching the first ``:root`` instead would silently
    # compare against the dark value if the two ever swapped order.
    page = dashboard.load_status_page().decode("utf-8")
    block = re.search(r"(?m)^  :root \{(.*?)^  \}", page, re.S)
    assert block is not None, "the page declares a top-level :root rule"
    match = re.search(r"--captured:\s*(#[0-9a-fA-F]{6})\s*;", block.group(1))
    assert match is not None
    assert match.group(1).lower() == "#" + bytes(favicon.INK).hex()


def _decode_png(png: bytes) -> list[list[bytes]]:
    """Rows of RGBA pixels from one of the icon's sub-images.

    This reads only what ``lake.favicon`` writes: 8-bit RGBA, filter type 0 on every
    scanline, one ``IDAT``. It is not a general PNG reader, and the filter assertion is
    what keeps it honest if the encoder ever starts writing something else.
    """
    width, height = struct.unpack_from(">II", png, 16)
    data = b""
    offset = 8
    while offset < len(png):
        length, tag = struct.unpack_from(">I4s", png, offset)
        if tag == b"IDAT":
            data += png[offset + 8 : offset + 8 + length]
        offset += 12 + length
    raw = zlib.decompress(data)
    stride = width * 4
    rows = []
    for y in range(height):
        start = y * (stride + 1)
        assert raw[start] == 0, "the encoder writes filter type 0 only"
        line = raw[start + 1 : start + 1 + stride]
        rows.append([line[x * 4 : x * 4 + 4] for x in range(width)])
    return rows


def test_every_rendered_pixel_matches_the_mark_including_its_transparency():
    # The waterline argument rests on rendered alpha, not on the grid. A renderer that
    # painted the empty cells opaque would still satisfy ``MARK``, the golden pin and the
    # container test, and the icon would quietly stop reading on one tab strip or the
    # other. This is the assertion that makes the hole real.
    icon = dashboard.load_favicon()
    count = struct.unpack_from("<H", icon, 4)[0]
    ink = bytes(favicon.INK) + b"\xff"
    clear = b"\x00\x00\x00\x00"
    for index in range(count):
        side = struct.unpack_from("<B", icon, 6 + 16 * index)[0]
        length, offset = struct.unpack_from("<II", icon, 6 + 16 * index + 8)
        pixels = _decode_png(icon[offset : offset + length])
        scale = side // favicon.GRID
        assert len(pixels) == side
        for y, row in enumerate(favicon.MARK):
            for x, cell in enumerate(row):
                expected = ink if cell == "#" else clear
                # Every pixel of the cell, so a scaled render cannot be right only at its
                # corner. This is the check the transparency claim actually needs.
                for dy in range(scale):
                    for dx in range(scale):
                        got = pixels[y * scale + dy][x * scale + dx]
                        assert got == expected, f"{side}px cell ({x},{y}) offset ({dx},{dy})"


def test_the_waterline_row_is_empty():
    # The row between the columns and the lake is transparent on purpose. It shows the
    # tab strip through, which is how the separation survives a light strip and a dark
    # one alike. Filling it would silently cost the icon that.
    assert favicon.MARK[11] == "." * favicon.GRID
    assert set("".join(favicon.MARK)) == {"#", "."}


# -- the status vocabulary ---------------------------------------------------


def test_the_statuses_are_the_six_the_strip_reports():
    # The strip's ``counts`` dict is keyed by exactly these, and it is the strip's
    # denominator. A status added here without the payload following it, or the reverse,
    # leaves a cell counted nowhere.
    assert STATUSES == ("captured", "suspect", "gap", "missing", "pending", "out_of_scope")
    assert len(set(STATUSES)) == len(STATUSES)
    assert set(STATUSES) == {
        dashboard.STATUS_CAPTURED,
        dashboard.STATUS_SUSPECT,
        dashboard.STATUS_GAP,
        dashboard.STATUS_MISSING,
        dashboard.STATUS_PENDING,
        dashboard.STATUS_OUT_OF_SCOPE,
    }


# -- the entry point's lake-root branch ---------------------------------------


class _ServiceRecorder:
    """A stand-in for the service that records how ``main`` constructed it."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def __call__(self, lake_root, *, clock, calendar, guards):
        self.calls.append(
            {"lake_root": lake_root, "clock": clock, "calendar": calendar, "guards": guards}
        )
        return object()


def _wire_main(monkeypatch, *, load_config) -> _ServiceRecorder:
    """Replace every seam ``main`` wires, and make the bind fail so it returns at once."""
    recorder = _ServiceRecorder()
    monkeypatch.setattr(dashboard, "SystemClock", lambda: "the system clock")
    monkeypatch.setattr(dashboard, "ExchangeCalendar", lambda: "the exchange calendar")
    monkeypatch.setattr(dashboard, "DashboardService", recorder)
    monkeypatch.setattr(dashboard, "load_config", load_config)

    def refuse_to_bind(service, port):
        raise OSError(errno.EADDRINUSE, "Address already in use")

    monkeypatch.setattr(dashboard, "make_server", refuse_to_bind)
    return recorder


def _never_called(path=None):
    raise AssertionError("load_config must not be read when --lake-root is given")


def test_main_serves_the_lake_root_flag_and_never_reads_the_config(monkeypatch, capsys):
    recorder = _wire_main(monkeypatch, load_config=_never_called)
    assert dashboard.main(["--lake-root", "/fixture/lake", "--port", "9001"]) == 2
    assert recorder.calls == [
        {
            "lake_root": Path("/fixture/lake"),
            "clock": "the system clock",
            "calendar": "the exchange calendar",
            "guards": GuardConstants(),
        }
    ]
    # Serving a lake root directly reads no config, so the guards are the pinned defaults.
    assert "9001" in capsys.readouterr().err


def test_main_reads_the_lake_root_and_the_guards_from_the_config(monkeypatch, capsys):
    recalibrated = GuardConstants(watchdog_page_minutes=9)
    seen: list[str | None] = []

    def load_config(path=None):
        seen.append(path)
        return SimpleNamespace(lake_root=Path("/configured/lake"), guards=recalibrated)

    recorder = _wire_main(monkeypatch, load_config=load_config)
    assert dashboard.main(["--config", "machine.yaml"]) == 2
    assert seen == ["machine.yaml"]
    assert recorder.calls[0]["lake_root"] == Path("/configured/lake")
    assert recorder.calls[0]["guards"] is recalibrated
    capsys.readouterr()


def test_main_falls_back_to_the_default_config_location(monkeypatch):
    recalibrated = GuardConstants(watchdog_page_minutes=9)
    seen: list[str | None] = []

    def load_config(path=None):
        seen.append(path)
        return SimpleNamespace(lake_root=Path("/configured/lake"), guards=recalibrated)

    recorder = _wire_main(monkeypatch, load_config=load_config)
    assert dashboard.main([]) == 2
    assert seen == [None]
    assert recorder.calls[0]["lake_root"] == Path("/configured/lake")
