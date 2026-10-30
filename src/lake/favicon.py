"""The dashboard's and the build board's browser-tab icons, and the grid they come from.

An alert names the dashboard panel to open, so the status page is a tab the owner returns
to. A tab with no icon is harder to pick out of a row of them. This module holds the mark
and the encoder that turns it into an ``.ico``, the multi-image container a browser asks
the origin for at ``/favicon.ico``.

**The mark.** Three capture columns of different heights stand in a band of water. The
columns are the minute cadence. The band is the lake those minutes land in.

**One ink, and why.** The icon is a single colour, ``#2e8b57``. That is the value the
page's ``--captured`` token takes in its light scheme. It is the colour a completeness
strip shows when a data cycle landed. The choice is the product's, not a contrast
optimum. Measuring it says what that costs. Contrast ratios against a white tab strip,
against Chrome's dark strip at ``#202124``, and against an active dark tab at
``#35363a``:

======  =====  =======  =======
ink     white  #202124  #35363a
======  =====  =======  =======
2e8b57  4.25   3.79     2.84
3aa870  2.99   5.38     4.03
7f8c8d  3.48   4.63     3.47
======  =====  =======  =======

No colour in the palette is comfortable everywhere. The bar below is 3:1, which is what
WCAG 2.2 asks of a graphic that carries meaning without text. Only ``--missing`` at
``#7f8c8d`` clears it on all three. That is the grey meaning no data, so a mark built from it reads
as the product failing. ``--captured`` also has a dark-scheme value, ``#3aa870``, and it
holds a better worst case, 2.99 against 2.84. It gives up the white strip to get there,
and a white strip is where most tabs sit. So ``#2e8b57`` ships. Its weakest reading is 2.84
against an active dark tab, under that bar. The mark survives
there because the silhouette is large blocks and not thin strokes.

A second ink would have to come from the rest of the palette, and the rest scores worse.
``--suspect`` falls to 2.15 against white. ``--gap`` falls to 2.22 against an active dark
tab. Either would carry detail that vanishes on one strip or the other. One ink avoids
spending an ink that way.

**The waterline is transparent.** The row between the columns and the lake carries no
ink, so it shows whatever the tab strip is. The browser's own background draws the
separation, which is why it reads on a light strip and a dark strip alike.

**Sizes.** The container holds 16, 32 and 48 pixel squares, and a browser picks the one
it wants. A display with two device pixels per CSS pixel draws a 16-pixel tab slot with
32, so that size earns its place. Every feature of the grid sits on a whole pixel at 16,
so the larger sizes are exact integer scalings with no blurred or half-covered edge.

The sub-images are PNG rather than the older BMP form. PNG avoids the BMP quirk where the
header must claim double the real height to leave room for a separate transparency mask.
Safari 26.6.2 and Chrome 152.0.7977.82 both decode the shipped file. That is the support
this dashboard needs, because it is opened by hand on one Mac over the loopback address.

**The board's icon.** The build board is the second page the owner keeps in a tab, so it
needs an icon too. It reuses the dashboard's mark rather than a design of its own, so both
tabs read as marketlake. The board's grid is ``MARK`` with a tick added in the top-left
corner, rows 0 to 4 and columns 0 to 4, which the mark leaves empty. A clear row and a clear
column still separate the tick from the columns, so it reads as its own shape. The tick is
there because two tabs from one product sit side by side, and a hue alone is easy to miss
at 16 pixels. ``with_corner`` refuses a corner that would ink a cell the mark already inks,
so the tick can never change the columns or the water it sits beside.

The board's ink is blue, ``#3b82f6``. Against the same three tab strips it measures 3.68,
4.38 and 3.28, so it clears 3:1 on all three, where the dashboard's green reads 4.25, 3.79
and 2.84. The board's mark carries no meaning tied to the dashboard's palette, so its ink
can be chosen for contrast alone. Amber was considered and rejected. The sibling
``quantitative-trading`` repository's board icon is an amber disc, and two amber board
tabs open side by side would differ only in shape.

``render`` is deterministic and depends on nothing outside the standard library. A golden
test compares each icon against its shipped file, so a checked-in binary is never the only
record of what an icon is. Regenerate the dashboard's file with ``python -m lake.favicon``.
The board's file lives in the repository rather than the package, at ``BOARD_ICON``, which
the board's caller workflow names in its ``icon`` input (#942). So its command names it,
run from the repository's root: ``python -m lake.favicon --board --out
.github/board/favicon.ico``.
"""

from __future__ import annotations

import argparse
import struct
import zlib
from collections.abc import Sequence
from pathlib import Path

# The mark at its 16-pixel size, one character per pixel. ``#`` is ink and ``.`` is
# transparent. The waterline is the empty row between the columns and the lake. It is
# ``MARK[11]``, so the twelfth row counting down from the top of the mark.
MARK: tuple[str, ...] = (
    "................",
    "................",
    "......####......",
    "......####......",
    "......####..####",
    "......####..####",
    "####..####..####",
    "####..####..####",
    "####..####..####",
    "####..####..####",
    "####..####..####",
    "................",
    "################",
    "################",
    "################",
    "################",
)

# The dashboard's ``--captured`` sea green, as red, green and blue bytes.
INK = (0x2E, 0x8B, 0x57)

# The board's tick, as the cells it inks in the mark's top-left corner. ``with_corner``
# lays it over ``MARK`` and refuses it if it would touch a cell the mark already inks.
TICK: tuple[str, ...] = (
    "....#",
    "...##",
    "#.##.",
    "###..",
    ".#...",
)

# The board's blue, as red, green and blue bytes.
BOARD_INK = (0x3B, 0x82, 0xF6)

# The square sizes the container carries. Each must be a whole multiple of the grid, so
# that every edge in the mark lands on a pixel boundary at every size.
SIZES = (16, 32, 48)

GRID = len(MARK)


def with_corner(mark: Sequence[str], corner: Sequence[str]) -> tuple[str, ...]:
    """``mark`` with the ink of ``corner`` added at its top-left.

    Every other cell is the mark's own, so the result shares the mark's columns and water
    rather than carrying a second copy of them. A corner that would ink a cell the mark
    already inks raises ``ValueError``, because the corner is meant to sit where the mark
    is empty, and overlapping ink would merge the two shapes into one.
    """
    rows = [list(line) for line in mark]
    for y, line in enumerate(corner):
        for x, cell in enumerate(line):
            if cell != "#":
                continue
            if rows[y][x] != ".":
                raise ValueError(f"the corner inks cell ({x},{y}), which the mark already inks")
            rows[y][x] = "#"
    return tuple("".join(line) for line in rows)


# The board's grid: the dashboard's mark with the tick in its empty corner.
BOARD_MARK = with_corner(MARK, TICK)


def _rgba(mark: Sequence[str], ink: tuple[int, int, int], scale: int) -> bytes:
    """``mark`` as raw top-down RGBA rows, each grid cell drawn ``scale`` pixels square."""
    opaque = bytes(ink) + b"\xff"
    clear = b"\x00\x00\x00\x00"
    rows = [b"".join((opaque if cell == "#" else clear) * scale for cell in line) for line in mark]
    return b"".join(row for row in rows for _ in range(scale))


def _chunk(tag: bytes, data: bytes) -> bytes:
    """One PNG chunk: length, tag, payload, then the CRC over tag and payload."""
    return (
        struct.pack(">I", len(data))
        + tag
        + data
        + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    )


def _png(side: int, rgba: bytes) -> bytes:
    """An 8-bit RGBA PNG of ``side`` squared, from raw rows.

    Every scanline is written with filter type 0, which means no filtering. Filters exist
    to make the zlib pass compress better on photographic rows. These rows are flat runs
    of two values, so filtering would buy nothing and would only add a way to be wrong.
    """
    raw = bytearray()
    stride = side * 4
    for y in range(side):
        raw.append(0)
        raw += rgba[y * stride : (y + 1) * stride]
    header = struct.pack(">IIBBBBB", side, side, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", header)
        + _chunk(b"IDAT", zlib.compress(bytes(raw), 9))
        + _chunk(b"IEND", b"")
    )


def _ico(images: Sequence[tuple[int, bytes]]) -> bytes:
    """Wrap sub-images in an ICO container.

    The layout is a six-byte directory header, then one sixteen-byte entry per image,
    then the image payloads. An entry's width and height are single bytes, so 256 is
    written as 0 and nothing larger can be named. ``render`` refuses a size past 256 for
    that reason, rather than letting it wrap here into a smaller number. Colour count and
    the reserved byte are 0 for a true-colour image, planes is 1, and bit count is 32 for
    RGBA.
    """
    offset = 6 + 16 * len(images)
    entries = bytearray()
    payloads = bytearray()
    for side, png in images:
        entries += struct.pack("<BBBBHHII", side % 256, side % 256, 0, 0, 1, 32, len(png), offset)
        payloads += png
        offset += len(png)
    return struct.pack("<HHH", 0, 1, len(images)) + bytes(entries) + bytes(payloads)


def render(mark: Sequence[str] = MARK, ink: tuple[int, int, int] = INK) -> bytes:
    """An icon's bytes: one PNG per size in ``SIZES``, wrapped in an ICO container.

    The defaults draw the dashboard's icon. ``render_board`` passes the board's grid and ink.
    """
    for side in SIZES:
        if side % GRID:
            raise ValueError(f"size {side} is not a whole multiple of the {GRID}-pixel grid")
        if not 0 < side <= 256:
            raise ValueError(f"size {side} is outside the range an ICO entry can name")
    return _ico([(side, _png(side, _rgba(mark, ink, side // GRID))) for side in SIZES])


def render_board() -> bytes:
    """The build board's icon: the board's grid in the board's ink."""
    return render(BOARD_MARK, BOARD_INK)


# Where the board's icon lives, relative to the repository's root. The reusable board
# workflow serves the file that ``.github/workflows/board.yml``'s ``icon`` input names,
# which is this path. It is relative because an installed package has no repository
# around it, so ``--board`` still needs ``--out``.
BOARD_ICON = ".github/board/favicon.ico"


def packaged_path() -> Path:
    """Where the rendered icon ships: beside the status page, inside the package."""
    return Path(__file__).resolve().parent / "static" / "favicon.ico"


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for regenerating the shipped icon."""
    parser = argparse.ArgumentParser(
        prog="python -m lake.favicon",
        description="Render the dashboard's browser-tab icon, or the build board's.",
    )
    parser.add_argument(
        "--board",
        action="store_true",
        help=f"Render the build board's icon instead. Needs --out, such as --out {BOARD_ICON}.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Where to write the icon. Defaults to the packaged path for the dashboard's.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Render an icon and write it. Fully offline, and deterministic.

    The board's file lives in the repository, outside the package, so no path inside the
    package can stand in for it. ``--board`` therefore needs ``--out``.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.board:
        if args.out is None:
            parser.error(
                f"--board needs --out, such as --out {BOARD_ICON} from the repository's root, "
                "since the board's icon lives outside the package"
            )
        out, body = args.out, render_board()
    else:
        out = args.out if args.out is not None else packaged_path()
        body = render()
    out.write_bytes(body)
    print(f"wrote {out} ({len(body)} bytes, sizes {', '.join(str(s) for s in SIZES)})")
    return 0


__all__ = [
    "BOARD_ICON",
    "BOARD_INK",
    "BOARD_MARK",
    "GRID",
    "INK",
    "MARK",
    "SIZES",
    "TICK",
    "build_parser",
    "main",
    "packaged_path",
    "render",
    "render_board",
    "with_corner",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
