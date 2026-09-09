"""The dashboard's browser-tab icon, and the pixel grid it is rendered from.

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

``render`` is deterministic and depends on nothing outside the standard library. A golden
test pins its output against the shipped file, so the checked-in binary is never the only
record of what the icon is. Regenerate the file with ``python -m lake.favicon``.
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

# The square sizes the container carries. Each must be a whole multiple of the grid, so
# that every edge in the mark lands on a pixel boundary at every size.
SIZES = (16, 32, 48)

GRID = len(MARK)


def _rgba(scale: int) -> bytes:
    """The mark as raw top-down RGBA rows, each grid cell drawn ``scale`` pixels square."""
    ink = bytes(INK) + b"\xff"
    clear = b"\x00\x00\x00\x00"
    rows = [b"".join((ink if cell == "#" else clear) * scale for cell in line) for line in MARK]
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


def render() -> bytes:
    """The icon's bytes: one PNG per size in ``SIZES``, wrapped in an ICO container."""
    for side in SIZES:
        if side % GRID:
            raise ValueError(f"size {side} is not a whole multiple of the {GRID}-pixel grid")
        if not 0 < side <= 256:
            raise ValueError(f"size {side} is outside the range an ICO entry can name")
    return _ico([(side, _png(side, _rgba(side // GRID))) for side in SIZES])


def packaged_path() -> Path:
    """Where the rendered icon ships: beside the status page, inside the package."""
    return Path(__file__).resolve().parent / "static" / "favicon.ico"


def build_parser() -> argparse.ArgumentParser:
    """The command-line contract for regenerating the shipped icon."""
    parser = argparse.ArgumentParser(
        prog="python -m lake.favicon",
        description="Render the dashboard's browser-tab icon.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Where to write the icon. Defaults to the packaged path.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Render the icon and write it. Fully offline, and deterministic."""
    args = build_parser().parse_args(argv)
    out = args.out if args.out is not None else packaged_path()
    body = render()
    out.write_bytes(body)
    print(f"wrote {out} ({len(body)} bytes, sizes {', '.join(str(s) for s in SIZES)})")
    return 0


__all__ = [
    "GRID",
    "INK",
    "MARK",
    "SIZES",
    "build_parser",
    "main",
    "packaged_path",
    "render",
]


if __name__ == "__main__":  # pragma: no cover - exercised via the console, not in CI
    raise SystemExit(main())
