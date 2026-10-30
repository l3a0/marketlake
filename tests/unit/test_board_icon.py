"""The build board's tab icon, decided from values alone (#886).

The board's icon is the dashboard's mark in blue with a tick in its empty top-left
corner, and ``lake.favicon`` renders it with the same encoder as the dashboard's. The
grid, the ink and the command-line contract are each a value or a pure function, so the
tier is unit. Two cases touch the filesystem, and neither crosses a subsystem boundary.

1. The command-line case writes one file to a temporary directory.
2. The pixel case and the golden case read the shipped icon, as bytes. It lives at
   ``.github/board/favicon.ico``, the path the board's caller workflow names in its
   ``icon`` input (#942), and ``tests/component/test_board_workflow.py`` checks that
   input against this file.

The expected grid and ink are written here as literals rather than read from the
module's own constants, so a change to a constant cannot carry the test with it.
"""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from lake import favicon
from tests.unit.test_dashboard_contract import _decode_png

SHIPPED_ICON = Path(__file__).resolve().parents[2] / ".github" / "board" / "favicon.ico"

# The tick's cells as (column, row), counting from the grid's top-left.
TICK_CELLS = {
    (4, 0),
    (3, 1),
    (4, 1),
    (0, 2),
    (2, 2),
    (3, 2),
    (0, 3),
    (1, 3),
    (2, 3),
    (1, 4),
}

BLUE = (0x3B, 0x82, 0xF6)

# The board's whole grid, written out: the dashboard's mark with the tick in its empty
# top-left corner. ``#`` is the board's blue and ``.`` is transparent.
BOARD_GRID = (
    "....#...........",
    "...##...........",
    "#.##..####......",
    "###...####......",
    ".#....####..####",
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


def _inked(grid: tuple[str, ...]) -> set[tuple[int, int]]:
    return {(x, y) for y, row in enumerate(grid) for x, cell in enumerate(row) if cell == "#"}


def test_the_board_grid_is_the_dashboards_mark_plus_the_tick():
    # Every cell outside the tick is the mark's own, which covers the columns, the
    # transparent waterline and the water band. The tick's cells are all empty in the
    # mark, so the tick adds a shape rather than reshaping one.
    assert len(favicon.BOARD_MARK) == favicon.GRID
    assert all(len(row) == favicon.GRID for row in favicon.BOARD_MARK)
    assert not TICK_CELLS & _inked(favicon.MARK)
    assert _inked(favicon.BOARD_MARK) == _inked(favicon.MARK) | TICK_CELLS
    assert set("".join(favicon.BOARD_MARK)) == {"#", "."}


def test_the_tick_sits_in_the_corner_with_a_clear_row_and_column_beside_it():
    # Rows 0 to 4 and columns 0 to 4. Row 5 and column 5 stay clear of ink in that
    # corner's reach, so the tick never touches the left or centre column.
    assert all(x <= 4 and y <= 4 for x, y in TICK_CELLS)
    assert favicon.BOARD_MARK[5][:6] == "......"
    assert all(favicon.BOARD_MARK[y][5] == "." for y in range(6))


def test_with_corner_refuses_a_corner_that_inks_the_mark():
    # The centre column starts at column 6 on row 2, so a corner reaching that cell
    # overlaps the mark and is refused rather than merged into it.
    corner = ("", "", "......#")
    with pytest.raises(ValueError, match=r"\(6,2\)"):
        favicon.with_corner(favicon.MARK, corner)


def test_with_corner_leaves_every_other_cell_alone():
    marked = favicon.with_corner(favicon.MARK, ("#",))
    assert _inked(marked) == _inked(favicon.MARK) | {(0, 0)}


def test_the_board_ink_is_blue():
    assert favicon.BOARD_INK == BLUE


def test_the_board_icon_draws_its_grid_in_blue():
    # The expected grid is built here from the mark and the literal tick cells, and the
    # ink is the literal blue. Both sides go through ``render``, so this says only that
    # the board passes ``render`` the right grid and ink. The pixel test below says what
    # ``render`` draws with them.
    expected = tuple(
        "".join("#" if (x, y) in TICK_CELLS or cell == "#" else "." for x, cell in enumerate(row))
        for y, row in enumerate(favicon.MARK)
    )
    assert expected == BOARD_GRID
    assert favicon.render_board() == favicon.render(expected, BLUE)
    assert favicon.render_board() != favicon.render()


@pytest.mark.parametrize("source", ["rendered", "shipped"])
def test_every_pixel_of_the_board_icon_matches_the_literal_grid(source):
    # Every pixel at every size is compared with ``BOARD_GRID`` in the literal blue,
    # fully opaque where inked and fully transparent elsewhere. Neither side reads the
    # module's constants or calls ``render`` for the expected value, so a ``render``
    # that drops its grid or its ink fails here even after the shipped file has been
    # regenerated from it. The rendered case fails such a ``render`` before that.
    icon = favicon.render_board() if source == "rendered" else SHIPPED_ICON.read_bytes()
    ink = bytes((0x3B, 0x82, 0xF6, 0xFF))
    clear = bytes((0, 0, 0, 0))
    count = struct.unpack_from("<H", icon, 4)[0]
    sides = []
    for index in range(count):
        side = struct.unpack_from("<B", icon, 6 + 16 * index)[0]
        length, offset = struct.unpack_from("<II", icon, 6 + 16 * index + 8)
        pixels = _decode_png(icon[offset : offset + length])
        sides.append(side)
        scale = side // 16
        assert len(pixels) == side
        assert all(len(row) == side for row in pixels)
        for y in range(side):
            for x in range(side):
                cell = BOARD_GRID[y // scale][x // scale]
                expected = ink if cell == "#" else clear
                assert pixels[y][x] == expected, f"{side}px pixel ({x},{y})"
    assert sides == [16, 32, 48]


def test_the_shipped_icon_is_exactly_what_the_renderer_draws():
    """Protects the board's icon from drifting away from its grid (#886).

    Comparing the shipped file with ``render_board`` means the checked-in binary is never
    the only record of the icon, as with the dashboard's. A grid, an ink or a size changed
    without regenerating the file fails here.
    """
    assert SHIPPED_ICON.read_bytes() == favicon.render_board()


def test_the_module_names_the_path_the_icon_ships_at():
    # The literal is written out, so moving the constant without moving the file fails.
    assert favicon.BOARD_ICON == ".github/board/favicon.ico"
    assert SHIPPED_ICON.is_file()


def test_the_cli_writes_the_board_icon_with_board(tmp_path):
    out = tmp_path / "favicon.ico"
    assert favicon.main(["--board", "--out", str(out)]) == 0
    assert out.read_bytes() == favicon.render_board()


def test_the_cli_refuses_board_without_out(tmp_path, monkeypatch):
    # The board's file lives outside the package, so ``--board`` alone has no default to
    # fall back on. Writing the packaged path instead would overwrite the dashboard's
    # icon with the board's, so the parser refuses before anything is written.
    target = tmp_path / "favicon.ico"
    monkeypatch.setattr(favicon, "packaged_path", lambda: target)
    with pytest.raises(SystemExit) as exit_info:
        favicon.main(["--board"])
    assert exit_info.value.code == 2
    assert not target.exists()
