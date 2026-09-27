"""The daemon's line naming enabled tickers the capture spans left out (marketlake #554).

``daemon._OutOfSpanLine`` decides when the line prints. The wiring tests drive it through
the loop for a few minutes. What those cannot reach cheaply is a session date turning
over, so the rules are driven here directly.
"""

from __future__ import annotations

import io
from datetime import UTC

from lake import daemon
from tests.support.calendar import et


def _lines(capsys) -> list[str]:
    return capsys.readouterr().err.splitlines()


def test_a_standing_set_prints_once_on_the_change_and_not_every_minute(capsys):
    line = daemon._OutOfSpanLine()
    for minute in range(3):
        line.observe(et(2026, 9, 2, 10, minute), ("XYZ",))
    (only,) = _lines(capsys)
    assert only.startswith("capture: 2026-09-02T10:00:00-04:00: 1 enabled ticker(s)")


def test_an_empty_set_prints_nothing_until_something_is_left_out(capsys):
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ())
    line.observe(et(2026, 9, 3, 10, 0), ())
    assert _lines(capsys) == []


def test_a_changed_set_prints_again_and_names_every_ticker(capsys):
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ("XYZ",))
    line.observe(et(2026, 9, 2, 10, 1), ("ABC", "XYZ"))
    first, second = _lines(capsys)
    assert "not captured: XYZ." in first
    assert "2 enabled ticker(s)" in second and "not captured: ABC, XYZ." in second


def test_a_swap_of_the_same_size_prints_again(capsys):
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ("XYZ",))
    line.observe(et(2026, 9, 2, 10, 1), ("ABC",))
    _, second = _lines(capsys)
    assert "not captured: ABC." in second


def test_a_set_that_shrinks_without_emptying_prints_again(capsys):
    """Otherwise the last line keeps naming a ticker that is back in a span."""
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ("ABC", "XYZ"))
    line.observe(et(2026, 9, 2, 10, 1), ("XYZ",))
    _, second = _lines(capsys)
    assert "1 enabled ticker(s)" in second and "not captured: XYZ." in second


def test_recovery_prints_once_and_does_not_claim_a_span(capsys):
    """The set also empties when a missing file widens the roster, so the line says less."""
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ("XYZ",))
    line.observe(et(2026, 9, 2, 10, 1), ())
    line.observe(et(2026, 9, 2, 10, 2), ())
    _, recovered = _lines(capsys)
    assert recovered.startswith("capture: 2026-09-02T10:01:00-04:00: ")
    assert "left out" in recovered and "inside" not in recovered


def test_a_set_standing_into_the_next_session_prints_again_once(capsys):
    """The dead-man pages again every morning, so the cause has to be near that page."""
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 15, 59), ("XYZ",))
    line.observe(et(2026, 9, 2, 16, 0), ("XYZ",))
    line.observe(et(2026, 9, 3, 9, 30), ("XYZ",))
    line.observe(et(2026, 9, 3, 9, 31), ("XYZ",))
    lines = _lines(capsys)
    assert [text.split(": ")[1] for text in lines] == [
        "2026-09-02T15:59:00-04:00",
        "2026-09-03T09:30:00-04:00",
    ]


def test_the_session_date_is_the_market_date_not_the_utc_one(capsys):
    """20:30 ET on 09-02 is 00:30 UTC on 09-03, and it is still the same session date.

    The slots go in as UTC, the way a clock may hand them over, so a date read without
    converting to the market's zone lands on 09-03 and prints a second line.
    """
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 16, 0).astimezone(UTC), ("XYZ",))
    line.observe(et(2026, 9, 2, 20, 30).astimezone(UTC), ("XYZ",))
    assert len(_lines(capsys)) == 1


def test_a_line_that_cannot_be_written_is_dropped_rather_than_raised(monkeypatch):
    """``run_loop`` wraps no hook, so a raise here would end the daemon."""

    class _Full:
        def write(self, text):
            raise OSError(28, "No space left on device")

        def flush(self):
            raise OSError(28, "No space left on device")

    monkeypatch.setattr("sys.stderr", _Full())
    line = daemon._OutOfSpanLine()
    line.observe(et(2026, 9, 2, 10, 0), ("XYZ",))
    line.observe(et(2026, 9, 2, 10, 1), ())


def test_a_closed_stderr_is_dropped_rather_than_raised(monkeypatch):
    """A closed stream raises ``ValueError``, not ``OSError``, and must not end the daemon."""
    closed = io.StringIO()
    closed.close()
    monkeypatch.setattr("sys.stderr", closed)
    daemon._OutOfSpanLine().observe(et(2026, 9, 2, 10, 0), ("XYZ",))


def test_a_dropped_line_still_records_the_set(monkeypatch, capsys):
    """A failed write costs that line. It does not make every later minute try again."""
    line = daemon._OutOfSpanLine()

    class _Full:
        def write(self, text):
            raise OSError(28, "No space left on device")

    monkeypatch.setattr("sys.stderr", _Full())
    line.observe(et(2026, 9, 2, 10, 0), ("XYZ",))
    monkeypatch.undo()
    line.observe(et(2026, 9, 2, 10, 1), ("XYZ",))
    assert _lines(capsys) == []
