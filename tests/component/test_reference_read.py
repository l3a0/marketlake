"""The daemon's three reference readers say when a file is there and cannot be read.

Marketlake #536. On 2026-09-19 a reboot started the daemon a few seconds before the owner's
login session existed, and its first read of the lake came back ``EPERM``. The readers the
daemon leans on every cycle answered a file like that exactly as they answer an absent one,
and printed nothing. They still answer the same way. What changed is the silence: an
unreadable file prints one line when it first fails and one when it next reads, and an
absent one stays quiet.

Each unreadable file here is made so with ``chmod 000``, the way ``test_schema_versions.py``
and ``test_close_guard.py`` already lock a file. The mode goes back in a ``finally``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake import daemon, reference_read
from lake.alert import Publisher
from lake.capture import _live_roster
from lake.capture_spans import CaptureSpans, SpansUnreadable, spans_path
from lake.schema_drift import SchemaDriftObserver
from lake.security_master import MasterUnreadable, SecurityMaster, master_path
from lake.session import SessionClock
from lake.tickers import Roster, TickerConfig
from tests.support.calendar import weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import NTFY_TOPIC, PING_KEY, write_config
from tests.support.transport import FakeTransport

START = datetime(2026, 9, 8, 17, 7, tzinfo=UTC)
AT = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
LATER = datetime(2026, 9, 21, 13, 31, tzinfo=UTC)

# SPY has an open span. QQQ has a closed one, so a clamped roster drops it and a widened one
# keeps it. That difference is what lets a test see which answer the reader gave.
ROSTER = Roster(tickers=(TickerConfig("SPY"), TickerConfig("QQQ")))


@pytest.fixture(autouse=True)
def _fresh_record() -> Iterator[None]:
    """Each test starts from a process that has reported nothing."""
    reference_read.reset()
    yield
    reference_read.reset()


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """A lake whose master names SPY and QQQ, with SPY in scope and QQQ retired."""
    root = tmp_path / "lake"
    root.mkdir()
    master = SecurityMaster()
    spy = master.register(kind="equity", capture_start=START, valid_from=START.date(), ticker="SPY")
    qqq = master.register(kind="equity", capture_start=START, valid_from=START.date(), ticker="QQQ")
    master.write(master_path(root))
    spans = CaptureSpans()
    spans.open_span(spy, START, False)
    spans.open_span(qqq, START, False)
    spans.close_span(qqq, datetime(2026, 9, 15, 20, 0, tzinfo=UTC))
    spans.write(spans_path(root))
    return root


class _Locked:
    """``chmod 000`` on a file for the length of a ``with`` block."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> Path:
        os.chmod(self.path, 0o000)
        return self.path

    def __exit__(self, *exc: object) -> None:
        os.chmod(self.path, 0o644)


class _RaisingStderr:
    """A stderr whose every write fails, the way a full log volume does."""

    def write(self, text: str) -> int:
        raise OSError(28, "No space left on device")

    def flush(self) -> None:
        raise OSError(28, "No space left on device")


def _captured(roster: Roster) -> list[str]:
    return [entry.ticker for entry in roster.enabled]


def _lines(capsys) -> list[str]:
    return [line for line in capsys.readouterr().err.splitlines() if line]


# -- capture._live_roster -----------------------------------------------------------------


def test_the_roster_clamps_when_both_files_read_and_prints_nothing(lake, capsys):
    """The baseline the widening tests below are read against."""
    assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY"]
    assert _lines(capsys) == []


def test_an_unreadable_master_widens_the_roster_and_says_so_once(lake, capsys):
    """The widened answer is unchanged. The line is new, and it names the file and the class."""
    with _Locked(master_path(lake)):
        first = _live_roster(ROSTER, lake, AT)
        second = _live_roster(ROSTER, lake, LATER)

    assert _captured(first) == ["SPY", "QQQ"]
    assert _captured(second) == ["SPY", "QQQ"]
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {master_path(lake)} could not be read at ")
    assert AT.isoformat() in line
    assert ": PermissionError: " in line


def test_an_unreadable_spans_file_widens_the_roster_and_says_so(lake, capsys):
    with _Locked(spans_path(lake)):
        roster = _live_roster(ROSTER, lake, AT)

    assert _captured(roster) == ["SPY", "QQQ"]
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {spans_path(lake)} could not be read at {AT.isoformat()}")


def test_a_master_that_reads_again_says_so_once_with_its_instant(lake, capsys):
    """The line the 2026-09-19 page never had: the denial cleared, and nothing said so."""
    with _Locked(master_path(lake)):
        _live_roster(ROSTER, lake, AT)
    capsys.readouterr()

    assert _captured(_live_roster(ROSTER, lake, LATER)) == ["SPY"]
    assert _captured(_live_roster(ROSTER, lake, LATER)) == ["SPY"]

    assert _lines(capsys) == [f"reference: {master_path(lake)} reads again at {LATER.isoformat()}"]


def test_a_second_denial_after_a_recovery_is_reported_again(lake, capsys):
    """Once per transition, not once per process."""
    with _Locked(master_path(lake)):
        _live_roster(ROSTER, lake, AT)
    _live_roster(ROSTER, lake, AT)
    with _Locked(master_path(lake)):
        _live_roster(ROSTER, lake, LATER)

    lines = _lines(capsys)
    assert [line.split(" at ")[0] for line in lines] == [
        f"reference: {master_path(lake)} could not be read",
        f"reference: {master_path(lake)} reads again",
        f"reference: {master_path(lake)} could not be read",
    ]


def test_an_absent_master_widens_the_roster_and_stays_quiet(lake, capsys):
    """A fresh lake has no master, which is not a problem to log."""
    master_path(lake).unlink()

    assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY", "QQQ"]
    assert _lines(capsys) == []


def test_a_torn_master_is_unreadable_too(lake, capsys):
    """Anything but ``FileNotFoundError`` is reported, including a file that will not parse."""
    master_path(lake).write_bytes(b"not parquet at all")

    assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY", "QQQ"]
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {master_path(lake)} could not be read at ")


def test_a_line_that_cannot_be_written_never_costs_the_cycle(lake, monkeypatch):
    """``run_loop`` calls the cycle unguarded, so a raise here would end the daemon."""
    monkeypatch.setattr("sys.stderr", _RaisingStderr())
    with _Locked(master_path(lake)):
        roster = _live_roster(ROSTER, lake, AT)

    assert _captured(roster) == ["SPY", "QQQ"]


# -- the daemon's master and spans readers ------------------------------------------------


def test_the_gap_marker_and_the_close_guard_readers_print_one_line_between_them(lake, capsys):
    """The daemon builds each reader twice, once per consumer, and the record is shared."""
    clock = ManualClock(AT)
    for_gap = daemon._master_reader(lake, clock)
    for_guard = daemon._master_reader(lake, clock)

    with _Locked(master_path(lake)):
        assert for_gap() is None
        assert for_guard() is None
        assert _live_roster(ROSTER, lake, AT).enabled

    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {master_path(lake)} could not be read at {AT.isoformat()}")


def test_the_spans_reader_reports_an_unreadable_file_and_its_recovery(lake, capsys):
    clock = ManualClock(AT)
    read = daemon._spans_reader(lake, clock)

    with _Locked(spans_path(lake)):
        assert read() is None
    clock.set(LATER)
    assert read() is not None

    lines = _lines(capsys)
    assert len(lines) == 2
    denied = f"reference: {spans_path(lake)} could not be read at {AT.isoformat()}"
    assert lines[0].startswith(denied)
    assert lines[1] == f"reference: {spans_path(lake)} reads again at {LATER.isoformat()}"


def test_the_daemon_readers_stay_quiet_on_absent_files(lake, capsys):
    master_path(lake).unlink()
    spans_path(lake).unlink()
    clock = ManualClock(AT)

    assert daemon._master_reader(lake, clock)() is None
    assert daemon._spans_reader(lake, clock)() is None
    assert _lines(capsys) == []


def test_the_daemon_readers_survive_a_stderr_that_raises(lake, monkeypatch):
    """Both run from hooks ``run_loop`` does not guard."""
    monkeypatch.setattr("sys.stderr", _RaisingStderr())
    clock = ManualClock(AT)

    with _Locked(master_path(lake)), _Locked(spans_path(lake)):
        assert daemon._master_reader(lake, clock)() is None
        assert daemon._spans_reader(lake, clock)() is None


# -- the helper's own edges ---------------------------------------------------------------


def test_an_error_outside_the_callers_list_still_propagates(tmp_path):
    """The helper widens nothing a reader did not already catch."""

    def read(path: Path) -> object:
        raise KeyError("a programming error")

    with pytest.raises(KeyError):
        reference_read.read_or_none(tmp_path / "x", read, (OSError,), now=lambda: AT)


def test_a_file_not_found_is_quiet_even_when_the_list_names_oserror(tmp_path, capsys):
    """``FileNotFoundError`` is an ``OSError``, so the absent arm has to come first."""
    result = reference_read.read_or_none(
        tmp_path / "absent.parquet", SecurityMaster.read, (OSError,), now=lambda: AT
    )

    assert result is None
    assert _lines(capsys) == []


def test_a_file_not_found_answers_none_even_when_the_list_leaves_out_oserror(tmp_path, capsys):
    """An absent reference file is a fresh lake to every reader, whatever the reader lists."""
    result = reference_read.read_or_none(
        tmp_path / "absent.parquet", SecurityMaster.read, (ValueError,), now=lambda: AT
    )

    assert result is None
    assert _lines(capsys) == []


def test_a_clock_that_raises_costs_the_line_its_instant_and_nothing_else(tmp_path, capsys):
    def denied(path: Path) -> object:
        raise PermissionError(1, "Operation not permitted", str(path))

    def broken() -> datetime:
        raise RuntimeError("no clock")

    result = reference_read.read_or_none(tmp_path / "x", denied, (OSError,), now=broken)

    assert result is None
    (line,) = _lines(capsys)
    assert "could not be read at an unknown time: PermissionError: " in line


def test_an_exception_whose_message_raises_costs_the_line_and_nothing_else(tmp_path, capsys):
    """Building the line calls ``__str__``, so it sits inside the same guard as the print."""

    class Unprintable(OSError):
        def __str__(self) -> str:
            raise RuntimeError("no message")

    def read(path: Path) -> object:
        raise Unprintable()

    assert reference_read.read_or_none(tmp_path / "x", read, (OSError,), now=lambda: AT) is None
    assert _lines(capsys) == []


def test_a_message_with_newlines_prints_as_one_line(tmp_path, capsys):
    """pyarrow's decode errors carry newlines, and one event must stay one line in the log."""

    def read(path: Path) -> object:
        raise OSError("Couldn't deserialize thrift\nDeserializing page header failed.\n\n")

    reference_read.read_or_none(tmp_path / "x", read, (OSError,), now=lambda: AT)

    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.endswith("OSError: Couldn't deserialize thrift Deserializing page header failed.\n")


# -- every class each reader already caught still widens rather than raising ------------------
#
# The readers' catch lists predate marketlake #536, and the change moved them into a new call.
# A list that lost a member would let that class out of ``_live_roster`` into the cycle
# ``run_loop`` calls unguarded, which ends the daemon. So every member is driven, for both files,
# through both the capture reader and the daemon's reader. ``UnicodeDecodeError`` is the
# ``ValueError`` a real bit flip produced, about one flip in 300 under the mutation review.


def _decode_error(path: Path) -> object:
    raise UnicodeDecodeError("utf-8", b"\x97", 0, 1, "invalid start byte")


@pytest.mark.parametrize(
    ("file", "raised"),
    [
        ("master", MasterUnreadable(Path("m"))),
        ("master", "decode"),
        ("master", ValueError("a bad value")),
        ("spans", SpansUnreadable(Path("s"))),
        ("spans", "decode"),
        ("spans", ValueError("a bad value")),
    ],
)
def test_every_class_a_reader_catches_widens_and_says_so(lake, monkeypatch, capsys, file, raised):
    def fail(cls, path):
        if raised == "decode":
            _decode_error(path)
        raise raised

    target = SecurityMaster if file == "master" else CaptureSpans
    monkeypatch.setattr(target, "read", classmethod(fail))
    reader = daemon._master_reader if file == "master" else daemon._spans_reader

    assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY", "QQQ"]
    assert reader(lake, ManualClock(AT))() is None
    (line,) = _lines(capsys)
    assert "could not be read" in line


def test_a_torn_spans_file_widens_the_roster_and_leaves_the_daemon_reader_answering_none(
    lake, capsys
):
    spans_path(lake).write_bytes(b"not parquet at all")

    assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY", "QQQ"]
    assert daemon._spans_reader(lake, ManualClock(AT))() is None
    (line,) = _lines(capsys)
    assert line.startswith(f"reference: {spans_path(lake)} could not be read at ")


def test_both_files_unreadable_print_one_line_because_the_spans_are_not_read(lake, capsys):
    """The roster widens on the master alone, so the spans are never opened that cycle."""
    with _Locked(master_path(lake)), _Locked(spans_path(lake)):
        assert _captured(_live_roster(ROSTER, lake, AT)) == ["SPY", "QQQ"]

    (line,) = _lines(capsys)
    assert str(master_path(lake)) in line


def test_a_file_denied_then_deleted_then_restored_still_says_it_reads_again(lake, capsys):
    """The module docstring promises this sequence. The absent step must not clear the record."""
    good = master_path(lake).read_bytes()
    with _Locked(master_path(lake)):
        _live_roster(ROSTER, lake, AT)
    master_path(lake).unlink()
    _live_roster(ROSTER, lake, AT)
    master_path(lake).write_bytes(good)
    _live_roster(ROSTER, lake, LATER)

    lines = _lines(capsys)
    assert [line.split(" at ")[0] for line in lines] == [
        f"reference: {master_path(lake)} could not be read",
        f"reference: {master_path(lake)} reads again",
    ]


def test_the_failure_line_carries_the_exception_message_as_well_as_its_class(tmp_path, capsys):
    def denied(path: Path) -> object:
        raise PermissionError(1, "Operation not permitted", str(path))

    reference_read.read_or_none(tmp_path / "x", denied, (OSError,), now=lambda: AT)

    (line,) = _lines(capsys)
    assert "PermissionError: [Errno 1] Operation not permitted" in line


# -- the daemon hands its own clock to both consumers' readers --------------------------------


def _daemon_config(tmp_path: Path, lake: Path) -> tuple[str, str]:
    config = write_config(tmp_path, lake)
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: false}\n")
    return str(config), str(tickers)


def test_the_gap_marker_readers_stamp_the_daemon_clock(tmp_path, lake, capsys):
    config, tickers = _daemon_config(tmp_path, lake)
    clock = ManualClock(AT)
    marker = daemon._gap_marker(
        config, tickers, SessionClock(clock=clock, calendar=weekday_sessions()), clock
    )
    assert marker is not None

    with _Locked(master_path(lake)), _Locked(spans_path(lake)):
        assert marker._master() is None
        assert marker._spans() is None

    lines = _lines(capsys)
    assert len(lines) == 2
    assert all(f"could not be read at {AT.isoformat()}" in line for line in lines)


def test_the_close_guard_readers_stamp_the_daemon_clock(tmp_path, lake, capsys):
    config, tickers = _daemon_config(tmp_path, lake)
    clock = ManualClock(AT)
    guard = daemon._close_guard(
        config,
        tickers,
        SessionClock(clock=clock, calendar=weekday_sessions()),
        clock,
        observer=SchemaDriftObserver(),
        publisher=Publisher(
            lake_root=lake, transport=FakeTransport(), secrets=(PING_KEY, NTFY_TOPIC)
        ),
    )
    assert guard is not None

    with _Locked(master_path(lake)), _Locked(spans_path(lake)):
        assert guard._master() is None
        assert guard._spans() is None

    lines = _lines(capsys)
    assert len(lines) == 2
    assert all(f"could not be read at {AT.isoformat()}" in line for line in lines)
