"""``python -m lake.roster apply`` refuses a roster that disagrees with the host's lake.

Marketlake #692. A tracked roster can drop a ticker the lake still owes: a retire pull
request merged before ``lake.retire`` ran, a restore from a bucket that lags a retire, or
an onboard on a host with no pull request yet. The lake then has an open capture span no
roster entry captures, and nothing pages on that. These tests build lakes with the real
writers, the way ``test_reference_read.py``'s ``lake`` fixture does, and drive the check
through ``apply`` and through ``main``.

SPY's span is open with options. QQQ's span is closed. A roster of SPY alone has to pass,
because a check that selected every span, or used ``SecurityMaster.in_scope``, would name
QQQ too and refuse it.
"""

from __future__ import annotations

import io
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import roster as roster_cli
from lake.capture_spans import CaptureSpans, UnsupportedSpansSchemaVersion, spans_path
from lake.onboard import DEFAULT_BARS, DEFAULT_CHAIN_CADENCE
from lake.roster import REPLACED, UNCHANGED, RosterError, apply
from lake.security_master import SecurityMaster, UnsupportedSchemaVersion, master_path
from lake.tickers import load_tickers, roster_from_bytes, upsert_ticker
from tests.component.test_reference_read import _Locked
from tests.support.clock import ManualClock
from tests.support.config import write_config

START = datetime(2026, 9, 8, 17, 7, tzinfo=UTC)
CLOSED = datetime(2026, 9, 15, 20, 0, tzinfo=UTC)
AT = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
# 21:00 in New York on 2026-09-21, which is already 2026-09-22 in UTC.
EVENING = datetime(2026, 9, 22, 1, 0, tzinfo=UTC)
# 10:00 in New York on 2026-09-22, when the master names the unnamed lake's IWM.
NAMED = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)

SPY_ONLY = b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
QQQ_ONLY = b"QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
SPY_DISABLED = (
    b"SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d], enabled: false}\n"
    b"QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
)
SPY_WITHOUT_OPTIONS = b"SPY: {options: false, bars: [1m, 1d]}\n"


def _build_lake(
    root: Path, *, unnamed: bool = False, ticker: str = "SPY", options: bool = True
) -> Path:
    """A lake whose master names SPY and QQQ, with SPY's span open and QQQ's closed.

    ``ticker`` and ``options`` replace SPY's name and its open span's options, for the
    tests that need another ticker or a span without options.

    ``unnamed`` adds a third instrument with an open span and a ticker valid from the day
    after ``AT``'s market date. The master cannot name it on ``AT``, and can on any day
    since, so a check that read the system clock instead of the injected one would name
    it and refuse.
    """
    root.mkdir()
    master = SecurityMaster()
    spy = master.register(
        kind="equity", capture_start=START, valid_from=START.date(), ticker=ticker
    )
    qqq = master.register(kind="equity", capture_start=START, valid_from=START.date(), ticker="QQQ")
    spans = CaptureSpans()
    spans.open_span(spy, START, options)
    spans.open_span(qqq, START, True)
    spans.close_span(qqq, CLOSED)
    if unnamed:
        later = master.register(
            kind="equity", capture_start=START, valid_from=date(2026, 9, 22), ticker="IWM"
        )
        spans.open_span(later, START, False)
    master.write(master_path(root))
    spans.write(spans_path(root))
    return root


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    return _build_lake(tmp_path / "lake")


@pytest.fixture
def target(tmp_path: Path) -> Path:
    """Where the roster lands. Its directory does not exist until a write creates it."""
    return tmp_path / "out" / "tickers.yaml"


def _apply(
    payload: bytes,
    tmp_path: Path,
    lake: Path,
    target: Path,
    role: str | None = None,
    at: datetime = AT,
):
    config = write_config(tmp_path / "cfg", lake, role=role)
    return apply(payload, clock=ManualClock(at), config_path=config, tickers_path=target)


def _held(target: Path, payload: bytes) -> None:
    target.parent.mkdir()
    target.write_bytes(payload)


def _listing(directory: Path) -> list[str]:
    return sorted(entry.name for entry in directory.iterdir())


# -- test 1: an open span the payload omits ---------------------------------------------


def test_a_roster_omitting_an_open_span_is_refused_on_a_fresh_write(tmp_path, lake, target):
    with pytest.raises(RosterError) as excinfo:
        _apply(QQQ_ONLY, tmp_path, lake, target)
    message = str(excinfo.value)
    assert "SPY has an open capture span" in message
    assert "with options true" in message
    assert "no entry in this roster names it" in message
    # The remedy is a whole entry, so pasting it cannot stop SPY's bars.
    assert "SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}" in message
    assert "\n" not in message
    assert not target.parent.exists()


def test_every_problem_is_named_on_one_line(tmp_path, target):
    # On NAMED the master names IWM, whose span is open without options. The roster
    # omits SPY and IWM, so the refusal carries two problems, joined on one line.
    lake = _build_lake(tmp_path / "lake", unnamed=True)
    with pytest.raises(RosterError) as excinfo:
        _apply(QQQ_ONLY, tmp_path, lake, target, at=NAMED)
    message = str(excinfo.value)
    assert "SPY has an open capture span" in message
    assert "IWM has an open capture span" in message
    assert "with options false" in message
    assert message.endswith("IWM: {options: false, bars: [1m, 1d]}")
    assert "\n" not in message
    assert not target.parent.exists()


def test_a_roster_omitting_an_open_span_leaves_the_old_roster_in_place(tmp_path, lake, target):
    _held(target, SPY_ONLY)
    before = target.stat().st_ino
    with pytest.raises(RosterError, match="SPY has an open capture span"):
        _apply(QQQ_ONLY, tmp_path, lake, target)
    assert target.read_bytes() == SPY_ONLY
    assert target.stat().st_ino == before
    assert _listing(target.parent) == ["tickers.yaml"]


def test_an_equal_roster_omitting_an_open_span_is_still_refused(tmp_path, lake, target):
    # The host already holds the roster that drops SPY, so the bytes match and nothing
    # would be written. The check still runs, because SPY is not being captured now.
    _held(target, QQQ_ONLY)
    with pytest.raises(RosterError, match="SPY has an open capture span"):
        _apply(QQQ_ONLY, tmp_path, lake, target)
    assert target.read_bytes() == QQQ_ONLY


def test_a_roster_of_the_open_span_alone_passes_and_says_so(tmp_path, lake, target, capsys):
    # QQQ's span is closed and the roster leaves it out. Selecting every span, or using
    # ``in_scope``, would name QQQ and refuse.
    assert _apply(SPY_ONLY, tmp_path, lake, target) == REPLACED
    assert target.read_bytes() == SPY_ONLY
    captured = capsys.readouterr()
    assert captured.out == f"roster: lake check passed, 1 open capture span checked in {lake}\n"
    assert captured.err == ""


def test_an_equal_passing_roster_is_checked_again(tmp_path, lake, target, capsys):
    _held(target, SPY_ONLY)
    assert _apply(SPY_ONLY, tmp_path, lake, target) == UNCHANGED
    assert "roster: lake check passed" in capsys.readouterr().out


# -- test 2: a disabled entry for an open span -------------------------------------------


def test_a_disabled_entry_for_an_open_span_names_both_branches(tmp_path, lake, target):
    # QQQ stays enabled, so the no-enabled-ticker refusal, which runs first, cannot be
    # what answers.
    with pytest.raises(RosterError) as excinfo:
        _apply(SPY_DISABLED, tmp_path, lake, target)
    message = str(excinfo.value)
    assert "SPY is disabled in this roster while its capture span" in message
    assert "(options true)" in message
    assert "python -m lake.retire SPY on this host after the close" in message
    assert "otherwise fix config/tickers.yaml in a pull request" in message
    # Onboarding on a host with no roster writes one holding only that ticker.
    assert "lake.onboard" not in message
    assert not target.parent.exists()


def test_a_disabled_line_prints_the_span_flag_not_the_entry_flag(tmp_path, lake, target):
    # SPY's span is open with options, and its disabled entry says options false.
    payload = (
        b"SPY: {options: false, bars: [1m, 1d], enabled: false}\n"
        b"QQQ: {options: true, chain_cadence: 1m, bars: [1m, 1d]}\n"
    )
    with pytest.raises(RosterError, match=r"is open \(options true\)"):
        _apply(payload, tmp_path, lake, target)
    assert not target.parent.exists()


# -- test 3: an enabled entry that drops the span's options ------------------------------


def test_an_entry_without_options_for_a_span_with_options_is_refused(tmp_path, lake, target):
    with pytest.raises(RosterError) as excinfo:
        _apply(SPY_WITHOUT_OPTIONS, tmp_path, lake, target)
    message = str(excinfo.value)
    assert "SPY has an open capture span" in message
    assert "with options true" in message
    assert "this roster's entry has options false" in message
    assert "SPY: {options: true, chain_cadence: 1m, bars: [1m, 1d]}" in message
    assert not target.parent.exists()


def test_an_entry_with_options_for_a_span_without_options_passes(tmp_path, target, capsys):
    # The opposite mismatch only captures more, so it is not refused. A check comparing
    # the two flags for inequality would refuse it.
    lake = _build_lake(tmp_path / "lake", options=False)
    assert _apply(SPY_ONLY, tmp_path, lake, target) == REPLACED
    assert target.read_bytes() == SPY_ONLY
    captured = capsys.readouterr()
    assert captured.out == f"roster: lake check passed, 1 open capture span checked in {lake}\n"
    assert captured.err == ""


def test_an_entry_without_options_for_a_span_without_options_passes(tmp_path, target, capsys):
    # A check refusing every entry without options would refuse this one.
    lake = _build_lake(tmp_path / "lake", options=False)
    assert _apply(SPY_WITHOUT_OPTIONS, tmp_path, lake, target) == REPLACED
    assert target.read_bytes() == SPY_WITHOUT_OPTIONS
    captured = capsys.readouterr()
    assert captured.out == f"roster: lake check passed, 1 open capture span checked in {lake}\n"
    assert captured.err == ""


# -- the remedy line pastes back as the entry onboard writes ----------------------------


REMEDY = "built from onboard's defaults: "


# The long ticker is past PyYAML's default width of 80, where a dump would wrap the line.
@pytest.mark.parametrize("ticker", ["SPY", "ON", "NO", "X" * 60], ids=["SPY", "ON", "NO", "long"])
@pytest.mark.parametrize("options", [True, False], ids=["options", "no-options"])
def test_the_remedy_line_reads_back_as_the_entry_onboard_writes(tmp_path, target, ticker, options):
    # ``ON`` and ``NO`` are booleans in YAML 1.1. Printed bare, the line would read back
    # as a ticker called ``True`` or ``False``. The roster omits the span's ticker, so
    # the refusal's only problem is that span, and its remedy ends the message. The line
    # is pasted under the roster's own entry, the way the refusal says to add it.
    lake = _build_lake(tmp_path / "lake", ticker=ticker, options=options)
    with pytest.raises(RosterError) as excinfo:
        _apply(QQQ_ONLY, tmp_path, lake, target)
    message = str(excinfo.value)
    assert "\n" not in message
    assert message.count(REMEDY) == 1
    pasted = roster_from_bytes(QQQ_ONLY + message.split(REMEDY)[1].encode() + b"\n")
    assert pasted.symbols == ("QQQ", ticker)
    onboarded = tmp_path / "onboarded.yaml"
    onboarded.write_bytes(QQQ_ONLY)
    upsert_ticker(
        ticker,
        options=options,
        chain_cadence=DEFAULT_CHAIN_CADENCE,
        bars=DEFAULT_BARS,
        path=onboarded,
    )
    # ``upsert_ticker`` sorts the file's keys, so the two rosters compare by ticker.
    assert {e.ticker: e for e in pasted} == {e.ticker: e for e in load_tickers(onboarded)}
    assert not target.parent.exists()


# -- test 4: a missing reference file, by role ------------------------------------------


@pytest.mark.parametrize("missing", ["master", "spans"])
def test_a_missing_file_on_an_exact_shadow_skips_and_says_so(
    tmp_path, lake, target, capsys, missing
):
    gone = master_path(lake) if missing == "master" else spans_path(lake)
    gone.unlink()
    # QQQ_ONLY would be refused if the check ran, so writing it says the check skipped.
    assert _apply(QQQ_ONLY, tmp_path, lake, target, role="shadow") == REPLACED
    captured = capsys.readouterr()
    assert captured.out == (
        f"roster: lake check skipped, because {gone} does not exist "
        "and this host's role is shadow\n"
    )
    assert captured.err == ""


@pytest.mark.parametrize("role", [None, "primary"], ids=["absent-role", "primary"])
def test_a_missing_master_on_a_primary_is_refused(tmp_path, lake, target, role):
    master_path(lake).unlink()
    with pytest.raises(RosterError, match="security_master.parquet does not exist"):
        _apply(SPY_ONLY, tmp_path, lake, target, role=role)
    assert not target.parent.exists()


def test_a_missing_master_with_an_empty_role_is_refused(tmp_path, lake, target, capsys):
    # ``role_of`` reads an empty ``role:`` as shadow with a warning. A skip keyed on the
    # fallen-back role would let this host take a roster with no lake behind it.
    master_path(lake).unlink()
    with pytest.raises(RosterError, match="role is not exactly shadow"):
        _apply(SPY_ONLY, tmp_path, lake, target, role="")
    (warning,) = capsys.readouterr().err.splitlines()
    assert warning.startswith("roster: role ")
    assert "is neither 'primary' nor 'shadow'" in warning
    assert not target.parent.exists()


def test_a_role_warning_prints_when_the_check_runs(tmp_path, lake, target, capsys):
    # Both files are present, so the warning cannot ride on the missing-file branch.
    assert _apply(SPY_ONLY, tmp_path, lake, target, role="") == REPLACED
    (warning,) = capsys.readouterr().err.splitlines()
    assert warning.startswith("roster: role ")
    assert "is neither 'primary' nor 'shadow'" in warning


def test_an_unmounted_lake_root_is_refused(tmp_path, target):
    # An empty mount point, or a lake never restored: nothing under ``lake_root`` at all.
    with pytest.raises(RosterError, match="does not exist"):
        _apply(SPY_ONLY, tmp_path, tmp_path / "empty", target)
    assert not target.parent.exists()


# -- test 5: a damaged file or a failure naming the spans --------------------------------


def _null_valid_from(root: Path) -> None:
    """Rewrite the master with every ``valid_from`` null, which reads cleanly."""
    table = SecurityMaster.read(master_path(root)).to_table()
    index = table.schema.get_field_index("valid_from")
    field = table.schema.field(index)
    table = table.set_column(index, field, pa.nulls(table.num_rows, type=field.type))
    pq.write_table(table, master_path(root))


def _raising_read(exc: BaseException):
    def read(cls, path):
        raise exc

    return classmethod(read)


ROLES = [pytest.param(None, id="primary"), pytest.param("shadow", id="shadow")]


@pytest.mark.parametrize("role", ROLES)
def test_a_torn_spans_file_is_refused(tmp_path, lake, target, role):
    spans_path(lake).write_bytes(b"not parquet at all")
    with pytest.raises(RosterError) as excinfo:
        _apply(SPY_ONLY, tmp_path, lake, target, role=role)
    assert str(excinfo.value).startswith(f"lake check refused: {spans_path(lake)} cannot be read")
    assert not target.parent.exists()


def test_a_torn_spans_file_beside_a_missing_master_on_an_exact_shadow_is_refused(
    tmp_path, lake, target
):
    # Both files are read before either is judged, so the missing master cannot hide the
    # damaged spans file behind the shadow skip. QQQ_ONLY would be written by a skip.
    master_path(lake).unlink()
    spans_path(lake).write_bytes(b"not parquet at all")
    with pytest.raises(RosterError) as excinfo:
        _apply(QQQ_ONLY, tmp_path, lake, target, role="shadow")
    assert str(excinfo.value).startswith(f"lake check refused: {spans_path(lake)} cannot be read")
    assert not target.parent.exists()


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("file", ["master", "spans"])
def test_a_bare_oserror_from_a_read_is_refused_on_one_line(
    tmp_path, lake, target, monkeypatch, role, file
):
    # pyarrow reports most corruption as a bare ``OSError``, with newlines in the message.
    raised = OSError("Couldn't deserialize thrift\nDeserializing page header failed.\n")
    owner = SecurityMaster if file == "master" else CaptureSpans
    monkeypatch.setattr(owner, "read", _raising_read(raised))
    with pytest.raises(RosterError) as excinfo:
        _apply(SPY_ONLY, tmp_path, lake, target, role=role)
    message = str(excinfo.value)
    assert "cannot be read (OSError: Couldn't deserialize thrift Deserializing" in message
    assert "\n" not in message
    assert not target.parent.exists()


@pytest.mark.parametrize("file", ["master", "spans"])
def test_an_unsupported_schema_version_is_refused(tmp_path, lake, target, monkeypatch, file):
    owner = SecurityMaster if file == "master" else CaptureSpans
    raised = UnsupportedSchemaVersion(99) if file == "master" else UnsupportedSpansSchemaVersion(99)
    monkeypatch.setattr(owner, "read", _raising_read(raised))
    with pytest.raises(RosterError, match=r"cannot be read \(Unsupported"):
        _apply(SPY_ONLY, tmp_path, lake, target, role="shadow")
    assert not target.parent.exists()


def test_an_exception_with_no_message_prints_its_class_alone(tmp_path, lake, target, monkeypatch):
    monkeypatch.setattr(SecurityMaster, "read", _raising_read(OSError()))
    with pytest.raises(RosterError, match=r"cannot be read \(OSError\), so"):
        _apply(SPY_ONLY, tmp_path, lake, target)
    assert not target.parent.exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a chmod 000 file")
@pytest.mark.parametrize("role", ROLES)
def test_a_locked_master_is_refused(tmp_path, lake, target, role):
    with _Locked(master_path(lake)):
        with pytest.raises(RosterError, match=r"cannot be read \(PermissionError"):
            _apply(SPY_ONLY, tmp_path, lake, target, role=role)
    assert not target.parent.exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root searches a directory without its x bit")
def test_a_locked_reference_directory_on_an_exact_shadow_is_refused(tmp_path, lake, target):
    # ``os.path.exists`` reads a file under this directory as missing, so an existence
    # check before the read would skip here, and ``Path.exists`` raises outside the catch.
    reference = master_path(lake).parent
    reference.chmod(0o600)
    try:
        with pytest.raises(RosterError, match=r"cannot be read \(PermissionError"):
            _apply(QQQ_ONLY, tmp_path, lake, target, role="shadow")
    finally:
        reference.chmod(0o755)
    assert not target.parent.exists()


@pytest.mark.parametrize("role", ROLES)
def test_a_lake_root_that_is_a_file_is_refused(tmp_path, target, role):
    root = tmp_path / "lake"
    root.write_bytes(b"")
    with pytest.raises(RosterError, match=r"cannot be read \(NotADirectoryError"):
        _apply(SPY_ONLY, tmp_path, root, target, role=role)
    assert not target.parent.exists()


@pytest.mark.parametrize("role", ROLES)
def test_a_null_valid_from_while_naming_the_spans_is_refused(tmp_path, lake, target, role):
    # The master reads cleanly, and the comparison inside ``symbol_at`` raises. A catch
    # narrowed to the readers' own classes would let this out as a traceback.
    _null_valid_from(lake)
    with pytest.raises(RosterError) as excinfo:
        _apply(SPY_ONLY, tmp_path, lake, target, role=role)
    message = str(excinfo.value)
    assert message.startswith(f"lake check refused: naming the open capture spans in {lake}")
    assert "(TypeError: " in message
    assert not target.parent.exists()


def test_any_exception_naming_a_span_is_refused(tmp_path, lake, target, monkeypatch):
    def symbol_at(self, instrument_id, on, id_type="ticker"):
        raise ValueError("bad mapping")

    monkeypatch.setattr(SecurityMaster, "symbol_at", symbol_at)
    with pytest.raises(RosterError, match=r"naming the open capture spans .*\(ValueError: bad"):
        _apply(SPY_ONLY, tmp_path, lake, target)
    assert not target.parent.exists()


# -- test 6: an open span the master cannot name ----------------------------------------


def test_an_open_span_the_master_cannot_name_warns_and_writes(tmp_path, target, capsys):
    lake = _build_lake(tmp_path / "lake", unnamed=True)
    assert _apply(SPY_ONLY, tmp_path, lake, target) == REPLACED
    assert target.read_bytes() == SPY_ONLY
    captured = capsys.readouterr()
    (warning,) = captured.err.splitlines()
    assert warning.startswith("roster: instrument 3 has an open capture span in ")
    assert "no ticker in the security master on 2026-09-21" in warning
    assert captured.out == f"roster: lake check passed, 2 open capture spans checked in {lake}\n"


def test_the_unnamed_warning_prints_the_span_options_flag(tmp_path, target, capsys):
    # The unnamed span is open without options, and SPY's with them.
    lake = _build_lake(tmp_path / "lake", unnamed=True)
    _apply(SPY_ONLY, tmp_path, lake, target)
    (warning,) = capsys.readouterr().err.splitlines()
    assert "(options false) and no ticker" in warning


def test_the_naming_date_is_the_market_date_not_the_utc_date(tmp_path, target, capsys):
    # EVENING is 2026-09-21 in New York and 2026-09-22 in UTC. The master names IWM from
    # 2026-09-22, so naming on the UTC date would ask for an IWM entry and refuse.
    lake = _build_lake(tmp_path / "lake", unnamed=True)
    assert _apply(SPY_ONLY, tmp_path, lake, target, at=EVENING) == REPLACED
    (warning,) = capsys.readouterr().err.splitlines()
    assert "no ticker in the security master on 2026-09-21" in warning


# -- the command line runs the check ----------------------------------------------------


def _run_main(monkeypatch, payload: bytes) -> None:
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8"))
    roster_cli.main(["apply"])


def _point_main_at(monkeypatch, tmp_path: Path, lake: Path, target: Path) -> None:
    monkeypatch.setenv("MARKETLAKE_CONFIG", str(write_config(tmp_path / "cfg", lake)))
    monkeypatch.setenv("MARKETLAKE_TICKERS", str(target))


@pytest.mark.parametrize("held", [SPY_ONLY, QQQ_ONLY], ids=["replace", "equal-bytes"])
def test_main_refuses_a_roster_the_lake_disagrees_with_in_one_line(
    tmp_path, lake, target, monkeypatch, capsys, held
):
    _held(target, held)
    _point_main_at(monkeypatch, tmp_path, lake, target)
    with pytest.raises(SystemExit) as excinfo:
        _run_main(monkeypatch, QQQ_ONLY)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    (line,) = captured.err.splitlines()
    assert line.startswith("roster: lake check refused, and this host's roster is left as it was")
    assert "SPY has an open capture span" in line
    assert captured.out == ""
    assert target.read_bytes() == held
    assert _listing(target.parent) == ["tickers.yaml"]


def test_main_prints_the_pass_line_before_the_outcome(tmp_path, lake, target, monkeypatch, capsys):
    _point_main_at(monkeypatch, tmp_path, lake, target)
    _run_main(monkeypatch, SPY_ONLY)
    assert capsys.readouterr().out == (
        f"roster: lake check passed, 1 open capture span checked in {lake}\n"
        f"roster: {REPLACED} {target}\n"
    )
    assert target.read_bytes() == SPY_ONLY
