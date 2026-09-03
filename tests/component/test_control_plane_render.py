"""The control-plane command line across one real boundary: the filesystem.

``render --out`` writes every plist and setup file into the directory and nothing
outside it. It refuses a system directory. The ``self-check``, ``sunday``, and
``pmset`` subcommands run against a throwaway config with every seam injected, so no
clock is read and nothing shells out.
"""

from __future__ import annotations

import json
import plistlib
import re
import shlex
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake.calendar import MARKET_TZ
from tests.support.calendar import FakeCalendar, SessionTimes
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake

RENDER_ARGS = [
    "--python",
    "/opt/py/bin/python",
    "--owner",
    "someone",
    "--home",
    "/Users/someone",
    "--project-dir",
    "/Users/someone/marketlake",
    "--log-dir",
    "/Users/someone/Library/Logs/marketlake",
    "--path-dir",
    "/Users/someone/.local/bin",
]

EXPECTED_FILES = {
    "com.marketlake.daemon.plist",
    "com.marketlake.dashboard.plist",
    "com.marketlake.self-check.plist",
    "com.marketlake.sunday.plist",
    cp.SUDOERS_FILE,
    cp.TMUTIL_FILE,
}


def _et(*args: int) -> datetime:
    return datetime(*args, tzinfo=MARKET_TZ)


class FakePinger:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def ping(self, url: str) -> None:
        self.urls.append(url)


# -- render ------------------------------------------------------------------------


def test_render_writes_every_file_into_the_directory_and_nothing_outside(tmp_path, capsys):
    out = tmp_path / "out"
    code = cp.main(["render", "--out", str(out), *RENDER_ARGS])
    assert code == 0
    assert {p.name for p in out.iterdir()} == EXPECTED_FILES
    # Nothing landed beside the output directory.
    assert [p.name for p in tmp_path.iterdir()] == ["out"]
    # Every plist parses and names the owner.
    for name in EXPECTED_FILES:
        if name.endswith(".plist"):
            plist = plistlib.loads((out / name).read_bytes())
            assert plist["UserName"] == "someone"
    printed = capsys.readouterr().out
    assert "sudo pmset repeat wakeorpoweron MTWRF 08:25:00" in printed
    assert "/Library/LaunchDaemons/" in printed
    assert "launchctl bootstrap system" in printed


def test_rendered_sudoers_grants_exactly_the_two_pmset_writes(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    text = (out / cp.SUDOERS_FILE).read_text()
    rules = [line for line in text.splitlines() if not line.startswith("#")]
    assert rules == [
        "someone ALL=(root) NOPASSWD: /usr/bin/pmset repeat wakeorpoweron MTWRF 08\\:25\\:00",
        "someone ALL=(root) NOPASSWD: /usr/bin/pmset ^schedule[[:space:]]wakeorpoweron"
        "[[:space:]][0-9][0-9]/[0-9][0-9]/[0-9][0-9][[:space:]]19:55:00$",
    ]
    assert "disablesleep" not in text


def test_neither_sudoers_rule_wildcards_its_argument(tmp_path):
    """A ``*`` would span whitespace, and both pmset writes read on past their event."""
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    rules = [
        line
        for line in (out / cp.SUDOERS_FILE).read_text().splitlines()
        if not line.startswith("#")
    ]
    assert len(rules) == 2
    for rule in rules:
        _, _, args = rule.partition("/usr/bin/pmset ")
        assert "*" not in args


# Python's ``re`` is not a POSIX engine, so the one bracket expression the drop-in
# uses is translated before matching. Every other construct in the rule means the same
# in both dialects.
_POSIX_CLASSES = {"[[:space:]]": "[ ]"}


def _sudo_args_match(spec: str, argv: Sequence[str]) -> bool:
    """Whether sudo would let ``argv`` run under the drop-in's argument ``spec``.

    ``sudo`` joins the arguments with single spaces and matches that one string. A
    spec framed by ``^`` and ``$`` is a POSIX extended regular expression. Anything
    else is compared literally, after the backslash a colon carries is dropped. The
    assertion guards the translation, so a POSIX class added later fails here rather
    than being read as a nested set.
    """
    joined = " ".join(argv)
    if not (spec.startswith("^") and spec.endswith("$")):
        return spec.replace("\\:", ":") == joined
    pattern = spec
    for posix, python in _POSIX_CLASSES.items():
        pattern = pattern.replace(posix, python)
    assert "[:" not in pattern, f"untranslated POSIX class in {spec!r}"
    return re.fullmatch(pattern[1:-1], joined) is not None


@pytest.mark.parametrize(
    ("argv", "permitted"),
    [
        # The exact commands the design pins. Both must run without a password.
        (["repeat", "wakeorpoweron", "MTWRF", "08:25:00"], True),
        (["schedule", "wakeorpoweron", "09/06/26 19:55:00"], True),
        (["schedule", "wakeorpoweron", "12/27/26 19:55:00"], True),
        # A second power-off event riding the repeat alarm. The wildcard admitted it.
        (
            ["repeat", "wakeorpoweron", "MTWRF", "08:25:00", "shutdown", "MTWRFSU", "03:00:00"],
            False,
        ),
        (["repeat", "wakeorpoweron", "MTWRF", "08:25:00", "sleep", "MTWRFSU", "20:00:00"], False),
        # A wake at another hour, on other days.
        (["repeat", "wakeorpoweron", "MTWRFSU", "03:00:00"], False),
        # The sleep-disabling write, trailing the one-shot. The wildcard admitted it.
        (["schedule", "wakeorpoweron", "09/06/26 19:55:00", "x", "disablesleep", "1"], False),
        (["schedule", "wakeorpoweron", "09/06/26 19:55:00", "disablesleep", "1"], False),
        # A one-shot at another time, of another kind, or in another date form.
        (["schedule", "wakeorpoweron", "09/06/26 03:00:00"], False),
        (["schedule", "sleep", "09/06/26 19:55:00"], False),
        (["schedule", "wakeorpoweron", "09/06/2026 19:55:00"], False),
    ],
)
def test_the_grant_admits_the_two_pinned_commands_and_nothing_further(argv, permitted):
    specs = [
        rule.partition("/usr/bin/pmset ")[2]
        for rule in cp.sudoers_dropin("someone").splitlines()
        if not rule.startswith("#")
    ]
    assert any(_sudo_args_match(spec, argv) for spec in specs) is permitted


def test_the_rendered_rules_cover_the_commands_the_module_composes(tmp_path):
    """The grant and the command builders must not drift apart."""
    specs = [
        rule.partition("/usr/bin/pmset ")[2]
        for rule in cp.sudoers_dropin("someone").splitlines()
        if not rule.startswith("#")
    ]
    repeat = shlex.split(cp.pmset_repeat_command())[1:]
    one_shot = shlex.split(cp.pmset_schedule_command(date(2026, 9, 6)))[1:]
    assert any(_sudo_args_match(spec, repeat) for spec in specs)
    assert any(_sudo_args_match(spec, one_shot) for spec in specs)


def test_rendered_tmutil_line_targets_the_token_under_home(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    text = (out / cp.TMUTIL_FILE).read_text()
    assert 'tmutil addexclusion "/Users/someone/.config/marketlake/token.json"' in text


def test_render_honours_an_explicit_token_path(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS, "--token", "/elsewhere/token.json"])
    assert 'addexclusion "/elsewhere/token.json"' in (out / cp.TMUTIL_FILE).read_text()


def test_nothing_rendered_mentions_the_rejected_sleep_override(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    for path in out.iterdir():
        assert "disablesleep" not in path.read_text()


@pytest.mark.parametrize("target", ["/Library/LaunchDaemons", "/etc/sudoers.d", "/private/etc"])
def test_render_refuses_a_system_directory(target, capsys):
    code = cp.main(["render", "--out", target, *RENDER_ARGS])
    assert code == 2
    assert "refusing" in capsys.readouterr().out


def test_write_rendered_refuses_a_system_directory_directly():
    with pytest.raises(ValueError):
        cp.write_rendered([cp.RenderedFile("x", "y")], Path("/Library/LaunchDaemons/nested"))


def test_sudoers_refuses_an_owner_that_is_not_an_account_name():
    with pytest.raises(ValueError):
        cp.sudoers_dropin("someone ALL=(ALL) NOPASSWD: ALL")


# -- the two jobs and pmset through the command line ----------------------------------


def _config(tmp_path: Path, lake_root: Path) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"lake_root: {lake_root}\n"
        f"backup_target: {tmp_path / 'ssd'}\n"
        "healthchecks_ping_key: secret-key\n"
        "ntfy_topic: secret-topic\n"
        "schwab_api_key: key\n"
        "schwab_app_secret: app-secret\n"
    )
    return path


def _week(monday: date) -> FakeCalendar:
    table = {}
    for offset in range(5):
        day = monday + timedelta(days=offset)
        table[day] = SessionTimes(
            open=_et(day.year, day.month, day.day, 9, 30),
            close=_et(day.year, day.month, day.day, 16, 0),
        )
    return FakeCalendar(table)


def test_self_check_cli_pings_the_pre_open_slug_when_the_daemon_is_up(tmp_path, capsys):
    config = _config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    code = cp.main(["self-check", "--config", str(config)], probe=lambda label: True, pinger=pinger)
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/pre-open"]
    printed = capsys.readouterr().out
    assert "slug=pre-open" in printed
    assert "secret-key" not in printed


def test_self_check_cli_exits_non_zero_without_pinging_when_down(tmp_path):
    config = _config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    code = cp.main(
        ["self-check", "--config", str(config)], probe=lambda label: False, pinger=pinger
    )
    assert code == 1
    assert pinger.urls == []


def test_sunday_cli_scrubs_the_configured_lake_and_pings(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = _config(tmp_path, lake)
    pinger = FakePinger()
    # An absent token file beside the override proves the override wins and that
    # the test can never fall through to the real token under HOME.
    code = cp.main(
        [
            "sunday",
            "--config",
            str(config),
            "--token",
            str(tmp_path / "absent.json"),
            "--mint",
            "2026-08-30T19:30:00-04:00",
        ],
        clock=ManualClock(start=_et(2026, 8, 30, 20, 0)),
        calendar=_week(date(2026, 8, 31)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    printed = capsys.readouterr().out
    assert "secret-key" not in printed
    assert "token file unreadable" not in printed


def test_sunday_cli_reads_the_mint_time_from_the_token_file(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = _config(tmp_path, lake)
    token = tmp_path / "token.json"
    # 2026-08-30 19:30 ET as an epoch second, the shape schwab-py writes.
    minted = _et(2026, 8, 30, 19, 30).timestamp()
    token.write_text(json.dumps({"creation_timestamp": minted, "token": {"x": "never-read"}}))
    pinger = FakePinger()
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=_et(2026, 8, 30, 20, 0)),
        calendar=_week(date(2026, 8, 31)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    printed = capsys.readouterr().out
    assert "never-read" not in printed and "secret-key" not in printed


def test_sunday_cli_reports_problems_and_exits_non_zero(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = _config(tmp_path, lake)
    pinger = FakePinger()
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(tmp_path / "absent.json")],
        clock=ManualClock(start=_et(2026, 8, 30, 20, 0)),
        calendar=_week(date(2026, 8, 31)),
        schedule_reader=lambda: "",
        pinger=pinger,
        canary=lambda: False,
    )
    assert code == 1
    assert pinger.urls == []
    printed = capsys.readouterr().out
    lines = printed.splitlines()
    assert any(
        line.startswith("sunday: report:") and "repeat alarm missing" in line for line in lines
    )
    assert "canary call failed" in printed
    assert "token file unreadable" in printed


def test_pmset_cli_prints_both_commands_for_the_coming_week(capsys):
    code = cp.main(
        ["pmset"],
        clock=ManualClock(start=_et(2026, 8, 28, 18, 30)),
        calendar=_week(date(2026, 8, 31)),
    )
    assert code == 0
    assert capsys.readouterr().out.splitlines() == [
        "pmset repeat wakeorpoweron MTWRF 08:25:00",
        'pmset schedule wakeorpoweron "08/30/26 19:55:00"',
    ]
