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
import urllib.error
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import pytest

from lake import control_plane as cp
from lake.schwab import DEFAULT_TOKEN_PATH
from tests.support.calendar import et, weekday_sessions
from tests.support.clock import ManualClock
from tests.support.config import write_config
from tests.support.lake import FixtureLake
from tests.support.pinger import FakePinger

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


def test_rendered_tmutil_line_excludes_the_whole_config_directory(tmp_path):
    # The directory, not the token file alone. config.yaml sits beside the token and
    # holds four secrets of its own, and a sticky exclusion on a hand-edited file dies
    # the first time an editor saves by writing a temp file and renaming over it.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    lines = [
        line
        for line in (out / cp.TMUTIL_FILE).read_text().splitlines()
        if line.startswith("tmutil")
    ]
    assert lines == ['tmutil addexclusion "/Users/someone/.config/marketlake"']


def test_render_takes_no_token_argument():
    # The render path derives the token from --home alone, so no override can point
    # the daemon, the Sunday job, and the exclusion at different files.
    with pytest.raises(SystemExit) as excinfo:
        cp.main(["render", "--out", "/tmp/x", *RENDER_ARGS, "--token", "/elsewhere/token.json"])
    assert excinfo.value.code == 2


def test_the_three_consumers_name_one_file(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    token = "/Users/someone/.config/marketlake/token.json"
    # The Sunday job reads it.
    sunday = plistlib.loads((out / "com.marketlake.sunday.plist").read_bytes())
    assert sunday["ProgramArguments"][-2:] == ["--token", token]
    # The daemon writes it. It carries no --token, so it resolves the path from the
    # HOME its plist sets, through schwab's own spelling of the same rule. Binding the
    # two spellings is the daemon leg of the invariant. Asserting control_plane's
    # helper against itself would pass while the daemon read another file entirely.
    daemon = plistlib.loads((out / "com.marketlake.daemon.plist").read_bytes())
    assert daemon["EnvironmentVariables"]["HOME"] == "/Users/someone"
    assert "--token" not in daemon["ProgramArguments"]
    assert str(DEFAULT_TOKEN_PATH) == cp.default_token_path(str(Path.home()))
    assert cp.default_token_path(daemon["EnvironmentVariables"]["HOME"]) == token
    # The exclusion protects the directory holding it.
    excluded = (out / cp.TMUTIL_FILE).read_text()
    assert 'tmutil addexclusion "/Users/someone/.config/marketlake"' in excluded
    assert token.startswith(cp.default_config_dir("/Users/someone") + "/")


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        # Inside the directory: the directory line covers it.
        ("/Users/someone/.config/marketlake/token.json", ("/Users/someone/.config/marketlake",)),
        ("/Users/someone/.config/marketlake/other.json", ("/Users/someone/.config/marketlake",)),
        # Outside it: a brokerage credential is excluded wherever it is put. The
        # render path cannot produce this, but `sunday --token` still can.
        (
            "/elsewhere/token.json",
            ("/Users/someone/.config/marketlake", "/elsewhere/token.json"),
        ),
    ],
)
def test_exclusion_targets_cover_a_token_wherever_it_sits(token, expected):
    assert cp.tmutil_exclusion_targets("/Users/someone/.config/marketlake", token) == expected


def test_the_install_text_excludes_the_directory_and_reads_it_back(tmp_path, capsys):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    assert 'tmutil addexclusion "/Users/someone/.config/marketlake"' in printed
    # visudo has its read-back and pmset has its own. So does this.
    assert "tmutil isexcluded /Users/someone/.config/marketlake" in printed


def test_the_install_text_says_who_sets_the_sunday_one_shot_until_slice_3(tmp_path, capsys):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    # Nothing in this deliverable sets the one-shot, and the Sunday read-back cannot
    # catch a week that missed it. So the install text has to hand the operator the
    # by-hand step rather than leave the gap to be discovered on a Monday.
    # The interpreter and the working directory come from the render, like every other
    # line. A bare `python` is not on a stock Mac, and this is the one step whose whole
    # job is to be pasted and run.
    assert (
        "cd /Users/someone/marketlake && /opt/py/bin/python -m lake.control_plane pmset" in printed
    )
    assert "each Friday" in printed
    assert "# prints under sudo." in printed


def test_the_install_text_numbers_its_steps_in_order(tmp_path, capsys):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    steps = re.findall(r"^# (\d+)\.", printed, flags=re.MULTILINE)
    assert steps == ["1", "2", "3", "4", "5", "6"]


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


def test_self_check_cli_pings_the_pre_open_slug_when_the_daemon_is_up(tmp_path, capsys):
    config = write_config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    code = cp.main(["self-check", "--config", str(config)], probe=lambda label: True, pinger=pinger)
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/pre-open"]
    printed = capsys.readouterr().out
    assert "slug=pre-open" in printed
    assert "secret-key" not in printed


def test_self_check_cli_names_a_failed_ping_and_still_reports(tmp_path, capsys):
    # The ping is the last step, so a raise there used to replace the summary line with
    # a traceback in the job's err log. The line is what the operator reads.
    config = write_config(tmp_path, tmp_path / "lake")

    class Boom:
        def ping(self, url: str) -> None:
            raise urllib.error.URLError(OSError("connection refused"))

    code = cp.main(["self-check", "--config", str(config)], probe=lambda label: True, pinger=Boom())
    assert code == 1
    printed = capsys.readouterr().out
    assert "self-check: ping failed: URLError" in printed
    assert "daemon up pinged=False slug=pre-open" in printed
    assert "secret-key" not in printed


def test_self_check_cli_exits_non_zero_without_pinging_when_down(tmp_path):
    config = write_config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    code = cp.main(
        ["self-check", "--config", str(config)], probe=lambda label: False, pinger=pinger
    )
    assert code == 1
    assert pinger.urls == []


def _token(tmp_path: Path, minted: datetime | None = None) -> Path:
    """A token.json in schwab-py's shape: an epoch second beside the secret half."""
    path = tmp_path / "token.json"
    when = et(2026, 8, 30, 19, 30) if minted is None else minted
    path.write_text(
        json.dumps({"creation_timestamp": when.timestamp(), "token": {"x": "never-read"}})
    )
    return path


def test_sunday_cli_scrubs_the_configured_lake_and_pings(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    pinger = FakePinger()
    # An explicit --token keeps the test off the real token under HOME.
    code = cp.main(
        [
            "sunday",
            "--config",
            str(config),
            "--token",
            str(_token(tmp_path)),
        ],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    printed = capsys.readouterr().out
    assert "secret-key" not in printed


def test_sunday_cli_withholds_the_ping_for_a_stale_token(tmp_path, capsys):
    # Minted late the prior week: still valid on Sunday, dead before Friday's option
    # close. Validity is not freshness, and the command line has to act on that, not
    # just the decision functions that already pin it. This is the case the deleted
    # `--mint` override could hide, by supplying a mint the token does not carry.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    pinger = FakePinger()
    code = cp.main(
        [
            "sunday",
            "--config",
            str(config),
            "--token",
            str(_token(tmp_path, et(2026, 8, 27, 18, 0))),
        ],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
    )
    assert code == 1
    assert pinger.urls == []
    printed = capsys.readouterr().out
    assert "does not clear the coming week" in printed
    assert "secret-key" not in printed


def test_sunday_cli_reads_the_mint_time_from_the_token_file(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    token = _token(tmp_path)
    pinger = FakePinger()
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    printed = capsys.readouterr().out
    assert "never-read" not in printed and "secret-key" not in printed


def test_sunday_cli_checks_the_time_machine_exclusion(tmp_path, capsys):
    # Production must always run the check, so the CLI passes the reader and the
    # standard targets. A lost exclusion rides the report and the ping still fires.
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    pinger = FakePinger()
    asked: list[tuple[str, ...]] = []

    def reader(paths):
        asked.append(tuple(paths))
        return "".join(f"[Included]\t{p}\n" for p in paths)

    code = cp.main(
        [
            "sunday",
            "--config",
            str(config),
            "--token",
            str(_token(tmp_path)),
        ],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
        schedule_reader=lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
        pinger=pinger,
        exclusion_reader=reader,
    )
    assert code == 0  # report tier: the ping still fires
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    assert len(asked) == 1
    # The config directory under the running account, and the token beside it.
    assert asked[0][0].endswith("/.config/marketlake")
    printed = capsys.readouterr().out
    assert "sunday: report: not excluded from time machine" in printed
    assert "secret-key" not in printed


def test_sunday_cli_reports_problems_and_exits_non_zero(tmp_path, capsys):
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    pinger = FakePinger()
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(tmp_path / "absent.json")],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
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
    # Until D13's publisher lands, this print is the only way a reminder reaches a
    # human, so the log line is the delivery path and is pinned as one.
    assert "sunday: reminder: The throwaway call" in printed


def test_pmset_cli_prints_both_commands_for_the_coming_week(capsys):
    code = cp.main(
        ["pmset"],
        clock=ManualClock(start=et(2026, 8, 28, 18, 30)),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )
    assert code == 0
    assert capsys.readouterr().out.splitlines() == [
        "pmset repeat wakeorpoweron MTWRF 08:25:00",
        'pmset schedule wakeorpoweron "08/30/26 19:55:00"',
    ]
