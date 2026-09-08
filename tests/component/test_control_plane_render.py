"""The control-plane command line across one real boundary: the filesystem.

``render --out`` writes every plist and setup file into the directory and nothing
outside it. It refuses a system directory. The ``self-check``, ``sunday``, and
``pmset`` subcommands run against a throwaway config with every seam injected, so no
clock is read and nothing shells out.
"""

from __future__ import annotations

import json
import os
import plistlib
import re
import shlex
import subprocess
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
]

EXPECTED_FILES = {
    "com.marketlake.daemon.plist",
    "com.marketlake.dashboard.plist",
    "com.marketlake.self-check.plist",
    "com.marketlake.calendar-probe.plist",
    "com.marketlake.sunday.plist",
    cp.SUDOERS_FILE,
    cp.INSTALL_SCRIPT_FILE,
    cp.REINSTALL_SCRIPT_FILE,
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


def test_rendered_tmutil_line_excludes_the_whole_config_directory(tmp_path, capsys):
    # The directory, not the token file alone. config.yaml sits beside the token and
    # holds four secrets of its own, and a sticky exclusion on a hand-edited file dies
    # the first time an editor saves by writing a temp file and renaming over it.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    lines = [line for line in printed.splitlines() if line.startswith("tmutil addexclusion")]
    assert lines == ["tmutil addexclusion /Users/someone/.config/marketlake"]


def test_render_takes_no_token_argument():
    # The render path derives the token from --home alone, so no override can point
    # the daemon, the Sunday job, and the exclusion at different files.
    with pytest.raises(SystemExit) as excinfo:
        cp.main(["render", "--out", "/tmp/x", *RENDER_ARGS, "--token", "/elsewhere/token.json"])
    assert excinfo.value.code == 2


def test_the_three_consumers_name_one_file(tmp_path, capsys):
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
    assert "tmutil addexclusion /Users/someone/.config/marketlake" in capsys.readouterr().out
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
    assert "tmutil addexclusion /Users/someone/.config/marketlake" in printed
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


def test_the_install_text_quotes_every_path_that_needs_it(tmp_path, capsys):
    # These lines are pasted into a shell. Every operator-supplied path gets a space
    # here, not just one of them, because a guard that only exercises --home lets the
    # other four lose their quoting unnoticed.
    out = tmp_path / "out dir"
    spaced = {
        "/Users/someone": "/Users/some one",
        "/Users/someone/marketlake": "/Users/some one/mark et",
        "/opt/py/bin/python": "/opt/p y/bin/python",
        "/Users/someone/Library/Logs/marketlake": "/Users/some one/Lo gs",
    }
    args = [spaced.get(a, a) for a in RENDER_ARGS]
    cp.main(["render", "--out", str(out), *args])
    printed = capsys.readouterr().out
    wanted = {*spaced.values(), str(out)}
    for line in printed.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # Each `&&`-joined command is its own argv. A spaced path must come back as one
        # token in it, never split across two.
        tokens = [t for part in line.split("&&") for t in shlex.split(part)]
        for path in wanted:
            if path in line:
                assert any(token == path or token.startswith(path + "/") for token in tokens), (
                    f"{path!r} is not one token in {line!r}"
                )


@pytest.mark.parametrize("flag", ["--python", "--home", "--project-dir", "--log-dir"])
def test_render_refuses_a_relative_machine_path(flag, capsys):
    # These land in a plist or in a printed line the operator pastes from anywhere, so
    # a relative value is never right. --out is the exception: it is resolved instead.
    args = list(RENDER_ARGS)
    args[args.index(flag) + 1] = "relative/path"
    code = cp.main(["render", "--out", "/tmp/unused-render", *args])
    assert code == 2
    assert "must be absolute" in capsys.readouterr().err


def test_the_install_text_names_absolute_paths_from_a_relative_out(tmp_path, capsys, monkeypatch):
    # An operator pastes these lines from any directory, not only the one the render
    # ran in, so a relative --out must not survive into them.
    monkeypatch.chdir(tmp_path)
    cp.main(["render", "--out", "rel-out", *RENDER_ARGS])
    printed = capsys.readouterr().out
    for line in printed.splitlines():
        assert "rel-out/" not in line or line.startswith(str(tmp_path)) or "/rel-out/" in line


def test_sudoers_refuses_the_reserved_word_all_as_the_owner():
    # ALL matches the account pattern and visudo accepts it, but sudoers reads it as the
    # reserved word for every account, so the drop-in would grant both pmset writes to
    # every local user.
    with pytest.raises(ValueError):
        cp.sudoers_dropin("ALL")
    # Case-sensitive: a real account named "all" is still an account.
    assert "all ALL=(root)" in cp.sudoers_dropin("all")


def test_the_install_text_pins_every_command_line_in_order(tmp_path, capsys):
    # Every runnable line, in order. The comments around them stay free to move. This
    # script is pasted by hand on a machine with no other guard, so the root ownership,
    # the 440 the sudoers drop-in needs, the visudo gate ahead of it, and all four
    # bootstrap labels are pinned rather than sampled.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    script = capsys.readouterr().out
    commands = [line for line in script.splitlines() if line and not line.startswith("#")]
    resolved = out.resolve()
    labels = [
        "com.marketlake.daemon",
        "com.marketlake.dashboard",
        "com.marketlake.self-check",
        "com.marketlake.calendar-probe",
        "com.marketlake.sunday",
    ]
    sudoers = resolved / cp.SUDOERS_FILE
    assert commands == [
        *(
            f"sudo install -o root -g wheel -m 644 {resolved / label}.plist /Library/LaunchDaemons/"
            for label in labels
        ),
        f"sudo visudo -cf {sudoers} && "
        f"sudo install -o root -g wheel -m 440 {sudoers} /etc/sudoers.d/marketlake",
        "sudo -l | grep pmset",
        "sudo pmset repeat wakeorpoweron MTWRF 08:25:00",
        "pmset -g sched",
        "tmutil addexclusion /Users/someone/.config/marketlake",
        "tmutil isexcluded /Users/someone/.config/marketlake",
        *(
            f"sudo launchctl bootstrap system /Library/LaunchDaemons/{label}.plist"
            for label in labels
        ),
        "launchctl print system/com.marketlake.daemon",
        "cd /Users/someone/marketlake && /opt/py/bin/python -m lake.control_plane pmset",
    ]


def test_the_install_text_no_longer_defers_the_dashboard(tmp_path, capsys):
    # D15 shipped `lake.dashboard`, so the warning to skip its bootstrap is spent. A
    # stale caution is worse than none: it tells an operator to leave a panel unloaded.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    assert "D15" not in printed
    assert "com.marketlake.dashboard.plist" in printed


def test_the_install_text_names_the_reload_and_leaves_it_commented(tmp_path, capsys):
    # Overwriting a plist does not reload it, and the operator has no way to know that
    # from a text that only covers the first install. The bootout lines stay commented,
    # because booting out a label that was never loaded fails.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    assert "Re-installing." in printed
    bootouts = [line for line in printed.splitlines() if "launchctl bootout" in line]
    assert len(bootouts) == 5
    assert all(line.startswith("# ") for line in bootouts)


def test_nothing_rendered_mentions_the_rejected_sleep_override(tmp_path):
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    for path in out.iterdir():
        assert "disablesleep" not in path.read_text()


# The default APFS volume is case-insensitive, so the upper-case spelling names the
# same directory and must be refused the same way.
@pytest.mark.parametrize(
    "target",
    ["/Library/LaunchDaemons", "/LIBRARY/LaunchDaemons", "/etc/sudoers.d", "/private/etc"],
)
def test_render_refuses_a_system_directory(target, capsys):
    code = cp.main(["render", "--out", target, *RENDER_ARGS])
    assert code == 2
    assert "refusing" in capsys.readouterr().err


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


def test_pmset_cli_skips_a_sunday_wake_that_already_fired(capsys):
    # On a Sunday evening after 19:55 this week's wake has already fired. Scheduling a
    # moment already past sets nothing, so the answer is the following Sunday.
    code = cp.main(
        ["pmset"],
        clock=ManualClock(start=et(2026, 8, 30, 21, 0)),
        calendar=weekday_sessions(date(2026, 8, 31), date(2026, 9, 7)),
    )
    assert code == 0
    assert capsys.readouterr().out.splitlines() == [
        "pmset repeat wakeorpoweron MTWRF 08:25:00",
        'pmset schedule wakeorpoweron "09/06/26 19:55:00"',
    ]


def test_the_install_text_counts_the_jobs_it_actually_installs(tmp_path, capsys):
    # The count sat at four for a release after the probe became the fifth. An operator
    # reads this line to know when the step is done.
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    installs = [line for line in printed.splitlines() if line.startswith("sudo install -o root")]
    plists = [line for line in installs if line.endswith("/Library/LaunchDaemons/")]
    assert len(plists) == len(list(out.glob("*.plist")))
    assert f"Install the {_spelled(len(plists))} LaunchDaemons" in printed


def _spelled(count: int) -> str:
    return {4: "four", 5: "five", 6: "six"}[count]


def test_render_puts_progress_on_stderr_so_stdout_is_pasteable(tmp_path, capsys):
    # The install text is read and pasted line by line, so stdout must carry nothing
    # but comments and commands. A ``wrote ...`` progress line on stdout lands at the
    # top of ``render ... > install.txt`` and a shell fed that file reports it as a
    # command not found.
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    captured = capsys.readouterr()

    assert "wrote " not in captured.out
    # Assert against the stream itself. Filtering blank lines out first would let a
    # leading blank through, and that separator is the other half of what moved.
    assert captured.out.strip(), "stdout carried no install text"
    assert captured.out.startswith("#"), f"stdout opens with {captured.out[:60]!r}"

    # Every written file is still reported, just on the other stream.
    for name in EXPECTED_FILES:
        assert f"wrote {out / name}" in captured.err


def test_render_reports_a_bad_path_on_stderr_and_prints_no_install_text(capsys):
    # The failure path shares the defect. A caller redirecting stdout to a file used to
    # get the reason written into the file rather than onto the terminal, so the screen
    # stayed silent and the file held one line that is not a command.
    args = list(RENDER_ARGS)
    args[args.index("--home") + 1] = "relative/path"
    assert cp.main(["render", "--out", "/tmp/unused-render-stderr", *args]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "render: these must be absolute paths" in captured.err


# -- the golden rendering ------------------------------------------------------

GOLDEN_DIR = Path(__file__).parent / "golden" / "render"

# The install text names the output directory, which is a fresh tmp_path on every run.
# Nothing else in the rendering varies, so that one path is normalised and every other
# byte is compared exactly.
OUT_PLACEHOLDER = "<OUT>"


def _normalise(text: str, out: Path) -> str:
    """The rendered text with the run's output directory replaced by a fixed token."""
    return text.replace(str(out.resolve()), OUT_PLACEHOLDER).replace(str(out), OUT_PLACEHOLDER)


def _check_golden(name: str, actual: bytes) -> None:
    """Compare one rendering to its golden file, or rewrite it when asked.

    The comparison is on bytes. ``read_text`` would open with universal newlines and
    fold a ``\\r\\n`` or a lone ``\\r`` to ``\\n`` on both sides, so a change to the line
    terminators would pass. That is not academic for these files. A lone-CR
    ``marketlake.sudoers`` holds no newline at all, so the whole drop-in reads as one
    comment, grants nothing, and still leaves ``visudo -cf`` saying parsed OK.

    Set ``MARKETLAKE_UPDATE_GOLDEN=1`` to rewrite. That is the only way these files
    change, so a diff in a pull request is the reviewable record of a rendering change.
    """
    path = GOLDEN_DIR / name
    if os.environ.get("MARKETLAKE_UPDATE_GOLDEN") == "1":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(actual)
        return
    assert path.exists(), (
        f"no golden for {name}. Run MARKETLAKE_UPDATE_GOLDEN=1 pytest {__file__} to write it."
    )
    assert actual == path.read_bytes(), (
        f"{name} no longer renders as its golden file. If the change is intended, run "
        f"MARKETLAKE_UPDATE_GOLDEN=1 pytest {__file__} and review the diff."
    )


@pytest.mark.parametrize("name", sorted(EXPECTED_FILES))
def test_each_rendered_file_matches_its_golden(name, tmp_path):
    """Every rendered byte is pinned, not only the fields other tests sample.

    The tests above assert on chosen lines: the sudoers rules, two jobs' program
    arguments, the install commands in order. A change to a plist key none of them
    names, such as a log path or a throttle, would land unreviewed. This compares the
    whole file, so any change to the rendering shows up as a diff.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    _check_golden(name, (out / name).read_bytes())


def test_the_install_text_matches_its_golden(tmp_path, capsys):
    """The pasted script is pinned whole, with only the output directory normalised."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    _check_golden("INSTALL.txt", _normalise(capsys.readouterr().out, out).encode())


def test_the_golden_directory_holds_exactly_what_is_pinned():
    """A golden for a file the renderer no longer writes would sit unread forever.

    The per-file test is parametrised over ``EXPECTED_FILES``, so it only ever asks
    about files the renderer still produces. Nothing looks the other way, and a stale
    golden reads in review as coverage that is not there.
    """
    assert {p.name for p in GOLDEN_DIR.iterdir()} == EXPECTED_FILES | {"INSTALL.txt"}


# -- the install script --------------------------------------------------------


def _host() -> cp.LaunchdHost:
    """A host carrying the same placeholder identity as ``RENDER_ARGS``."""
    pairs = dict(zip(RENDER_ARGS[::2], RENDER_ARGS[1::2], strict=True))
    return cp.LaunchdHost(
        python=pairs["--python"],
        owner=pairs["--owner"],
        home=pairs["--home"],
        project_dir=pairs["--project-dir"],
        log_dir=pairs["--log-dir"],
    )


# Every privileged command the script runs is shadowed by a fake on PATH, so a test
# exercises the script's control flow without touching the machine.
_FAKE = """#!/bin/bash
printf '%s %s\\n' "$(basename "$0")" "$*" >> "$LOG"
{body}
"""


def _fake_tools(bin_dir: Path, *, visudo_fails: bool) -> None:
    """Put stand-ins for every command the script calls on PATH."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    fail = 'if [[ "$*" == *"visudo -cf"* ]]; then exit 1; fi\nexit 0'
    for name in ("sudo", "pmset", "tmutil", "launchctl", "grep"):
        body = "exit 0"
        if name == "sudo":
            body = fail if visudo_fails else "exit 0"
        (bin_dir / name).write_text(_FAKE.format(body=body))
        (bin_dir / name).chmod(0o755)


def _run_script(tmp_path: Path, *, visudo_fails: bool):
    """Render, then run install.sh against the fakes. Returns (returncode, log lines)."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    bin_dir = tmp_path / "bin"
    log = tmp_path / "log"
    log.write_text("")
    _fake_tools(bin_dir, visudo_fails=visudo_fails)
    # Invoked directly rather than through ``bash <path>``, so the executable bit is
    # part of what this exercises.
    proc = subprocess.run(
        [str(out / cp.INSTALL_SCRIPT_FILE)],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "LOG": str(log)},
        capture_output=True,
        text=True,
    )
    return proc, [line for line in log.read_text().splitlines() if line]


def test_the_written_install_script_is_executable(tmp_path):
    """The bit is read off disk, because that is where it has to be.

    Asserting ``RenderedFile.mode`` instead would pass with the ``chmod`` deleted, and
    the golden cannot cover it either: git stores that fixture at 0o644. Being one
    command the operator runs is the whole point of the script, so ``./install.sh``
    has to work.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = out / cp.INSTALL_SCRIPT_FILE
    assert script.stat().st_mode & 0o777 == 0o755
    # The plists are not executable. A blanket chmod would pass the line above.
    assert (out / cp.SUDOERS_FILE).stat().st_mode & 0o777 == 0o644


def test_the_install_script_runs_every_step_in_order(tmp_path):
    """The happy path reaches the read-back, which is the third obligation D14 names."""
    proc, log = _run_script(tmp_path, visudo_fails=False)
    assert proc.returncode == 0, proc.stderr
    installs = [line for line in log if line.startswith("sudo install")]
    assert len(installs) == 6, log  # five plists plus the sudoers drop-in
    assert sum(1 for line in log if "launchctl bootstrap" in line) == 5, log
    assert log[-1].startswith("launchctl print system/com.marketlake.daemon"), log[-1]


def test_a_rejected_sudoers_file_stops_before_it_is_installed(tmp_path):
    """The first obligation D14 names, exercised rather than asserted from the text.

    A paste keeps going after a failed line. This script must not, because installing a
    drop-in that visudo just rejected is how sudo stops parsing the file at all.
    """
    proc, log = _run_script(tmp_path, visudo_fails=True)
    assert proc.returncode != 0
    assert any("visudo -cf" in line for line in log), log
    assert not any("/etc/sudoers.d/marketlake" in line for line in log), log
    assert not any("launchctl bootstrap" in line for line in log), log


def test_the_install_script_echoes_every_command_before_running_it(tmp_path):
    """The second obligation D14 names: the transcript shows what ran as root."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    lines = (out / cp.INSTALL_SCRIPT_FILE).read_text().splitlines()
    body = [line for line in lines if line and not line.startswith("#")]
    commands = [line for line in body if not line.startswith(("echo ", "set ", "HERE="))]
    for command in commands:
        echo = f"echo {shlex.quote('+ ' + command)}"
        assert echo in lines, command
        # Immediately before, not merely present. An echo that trails its command
        # describes what already ran as root, which is not what the obligation buys.
        assert lines[lines.index(echo) + 1] == command, command


def test_the_install_script_omits_the_standing_friday_step(tmp_path):
    """Step 6 is not part of the first install, so it must not run with it."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = (out / cp.INSTALL_SCRIPT_FILE).read_text()
    assert "lake.control_plane pmset" not in script
    assert "# 6." not in script


def test_the_install_script_header_counts_match_its_body(tmp_path):
    """The header's usage note counts what the script runs, rather than saying a number.

    A written-down count goes stale the first time a step is added. These are derived
    from the body, so adding a command moves them.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    lines = (out / cp.INSTALL_SCRIPT_FILE).read_text().splitlines()

    commands = [
        line for line in lines if line and not line.startswith(("#", "echo ", "set ", "HERE="))
    ]
    claimed = re.search(r"Of the (\d+) commands below, (\d+) run under sudo", "\n".join(lines))
    assert claimed, "the header states no counts"
    assert int(claimed.group(1)) == len(commands)
    assert int(claimed.group(2)) == sum(1 for line in commands if line.startswith("sudo "))


def test_the_install_script_header_shows_how_to_run_it(tmp_path):
    """The file says how to invoke itself, since that is the first thing a reader needs."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    header = (out / cp.INSTALL_SCRIPT_FILE).read_text().split("set -euo pipefail")[0]
    assert f"./{cp.INSTALL_SCRIPT_FILE}" in header
    assert "Usage" in header


# -- the reinstall script ------------------------------------------------------

# sudo must run what it is given, or the command under it is never exercised.
_FAKE_SUDO = """#!/bin/bash
printf 'sudo %s\\n' "$*" >> "$LOG"
exec "$@"
"""

# launchctl tracks which labels are loaded in a directory, so `print` answers truthfully
# before and after a bootstrap rather than returning a fixed code.
_FAKE_LAUNCHCTL = """#!/bin/bash
printf 'launchctl %s\\n' "$*" >> "$LOG"
# print and bootout name the label in $2, bootstrap names a plist path in $3.
case "$1" in
  print|bootout) label="${2##*/}" ;;
  bootstrap)     label="${3##*/}"; label="${label%.plist}" ;;
esac
case "$1" in
  print)     [[ -e "$LOADED/$label" ]] && exit 0 || exit 1 ;;
  bootout)   [[ "$BOOTOUT_RC" != "0" ]] && exit "$BOOTOUT_RC"; rm -f "$LOADED/$label"; exit 0 ;;
  bootstrap) touch "$LOADED/$label"; exit 0 ;;
  *)         exit 0 ;;
esac
"""


def _run_reinstall(tmp_path: Path, *, loaded: bool, bootout_rc: int = 0):
    """Render, then run reinstall.sh against fakes. Returns (proc, log lines)."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    log = tmp_path / "log"
    log.write_text("")
    loaded_dir = tmp_path / "loaded"
    loaded_dir.mkdir(exist_ok=True)
    if loaded:
        for job in cp.all_jobs(_host()):
            (loaded_dir / job.label).touch()
    (bin_dir / "install").write_text(_FAKE.format(body="exit 0"))
    (bin_dir / "install").chmod(0o755)
    (bin_dir / "sudo").write_text(_FAKE_SUDO)
    (bin_dir / "sudo").chmod(0o755)
    (bin_dir / "launchctl").write_text(_FAKE_LAUNCHCTL)
    (bin_dir / "launchctl").chmod(0o755)
    proc = subprocess.run(
        [str(out / cp.REINSTALL_SCRIPT_FILE)],
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "LOG": str(log),
            "LOADED": str(loaded_dir),
            "BOOTOUT_RC": str(bootout_rc),
        },
        capture_output=True,
        text=True,
    )
    return proc, [line for line in log.read_text().splitlines() if line]


def test_reinstall_boots_each_label_out_before_replacing_it(tmp_path):
    """Overwriting a plist alone changes nothing, so the bootout is the load-bearing step."""
    proc, log = _run_reinstall(tmp_path, loaded=True)
    assert proc.returncode == 0, proc.stderr
    boots = [line for line in log if line.startswith("launchctl bootout")]
    assert len(boots) == 5, log
    # For each label the order is bootout, then install, then bootstrap.
    for label in (job.label for job in cp.all_jobs(_host())):
        acts = [
            line.split("launchctl ")[1].split()[0]
            for line in log
            if line.startswith("launchctl ") and label in line
        ]
        assert acts == ["print", "bootout", "bootstrap"] or acts[:3] == [
            "print",
            "bootout",
            "bootstrap",
        ], (label, acts)


def test_reinstall_skips_the_bootout_when_nothing_is_loaded(tmp_path):
    """A fresh machine must converge too, so a missing label is skipped, not fatal.

    The install text keeps its bootout lines commented for exactly this reason. Guarding
    on ``launchctl print`` is what lets the script carry them uncommented.
    """
    proc, log = _run_reinstall(tmp_path, loaded=False)
    assert proc.returncode == 0, proc.stderr
    assert not [line for line in log if line.startswith("launchctl bootout")], log
    assert sum(1 for line in log if line.startswith("launchctl bootstrap")) == 5, log


def test_a_bootout_that_refuses_stops_the_reinstall(tmp_path):
    """The guard must not swallow a real refusal, which is why it is not ``|| true``.

    A job that will not stop is the one bootout failure worth halting for. Replacing its
    plist underneath a running definition is how the two drift apart.
    """
    proc, log = _run_reinstall(tmp_path, loaded=True, bootout_rc=1)
    assert proc.returncode != 0
    assert not [line for line in log if line.startswith("launchctl bootstrap")], log


def test_reinstall_runs_none_of_the_write_once_steps(tmp_path):
    """It re-installs the launchd jobs and runs nothing from steps 2, 3 or 4.

    Scoped to command lines rather than the whole file. The header has to name those
    steps, because skipping them is a limit the operator needs told about, and a check
    that banned the words would forbid saying so.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    lines = (out / cp.REINSTALL_SCRIPT_FILE).read_text().splitlines()
    commands = [
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith(("#", "echo ", "set ", "HERE="))
    ]
    for absent in ("visudo", "pmset", "tmutil"):
        assert not [c for c in commands if c.startswith(absent) or f" {absent} " in c], absent
    assert not [c for c in commands if "/etc/sudoers.d" in c]


def test_reinstall_copies_each_plist_between_the_bootout_and_the_bootstrap(tmp_path):
    """The copy is the step this script exists to perform, so it is asserted by running it.

    Without this, deleting the copy, reordering it after the bootstrap, or retargeting it
    to another directory all pass every behavioural test, leaving only the byte golden to
    object. A golden whose failure message says to regenerate it is a weak last line.
    """
    proc, log = _run_reinstall(tmp_path, loaded=True)
    assert proc.returncode == 0, proc.stderr
    copies = [line for line in log if line.startswith("install -o root -g wheel -m 644")]
    assert len(copies) == 5, log
    assert all("/Library/LaunchDaemons/" in line for line in copies), copies
    for label in (job.label for job in cp.all_jobs(_host())):
        acts = [line for line in log if label in line and not line.startswith("sudo ")]
        kinds = [
            "bootout"
            if "bootout" in a
            else "copy"
            if a.startswith("install ")
            else "bootstrap"
            if "bootstrap" in a
            else "print"
            for a in acts
        ]
        assert kinds[:4] == ["print", "bootout", "copy", "bootstrap"], (label, kinds)


def test_reinstall_ends_on_the_read_back(tmp_path):
    """Same last obligation as the install: the operator reads whether the daemon is up."""
    proc, log = _run_reinstall(tmp_path, loaded=True)
    assert proc.returncode == 0
    assert log[-1] == f"launchctl print {cp.LAUNCHD_DOMAIN}/{cp.DAEMON_LABEL}", log[-1]


def test_the_written_reinstall_script_is_executable(tmp_path):
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert (out / cp.REINSTALL_SCRIPT_FILE).stat().st_mode & 0o777 == 0o755
