"""The control-plane command line across one real boundary: the filesystem.

``render --out`` writes every plist and setup file into the directory and nothing
outside it. It refuses a system directory. The ``self-check``, ``sunday``, and
``pmset`` subcommands run against a throwaway config with every seam injected, so no
clock is read and nothing shells out.

Two of the ``sunday`` seams reach the outside world in production. The canary quotes a
symbol through the real vendor, and the transport POSTs to ntfy. Every test here passes
its own for both, so nothing reaches the network and no push lands on a phone.
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
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from lake import control_plane as cp
from lake.metadata import stamp_assertion_pid, stamp_cycle
from lake.paths import TOKEN_FILE, config_dir
from lake.schwab import DEFAULT_TOKEN_PATH
from lake.tickers import Roster
from tests.support.backup import mirror_lake
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
    cp.UNINSTALL_SCRIPT_FILE,
    cp.RESTART_SCRIPT_FILE,
    cp.REAUTH_SCRIPT_FILE,
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
    # config_dir rather than this process's own home: DEFAULT_TOKEN_PATH honours
    # MARKETLAKE_CONFIG_DIR, and this leg is about schwab spelling the shared rule.
    # The renderer's leg of the same rule is asserted on the line below and in
    # test_paths.test_the_control_plane_renderer_agrees_with_the_shared_rule.
    assert str(DEFAULT_TOKEN_PATH) == str(config_dir() / TOKEN_FILE)
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


def _pasted_comment_lines(printed: str) -> list[str]:
    """Every comment line of the printed install text, at any indentation.

    zsh reads a line as a comment only when ``#`` is the first character it meets, not
    only when that character sits at column 0. So the sweep strips leading whitespace
    before checking for ``#``, and a comment nested under a numbered step is swept the
    same as one that starts the line.
    """
    return [line for line in printed.splitlines() if line.lstrip().startswith("#")]


def test_the_comment_sweep_covers_an_indented_comment_line():
    """Some of the printed comments carry an indented example rather than starting
    flush against the left margin. The sweep must not skip them: a character that
    changes meaning when pasted is just as dangerous there as at column 0.
    """
    indented = "    # an indented example carrying an apostrophe: it is here"
    assert _pasted_comment_lines(indented) == [indented]


def test_the_install_text_carries_no_apostrophe_in_a_comment(tmp_path, capsys):
    """zsh does not set INTERACTIVE_COMMENTS, so at an interactive prompt ``#`` does not
    start a comment. An apostrophe in a comment line therefore opens a quote, and the
    shell swallows every pasted line after it as part of that string until a later line
    happens to close it. ``sudo -l | grep pmset``, the read-back that proves the sudoers
    rules parsed as intended, is one of the lines an apostrophe above it used to
    swallow.

    The sweep reads the render command's own standard output, captured with ``capsys``,
    rather than the golden file. The golden's ``<OUT>`` placeholder is written by this
    module's own ``_normalise``, so checking the golden would pass or fail for the
    wrong reason.
    """
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    for line in _pasted_comment_lines(printed):
        assert "'" not in line, line


def test_the_install_text_carries_no_backtick_in_a_comment(tmp_path, capsys):
    """A paired backtick does not stall a paste the way an apostrophe does, so there is
    no ``quote>`` prompt to warn the operator that anything went wrong. The command
    inside the backticks runs instead, silently, which is worse than a stalled paste.
    """
    out = tmp_path / "out"
    cp.main(["render", "--out", str(out), *RENDER_ARGS])
    printed = capsys.readouterr().out
    for line in _pasted_comment_lines(printed):
        assert "`" not in line, line


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


def test_the_install_text_carries_every_command_line_in_order(tmp_path, capsys):
    # Every runnable line, in order. The comments around them stay free to move. This
    # script is pasted by hand on a machine with no other guard, so the root ownership,
    # the 440 the sudoers drop-in needs, the visudo gate ahead of it, and all five
    # bootstrap labels are checked exactly rather than sampled.
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


def test_self_check_cli_pings_the_pre_open_slug_when_the_daemon_is_up(
    tmp_path, capsys, monkeypatch
):
    config = write_config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    # main builds the probe and the pinger itself, so a fake reaches them by replacing
    # the producer main names, not by a seam this entry no longer accepts.
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    # Saturday, so no assertion is owed and this stays about the slug. Without a fixed
    # clock the answer would depend on the hour the suite ran, because a lake with no
    # stamped pid fails the check inside a weekday window.
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=et(2026, 9, 5, 12, 0)))
    code = cp.main(["self-check", "--config", str(config)])
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/pre-open"]
    printed = capsys.readouterr().out
    assert "slug=pre-open" in printed
    assert "secret-key" not in printed


def test_self_check_cli_names_a_failed_ping_and_still_reports(tmp_path, capsys, monkeypatch):
    # The ping is the last step, so a raise there used to replace the summary line with
    # a traceback in the job's err log. The line is what the operator reads.
    config = write_config(tmp_path, tmp_path / "lake")

    class Boom:
        def ping(self, url: str) -> None:
            raise urllib.error.URLError(OSError("connection refused"))

    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", Boom)
    # Saturday, for the reason the test above names: nothing owed, so the ping is what
    # this measures.
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=et(2026, 9, 5, 12, 0)))
    code = cp.main(["self-check", "--config", str(config)])
    assert code == 1
    printed = capsys.readouterr().out
    assert "self-check: ping failed: URLError" in printed
    assert "daemon up pinged=False slug=pre-open" in printed
    assert "secret-key" not in printed


def test_self_check_cli_exits_non_zero_without_pinging_when_down(tmp_path, monkeypatch):
    config = write_config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: False)
    # Faked even though the daemon probe returns first, so a reordering cannot reach the
    # real tool. The guard would catch it, but as a refusal rather than as the finding.
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    code = cp.main(["self-check", "--config", str(config)])
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


def _passing_canary() -> bool:
    """A canary that answers True without calling anything.

    Every ``sunday`` test replaces ``token_canary`` with one. main builds the real
    canary, which quotes a symbol through the vendor, so a test that left the producer
    live would reach the network.
    """
    return True


def _excluded(paths: Sequence[str]) -> str:
    """A reader reporting every path already excluded, the healthy Time Machine state.

    Every ``sunday`` test replaces ``read_exclusions`` with one. main builds the real
    reader, which shells out to ``tmutil``, so a test that left the producer live would
    reach a real subprocess.
    """
    return "".join(f"[Excluded]\t{p}\n" for p in paths)


class _Pushes:
    """A transport recording each push. The real one POSTs to ntfy."""

    def __init__(self) -> None:
        self.sent = []

    def send(self, message) -> None:
        self.sent.append(message)


class _BrokenTransport:
    """A transport that cannot deliver, which is an unreachable ntfy."""

    def send(self, message) -> None:
        raise OSError("network down")


def _sunday_lake(tmp_path: Path) -> tuple[Path, Path]:
    """A one-partition lake, a config naming it, and the backup copy on the SSD.

    The Sunday job scrubs both copies, so a run that should come back clean needs the
    copy to be there. ``write_config`` makes the target directory and this fills it the
    way the close+15 sync would have. A plain file copy, never an ``rsync``: the
    subprocess guard fails any test that reaches one.
    """
    lake = FixtureLake(tmp_path / "lake").with_chains("SPY", date(2026, 8, 28)).build()
    config = write_config(tmp_path, lake)
    mirror_lake(lake, tmp_path / "ssd")
    return lake, config


def test_sunday_cli_scrubs_the_configured_lake_and_pings(tmp_path, capsys, monkeypatch):
    lake, config = _sunday_lake(tmp_path)
    # A stamped pid says the daemon is healthy, which is what this test's healthy Sunday
    # is meant to exercise. An unstamped lake would read as a dead daemon here too.
    stamp_assertion_pid(lake, pid=_DAEMON_PID)
    pinger = FakePinger()
    pushes = _Pushes()
    # main builds each past-process producer itself. A fake reaches the run by replacing
    # the producer, so the test drives main's real wiring with nothing live at the leaves.
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: pushes)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
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
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    # A Sunday where the ritual was done owes no reminder, so the phone stays quiet.
    assert pushes.sent == []
    printed = capsys.readouterr().out
    assert "secret-key" not in printed


def test_the_sunday_cli_scrubs_the_configured_backup_target(tmp_path, capsys, monkeypatch):
    """The config's ``backup_target`` reaches the scrub, and not the lake root.

    A lake scrubbed as its own backup comes back clean forever, so that substitution is
    invisible from any test whose lake and copy are both clean. Damaging the copy alone
    is what tells the two apart, and it is the whole feature: a wiring slip here would
    check the lake against itself and report green every Sunday.
    """
    lake, config = _sunday_lake(tmp_path)
    next((tmp_path / "ssd").glob("chains/**/*.parquet")).write_bytes(b"rot")
    pinger = FakePinger()
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path))],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )

    assert code == 1
    assert pinger.urls == []
    printed = capsys.readouterr().out
    assert "backup scrub failed" in printed
    # The disk is named, and so is the file on it, because a count alone cannot be acted
    # on. The lake itself is clean, which is what says the copy is the side that went bad.
    assert str(tmp_path / "ssd") in printed
    assert "backup file does not match the lake: chains/" in printed
    assert "scrub failed: missing=0" not in printed.replace("backup scrub failed", "")


def test_sunday_cli_withholds_the_ping_for_a_stale_token(tmp_path, capsys, monkeypatch):
    # Minted late the prior week: still valid on Sunday, dead before Friday's option
    # close. Validity is not freshness, and the command line has to act on that, not
    # just the decision functions that already enforce it. This is the case the deleted
    # `--mint` override could hide, by supplying a mint the token does not carry.
    lake, config = _sunday_lake(tmp_path)
    pinger = FakePinger()
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
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
    )
    assert code == 1
    assert pinger.urls == []
    printed = capsys.readouterr().out
    assert "does not clear the coming week" in printed
    assert "secret-key" not in printed


def test_sunday_cli_reads_the_mint_time_from_the_token_file(tmp_path, capsys, monkeypatch):
    lake, config = _sunday_lake(tmp_path)
    token = _token(tmp_path)
    pinger = FakePinger()
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )
    assert code == 0
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    printed = capsys.readouterr().out
    assert "never-read" not in printed and "secret-key" not in printed


def test_sunday_cli_checks_the_time_machine_exclusion(tmp_path, capsys, monkeypatch):
    # Production must always run the check, so main builds the reader and the standard
    # targets. A lost exclusion rides the report and the ping still fires.
    lake, config = _sunday_lake(tmp_path)
    pinger = FakePinger()
    asked: list[tuple[str, ...]] = []

    def reader(paths):
        asked.append(tuple(paths))
        return "".join(f"[Included]\t{p}\n" for p in paths)

    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", reader)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
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
    )
    assert code == 0  # report tier: the ping still fires
    assert pinger.urls == ["https://hc-ping.com/secret-key/sunday"]
    assert len(asked) == 1
    # The config directory under the running account, and the token beside it.
    assert asked[0][0].endswith("/.config/marketlake")
    printed = capsys.readouterr().out
    assert "sunday: report: not excluded from time machine" in printed
    assert "secret-key" not in printed


def test_sunday_cli_reports_problems_and_exits_non_zero(tmp_path, capsys, monkeypatch):
    lake, config = _sunday_lake(tmp_path)
    pinger = FakePinger()
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: "")
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: lambda: False)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(tmp_path / "absent.json")],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
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
    # The job's log carries the reminder too. The phone is the channel that matters and
    # the push is checked below, but the log is what an operator reads after the fact.
    assert "sunday: reminder: The throwaway call" in printed


def test_the_sunday_cli_asks_about_the_daemon(tmp_path, capsys, monkeypatch):
    """The production entry has to pass the daemon probe, or a dead daemon through the
    Sunday window is never noticed. An unwired ``main`` would look exactly this healthy.
    """
    lake, config = _sunday_lake(tmp_path)
    pushes = _Pushes()
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: pushes)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: False)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path))],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )
    assert code == 0  # report tier: the ping still fires despite the finding
    printed = capsys.readouterr().out
    assert "sunday: report: Capture at risk: Sunday daemon down" in printed
    assert [m.event for m in pushes.sent] == [cp.SUNDAY_DAEMON_DOWN_EVENT]


def test_the_sunday_cli_reads_the_pid_from_the_journal_stamp(tmp_path, monkeypatch):
    """The production entry has to find the stamp, or the probe is asked about nothing.

    Reading a pid from somewhere the daemon never writes looks the same as a healthy
    machine on every other signal. Only the value reaching the probe tells them apart.
    """
    lake, config = _sunday_lake(tmp_path)
    asked: list[int] = []
    stamp_assertion_pid(lake, pid=7331)
    monkeypatch.setattr(
        cp,
        "read_pmset_schedule",
        lambda: "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n",
    )
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: asked.append(pid) or True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path))],
        clock=ManualClock(start=et(2026, 8, 30, 20, 0)),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )
    assert code == 0
    assert asked == [7331], "the CLI did not carry the stamped pid to the probe"


def test_the_sunday_cli_reads_the_stamp_instant_as_well_as_the_pid(tmp_path, monkeypatch):
    """The Sunday entry owes the page the stamp's instant, not just its pid.

    ``sunday_daemon_page`` defaults ``stamped_at`` to ``None``, which is what an unwired
    entry passes, and ``None`` reads as a stamp nobody is writing. So a CLI that carried
    the pid alone would page every half-done deploy with the words for a dead writer.
    The lake here carries a fresh heartbeat stamp and no pid, which is exactly what a
    daemon from before the pid field leaves behind.
    """
    lake, config = _sunday_lake(tmp_path)
    now = et(2026, 8, 30, 20, 0)
    stamp_cycle(
        lake,
        at=now - timedelta(seconds=30),
        token_minted_at=now - timedelta(days=1),
        roster=Roster(()),
    )
    pushes = _Pushes()
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: pushes)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)

    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path))],
        clock=ManualClock(start=now),
        calendar=weekday_sessions(date(2026, 8, 31)),
    )

    assert code == 0  # report tier: the ping still fires despite the finding
    assert [m.event for m in pushes.sent] == [cp.SUNDAY_ASSERTION_UNHELD_EVENT]
    assert cp.NO_PID_FRESH_STAMP in pushes.sent[0].body, "the CLI dropped the stamp instant"


# -- the two producers the launchd job runs on ---------------------------------------

# Both seams were built and never supplied. The canary fell back to a pass-through that
# returned True without calling anything, and the reminder had no sink, so it reached a
# log file and never a phone. These tests hold the wiring the installed job runs.

SUNDAY_20 = et(2026, 8, 30, 20, 0)
LATE_LAST_WEEK = et(2026, 8, 23, 18, 0)
REPEAT_ONLY = "Repeating power events:\n  wakepoweron at 8:25AM weekdays only\n"
WEEK_AHEAD = weekday_sessions(date(2026, 8, 31), date(2026, 9, 7))


def test_the_sunday_cli_builds_a_real_canary_rather_than_passing_through(tmp_path, monkeypatch):
    # The seam's producer is built from the token path and the config's credentials. A
    # command line that passed none would report a healthy weekend on a dead token.
    lake, config = _sunday_lake(tmp_path)
    token = _token(tmp_path)
    asked: list[dict] = []

    def fake_token_canary(*, token_path, api_key, app_secret):
        asked.append({"token_path": token_path, "api_key": api_key, "app_secret": app_secret})
        return lambda: False

    monkeypatch.setattr(cp, "token_canary", fake_token_canary)
    pinger = FakePinger()
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _Pushes())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(token)],
        clock=ManualClock(start=SUNDAY_20),
        calendar=WEEK_AHEAD,
    )
    assert asked == [{"token_path": str(token), "api_key": "api-key", "app_secret": "app-secret"}]
    # The producer's answer drives the run, so a failing call withholds the ping.
    assert code == 1
    assert pinger.urls == []


def test_the_sunday_cli_pushes_the_reminder_to_the_phone(tmp_path, monkeypatch):
    # A token minted late last week is still valid on Sunday and dead before Friday's
    # option close, so the ritual was skipped and the reminder is owed. The design sends
    # it on the 20:00, 21:00 and 22:00 runs while the check still fails.
    lake, config = _sunday_lake(tmp_path)
    # A stamped pid keeps the daemon-liveness page out of this evening's three reminder
    # pushes, which is what this test counts.
    stamp_assertion_pid(lake, pid=_DAEMON_PID)
    pushes = _Pushes()
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: pushes)
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path, LATE_LAST_WEEK))],
        clock=ManualClock(start=SUNDAY_20),
        calendar=WEEK_AHEAD,
    )
    assert code == 1
    assert len(pushes.sent) == 3
    first = pushes.sent[0]
    assert first.title == cp.REMINDER_TITLE
    assert first.event == cp.REMINDER_EVENT
    assert first.priority == cp.REMINDER_PRIORITY
    assert first.body.startswith("The coverage assertion failed.")
    # The mint date is the one fact from the config directory a message may carry.
    assert LATE_LAST_WEEK.date().isoformat() in first.body
    assert "app-secret" not in first.body and "secret-key" not in first.body


def test_a_reminder_that_cannot_be_pushed_is_written_down_and_the_evening_carries_on(
    tmp_path, capsys, monkeypatch
):
    # ntfy is unreachable. A Sunday job that died here would lose the scrub, the alarm
    # read-back and the check's ping with it, and the missed ping would page at 23:30
    # naming the wrong cause. So the push is recorded under reports/ and the run ends
    # on its own summary line.
    lake, config = _sunday_lake(tmp_path)
    # A stamped pid keeps the daemon-liveness page from adding a fourth undelivered
    # record to the three this test counts.
    stamp_assertion_pid(lake, pid=_DAEMON_PID)
    monkeypatch.setattr(cp, "read_pmset_schedule", lambda: REPEAT_ONLY)
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "token_canary", lambda **kwargs: _passing_canary)
    monkeypatch.setattr(cp, "NtfyTransport", lambda topic: _BrokenTransport())
    monkeypatch.setattr(cp, "read_exclusions", _excluded)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    code = cp.main(
        ["sunday", "--config", str(config), "--token", str(_token(tmp_path, LATE_LAST_WEEK))],
        clock=ManualClock(start=SUNDAY_20),
        calendar=WEEK_AHEAD,
    )
    assert code == 1
    printed = capsys.readouterr().out
    assert printed.strip().endswith("slug=sunday")
    assert "sunday: reminder not sent: post_failed, written down" in printed
    # This job writes into the lake it also scrubs, and it scrubs again on every retry.
    # `reports/` is on the scrub's enumerated exclusions, so the record the 20:00 attempt
    # wrote is not an orphan to the 20:30 one.
    assert "scrub failed" not in printed
    # One write-once file per undelivered message, under the day it was owed.
    records = sorted((lake / "reports" / "alerts" / "date=2026-08-30").glob("*.json"))
    assert len(records) == 3
    entry = json.loads(records[0].read_text())
    assert entry["event"] == cp.REMINDER_EVENT
    assert entry["reason"] == "post_failed"
    assert entry["detail"] == "OSError"


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


# -- the re-auth script --------------------------------------------------------


def test_the_install_points_at_the_reauth_script_without_running_it(tmp_path, capsys):
    """A fresh machine captures nothing until a token exists, so the install says so.

    Both renderings carry the pointer, because they come from one function. Every line
    of it is a comment: acquiring a token needs a browser and a person, so the install
    names the step rather than taking it. A pointer that ran would also break ``set -e``
    on a machine with no browser.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = (out / cp.INSTALL_SCRIPT_FILE).read_text()
    printed = capsys.readouterr().out
    for text in (script, printed):
        pointer = [line for line in text.splitlines() if cp.REAUTH_SCRIPT_FILE in line]
        assert pointer, text
        assert all(line.startswith("#") for line in pointer), pointer
    assert cp.CALLBACK_KEY in script


def test_the_reauth_script_runs_the_reauth_module_and_nothing_else(tmp_path):
    """The script is a wrapper. The logic lives in ``lake.reauth``, where tests reach it.

    It also calls no sudo. The token is written as the owner, which the design's
    Deployment section requires: a token re-created under root would be unreadable by
    every job that runs as the owner.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    lines = (out / cp.REAUTH_SCRIPT_FILE).read_text().splitlines()
    # The interpreter is named, not merely present. The execution test below proves a
    # shebang exists; only this says which one, and the golden alone would report a
    # swapped interpreter as a stale fixture.
    assert lines[0] == "#!/bin/bash"
    commands = [line for line in lines if line and not line.startswith("#")]
    assert commands == [
        "set -euo pipefail",
        "unset MARKETLAKE_CONFIG_DIR",
        "cd /Users/someone/marketlake",
        'exec /opt/py/bin/python -m lake.reauth "$@"',
    ]
    assert not any("sudo" in line for line in commands)


def test_the_written_reauth_script_is_executable(tmp_path):
    """``./reauth.sh`` is the whole interface, so the bit is read off disk.

    The uninstall and restart scripts each have the same test in their own section. The
    golden cannot stand in for any of them: git stores those fixtures at 0o644.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert (out / cp.REAUTH_SCRIPT_FILE).stat().st_mode & 0o777 == 0o755


def test_the_reauth_script_says_it_cannot_run_unattended(tmp_path):
    """The header states it, and says the tool itself is what enforces it.

    Documentation alone would leave the foot-gun in place, so the header has to point at
    the refusal rather than stand in for it.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    header = (out / cp.REAUTH_SCRIPT_FILE).read_text()
    assert "Do not add it to a plist." in header
    assert "refuses when stdin is not a terminal" in header


def test_nothing_rendered_bakes_in_a_secret_url(tmp_path, capsys):
    """The renderer reads no config, so no callback and no secret can reach a file.

    ``render_all`` takes a ``LaunchdHost`` and nothing else, so it cannot know the
    registered callback or the healthchecks ping key. Every rendering names a config key
    or a check slug and leaves the value to the tool, which is what keeps a rendered
    directory safe to paste into a bug report.

    The sweep covers every rendered file and the install text rather than the re-auth
    script alone. The arming step names a healthchecks row, and a ping URL written
    beside it would put a secret in a tracked rendering.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    texts = {path.name: path.read_text() for path in out.iterdir()}
    texts["INSTALL.txt"] = capsys.readouterr().out
    assert set(texts) == EXPECTED_FILES | {"INSTALL.txt"}
    for name, text in texts.items():
        for marker in ("hc-ping.com", "healthchecks.io/", "ntfy.sh", f"{cp.CALLBACK_KEY}:"):
            assert marker not in text, (name, marker)
        # A ping key needs no host beside it to be a secret, so its shape is swept for
        # too. healthchecks mints one as a UUID. The sweep names shapes rather than
        # refusing "://" outright, because every plist carries the Apple DTD URL, and it
        # avoids a bare run-length rule, because a tmp_path directory name trips one.
        assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-", text), name
        assert not re.search(r"\b[0-9a-f]{32}\b", text), name
    # The re-auth script carries no URL at all. Its one job is the browser login, so a
    # baked-in callback is likelier there than anywhere else.
    assert "://" not in texts[cp.REAUTH_SCRIPT_FILE]


def test_the_rendered_reauth_script_runs_and_forwards_its_arguments(tmp_path):
    """Executed rather than read. The ``cd`` lands, and ``"$@"`` reaches the module.

    The interpreter is a stand-in that records how it was called, so nothing imports
    ``lake.reauth`` here and no login is attempted. What this exercises is the script
    itself: its shebang, its executable bit, the working directory it moves to, and the
    arguments it passes through.
    """
    project = tmp_path / "project"
    project.mkdir()
    log = tmp_path / "log"
    python = tmp_path / "fake-python"
    python.write_text(f'#!/bin/bash\nprintf \'%s|%s\\n\' "$PWD" "$*" >> {log}\n')
    python.chmod(0o755)

    out = tmp_path / "out"
    args = [
        "--python",
        str(python),
        "--owner",
        "someone",
        "--home",
        "/Users/someone",
        "--project-dir",
        str(project),
        "--log-dir",
        "/Users/someone/Library/Logs/marketlake",
    ]
    assert cp.main(["render", "--out", str(out), *args]) == 0

    proc = subprocess.run(
        [str(out / cp.REAUTH_SCRIPT_FILE), "--config", "/tmp/throwaway.yaml"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert log.read_text().splitlines() == [
        f"{project}|-m lake.reauth --config /tmp/throwaway.yaml"
    ]


# -- arming the live checks ----------------------------------------------------

# The button the operator presses. Every test below finds the step by this phrase rather
# than by a slug, so a rendering that dropped a slug is still found and still fails the
# test that asks for it.
PING_NOW = "Ping Now"

# Every check the install tells the operator to arm, read from the renderer's own
# constants rather than spelled here. Four of the five ping only when their own job
# succeeds, so a failing install leaves each row in the never-pinged state where it
# cannot page. ``calendar-probe`` arms itself by the next weekday 09:35 and is in the
# list anyway, because a roster with one member left out is how the gap comes back.
ARMED_SLUGS = (
    "CAPTURE_SLUG",
    "PRE_OPEN_SLUG",
    "SUNDAY_SLUG",
    "COMPACTION_SLUG",
    "CALENDAR_PROBE_SLUG",
)

# The one place the block uses the word capture as English rather than as the slug. The
# rename check below exempts it by name, so every other occurrence has to have moved.
CAPTURE_AS_ENGLISH = "capture window"


def _pressed(text: str) -> int:
    """The index of the one rendered line telling the operator to press the button.

    One line, not five. The step names five checks and asks for a press on each of them
    in a single instruction, which is the shape ``uninstall_script`` already uses to name
    four slugs to the operator. A second press line would be a second instruction to
    read, and a step the operator gives up on is the same as no step.
    """
    hits = [i for i, line in enumerate(text.splitlines()) if PING_NOW in line]
    assert len(hits) == 1, hits
    return hits[0]


def _rendered_install(tmp_path: Path, capsys) -> tuple[str, str]:
    """The install script and the install text from one render, in that order."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    return (out / cp.INSTALL_SCRIPT_FILE).read_text(), capsys.readouterr().out


def _arming_block(text: str) -> list[str]:
    """The rendered lines of the arming step, found by the run that holds the button.

    Bounded by the step itself rather than by a line count, so the block is located
    without asserting what it says. The press line anchors it and the run extends to the
    length the renderer produced.
    """
    lines = text.splitlines()
    start = _pressed(text) - cp._arm_checks_step_lines().index(
        next(line for line in cp._arm_checks_step_lines() if PING_NOW in line)
    )
    return lines[start : start + len(cp._arm_checks_step_lines())]


def _press_instruction(block: list[str]) -> str:
    """The one sentence telling the operator to press the button, unwrapped.

    Read as a sentence rather than as the line the button falls on, because the renderer
    wraps and the slugs spill onto the lines under it. A test that asked only whether the
    five appear somewhere in the block would pass an install that named one check to press
    and said the other four arm themselves, which is the claim this whole step exists to
    undo.
    """
    prose = " ".join(line.removeprefix("#").strip() for line in block)
    hits = [s.strip() for s in prose.split(". ") if PING_NOW in s]
    assert len(hits) == 1, hits
    return hits[0]


def _checks_named(instruction: str) -> list[str]:
    """The checks the press instruction names, read off the list it introduces."""
    _, colon, listed = instruction.partition(":")
    assert colon, instruction
    names = []
    for part in listed.strip().rstrip(".").split(","):
        names += [name.strip() for name in part.split(" and ")]
    return [name for name in names if name]


def test_both_renderings_carry_one_arming_step(tmp_path, capsys):
    """The same block, whole and contiguous, in the script and in the pasted text.

    The other tests in this section ask each rendering the same questions separately, so
    a second copy of the step that answered all of them would pass while saying
    something different from the first. That is the drift one shared source exists to
    stop, and only the golden files caught it. A golden carries a documented way to
    regenerate, so a drift blessed by a refresh left nothing red.
    """
    step = cp._arm_checks_step_lines()
    assert step, step
    for text in _rendered_install(tmp_path, capsys):
        assert _arming_block(text) == step, text


def test_the_step_reads_every_slug_rather_than_spelling_it(tmp_path, capsys):
    """Renaming a check moves the rendering, rather than leaving it pointing nowhere.

    Comparing the rendering against a constant's current value cannot tell a real read
    from the same word typed twice. Both agree either way. Renaming the check under the
    test is what separates them, and it is the rename that would otherwise ship an
    install pointing the operator at a row that no longer exists.

    Every one of the five is renamed, one at a time, because a block that reads one slug
    and spells the other four is the half-fix this section exists to stop. The original
    has to be gone from the block afterwards, so a renderer that read the constant and
    spelled the word beside it fails too. ``capture`` is the one exception, and it is
    named rather than waived: the block also says *capture window*, which is the session
    term and not the check.
    """
    monkey = "sentinel-slug"
    for text in _rendered_install(tmp_path, capsys):
        assert monkey not in text

    import lake.control_plane

    for name in ARMED_SLUGS:
        original = getattr(lake.control_plane, name)
        setattr(lake.control_plane, name, monkey)
        try:
            for text in _rendered_install(tmp_path, capsys):
                block = "\n".join(_arming_block(text))
                assert monkey in block, (name, block)
                assert block.count(CAPTURE_AS_ENGLISH) == 1, block
                remaining = block.replace(CAPTURE_AS_ENGLISH, "<window>")
                assert original not in remaining, (name, remaining)
        finally:
            setattr(lake.control_plane, name, original)


def _pressed_offset() -> int:
    """Where the press line sits inside the step, so the rename check reads that line."""
    step = cp._arm_checks_step_lines()
    return next(i for i, line in enumerate(step) if PING_NOW in line)


def test_the_arming_step_closes_the_rendered_install(tmp_path, capsys):
    """Both renderings end their install on it, because it is the last step of installing.

    A check that has never been pinged never goes down until something pings it, so an
    install whose jobs all fail leaves every one of those rows silent. Arming them is the
    step that turns the silence into a page, and a step the operator never reaches is the
    same as no step.

    Both renderings are checked, because the same install is rendered twice. A step
    added to the script and not to the pasted text is the half-fix this guards.
    """
    script, printed = _rendered_install(tmp_path, capsys)

    # The script closes on the step. Nothing follows it, and the token pointer, the one
    # other comment-only block, comes before it.
    lines = script.rstrip("\n").splitlines()
    press = _pressed(script)
    assert all(not line or line.startswith("#") for line in lines[press:]), lines[press:]
    # The step's own last line is the file's last line. Compared against the step rather
    # than against the words it happens to use, so rewording the block is free and
    # appending anything after it is not.
    assert lines[-1] == cp._arm_checks_step_lines()[-1]
    token = next(i for i, line in enumerate(lines) if line.startswith("# The token."))
    assert token < press

    # The text carries the same block last, ahead of the standing Friday task. Step 6
    # is not part of the first install, and the reference notes under it are not steps.
    text_lines = printed.splitlines()
    text_press = _pressed(printed)
    text_token = next(i for i, line in enumerate(text_lines) if line.startswith("# The token."))
    friday = next(i for i, line in enumerate(text_lines) if line.startswith("# 6."))
    assert text_token < text_press < friday


def test_the_arming_step_comes_after_the_jobs_are_bootstrapped(tmp_path, capsys):
    """Order, not presence. Arming ahead of the bootstrap pages about the install order.

    A check armed before the jobs are loaded starts its grace running against a machine
    that has nothing to ping it, so the page that follows says the operator was slow
    rather than that the daemon is broken. The read-back sits between the two, so the
    step lands after the operator has seen whether the daemon came up.
    """
    for text in _rendered_install(tmp_path, capsys):
        lines = text.splitlines()
        bootstraps = [
            i for i, line in enumerate(lines) if line.startswith("sudo launchctl bootstrap")
        ]
        assert len(bootstraps) == 5, bootstraps
        read_back = [
            i
            for i, line in enumerate(lines)
            if line.startswith(f"launchctl print {cp.LAUNCHD_DOMAIN}/{cp.DAEMON_LABEL}")
        ]
        assert len(read_back) == 1, read_back
        assert max(bootstraps) < read_back[0] < _pressed(text), text


def test_the_arming_step_asks_for_a_press_on_every_check_and_on_no_other(tmp_path, capsys):
    """The instruction names every row to press the button on, and names nothing else.

    Healthchecks shows one row per check, and a row nobody presses stays in the state
    where it cannot page. The install used to name one, ``capture``, and said nothing
    about the other four. The daemon builds one dead-man and feeds ``capture`` with it,
    so nothing was arming them.

    Read off the instruction rather than off the block, and compared as a set rather than
    as a search. Presence alone would pass an install that named ``capture`` to press and
    said the rest arm themselves, and a one-way search would pass one that sent the
    operator hunting for a sixth row no job pings. The quantifier is asserted for the same
    reason: an instruction to press one of five is not an instruction to press five.

    The slugs are also all the step may carry, because a ping URL is a secret and the
    renderer's output is tracked.

    ``lake.control_plane`` defines all five, so the two read through ``lake.deadman`` and
    ``lake.compact`` assert that the job re-exports the same constant the renderer names
    rather than a second spelling of it.
    """
    from lake.compact import COMPACTION_SLUG
    from lake.control_plane import CALENDAR_PROBE_SLUG, PRE_OPEN_SLUG, SUNDAY_SLUG
    from lake.deadman import CAPTURE_SLUG

    slugs = [CAPTURE_SLUG, PRE_OPEN_SLUG, SUNDAY_SLUG, COMPACTION_SLUG, CALENDAR_PROBE_SLUG]
    assert len(set(slugs)) == len(slugs), slugs
    for text in _rendered_install(tmp_path, capsys):
        instruction = _press_instruction(_arming_block(text))
        assert sorted(_checks_named(instruction)) == sorted(slugs), instruction
        assert "each" in instruction, instruction


def test_every_line_of_the_arming_step_is_a_comment(tmp_path, capsys):
    """``set -e`` cannot trip on it, because there is nothing there to run.

    Pressing a button on a web page needs a browser and a person. Rendered as a command
    it would be a command that cannot succeed, and the script stops at the first failure
    by design, so the install would end by failing on the step that closes it.
    """
    script, printed = _rendered_install(tmp_path, capsys)
    lines = script.rstrip("\n").splitlines()
    runnable = [line for line in lines if line and not line.startswith("#")]
    # The last thing the script runs is the read-back. Anything the arming step added
    # would run after it.
    assert runnable[-1] == f"launchctl print {cp.LAUNCHD_DOMAIN}/{cp.DAEMON_LABEL}", runnable[-1]
    # Both renderings, not the script alone. The pasted text is run by hand line by
    # line, so a runnable line there is a command the operator runs at a browser step.
    for text in (script, printed):
        assert all(line.startswith("#") for line in _arming_block(text)), text


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
    """Every rendered byte is checked, not only the fields other tests sample.

    The tests above assert on chosen lines: the sudoers rules, two jobs' program
    arguments, the install commands in order. A change to a plist key none of them
    names, such as a log path or a throttle, would land unreviewed. This compares the
    whole file, so any change to the rendering shows up as a diff.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    _check_golden(name, (out / name).read_bytes())


def test_the_install_text_matches_its_golden(tmp_path, capsys):
    """The pasted script is checked whole, with only the output directory normalised."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    _check_golden("INSTALL.txt", _normalise(capsys.readouterr().out, out).encode())


def test_the_golden_directory_holds_exactly_what_is_covered():
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
    """The file says how to invoke itself on its own, which is what a reader needs first.

    A substring check for ``./install.sh`` no longer discriminates. The header also carries
    the reinstall line ``./uninstall.sh && ./install.sh``, which contains that substring, so
    the whole Usage block could be deleted and a substring check would stay green. This
    reads the commands out of the block instead, and requires one that is install.sh alone.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    header = (out / cp.INSTALL_SCRIPT_FILE).read_text().split("set -euo pipefail")[0]
    assert "Usage" in header
    # Indented comment lines are the header's worked examples. Strip the marker and the
    # trailing explanatory comment to get the command each one shows.
    shown = [
        line.lstrip("# ").split("#")[0].strip()
        for line in header.splitlines()
        if line.startswith("#     ")
    ]
    assert f"./{cp.INSTALL_SCRIPT_FILE}" in shown, shown
    assert [line for line in shown if line.endswith(f"/{cp.INSTALL_SCRIPT_FILE}")], shown


# -- reinstalling, which is the two scripts composed --------------------------

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
  bootout)   [[ "$BOOTOUT_RC" != "0" ]] && exit "$BOOTOUT_RC"
             /bin/rm -f "$LOADED/$label"; exit 0 ;;
  bootstrap) /usr/bin/touch "$LOADED/$label"; exit 0 ;;
  *)         exit 0 ;;
esac
"""

REINSTALL_COMMAND = f"./{cp.UNINSTALL_SCRIPT_FILE} && ./{cp.INSTALL_SCRIPT_FILE}"


def _run_reinstall(tmp_path: Path, *, bootout_rc: int = 0):
    """Render, then reinstall by composing the two scripts, exactly as an operator would.

    There is no third script to run. This is the composed command from both headers,
    executed end to end against fakes on ``PATH``. Both halves run for real, so what it
    exercises is the composition rather than the text of any file.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    log = tmp_path / "log"
    log.write_text("")
    # Everything starts loaded, which is the state a reinstall actually meets.
    loaded_dir = tmp_path / "loaded"
    loaded_dir.mkdir(exist_ok=True)
    for job in cp.all_jobs(_host()):
        (loaded_dir / job.label).touch()
    for name in ("install", "rm", "pmset", "tmutil", "grep", "visudo"):
        (bin_dir / name).write_text(_FAKE.format(body="exit 0"))
        (bin_dir / name).chmod(0o755)
    (bin_dir / "sudo").write_text(_FAKE_SUDO)
    (bin_dir / "sudo").chmod(0o755)
    (bin_dir / "launchctl").write_text(_FAKE_LAUNCHCTL)
    (bin_dir / "launchctl").chmod(0o755)
    proc = subprocess.run(
        ["/bin/bash", "-c", REINSTALL_COMMAND],
        cwd=out,
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


def test_reinstalling_runs_the_whole_uninstall_before_the_whole_install(tmp_path):
    """Running the composed command end to end, because that is what a reinstall is.

    The order is the property worth covering. Every removal has to land before the first
    installation, or a bootout races a bootstrap for the same label.
    """
    proc, log = _run_reinstall(tmp_path)
    assert proc.returncode == 0, proc.stderr
    boots_out = [i for i, line in enumerate(log) if line.startswith("launchctl bootout")]
    deletes = [i for i, line in enumerate(log) if line.startswith("rm -f /Library/LaunchDaemons")]
    installs = [i for i, line in enumerate(log) if line.startswith("install ")]
    boots_in = [i for i, line in enumerate(log) if line.startswith("launchctl bootstrap")]
    assert len(boots_out) == 5 and len(deletes) == 5, log
    assert len(installs) == 6 and len(boots_in) == 5, log
    assert max(deletes) < min(installs), log
    assert max(boots_out) < min(boots_in), log
    # The sudoers drop-in is removed and written again. The re-tune case that the
    # earlier in-place plist swap skipped.
    assert any(line == "rm -f /etc/sudoers.d/marketlake" for line in log), log
    assert any(line.startswith("install ") and "sudoers.d" in line for line in log), log


def test_a_failing_uninstall_leaves_the_install_half_unrun(tmp_path):
    """The ``&&`` is load-bearing, so it is asserted by running it rather than described.

    A job that will not stop is the bootout failure worth halting for. Carrying on would
    write a fresh plist under a definition launchd is still holding. This is the whole
    reason both headers spell the separator ``&&`` and not ``;``.

    Two things make the uninstall exit non-zero here, which is deliberate. ``set -e``
    catches the failing bootout, and the step 5 read-back exits 1 on its own because the
    daemon is still loaded. Removing ``set -e`` alone does not reach this test, and the
    survival is the point rather than a gap: ``test_the_uninstall_stops_at_a_failure``
    ``_partway_down`` covers that half.
    """
    proc, log = _run_reinstall(tmp_path, bootout_rc=1)
    assert proc.returncode != 0
    assert not [line for line in log if line.startswith("install ")], log
    assert not [line for line in log if line.startswith("launchctl bootstrap")], log


def test_both_headers_give_the_reinstall_command_and_say_why_the_separator_matters(tmp_path):
    """The composed command has no file of its own, so it lives in the two that remain.

    ``render`` writes the scripts and prints the install text to stdout. An operator who
    comes back to the rendered directory has only these files to read, so the third verb
    has to be named in them or it is named nowhere they will look.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    for name in (cp.INSTALL_SCRIPT_FILE, cp.UNINSTALL_SCRIPT_FILE):
        header = (out / name).read_text().split("set -euo pipefail")[0]
        assert REINSTALL_COMMAND in header, name
        assert "`&&` is load-bearing" in header, name


def test_render_writes_no_reinstall_script(tmp_path):
    """A reinstall is the two scripts composed, so a third file would be a third answer.

    The one it had drifted inside a single commit: its header restated the uninstall's
    counted set of three survivals as two, dropping the Sunday one-shot. That is the
    cost of a second description, and the cut removes the class rather than the typo.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert not [path for path in out.iterdir() if "reinstall" in path.name], list(out.iterdir())
    assert not hasattr(cp, "reinstall_script")
    assert not hasattr(cp, "REINSTALL_SCRIPT_FILE")


# -- the restart script --------------------------------------------------------

# launchd has three states this must reproduce, not two. A label absent from the
# domain exits 113. A label present but with no process exits 0 and prints no pid
# line. A running label exits 0 with one pid line. Relaunch is asynchronous, so a
# kickstart clears the pid and a later poll brings it back.
_FAKE_LAUNCHCTL_STATES = """#!/bin/bash
printf 'launchctl %s\\n' "$*" >> "$LOG"
label_of() { printf '%s' "${1##*/}"; }

case "$1" in
  print)
    label="$(label_of "$2")"
    if [[ ! -e "$STATE/$label.loaded" ]]; then exit 113; fi
    printf 'system/%s = {\\n' "$label"
    if [[ -e "$STATE/$label.pid" ]]; then st=running; else st='not running'; fi
    printf '\\tstate = %s\\n' "$st"
    if [[ -e "$STATE/$label.pending" ]]; then
      n="$(cat "$STATE/$label.pending")"
      if [[ "$n" -le 0 ]]; then
        /bin/rm -f "$STATE/$label.pending"
        printf '%s' "$(( $(cat "$STATE/$label.base") + 1 ))" > "$STATE/$label.pid"
      else
        printf '%s' "$(( n - 1 ))" > "$STATE/$label.pending"
      fi
    elif [[ "$MODE" == "crash_loop" && -e "$STATE/$label.pid" ]]; then
      printf '%s' "$(( $(cat "$STATE/$label.pid") + 1 ))" > "$STATE/$label.pid"
    fi
    if [[ -e "$STATE/$label.pid" ]]; then
      printf '\\tpid = %s\\n' "$(cat "$STATE/$label.pid")"
    fi
    printf '}\\n'
    exit 0 ;;
  kickstart)
    label="$(label_of "$3")"
    if [[ "$KICKSTART_RC" != "0" ]]; then exit "$KICKSTART_RC"; fi
    case "$MODE" in
      no_change) ;;
      never) /bin/rm -f "$STATE/$label.pid" ;;
      *)
        if [[ -e "$STATE/$label.pid" ]]; then cp "$STATE/$label.pid" "$STATE/$label.base"; fi
        /bin/rm -f "$STATE/$label.pid"
        printf '%s' "$DELAY" > "$STATE/$label.pending" ;;
    esac
    exit 0 ;;
esac
exit 0
"""

_FAKE_GIT = """#!/bin/bash
printf 'git %s\\n' "$*" >> "$LOG"
case "$*" in
  *rev-parse*--git-dir*)      [[ "$IS_REPO" == "1" ]] && exit 0 || exit 128 ;;
  *rev-parse*--abbrev-ref*)   echo "$BRANCH"; exit 0 ;;
  *status*--porcelain*)       [[ "$DIRTY" == "1" ]] && echo " M src/lake/x.py"; exit 0 ;;
esac
exit 0
"""


def _run_restart(
    tmp_path: Path,
    *,
    argv=(),
    running=("daemon", "dashboard"),
    loaded=None,
    mode="restart",
    delay=0,
    kickstart_rc=0,
    branch="main",
    dirty=False,
    is_repo=True,
):
    """Render, then run restart.sh against fakes. Returns (proc, log lines).

    ``loaded`` is every label in the domain and defaults to ``running``. A label in
    ``loaded`` but not ``running`` is the state real launchd reports as present with no
    pid, which is the one the first harness could not express.

    ``mode`` picks what the kickstart does: ``restart`` brings a new pid back after
    ``delay`` polls, ``no_change`` leaves the pid alone, ``never`` never brings one back,
    and ``crash_loop`` returns a fresh pid on every poll.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    log = tmp_path / "log"
    log.write_text("")
    state = tmp_path / "state"
    state.mkdir(exist_ok=True)
    for name in running if loaded is None else loaded:
        (state / f"com.marketlake.{name}.loaded").write_text("")
    for i, name in enumerate(running):
        (state / f"com.marketlake.{name}.pid").write_text(str(1000 + i))
        (state / f"com.marketlake.{name}.base").write_text(str(1000 + i))
    (bin_dir / "sudo").write_text(_FAKE_SUDO)
    (bin_dir / "sudo").chmod(0o755)
    (bin_dir / "launchctl").write_text(_FAKE_LAUNCHCTL_STATES)
    (bin_dir / "launchctl").chmod(0o755)
    (bin_dir / "git").write_text(_FAKE_GIT)
    (bin_dir / "git").chmod(0o755)
    # rm and pmset are faked so the "never boots a label out" exclusions are live rather
    # than unfalsifiable. Nothing in the script should reach them.
    for name in ("rm", "pmset", "tmutil"):
        (bin_dir / name).write_text(_FAKE.format(body="exit 0"))
        (bin_dir / name).chmod(0o755)
    # sleep is faked so the settle wait and the retry loop cost no wall clock.
    (bin_dir / "sleep").write_text("#!/bin/bash\nexit 0\n")
    (bin_dir / "sleep").chmod(0o755)
    proc = subprocess.run(
        [str(out / cp.RESTART_SCRIPT_FILE), *argv],
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "LOG": str(log),
            "STATE": str(state),
            "MODE": mode,
            "DELAY": str(delay),
            "KICKSTART_RC": str(kickstart_rc),
            "BRANCH": branch,
            "DIRTY": "1" if dirty else "0",
            "IS_REPO": "1" if is_repo else "0",
        },
        capture_output=True,
        text=True,
    )
    return proc, [line for line in log.read_text().splitlines() if line]


def test_the_restart_offers_exactly_the_jobs_that_can_go_stale(tmp_path):
    """Derived from ``keep_alive``, because that is what makes a job able to go stale.

    A resident job holds the Python it imported at start. The three calendar jobs exec
    fresh on every fire, so restarting one would be meaningless. Listing the two by hand
    would let a sixth resident job be added without this script learning about it.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = (out / cp.RESTART_SCRIPT_FILE).read_text()
    resident = [job.label for job in cp.all_jobs(_host()) if job.keep_alive]
    transient = [job.label for job in cp.all_jobs(_host()) if not job.keep_alive]
    assert len(resident) == 2 and len(transient) == 3, (resident, transient)
    for label in resident:
        assert f"LABELS=({label})" in script, label
    for label in transient:
        assert label not in script, label


def test_the_restart_defaults_to_the_dashboard(tmp_path):
    """Restarting the daemon costs the in-flight cycle, so it has to be asked for.

    The dashboard only drops open connections. A bare ``./restart.sh`` must not be the
    command that takes capture down.
    """
    proc, log = _run_restart(tmp_path)
    assert proc.returncode == 0, proc.stderr
    # The fake sudo execs what it is given, so each kickstart logs twice. Counting the
    # sudo-prefixed line alone also checks that the restart runs as root.
    kicks = [line for line in log if line.startswith("sudo launchctl kickstart")]
    assert len(kicks) == 1, log
    assert cp.DASHBOARD_LABEL in kicks[0], kicks[0]
    assert cp.DAEMON_LABEL not in kicks[0], kicks[0]


def test_the_restart_proves_the_process_changed(tmp_path):
    """A kickstart that silently did nothing is the failure this script exists to catch."""
    proc, _ = _run_restart(tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "restarted: pid 1001 -> 1002" in proc.stdout, proc.stdout


def test_a_kickstart_that_leaves_the_pid_alone_fails(tmp_path):
    """Same pid means the process never came down, so reporting success would be a lie."""
    proc, log = _run_restart(tmp_path, mode="no_change")
    assert proc.returncode != 0
    assert "did not restart" in proc.stderr, proc.stderr


def test_an_unknown_job_name_is_refused_before_anything_runs(tmp_path):
    """A typo must not silently restart the default."""
    proc, log = _run_restart(tmp_path, argv=("dashbaord",))
    assert proc.returncode == 2, proc.stdout
    assert "usage:" in proc.stderr, proc.stderr
    assert not [line for line in log if "kickstart" in line], log


def test_the_restart_warns_when_the_tree_is_not_what_will_ship(tmp_path):
    """The services import from the working tree, so a restart adopts whatever is there.

    This is the hazard that motivated the script. A restart taken while the checkout sits
    on a feature branch silently promotes that branch into the running service.
    """
    on_branch, _ = _run_restart(tmp_path, branch="claude/some-work")
    assert "not on main" in on_branch.stdout, on_branch.stdout
    dirty, _ = _run_restart(tmp_path, dirty=True)
    assert "uncommitted changes" in dirty.stdout, dirty.stdout
    clean, _ = _run_restart(tmp_path)
    assert "not on main" not in clean.stdout and "uncommitted" not in clean.stdout, clean.stdout


def test_the_restart_survives_a_project_dir_that_is_not_a_checkout(tmp_path):
    """The design never promises the project directory is a git repo, so this cannot die."""
    proc, _ = _run_restart(tmp_path, is_repo=False)
    assert proc.returncode == 0, proc.stderr
    assert "not a git checkout" in proc.stdout, proc.stdout


def test_the_restart_never_boots_a_label_out(tmp_path):
    """The bootout-and-bootstrap restart is considered and rejected. It stays cut.

    It would also pick up a changed plist, which makes it look like the general tool. It
    is the more dangerous one: a failure between the bootout and the bootstrap leaves the
    service down, where a kickstart cannot, because launchd holds the definition
    throughout. A changed plist is the install's job.
    """
    proc, log = _run_restart(tmp_path, argv=("all",))
    assert proc.returncode == 0, proc.stderr
    kicks = [line for line in log if line.startswith("sudo launchctl kickstart")]
    assert len(kicks) == 2, log
    for word in ("bootout", "bootstrap", "rm ", "pmset"):
        assert not [line for line in log if word in line], (word, log)
    script = (tmp_path / "out" / cp.RESTART_SCRIPT_FILE).read_text()
    assert "bootout" not in script and "bootstrap" not in script


def test_a_loaded_job_between_processes_is_started_not_refused(tmp_path):
    """The state real launchd reports as present with no pid, which is not "not loaded".

    ``launchctl print`` exits 0 for any label in the domain and 113 for one that is not,
    while the pid line appears only while a process runs. A resident crash-looping on bad
    code sits in the first state. Refusing there would be wrong twice: the diagnosis is
    false, and the remedy it offers, the install, bootstraps labels already in the domain
    and fails under ``set -e``. ``kickstart`` runs a service whatever its launch
    conditions say, so it is exactly the right thing to do here.
    """
    proc, log = _run_restart(tmp_path, running=("daemon",), loaded=("daemon", "dashboard"))
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "loaded but not running, so this starts it" in proc.stdout, proc.stdout
    assert [line for line in log if line.startswith("sudo launchctl kickstart")], log


def test_a_label_absent_from_the_domain_is_refused(tmp_path):
    """Not installed is the other empty-pid reading, and this one really is fatal."""
    proc, log = _run_restart(tmp_path, running=("daemon",), loaded=("daemon",))
    assert proc.returncode != 0
    assert "is not in the system domain" in proc.stderr, proc.stderr
    assert not [line for line in log if "kickstart" in line], log


def test_a_job_that_will_not_stay_up_is_reported_as_a_failure(tmp_path):
    """A new pid is not a working service, which is the failure a restart most often causes.

    A resident that dies on import gets a fresh pid within seconds, so "the pid changed"
    is satisfied by exactly the case an operator most needs told about. The new pid has to
    still be there after the settle wait.
    """
    proc, _ = _run_restart(tmp_path, mode="crash_loop")
    assert proc.returncode != 0
    assert "will not stay up" in proc.stderr, proc.stderr
    assert "crash-looping on the new code" in proc.stderr, proc.stderr
    assert ".err.log" in proc.stderr, proc.stderr


def test_a_job_that_never_comes_back_is_reported_as_a_failure(tmp_path):
    """The other half of the read-back: a kickstart that killed it and got nothing back."""
    proc, _ = _run_restart(tmp_path, mode="never")
    assert proc.returncode != 0
    assert "has no pid after the restart" in proc.stderr, proc.stderr
    assert ".err.log" in proc.stderr, proc.stderr


def test_the_restart_waits_for_a_relaunch_that_is_not_instant(tmp_path):
    """KeepAlive relaunches within seconds, so reading the pid once would race it.

    The first harness bumped the pid inside the kickstart call, so the retry loop was
    never exercised and could be deleted with every test still green.
    """
    proc, _ = _run_restart(tmp_path, delay=3)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "restarted: pid 1001 -> 1002" in proc.stdout, proc.stdout


def test_the_written_restart_script_is_executable(tmp_path):
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert (out / cp.RESTART_SCRIPT_FILE).stat().st_mode & 0o777 == 0o755


# -- the uninstall script ------------------------------------------------------


def _commands(script: str) -> list[str]:
    """Every runnable line of a rendered script, comments and echoes dropped."""
    return [
        line.strip()
        for line in script.splitlines()
        if line.strip() and not line.strip().startswith(("#", "echo ", "set ", "HERE="))
    ]


def _install_targets(out: Path) -> set[str]:
    """Every absolute path install.sh writes to, derived from the script itself."""
    targets = set()
    for line in _commands((out / cp.INSTALL_SCRIPT_FILE).read_text()):
        if not line.startswith("sudo install "):
            continue
        dest, src = line.split()[-1], line.split()[-2].strip('"')
        name = src.rsplit("/", 1)[-1]
        targets.add(dest.rstrip("/") + "/" + name if dest.endswith("/") else dest)
    return targets


def _rm_targets(out: Path) -> set[str]:
    """Every path uninstall.sh deletes. Runnable lines only, so a comment cannot count."""
    return {
        line.split()[-1]
        for line in _commands((out / cp.UNINSTALL_SCRIPT_FILE).read_text())
        if " rm " in f" {line} "
    }


def test_the_uninstall_deletes_exactly_what_the_install_writes(tmp_path):
    """Set equality, derived from install.sh, so neither direction can drift.

    A subset check in one direction misses a forgotten removal. A subset in the other
    misses a removal of something the install never placed, which is the sharper of the
    two: it takes a file belonging to somebody else.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    installed = _install_targets(out)
    assert len(installed) == 6, installed  # five plists plus the sudoers drop-in
    assert _rm_targets(out) == installed


def test_the_uninstall_deletes_every_label_by_its_own_name(tmp_path):
    """Each plist is named once. Five deletions of one path would satisfy a count."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    deleted = _rm_targets(out)
    for job in cp.all_jobs(_host()):
        assert f"/Library/LaunchDaemons/{job.label}.plist" in deleted, job.label


def test_every_privileged_uninstall_command_runs_under_sudo(tmp_path):
    """Stripping sudo would leave a script that fails on its first step.

    The fake sudo execs what it is given, so a runtime log alone cannot tell the two
    spellings apart. This reads the text instead.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    commands = _commands((out / cp.UNINSTALL_SCRIPT_FILE).read_text())
    privileged = ("launchctl bootout", "rm -f", "pmset repeat cancel")
    for line in commands:
        body = line[2:].lstrip() if line.startswith("  ") else line
        if any(word in body for word in privileged):
            assert body.startswith("sudo "), body
    # And the read-only commands do not, because they need no root.
    assert "pmset -g sched" in commands
    assert not [line for line in commands if line.startswith("sudo pmset -g")]


def test_the_uninstall_deletes_nothing_but_those_six_paths(tmp_path):
    """No recursive delete, no path outside the install's own, however it is spelled.

    Banning the literal ``rm -rf`` catches one spelling. Comparing every delete against
    the derived install set catches the class, including ``-fr``, a variable, or a path
    this test never thought to name.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = (out / cp.UNINSTALL_SCRIPT_FILE).read_text()
    installed = _install_targets(out)
    for line in _commands(script):
        if " rm " not in f" {line} ":
            continue
        assert line.split()[-1] in installed, line
        assert set(line.split()[-2]) <= {"-", "f"}, line  # -f only, never -r
    assert "$" not in "".join(_commands(script)), "no variable reaches a command"


def test_the_uninstall_leaves_the_config_directory_and_its_exclusion(tmp_path):
    """The token outlives the install, so the guard over it outlives the install too.

    Lifting the exclusion would put token.json and config.yaml's secrets on the next
    hourly backup, and a backup that already ran cannot be un-run.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    commands = _commands((out / cp.UNINSTALL_SCRIPT_FILE).read_text())
    excluded = cp.default_config_dir("/Users/someone")
    assert not [line for line in commands if excluded in line], commands
    assert not [line for line in commands if line.startswith("tmutil")], commands
    # The install does place it, so this is a deliberate asymmetry rather than an
    # omission that nobody noticed.
    installed = _commands((out / cp.INSTALL_SCRIPT_FILE).read_text())
    assert [line for line in installed if line.startswith("tmutil addexclusion")]


def test_the_uninstall_never_cancels_every_scheduled_event(tmp_path):
    """``pmset schedule cancelall`` takes events nothing here created. It stays cut."""
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    script = (out / cp.UNINSTALL_SCRIPT_FILE).read_text()
    assert "cancelall" not in script
    assert not [line for line in _commands(script) if "pmset schedule" in line]


def test_the_uninstall_prints_the_schedule_before_it_cancels_the_pair(tmp_path):
    """``pmset repeat cancel`` clears the power-off half too, so the before-shot matters.

    macOS holds one pair of repeating events and offers no way to cancel half of it. A
    repeating sleep the operator set elsewhere goes with the marketlake wake. The only
    thing standing between that and a silent loss is a read-back taken first.
    """
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    commands = _commands((out / cp.UNINSTALL_SCRIPT_FILE).read_text())
    cancel = commands.index("sudo pmset repeat cancel")
    reads = [i for i, line in enumerate(commands) if line == "pmset -g sched"]
    assert [i for i in reads if i < cancel], commands
    assert [i for i in reads if i > cancel], commands


# pmset carries its own return code, so a failure can be injected partway down the
# uninstall rather than only at the first bootout.
_FAKE_PMSET = """#!/bin/bash
printf 'pmset %s\\n' "$*" >> "$LOG"
[[ "$1" == "repeat" ]] && exit "$PMSET_RC"
exit 0
"""


def _run_uninstall(tmp_path: Path, *, loaded: bool, pmset_rc: int = 0):
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    loaded_dir = tmp_path / "loaded"
    loaded_dir.mkdir(exist_ok=True)
    if loaded:
        for job in cp.all_jobs(_host()):
            (loaded_dir / job.label).touch()
    log = tmp_path / "log"
    log.write_text("")
    (bin_dir / "rm").write_text(_FAKE.format(body="exit 0"))
    (bin_dir / "rm").chmod(0o755)
    (bin_dir / "pmset").write_text(_FAKE_PMSET)
    (bin_dir / "pmset").chmod(0o755)
    (bin_dir / "sudo").write_text(_FAKE_SUDO)
    (bin_dir / "sudo").chmod(0o755)
    (bin_dir / "launchctl").write_text(_FAKE_LAUNCHCTL)
    (bin_dir / "launchctl").chmod(0o755)
    proc = subprocess.run(
        [str(out / cp.UNINSTALL_SCRIPT_FILE)],
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "LOG": str(log),
            "LOADED": str(loaded_dir),
            "BOOTOUT_RC": "0",
            "PMSET_RC": str(pmset_rc),
        },
        capture_output=True,
        text=True,
    )
    return proc, [line for line in log.read_text().splitlines() if line]


def test_the_uninstall_runs_the_install_backwards(tmp_path):
    """Reverse order, asserted by running it, because the header claims it in those words.

    The install writes plists, then the drop-in, then the wake, then the exclusion, then
    bootstraps. Undoing it in reverse puts the bootout first and the plists last. The
    bootout leading is the load-bearing half: a plist deleted under a loaded label leaves
    launchd holding a definition whose file is gone.
    """
    proc, log = _run_uninstall(tmp_path, loaded=True)
    assert proc.returncode == 0, proc.stderr
    boots = [i for i, line in enumerate(log) if line.startswith("launchctl bootout")]
    wake = [i for i, line in enumerate(log) if line == "pmset repeat cancel"]
    dropin = [i for i, line in enumerate(log) if line == "rm -f /etc/sudoers.d/marketlake"]
    plists = [i for i, line in enumerate(log) if line.startswith("rm -f /Library/LaunchDaemons")]
    assert len(boots) == 5 and len(wake) == 1 and len(dropin) == 1 and len(plists) == 5, log
    assert max(boots) < wake[0] < dropin[0] < min(plists), log


def test_the_uninstall_converges_from_a_partial_install(tmp_path):
    """Nothing loaded is not a failure, so a half-finished install can still be undone."""
    proc, log = _run_uninstall(tmp_path, loaded=False)
    assert proc.returncode == 0, proc.stderr
    assert not [line for line in log if line.startswith("launchctl bootout")], log
    assert len([line for line in log if line.startswith("rm -f /Library/LaunchDaemons")]) == 5
    assert any(line == "pmset repeat cancel" for line in log), log
    assert any(line == "rm -f /etc/sudoers.d/marketlake" for line in log), log


def test_the_uninstall_stops_at_a_failure_partway_down(tmp_path):
    """``set -e`` has to hold for every step, not only the guarded bootout.

    A failing wake cancel means the machine still wakes at 08:25. Carrying on would
    delete the plists anyway, leaving a machine that wakes every weekday for a daemon
    that is no longer there and no longer says so.
    """
    proc, log = _run_uninstall(tmp_path, loaded=True, pmset_rc=1)
    assert proc.returncode != 0
    assert any(line == "pmset repeat cancel" for line in log), log
    assert not [line for line in log if line.startswith("rm -f")], log


def test_the_written_uninstall_script_is_executable(tmp_path):
    out = tmp_path / "out"
    assert cp.main(["render", "--out", str(out), *RENDER_ARGS]) == 0
    assert (out / cp.UNINSTALL_SCRIPT_FILE).stat().st_mode & 0o777 == 0o755


# -- the assertion the self-check now also verifies -------------------------------------

# The pid the daemon stamped for its own child. Every dump below that should read as
# held carries this pid, and every dump that should not carries another.
_DAEMON_PID = 4242

# Trimmed from a real `pmset -g assertions` dump. The tally comes first, then one line
# per owning process, which is the half that names who is holding what.
_HELD = """Assertion status system-wide:
   PreventUserIdleDisplaySleep    0
   PreventUserIdleSystemSleep     1
   PreventSystemSleep             0
Listed by owning process:
   pid 4242(caffeinate): [0x0000f0a] 09:59:58 PreventUserIdleSystemSleep named: ""
"""

# The shape that matters most: something holds idle sleep off, and it is not the daemon.
# The tally reads 1 either way, which is why the tally is the wrong line to read.
_HELD_BY_SOMETHING_ELSE = """Assertion status system-wide:
   PreventUserIdleSystemSleep     1
Listed by owning process:
   pid 991(Music): [0x0000abc] 00:31:02 PreventUserIdleSystemSleep named: "playing"
"""

_NOT_HELD = """Assertion status system-wide:
   PreventUserIdleSystemSleep     0
Listed by owning process:
   pid 991(Safari): [0x0000abc] 00:02:00 PreventUserIdleDisplaySleep named: "video"
"""


def test_a_caffeinate_assertion_is_recognised():
    assert cp.parse_pmset_assertions(_HELD, pid=_DAEMON_PID)


def test_a_caffeinate_under_another_pid_is_not_the_daemons():
    """The whole of #111. A hand-run ``caffeinate -i`` is a real holder and not ours.

    The dump here is the held one verbatim, so nothing about the assertion differs. Only
    the pid the check was told to look for does, and that is enough to refuse it. An
    operator who ran ``caffeinate -i`` the night before leaves exactly this: a genuine
    ``PreventUserIdleSystemSleep`` under a pid the daemon never spawned, whose timer
    says nothing about 15:59.
    """
    assert not cp.parse_pmset_assertions(_HELD, pid=9999)


def test_a_pid_with_no_caffeinate_on_it_is_not_held():
    """A stale stamp names a process that is gone, and the check must say so.

    A daemon that died overnight leaves its last pid in the journal. The dump the next
    morning carries no line for it, so there is nothing to match and nothing to vouch
    for the machine.
    """
    dump = """Assertion status system-wide:
   PreventUserIdleSystemSleep     1
Listed by owning process:
   pid 991(Music): [0x0000abc] 00:31:02 PreventUserIdleSystemSleep named: "playing"
"""

    assert not cp.parse_pmset_assertions(dump, pid=_DAEMON_PID)


def test_the_owner_field_is_read_rather_than_the_whole_line():
    """``named:`` is free text, so the pid shape written there must not pass for the owner.

    The match is anchored at the start of the line, where ``pmset`` prints the owner. A
    substring search over the line would let any process claim the daemon's identity by
    naming its assertion after it, which is the same spoof the process-name test below
    refuses, one field further in.
    """
    dump = """Assertion status system-wide:
   PreventUserIdleSystemSleep     1
Listed by owning process:
   pid 991(Music): [0x0abc] 00:31:02 PreventUserIdleSystemSleep named: "pid 4242(caffeinate):"
"""

    assert not cp.parse_pmset_assertions(dump, pid=_DAEMON_PID)


def test_a_pid_that_is_a_prefix_of_another_does_not_match_it():
    """424 and 4242 are different processes, and a loose match would conflate them.

    The owner field is read whole rather than searched for, so a stamped pid only ever
    matches the process that actually owns the line.
    """
    dump = """Assertion status system-wide:
   PreventUserIdleSystemSleep     1
Listed by owning process:
   pid 4242(caffeinate): [0x0000f0a] 09:59:58 PreventUserIdleSystemSleep named: ""
"""

    assert not cp.parse_pmset_assertions(dump, pid=424)
    assert not cp.parse_pmset_assertions(dump, pid=42420)


def test_an_assertion_held_by_another_process_does_not_count():
    """The tally says the machine is safe this minute and nothing about the window.

    A video player's assertion ends when the video does. The daemon's ``caffeinate``
    carries a timer to the window's end, so it is the only holder whose presence at 08:30
    says anything about 15:59. Reading the system-wide tally would accept either.
    """
    assert not cp.parse_pmset_assertions(_HELD_BY_SOMETHING_ELSE, pid=991)


def test_a_caffeinate_holding_only_display_sleep_does_not_count():
    """``caffeinate`` has flags, and only one of them puts the kind on its own line.

    The daemon spawns ``-i``, and that is what makes its child's line read
    ``PreventUserIdleSystemSleep``. A ``-d`` at the same pid prints
    ``PreventUserIdleDisplaySleep`` there. A ``-d`` does keep the machine awake, because
    macOS holds system sleep off while the display is on, but it does that on a separate
    ``powerd`` line at ``powerd``'s pid. So the daemon's own line is the only evidence
    this reads, and a display-only holder at that pid is not it.
    """
    dump = """Assertion status system-wide:
   PreventUserIdleDisplaySleep    1
   PreventUserIdleSystemSleep     0
Listed by owning process:
   pid 4242(caffeinate): [0x0000f0a] 09:59:58 PreventUserIdleDisplaySleep named: ""
"""

    assert not cp.parse_pmset_assertions(dump, pid=_DAEMON_PID)


def test_no_assertion_at_all_is_not_held():
    assert not cp.parse_pmset_assertions(_NOT_HELD, pid=991)


def test_an_empty_dump_is_not_held():
    assert not cp.parse_pmset_assertions("", pid=_DAEMON_PID)


def test_a_daemon_up_without_its_assertion_does_not_ping(tmp_path):
    """Up is not the same as awake, and only the ping says which one the check saw.

    The ping means the machine is awake and will stay awake. A daemon whose caffeinate
    never started reports healthy on every other signal right up to the moment the
    machine sleeps through the open, so the check has to withhold the ping and let
    healthchecks page an hour before the bell.
    """
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: False,
        assertion_pid=_DAEMON_PID,
        now=et(2026, 9, 2, 8, 30),
    )

    assert outcome.daemon_up is True
    assert outcome.assertion_held is False
    assert outcome.pinged is False, "the check pinged for a machine that may sleep"
    assert outcome.problem == f"no caffeinate assertion held by pid {_DAEMON_PID}"
    assert pinger.urls == [], "a ping left despite the missing assertion"


def test_the_check_asks_about_the_pid_the_daemon_stamped(tmp_path):
    """The stamp is the only thing that makes this an answer about the daemon.

    A check that asked the probe about anything else, or about nothing, would be back to
    accepting any holder on the machine. The pid handed to the probe is what says which
    ``caffeinate`` was looked for.
    """
    asked: list[int] = []
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: asked.append(pid) or True,
        assertion_pid=_DAEMON_PID,
        now=et(2026, 9, 2, 8, 30),
    )

    assert asked == [_DAEMON_PID], "the probe was asked about the wrong process"
    assert outcome.pinged is True


def test_a_daemon_up_with_no_recorded_pid_does_not_ping(tmp_path):
    """No stamp is no confirmation, and the check reports that rather than assuming.

    Two machines leave no pid inside an open window: one whose daemon predates the stamp,
    and one whose ``caffeinate`` would not spawn. Neither can be vouched for, and the
    green this used to print was the vouching. The price is a page on the first morning
    after an upgrade that did not restart the daemon, which the restart script fixes.

    Nothing is stamped at all here, so the words are the ones for a stamp that is not
    being written. The sibling below covers the other half.
    """
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        now=et(2026, 9, 2, 8, 30),
    )

    assert outcome.daemon_up is True
    assert outcome.assertion_held is False, "an unconfirmed machine read as confirmed"
    assert outcome.pinged is False, "the check pinged for a machine nothing vouches for"
    assert outcome.problem == f"no caffeinate pid recorded. {cp.NO_PID_NO_STAMP}"
    assert pinger.urls == []


def test_a_fresh_stamp_with_no_pid_names_the_daemon_running_old_code(tmp_path):
    """The deploy that moved the scheduled job and left the resident behind.

    A resident daemon loads its Python once and keeps it, while this check is a calendar
    job that execs fresh every run. So the morning after a pull, a check that reads the
    pid runs against a daemon from before the pid was stamped, and the pid is missing on
    a machine that is otherwise entirely healthy.

    The check still fails, which is right: the deploy really is half-done. What the words
    have to do is send the operator to the resident rather than to power assertions, and
    the fresh stamp beside the missing pid is what says which of the two it is. The
    daemon writes that stamp every minute, and has since long before the pid field
    existed, so an old daemon writes it too.
    """
    now = et(2026, 9, 2, 8, 30)
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=now - timedelta(seconds=30),
        now=now,
    )

    assert outcome.pinged is False, "a half-done deploy pinged as healthy"
    assert outcome.problem == f"no caffeinate pid recorded. {cp.NO_PID_FRESH_STAMP}"
    assert "Restart it" in outcome.problem
    assert pinger.urls == []


def test_a_fresh_stamp_with_no_pid_also_names_a_caffeinate_that_would_not_spawn(tmp_path):
    """The deploy is not the only daemon that stamps an instant and no pid.

    ``AssertionHolder.child_pid`` answers ``None`` for a spawn that failed and for a
    re-take that failed, and the daemon stamps that ``None`` on the same tick the idle
    heartbeat writes the instant. So a current, correctly deployed daemon whose
    ``caffeinate`` would not start leaves the identical signature. A page that read the
    fresh stamp as the deploy alone would send an operator to restart a daemon that is
    already running the right code, while the actual failure sat in its log.

    Both states are named, and the restart is offered because it repairs one of them and
    costs nothing on the other at this hour, which is an hour before the open.
    """
    now = et(2026, 9, 2, 8, 30)

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=now - timedelta(seconds=30),
        now=now,
    )

    assert "caffeinate would not spawn" in outcome.problem, (
        "a current daemon with a failed spawn was reported as running old code"
    )
    assert "read its log" in outcome.problem


def test_the_freshness_span_is_two_minutes(tmp_path):
    """The span is pinned against the clock, not against itself.

    The staleness test below writes its input as ``now - STAMP_FRESH_WITHIN - 1s``, so it
    moves with the constant and can never disagree with it. Widening the constant to a
    day would leave that test green while a stamp from a daemon dead since yesterday read
    as one writing this minute. These two cases are absolute, so they fail when the span
    moves.
    """
    assert cp.STAMP_FRESH_WITHIN == timedelta(minutes=2)
    now = et(2026, 9, 2, 8, 30)

    def reason_at(age: timedelta) -> str:
        return cp.self_check(
            probe=lambda label: True,
            pinger=FakePinger(),
            ping_url="https://example.invalid/ping",
            assertion_probe=lambda pid: True,
            assertion_pid=None,
            stamped_at=now - age,
            now=now,
        ).problem

    assert cp.NO_PID_FRESH_STAMP in reason_at(timedelta(minutes=1))
    assert cp.NO_PID_NO_STAMP in reason_at(timedelta(minutes=5))


def test_a_stamp_exactly_at_the_span_is_still_fresh(tmp_path):
    """The bound is inclusive, and the edge is where an off-by-one lives."""
    now = et(2026, 9, 2, 8, 30)

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=now - cp.STAMP_FRESH_WITHIN,
        now=now,
    )

    assert cp.NO_PID_FRESH_STAMP in outcome.problem


def test_a_stamp_ahead_of_now_reads_as_fresh(tmp_path):
    """A deliberate decision in ``_no_pid_reason``, so something has to hold it.

    A stamp dated after the moment the check runs is a clock that moved backwards, not a
    daemon that went quiet. Something is writing, so the fresh reading is the honest one.
    A later reader adding a plausible-looking lower bound would reverse that silently.
    """
    now = et(2026, 9, 2, 8, 30)

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=now + timedelta(minutes=5),
        now=now,
    )

    assert cp.NO_PID_FRESH_STAMP in outcome.problem


def test_a_stamp_older_than_the_freshness_span_is_not_read_as_old_code(tmp_path):
    """A daemon that stopped writing is a different repair, so it gets different words.

    The fresh stamp is the whole discriminator. Without a bound on it, a stamp left hours
    ago by a daemon that has since died would read as a resident happily stamping away,
    and the page would send an operator to restart something that is already gone.
    """
    now = et(2026, 9, 2, 8, 30)

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=None,
        stamped_at=now - cp.STAMP_FRESH_WITHIN - timedelta(seconds=1),
        now=now,
    )

    assert outcome.problem == f"no caffeinate pid recorded. {cp.NO_PID_NO_STAMP}"


def test_a_daemon_up_with_its_assertion_pings(tmp_path):
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: True,
        assertion_pid=_DAEMON_PID,
        now=et(2026, 9, 2, 8, 30),
    )

    assert outcome.pinged is True and outcome.assertion_held is True
    assert pinger.urls == ["https://example.invalid/ping"]


def test_the_self_check_cli_asks_about_the_assertion(tmp_path, capsys, monkeypatch):
    """The production entry has to pass the probe, or the check never asks at all.

    ``self_check`` skips the question when no probe is handed in, which is right for a
    caller that has no way to ask. That default is also exactly what an unwired ``main``
    would look like, and it would look healthy every morning.
    """
    lake_root = tmp_path / "lake"
    config = write_config(tmp_path, lake_root)
    pinger = FakePinger()
    stamp_assertion_pid(lake_root, pid=_DAEMON_PID)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: False)
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=et(2026, 9, 2, 8, 30)))

    code = cp.main(["self-check", "--config", str(config)])

    assert code == 1, "the check passed a daemon holding no assertion"
    assert pinger.urls == [], "it pinged anyway"
    assert f"no caffeinate assertion held by pid {_DAEMON_PID}" in capsys.readouterr().out


def test_the_self_check_cli_reads_the_pid_from_the_journal_stamp(tmp_path, monkeypatch):
    """The production entry has to find the stamp, or the probe is asked about nothing.

    ``self_check`` fails closed on a missing pid, so an unwired ``main`` would page every
    weekday morning on a healthy machine. The opposite wiring mistake, a pid read from
    somewhere the daemon never writes, looks the same. Only the value reaching the probe
    tells them apart.
    """
    lake_root = tmp_path / "lake"
    config = write_config(tmp_path, lake_root)
    asked: list[int] = []
    stamp_assertion_pid(lake_root, pid=7331)
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: asked.append(pid) or True)
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=et(2026, 9, 2, 8, 30)))

    assert cp.main(["self-check", "--config", str(config)]) == 0
    assert asked == [7331], "the CLI did not carry the stamped pid to the probe"


def test_the_self_check_cli_fails_a_lake_with_no_stamped_pid(tmp_path, capsys, monkeypatch):
    """Every lake written before the stamp shipped looks like this, and so does a
    daemon whose ``caffeinate`` never started. The check says it could not confirm.
    """
    config = write_config(tmp_path, tmp_path / "lake")
    pinger = FakePinger()
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", lambda: pinger)
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=et(2026, 9, 2, 8, 30)))

    code = cp.main(["self-check", "--config", str(config)])

    assert code == 1, "an unconfirmable machine passed the check"
    assert pinger.urls == [], "it pinged for a machine nothing vouches for"
    out = capsys.readouterr().out
    assert "no caffeinate pid recorded" in out
    # Nothing has ever been stamped into this lake, so the words are the ones for a
    # stamp that is not being written rather than the ones for a stale resident.
    assert cp.NO_PID_NO_STAMP in out


def test_the_self_check_cli_reads_the_stamp_instant_as_well_as_the_pid(
    tmp_path, capsys, monkeypatch
):
    """The extra field has to come off the real stamp, or the CLI prints the wrong page.

    ``self_check`` defaults ``stamped_at`` to ``None``, which is what an unwired entry
    would pass, and ``None`` reads as a stamp nobody is writing. So a CLI that read the
    pid alone would answer every half-done deploy with the words for a dead writer, and
    look correct from inside ``self_check``'s own tests while doing it. Only a run of
    ``main`` against a lake carrying a fresh stamp and no pid tells the two apart.
    """
    lake_root = tmp_path / "lake"
    config = write_config(tmp_path, lake_root)
    now = et(2026, 9, 2, 8, 30)
    # What a daemon from before the pid field leaves behind: the idle heartbeat's own
    # stamp, written this minute, and no pid beside it.
    stamp_cycle(
        lake_root,
        at=now - timedelta(seconds=30),
        token_minted_at=now - timedelta(days=1),
        roster=Roster(()),
    )
    monkeypatch.setattr(cp, "launchctl_probe", lambda label: True)
    monkeypatch.setattr(cp, "pmset_assertions_probe", lambda pid: True)
    monkeypatch.setattr(cp, "UrllibPinger", FakePinger)
    monkeypatch.setattr(cp, "_system_clock", lambda: ManualClock(start=now))

    reads: list[Path] = []
    real_read = cp.read_metadata
    monkeypatch.setattr(cp, "read_metadata", lambda root: reads.append(root) or real_read(root))

    assert cp.main(["self-check", "--config", str(config)]) == 1
    out = capsys.readouterr().out
    assert cp.NO_PID_FRESH_STAMP in out, "the CLI did not carry the stamp instant"
    assert cp.NO_PID_NO_STAMP not in out
    # One reading, not one per field. The daemon restamps the pid whenever it re-takes a
    # lost caffeinate, so two reads can straddle a restamp and pair a pid with an instant
    # that never stood beside it.
    assert len(reads) == 1, "the CLI read the stamp once per field"


def test_no_assertion_is_owed_outside_a_window_so_none_is_demanded(tmp_path):
    """A check that demanded one every hour would fail on the hour it arms itself.

    ``RunAtLoad`` runs the self-check once when the jobs are bootstrapped, and an install
    on a Saturday afternoon sits in no window at all. Nothing is holding the machine awake
    then and nothing should be, so a check that asked anyway would print a failure on a
    healthy machine and withhold the very first ping, the one that takes the ``pre-open``
    row out of the never-pinged state where it can never page.
    """
    pinger = FakePinger()

    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=pinger,
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: False,
        # No pid either, which is what the daemon stamps outside a window. The
        # fail-closed rule is gated on being owed one, so it must not reach here.
        assertion_pid=None,
        # Saturday. ``assertion_window`` answers ``None``, so nothing is owed.
        now=et(2026, 9, 5, 12, 0),
    )

    assert outcome.pinged is True, "the check failed where no assertion was owed"
    assert outcome.assertion_held is None, "it claimed an answer it never asked for"


def test_a_check_with_no_clock_asks_nothing(tmp_path):
    """No moment means no window to judge against, so the question is not asked."""
    outcome = cp.self_check(
        probe=lambda label: True,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda pid: False,
    )

    assert outcome.pinged is True
    assert outcome.assertion_held is None


# -- the probe itself, which was only ever replaced and never run -----------------------


class _FakeRun:
    """Stands in for ``subprocess.run``, recording argv and answering a fixed result."""

    def __init__(self, returncode: int, stdout: str) -> None:
        self.calls: list[list[str]] = []
        self._result = SimpleNamespace(returncode=returncode, stdout=stdout)

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        return self._result


def test_the_probe_asks_pmset_for_assertions(monkeypatch):
    """The production command is pinned, because nothing else in the suite runs it.

    Every other test replaces this function wholesale, so its body was free to ask any
    tool anything, or to answer a constant, with the whole suite green. That is the shape
    where a feature ships as a no-op.
    """
    run = _FakeRun(0, _HELD)
    monkeypatch.setattr(subprocess, "run", run)

    assert cp.pmset_assertions_probe(_DAEMON_PID) is True
    assert run.calls == [["pmset", "-g", "assertions"]]


def test_the_probe_carries_the_pid_into_the_parse(monkeypatch):
    """The command is fixed, so the pid can only reach the answer through the parse.

    A probe that took the pid and dropped it would ask ``pmset`` the same question and
    answer the old, undiscriminating one, with the command assertion above still green.
    """
    run = _FakeRun(0, _HELD)
    monkeypatch.setattr(subprocess, "run", run)

    assert cp.pmset_assertions_probe(9999) is False


def test_a_pmset_that_exits_non_zero_is_not_an_assertion_held(monkeypatch):
    """A dump that would parse as held still counts for nothing behind a failed exit.

    The stdout here is the held dump verbatim, so the only thing deciding the answer is
    the return code. A `pmset` that failed may have printed a partial or stale dump, and
    reading it would answer a question it was not able to ask.
    """
    run = _FakeRun(1, _HELD)
    monkeypatch.setattr(subprocess, "run", run)

    assert cp.pmset_assertions_probe(_DAEMON_PID) is False


def test_an_assertion_named_after_caffeinate_is_not_caffeinate_holding_it():
    """The owner field is the claim, and ``named:`` is free text anyone can write.

    Matching the bare word rather than the ``pid N(caffeinate)`` shape would let any
    process pass by calling its assertion caffeinate, which is a false green on the one
    check standing between a sleeping machine and a lost session.
    """
    dump = """Assertion status system-wide:
   PreventUserIdleSystemSleep     1
Listed by owning process:
   pid 991(Music): [0x0000abc] 00:31:02 PreventUserIdleSystemSleep named: "caffeinate"
"""

    assert not cp.parse_pmset_assertions(dump, pid=991)


def test_a_down_daemon_is_reported_as_down_even_with_no_assertion(tmp_path):
    """Both are wrong at once on the morning that matters, and only one is the cause.

    A machine that failed to wake has no daemon and no assertion. Asking about the
    assertion first would answer "no caffeinate assertion held" and print "daemon up",
    sending the operator after the assertion on a morning when the wake is what failed.
    """
    outcome = cp.self_check(
        probe=lambda label: False,
        pinger=FakePinger(),
        ping_url="https://example.invalid/ping",
        assertion_probe=lambda: False,
        now=et(2026, 9, 2, 8, 30),
    )

    assert outcome.daemon_up is False, "a down daemon reported as up"
    assert outcome.problem is None, "it named the assertion for a daemon that is not there"
    assert outcome.assertion_held is None, "it asked about an assertion before the daemon"
