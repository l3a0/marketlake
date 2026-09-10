"""Config loaded from a real YAML file on disk, with env-var and argument overrides."""

from __future__ import annotations

import traceback
from pathlib import Path

import pytest

from lake.config import ConfigError, input_errors_exit, load_config

YAML = """\
lake_root: {root}
backup_target: {backup}
healthchecks_ping_key: PINGKEY
ntfy_topic: mytopic
schwab_api_key: SCHWABKEY
schwab_app_secret: SCHWABSECRET
guards:
  watchdog_page_minutes: 4
"""


def _write(path: Path, *, root: str = "/data/lake", backup: str = "/Volumes/ssd") -> Path:
    path.write_text(YAML.format(root=root, backup=backup))
    return path


def test_load_config_reads_a_file(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    assert cfg.lake_root == Path("/data/lake")
    assert cfg.backup_target == Path("/Volumes/ssd")
    assert cfg.healthchecks_ping_key.reveal() == "PINGKEY"
    assert cfg.guards.watchdog_page_minutes == 4


def test_env_var_points_the_loader_at_a_file(tmp_path: Path):
    cfg_file = _write(tmp_path / "elsewhere.yaml")
    cfg = load_config(env={"MARKETLAKE_CONFIG": str(cfg_file)})
    assert cfg.backup_target == Path("/Volumes/ssd")


def test_a_typed_tilde_still_expands(tmp_path: Path, monkeypatch):
    # An argument and an environment override are whatever a person typed, so both
    # expand. Only the default comes from lake.paths already resolved. Removing the
    # expansion here would make the loader open a literal "~" directory.
    monkeypatch.setenv("HOME", str(tmp_path))
    _write(tmp_path / "typed.yaml")
    assert load_config("~/typed.yaml").backup_target == Path("/Volumes/ssd")
    assert load_config(env={"MARKETLAKE_CONFIG": "~/typed.yaml"}).backup_target == Path(
        "/Volumes/ssd"
    )


def test_explicit_argument_beats_the_env_var(tmp_path: Path):
    chosen = _write(tmp_path / "chosen.yaml", root="/data/chosen")
    ignored = _write(tmp_path / "ignored.yaml", root="/data/ignored")
    cfg = load_config(chosen, env={"MARKETLAKE_CONFIG": str(ignored)})
    assert cfg.lake_root == Path("/data/chosen")


def test_malformed_yaml_names_the_file_and_never_the_secret(tmp_path: Path):
    # PyYAML quotes the offending line back in its message, and four of this file's
    # values are secrets. Jobs run from launchd with stderr going to a log file, so an
    # uncaught traceback would write the ping key to disk.
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "lake_root: /data/lake\n"
        "backup_target: /Volumes/ssd\n"
        'healthchecks_ping_key: "SUPERSECRET-abc123\n'
        "ntfy_topic: topic\n"
    )
    with pytest.raises(ConfigError) as excinfo:
        load_config(bad)
    assert str(excinfo.value) == f"config file is not valid YAML: {bad}"
    # `from None` drops the original, so the quoted line cannot ride the traceback.
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
    rendered = "".join(traceback.format_exception(excinfo.value))
    assert "SUPERSECRET" not in rendered


def test_missing_file_raises(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.yaml")


def test_schwab_credentials_load_from_the_file(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    assert cfg.schwab_api_key.reveal() == "SCHWABKEY"
    assert cfg.schwab_app_secret.reveal() == "SCHWABSECRET"


def test_secret_stays_out_of_repr_after_a_file_load(tmp_path: Path):
    cfg = load_config(_write(tmp_path / "config.yaml"))
    for secret_value in ("PINGKEY", "mytopic", "SCHWABKEY", "SCHWABSECRET"):
        assert secret_value not in repr(cfg)


# -- the command-line entries ----------------------------------------------------------


def test_input_errors_exit_prints_one_line_and_exits_two(capsys):
    # argparse already exits 2 with one line for a bad argument in these same entries.
    # A bad config is the same kind of operator mistake, so it reads the same way.
    with pytest.raises(SystemExit) as excinfo:
        with input_errors_exit("thing"):
            raise ConfigError("config file not found: /nope/config.yaml")
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.err == "thing: config file not found: /nope/config.yaml\n"
    assert captured.out == ""


def test_input_errors_exit_lets_every_other_exception_through():
    # It converts one kind of failure. A bug still raises, with its traceback intact.
    with pytest.raises(ZeroDivisionError):
        with input_errors_exit("thing"):
            raise ZeroDivisionError("a real bug")


@pytest.mark.parametrize(
    ("module", "argv"),
    [
        ("lake.compact", ["--config", "MISSING"]),
        ("lake.measure", ["SPY", "2026-01-02", "--config", "MISSING"]),
        ("lake.onboard", ["SPY", "--config", "MISSING"]),
        ("lake.runner", ["run", "--config", "MISSING"]),
        ("lake.control_plane", ["self-check", "--config", "MISSING"]),
        ("lake.daemon", ["--config", "MISSING"]),
        ("lake.control_plane", ["sunday", "--config", "MISSING"]),
    ],
)
def test_a_missing_config_names_itself_at_every_cli_entry(module, argv, tmp_path, capsys):
    # The class, not one entry. Each of these loads the config somewhere below `main`,
    # two of them inside a library helper a long-running caller also uses.
    import importlib

    missing = str(tmp_path / "nope.yaml")
    entry = importlib.import_module(module)
    with pytest.raises(SystemExit) as excinfo:
        entry.main([arg if arg != "MISSING" else missing for arg in argv])
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    # The control plane names its subcommand rather than its module, because one
    # module carries four of them and "control_plane:" would not say which failed.
    expected = argv[0] if module == "lake.control_plane" else module.removeprefix("lake.")
    assert err.startswith(f"{expected}: config file not found:")


@pytest.mark.parametrize("module", ["lake.probe", "lake.record"])
def test_a_missing_config_names_itself_at_the_env_driven_entries(
    module, tmp_path, monkeypatch, capsys
):
    # These two take no --config. They resolve through MARKETLAKE_CONFIG.
    import importlib

    monkeypatch.setenv("MARKETLAKE_CONFIG", str(tmp_path / "nope.yaml"))
    entry = importlib.import_module(module)
    argv = [] if module == "lake.probe" else ["--out", str(tmp_path / "out")]
    with pytest.raises(SystemExit) as excinfo:
        entry.main(argv)
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith(f"{module.removeprefix('lake.')}: config file not found:")


def test_a_malformed_config_names_itself_rather_than_tracing_back(tmp_path, capsys):
    # Not just a missing file. A file that is not a mapping is the same operator
    # mistake, and the message must still not carry the file's contents.
    import lake.compact as compact

    bad = tmp_path / "config.yaml"
    bad.write_text("just a string\n")
    with pytest.raises(SystemExit) as excinfo:
        compact.main(["--config", str(bad)])
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert err == f"compact: config file is not a mapping: {bad}\n"


@pytest.mark.parametrize(
    ("kind", "write"),
    [
        ("a directory", lambda p: p.mkdir()),
        ("a binary file", lambda p: p.write_bytes(b"\x80\x81\x82")),
    ],
)
def test_a_config_that_cannot_be_read_names_itself(kind, write, tmp_path, capsys):
    # `exists()` passing does not mean the file can be read. A --config one character
    # short of the file names its directory, which is the plausible typo.
    import lake.compact as compact

    target = tmp_path / "config.yaml"
    write(target)
    with pytest.raises(SystemExit) as excinfo:
        compact.main(["--config", str(target)])
    assert excinfo.value.code == 2
    assert capsys.readouterr().err == f"compact: config file cannot be read: {target}\n"


def test_a_bad_tickers_file_names_itself_at_the_same_entries(tmp_path, capsys):
    # The sibling class. config.yaml, tickers.yaml, and chain_plan.json are all the
    # operator's to edit, and all three sit in the config directory, so one bad file
    # must not read differently from another.
    import lake.runner as runner

    config = _write(tmp_path / "config.yaml")
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("just a string\n")
    with pytest.raises(SystemExit) as excinfo:
        runner.main(["run", "--config", str(config), "--tickers", str(tickers)])
    assert excinfo.value.code == 2
    assert capsys.readouterr().err == f"runner: tickers file is not a mapping: {tickers}\n"


def test_a_roster_caught_mid_save_names_itself_the_same_way(tmp_path, capsys):
    # The same sibling class, for the way the file actually goes bad. A hand edit caught
    # part way through a save is a parse error. Unguarded that prints a raw
    # yaml.ParserError traceback, where a bad config exits 2 on one line.
    import lake.runner as runner

    config = _write(tmp_path / "config.yaml")
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("SPY: {options: fal")
    with pytest.raises(SystemExit) as excinfo:
        runner.main(["run", "--config", str(config), "--tickers", str(tickers)])
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    # One line, as the guard promises for every operator file.
    assert err.count("\n") == 1
    assert err == f"runner: tickers file is not valid YAML at line 1: {tickers}\n"


def test_the_guard_names_the_three_operator_files_and_nothing_else():
    # It converts operator mistakes. A bug still raises, with its traceback intact.
    from lake.chain_plan import ChainPlanError
    from lake.tickers import TickersError

    for error in (
        ChainPlanError("bad plan"),
        ConfigError("bad config"),
        TickersError("bad roster"),
    ):
        with pytest.raises(SystemExit):
            with input_errors_exit("thing"):
                raise error
    with pytest.raises(ZeroDivisionError):
        with input_errors_exit("thing"):
            raise ZeroDivisionError("a real bug")
