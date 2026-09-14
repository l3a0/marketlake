"""The re-auth command: the two refusals, the atomic write, and what it prints.

Nothing here reaches Schwab. The login flow is a parameter, so every test passes its own
and the real ``schwab-py`` import in ``_schwab_login_flow`` is never reached. The
component test beside this one drives the real ``main`` with a fake ``schwab`` package in
``sys.modules``, so the module's own entry is executed rather than only replaced.

The three properties worth stating up front, because each is a thing the code could lose
with every other test still green.

1. The flow is not called when stdin is not a terminal. Asserting only that a refusal is
   raised would pass with the refusal moved after the call, which is the five-minute
   launchd hang the guard exists to stop.
2. The write publishes by rename. Asserting only that the token lands would pass with a
   plain truncating write, which is exactly what ``schwab-py`` does and what this module
   exists to replace.
3. No secret is printed. The api key and the app secret pass through this module, so the
   report is checked against both rather than eyeballed.
"""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path

import pytest

from lake import reauth as m
from lake.paths import temp_write_path

# Stand-ins, chosen so a leak is greppable. None is a real value and none ever reaches a
# tracked file beyond this one, which is the point of picking them rather than quoting a
# registration.
API_KEY = "FAKE-API-KEY"
APP_SECRET = "FAKE-APP-SECRET"
CALLBACK = "https://127.0.0.1:8182"

# The shape ``schwab-py`` hands a token writer: the token wrapped in its metadata
# envelope, with the mint epoch the seven-day clock is read off later.
FRESH_TOKEN = {
    "creation_timestamp": 1787529900,
    "token": {"refresh_token": "fake-refresh-token"},
}
OLD_TOKEN = {
    "creation_timestamp": 1786925100,
    "token": {"refresh_token": "fake-previous-token"},
}


class RecordingFlow:
    """A stand-in for ``client_from_login_flow``. It records and optionally writes.

    ``token`` is what it hands the writer, standing for a login that succeeded. Left
    ``None`` it writes nothing, standing for a flow that returned without a token.
    """

    def __init__(self, token: object | None = FRESH_TOKEN) -> None:
        self.calls: list[dict[str, object]] = []
        self.writers: list[object] = []
        self._token = token

    def __call__(
        self,
        api_key: str,
        app_secret: str,
        callback_url: str,
        token_path: str,
        *,
        token_write_func,
    ) -> object:
        self.calls.append(
            {
                "api_key": api_key,
                "app_secret": app_secret,
                "callback_url": callback_url,
                "token_path": token_path,
            }
        )
        self.writers.append(token_write_func)
        if self._token is not None:
            token_write_func(self._token)
        return "a client this command discards"


def _run(flow: RecordingFlow, token_path: Path, **overrides) -> m.ReauthReport:
    """Drive ``reauth`` with the happy-path arguments, overriding what a test varies."""
    kwargs = {
        "api_key": API_KEY,
        "app_secret": APP_SECRET,
        "callback_url": CALLBACK,
        "token_path": token_path,
        "login_flow": flow,
        "stdin_is_tty": True,
    }
    kwargs.update(overrides)
    return m.reauth(**kwargs)


# -- the two refusals ------------------------------------------------------------------


def test_a_non_tty_refuses_before_the_flow_is_called(tmp_path):
    """The enforcement the issue's decision names, and the call count is what holds it.

    ``client_from_login_flow`` waits five minutes for a callback. A launchd job has no
    browser to send one, so a refusal that happened after the call would still take five
    minutes and still read as a broken job. The flow must not run at all.
    """
    flow = RecordingFlow()
    with pytest.raises(m.ReauthError) as excinfo:
        _run(flow, tmp_path / "token.json", stdin_is_tty=False)
    assert flow.calls == []
    assert "terminal" in str(excinfo.value)
    assert not (tmp_path / "token.json").exists()


def test_the_tty_answer_has_no_default(tmp_path):
    """A default would put the refusal one forgotten argument away from being off.

    Deleting the refusal is caught by the test above. Defaulting ``stdin_is_tty`` to
    ``True`` is the other way to lose it, and that one leaves every existing call site
    green, so it is stated here rather than inferred.
    """
    for entry in (m.reauth, m.reauth_from_config):
        parameter = inspect.signature(entry).parameters["stdin_is_tty"]
        assert parameter.default is inspect.Parameter.empty, entry.__name__


@pytest.mark.parametrize("absent", [None, ""])
def test_a_missing_callback_refuses_and_names_the_key(tmp_path, absent):
    """The refusal lives here because the config loader must stay fail-open.

    Capture never reads the callback, so ``load_config`` cannot require it without
    taking the daemon down. This command is where the requirement belongs, and the
    message has to name the key so the operator knows what to add.
    """
    flow = RecordingFlow()
    with pytest.raises(m.ReauthError) as excinfo:
        _run(flow, tmp_path / "token.json", callback_url=absent)
    assert flow.calls == []
    assert m.CALLBACK_KEY in str(excinfo.value)


def test_the_tty_refusal_comes_before_the_callback_check(tmp_path):
    """An unattended run is refused for what it is, whatever else is unset.

    A launchd job that also has no callback configured must be told it cannot run
    unattended, rather than sent off to fix a config key that would not save it.
    """
    with pytest.raises(m.ReauthError) as excinfo:
        _run(RecordingFlow(), tmp_path / "token.json", stdin_is_tty=False, callback_url=None)
    assert "terminal" in str(excinfo.value)
    assert m.CALLBACK_KEY not in str(excinfo.value)


# -- the flow, and what reaches it -----------------------------------------------------


def test_the_flow_gets_the_credentials_the_callback_and_the_token_path(tmp_path):
    token = tmp_path / "token.json"
    report = _run(RecordingFlow(), token)
    assert report.token_written is True
    assert json.loads(token.read_text()) == FRESH_TOKEN


def test_the_flow_is_called_with_exactly_what_it_needs(tmp_path):
    """The four arguments are ``client_from_login_flow``'s own first four, in order."""
    token = tmp_path / "token.json"
    flow = RecordingFlow()
    _run(flow, token)
    assert flow.calls == [
        {
            "api_key": API_KEY,
            "app_secret": APP_SECRET,
            "callback_url": CALLBACK,
            "token_path": str(token),
        }
    ]


def test_the_flow_is_handed_this_modules_atomic_writer(tmp_path, monkeypatch):
    """The hook is the whole point, so the writer that reaches the flow is checked.

    ``client_from_login_flow`` writes the token itself unless it is given a
    ``token_write_func``. Dropping the hook, or handing it a plain write, leaves the
    token landing and every call-shape assertion green while the torn-write risk is back.
    So the writer the flow received is exercised and the rename is what is observed.
    """
    token = tmp_path / "token.json"
    flow = RecordingFlow()
    renames: list[tuple[str, str]] = []
    real_replace = os.replace

    def record(src, dst):
        renames.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", record)
    _run(flow, token)

    assert renames == [(str(temp_write_path(token, os.getpid())), str(token))]
    assert token.stat().st_mode & 0o777 == m.TOKEN_MODE


def test_an_existing_token_is_overwritten_and_the_report_says_so(tmp_path):
    """Overwriting is allowed and wanted, because the write cannot tear.

    It costs one login and mints a fresher token, and the Sunday assertion tests
    freshness rather than validity. A refuse-by-default guard was considered and
    dropped, so a run over a live token must land the new one.
    """
    token = tmp_path / "token.json"
    token.write_text(json.dumps(OLD_TOKEN))
    report = _run(RecordingFlow(), token)
    assert json.loads(token.read_text()) == FRESH_TOKEN
    assert report.replaced_existing is True
    assert "replaced the previous token" in report.render()


def test_a_flow_that_writes_no_token_reports_none_landed(tmp_path):
    """The report states the fact rather than assuming the login worked."""
    report = _run(RecordingFlow(token=None), tmp_path / "token.json")
    assert report.token_written is False
    assert report.replaced_existing is False
    assert "token landed:  no" in report.render()


# -- the atomic write ------------------------------------------------------------------


def test_a_failed_rename_leaves_the_previous_token_untouched(tmp_path, monkeypatch):
    """The property the whole atomic write exists for, exercised by breaking the rename.

    ``schwab-py`` writes with ``open(token_path, 'w')``, which truncates before the new
    contents exist. A plain write here would put the new token at the path before this
    failure could happen, so this test goes red the moment the temp-and-rename is
    replaced by a direct write.
    """
    token = tmp_path / "token.json"
    token.write_text(json.dumps(OLD_TOKEN))

    def boom(src, dst):
        raise OSError("the rename failed")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        m.write_token(token, FRESH_TOKEN)

    assert json.loads(token.read_text()) == OLD_TOKEN
    # The temp file is cleaned up, so a failed re-auth leaves nothing beside the token.
    assert [p.name for p in tmp_path.iterdir()] == ["token.json"]


def test_the_write_publishes_by_renaming_the_paths_temp_file(tmp_path, monkeypatch):
    """The temp name comes from ``paths.temp_write_path``, not a spelling of its own.

    That function owns the one marker the backup exclusion matches, so a temp file named
    any other way would ride into a backup. Recording the rename also states that a
    rename is what publishes: a direct write calls ``os.replace`` never, and this fails.
    """
    token = tmp_path / "token.json"
    renames: list[tuple[str, str]] = []
    real_replace = os.replace

    def record(src, dst):
        renames.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", record)
    m.write_token(token, FRESH_TOKEN)

    assert renames == [(str(temp_write_path(token, os.getpid())), str(token))]


def test_the_token_is_written_owner_only(tmp_path):
    """The design pins ``chmod 600`` on the token. It is a full brokerage credential.

    The mode is set on the temp file before the rename, so the token is never briefly
    world-readable at its real path.
    """
    token = tmp_path / "token.json"
    m.write_token(token, FRESH_TOKEN)
    assert token.stat().st_mode & 0o777 == m.TOKEN_MODE


def test_a_payload_json_cannot_encode_fails_before_the_file_is_touched(tmp_path):
    """Serialising first is what keeps a bad payload from truncating a good token.

    ``json.dump`` into an open file writes what it can and then raises, so the streaming
    form would leave a half-written file. Nothing is opened here until the whole string
    exists.
    """
    token = tmp_path / "token.json"
    token.write_text(json.dumps(OLD_TOKEN))
    with pytest.raises(TypeError):
        m.write_token(token, {"creation_timestamp": 1, "token": {"nope"}})
    assert json.loads(token.read_text()) == OLD_TOKEN
    assert [p.name for p in tmp_path.iterdir()] == ["token.json"]


def test_the_writer_takes_the_extra_arguments_schwab_py_passes(tmp_path):
    """``schwab-py`` calls the writer as ``func(token, *args, **kwargs)``.

    Its wrapper forwards whatever the oauth client hands it. A writer that accepted the
    token alone would raise inside the library on a later refresh, which is the path with
    no person watching.
    """
    token = tmp_path / "token.json"
    m.token_writer(token)(FRESH_TOKEN, "positional", keyword="value")
    assert json.loads(token.read_text()) == FRESH_TOKEN


def test_the_writer_makes_the_config_directory_when_it_is_missing(tmp_path):
    """A token path whose directory does not exist yet still lands."""
    token = tmp_path / "fresh" / "token.json"
    m.write_token(token, FRESH_TOKEN)
    assert json.loads(token.read_text()) == FRESH_TOKEN


# -- where the token goes --------------------------------------------------------------


def test_the_default_token_path_is_where_the_vendor_reads_a_token_back():
    """The re-auth must write where ``SchwabVendor.from_token`` looks.

    Both spell the design's standard location, and they spell it separately: this module
    builds it from ``lake.paths`` so it needs nothing from the vendor layer. Two
    spellings can drift, and a drift would land a fresh token somewhere capture never
    reads, which looks exactly like a re-auth that did not happen.
    """
    from lake.schwab import DEFAULT_TOKEN_PATH as VENDOR_TOKEN_PATH

    assert m.DEFAULT_TOKEN_PATH == VENDOR_TOKEN_PATH


# -- what it prints --------------------------------------------------------------------


def test_the_report_names_no_secret(tmp_path):
    """Both credentials pass through this module, and neither may reach the block."""
    report = _run(RecordingFlow(), tmp_path / "token.json")
    rendered = report.render()
    assert API_KEY not in rendered
    assert APP_SECRET not in rendered
    # The dataclass repr too, because a traceback from a launchd job prints it to a log
    # file. Neither credential is a field, and this is what says so.
    assert API_KEY not in repr(report)
    assert APP_SECRET not in repr(report)


def test_the_report_prints_the_callback_and_the_token_path(tmp_path):
    """The two facts the issue names, plus the callback the operator has to verify.

    The callback is printable only because it is not wrapped in ``Secret``. A wrapped
    value would render as the redaction and the operator could not check it against the
    Schwab app registration.
    """
    token = tmp_path / "token.json"
    rendered = _run(RecordingFlow(), token).render()
    assert CALLBACK in rendered
    assert str(token) in rendered
    assert "token landed:  yes" in rendered
