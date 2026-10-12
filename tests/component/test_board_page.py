"""The build board's page, run rather than read, on the fetch path GitHub Pages takes.

``.claude/skills/sync-prune-next/board.html`` draws the board from one of two sources.
Inside the claude.ai artifact it subscribes to the artifact's database. On GitHub
Pages it has no database, so it fetches ``board.json`` every two minutes, which a
GitHub Action builds beside it (#872). It carries no copy of the board, so when it has
no good data it must say so and draw nothing that reads as data.

A parse check would not catch the failures that matter. A page that draws a refused
document parses, and a banner sentence that renders perfectly can still say something
false. So each test executes the page's script under a JavaScript engine, with
``dom-stub.js`` standing in for the browser and a fake fetch serving each answer in
turn, and reads what the page drew. The driver fires the page's two-minute timer once
per answer after the first.

The engine is ``node`` where it is installed, as on GitHub's runners, and ``osascript
-l JavaScript`` otherwise, which ships with macOS. With neither, the tests skip rather
than pass. JavaScript for Automation runs promise callbacks only after the script's
last line, and writes ``console.log`` to stderr, so the driver prints from inside its
callbacks and both streams are read.

``board-harness.js`` still drives the database path in its seven modes. It cannot reach
the fetch path, since it passes no ``fetch``.
"""

from __future__ import annotations

import copy
import html
import json
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.support.board_github import (
    REPO,
    SKILL,
    FakeGitHub,
    blocker,
    issue,
    load_generator,
)

GENERATOR = load_generator()
PAGE = SKILL / "board.html"
STUB = SKILL / "dom-stub.js"

# What the page draws only when it has data. With none, each is hidden.
CONTENT = ("msbar", "flightsec", "ordersec", "foot")


def _engine() -> list[str]:
    """The command that runs a script file, or a skip naming what is missing."""
    node = shutil.which("node")
    if node:
        return [node]
    osascript = shutil.which("osascript")
    if osascript:
        return [osascript, "-l", "JavaScript"]
    pytest.skip("no JavaScript engine: neither node nor osascript is installed")


def _page_parts(page: Path = PAGE) -> tuple[str, str]:
    """The static markup before the page's one inline script, and the script."""
    text = page.read_text(encoding="utf-8")
    start = text.find("<script>")
    assert start >= 0, f"{page} has no inline script"
    end = text.find("</script>", start)
    assert end >= 0, f"{page} never closes its script"
    return text[:start], text[start + len("<script>") : end]


def _starts_hidden(markup: str) -> list[str]:
    """The ids the static markup marks ``hidden``, as a browser starts them."""
    return re.findall(r'id="([^"]+)"[^>]*\shidden\b', markup)


# The driver. It waits for the page's first fetch to settle, then fires the page's
# fetch timer once per further answer, as two minutes passing would. A page with a
# database gets each of `__DB_STEPS` delivered instead: a snapshot holding `data`, or a
# subscription error carrying `error`. The result arrives after the script's last
# line, so it is printed rather than returned.
DRIVER = """
var __flush = async function () { for (var i = 0; i < 50; i++) { await null; } };
(async function () {
  var out = { error: null, ids: {}, fetches: __FETCHES, timers: [] };
  try {
    await __flush();
    for (var s = 0; s < __DB_STEPS.length; s++) {
      var step = __DB_STEPS[s];
      if (step.error) { __DB.onError({ code: step.error }); }
      else { __DB.onData({ exists: true, data: function () { return step.data; } }); }
      await __flush();
    }
    for (var k = 1; k < __RESP.length; k++) {
      __CUR = k;
      __TIMERS.forEach(function (t) { if (t.ms !== 60000) { t.fn(); } });
      await __flush();
    }
  } catch (e) { out.error = String(e && e.stack || e); }
  out.timers = __TIMERS.map(function (t) { return t.ms; });
  Object.keys(store).forEach(function (id) {
    var e = store[id];
    out.ids[id] = { html: e.innerHTML, text: e.textContent, hidden: e.hidden === true,
                    value: e.value };
  });
  var line = "BOARD_RESULT " + JSON.stringify(out);
  if (typeof console !== "undefined" && console.log) { console.log(line); } else { print(line); }
})();
void 0;
"""


def _prelude(responses: list[dict] | None, db_steps: list[dict] | None) -> str:
    """A fake fetch serving ``responses`` in turn, an optional fake database, and timers.

    Each response is ``{"status": n, "body": text}``, ``{"throw": true}`` for a network
    failure, or ``{"never": true}`` for a fetch that never settles. ``responses`` of
    None is a host with no fetch at all. ``db_steps`` of None is a host with no
    database, which is GitHub Pages.
    """
    lines = [
        "var setTimeout = function () { return 0; };",
        "var __TIMERS = [];",
        "setInterval = function (fn, ms) { __TIMERS.push({ fn: fn, ms: ms }); return 0; };",
        f"var __RESP = {json.dumps(responses or [])};",
        "var __CUR = 0;",
        "var __FETCHES = [];",
        f"var __DB_STEPS = {json.dumps(db_steps or [])};",
        "var __DB = {};",
    ]
    if responses is None:
        lines.append("var fetch = undefined;")
    else:
        lines += [
            "var fetch = function (url, opts) {",
            "  __FETCHES.push({ url: url, cache: opts && opts.cache });",
            "  var r = __RESP[__CUR];",
            "  if (r.never) { return new Promise(function () {}); }",
            "  if (r.throw) { return Promise.reject(new Error('offline')); }",
            "  return Promise.resolve({ ok: r.status >= 200 && r.status < 300, status: r.status,",
            "    text: function () { return Promise.resolve(r.body); } });",
            "};",
        ]
    if db_steps is not None:
        lines += [
            "var window = { claude: { use: function () { return Promise.resolve({",
            "  doc: function () { return { onSnapshot: function (cb, err) {",
            "    __DB.onData = cb; __DB.onError = err; } }; } }); } } };",
        ]
    return "\n".join(lines) + "\n"


def run_page(
    tmp_path: Path,
    responses: list[dict] | None,
    *,
    db_steps: list[dict] | None = None,
) -> dict[str, Any]:
    """Execute the page and return what it drew, what it fetched and its timers."""
    markup, script = _page_parts()
    source = (
        f"var __STARTS_HIDDEN = {json.dumps(_starts_hidden(markup))};\n"
        + STUB.read_text(encoding="utf-8")
        + _prelude(responses, db_steps)
        + script
        + DRIVER
    )
    runner = tmp_path / "run.js"
    runner.write_text(source, encoding="utf-8")
    done = subprocess.run(
        [*_engine(), str(runner)], capture_output=True, text=True, timeout=60, check=False
    )
    output = done.stdout + done.stderr
    assert done.returncode == 0, output
    found = [line for line in output.splitlines() if line.startswith("BOARD_RESULT ")]
    assert len(found) == 1, output
    result = json.loads(found[0][len("BOARD_RESULT ") :])
    assert result["error"] is None, result["error"]
    return result


def served(board: dict[str, Any], status: int = 200) -> dict[str, Any]:
    return {"status": status, "body": json.dumps(board)}


def text(result: dict[str, Any], element: str) -> str:
    """An element's rendered text, tags dropped and whitespace collapsed."""
    entry = result["ids"].get(element, {})
    raw = entry.get("html") or entry.get("text") or ""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", raw))).strip()


def cards(result: dict[str, Any], element: str) -> set[int]:
    raw = result["ids"].get(element, {}).get("html", "")
    return {int(n) for n in re.findall(r'data-n="(\d+)"', raw)}


def hidden(result: dict[str, Any], element: str) -> bool:
    return result["ids"].get(element, {}).get("hidden", element in ("source", "untracked"))


def assert_no_board_data(result: dict[str, Any]) -> None:
    """Nothing but the banner is drawn, so nothing renders as if it were data."""
    for element in CONTENT:
        assert hidden(result, element), f"{element} is not hidden"
    for element in ("strip", "flow", "board", "foot"):
        assert text(result, element) == "", f"{element} drew {text(result, element)!r}"
    assert not hidden(result, "source"), "the banner is hidden"


def board(**state: Any) -> dict[str, Any]:
    """A small good board: two MVP 2 cards, one behind the other, and one deferred card."""
    return {
        "schema": 1,
        "state": {
            "main": "5f0b12c",
            "updatedAt": "2026-10-12T17:00:00Z",
            "issues": {"open": 3},
            **state,
        },
        "prs": [
            {
                "issue": 2,
                "pr": 40,
                "state": "open",
                "review": "no Review complete marker since the head commit",
                "linked": True,
                "partOf": False,
                "reviewed": False,
                "rollup": [["test", "success"]],
            }
        ],
        "working": [],
        "planned": [],
        "next": [{"issue": 1, "band": 1, "ready": "plan"}],
        "tracker": [
            {"n": 1, "label": "First", "labels": ["enhancement"], "ms": "MVP 2",
             "kind": "ready", "needs": []},
            {"n": 2, "label": "Second", "labels": [], "ms": "MVP 2", "kind": "ready",
             "needs": [1]},
            {"n": 3, "label": "Third", "labels": ["deferred"], "ms": "no milestone",
             "kind": "deferred", "needs": []},
        ],
    }  # fmt: skip


STOPPED = "Live updates stopped"
RETRY = "The page tries again every 2 minutes."


# -- the old modes, as fetch answers -------------------------------------------------


def test_a_host_with_no_fetch_says_it_cannot_reach_the_board(tmp_path: Path) -> None:
    result = run_page(tmp_path, None)
    banner = text(result, "source")
    assert banner.startswith("Showing no board data.")
    assert "This view cannot reach the live board" in banner
    assert result["fetches"] == []
    assert_no_board_data(result)


def test_a_fetch_that_never_settles_says_the_board_has_not_arrived(tmp_path: Path) -> None:
    result = run_page(tmp_path, [{"never": True}])
    assert text(result, "source") == (
        "Showing no board data. The live board has not arrived yet, "
        "and the page carries no copy of its own."
    )
    assert len(result["fetches"]) == 1
    assert_no_board_data(result)


@pytest.mark.parametrize(
    "answer",
    [{"status": 404, "body": "Not Found"}, {"throw": True}, {"status": 200, "body": "<html>"}],
    ids=["404", "network failure", "not JSON"],
)
def test_a_first_fetch_that_fails_says_there_is_no_board(tmp_path: Path, answer: dict) -> None:
    result = run_page(tmp_path, [answer])
    assert "This view cannot reach the live board" in text(result, "source")
    assert_no_board_data(result)


def test_a_first_document_the_page_refuses_draws_nothing(tmp_path: Path) -> None:
    result = run_page(tmp_path, [served({"schema": 1})])
    assert text(result, "source") == (
        "Showing no board data. The live board's document is missing fields this page "
        "needs, so it was not used."
    )
    assert_no_board_data(result)


def test_a_good_document_draws_the_board_and_says_how_it_was_built(tmp_path: Path) -> None:
    result = run_page(tmp_path, [served(board())])
    assert hidden(result, "source")
    assert text(result, "source") == ""
    for element in CONTENT:
        assert not hidden(result, element), f"{element} is hidden on a live board"
    assert text(result, "strip").startswith("3 open issues")
    assert cards(result, "flow") == {2}
    assert cards(result, "board") == {1}
    assert result["ids"]["mssel"]["value"] == "MVP 2"
    assert text(result, "mscount") == "2 of 3 open"
    foot = text(result, "foot")
    assert foot.startswith(
        "Built from GitHub against main at 5f0b12c by an Action on each change and on a "
        "schedule, and fetched again every 2 minutes, so every figure on it is current"
    )
    assert "Measured by hand" not in foot


def test_each_fetch_asks_for_a_url_no_cache_has_seen(tmp_path: Path) -> None:
    """Pages tells a browser to cache board.json for ten minutes."""
    result = run_page(tmp_path, [served(board()), served(board())])
    assert len(result["fetches"]) == 2
    for fetch in result["fetches"]:
        assert re.fullmatch(r"board\.json\?t=\d+", fetch["url"]), fetch
        assert fetch["cache"] == "no-store"
    assert 120000 in result["timers"]


@pytest.mark.parametrize(
    ("answer", "code"),
    [
        ({"status": 404, "body": ""}, "board.json answered 404"),
        ({"throw": True}, "board.json could not be fetched"),
        (served({"schema": 1}), "unreadable board.json"),
    ],
    ids=["404", "network failure", "refused document"],
)
def test_a_failed_poll_keeps_the_last_board_and_says_it_retries(
    tmp_path: Path, answer: dict, code: str
) -> None:
    result = run_page(tmp_path, [served(board()), answer])
    banner = text(result, "source")
    assert banner.startswith(f"{STOPPED} ({code}). What is shown is the last live data")
    assert banner.endswith(RETRY)
    assert "Reload" not in banner
    assert cards(result, "board") == {1}
    assert text(result, "strip").startswith("3 open issues")


def test_a_good_poll_after_a_failed_one_goes_live_again(tmp_path: Path) -> None:
    later = board()
    later["state"]["issues"]["open"] = 4
    result = run_page(tmp_path, [served(board()), {"throw": True}, served(later)])
    assert hidden(result, "source")
    assert text(result, "strip").startswith("4 open issues")


# -- the database path ---------------------------------------------------------------


def test_a_page_with_a_database_never_fetches(tmp_path: Path) -> None:
    """The fetch starts only once the lookup settles with no database.

    The second pull request for #872 deletes this case with the database path.
    """
    result = run_page(
        tmp_path,
        [served(board(issues={"open": 99}))],
        db_steps=[{"data": board()}, {"error": "unavailable"}],
    )
    assert result["fetches"] == []
    assert 120000 not in result["timers"]
    assert text(result, "strip").startswith("3 open issues")
    assert text(result, "foot").startswith(
        "Measured by hand against main at 5f0b12c . The page cannot"
    )
    banner = text(result, "source")
    assert banner.startswith(f"{STOPPED} (unavailable).")
    assert banner.endswith("Reload to reconnect.")


# -- what the page refuses -----------------------------------------------------------

QUOTE = '1" onmouseover="alert(1)'
DELETE = object()


def _set(path: str, value: Any) -> dict[str, Any]:
    doc = board()
    doc["planned"] = [{"n": 1, "passes": 2, "ready": "build"}]
    keys = path.split(".")
    target: Any = doc
    for key in keys[:-1]:
        target = target[int(key)] if key.isdigit() else target[key]
    last = keys[-1]
    if value is DELETE:
        del target[last]
    else:
        target[last] = value
    return doc


@pytest.mark.parametrize(
    "doc",
    [
        _set("tracker.0.n", QUOTE),
        _set("prs.0.pr", QUOTE),
        _set("prs.0.issue", QUOTE),
        _set("state.issues.open", QUOTE),
        _set("tracker.0.kind", QUOTE),
        _set("prs.0.state", QUOTE),
        _set("planned.0.ready", QUOTE),
        _set("next.0.ready", QUOTE),
        _set("prs.0.review", DELETE),
        _set("tracker.1.needs", QUOTE),
        _set("tracker.2.needs", DELETE),
    ],
    ids=[
        "tracker n",
        "pr number",
        "pr issue",
        "open count",
        "kind",
        "pr state",
        "planned ready",
        "next ready",
        "missing review",
        "needs a string",
        "missing needs",
    ],
)
def test_one_bad_item_refuses_the_whole_document(tmp_path: Path, doc: dict) -> None:
    """A field the page writes into an attribute unescaped must be a number."""
    result = run_page(tmp_path, [served(doc)])
    assert "missing fields this page needs" in text(result, "source")
    assert_no_board_data(result)
    drawn = json.dumps(result["ids"])
    assert "onmouseover" not in drawn


def test_the_document_still_draws_with_every_checked_field_good(tmp_path: Path) -> None:
    """The control for the refusals above: the same document unchanged draws."""
    doc = _set("tracker.0.label", "First")
    assert "missing fields" not in text(run_page(tmp_path, [served(doc)]), "source")


def test_a_title_with_a_quote_is_escaped(tmp_path: Path) -> None:
    doc = board()
    doc["tracker"][0]["label"] = "Don't draw <b>this</b>"
    raw = run_page(tmp_path, [served(doc)])["ids"]["board"]["html"]
    assert "Don&#39;t draw &lt;b&gt;this&lt;/b&gt;" in raw
    assert "Don't" not in raw


def _throws() -> dict[str, Any]:
    """A document that passes the item check and still breaks the drawing code."""
    doc = board()
    doc["tracker"][0]["after"] = 5
    return doc


def test_a_first_document_that_breaks_the_drawing_code_draws_nothing(tmp_path: Path) -> None:
    result = run_page(tmp_path, [served(_throws())])
    assert text(result, "source") == (
        f"{STOPPED} (the board could not be drawn). Showing no board data."
    )
    assert_no_board_data(result)


def test_a_later_document_that_breaks_the_drawing_code_keeps_the_last_board(
    tmp_path: Path,
) -> None:
    result = run_page(tmp_path, [served(board()), served(_throws())])
    banner = text(result, "source")
    assert banner.startswith(f"{STOPPED} (the board could not be drawn).")
    assert banner.endswith(RETRY)
    assert cards(result, "board") == {1}


# -- what the generator writes -------------------------------------------------------


def _generated(issues: list[dict[str, Any]]) -> dict[str, Any]:
    return GENERATOR.build(FakeGitHub(issues), REPO, datetime(2026, 10, 12, 17, 0, tzinfo=UTC))[0]


def test_the_page_draws_every_card_the_generator_writes_when_nothing_is_ranked(
    tmp_path: Path,
) -> None:
    """With ``next`` empty, the ranked-only view drew only the milestone's parents.

    A render of the planned document from live data showed 0 cards under "All" and
    only the parents under "MVP 2", beneath a sentence calling them ranked cards.
    """
    doc = _generated(
        [
            issue(755, sub_issues=(3, 1)),
            issue(638),
            issue(759, blocked_by=(blocker(638),)),
            issue(812),
            issue(500, milestone=None, labels=("deferred",)),
            issue(501, milestone=None, labels=("bug",)),
        ]
    )
    assert doc["next"] == []
    result = run_page(tmp_path, [served(doc)])
    assert result["ids"]["mssel"]["value"] == "MVP 2"
    assert cards(result, "board") == {755, 638, 759, 812}
    assert text(result, "more") == (
        "Nothing in MVP 2 is ranked yet, so all 4 open issues in MVP 2 not in flight are showing."
    )
    note = text(result, "boardnote")
    assert note.startswith(
        "2 cards are free for a session to take today , out of 3 in the first column: #638, #812."
    )


def test_a_first_column_with_no_free_card_lists_none(tmp_path: Path) -> None:
    doc = _generated([issue(755, sub_issues=(3, 1)), issue(865, sub_issues=(2, 0))])
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {755, 865}
    assert text(result, "more") == (
        "Nothing in MVP 2 is ranked yet, so all 2 open issues in MVP 2 not in flight are showing."
    )
    assert text(result, "boardnote").startswith(
        "0 cards are free for a session to take today , out of 2 in the first column. "
    )


def test_a_ranked_card_keeps_the_ranked_only_view(tmp_path: Path) -> None:
    """The fallback applies only when nothing in the milestone is ranked."""
    doc = _generated([issue(638), issue(812), issue(755, sub_issues=(1, 0))])
    doc = copy.deepcopy(doc)
    doc["next"] = [{"issue": 638, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {638, 755}
    assert text(result, "more").startswith("Showing the 2 ranked cards in MVP 2.")


def test_the_page_carries_its_own_document_skeleton() -> None:
    """The artifact host wrapped the page in a document, and GitHub Pages does not.

    Served raw without these, the page renders in quirks mode at 980 pixels wide on a
    phone, and ``.msbar``'s ``display: flex`` beats ``hidden``. No engine here applies
    CSS, so the rules are read rather than run.
    """
    markup, _ = _page_parts()
    assert markup.startswith("<!doctype html>\n<html")
    assert '<meta charset="utf-8">' in markup
    assert '<meta name="viewport" content="width=device-width,initial-scale=1">' in markup
    assert re.search(r"\n  body \{ margin: 0;", markup)
    assert "\n  [hidden] { display: none !important; }\n" in markup
    assert PAGE.read_text(encoding="utf-8").endswith("</script>\n</body>\n</html>\n")
