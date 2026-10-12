"""The build board's page, run rather than read.

``.claude/skills/sync-prune-next/board.html`` is served on GitHub Pages, where it
fetches ``board.json`` at once and every two minutes. A GitHub Action builds that file
beside it (#872). The page carries no copy of the board, so when it has no good data it
must say so and draw nothing that reads as data.

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
"""

from __future__ import annotations

import copy
import html
import json
import re
import shutil
import struct
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
from tests.support.board_page import icon_links

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
# fetch timer once per further answer, as two minutes passing would. The result
# arrives after the script's last line, so it is printed rather than returned.
DRIVER = """
var __flush = async function () { for (var i = 0; i < 50; i++) { await null; } };
(async function () {
  var out = { error: null, ids: {}, fetches: __FETCHES, timers: [] };
  try {
    await __flush();
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


def _prelude(responses: list[dict] | None) -> str:
    """A fake fetch serving ``responses`` in turn, and timers the driver can fire.

    Each response is ``{"status": n, "body": text}``, ``{"throw": true}`` for a network
    failure, or ``{"never": true}`` for a fetch that never settles. ``responses`` of
    None is a host with no fetch at all.
    """
    lines = [
        "var setTimeout = function () { return 0; };",
        "var __TIMERS = [];",
        "setInterval = function (fn, ms) { __TIMERS.push({ fn: fn, ms: ms }); return 0; };",
        f"var __RESP = {json.dumps(responses or [])};",
        "var __CUR = 0;",
        "var __FETCHES = [];",
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
    return "\n".join(lines) + "\n"


def run_page(tmp_path: Path, responses: list[dict] | None) -> dict[str, Any]:
    """Execute the page and return what it drew, what it fetched and its timers."""
    markup, script = _page_parts()
    source = (
        f"var __STARTS_HIDDEN = {json.dumps(_starts_hidden(markup))};\n"
        + STUB.read_text(encoding="utf-8")
        + _prelude(responses)
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


def served(board: dict[str, Any] | str, status: int = 200) -> dict[str, Any]:
    """A fetch answer serving a document, or a raw body when ``board`` is text."""
    return {"status": status, "body": board if isinstance(board, str) else json.dumps(board)}


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
NONE_LINE = (
    "Showing no board data. This view cannot reach the live board, "
    "and the page carries no copy of its own."
)


# -- the old modes, as fetch answers -------------------------------------------------


def test_a_host_with_no_fetch_says_it_cannot_reach_the_board(tmp_path: Path) -> None:
    result = run_page(tmp_path, None)
    banner = text(result, "source")
    assert banner == NONE_LINE
    assert result["fetches"] == []
    assert 120000 not in result["timers"], "a page with no fetch set a fetch timer"
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
    assert text(result, "source") == NONE_LINE
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
        "Built from GitHub against main at 5f0b12c by an Action, which rebuilds it on most "
        "issue and pull request activity, whenever CI or Infra finishes, and on a schedule. "
        "The page fetches it again every 2 minutes, but every figure on it is current as of "
        "the stamp above rather than live."
    )
    assert "Measured by hand" not in foot
    assert foot.endswith(
        "The In flight section decays fastest: a check that has gone green or red since "
        "then still reads here as it read then and a review posted since then still reads "
        "as owed."
    )


def test_a_board_with_nothing_in_flight_names_no_decay(tmp_path: Path) -> None:
    doc = board()
    doc["prs"] = []
    foot = text(run_page(tmp_path, [served(doc)]), "foot")
    assert foot.endswith("is current as of the stamp above rather than live.")


def test_the_footer_escapes_the_main_sha(tmp_path: Path) -> None:
    raw = run_page(tmp_path, [served(board(main="5f0<b>12c"))])["ids"]["foot"]["html"]
    assert "5f0&lt;b&gt;12c" in raw
    assert "<b>12c" not in raw


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
        ({"status": 200, "body": "<html>"}, "board.json is not JSON"),
    ],
    ids=["404", "network failure", "refused document", "not JSON"],
)
def test_a_failed_poll_keeps_the_last_board_and_says_it_retries(
    tmp_path: Path, answer: dict, code: str
) -> None:
    result = run_page(tmp_path, [served(board()), answer])
    banner = text(result, "source")
    assert banner.startswith(f"{STOPPED} ({code}). What is shown is the last live data, measured ")
    assert banner.endswith(f" against 5f0b12c . {RETRY}")
    assert "Reload" not in banner
    assert cards(result, "board") == {1}
    assert text(result, "strip").startswith("3 open issues")


def test_a_good_poll_after_a_failed_one_goes_live_again(tmp_path: Path) -> None:
    later = board()
    later["state"]["issues"]["open"] = 4
    result = run_page(tmp_path, [served(board()), {"throw": True}, served(later)])
    assert hidden(result, "source")
    assert text(result, "strip").startswith("4 open issues")


# -- what the page refuses -----------------------------------------------------------

QUOTE = '1" onmouseover="alert(1)'
DELETE = object()


def _control() -> dict[str, Any]:
    """``board()`` carrying every optional field and every allowed value once.

    It adds a plan, a session with a ``kind`` and one without, a closed and a merged
    pull request with no ``partOf``, cards of the kinds ``data`` and ``decision``, one
    with ``after`` and no ``labels``, and a ranking entry with an ``order``.
    """
    doc = board()
    doc["planned"] = [{"n": 1, "passes": 2, "ready": "build"}]
    # A session with no `kind` reads as a decompose loop, so it may leave it out.
    doc["working"] = [
        {"n": 3, "kind": "plan", "what": "a decompose loop"},
        {"n": 5, "what": "a loop with no kind"},
    ]
    # Rounds writing the retired artifact board wrote `linked` as closing references.
    for pr, issue_n, state, linked in ((41, 1, "closed", True), (42, 3, "merged", [3])):
        doc["prs"].append(
            {"issue": issue_n, "pr": pr, "state": state, "review": "review complete",
             "linked": linked, "reviewed": True, "rollup": []}
        )  # fmt: skip
    doc["tracker"] += [
        {"n": 4, "label": "Fourth", "labels": [], "ms": "MVP 2", "kind": "data", "needs": []},
        {"n": 5, "label": "Fifth", "ms": "MVP 2", "kind": "decision", "needs": [],
         "after": [1]},
    ]  # fmt: skip
    doc["next"] += [
        {"issue": 4, "band": 1, "ready": "build"},
        {"issue": 5, "band": 2, "order": 1, "ready": "decide"},
    ]
    doc["state"]["issues"]["open"] = 5
    return doc


def _set(path: str, value: Any) -> dict[str, Any]:
    doc = _control()
    keys = path.split(".")
    target: Any = doc
    for key in keys[:-1]:
        target = target[int(key)] if key.isdigit() else target[key]
    last: Any = int(keys[-1]) if keys[-1].isdigit() else keys[-1]
    if value is DELETE:
        del target[last]
    else:
        target[last] = value
    return doc


# Each refusal names the field it breaks. A raw body carries what JSON text can say and
# a Python value cannot be dumped as: `1e999` parses to Infinity.
REFUSED = {
    "tracker n": _set("tracker.0.n", QUOTE),
    "pr number": _set("prs.0.pr", QUOTE),
    "pr issue": _set("prs.0.issue", QUOTE),
    "open count": _set("state.issues.open", QUOTE),
    "open count infinite": json.dumps(_control()).replace('"open": 5', '"open": 1e999'),
    "kind": _set("tracker.0.kind", QUOTE),
    "pr state": _set("prs.0.state", QUOTE),
    "planned ready": _set("planned.0.ready", QUOTE),
    "planned passes": _set("planned.0.passes", '2" onmouseover="alert(1)'),
    "next ready": _set("next.0.ready", QUOTE),
    "missing review": _set("prs.0.review", DELETE),
    "reviewed a string": _set("prs.0.reviewed", "false"),
    "linked a string": _set("prs.0.linked", "true"),
    "linked holds a string": _set("prs.0.linked", ["2"]),
    "missing linked": _set("prs.0.linked", DELETE),
    "partOf a string": _set("prs.0.partOf", "false"),
    "rollup not a list": _set("prs.0.rollup", "success"),
    "rollup pair of three": _set("prs.0.rollup", [["test", "success", "x"]]),
    "rollup word a number": _set("prs.0.rollup", [["test", 1]]),
    "rollup entry a two-character string": _set("prs.0.rollup", ["ab"]),
    "needs a string": _set("tracker.1.needs", QUOTE),
    "needs holds a string": _set("tracker.1.needs", ["1"]),
    "needs holds a string after a number": _set("tracker.1.needs", [1, "1"]),
    "missing needs": _set("tracker.2.needs", DELETE),
    "after holds a string": _set("tracker.4.after", ["1"]),
    "labels holds a number": _set("tracker.0.labels", [1]),
    "missing label": _set("tracker.0.label", DELETE),
    "missing ms": _set("tracker.0.ms", DELETE),
    "tracker item null": _set("tracker.1", None),
    "empty tracker": _set("tracker", []),
    "missing main": _set("state.main", DELETE),
    "updatedAt a number": _set("state.updatedAt", 1),
    "missing updatedAt": _set("state.updatedAt", DELETE),
    "working n a string": _set("working.0.n", "3"),
    "working what a number": _set("working.0.what", 1),
    "missing working what": _set("working.0.what", DELETE),
    "working kind a number": _set("working.0.kind", 1),
    "planned n a string": _set("planned.0.n", "1"),
    "missing planned n": _set("planned.0.n", DELETE),
    "next issue a string": _set("next.0.issue", "1"),
    "missing next issue": _set("next.0.issue", DELETE),
    "next band a string": _set("next.0.band", "1"),
    "missing next band": _set("next.0.band", DELETE),
    "next order a string": _set("next.2.order", "1"),
    "working item null": _set("working.0", None),
    "missing working": _set("working", DELETE),
    "planned item null": _set("planned.0", None),
    "next item null": _set("next.0", None),
}


@pytest.mark.parametrize("doc", list(REFUSED.values()), ids=list(REFUSED))
def test_one_bad_item_refuses_the_whole_document(tmp_path: Path, doc: dict | str) -> None:
    """A field the page writes into an attribute unescaped must be a number.

    A field the page tests for truth must be a boolean, since the string "false" is
    true, and a list it compares against card numbers must hold numbers.
    """
    result = run_page(tmp_path, [served(doc)])
    assert "missing fields this page needs" in text(result, "source")
    assert_no_board_data(result)
    drawn = json.dumps(result["ids"])
    assert "onmouseover" not in drawn


def test_the_document_still_draws_with_every_checked_field_good(tmp_path: Path) -> None:
    """The control for the refusals above: the same document unchanged draws."""
    result = run_page(tmp_path, [served(_control())])
    assert hidden(result, "source")
    assert cards(result, "board") | cards(result, "flow") >= {1, 2, 4, 5}


def test_a_title_with_a_quote_is_escaped(tmp_path: Path) -> None:
    doc = board()
    doc["tracker"][0]["label"] = "Don't draw <b>this</b>"
    raw = run_page(tmp_path, [served(doc)])["ids"]["board"]["html"]
    assert "Don&#39;t draw &lt;b&gt;this&lt;/b&gt;" in raw
    assert "Don't" not in raw


NO_LINK_CHIP = "no closing link registered, so merging will leave #2 open"
NO_LINK_NOTE = "GitHub registered no closing link for PR #40 , so merging would leave #2 open"


@pytest.mark.parametrize("linked", [[], [999], [1]], ids=["empty", "another", "another card"])
def test_a_linked_list_not_naming_the_issue_draws_no_closing_link(
    tmp_path: Path, linked: list[int]
) -> None:
    """A list of closing references links this card only when it names the card."""
    doc = board()
    doc["prs"][0]["linked"] = linked
    result = run_page(tmp_path, [served(doc)])
    assert NO_LINK_CHIP in text(result, "flow")
    assert NO_LINK_NOTE in text(result, "flownote")


@pytest.mark.parametrize("linked", [True, [2], [999, 2]], ids=["true", "the issue", "among others"])
def test_a_linked_list_naming_the_issue_draws_as_linked(tmp_path: Path, linked: Any) -> None:
    doc = board()
    doc["prs"][0]["linked"] = linked
    result = run_page(tmp_path, [served(doc)])
    assert "no closing link" not in text(result, "flow")
    assert "no closing link" not in text(result, "flownote")


def _throws(**state: Any) -> dict[str, Any]:
    """A document that passes the item check and still breaks the drawing code.

    The check does not read ``untracked``, which the page maps over as a list.
    """
    doc = board(**state)
    doc["untracked"] = "not a list"
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
    """The kept board is the earlier one, so none of the broken document's state shows."""
    broken = _throws(main="badbad1", issues={"open": 99})
    result = run_page(tmp_path, [served(board()), served(broken)])
    banner = text(result, "source")
    assert banner.startswith(f"{STOPPED} (the board could not be drawn).")
    assert banner.endswith(f" against 5f0b12c . {RETRY}")
    assert "badbad1" not in banner
    assert cards(result, "board") == {1}
    for element in CONTENT:
        assert not hidden(result, element), f"{element} is hidden behind a kept board"
    assert text(result, "strip").startswith("3 open issues")
    assert "99" not in text(result, "strip")
    assert "badbad1" not in text(result, "foot")


def test_a_failed_poll_after_a_first_draw_that_threw_still_shows_no_board(
    tmp_path: Path,
) -> None:
    """A document that never drew is no live data, so a later failure has none to keep."""
    result = run_page(tmp_path, [served(_throws()), {"status": 404, "body": ""}])
    assert "This view cannot reach the live board" in text(result, "source")
    assert_no_board_data(result)


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
    # #755 is a parent, shown because its children serve it, and not a ranked card.
    assert text(result, "more") == (
        "Showing the 1 ranked card and 1 parent in MVP 2. 1 other open issue in MVP 2 is not "
        "in the priority order. Show all 3"
    )


IN_FLIGHT_NOTE = (
    "Priority is not a key today, because every ranked card in MVP 2 is in the In flight "
    "section above. Pick a card to trace its chain."
)


def test_a_ranked_card_in_flight_keeps_the_ranked_only_view(tmp_path: Path) -> None:
    """Saying nothing in the milestone is ranked would be false while #2 is.

    A card in flight can be planned with nobody on it, so the note does not call it a
    card nobody can take.
    """
    doc = board()
    doc["next"] = [{"issue": 2, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "flow") == {2}
    assert cards(result, "board") == set()
    assert text(result, "more") == (
        "Every ranked card in MVP 2 is in flight, so no card is showing here. 1 other open "
        "issue in MVP 2 is not in the priority order. Show all 1"
    )
    assert text(result, "ordernote").endswith(IN_FLIGHT_NOTE)


def test_with_every_ranked_card_in_flight_and_nothing_hidden_the_line_says_why(
    tmp_path: Path,
) -> None:
    """An empty board with no word of why reads as a board that failed to draw."""
    doc = board()
    del doc["tracker"][0]
    doc["state"]["issues"]["open"] = 2
    doc["next"] = [{"issue": 2, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "flow") == {2}
    assert cards(result, "board") == set()
    assert (
        text(result, "more")
        == "Every ranked card in MVP 2 is in flight, so no card is showing here."
    )


def test_with_every_ranked_card_in_flight_a_parent_is_not_called_ranked(tmp_path: Path) -> None:
    doc = board()
    doc["tracker"].append(
        {"n": 4, "label": "Fourth", "labels": [], "ms": "MVP 2", "kind": "parent", "needs": []}
    )
    doc["next"] = [{"issue": 2, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {4}
    assert text(result, "more") == (
        "Every ranked card in MVP 2 is in flight, so only 1 parent is showing here. 1 other "
        "open issue in MVP 2 is not in the priority order. Show all 2"
    )
    assert text(result, "ordernote").endswith(IN_FLIGHT_NOTE)


def test_a_ranked_parent_alone_in_the_ranking_keeps_the_ranked_only_view(
    tmp_path: Path,
) -> None:
    """A parent the ranking names is ranked, so the milestone is not unranked."""
    doc = board()
    doc["tracker"].append(
        {"n": 4, "label": "Fourth", "labels": [], "ms": "MVP 2", "kind": "parent", "needs": []}
    )
    doc["next"] = [{"issue": 4, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {4}
    more = text(result, "more")
    assert not more.startswith("Nothing")
    assert more == (
        "Showing the 1 ranked card in MVP 2. 1 other open issue in MVP 2 is not in the "
        "priority order. Show all 2"
    )


def test_a_deferred_card_alone_in_the_ranking_leaves_the_milestone_unranked(
    tmp_path: Path,
) -> None:
    """A deferred card draws no position, so it does not count as ranked here either."""
    doc = board()
    doc["tracker"][0]["kind"] = "deferred"
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {1}
    assert text(result, "more") == (
        "Nothing in MVP 2 is ranked yet, so the 1 open issue in MVP 2 not in flight is showing."
    )
    # A deferred card is one the ranking says nobody should take, so this stays true.
    assert text(result, "ordernote").endswith(
        "Priority is not a key today, because nothing left in the ranking is a card anybody "
        "can take. Pick a card to trace its chain."
    )


def test_a_ranking_in_another_milestone_leaves_this_one_unranked(tmp_path: Path) -> None:
    doc = _generated([issue(638), issue(812), issue(900, milestone="MVP 3")])
    doc["next"] = [{"issue": 900, "band": 1, "ready": "plan"}]
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "board") == {638, 812}
    assert text(result, "more") == (
        "Nothing in MVP 2 is ranked yet, so all 2 open issues in MVP 2 not in flight are showing."
    )
    # #900 is free in MVP 3, so calling the ranking a list nobody can take is false.
    assert text(result, "ordernote").endswith(
        "Priority is not a key today, because nothing in MVP 2 is ranked. "
        "Pick a card to trace its chain."
    )


def test_a_milestone_with_every_card_in_flight_draws_no_unranked_line(tmp_path: Path) -> None:
    doc = board()
    del doc["tracker"][0]
    doc["state"]["issues"]["open"] = 2
    result = run_page(tmp_path, [served(doc)])
    assert cards(result, "flow") == {2}
    assert text(result, "more") == ""


def test_with_no_ranking_the_note_says_nothing_is_ranked(tmp_path: Path) -> None:
    result = run_page(tmp_path, [served(_generated([issue(638), issue(812)]))])
    note = text(result, "ordernote")
    assert note.endswith(
        "Priority is not a key today, because nothing is ranked. Pick a card to trace its chain."
    )


def test_a_deferred_card_ranked_here_beside_a_free_one_elsewhere_names_the_milestone(
    tmp_path: Path,
) -> None:
    """With #900 free in MVP 3, only the cards ranked in MVP 2 are ones nobody can take."""
    doc = board()
    doc["tracker"][0]["kind"] = "deferred"
    doc["tracker"].append(
        {"n": 900, "label": "Later", "labels": [], "ms": "MVP 3", "kind": "ready", "needs": []}
    )
    doc["next"] = [
        {"issue": 1, "band": 1, "ready": "plan"},
        {"issue": 900, "band": 1, "ready": "plan"},
    ]
    result = run_page(tmp_path, [served(doc)])
    assert text(result, "ordernote").endswith(
        "Priority is not a key today, because nothing in MVP 2 left in the ranking is a card "
        "anybody can take. Pick a card to trace its chain."
    )


def test_the_board_note_agrees_in_number_for_one(tmp_path: Path) -> None:
    doc = _generated([issue(638), issue(759, blocked_by=(blocker(638),))])
    note = text(run_page(tmp_path, [served(doc)]), "boardnote")
    assert note.startswith(
        "1 card is free for a session to take today , out of 1 in the first column: #638."
    )
    assert "#638 is the single card whose landing frees the most , at 1 other." in note


def test_the_board_note_agrees_in_number_for_a_tie_at_one(tmp_path: Path) -> None:
    doc = _generated(
        [
            issue(1),
            issue(2),
            issue(3, blocked_by=(blocker(1),)),
            issue(4, blocked_by=(blocker(2),)),
        ]
    )
    note = text(run_page(tmp_path, [served(doc)]), "boardnote")
    assert "#1 and #2 each free 1 other , and nothing frees more." in note


def test_the_board_note_agrees_in_number_for_two(tmp_path: Path) -> None:
    doc = _generated(
        [issue(638), issue(759, blocked_by=(blocker(638),)), issue(760, blocked_by=(blocker(638),))]
    )
    note = text(run_page(tmp_path, [served(doc)]), "boardnote")
    assert "#638 is the single card whose landing frees the most , at 2 others." in note


def test_the_board_note_agrees_in_number_for_a_tie_at_two(tmp_path: Path) -> None:
    doc = _generated(
        [
            issue(1),
            issue(2),
            issue(3, blocked_by=(blocker(1),)),
            issue(4, blocked_by=(blocker(1),)),
            issue(5, blocked_by=(blocker(2),)),
            issue(6, blocked_by=(blocker(2),)),
        ]
    )
    note = text(run_page(tmp_path, [served(doc)]), "boardnote")
    assert "#1 and #2 each free 2 others , and nothing frees more." in note


def test_the_page_carries_its_own_document_skeleton() -> None:
    """GitHub Pages serves the page as written, so it carries its own skeleton.

    Without these, the page renders in quirks mode at 980 pixels wide on a phone, and
    ``.msbar``'s ``display: flex`` beats ``hidden``. No engine here applies CSS, so the
    rules are read rather than run.
    """
    markup, _ = _page_parts()
    assert re.match(r"\s*<!doctype\s+html\s*>\s*<html\b", markup, re.I)
    assert re.search(r"<meta\s+charset\s*=\s*\"?utf-8\"?\s*/?>", markup, re.I)
    viewport = re.search(r"<meta\s+name\s*=\s*\"viewport\"\s+content\s*=\s*\"([^\"]*)\"", markup)
    assert viewport
    assert re.sub(r"\s+", "", viewport.group(1)) == "width=device-width,initial-scale=1"
    assert re.search(r"(?:^|[}\s])body\s*\{[^}]*\bmargin\s*:\s*0\s*[;}]", markup)
    assert re.search(
        r"(?:^|[}\s])\[hidden\]\s*\{\s*display\s*:\s*none\s*!\s*important\s*;?\s*\}", markup
    )
    assert re.search(r"</script>\s*</body>\s*</html>\s*$", PAGE.read_text(encoding="utf-8"))


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def check_icon(icon: bytes) -> None:
    """Fail unless ``icon`` is an ICO file whose every image a browser can decode.

    An ICO file opens with a six-byte header, reserved zero then type 1 then a count,
    and a sixteen-byte directory entry per image. An entry's first two bytes are the
    image's width and height, with 0 meaning 256, and it ends with the image's size and
    its offset. Reading the directory alone passes a file whose entries point at the
    wrong bytes, so each image is read too.

    1. Its size is above zero and it lies inside the file, which a file cut short fails.
    2. Its bytes open with the PNG signature, since every image here is a PNG.
    3. The PNG's IHDR chunk, the first after the signature, states the width and height
       the entry claims. That chunk holds the width then the height, big-endian, at
       bytes 16 and 20 of the image.
    4. No two images share bytes, which an entry pointing at its neighbour fails.
    """
    reserved, kind, count = struct.unpack_from("<HHH", icon, 0)
    assert (reserved, kind) == (0, 1)
    entries = [icon[6 + 16 * i : 22 + 16 * i] for i in range(count)]
    assert sorted(entry[0] or 256 for entry in entries) == [16, 32, 48, 256]
    spans = []
    for entry in entries:
        width, height = entry[0] or 256, entry[1] or 256
        size, offset = struct.unpack_from("<II", entry, 8)
        assert size > 0, f"the {width} px image is empty"
        assert offset + size <= len(icon), f"the {width} px image runs past the file"
        image = icon[offset : offset + size]
        assert image.startswith(PNG_SIGNATURE), f"the {width} px image is not a PNG"
        assert image[12:16] == b"IHDR", f"the {width} px image has no IHDR first"
        assert struct.unpack_from(">II", image, 16) == (width, height), (
            f"the {width} px entry points at an image of another size"
        )
        spans.append((offset, offset + size))
    spans.sort()
    for (_, end), (start, _) in zip(spans, spans[1:], strict=False):
        assert end <= start, "two images share bytes"


def test_the_page_links_its_icon_and_the_icon_is_one() -> None:
    """Protects the board's tab from looking like every other page's (#884).

    The link sits in the head, where a browser looks for it, and not inside a comment
    or a ``noscript`` block, where a browser never loads it. The file beside the page
    has to be an icon a browser can read at tab size, which ``check_icon`` reads.
    """
    (link,) = icon_links(PAGE.read_text(encoding="utf-8"))
    assert (link["href"], link.get("sizes")) == ("favicon.ico", "any")
    check_icon((SKILL / "favicon.ico").read_bytes())
