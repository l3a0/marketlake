"""The dashboard's Cost panel, run rather than read, marketlake #921.

``src/lake/static/status.html`` draws the Cost panel's charts as inline SVG from the payload
``/api/cost`` serves. The query decides every series, slot and sentence, and
``tests/component/test_dashboard_cost.py`` covers those decisions. What only running the
page shows is whether the drawing says the same thing: usage above the baseline, credits
below it, a figure the chart draws also in its table, and a report written before the
daily usage split still drawing.

Each test executes the page's whole script under a JavaScript engine, as
``tests/component/test_board_page.py`` does for the build board, with
``tests/support/status_dom.js`` standing in for the browser. The fetch routes by URL. It
serves ``/api/cost`` the payload the real query returns for a fixture lake, so the test
crosses the real contract, and rejects every other route, so the other panels run only
their failure paths. A throw while drawing the panel lands in ``costFailed``, which writes
a sentence rather than escaping, so every test asserts that sentence is absent.

The engine is ``node`` where it is installed, as on GitHub's runners, and ``osascript -l
JavaScript`` otherwise, which ships with macOS. With neither, the tests skip rather than
pass. JavaScript for Automation has no timers, so the prelude supplies them, and the driver
prints its result from inside its own promise callback.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from lake import dashboard
from tests.component.test_dashboard_cost import (
    cost,
    entry,
    file,
    october,
    september,
    split_october,
)

PAGE = Path(dashboard.__file__).parent / "static" / "status.html"
STUB = Path(__file__).parents[1] / "support" / "status_dom.js"
SECTIONS = ("cost-state", "cost-services", "cost-sessions", "cost-trend", "cost-s3")
FAILED = "The cost query is failing"


def _engine() -> list[str]:
    """The command that runs a script file, or a skip naming what is missing."""
    node = shutil.which("node")
    if node:
        return [node]
    osascript = shutil.which("osascript")
    if osascript:
        return [osascript, "-l", "JavaScript"]
    pytest.skip("no JavaScript engine: neither node nor osascript is installed")


def _script() -> str:
    text = PAGE.read_text(encoding="utf-8")
    start = text.index("<script>") + len("<script>")
    return text[start : text.index("</script>", start)]


# Timers the driver fires by hand, and a fetch that answers only ``/api/cost``.
PRELUDE = """
var __TIMERS = [];
var __FETCHES = [];
var setInterval = function () { return 0; };
var setTimeout = function (fn, ms) { __TIMERS.push(fn); return __TIMERS.length; };
var clearTimeout = function (id) { __TIMERS[id - 1] = null; };
var fetch = function (url) {
  __FETCHES.push(url);
  if (url !== "/api/cost") { return Promise.reject(new Error("offline")); }
  return Promise.resolve({ ok: true, status: 200,
    json: function () { return Promise.resolve(JSON.parse(__COST)); } });
};
"""

# Waits for the page's first refresh to settle, optionally resizes, and prints the tree of
# each Cost section.
DRIVER = """
var __flush = async function () { for (var i = 0; i < 50; i++) { await null; } };
(async function () {
  var out = { error: null, sections: {}, fetches: __FETCHES };
  try {
    await __flush();
    if (__RESIZE_TO) {
      __WIDTH = __RESIZE_TO;
      (window.listeners.resize || []).forEach(function (fn) { fn({}); });
      __TIMERS.forEach(function (fn) { if (fn) { fn(); } });
      await __flush();
    }
    __SECTIONS.forEach(function (id) { out.sections[id] = __dump(document.getElementById(id)); });
  } catch (e) { out.error = String(e && e.stack || e); }
  var line = "COST_RESULT " + JSON.stringify(out);
  if (typeof console !== "undefined" && console.log) { console.log(line); } else { print(line); }
})();
void 0;
"""


def run_page(tmp_path: Path, payload: dict, *, width: int = 640, resize_to: int = 0) -> dict:
    """Execute the page against ``payload`` and return the tree of each Cost section."""
    source = (
        f"var __WIDTH = {width};\n"
        + STUB.read_text(encoding="utf-8")
        + PRELUDE
        + f"var __COST = {json.dumps(json.dumps(payload))};\n"
        + f"var __SECTIONS = {json.dumps(list(SECTIONS))};\n"
        + f"var __RESIZE_TO = {resize_to};\n"
        + _script()
        + DRIVER
    )
    runner = tmp_path / "run.js"
    runner.write_text(source, encoding="utf-8")
    done = subprocess.run(
        [*_engine(), str(runner)], capture_output=True, text=True, timeout=60, check=False
    )
    output = done.stdout + done.stderr
    assert done.returncode == 0, output
    found = [line for line in output.splitlines() if line.startswith("COST_RESULT ")]
    assert len(found) == 1, output
    result = json.loads(found[0][len("COST_RESULT ") :])
    assert result["error"] is None, result["error"]
    assert "/api/cost" in result["fetches"]
    sections = result["sections"]
    assert FAILED not in text_of(sections["cost-state"]), text_of(sections["cost-state"])
    return sections


# -- reading the tree --------------------------------------------------------------------


def walk(node: dict[str, Any]):
    yield node
    for child in node.get("children", ()):
        yield from walk(child)


def text_of(node: dict[str, Any]) -> str:
    """A node's text, the way ``textContent`` joins it, with whitespace collapsed."""
    parts = [n["text"] for n in walk(node) if n["tag"] == "#text"]
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def find(node: dict[str, Any], tag: str, cls: str | None = None) -> list[dict[str, Any]]:
    return [
        n
        for n in walk(node)
        if n["tag"] == tag and (cls is None or cls in (n.get("cls") or "").split())
    ]


def segments(chart: dict[str, Any]) -> list[dict[str, Any]]:
    """The trend's bar segments, each a path carrying its series and value."""
    return [n for n in find(chart, "path") if "data-series" in n["attrs"]]


def path_top_bottom(d: str) -> tuple[float, float]:
    """The highest and lowest y a bar path reaches, from its absolute start and moves."""
    match = re.match(r"M([-0-9.e]+),([-0-9.e]+)", d)
    assert match, d
    y = float(match.group(2))
    ys = [y]
    for command, args in re.findall(r"([vqhVZ])([^vqhVZ]*)", d[match.end() :]):
        if command == "v":
            y += float(args)
            ys.append(y)
        elif command == "q":
            numbers = [float(n) for n in re.findall(r"-?[0-9.]+(?:e-?[0-9]+)?", args)]
            y += numbers[3]
            ys.append(y)
    return min(ys), max(ys)


def baseline_y(chart: dict[str, Any]) -> float:
    [line] = find(chart, "line", "baseline")
    return float(line["attrs"]["y1"])


def tables(node: dict[str, Any]) -> list[list[list[str]]]:
    """Each table under ``node`` as rows of cell text."""
    return [
        [[text_of(cell) for cell in row["children"]] for row in find(table, "tr")]
        for table in find(node, "table")
    ]


@pytest.fixture
def root(tmp_path: Path) -> Path:
    lake = tmp_path / "lake"
    lake.mkdir()
    return lake


# -- the trend -------------------------------------------------------------------------------


def test_the_trend_draws_usage_above_the_baseline_and_credits_below_it(
    root: Path, tmp_path: Path
):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))
    payload = cost(root)

    trend = run_page(tmp_path, payload)["cost-trend"]

    [chart] = find(trend, "svg")
    base = baseline_y(chart)
    drawn = segments(chart)
    usage = [n for n in drawn if n["attrs"]["data-series"] not in ("credits", "other_lines")]
    credits = [n for n in drawn if n["attrs"]["data-series"] == "credits"]
    # The 14th draws four usage series and a tax segment, the 15th one usage series.
    assert sorted(n["attrs"]["data-series"] for n in usage) == [
        "AmazonEC2",
        "AmazonEC2",
        "AmazonS3",
        "AmazonVPC",
        "other_services",
    ]
    assert len(credits) == 2
    for node in usage:
        top, bottom = path_top_bottom(node["attrs"]["d"])
        assert bottom <= base, node
    for node in credits:
        top, bottom = path_top_bottom(node["attrs"]["d"])
        assert top >= base, node
        assert float(node["attrs"]["data-value"]) < 0
    # Each series wears its slot's class, which the page's colour tokens fill.
    slots = {series["key"]: series["slot"] for series in payload["trend_series"]}
    for node in drawn:
        assert node["cls"] == f"s{slots[node['attrs']['data-series']]}"
    # A tick marks each figured day's net, which credits bring to the baseline on the 15th.
    nets = [n for n in find(chart, "line") if "data-net" in n["attrs"]]
    assert [float(n["attrs"]["data-net"]) for n in nets] == [pytest.approx(0.02), 0.0]
    assert float(nets[1]["attrs"]["y1"]) == pytest.approx(base)


def test_the_trend_has_a_legend_and_its_figures_in_a_table(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))
    payload = cost(root)

    trend = run_page(tmp_path, payload)["cost-trend"]

    [legend] = find(trend, "div", "chart-legend")
    assert [text_of(item) for item in legend["children"]] == [
        series["label"] for series in payload["trend_series"]
    ] + ["Net, after credits"]
    [details] = find(trend, "details")
    assert details["open"] is False
    [rows] = tables(details)
    assert rows[0] == ["Day", "Usage", "Credits", "Other lines", "Net", "Usage by service"]
    by_day = {row[0]: row for row in rows[1:]}
    assert by_day["2026-10-14"][1:5] == ["$0.38", "−$0.38", "$0.02", "$0.02"]
    assert "AmazonCloudWatch $0.03" in by_day["2026-10-14"][5]
    assert by_day["2026-10-15, partial"][1:5] == ["$0.10", "−$0.10", "$0.00", "$0.00"]
    assert by_day["2026-10-13"][1] == "no figure yet"
    assert len(rows) == 31


def test_each_trend_day_names_its_figures_to_a_screen_reader_and_a_tooltip(
    root: Path, tmp_path: Path
):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    trend = run_page(tmp_path, cost(root))["cost-trend"]

    labels = [n["attrs"]["aria-label"] for n in find(trend, "rect", "hit")]
    assert len(labels) == 30
    assert labels[-2] == (
        "Oct 14. Usage $0.38. EC2, the VM and its volumes $0.20. S3 $0.05. "
        "VPC, the VM's public address $0.10. Other services $0.03. Credits −$0.38. "
        "Tax $0.02. Net $0.02"
    )
    assert labels[-1].startswith("Oct 15, a partial day. Usage $0.10.")
    assert labels[0] == "Sep 16. No figure yet. The export has not delivered this day."


def test_the_partial_day_is_drawn_lighter(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    trend = run_page(tmp_path, cost(root))["cost-trend"]

    [partial] = find(trend, "g", "partial")
    assert {n["attrs"]["data-series"] for n in segments(partial)} == {"AmazonEC2", "credits"}


def test_an_old_report_draws_the_net_under_the_querys_sentence(root: Path, tmp_path: Path):
    """A report written before #921 carries no split, so the page draws each day's net as
    one series, under the sentence the query writes, and its table has the net columns."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))
    payload = cost(root)

    trend = run_page(tmp_path, payload)["cost-trend"]

    assert text_of(trend).startswith(dashboard.TREND_SPLIT_TEXT)
    assert find(trend, "div", "chart-legend") == []
    [chart] = find(trend, "svg")
    drawn = segments(chart)
    assert [(n["attrs"]["data-series"], n["cls"]) for n in drawn] == [("net", "s1")] * 2
    assert [n for n in find(chart, "line") if "data-net" in n["attrs"]] == []
    [rows] = tables(trend)
    assert rows[0] == ["Day", "Net", "By service"]
    assert ["2026-10-14", "$0.70", "AmazonEC2 $0.70"] in rows


def test_a_window_with_no_figure_draws_no_column(root: Path, tmp_path: Path):
    current = october(daily={}, newest_day="2026-10-15")
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    trend = run_page(tmp_path, cost(root))["cost-trend"]

    [chart] = find(trend, "svg")
    assert segments(chart) == []


# -- month to date -------------------------------------------------------------------------


def test_month_to_date_shows_usage_credits_and_net_apart(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    services = run_page(tmp_path, cost(root))["cost-services"]

    [figures] = find(services, "div", "figures")
    assert [text_of(item) for item in figures["children"]] == [
        "Usage$10.55",
        "Credits−$10.55",
        "Tax, fees and other lines$0.10",
        "Net$0.10",
    ]
    [chart] = find(services, "svg")
    bars = [n for n in find(chart, "path")]
    assert [n["cls"] for n in bars] == ["s1", "s2"]
    assert [text_of(n) for n in find(chart, "text")] == [
        "EC2, the VM and its volumes",
        "$10.15",
        "S3",
        "$0.40",
    ]
    [rows] = tables(services)
    assert rows[1] == ["AmazonEC2", "$10.15", "$3.50", "$13.65"]


def test_month_to_date_of_an_old_report_keeps_its_table(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=october()))

    services = run_page(tmp_path, cost(root))["cost-services"]

    assert find(services, "div", "figures") == []
    # The usage bars read ``mtd.service``, which every report carries.
    assert len(find(services, "svg")) == 1
    [rows] = tables(services)
    assert rows[0] == ["Service", "Usage", "Other lines", "Total"]


# -- S3 ----------------------------------------------------------------------------------


def test_each_bucket_draws_its_billed_size_as_a_line(root: Path, tmp_path: Path):
    previous = september(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-09-20": 90 / 30}}}
    )
    current = split_october(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 93.1 / 31}}}
    )
    file(
        root,
        "2026-10-16",
        entry("2026-10-16", "2026-10", "delivery", current=current, previous=previous),
    )

    s3 = run_page(tmp_path, cost(root))["cost-s3"]

    [chart] = find(s3, "svg")
    [line] = find(chart, "path", "line-s1")
    assert line["attrs"]["d"].count("L") == 1
    texts = [text_of(n) for n in find(chart, "text")]
    assert "93.10 GB" in texts
    assert texts[-2:] == ["Sep 20", "Oct 14"]
    tables_found = tables(s3)
    assert tables_found[0] == [["Day", "Billed size"], ["2026-09-20", "90.00 GB"],
                               ["2026-10-14", "93.10 GB"]]


def test_a_bucket_with_one_sized_day_draws_no_line(root: Path, tmp_path: Path):
    current = split_october(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 3.0}}}
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    s3 = run_page(tmp_path, cost(root))["cost-s3"]

    assert find(s3, "svg") == []
    assert "Billed size on 2026-10-14: 93.00 GB" in text_of(s3)


# -- width ---------------------------------------------------------------------------------


@pytest.mark.parametrize("width", [343, 1068])
def test_a_chart_takes_its_containers_width(root: Path, tmp_path: Path, width: int):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root), width=width)

    for section in ("cost-services", "cost-trend"):
        [chart] = find(sections[section], "svg")
        assert chart["attrs"]["width"] == str(width)


def test_a_resize_redraws_at_the_new_width(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root), width=900, resize_to=343)

    [chart] = find(sections["cost-trend"], "svg")
    assert chart["attrs"]["width"] == "343"
