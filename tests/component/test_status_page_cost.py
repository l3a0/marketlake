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
  var out = { error: null, sections: {}, fetches: __FETCHES, observed: __OBSERVED };
  try {
    await __flush();
    if (__RESIZE_TO) {
      // Mark what the first draw made, and open every table toggle as a reader would, so
      // a test can tell a redraw from none and see whether an open table stays open.
      __SECTIONS.forEach(function (id) {
        (function mark(node) {
          if (node.tagName === "svg") { node.setAttribute("data-first-draw", "1"); }
          if (node.tagName === "details") { node.open = true; node.dispatch("toggle"); }
          node.childNodes.forEach(mark);
        })(document.getElementById(id));
      });
      __WIDTH = __RESIZE_TO;
      (window.listeners.resize || []).forEach(function (fn) { fn({}); });
      __OBSERVERS.forEach(function (fn) { fn([]); });
      __TIMERS.forEach(function (fn) { if (fn) { fn(); } });
      await __flush();
    }
    __SECTIONS.forEach(function (id) { out.sections[id] = __dump(document.getElementById(id)); });
    // Hover each mark of each chart in turn and read the tooltip it shows.
    out.tips = {};
    __SECTIONS.forEach(function (id) {
      var tips = [];
      (function visit(node) {
        if (node.className === "chart") {
          var tip = node.childNodes[0];
          (function hits(n) {
            if (n.className === "hit") {
              n.dispatch("mouseenter");
              var lines = tip.childNodes.map(function (line) { return line.textContent; });
              tips.push({ shown: tip.classList.contains("shown"), lines: lines });
              n.dispatch("mouseleave");
              if (tip.classList.contains("shown")) { throw new Error("the tip stayed shown"); }
            }
            n.childNodes.forEach(hits);
          })(node);
        }
        node.childNodes.forEach(visit);
      })(document.getElementById(id));
      out.tips[id] = tips;
    });
  } catch (e) { out.error = String(e && e.stack || e); }
  var line = "COST_RESULT " + JSON.stringify(out);
  if (typeof console !== "undefined" && console.log) { console.log(line); } else { print(line); }
})();
void 0;
"""


# A ``ResizeObserver`` that records what the page observes, for a host that has one.
OBSERVER = """
var ResizeObserver = function (fn) {
  this.observe = function (node) { __OBSERVED.push(node.id); __OBSERVERS.push(fn); };
};
"""


def run_page(
    tmp_path: Path,
    payload: dict,
    *,
    width: int = 640,
    resize_to: int = 0,
    observer: bool = False,
) -> dict:
    """Execute the page against ``payload`` and return the tree of each Cost section.

    ``observer`` gives the host a ``ResizeObserver``, as every current browser has, and
    leaves it out otherwise, so the page falls back to the window's resize event.
    """
    source = (
        f"var __WIDTH = {width};\n"
        + "var __OBSERVERS = [];\nvar __OBSERVED = [];\n"
        + STUB.read_text(encoding="utf-8")
        + (OBSERVER if observer else "")
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
    sections["tips"] = result["tips"]
    sections["observed"] = result["observed"]
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


def test_the_trend_draws_usage_above_the_baseline_and_credits_below_it(root: Path, tmp_path: Path):
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


def test_hovering_a_trend_day_shows_its_figures(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    tips = run_page(tmp_path, cost(root))["tips"]["cost-trend"]

    assert len(tips) == 30 and all(tip["shown"] for tip in tips)
    assert tips[-2]["lines"] == [
        "Oct 14",
        "Usage $0.38",
        "EC2, the VM and its volumes $0.20",
        "S3 $0.05",
        "VPC, the VM's public address $0.10",
        "Other services $0.03",
        "Credits −$0.38",
        "Tax $0.02",
        "Net $0.02",
    ]
    assert tips[-1]["lines"][:2] == ["Oct 15, a partial day", "Usage $0.10"]
    assert tips[0]["lines"] == ["Sep 16", "No figure yet. The export has not delivered this day."]


def test_a_chart_is_one_named_image_and_its_marks_take_no_focus(root: Path, tmp_path: Path):
    """The table toggle carries every figure to a keyboard and a screen reader, so the
    marks take no tab stop, and each chart names itself and points at the table."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root))

    for section in ("cost-services", "cost-trend"):
        [chart] = find(sections[section], "svg")
        assert chart["attrs"]["role"] == "img"
        assert chart["attrs"]["aria-label"].endswith("The figures are in the table below.")
        assert not [n for n in walk(chart) if "tabindex" in n.get("attrs", {})]


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
    # No chart, since a chart of the net is empty when credits offset every dollar, and the
    # table is drawn open rather than behind a toggle.
    assert find(trend, "svg") == [] and find(trend, "details") == []
    assert find(trend, "div", "chart-legend") == []
    [rows] = tables(trend)
    assert rows[0] == ["Day", "Net", "By service"]
    assert ["2026-10-14", "$0.70", "AmazonEC2 $0.70"] in rows


def test_a_window_with_no_figure_draws_no_column(root: Path, tmp_path: Path):
    current = split_october(daily={}, newest_day="2026-10-15")
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    payload = cost(root)
    trend = run_page(tmp_path, payload)["cost-trend"]

    # No day has a figure, so there is nothing to chart and no old report to name.
    assert payload["trend_split_text"] is None
    assert find(trend, "svg") == []
    [rows] = tables(trend)
    assert {row[1] for row in rows[1:]} == {"no figure yet"}
    assert dashboard.TREND_SPLIT_TEXT not in text_of(trend)


def test_a_window_with_figures_all_zero_draws_its_table_alone(root: Path, tmp_path: Path):
    current = split_october(
        daily={"2026-10-15": {"AmazonEC2": 0.0}},
        daily_usage={"2026-10-15": {"AmazonEC2": 0.0}},
        daily_other={"2026-10-15": {}},
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))
    payload = cost(root)

    trend = run_page(tmp_path, payload)["cost-trend"]

    assert payload["trend_split"] is True and payload["trend_series"] == []
    assert find(trend, "svg") == [] and find(trend, "div", "chart-legend") == []
    assert len(find(trend, "details")) == 1


def test_axis_ticks_never_step_finer_than_a_cent(root: Path, tmp_path: Path):
    """A window of fractions of a cent would otherwise tick every tenth of one, and every
    tick would print $0.00."""
    current = split_october(
        daily={"2026-10-15": {"AmazonEC2": 0.0}},
        daily_usage={"2026-10-15": {"AmazonS3": 0.004}},
        daily_other={"2026-10-15": {"Credit": -0.004}},
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    trend = run_page(tmp_path, cost(root))["cost-trend"]

    [chart] = find(trend, "svg")
    ticks = [text_of(n) for n in find(chart, "text", "axis") if "$" in text_of(n)]
    assert len(ticks) == len(set(ticks)), ticks


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


def test_the_projection_names_the_usage_beside_the_net(root: Path, tmp_path: Path):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    services = run_page(tmp_path, cost(root))["cost-services"]

    assert (
        "Projected usage by month end: $21.70. An estimate: usage through 2026-10-14 scaled "
        "to the whole month. With the credits, tax, fees and other lines to date added "
        "unscaled, $3.50, it is $25.20."
    ) in text_of(services)


def test_a_projection_without_the_usage_figure_reads_as_before(root: Path, tmp_path: Path):
    current = split_october()
    current["projection"]["window_days"] = 0
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    services = run_page(tmp_path, cost(root))["cost-services"]

    assert "Projected month end: $25.20. An estimate: usage through 2026-10-14" in text_of(services)


def test_a_zero_net_keeps_its_tick_above_the_zero_line(root: Path, tmp_path: Path):
    """Credits that offset every dollar put each net tick on the zero line, so the line is
    drawn first and the query's sentence says what the ticks show."""
    current = split_october(
        daily={"2026-10-14": {"AmazonEC2": 0.0}, "2026-10-15": {"AmazonEC2": 0.0}},
        daily_usage={"2026-10-14": {"AmazonEC2": 0.2}, "2026-10-15": {"AmazonEC2": 0.1}},
        daily_other={"2026-10-14": {"Credit": -0.2}, "2026-10-15": {"Credit": -0.1}},
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    trend = run_page(tmp_path, cost(root))["cost-trend"]

    assert dashboard.TREND_OFFSET_TEXT in text_of(trend)
    [chart] = find(trend, "svg")
    order = [
        "baseline" if "baseline" in (n.get("cls") or "") else "net"
        for n in walk(chart)
        if n["tag"] == "line" and ("data-net" in n["attrs"] or "baseline" in (n.get("cls") or ""))
    ]
    assert order == ["baseline", "net", "net"]


def test_a_narrow_bar_chart_puts_each_label_above_its_bar(root: Path, tmp_path: Path):
    """At phone width a label column would leave the bars no room, so each label takes the
    line above its bar and starts where the bar does."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    for width, label_left in ((343, True), (640, False)):
        services = run_page(tmp_path, cost(root), width=width)["cost-services"]
        [chart] = find(services, "svg")
        bars = find(chart, "path")
        labels = [n for n in find(chart, "text") if text_of(n).startswith(("EC2", "S3"))]
        starts = [float(re.match(r"M([-0-9.]+),", bar["attrs"]["d"]).group(1)) for bar in bars]
        if label_left:
            assert starts == [0.0, 0.0]
            for label, bar in zip(labels, bars, strict=True):
                top, _ = path_top_bottom(bar["attrs"]["d"])
                assert float(label["attrs"]["y"]) < top
        else:
            assert all(start > 0 for start in starts)


def test_a_month_with_no_usage_day_says_so_rather_than_null(root: Path, tmp_path: Path):
    current = split_october(newest_day=None, complete_through=None)
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    services = run_page(tmp_path, cost(root))["cost-services"]

    assert "for 2026-10, with no usage day yet, from the delivery of" in text_of(services)
    assert "null" not in text_of(services)


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
    assert all(re.fullmatch(r"[0-9.]+ GB", t) for t in texts if t.endswith("GB")), texts
    tables_found = tables(s3)
    assert tables_found[0] == [
        ["Day", "Billed size"],
        ["2026-09-20", "90.00 GB"],
        ["2026-10-14", "93.10 GB"],
    ]


def test_a_bucket_with_one_sized_day_draws_no_line(root: Path, tmp_path: Path):
    current = split_october(
        storage_daily={"example-lake": {"TimedStorage-ByteHrs": {"2026-10-14": 3.0}}}
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    s3 = run_page(tmp_path, cost(root))["cost-s3"]

    assert find(s3, "svg") == []
    assert "Billed size on 2026-10-14: 93.00 GB" in text_of(s3)


def test_a_bucket_of_a_few_megabytes_reads_in_significant_figures(root: Path, tmp_path: Path):
    current = split_october(
        storage_daily={
            "example-billing": {
                "TimedStorage-ByteHrs": {"2026-10-13": 0.0034 / 31, "2026-10-14": 0.0036 / 31}
            }
        }
    )
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=current))

    s3 = run_page(tmp_path, cost(root))["cost-s3"]

    [chart] = find(s3, "svg")
    assert "0.0036 GB" in [text_of(n) for n in find(chart, "text")]
    assert ["2026-10-14", "0.0036 GB"] in tables(s3)[0]


# -- width ---------------------------------------------------------------------------------


@pytest.mark.parametrize("width", [343, 1068])
def test_a_chart_takes_its_containers_width(root: Path, tmp_path: Path, width: int):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root), width=width)

    for section in ("cost-services", "cost-trend"):
        [chart] = find(sections[section], "svg")
        assert chart["attrs"]["width"] == str(width)


@pytest.mark.parametrize("observer", [True, False], ids=["ResizeObserver", "window resize"])
def test_a_resize_redraws_at_the_new_width(root: Path, tmp_path: Path, observer: bool):
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root), width=900, resize_to=343, observer=observer)

    [chart] = find(sections["cost-trend"], "svg")
    assert chart["attrs"]["width"] == "343"
    assert "data-first-draw" not in chart["attrs"]
    # The tables the reader opened before the redraw are open after it.
    toggles = [n for id in SECTIONS for n in find(sections[id], "details")]
    assert toggles and all(n["open"] for n in toggles)
    # Where the host has an observer, the page watches the Cost panel itself, which a
    # scrollbar narrows without any window resize.
    assert sections["observed"] == (["cost-panel"] if observer else [])


def test_a_resize_that_keeps_the_width_draws_nothing_new(root: Path, tmp_path: Path):
    """A phone fires a resize whenever its address bar shows or hides, which keeps the
    width, so the first draw is still the one on the page."""
    file(root, "2026-10-16", entry("2026-10-16", "2026-10", "delivery", current=split_october()))

    sections = run_page(tmp_path, cost(root), width=640, resize_to=640, observer=True)

    [chart] = find(sections["cost-trend"], "svg")
    assert chart["attrs"]["data-first-draw"] == "1"
