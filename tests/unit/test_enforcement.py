"""The enforcement scanners must catch real bypasses and must not cry wolf.

These tests exercise the two scanners on inline snippets. A guard that never fires,
or one that fires on innocent code, is worse than none. So both the positive and the
negative cases are pinned here.
"""

from __future__ import annotations

import pytest

from tests.support.enforcement import (
    find_clock_violations,
    find_session_time_violations,
    scan_source,
)

# -- clock scanner: calls it must catch --------------------------------------

CLOCK_HITS = [
    "from datetime import datetime\nx = datetime.now()\n",
    "from datetime import datetime\nx = datetime.utcnow()\n",
    "import datetime\nx = datetime.datetime.now()\n",
    "import datetime as dt\nx = dt.datetime.today()\n",
    "from datetime import date\nx = date.today()\n",
    "import time\nx = time.monotonic()\n",
    "import time as t\nx = t.time()\n",
    "from time import sleep\nsleep(1)\n",
    "import pandas as pd\nx = pd.Timestamp.now()\n",
    "from pandas import Timestamp\nx = Timestamp.today()\n",
    # The pandas string constructor reads the clock too.
    "import pandas as pd\nx = pd.Timestamp('now')\n",
    "from pandas import Timestamp\nx = Timestamp('today')\n",
]

# -- clock scanner: calls it must ignore -------------------------------------

CLOCK_MISSES = [
    # Constructing a datetime is not reading the clock.
    "import datetime\nx = datetime.datetime(2026, 8, 24, 16, 15)\n",
    # A user object with its own now() method is not the system clock.
    "class C:\n    def now(self):\n        return 1\n\n\nC().now()\n",
    # Constructing a time-of-day is a session-time concern, not a clock read.
    "import datetime\nx = datetime.time(16, 15)\n",
    # A pandas Timestamp from a fixed value is not a clock read.
    "import pandas as pd\nx = pd.Timestamp('2026-08-24')\n",
    # Injected clock use is exactly the point.
    "def f(clock):\n    return clock.now()\n",
]


@pytest.mark.parametrize("source", CLOCK_HITS)
def test_clock_scanner_catches(source: str):
    assert scan_source(source, "clock"), source


@pytest.mark.parametrize("source", CLOCK_MISSES)
def test_clock_scanner_ignores(source: str):
    assert not scan_source(source, "clock"), source


# -- session-time scanner: literals it must catch ----------------------------

SESSION_HITS = [
    'x = "16:15"\n',
    'x = "9:30"\n',
    'x = "13:00:00"\n',
    "import datetime\nx = datetime.time(16, 15)\n",
    "from datetime import time\nx = time(13, 0)\n",
    "y = base.replace(hour=16, minute=0)\n",
    "y = base.replace(minute=15)\n",
]

# -- session-time scanner: code it must ignore -------------------------------

SESSION_MISSES = [
    # A full ISO timestamp is not a bare time-of-day literal.
    'x = "2026-08-24T16:15:00-04:00"\n',
    # A duration is not a time of day.
    "from datetime import timedelta\nx = timedelta(minutes=15)\n",
    # A host:port is not a time.
    'x = "127.0.0.1:8080"\n',
    # A split ratio is not a time.
    'x = "3:2"\n',
    # String replacement is not building a time.
    'x = text.replace("a", "b")\n',
    # Replacing a non-time field is fine.
    "x = d.replace(day=1)\n",
    # Flooring to the minute zeroes second and microsecond. It is not a time literal.
    "ts = base.replace(second=0, microsecond=0)\n",
    # Reading the clock is the clock scanner's job, not this one.
    "import time\nx = time.time()\n",
]


@pytest.mark.parametrize("source", SESSION_HITS)
def test_session_scanner_catches(source: str):
    assert scan_source(source, "session_time"), source


@pytest.mark.parametrize("source", SESSION_MISSES)
def test_session_scanner_ignores(source: str):
    assert not scan_source(source, "session_time"), source


def test_unknown_kind_rejected():
    with pytest.raises(ValueError):
        scan_source("x = 1\n", "nonsense")


def test_tree_walk_flags_violations_and_honors_carveouts(tmp_path):
    # This exercises the exact tree-walking entry points the two continuous-integration
    # seam tests call. So neither of those tests can pass vacuously if the walk, the
    # kind, or the carve-out ever regresses.
    root = tmp_path / "lake"
    root.mkdir()
    (root / "clock.py").write_text(
        "from datetime import datetime\n\n\ndef now():\n    return datetime.now()\n"
    )
    (root / "calendar.py").write_text('OPEN = "9:30"\n')
    (root / "worker.py").write_text(
        'from datetime import datetime\n\nfetched = datetime.now()\nclose = "16:15"\n'
    )

    clock = find_clock_violations(root=root)
    session = find_session_time_violations(root=root)

    assert [v.path.rsplit("/", 1)[-1] for v in clock] == ["worker.py"]
    assert "datetime.now()" in clock[0].detail
    assert [v.path.rsplit("/", 1)[-1] for v in session] == ["worker.py"]
    assert "16:15" in session[0].detail
