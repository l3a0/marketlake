"""The chain chunk plan: a set of date windows read off the hot path.

A full SPY option chain in one request exceeds Schwab's gateway body limit, the ``502``
``protocol.http.TooBigBody`` fault. So the chain is fetched in pieces. This module owns
the plan that decides those pieces. The plan is a set of *date windows*. A window is a
``fromDate``/``toDate`` range sized so its slice of the chain stays under the body limit.

The plan is *date-relative*. A window is a pair of day offsets from the session date,
never a pair of fixed calendar dates. So the plan never goes stale as the calendar rolls,
and a newly listed expiration falls inside an existing window without a lookup. The near
term is dense and the far term sparse, so the near-term windows are narrow and the
far-term ones wide. The last window is open-ended, its end left ``None``, so a new
far-dated series is caught too.

Three terms recur, defined here at first use.

- A *window* is one ``(start_days, end_days)`` day-offset range. ``end_days`` is ``None``
  on the final window, which runs from its start to infinity. ``windows_for`` turns a
  window into a concrete ``(from_date, to_date)`` pair against a session date.
- A *tiling* is an ordered list of windows that covers ``[0, ∞)`` with no gap and no
  overlap. The first window starts at offset 0. Each next window starts one day after the
  previous window's last day. The final window's end is ``None``. ``ChainPlan`` validates
  this on construction, so a malformed plan never reaches the fetcher.
- The *built-in default* is the plan seeded from the day-one measurement. It carries the
  load when the plan file is absent or unreadable, so a fresh machine needs no file and a
  corrupt one fails safe.

This module reads no wall clock. ``windows_for`` takes the session date as an argument. It
never calls ``date.today``. The one thing it does touch is the plan file, read by
``load_chain_plan``, which never raises on the hot path: any failure falls back to the
built-in default.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# The machine-owned plan file, beside ``token.json`` in the config dir. It is
# *machine-derived* rather than hand config: the nightly job writes it, so no job ever
# rewrites the hand-owned ``config.yaml``. Absent on a fresh machine, which is fine, since
# ``load_chain_plan`` falls back to the built-in default.
DEFAULT_CHAIN_PLAN_PATH = Path.home() / ".config" / "marketlake" / "chain_plan.json"

# One window: a start day-offset and an end day-offset, the end ``None`` on the open tail.
Window = tuple[int, "int | None"]


class ChainPlanError(ValueError):
    """Raised when a window list does not tile ``[0, ∞)`` contiguously."""


@dataclass(frozen=True)
class ChainPlan:
    """An ordered tiling of ``[0, ∞)`` into day-offset windows.

    The windows tile the offset line with no gap and no overlap. The first window starts
    at 0. Each subsequent window starts one day after the previous window's last day, so
    ``start == previous end + 1``. Only the final window is open-ended, its end ``None``.
    The invariant is checked on construction, so an invalid plan cannot exist.
    """

    windows: tuple[Window, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "windows", tuple(self.windows))
        self._validate()

    def _validate(self) -> None:
        windows = self.windows
        if not windows:
            raise ChainPlanError("a chain plan needs at least one window")
        if windows[0][0] != 0:
            raise ChainPlanError(f"the first window must start at offset 0, got {windows[0][0]}")
        if windows[-1][1] is not None:
            raise ChainPlanError(
                f"the last window must be open-ended (end None), got {windows[-1][1]}"
            )
        for index, (start, end) in enumerate(windows):
            last = index == len(windows) - 1
            if last:
                continue
            if end is None:
                raise ChainPlanError(f"only the last window may be open-ended, window {index} is")
            if end < start:
                raise ChainPlanError(f"window {index} ends before it starts: ({start}, {end})")
            next_start = windows[index + 1][0]
            if next_start != end + 1:
                raise ChainPlanError(
                    f"window {index + 1} must start at {end + 1} to tile contiguously, "
                    f"got {next_start}"
                )

    def windows_for(self, session_date: date) -> list[tuple[date, date | None]]:
        """The concrete ``(from_date, to_date)`` fetch ranges for one session date.

        Each window's day offsets are added to ``session_date``. The open tail's end stays
        ``None``, which the fetcher passes straight to ``get_chain`` as ``to_date=None`` so
        Schwab returns every expiration from the start onward. The session date arrives as
        an argument, so this reads no wall clock.
        """
        ranges: list[tuple[date, date | None]] = []
        for start, end in self.windows:
            from_date = session_date + timedelta(days=start)
            to_date = None if end is None else session_date + timedelta(days=end)
            ranges.append((from_date, to_date))
        return ranges


# The built-in default, seeded from the day-one chain-size measurement (run 2026-09-01).
# Cumulative SPY sizes at that measurement: the first 5 expirations (out to +7 days) came
# back at 2.2 MB, 9 expirations (+13 days) at 3.5 MB, 17 expirations (+59 days) at 7.4 MB,
# and the full chain (33 expirations, out to +~1170 days) exceeded the body limit and
# 502'd. So the near term is dense and the far term sparse. This tiling keeps each window
# near or under ~3 MB, erring toward narrow near-term windows where expirations cluster,
# and lets the far term run wide. The offsets are a starting point, not a precise fit: the
# midpoint splitter in the fetcher handles any window that still comes back too big, and
# the nightly job rewrites this plan as the real density profile drifts.
DEFAULT_CHAIN_PLAN = ChainPlan(
    (
        (0, 9),
        (10, 30),
        (31, 90),
        (91, 365),
        (366, None),
    )
)


def _plan_from_mapping(data: object) -> ChainPlan:
    """Build a ``ChainPlan`` from the parsed JSON, raising on any malformed shape.

    The shape is ``{"windows": [{"start": 0, "end": 9}, ..., {"start": 366, "end": null}]}``.
    A missing key, a wrong type, or a tiling that does not validate all raise, which
    ``load_chain_plan`` turns into a fall back to the default.
    """
    if not isinstance(data, dict):
        raise ChainPlanError("plan file must be a JSON object")
    raw_windows = data["windows"]
    if not isinstance(raw_windows, list):
        raise ChainPlanError("'windows' must be a list")
    windows: list[Window] = []
    for entry in raw_windows:
        start = int(entry["start"])
        end_value = entry["end"]
        end = None if end_value is None else int(end_value)
        windows.append((start, end))
    return ChainPlan(tuple(windows))


def load_chain_plan(path: str | Path = DEFAULT_CHAIN_PLAN_PATH) -> ChainPlan:
    """Read and validate the plan file, falling back to the built-in default.

    This is a hot-path read, so it never raises. A missing file, an unreadable file,
    malformed JSON, or a plan that fails the tiling invariant all resolve to
    ``DEFAULT_CHAIN_PLAN``. So a fresh machine with no plan file and a machine with a
    corrupt one both capture with the measured default.
    """
    try:
        raw = Path(path).read_text()
        return _plan_from_mapping(json.loads(raw))
    except Exception:
        return DEFAULT_CHAIN_PLAN


__all__ = [
    "DEFAULT_CHAIN_PLAN",
    "DEFAULT_CHAIN_PLAN_PATH",
    "ChainPlan",
    "ChainPlanError",
    "Window",
    "load_chain_plan",
]
