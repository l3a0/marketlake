# Marketlake

Marketlake is a capture-first market data lake. It records full option chains and
equity quotes at one-minute cadence from the Schwab Trader API. Every snapshot not
taken is gone forever. So capture reliability is the first-order concern.

The design doc at [docs/design.md](docs/design.md) is the source of truth. Read it
before proposing any change. The build plan at [docs/build-plan.md](docs/build-plan.md)
sequences the work into deliverables D0 through D21.

## Status

Build in progress. This is slice 1, the capture clock.

The first deliverable, D0, is the test harness. It builds the seams the whole suite
leans on. A seam is an injection point where a real dependency is swapped for a fake
one in a test. There are four seams and one builder.

1. An injected clock, so a test decides what time it is.
2. An injected calendar, so a test decides which sessions and half-days exist.
3. The vendor behind an interface, fed by recorded cassettes. A cassette is a saved
   vendor response replayed offline, so a test never touches the network.
4. The lake root as a temporary directory, so a test writes to a throwaway lake.
5. A fixture-lake builder, which assembles a known lake on disk for a test to read.

Two enforcement tests then stay in continuous integration for the life of the
project.

1. One fails the build on any direct clock call outside the clock module.
2. The other fails the build on any hardcoded session time outside the calendar
   module.

## Layout

Production code lives under `src/lake`. Tests and their fakes live under `tests`.

- `src/lake/clock.py` is the clock module. It is the one place in production code
  that reads wall-clock time.
- `src/lake/calendar.py` is the calendar module. It is the one place in production
  code that names session times.
- `src/lake/vendor.py` and `src/lake/cassette.py` define the vendor interface and the
  cassette format.
- `tests/support` holds the fakes, the fixture-lake builder, and the enforcement
  scanners.

Tests sit in one folder per tier, matching the build plan's placement rule.

- `tests/unit` is decided from values alone with every seam faked. It holds the two
  enforcement guards.
- `tests/component` crosses exactly one real boundary: the real filesystem, or the
  real dependency behind a seam.
- `tests/integration` wires two or more subsystems through real boundaries. It is
  empty until the slice-1 cycle test lands at D7.

## Develop

The toolchain is [uv](https://docs.astral.sh/uv/). Set up the environment, then run
the linter and the test suite.

```bash
uv sync
uv run ruff check
uv run ruff format --check
uv run pytest
```
