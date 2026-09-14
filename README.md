# Marketlake

Marketlake is a capture-first market data lake. It records full option chains and
equity quotes at one-minute cadence from the Schwab Trader API. Every snapshot not
taken is gone forever. So capture reliability is the first-order concern.

An unbuilt deliverable's issue is the source of truth for its scope. The design doc at
[docs/design.md](docs/design.md) carries the reasoning, the premise, and the
considered-and-rejected register, and it is the source of truth for everything already built.
The build plan at [docs/build-plan.md](docs/build-plan.md) sequences the work into
deliverables D0 through D21 and links each unbuilt one to its issue.

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
- `tests/integration` wires two or more subsystems through real boundaries. It is also
  where a test that needs a second real process lives.

## Develop

The toolchain is [uv](https://docs.astral.sh/uv/). Set up the environment, then run
the linter and the test suite.

```bash
uv sync
uv run ruff check
uv run ruff format --check
uv run pytest
```

### Keep development runs off the real config directory

`~/.config/marketlake/` holds the live Schwab token, and several commands default to it.
`python -m lake.reauth` with no `--token` writes the standard location, which is right
for the weekly ritual and wrong for anyone exercising the tool. On 2026-09-13 that is how
a stub reached the production token path and a working token was lost.

`MARKETLAKE_CONFIG_DIR` moves the whole directory for one process. Set it and the run
cannot reach the real token, the real `config.yaml`, or the real roster, whatever it is
given on the command line.

```bash
MARKETLAKE_CONFIG_DIR=/tmp/marketlake-dev uv run python -m lake.reauth
```

It has to be set before the process starts, because every default is built when the
module is imported. Exporting it in the shell being worked in covers that whole session.

Do not put it in a shell profile. The weekly re-auth runs in that same shell, so a
profile export would send the week's token to a throwaway directory while the daemon
kept reading the real one as it expired. The rendered `reauth.sh` unsets the variable to
make the ritual immune to this, and a re-auth run any other way prints the token path it
wrote, so the sign-off block is where to check it landed where you meant.

The test suite needs none of this, because `tests/conftest.py` covers it three ways. A
guard there fails any test that writes the real directory and names the path. That guard
is a monkeypatch, so it reaches no child process, and the same file therefore exports
`MARKETLAKE_CONFIG_DIR` at a throwaway directory when it is imported. A child inheriting
the suite's environment picks that up, whether or not the test that spawned it arranged
anything. Both of those are checks on the attempt, so the directory is also listed at the
start of a run and again at the end, and a run that changed it fails even when every test
passed.
