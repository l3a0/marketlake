"""Shared fixtures that expose the four seams and the fixture-lake builder."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.cassette import load_cassette
from tests.support.clock import ManualClock
from tests.support.lake import FixtureLake
from tests.support.vendor import CassetteVendor

# A fixed instant for the default manual clock: 2026-08-24 09:30 ET.
_DEFAULT_NOW = datetime(2026, 8, 24, 13, 30, tzinfo=UTC)

CASSETTES = Path(__file__).parent / "cassettes"


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    """A throwaway lake root under the test's temp directory."""
    root = tmp_path / "lake"
    root.mkdir()
    return root


@pytest.fixture
def fixture_lake(lake_root: Path) -> FixtureLake:
    """A fixture-lake builder rooted at a throwaway lake."""
    return FixtureLake(lake_root)


@pytest.fixture
def manual_clock() -> ManualClock:
    """A manual clock a test can advance by hand."""
    return ManualClock(start=_DEFAULT_NOW)


@pytest.fixture
def cassette_vendor() -> CassetteVendor:
    """A cassette-backed vendor over the checked-in minimal cassette."""
    return CassetteVendor(load_cassette(CASSETTES / "spy_minimal.json"))
