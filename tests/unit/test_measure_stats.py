"""The measurement helpers, decided from values alone.

Percentiles and OCC-symbol expiration parsing are pure functions over values. So the
tier is unit. The query over real journal segments lives in the component tier.
"""

from __future__ import annotations

from datetime import date

from lake.measure import _occ_expiry, _percentile


def test_percentile_of_empty_is_none():
    assert _percentile([], 0.5) is None


def test_percentile_of_a_single_value():
    assert _percentile([0.42], 0.5) == 0.42
    assert _percentile([0.42], 0.99) == 0.42


def test_percentile_interpolates():
    values = [0.0, 1.0, 2.0, 3.0, 4.0]
    assert _percentile(values, 0.5) == 2.0
    # p99 of five points lands almost at the top by linear interpolation.
    assert _percentile(values, 0.99) == 3.96


def test_occ_expiry_parses_the_yymmdd():
    assert _occ_expiry("SPY   260918C00650000") == date(2026, 9, 18)
    assert _occ_expiry("QQQ   260827P00600000") == date(2026, 8, 27)


def test_occ_expiry_rejects_junk():
    assert _occ_expiry(None) is None
    assert _occ_expiry("") is None
    assert _occ_expiry("SPY   ZZZZZZC00650000") is None
    # An impossible month is refused rather than raising.
    assert _occ_expiry("SPY   261398C00650000") is None
