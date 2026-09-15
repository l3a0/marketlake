"""``epoch_second_to_utc`` is the guard both token readers share, tested on its own.

Value-only unit tests. Neither a file nor a client is involved, so a defect here would
otherwise only surface through one of the two readers built on top of it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lake.token_epoch import epoch_second_to_utc

MINTED = 1756596600  # 2025-08-30 23:30:00 UTC


def test_reads_an_int_epoch_second_as_an_aware_utc_datetime():
    minted = epoch_second_to_utc(MINTED)
    assert minted == datetime.fromtimestamp(MINTED, tz=UTC)
    assert minted.tzinfo is not None


def test_reads_a_float_epoch_second_too():
    minted = epoch_second_to_utc(MINTED + 0.5)
    assert minted == datetime.fromtimestamp(MINTED + 0.5, tz=UTC)


@pytest.mark.parametrize("stamp", [True, False])
def test_a_bool_is_refused_rather_than_read_as_zero_or_one(stamp):
    # float(True) is 1.0 and float(False) is 0.0, both of which convert without error.
    # A bool is an int to Python, so it must be excluded by name rather than by type.
    with pytest.raises(ValueError, match="epoch second"):
        epoch_second_to_utc(stamp)


def test_a_numeric_string_is_refused():
    # str(MINTED) converts fine under float(), but a numeric string is not a shape
    # schwab-py ever writes into creation_timestamp.
    with pytest.raises(ValueError, match="epoch second"):
        epoch_second_to_utc(str(MINTED))


def test_a_non_numeric_string_is_refused():
    with pytest.raises(ValueError, match="epoch second"):
        epoch_second_to_utc("soon")


def test_an_out_of_range_stamp_raises_value_error_not_overflow_error():
    # datetime.fromtimestamp raises OverflowError on a float this large, and that
    # escaping uncaught is the exact defect this module exists to close.
    with pytest.raises(ValueError, match="epoch second") as excinfo:
        epoch_second_to_utc(10**400)
    # The original is kept as the cause, so a traceback still says what fromtimestamp
    # actually raised.
    assert isinstance(excinfo.value.__cause__, OverflowError)


def test_an_out_of_range_float_also_raises_value_error():
    with pytest.raises(ValueError, match="epoch second"):
        epoch_second_to_utc(1e20)


def test_a_stamp_that_overflows_a_c_long_raises_value_error_not_os_error():
    # This magnitude overflows the platform's underlying C long rather than the float
    # conversion, which datetime.fromtimestamp reports as OSError rather than
    # OverflowError. Both must be refused the same way, or this arm of the guard is
    # covered by nothing and an OSError would escape uncaught instead.
    with pytest.raises(ValueError, match="epoch second") as excinfo:
        epoch_second_to_utc(1e17)
    assert isinstance(excinfo.value.__cause__, OSError)


def test_nan_raises_value_error_from_the_conversion_itself():
    # datetime.fromtimestamp raises its own ValueError on NaN, distinct from the
    # guard's type check above, so this pins the third member of the except tuple.
    with pytest.raises(ValueError, match="epoch second") as excinfo:
        epoch_second_to_utc(float("nan"))
    assert isinstance(excinfo.value.__cause__, ValueError)
