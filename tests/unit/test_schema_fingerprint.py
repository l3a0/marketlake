"""The derived schema fingerprint, checked against the hand-set ``SCHEMA_VERSION``.

``schema_version`` is the only provenance that survives a seal. Compaction unlinks a
ticker-day's segments once the partition is manifested, so after that moment the
per-segment schemas are gone and the integer stamped on every row is the sole record of
which code shape wrote it. That is the whole answer to a dropped column, which has no
repair, because the values were never written.

The enforcement is a pair, and each half does a job the other cannot.

1. The *fingerprint* is derived, by ``journal.schema_fingerprint``, from the pinned
   schemas themselves. Nothing restates the column set by hand, so nothing can go stale.
2. ``SCHEMA_VERSION`` stays hand-set, so it stays orderable. A read-time projection has
   to ask whether a row sits below the version that promoted a field out of ``extra``
   into its own column, and that is an inequality a digest cannot answer. A hand-set
   version also lets a human mint one for a change that leaves the shape alone, as when
   the vendor keeps ``open_interest`` an int64 and changes what it counts.

``RECORDED_FINGERPRINTS`` below records what each version's shape is. A column added,
dropped, or retyped moves the derived fingerprint away from the recorded one and fails
the suite, and the fix the failure names is a deliberate bump with its own entry.

These tests read the schema objects in memory. No file, process, or query engine is
crossed, so they sit in the unit tier.

The map recorded here is the in-repo half, and it is not the only copy. Read against a
sealed partition, the map is what says a null on a version N+1 row is a non-observation
rather than a vendor null, and a map that lives only in the repo cannot be read beside a
restored backup. So ``lake.schema_versions`` writes the same shape into the lake itself,
at ``reference/schema_versions.parquet``, where it restores with the data.

The two are not a second source of truth for the same fact. Both read the shape off
``journal.schema_fingerprint``, which is the one derivation. What each adds is different.
The literal below is what forces a human to notice a shape change, because a test cannot
read the owner's machine-local lake. The ledger is what lets a reader interpret a version
long after the code that wrote it is gone.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from lake import journal, paths

# A surface the lake lays out a directory for and this module pins no capture schema for,
# which is what these tests need a stand-in for. It was ``bars`` until marketlake #336
# pinned one. ``actions`` is a single append-only ledger rather than a measurement, so it
# has no capture schema to pin, but the right response to this going stale again is to
# repoint it rather than to assume it cannot.
UNPINNED_SURFACE = paths.ACTIONS

# The column-name-to-type mapping of every pinned surface, per schema version. Types are
# spelled the way pyarrow renders them, so ``pa.float64()`` reads as ``double``. Each
# surface's entry is written in schema order for readability, though order is not part of
# the comparison. Never edit an existing version's entry. Rows already written at that
# version carry the integer alone, so rewriting what a version meant makes those rows
# unreadable. A shape change adds a new version instead.
RECORDED_FINGERPRINTS: dict[int, dict[str, dict[str, str]]] = {
    1: {
        journal.CHAINS_SURFACE: {
            "snap_ts": "string",
            "fetch_ts": "string",
            "fetch_end_ts": "string",
            "vendor_quote_ts": "string",
            "ticker": "string",
            "occ_symbol": "string",
            "put_call": "string",
            "bid": "double",
            "ask": "double",
            "last": "double",
            "bid_size": "int64",
            "ask_size": "int64",
            "last_size": "int64",
            "bid_ask_size": "string",
            "open_interest": "int64",
            "volume": "int64",
            "open_price": "double",
            "high_price": "double",
            "low_price": "double",
            "close_price": "double",
            "mark": "double",
            "mark_change": "double",
            "mark_percent_change": "double",
            "net_change": "double",
            "percent_change": "double",
            "volatility": "double",
            "delta": "double",
            "gamma": "double",
            "theta": "double",
            "vega": "double",
            "rho": "double",
            "theoretical_option_value": "double",
            "theoretical_volatility": "double",
            "intrinsic_value": "double",
            "extrinsic_value": "double",
            "time_value": "double",
            "break_even": "double",
            "high_52_week": "double",
            "low_52_week": "double",
            "strike_price": "double",
            "multiplier": "double",
            "days_to_expiration": "int64",
            "expiration_date": "string",
            "expiration_type": "string",
            "exercise_type": "string",
            "settlement_type": "string",
            "option_root": "string",
            "deliverable_note": "string",
            "description": "string",
            "exchange_name": "string",
            "option_deliverables_list": "string",
            "in_the_money": "bool",
            "non_standard": "bool",
            "mini": "bool",
            "penny_pilot": "bool",
            "ssid": "int64",
            "last_trading_day": "int64",
            "trade_time": "int64",
            "interest_rate": "double",
            "underlying_price": "double",
            "dividend_yield": "double",
            "is_delayed": "bool",
            "is_chain_truncated": "bool",
            "number_of_contracts": "int64",
            "row_kind": "string",
            "error_class": "string",
            "suspect": "bool",
            "close_tag": "string",
            "session_phase": "string",
            "schema_version": "int64",
            "extra": "string",
            "window_start": "string",
            "window_end": "string",
        },
        journal.QUOTES_SURFACE: {
            "snap_ts": "string",
            "fetch_ts": "string",
            "fetch_end_ts": "string",
            "vendor_quote_ts": "string",
            "ticker": "string",
            "bid": "double",
            "ask": "double",
            "last": "double",
            "bid_size": "int64",
            "ask_size": "int64",
            "last_size": "int64",
            "bid_mic_id": "string",
            "ask_mic_id": "string",
            "last_mic_id": "string",
            "bid_time": "int64",
            "ask_time": "int64",
            "trade_time": "int64",
            "high_price": "double",
            "low_price": "double",
            "open_price": "double",
            "close_price": "double",
            "mark": "double",
            "mark_change": "double",
            "mark_percent_change": "double",
            "net_change": "double",
            "net_percent_change": "double",
            "post_market_change": "double",
            "post_market_percent_change": "double",
            "total_volume": "int64",
            "volatility": "double",
            "week_52_high": "double",
            "week_52_low": "double",
            "security_status": "string",
            "realtime": "bool",
            "cusip": "string",
            "div_pay_amount": "double",
            "div_ex_date": "string",
            "div_amount": "double",
            "div_freq": "int64",
            "declaration_date": "string",
            "next_div_ex_date": "string",
            "next_div_pay_date": "string",
            "div_pay_date": "string",
            "div_yield": "double",
            "pe_ratio": "double",
            "eps": "double",
            "high_52": "double",
            "low_52": "double",
            "avg_10_days_volume": "double",
            "avg_1_year_volume": "double",
            "last_earnings_date": "string",
            "fund_leverage_factor": "double",
            "shares_outstanding": "int64",
            "regular_market_last_price": "double",
            "regular_market_last_size": "int64",
            "regular_market_net_change": "double",
            "regular_market_percent_change": "double",
            "regular_market_trade_time": "int64",
            "extended_last_price": "double",
            "extended_bid_price": "double",
            "extended_ask_price": "double",
            "extended_bid_size": "int64",
            "extended_ask_size": "int64",
            "extended_last_size": "int64",
            "extended_mark": "double",
            "extended_quote_time": "int64",
            "extended_trade_time": "int64",
            "extended_total_volume": "int64",
            "row_kind": "string",
            "error_class": "string",
            "suspect": "bool",
            "close_tag": "string",
            "session_phase": "string",
            "schema_version": "int64",
            "extra": "string",
        },
    },
    # Version 2 pins ``bars``. Chains and quotes are version 1's entries copied verbatim,
    # which is what this file's own note says a bump that leaves a surface's columns alone
    # calls for. ``test_the_bump_that_pinned_bars_left_the_capture_surfaces_alone`` below
    # compares the two copies, so a typo in one of them fails rather than passing as a
    # shape change nobody made.
    2: {
        journal.CHAINS_SURFACE: {
            "snap_ts": "string",
            "fetch_ts": "string",
            "fetch_end_ts": "string",
            "vendor_quote_ts": "string",
            "ticker": "string",
            "occ_symbol": "string",
            "put_call": "string",
            "bid": "double",
            "ask": "double",
            "last": "double",
            "bid_size": "int64",
            "ask_size": "int64",
            "last_size": "int64",
            "bid_ask_size": "string",
            "open_interest": "int64",
            "volume": "int64",
            "open_price": "double",
            "high_price": "double",
            "low_price": "double",
            "close_price": "double",
            "mark": "double",
            "mark_change": "double",
            "mark_percent_change": "double",
            "net_change": "double",
            "percent_change": "double",
            "volatility": "double",
            "delta": "double",
            "gamma": "double",
            "theta": "double",
            "vega": "double",
            "rho": "double",
            "theoretical_option_value": "double",
            "theoretical_volatility": "double",
            "intrinsic_value": "double",
            "extrinsic_value": "double",
            "time_value": "double",
            "break_even": "double",
            "high_52_week": "double",
            "low_52_week": "double",
            "strike_price": "double",
            "multiplier": "double",
            "days_to_expiration": "int64",
            "expiration_date": "string",
            "expiration_type": "string",
            "exercise_type": "string",
            "settlement_type": "string",
            "option_root": "string",
            "deliverable_note": "string",
            "description": "string",
            "exchange_name": "string",
            "option_deliverables_list": "string",
            "in_the_money": "bool",
            "non_standard": "bool",
            "mini": "bool",
            "penny_pilot": "bool",
            "ssid": "int64",
            "last_trading_day": "int64",
            "trade_time": "int64",
            "interest_rate": "double",
            "underlying_price": "double",
            "dividend_yield": "double",
            "is_delayed": "bool",
            "is_chain_truncated": "bool",
            "number_of_contracts": "int64",
            "row_kind": "string",
            "error_class": "string",
            "suspect": "bool",
            "close_tag": "string",
            "session_phase": "string",
            "schema_version": "int64",
            "extra": "string",
            "window_start": "string",
            "window_end": "string",
        },
        journal.QUOTES_SURFACE: {
            "snap_ts": "string",
            "fetch_ts": "string",
            "fetch_end_ts": "string",
            "vendor_quote_ts": "string",
            "ticker": "string",
            "bid": "double",
            "ask": "double",
            "last": "double",
            "bid_size": "int64",
            "ask_size": "int64",
            "last_size": "int64",
            "bid_mic_id": "string",
            "ask_mic_id": "string",
            "last_mic_id": "string",
            "bid_time": "int64",
            "ask_time": "int64",
            "trade_time": "int64",
            "high_price": "double",
            "low_price": "double",
            "open_price": "double",
            "close_price": "double",
            "mark": "double",
            "mark_change": "double",
            "mark_percent_change": "double",
            "net_change": "double",
            "net_percent_change": "double",
            "post_market_change": "double",
            "post_market_percent_change": "double",
            "total_volume": "int64",
            "volatility": "double",
            "week_52_high": "double",
            "week_52_low": "double",
            "security_status": "string",
            "realtime": "bool",
            "cusip": "string",
            "div_pay_amount": "double",
            "div_ex_date": "string",
            "div_amount": "double",
            "div_freq": "int64",
            "declaration_date": "string",
            "next_div_ex_date": "string",
            "next_div_pay_date": "string",
            "div_pay_date": "string",
            "div_yield": "double",
            "pe_ratio": "double",
            "eps": "double",
            "high_52": "double",
            "low_52": "double",
            "avg_10_days_volume": "double",
            "avg_1_year_volume": "double",
            "last_earnings_date": "string",
            "fund_leverage_factor": "double",
            "shares_outstanding": "int64",
            "regular_market_last_price": "double",
            "regular_market_last_size": "int64",
            "regular_market_net_change": "double",
            "regular_market_percent_change": "double",
            "regular_market_trade_time": "int64",
            "extended_last_price": "double",
            "extended_bid_price": "double",
            "extended_ask_price": "double",
            "extended_bid_size": "int64",
            "extended_ask_size": "int64",
            "extended_last_size": "int64",
            "extended_mark": "double",
            "extended_quote_time": "int64",
            "extended_trade_time": "int64",
            "extended_total_volume": "int64",
            "row_kind": "string",
            "error_class": "string",
            "suspect": "bool",
            "close_tag": "string",
            "session_phase": "string",
            "schema_version": "int64",
            "extra": "string",
        },
        journal.BARS_SURFACE: {
            "bar_ts": "string",
            "fetch_ts": "string",
            "fetch_end_ts": "string",
            "ticker": "string",
            "instrument_id": "int64",
            "freq": "string",
            "open": "double",
            "high": "double",
            "low": "double",
            "close": "double",
            "volume": "int64",
            "window_start": "string",
            "window_end": "string",
            "extended_hours": "bool",
            "schema_version": "int64",
            "extra": "string",
        },
    },
}


def _drift_report(
    surface: str, version: int, derived: dict[str, str], recorded: dict[str, str]
) -> str:
    """Name every column that moved, then name the bump as the fix.

    Naming the columns is the reason the fingerprint is a column list rather than a
    digest. A digest says only that something moved. Which columns moved comes from
    ``journal.fingerprint_diff``, so the ledger's own refusal message and this one agree
    on what counts as a change.
    """
    diff = journal.fingerprint_diff(derived, recorded)
    retyped = [f"{name} {was} -> {now}" for name, was, now in diff.retyped]
    return "\n".join(
        [
            f"the {surface} shape no longer matches the fingerprint recorded for "
            f"schema_version {version}.",
            f"  dropped: {', '.join(diff.dropped) or 'none'}",
            f"  added: {', '.join(diff.added) or 'none'}",
            f"  retyped: {', '.join(retyped) or 'none'}",
            "Fix: bump journal.SCHEMA_VERSION and record the new shape under that new "
            "version in RECORDED_FINGERPRINTS.",
            "Do not edit the entry of a version already recorded here. Sealed rows carry "
            "the version integer alone, so rewriting what a version meant makes those "
            "rows unreadable.",
        ]
    )


# -- the version and the shapes it names -------------------------------------


def test_schema_version_is_the_newest_recorded_version():
    """The hand-set version and the recorded shapes move together.

    A bump with no entry fails here. That is the half a derived version would lose,
    because a semantic change that leaves the columns alone still mints a version and
    only a human knows it did.
    """
    newest = max(RECORDED_FINGERPRINTS)
    assert journal.SCHEMA_VERSION == newest, (
        f"journal.SCHEMA_VERSION is {journal.SCHEMA_VERSION} and the newest version "
        f"recorded in RECORDED_FINGERPRINTS is {newest}. A bump needs its own entry "
        "holding the shape that version writes. Copy the previous entry verbatim when "
        "the bump records a semantic change that leaves the columns alone."
    )


def test_recorded_versions_are_positive_and_start_at_one():
    """Version numbers are an ordered sequence, which is why they are not derived."""
    assert sorted(RECORDED_FINGERPRINTS) == list(range(1, len(RECORDED_FINGERPRINTS) + 1))


def test_the_pinned_set_is_enumerated_rather_than_assumed():
    """A surface added to one half and not the other fails here.

    The tuple goes against the module's own schema map first, so a surface added to the map
    and not to the tuple fails. Then the tuple is spelled out, so the set is enumerated
    rather than assumed. The first alone would pass a hand-written tuple that happens to
    match, and the second alone would pass a surface added to neither.
    """
    assert journal.PINNED_SURFACES == tuple(journal._SCHEMAS)
    assert journal.PINNED_SURFACES == (
        journal.CHAINS_SURFACE,
        journal.QUOTES_SURFACE,
        journal.BARS_SURFACE,
    )


def test_the_newest_version_covers_every_pinned_surface():
    """The running shape is recorded in full, so no pinned surface is recorded nowhere.

    Only the newest version is held to this. An older version covers the surfaces that
    existed when it was minted, which is what makes the record an honest history rather
    than a running total. Version 1 knew two surfaces and version 2 knows three.
    """
    newest = max(RECORDED_FINGERPRINTS)
    assert tuple(RECORDED_FINGERPRINTS[newest]) == journal.PINNED_SURFACES


# The version each surface was first recorded under. Spelled out, because it is the one
# fact about the record's history that nothing else can derive: the schemas say what a
# surface is now, and the growth rule below says the set never shrinks, but neither can tell
# a surface added at version N from one silently deleted from N-1.
FIRST_RECORDED_AT = {
    journal.CHAINS_SURFACE: 1,
    journal.QUOTES_SURFACE: 1,
    journal.BARS_SURFACE: 2,
}


def test_each_surface_first_appears_at_the_version_that_pinned_it():
    """A surface's debut is fixed, so an earlier version's entry cannot be edited out.

    The growth rule below is not enough on its own once a third version exists. Deleting
    bars from version 2 while version 3 carries them leaves every set still growing pairwise
    and every other rule here satisfied, and it is a false history: rows stamped 2 would read
    back through a ledger with no version-2 bars shape, so the projection fills nothing and
    the loader refuses. Pinning the debut is what catches that, and it is the assertion this
    file's own "do not edit the entry of a version already recorded here" needs to mean
    something.
    """
    assert set(FIRST_RECORDED_AT) == set(journal.PINNED_SURFACES)
    for surface, first in FIRST_RECORDED_AT.items():
        debut = min(
            version for version, surfaces in RECORDED_FINGERPRINTS.items() if surface in surfaces
        )
        assert debut == first, (surface, debut, first)


def test_a_version_never_drops_a_surface_an_earlier_one_recorded():
    """The surface set grows up the sequence and never shrinks.

    This is what the old whole-sequence rule was really protecting. A surface recorded at
    version N and missing at N+1 would leave rows at N+1 with no recorded shape while the
    surface plainly still exists, and ``has_column`` would answer false for every column of
    it. Growth is checked pair by pair rather than against the newest, so a surface dropped
    in the middle of the sequence and restored at the top still fails.
    """
    versions = sorted(RECORDED_FINGERPRINTS)
    for lower, upper in zip(versions, versions[1:], strict=False):
        below = set(RECORDED_FINGERPRINTS[lower])
        above = set(RECORDED_FINGERPRINTS[upper])
        assert below <= above, (lower, upper, sorted(below - above))


def test_no_version_records_a_surface_the_code_no_longer_pins():
    """A recorded surface that is not pinned would be a shape nothing can derive again."""
    pinned = set(journal.PINNED_SURFACES)
    for version, surfaces in RECORDED_FINGERPRINTS.items():
        assert set(surfaces) <= pinned, (version, sorted(set(surfaces) - pinned))


def test_version_one_does_not_claim_it_captured_bars():
    """The retroactive claim this file's own note refuses, stated as an assertion.

    Adding bars to version 1's entry would make every row already written at version 1 say
    the code that wrote it knew a surface that did not exist. The entry cannot be read back
    off anything, so nothing else in the suite would notice.
    """
    assert journal.BARS_SURFACE not in RECORDED_FINGERPRINTS[1]
    assert tuple(RECORDED_FINGERPRINTS[1]) == (journal.CHAINS_SURFACE, journal.QUOTES_SURFACE)


def test_the_bump_that_pinned_bars_left_the_capture_surfaces_alone():
    """Version 2's chains and quotes are version 1's, copied verbatim.

    The copy is what this file's own note calls for when a bump leaves a surface's columns
    alone. Comparing the two copies is what keeps the duplication honest: a typo in either
    one reads as a shape change nobody made, and version 1 is compared against nothing else
    in the suite.
    """
    for surface in (journal.CHAINS_SURFACE, journal.QUOTES_SURFACE):
        assert RECORDED_FINGERPRINTS[2][surface] == RECORDED_FINGERPRINTS[1][surface], surface


@pytest.mark.parametrize("surface", journal.PINNED_SURFACES)
def test_surface_shape_matches_the_newest_recorded_fingerprint(surface):
    """A column added, dropped, or retyped without a bump fails the suite here.

    The comparison is against the newest recorded version rather than against whatever
    ``SCHEMA_VERSION`` currently reads. The test above owns the coupling between the two,
    so a bump with no entry fails there with a message rather than here on a missing key.
    """
    newest = max(RECORDED_FINGERPRINTS)
    derived = journal.schema_fingerprint(surface)
    recorded = RECORDED_FINGERPRINTS[newest][surface]
    assert derived == recorded, _drift_report(surface, newest, derived, recorded)


# -- the fingerprint is read off the schema, not restated ---------------------


@pytest.mark.parametrize("surface", journal.PINNED_SURFACES)
def test_fingerprint_is_derived_from_the_schema_object(surface):
    """The fingerprint reads the schema rather than repeating it.

    This is what keeps the record from going stale. A fingerprint frozen into a literal
    of its own would pass the comparison above on the day it was written and drift from
    the schema silently afterwards. Here the names and the types both come from the
    schema itself.
    """
    schema = journal.schema_for(surface)
    assert journal.schema_fingerprint(surface) == {
        name: str(schema.field(name).type) for name in schema.names
    }


def test_fingerprint_uses_only_the_type_spellings_recorded_here():
    """Every type spelling in use is enumerated, so a new one is examined rather than assumed.

    The fingerprint records a type as pyarrow renders it, and that rendering is pyarrow's
    contract rather than this repo's. Naming the whole set in use means a spelling that
    changes, under a pyarrow upgrade or a schema that starts using a type not listed here,
    fails loudly instead of quietly moving every recorded fingerprint at once.
    """
    seen = {
        type_name
        for surface in journal.PINNED_SURFACES
        for type_name in journal.schema_fingerprint(surface).values()
    }
    assert seen == {"string", "double", "int64", "bool"}, seen


def test_a_reordered_schema_produces_the_same_fingerprint(monkeypatch):
    """Order is excluded on purpose, so a pure reorder does not fail the suite.

    What a version records is which values it captured, and a reorder captures the same
    values. So a reorder mints no version, and the recorded fingerprint has to survive
    one.

    The reorder is fed through a real schema rather than through a rearranged copy of the
    output, because rearranging the output would only exercise ``dict.__eq__``. Swapping
    the module's schema for a reordered one exercises ``schema_fingerprint`` itself, so a
    version of it that returned an ordered sequence would fail here.
    """
    original = journal.schema_for(journal.QUOTES_SURFACE)
    reordered = pa.schema(list(reversed(list(original))))
    assert reordered.names != original.names
    monkeypatch.setitem(journal._SCHEMAS, journal.QUOTES_SURFACE, reordered)
    newest = max(RECORDED_FINGERPRINTS)
    assert (
        journal.schema_fingerprint(journal.QUOTES_SURFACE)
        == RECORDED_FINGERPRINTS[newest][journal.QUOTES_SURFACE]
    )


def test_the_drift_report_names_what_moved_and_the_bump():
    """The failure message is the reason the fingerprint is a column list, so it is covered.

    ``_drift_report`` runs only on a failing assertion, so the suite passing says nothing
    about what it would have said. This calls it directly on a synthetic pair carrying one
    of each kind of change.
    """
    recorded = {"kept": "string", "dropped_one": "double", "retyped_one": "int64"}
    derived = {"kept": "string", "retyped_one": "double", "added_one": "bool"}
    report = _drift_report("chains", 1, derived, recorded)
    assert "the chains shape no longer matches" in report
    assert "schema_version 1" in report
    assert "dropped: dropped_one" in report
    assert "added: added_one" in report
    assert "retyped: retyped_one int64 -> double" in report
    assert "bump journal.SCHEMA_VERSION" in report
    assert "kept" not in report.replace("dropped_one", "").replace("retyped_one", "")


def test_the_drift_report_says_none_rather_than_an_empty_list():
    """An empty category reads as ``none``, so a report never trails off mid-line."""
    same = {"kept": "string"}
    report = _drift_report("quotes", 1, same, same)
    assert "dropped: none" in report
    assert "added: none" in report
    assert "retyped: none" in report


def test_unknown_surface_raises():
    """The fingerprint inherits the loud failure ``schema_for`` already gives."""
    with pytest.raises(ValueError, match=f"unknown surface '{UNPINNED_SURFACE}'"):
        journal.schema_fingerprint(UNPINNED_SURFACE)


# -- what counts as a change, defined once ------------------------------------


def test_the_diff_names_what_moved_in_each_direction():
    """The comparison both callers share, exercised directly rather than only through one.

    The suite's drift report and the ledger's refusal message each format their own text
    and each read the difference off this, so a change to what counts as a move has to
    show up here.
    """
    recorded = {"kept": "string", "dropped_one": "double", "retyped_one": "int64"}
    derived = {"kept": "string", "retyped_one": "double", "added_one": "bool"}
    diff = journal.fingerprint_diff(derived, recorded)
    assert diff.dropped == ("dropped_one",)
    assert diff.added == ("added_one",)
    assert diff.retyped == (("retyped_one", "int64", "double"),)
    assert diff.moved is True


def test_the_diff_reads_the_recorded_type_first_and_the_derived_type_second():
    """The order inside a retyped triple is what the message renders as ``was -> now``.

    Swapping the two would read the change backwards, and a reader would bump for the
    opposite change.
    """
    ((_, was, now),) = journal.fingerprint_diff({"c": "double"}, {"c": "int64"}).retyped
    assert (was, now) == ("int64", "double")


def test_identical_fingerprints_moved_nothing():
    same = {"kept": "string", "other": "int64"}
    diff = journal.fingerprint_diff(same, dict(same))
    assert diff == ((), (), ())
    assert diff.moved is False


@pytest.mark.parametrize(
    ("derived", "recorded"),
    [
        pytest.param({}, {"gone": "string"}, id="dropped-only"),
        pytest.param({"fresh": "string"}, {}, id="added-only"),
        pytest.param({"c": "double"}, {"c": "int64"}, id="retyped-only"),
    ],
)
def test_one_category_alone_still_counts_as_moved(derived, recorded):
    """Each category on its own is a change, and the retype case is the one that hides.

    ``moved`` decides whether a surface is named at all in the ledger's refusal message,
    and a conflict is not always all three kinds at once. A vendor that retypes a column
    and touches nothing else moves only ``retyped``, so a ``moved`` reading any two of the
    three would let that conflict be raised with no columns named.
    """
    assert journal.fingerprint_diff(derived, recorded).moved is True


def test_a_reordered_fingerprint_moved_nothing():
    """Order is not a change, which is the same rule ``schema_fingerprint`` encodes."""
    recorded = {"a": "string", "b": "int64", "c": "double"}
    reordered = {"c": "double", "b": "int64", "a": "string"}
    assert journal.fingerprint_diff(reordered, recorded).moved is False


def test_the_diff_sorts_every_category_so_a_message_reads_the_same_every_run():
    """Each category comes out sorted, whatever order the sets iterate in.

    The categories are built by set arithmetic, and a set of strings iterates in an order
    Python decides per process from its hash seed. So a two-name category lands in sorted
    order about half the time by luck, and a test built on one would pass or fail on the
    run rather than on the code. Six names per category drops that coincidence to one run
    in 720, and the assertion is stated twice: against the spelled-out sorted answer, and
    against ``sorted`` of the result itself.
    """
    common = {f"same_{i}": "string" for i in range(3)}
    recorded = {
        **common,
        "zulu": "string",
        "yankee": "string",
        "xray": "string",
        "whiskey": "string",
        "victor": "string",
        "uniform": "string",
        **{f"moved_{i}": "int64" for i in range(6)},
    }
    derived = {
        **common,
        "foxtrot": "string",
        "echo": "string",
        "delta": "string",
        "charlie": "string",
        "bravo": "string",
        "alfa": "string",
        **{f"moved_{i}": "double" for i in range(6)},
    }
    diff = journal.fingerprint_diff(derived, recorded)
    assert diff.dropped == ("uniform", "victor", "whiskey", "xray", "yankee", "zulu")
    assert diff.added == ("alfa", "bravo", "charlie", "delta", "echo", "foxtrot")
    assert [name for name, _, _ in diff.retyped] == [f"moved_{i}" for i in range(6)]
    assert list(diff.dropped) == sorted(diff.dropped)
    assert list(diff.added) == sorted(diff.added)
    assert list(diff.retyped) == sorted(diff.retyped)
