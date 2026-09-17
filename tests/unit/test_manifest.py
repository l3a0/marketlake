"""Manifest logic decided from values alone.

These cover the line parser, last-entry-wins, the torn-tail discard, the count that
decides whether a read left entries behind it, the segment to compacted-partition mapping
the supersession rule leans on, and the reverse-pass exclusion predicate. None of them
touch a real lake. The disk-backed behaviors live in the component tier.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lake.manifest import (
    SCRUB_EXCLUSIONS,
    ManifestError,
    TornLedger,
    _compacted_partition_for_segment,
    _is_excluded,
    _latest_by_partition,
    _refuse_hidden_entries,
    is_quarantined,
    latest_entries,
    latest_quarantine,
    latest_quarantine_by_check,
    manifest_path,
    parse_jsonl,
    quarantine_path,
    withholding,
)


def _entry(partition: str, **extra) -> dict:
    base = {"partition": partition, "source": "capture", "sha256": "x", "rows": 1}
    base.update(extra)
    return base


# -- line parsing and the torn tail ------------------------------------------


def test_parse_reads_every_complete_line():
    text = "".join(json.dumps(_entry(p)) + "\n" for p in ("a", "b", "c"))
    parsed = parse_jsonl(text)
    assert [e["partition"] for e in parsed] == ["a", "b", "c"]


def test_parse_skips_blank_lines():
    text = json.dumps(_entry("a")) + "\n\n" + json.dumps(_entry("b")) + "\n"
    assert [e["partition"] for e in parse_jsonl(text)] == ["a", "b"]


def test_parse_discards_a_torn_trailing_line():
    good = json.dumps(_entry("a")) + "\n" + json.dumps(_entry("b")) + "\n"
    torn = good + '{"partition": "c", "sha256": "untermin'
    parsed = parse_jsonl(torn)
    assert [e["partition"] for e in parsed] == ["a", "b"]


def test_parse_of_empty_text_is_empty():
    assert parse_jsonl("") == []


# -- the count that says the read left entries behind ------------------------

_LEDGER = Path("/lake/quarantine.jsonl")


def _line(partition: str) -> str:
    return json.dumps({"partition": partition, "verdict": "clean", "check": "e"}) + "\n"


def _refuses(text: str) -> TornLedger:
    """The refusal ``_refuse_hidden_entries`` makes about ``text``, parsed the real way.

    Handing it ``parse_jsonl``'s own output rather than a count written by hand is the
    point. The two have to agree about where the read stopped, and a test that supplied
    its own number would pass while they disagreed.
    """
    with pytest.raises(TornLedger) as refusal:
        _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text))
    return refusal.value


def test_a_read_that_consumed_every_line_left_nothing_behind():
    text = _line("a") + _line("b")
    assert _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text)) is None


def test_a_torn_tail_hides_nothing_and_is_not_refused():
    text = _line("a") + '{"partition": "b", "verdict": "cle'
    assert _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text)) is None


def test_a_fusion_with_nothing_behind_it_is_the_cost_append_line_accepts():
    """One entry lost and none hidden, which is the bound ``append_line`` states.

    The fragment has no terminating newline, so the entry appended onto it becomes part of
    the same line and no reader sees it. Refusing here would refuse the case the module
    already decided to pay for. Nothing is hidden yet either: the very next append is what
    puts a whole line behind the fused one, and from that read on this does refuse.
    """
    text = '{"partition": "a", "verd' + _line("b")
    assert _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text)) is None
    assert parse_jsonl(text) == []


def test_one_entry_behind_the_fused_line_is_refused_and_counted():
    text = '{"partition": "a", "verd' + _line("b") + _line("c")
    message = str(_refuses(text))
    assert "1 line after it is written and unreachable" in message
    assert "the read stopped at line 1" in message
    assert str(_LEDGER) in message
    assert "human's job under the lock" in message
    # The sentence that says what the count means. Without it the message names a line and a
    # number and never says the verdicts behind them are the reason a read cannot be trusted.
    assert (
        "Every verdict behind that line is invisible, so this ledger cannot say which "
        "partitions it withholds." in message
    )


def test_two_entries_behind_it_are_counted_and_read_as_plural():
    """Three appends behind the fragment hide two, because the first fuses onto it."""
    text = _line("a") + '{"partition": "b", "verd' + _line("c") + _line("d") + _line("e")
    message = str(_refuses(text))
    assert "2 lines after it are written and unreachable" in message
    assert "the read stopped at line 2" in message


def test_a_fragment_of_its_own_costs_no_entry_and_still_hides_what_follows():
    """The other shape, where the partial write did end in a newline.

    It is a line rather than a fusion, so it swallows no entry of its own. What follows it
    is hidden exactly as it is behind a fusion, and is counted the same way.
    """
    text = '{"partition": "a", "verd\n' + _line("b")
    assert "1 line after it is written and unreachable" in str(_refuses(text))


def test_the_reported_line_is_the_one_an_editor_shows():
    """The number's whole job is to send a person to the right line in the file.

    Counting the parsed entries gives the stop's position among *non-blank* lines, and the
    two diverge as soon as the file holds a blank one. No writer here makes a blank line, so
    a file that has one has already been hand edited, which is exactly the file the person
    this message addresses is looking at.
    """
    text = "\n\n\n" + '{"partition": "a", "verd' + _line("b") + _line("c")
    assert "the read stopped at line 4" in str(_refuses(text))

    between = _line("a") + "\n\n" + '{"partition": "b", "verd' + _line("c") + _line("d")
    assert "the read stopped at line 4" in str(_refuses(between))


def test_what_sits_behind_the_stop_is_counted_as_lines_not_entries():
    """Damage does not have to be well formed, so the count says what it measured.

    Every line behind the stop is unreachable whether or not it parses, and on a ledger a
    writer produced they are verdicts. Calling them entries would be a claim this never
    checked, in a message written for somebody deciding what to repair.
    """
    assert "1 line after it is written and unreachable" in str(_refuses("@@@\n$$$\n"))


def test_blank_lines_are_not_counted_as_hidden_entries():
    """A blank line is skipped by the parser, so counting it would refuse a clean ledger."""
    text = _line("a") + "\n\n" + _line("b") + "\n\n"
    assert _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text)) is None


@pytest.mark.parametrize("filler", ["   ", "\t", " \t "])
def test_a_whitespace_only_line_is_blank_to_both_sides_of_the_count(filler: str):
    """The two sides have to agree on what a blank line is, or the guard inverts.

    ``parse_jsonl`` strips a line before testing it, so a line of spaces or tabs is invisible
    to the parse. A count that tested the raw line instead would see lines the parse never
    consumed, read them as written entries hiding behind the stop, and refuse a ledger nothing
    is wrong with. Every reader funnels through here, so that would lock ``load_chain``, the
    battery, the dashboard and the sign-off tool out of a healthy lake.

    An empty line cannot catch it, because it is falsy under both spellings. These have to be
    lines that are truthy and blank, and they have to outnumber what parsed.
    """
    text = _line("a") + filler + "\n" + filler + "\n"
    assert _refuse_hidden_entries(_LEDGER, text, parse_jsonl(text)) is None
    assert _refuse_hidden_entries(_LEDGER, filler + "\n" + filler + "\n", []) is None


def test_the_refusal_is_a_manifest_error():
    """What puts it inside every ``except ManifestError`` already in the tree.

    ``bars`` files a ticker-day unfit on one and ``loader`` publishes it as the error a
    damaged ledger raises out of a read. A refusal outside that hierarchy would escape both.
    """
    assert issubclass(TornLedger, ManifestError)


# -- last entry wins ---------------------------------------------------------


def test_last_entry_wins_per_partition():
    entries = [
        _entry("p", rows=100, sha256="old"),
        _entry("q", rows=5),
        _entry("p", rows=405, sha256="new"),
    ]
    latest = _latest_by_partition(entries, Path("manifest.jsonl"))
    assert latest["p"]["rows"] == 405
    assert latest["p"]["sha256"] == "new"
    assert latest["q"]["rows"] == 5


def test_latest_by_partition_of_nothing_is_empty():
    assert _latest_by_partition([], Path("manifest.jsonl")) == {}


# -- the segment to compacted-partition mapping ------------------------------


def test_segment_maps_to_its_compacted_partition():
    seg = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"
    assert _compacted_partition_for_segment(seg) == "chains/ticker=SPY/date=2026-08-24.parquet"


def test_segment_mapping_handles_the_quotes_surface():
    seg = "journal/date=2026-11-28/surface=quotes/ticker=QQQ/seg-20261128T130000-9.arrows"
    assert _compacted_partition_for_segment(seg) == "quotes/ticker=QQQ/date=2026-11-28.parquet"


@pytest.mark.parametrize(
    "rel",
    [
        "chains/ticker=SPY/date=2026-08-24.parquet",  # already a compacted partition
        "reference/security_master.parquet",  # not a segment at all
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg.parquet",  # wrong suffix
        # A Parquet file named like a segment. The filename is matched on its suffix, so
        # carrying the segment prefix is not enough.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.parquet",
        "journal/date=2026-08-24/ticker=SPY/seg-1.arrows",  # too few path parts
    ],
)
def test_non_segment_paths_map_to_none(rel: str):
    assert _compacted_partition_for_segment(rel) is None


def test_a_ticker_carrying_a_dot_maps_like_any_other():
    # The parser slices each part at its key prefix and passes the value through. A
    # ticker with a dot in it, the shape a class-B share takes, is not a special case.
    seg = "journal/date=2026-08-24/surface=quotes/ticker=BRK.B/seg-20260824T160000-7.arrows"
    assert _compacted_partition_for_segment(seg) == "quotes/ticker=BRK.B/date=2026-08-24.parquet"


@pytest.mark.parametrize(
    "rel",
    [
        # A segment path carries three keys under the journal directory, and the parser
        # matches each one as a literal prefix.
        #
        # 1. The date key.
        # 2. The surface key.
        # 3. The ticker key.
        #
        # Each key gets two cases. The first drops it. The second spells its separator
        # wrong, which is the harder one, because the key is still the right length. A
        # parser that sliced by length rather than matching the prefix would map such a
        # path onto a real-looking partition instead of refusing it. The journal
        # directory name is checked as well, so a path outside the journal gets a case.
        "journal/2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date:2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface:chains/ticker=SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/SPY/seg-1.arrows",
        "journal/date=2026-08-24/surface=chains/ticker:SPY/seg-1.arrows",
        "reports/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        # The part count is exact, so a deeper path is refused as well as a shallower one.
        "journal/date=2026-08-24/surface=chains/ticker=SPY/extra/seg-1.arrows",
    ],
)
def test_a_path_that_fails_one_of_the_parser_checks_maps_to_none(rel: str):
    assert _compacted_partition_for_segment(rel) is None


def test_supersession_decision_is_read_from_the_latest_dict():
    seg = "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-20260824T160000-4242.arrows"
    compacted = _compacted_partition_for_segment(seg)
    # Without the compacted entry the segment stands on its own.
    assert compacted not in _latest_by_partition([_entry(seg)], Path("m.jsonl"))
    # With it present the segment is superseded.
    latest = _latest_by_partition([_entry(seg), _entry(compacted)], Path("m.jsonl"))
    assert compacted in latest


# -- the reverse-pass exclusion predicate ------------------------------------


def test_manifest_and_journal_are_excluded():
    assert _is_excluded("manifest.jsonl", SCRUB_EXCLUSIONS)
    assert _is_excluded(
        "journal/date=2026-08-24/surface=chains/ticker=SPY/seg-1.arrows",
        SCRUB_EXCLUSIONS,
    )


def test_data_files_and_the_two_other_ledgers_are_not_excluded():
    assert not _is_excluded("chains/ticker=SPY/date=2026-08-24.parquet", SCRUB_EXCLUSIONS)
    # Each of these carries its own manifest entry, so both are scrubbed, not skipped.
    assert not _is_excluded("quarantine.jsonl", SCRUB_EXCLUSIONS)
    assert not _is_excluded("actions/corporate_actions.jsonl", SCRUB_EXCLUSIONS)


def test_enumerated_exclusion_set_is_exactly_the_three_documented_members():
    assert SCRUB_EXCLUSIONS == ("manifest.jsonl", "journal/", "reports/")


# -- a ledger line nobody can interpret ------------------------------------------------


def test_a_line_naming_no_partition_raises_and_locates_itself(tmp_path):
    """The integrity root refuses to be read past, and says which line to look at.

    Skipping the line was considered and rejected. A torn trailing line is a write that
    did not finish, which ``_read_jsonl`` already discards. A line in the body that parses
    and names nothing is a record no reader can interpret, and stepping over damage in the
    file every other check is measured against would make all of them weaker than they
    read.
    """
    manifest_path(tmp_path).write_text(
        '{"partition": "a.parquet", "rows": 1}\n{"source": "compaction", "rows": 406}\n'
    )

    with pytest.raises(ManifestError) as raised:
        latest_entries(tmp_path)

    assert "entry 2" in str(raised.value), "the error did not locate the bad line"
    assert str(manifest_path(tmp_path)) in str(raised.value), "the error did not name the ledger"


def test_a_line_that_is_not_an_object_raises_the_same_way(tmp_path):
    """Damage that indexes differently is damage the same, so it answers the same."""
    manifest_path(tmp_path).write_text("[1, 2, 3]\n")

    with pytest.raises(ManifestError):
        latest_entries(tmp_path)


@pytest.mark.parametrize(
    "partition", [[], {"a": 1}, ["a", ["b"]]], ids=["list", "object", "nested"]
)
def test_a_manifest_partition_that_cannot_be_a_key_names_itself(tmp_path, partition):
    """Marketlake #514 on the manifest half. The hash below the guarded subscript was bare.

    ``entry["partition"]`` was guarded and ``latest[partition] = entry`` one line under it was
    not, so a JSON list or object raised ``TypeError: unhashable type``. That is neither a
    ``ManifestError`` nor an ``OSError``, so it named no ledger, located no line, and reached
    none of the tuples a damaged ledger is meant to land in.

    **The refusal has to be distinguishable from its neighbour, and that is what the last
    assertion holds.** Folding this shape into the arm above is the one-line version of this
    fix, and it passes a test asserting only the class, the ledger and the position: the
    message it produces is ``entry 1 names no partition``, about an entry that names one. An
    operator sent to look for a missing field finds a present one and the real damage keeps its
    hiding place.
    """
    manifest_path(tmp_path).write_text(json.dumps({"partition": partition, "rows": 1}) + "\n")

    with pytest.raises(ManifestError) as raised:
        latest_entries(tmp_path)

    message = str(raised.value)
    assert str(manifest_path(tmp_path)) in message, message
    assert "entry 1" in message, message
    assert repr(partition) in message, "the operator cannot see which key refused"
    assert "names no partition" not in message, (
        "an entry carrying a partition was reported as carrying none"
    )


def test_a_manifest_partition_that_can_be_a_key_still_passes_through(tmp_path):
    """The guard refuses what cannot be keyed, never what merely is not a string.

    A hand-repaired ledger holding an integer key is damage nothing here can decide about, and
    ``signoff.open_quarantines`` prints it on purpose so the human can find what to repair.
    Refusing it would take that listing away and turn a repairable ledger into an unreadable
    one, which is the guard inverted.
    """
    manifest_path(tmp_path).write_text(
        "".join(
            json.dumps({"partition": key, "rows": 1}) + "\n" for key in (7, 1.5, None, True, "a")
        )
    )

    assert set(latest_entries(tmp_path)) == {7, 1.5, None, True, "a"}


def test_the_quarantine_ledger_names_itself_rather_than_the_manifest(tmp_path):
    """Two ledgers share the reader, so the message has to say which one broke."""
    quarantine_path(tmp_path).write_text('{"verdict": "keep"}\n')

    with pytest.raises(ManifestError) as raised:
        latest_quarantine(tmp_path)

    # Compared against the path itself. An earlier version asserted the word
    # "quarantine" appeared anywhere in the message, which pytest satisfies for free:
    # tmp_path is derived from the test's own name. It passed with the manifest's path
    # substituted, which is the mutation it existed to catch.
    assert str(quarantine_path(tmp_path)) in str(raised.value), str(raised.value)


# -- the quarantine ledger resolves per check --------------------------------


def _verdict(partition: str, verdict: str, check: str | None = "realtime_entitlement") -> dict:
    entry = {"partition": partition, "verdict": verdict}
    if check is not None:
        entry["check"] = check
    return entry


def _ledger(tmp_path: Path, *entries: dict) -> Path:
    quarantine_path(tmp_path).write_text("".join(json.dumps(e) + "\n" for e in entries))
    return tmp_path


CHAINS = "chains/ticker=SPY/date=2026-09-16.parquet"


def test_each_check_keeps_its_own_current_verdict(tmp_path):
    """Marketlake #426. Resolving on the partition alone kept whichever line landed last."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "row_count_band"),
        _verdict(CHAINS, "quarantined", "realtime_entitlement"),
        _verdict(CHAINS, "clean", "realtime_entitlement"),
    )

    by_check = latest_quarantine_by_check(tmp_path)[CHAINS]

    assert by_check["row_count_band"]["verdict"] == "quarantined"
    assert by_check["realtime_entitlement"]["verdict"] == "clean"
    assert [e["check"] for e in withholding(by_check)] == ["row_count_band"]


def test_the_partition_reads_only_when_every_check_clears(tmp_path):
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "row_count_band"),
        _verdict(CHAINS, "quarantined", "realtime_entitlement"),
        _verdict(CHAINS, "clean", "realtime_entitlement"),
        _verdict(CHAINS, "clean", "row_count_band"),
    )

    assert withholding(latest_quarantine_by_check(tmp_path)[CHAINS]) == ()
    assert is_quarantined(latest_quarantine(tmp_path)[CHAINS]) is False


def test_the_deciding_entry_is_the_longest_standing_one_still_withholding(tmp_path):
    """Not the last line. The last line here is a pass from a check that saw no fault."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "row_count_band"),
        _verdict(CHAINS, "quarantined", "realtime_entitlement"),
        _verdict(CHAINS, "clean", "realtime_entitlement"),
    )

    deciding = latest_quarantine(tmp_path)[CHAINS]

    assert deciding["check"] == "row_count_band"
    assert is_quarantined(deciding) is True


def test_the_order_follows_each_checks_current_entry_not_its_first(tmp_path):
    """A plain reassignment keeps the position a key was first seen at, which differs."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "a"),
        _verdict(CHAINS, "quarantined", "b"),
        _verdict(CHAINS, "clean", "a"),
        _verdict(CHAINS, "quarantined", "a"),
    )

    assert [e["check"] for e in withholding(latest_quarantine_by_check(tmp_path)[CHAINS])] == [
        "b",
        "a",
    ]


def test_a_partition_with_no_entry_withholds_nothing(tmp_path):
    assert withholding(None) == ()
    assert withholding({}) == ()
    assert latest_quarantine_by_check(tmp_path) == {}


def test_a_clean_entry_naming_no_check_clears_only_its_own_bucket(tmp_path):
    """Fail closed gets stronger, not weaker.

    Under per-partition resolution this line was the last one and released the partition
    outright. It now clears the bucket for an unnamed check, which nothing withheld.
    """
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "row_count_band"),
        _verdict(CHAINS, "clean", None),
    )

    assert [e["check"] for e in withholding(latest_quarantine_by_check(tmp_path)[CHAINS])] == [
        "row_count_band"
    ]


def test_an_entry_naming_no_check_still_withholds_on_a_bad_verdict(tmp_path):
    _ledger(tmp_path, _verdict(CHAINS, "stale", None))

    assert is_quarantined(latest_quarantine(tmp_path)[CHAINS]) is True


def test_a_check_that_cannot_be_a_key_names_this_ledger_and_its_position(tmp_path):
    """Damage this reader cannot key answers the way a missing partition already does."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "clean", "row_count_band"),
        {"partition": CHAINS, "verdict": "quarantined", "check": ["not", "a", "key"]},
    )

    with pytest.raises(ManifestError) as raised:
        latest_quarantine_by_check(tmp_path)

    message = str(raised.value)
    assert str(quarantine_path(tmp_path)) in message
    assert "entry 2" in message, message
    # The message itself, which nothing held until marketlake #514 gave its twin one arm up.
    # The ledger and the position alone are satisfied by every refusal this reader raises, so
    # a test carrying only those cannot tell which damage it met.
    assert "check that cannot be a key" in message, message
    assert repr(["not", "a", "key"]) in message, message


@pytest.mark.parametrize("partition", [[], {"a": 1}], ids=["list", "object"])
def test_a_quarantine_partition_that_cannot_be_a_key_names_this_ledger(tmp_path, partition):
    """Marketlake #514, the shape that took the whole 18:30 run down.

    ``latest.setdefault(partition, {})`` sat one line under a guarded subscript and hashed
    whatever that subscript returned. A JSON list or object raised ``TypeError: unhashable
    type``, which is neither a ``ManifestError`` nor an ``OSError``, so it escaped
    ``sweep._LEDGER_REFUSALS`` and every other containment around this ledger. That is the same
    escape marketlake #495 closed for bytes that do not decode.

    The last assertion is what separates this arm from the one above it. The one-line version of
    this fix folds the hash into that arm and answers ``entry 1 names no partition`` about an
    entry that names one, which satisfies a test written to the ledger and the position alone.
    """
    _ledger(
        tmp_path,
        {"partition": partition, "verdict": "quarantined", "check": "row_count_band"},
    )

    with pytest.raises(ManifestError) as raised:
        latest_quarantine_by_check(tmp_path)

    message = str(raised.value)
    assert str(quarantine_path(tmp_path)) in message
    assert "entry 1" in message, message
    assert repr(partition) in message, "the operator cannot see which key refused"
    assert "names no partition" not in message, (
        "an entry carrying a partition was reported as carrying none"
    )


def test_latest_quarantine_inherits_the_refusal_rather_than_repeating_it(tmp_path):
    """``latest_quarantine`` resolves through ``latest_quarantine_by_check`` and adds no guard.

    It keys on what that reader already returned, so its keys are hashable by construction.
    This holds that the refusal still reaches its callers through it, which is the path
    ``sweep.count_quarantined`` and ``signoff.open_quarantines`` both take.
    """
    _ledger(tmp_path, {"partition": [], "verdict": "quarantined", "check": "row_count_band"})

    with pytest.raises(ManifestError) as raised:
        latest_quarantine(tmp_path)

    assert "cannot be a key" in str(raised.value), str(raised.value)


def test_the_deciding_entry_is_the_earlier_of_two_still_withholding(tmp_path):
    """With one holder left, ``held[0]`` and ``held[-1]`` are the same entry and prove nothing."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "quarantined", "row_count_band"),
        _verdict(CHAINS, "quarantined", "realtime_entitlement"),
    )

    assert latest_quarantine(tmp_path)[CHAINS]["check"] == "row_count_band"


def test_a_partition_every_check_cleared_reports_the_last_line_written(tmp_path):
    """Nothing withholds, so the deciding entry is the newest rather than the oldest."""
    _ledger(
        tmp_path,
        _verdict(CHAINS, "clean", "row_count_band"),
        _verdict(CHAINS, "clean", "realtime_entitlement"),
    )

    deciding = latest_quarantine(tmp_path)[CHAINS]

    assert deciding["check"] == "realtime_entitlement"
    assert is_quarantined(deciding) is False
