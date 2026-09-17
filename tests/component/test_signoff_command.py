"""The quarantine sign-off tool against a real ledger file.

These run the sign-off core over a throwaway lake, with a manual clock, no vendor and no
network. The ledger is a real file on disk, because the whole deliverable is about what lands
in it and what reads back out, so faking it would fake the thing under test. That places these
at component tier by the build plan's rule: a real file within one subsystem, no second
subsystem crossed.

The refusal tests assert the exit code and the line an operator reads, the way
``test_onboard_command.py`` does, because a refusal that arrives as a stack is the weaker guard
and that is the class this tool exists on the right side of.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lake.battery import (
    BATTERY_SOURCE,
    CHECK_ENTITLEMENT,
    CLEAN_VERDICT,
    PROVENANCE_BATTERY,
    PROVENANCE_HUMAN,
    QUARANTINED_VERDICT,
    SIGNOFF_SOURCE,
    Finding,
    _transition,
    append_verdict,
    build_entry,
    human_precedence,
)
from lake.calendar import MARKET_TZ
from lake.manifest import (
    ManifestError,
    append_quarantine,
    is_quarantined,
    latest_entries,
    latest_quarantine,
    quarantine_path,
    read_quarantine,
)
from lake.signoff import (
    SignoffError,
    main,
    open_quarantines,
    render_open,
    signoff,
)
from tests.support.clock import ManualClock
from tests.support.config import write_config

NOW = datetime(2026, 9, 17, 22, 30, tzinfo=UTC)  # 18:30 ET
PARTITION = "chains/ticker=SPY/date=2026-09-14.parquet"
OTHER = "quotes/ticker=QQQ/date=2026-09-14.parquet"


def _clock() -> ManualClock:
    return ManualClock(NOW)


def _quarantine(lake: Path, partition: str = PARTITION, check: str = CHECK_ENTITLEMENT) -> dict:
    """One battery verdict on disk, which is the state every sign-off starts from."""
    return append_verdict(
        lake,
        build_entry(
            partition=partition,
            verdict=QUARANTINED_VERDICT,
            check=check,
            observed_at=NOW,
            reason="session-median staleness 902.0s against 60",
        ),
        observed_at=NOW,
    )


# -- the two directions ------------------------------------------------------


def test_a_sign_off_clears_the_partition_for_every_reader(tmp_path: Path):
    """The consumer-side meaning: the guard `load_chain` reads stops withholding it."""
    _quarantine(tmp_path)
    assert is_quarantined(latest_quarantine(tmp_path)[PARTITION])

    report = signoff(
        PARTITION, reason="re-read by hand, feed is real time", clock=_clock(), lake_root=tmp_path
    )

    assert report.verdict == CLEAN_VERDICT
    assert report.still_withheld is False
    assert is_quarantined(latest_quarantine(tmp_path)[PARTITION]) is False


def test_the_sign_off_carries_the_check_that_quarantined_the_partition(tmp_path: Path):
    """The finding that decided the interface, asserted on the entry rather than inferred.

    A sign-off under any other token is invisible to ``human_precedence``, so the next nightly
    run re-quarantines and the sign-off lasts until 18:30. This is the reason nothing in the
    tool mints a check name.
    """
    _quarantine(tmp_path)

    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    entry = latest_quarantine(tmp_path)[PARTITION]
    assert entry["check"] == CHECK_ENTITLEMENT
    assert entry["provenance"] == PROVENANCE_HUMAN
    assert human_precedence(entry, CHECK_ENTITLEMENT) is True


def test_the_next_nightly_run_leaves_the_sign_off_standing(tmp_path: Path):
    """The whole point, driven through the two functions `battery.judge` decides with.

    `judge` consults `human_precedence` first and `_transition` after. Asserting both is what
    separates "the sign-off survives" from "the sign-off happens to survive": `_transition`
    returns True on this finding, so a line would be appended and the partition re-quarantined
    if precedence were not what stops it first.
    """
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    tonight = Finding(
        partition=PARTITION,
        surface="chains",
        ticker="SPY",
        day=NOW.date(),
        check=CHECK_ENTITLEMENT,
        verdict=QUARANTINED_VERDICT,
        computed=902.0,
        against=60.0,
        reason="session-median staleness 902.0s against 60",
    )
    entry = latest_quarantine(tmp_path)[PARTITION]

    assert human_precedence(entry, tonight.check) is True
    assert _transition(entry, tonight) is True
    assert is_quarantined(entry) is False


def test_a_revoke_withholds_the_partition_again_under_the_same_check(tmp_path: Path):
    """Without this the tool is a one-way door in the other direction."""
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    report = signoff(
        PARTITION, reason="that sign-off was wrong", clock=_clock(), lake_root=tmp_path, revoke=True
    )

    assert report.verdict == QUARANTINED_VERDICT
    assert report.still_withheld is True
    entry = latest_quarantine(tmp_path)[PARTITION]
    assert entry["check"] == CHECK_ENTITLEMENT
    assert entry["provenance"] == PROVENANCE_HUMAN
    # The report is the only thing an operator reads back, so it must not say "Signed off".
    rendered = report.render()
    assert rendered.startswith("Revoked ")
    assert "partition was:   readable" in rendered
    assert "partition now:   withheld" in rendered
    assert "that sign-off was wrong" in rendered


def test_a_revoked_partition_can_be_signed_off_again(tmp_path: Path):
    """Neither direction is a door that only opens once."""
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)
    signoff(PARTITION, reason="wrong", clock=_clock(), lake_root=tmp_path, revoke=True)

    report = signoff(PARTITION, reason="checked again", clock=_clock(), lake_root=tmp_path)

    assert report.still_withheld is False
    assert len(read_quarantine(tmp_path)) == 4


def test_the_reason_rides_into_the_ledger_entry(tmp_path: Path):
    """It is what tells the next reader why a human cleared a partition."""
    _quarantine(tmp_path)

    signoff(PARTITION, reason="checked the raw payload by hand", clock=_clock(), lake_root=tmp_path)

    assert latest_quarantine(tmp_path)[PARTITION]["reason"] == "checked the raw payload by hand"


def test_the_entry_and_its_manifest_row_carry_the_run_clock(tmp_path: Path):
    """The ledger is where a reader finds out when a human cleared a partition.

    Both stamps come from the same injected clock, and nothing else in the suite reads either,
    so without this the clock every other test bothers to inject reaches nothing.
    """
    _quarantine(tmp_path)

    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    stamp = NOW.astimezone(MARKET_TZ).isoformat()
    assert latest_quarantine(tmp_path)[PARTITION]["observed_at"] == stamp
    assert latest_entries(tmp_path)["quarantine.jsonl"]["fetched_at"] == stamp


# -- the ledger's own rules --------------------------------------------------


def test_the_sign_off_supersedes_rather_than_deleting_history(tmp_path: Path):
    """Un-quarantine is an explicit superseding entry, which is the ledger's rule."""
    _quarantine(tmp_path)

    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    entries = read_quarantine(tmp_path)
    assert len(entries) == 2
    assert entries[0]["verdict"] == QUARANTINED_VERDICT
    assert entries[0]["provenance"] == PROVENANCE_BATTERY
    assert entries[1]["verdict"] == CLEAN_VERDICT


def test_the_manifest_entry_is_refreshed_in_the_same_invocation(tmp_path: Path):
    """A weekend sign-off must not leave the Sunday scrub facing a stale sha."""
    _quarantine(tmp_path)
    before = latest_entries(tmp_path)["quarantine.jsonl"]

    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    after = latest_entries(tmp_path)["quarantine.jsonl"]
    assert after["rows"] == before["rows"] + 1
    assert after["sha256"] != before["sha256"]


def test_the_manifest_entry_names_the_sign_off_tool_as_its_source(tmp_path: Path):
    """The battery refreshed the first entry and a human refreshed the second."""
    _quarantine(tmp_path)
    assert latest_entries(tmp_path)["quarantine.jsonl"]["source"] == "battery"

    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    # The literal, not the constant. Comparing the written value against the constant that
    # wrote it can never fail, and the whole point of the constant is that the two writers
    # stamp different strings.
    assert latest_entries(tmp_path)["quarantine.jsonl"]["source"] == "signoff"
    assert SIGNOFF_SOURCE != BATTERY_SOURCE


def test_a_reason_carrying_newlines_stays_one_entry_on_one_line(tmp_path: Path):
    """Operator free text is the only data a person writes into this ledger by hand."""
    _quarantine(tmp_path)
    nasty = 'first\nsecond\n{"partition": "forged", "verdict": "clean"}'

    signoff(PARTITION, reason=nasty, clock=_clock(), lake_root=tmp_path)

    entries = read_quarantine(tmp_path)
    assert len(quarantine_path(tmp_path).read_text().splitlines()) == 2
    assert [entry["partition"] for entry in entries] == [PARTITION, PARTITION]
    assert entries[1]["reason"] == nasty


# -- the refusals ------------------------------------------------------------


def test_signing_off_a_partition_nothing_withholds_refuses(tmp_path: Path):
    """The second sign-off. Appending a line that changes nothing is what this prevents."""
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="verified again", clock=_clock(), lake_root=tmp_path)

    assert "no entry withholds it" in str(refusal.value)
    assert len(read_quarantine(tmp_path)) == 2


def test_an_unknown_partition_refuses_and_names_what_is_withheld(tmp_path: Path):
    """A mistyped path corrects itself by being read, which is why the line lists them."""
    _quarantine(tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(
            "chains/ticker=spy/date=2026-09-14.parquet",
            reason="x",
            clock=_clock(),
            lake_root=tmp_path,
        )

    message = str(refusal.value)
    assert PARTITION in message
    assert "case matters" in message


def test_an_empty_ledger_refuses_without_offering_an_empty_list(tmp_path: Path):
    """The live lake's state: the battery has never run, so nothing is withheld at all."""
    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path)

    assert "withholds no partition" in str(refusal.value)


def test_a_withholding_entry_naming_no_check_refuses_rather_than_raising(tmp_path: Path):
    """Left alone this is a ValueError out of ``build_entry``, read as a stack."""
    append_quarantine(tmp_path, {"partition": PARTITION, "verdict": QUARANTINED_VERDICT})

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path)

    assert "names no check" in str(refusal.value)
    assert len(read_quarantine(tmp_path)) == 1


def test_a_check_no_withholding_entry_carries_refuses(tmp_path: Path):
    _quarantine(tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path, check="quote_sanity")

    assert "quote_sanity" in str(refusal.value)
    assert CHECK_ENTITLEMENT in str(refusal.value)


def test_naming_the_withholding_check_explicitly_is_accepted(tmp_path: Path):
    _quarantine(tmp_path)

    report = signoff(
        PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path, check=CHECK_ENTITLEMENT
    )

    assert report.still_withheld is False


def test_a_blank_reason_refuses_before_reading_the_ledger(tmp_path: Path):
    _quarantine(tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="   ", clock=_clock(), lake_root=tmp_path)

    assert "a reason is required" in str(refusal.value)
    assert len(read_quarantine(tmp_path)) == 1


def test_revoking_a_partition_the_ledger_has_never_judged_refuses(tmp_path: Path):
    """There is no entry, so there is no check to revoke under and none to invent."""
    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path, revoke=True)

    assert "nothing to revoke" in str(refusal.value)


def test_revoking_a_partition_already_withheld_refuses(tmp_path: Path):
    """The second revoke, which is the mirror of the second sign-off."""
    _quarantine(tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path, revoke=True)

    assert "already withheld" in str(refusal.value)
    assert len(read_quarantine(tmp_path)) == 1


# -- the write is confirmed --------------------------------------------------


def test_a_torn_line_earlier_in_the_ledger_is_caught_rather_than_reported_as_success(
    tmp_path: Path,
):
    """The condition that makes a successful append invisible to every reader.

    ``parse_jsonl`` stops at the first line that does not parse, so a fragment in the body
    hides every entry after it. The sign-off lands on disk and no reader sees it. A tool that
    printed a success line here would leave the operator holding a partition still withheld
    with nothing saying why.
    """
    _quarantine(tmp_path)
    with quarantine_path(tmp_path).open("a") as handle:
        handle.write('{"partition": "chains/ticker=QQQ/date=2026-09-15.parq')

    with pytest.raises(SignoffError) as refusal:
        signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    message = str(refusal.value)
    assert "does not read back" in message
    assert "human's job under the lock" in message
    assert is_quarantined(latest_quarantine(tmp_path)[PARTITION]) is True


def test_the_read_back_accepts_a_sign_off_a_later_entry_superseded(tmp_path: Path):
    """Membership, not currency. A supersede is a different outcome from a lost write.

    The tool reads the ledger to pick the check and ``append_verdict`` then takes the lock, and
    those are not one hold. A writer landing in that window supersedes the sign-off, which the
    report shows rather than refuses.
    """
    _quarantine(tmp_path)
    original = signoff

    appended: list[dict] = []

    def _racing_append(lake_root, entry, *, observed_at, source):
        appended.append(entry)
        result = append_verdict(lake_root, entry, observed_at=observed_at, source=source)
        # A second writer lands between the append and the read-back.
        append_verdict(
            lake_root,
            build_entry(
                partition=PARTITION,
                verdict=QUARANTINED_VERDICT,
                check="quote_sanity",
                observed_at=observed_at,
            ),
            observed_at=observed_at,
        )
        return result

    import lake.signoff as module

    module.append_verdict = _racing_append
    try:
        report = original(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)
    finally:
        module.append_verdict = append_verdict

    assert report.still_withheld is True
    assert report.after["check"] == "quote_sanity"
    assert appended[0] in read_quarantine(tmp_path)
    # A racing writer is the only way a second holder exists under today's resolution, so this
    # is where the report's "still withheld under" line can be held at all. It names the other
    # writer's check rather than the one this run signed off.
    rendered = report.render()
    assert "still withheld under: 'quote_sanity'" in rendered
    assert "partition now:   withheld" in rendered


# -- the dry run and the listing ---------------------------------------------


def test_a_dry_run_writes_nothing_and_reports_the_consequence(tmp_path: Path):
    """A preview that showed the state it started from would read as a write that does nothing.

    The first draft set ``after`` to ``before`` here, so a dry-run sign-off of a withheld
    partition printed "partition now: withheld". An operator reading that concludes the real
    run will not clear it, which is the opposite of what happens.
    """
    _quarantine(tmp_path)

    report = signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path, dry_run=True)

    assert report.dry_run is True
    assert report.still_withheld is False
    assert len(read_quarantine(tmp_path)) == 1
    assert is_quarantined(latest_quarantine(tmp_path)[PARTITION]) is True
    rendered = report.render()
    assert rendered.startswith("Would sign off ")
    assert "partition is:    withheld" in rendered
    assert "would be:        readable" in rendered
    assert "verified" in rendered
    assert "nothing was written" in rendered


def test_a_dry_run_revoke_says_it_would_revoke(tmp_path: Path):
    """The dry run's other direction, which nothing rendered."""
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    report = signoff(
        PARTITION, reason="wrong", clock=_clock(), lake_root=tmp_path, revoke=True, dry_run=True
    )

    rendered = report.render()
    assert rendered.startswith("Would revoke ")
    assert "partition is:    readable" in rendered
    assert "would be:        withheld" in rendered
    assert len(read_quarantine(tmp_path)) == 2


def test_the_report_names_a_second_holder_and_never_the_verdict_it_just_wrote(tmp_path: Path):
    """ "still withheld under" must mean something else holds it, or it reads as a false alarm.

    A revoke leaves the partition withheld under the entry the run itself wrote, and naming
    that check would read as a warning about a second holder where there is none.
    """
    _quarantine(tmp_path)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    report = signoff(PARTITION, reason="wrong", clock=_clock(), lake_root=tmp_path, revoke=True)

    assert report.still_withheld is True
    assert "still withheld under:" not in report.render()


def test_a_dry_run_refuses_everything_a_real_run_refuses(tmp_path: Path):
    """The same work with the writer switched off, rather than a second path beside it."""
    with pytest.raises(SignoffError):
        signoff(PARTITION, reason="x", clock=_clock(), lake_root=tmp_path, dry_run=True)


def test_the_listing_names_every_withheld_partition_and_its_check(tmp_path: Path):
    _quarantine(tmp_path)
    _quarantine(tmp_path, partition=OTHER, check="quote_sanity")

    listed = open_quarantines(tmp_path)

    assert [one.partition for one in listed] == [PARTITION, OTHER]
    assert [one.check for one in listed] == [CHECK_ENTITLEMENT, "quote_sanity"]


def test_the_listing_leaves_out_a_partition_a_sign_off_cleared(tmp_path: Path):
    _quarantine(tmp_path)
    _quarantine(tmp_path, partition=OTHER)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    assert [one.partition for one in open_quarantines(tmp_path)] == [OTHER]


def test_an_empty_listing_says_nothing_is_withheld(tmp_path: Path):
    rendered = render_open(open_quarantines(tmp_path), quarantine_path(tmp_path))

    assert "No partition is withheld." in rendered


# -- the command entry -------------------------------------------------------


def _config(tmp_path: Path, lake: Path) -> Path:
    """A real config naming the throwaway lake, through the helper every command test uses."""
    return write_config(tmp_path, lake)


def test_the_command_prints_the_report_and_exits_zero(tmp_path: Path, capsys):
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    config = _config(tmp_path, lake)

    code = main(
        [PARTITION, "--reason", "verified by hand", "--config", str(config)], clock=_clock()
    )

    printed = capsys.readouterr().out
    assert code == 0
    assert "Signed off" in printed
    assert "partition now:   readable" in printed


def test_the_command_refuses_with_one_line_and_exit_two(tmp_path: Path, capsys):
    lake = tmp_path / "lake"
    lake.mkdir()
    config = _config(tmp_path, lake)

    with pytest.raises(SystemExit) as exit_info:
        main([PARTITION, "--reason", "x", "--config", str(config)], clock=_clock())

    captured = capsys.readouterr()
    assert exit_info.value.code == 2
    assert captured.err.startswith("signoff: ")
    assert len(captured.err.strip().splitlines()) == 1
    assert "Traceback" not in captured.err


def test_the_command_refuses_a_partition_named_without_a_reason(tmp_path: Path, capsys):
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    config = _config(tmp_path, lake)

    with pytest.raises(SystemExit) as exit_info:
        main([PARTITION, "--config", str(config)], clock=_clock())

    assert exit_info.value.code == 2
    assert "--reason is required" in capsys.readouterr().err
    assert len(read_quarantine(lake)) == 1


def test_the_command_with_no_partition_lists_and_writes_nothing(tmp_path: Path, capsys):
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    config = _config(tmp_path, lake)

    code = main(["--config", str(config)], clock=_clock())

    assert code == 0
    assert PARTITION in capsys.readouterr().out
    assert len(read_quarantine(lake)) == 1


def test_the_command_wires_dry_run_through_to_the_core(tmp_path: Path, capsys):
    """The flag reaches the core, rather than the core's flag defaulting off underneath it.

    Every other command test passes a partition and a reason, so the whole argparse-to-core
    wiring for the three behaviour flags was exercised by nothing. Replacing `args.dry_run`
    with a literal `False` left the suite green while `--dry-run` cleared the partition.
    """
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    config = _config(tmp_path, lake)

    code = main(
        [PARTITION, "--reason", "preview", "--dry-run", "--config", str(config)], clock=_clock()
    )

    assert code == 0
    assert "Would sign off" in capsys.readouterr().out
    assert len(read_quarantine(lake)) == 1
    assert is_quarantined(latest_quarantine(lake)[PARTITION]) is True


def test_the_command_wires_revoke_through_to_the_core(tmp_path: Path, capsys):
    """Without this, `--revoke` signed off instead of withholding and nothing noticed."""
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    signoff(PARTITION, reason="verified", clock=_clock(), lake_root=lake)
    config = _config(tmp_path, lake)

    code = main(
        [PARTITION, "--reason", "wrong", "--revoke", "--config", str(config)], clock=_clock()
    )

    assert code == 0
    assert "Revoked" in capsys.readouterr().out
    assert is_quarantined(latest_quarantine(lake)[PARTITION]) is True


def test_the_command_wires_check_through_to_the_core(tmp_path: Path, capsys):
    """Without this, `--check` was discarded and every confirmation silently passed."""
    lake = tmp_path / "lake"
    lake.mkdir()
    _quarantine(lake)
    config = _config(tmp_path, lake)

    with pytest.raises(SystemExit) as exit_info:
        main(
            [PARTITION, "--reason", "x", "--check", "quote_sanity", "--config", str(config)],
            clock=_clock(),
        )

    assert exit_info.value.code == 2
    assert "--check says 'quote_sanity'" in capsys.readouterr().err
    assert len(read_quarantine(lake)) == 1


def test_a_corrupt_ledger_keeps_its_traceback(tmp_path: Path):
    """`main` catches `SignoffError` and nothing wider, which the module docstring states twice.

    Widening the catch to `Exception` left every test green while a corrupt integrity root
    reached the operator dressed as a typo. `SystemExit` is a `BaseException`, so the
    bad-config test cannot see the difference.
    """
    lake = tmp_path / "lake"
    lake.mkdir()
    append_quarantine(lake, {"verdict": QUARANTINED_VERDICT, "check": CHECK_ENTITLEMENT})
    config = _config(tmp_path, lake)

    with pytest.raises(ManifestError) as raised:
        main([PARTITION, "--reason", "x", "--config", str(config)], clock=_clock())

    assert "names no partition" in str(raised.value)


def test_the_listing_renders_the_count_the_verdict_and_the_check(tmp_path: Path):
    """`render_open` is what the operator reads, and nothing exercised it on a real ledger."""
    _quarantine(tmp_path)
    _quarantine(tmp_path, partition=OTHER, check="quote_sanity")

    rendered = render_open(open_quarantines(tmp_path), quarantine_path(tmp_path))

    assert rendered.startswith("2 partition(s) withheld:")
    assert f"  {PARTITION}  {QUARANTINED_VERDICT}  under {CHECK_ENTITLEMENT}" in rendered
    assert f"  {OTHER}  {QUARANTINED_VERDICT}  under quote_sanity" in rendered


def test_the_listing_sorts_rather_than_following_the_ledger_order(tmp_path: Path):
    """Written in reverse order, so following the file would be visible."""
    _quarantine(tmp_path, partition=OTHER)
    _quarantine(tmp_path, partition=PARTITION)

    assert [one.partition for one in open_quarantines(tmp_path)] == [PARTITION, OTHER]


def test_a_damaged_partition_key_is_listed_rather_than_crashing_the_refusal(tmp_path: Path):
    """The path three of this module's own refusals send a human down.

    They say repairing a ledger is a human's job under the lock, and a hand-repaired ledger is
    what the next run reads. A partition key that is not a string used to raise `TypeError` out
    of the listing, which is a bare traceback on the refusal the docstring calls the common one.
    """
    append_quarantine(
        tmp_path,
        {"partition": 20260914, "verdict": QUARANTINED_VERDICT, "check": CHECK_ENTITLEMENT},
    )
    _quarantine(tmp_path)

    with pytest.raises(SignoffError) as refusal:
        signoff(
            "chains/ticker=QQQ/date=2026-09-15.parquet",
            reason="x",
            clock=_clock(),
            lake_root=tmp_path,
        )

    assert "20260914" in str(refusal.value)
    assert PARTITION in str(refusal.value)
    assert "20260914" in render_open(open_quarantines(tmp_path), quarantine_path(tmp_path))


def test_a_bad_config_file_refuses_with_one_line_rather_than_a_stack(tmp_path: Path, capsys):
    """A malformed operator file is a mistake, so it gets the line rather than the traceback."""
    config = tmp_path / "config.yaml"
    config.write_text("lake_root: [not, a, path]\n")

    with pytest.raises(SystemExit) as exit_info:
        main(["--config", str(config)], clock=_clock())

    captured = capsys.readouterr()
    assert exit_info.value.code == 2
    assert captured.err.startswith("signoff: ")
    assert "Traceback" not in captured.err


def test_the_check_comes_from_the_ledger_and_not_from_the_battery_default(tmp_path: Path):
    """A check token this tool has never heard of still gets signed off under its own name.

    `CHECK_ENTITLEMENT` is the only token any writer emits today, so a tool that defaulted to
    it would look correct on every test that used it. This one quarantines under a different
    token to catch that.
    """
    _quarantine(tmp_path, check="quote_sanity")

    report = signoff(PARTITION, reason="verified", clock=_clock(), lake_root=tmp_path)

    assert report.check == "quote_sanity"
    assert report.still_withheld is False
    entry = latest_quarantine(tmp_path)[PARTITION]
    assert entry["check"] == "quote_sanity"
    assert human_precedence(entry, "quote_sanity") is True
    assert human_precedence(entry, CHECK_ENTITLEMENT) is False
