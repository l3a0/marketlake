"""The quarantine sign-off tool: the one way a human clears a verdict, and the one way back.

``lake.battery`` writes quarantine verdicts and nothing clears them. ``load_chain`` and
``load_bars`` refuse a withheld partition by default, so one verdict withholds a partition from
every reader until a person says otherwise. Run this as ``python -m lake.signoff``.

Nothing but a human can clear one. The check that wrote the verdict judges a sealed immutable
partition, so it returns the same finding every night and can never clear its own entry.
``include_quarantined=True`` reads past the guard rather than resolving it, and using that as
the remedy would make the guard inert for every reader. The ledger is append-only, so there is
no line to delete. That is why this exists.

**The check token is the whole interface.** A sign-off has to be written under the check that
quarantined the partition, because ``battery.human_precedence`` compares
``current["check"]`` against the check being run. A sign-off carrying a token no check emits is
invisible to it, ``battery._transition`` then sees a different check and appends a fresh
quarantine, and the sign-off lasts until 18:30. So nothing here mints a check name. The check is
read off the entry currently withholding the partition.

**This inherits three rules rather than inventing them.**

1. ``battery.append_verdict`` takes the lake-root ``flock`` itself and appends the ledger line
   beside the refreshed manifest entry inside one hold, which is the rule a weekend sign-off
   needs so the Sunday scrub never faces a sha nothing has caught up to.
2. ``battery.build_entry`` is the one place an entry is assembled and checked.
3. Un-quarantine as a superseding entry rather than a deletion is the ledger's own rule, stated
   in ``manifest.py``.

This module adds none of the three and re-implements none of them.

**Resolution is read, never re-derived.** ``manifest.latest_quarantine`` is what decides a
partition's readability, and ``manifest.is_quarantined``'s docstring says why both sides have to
meet there: "reader and writer have to meet at one definition or the exclusion silently
inverts."

**This tool addresses the deciding entry and nothing beside it.** Marketlake #426 moved the
ledger to last entry wins per ``(partition, check)``, so several checks can withhold one
partition at once, and ``manifest.latest_quarantine`` hands back the one that decides
readability. This signs that one off, so a partition two checks withhold takes one run per
check, and the report names what still holds it after each.

Which check a run picks is the ledger's order rather than a choice made here, and
``manifest.withholding`` is where that order is defined: where each check's current entry sits
in the file. It is deliberately not longest-standing first, because a check that re-states its
verdict moves to the back. No shipped writer re-states one, since ``battery._transition``
appends only on a flip and this tool refuses a repeat in the same direction, so today the order
is the order the checks first withheld. A hand-edited ledger is what separates the two.

**The listing names the deciding check and not every holder.** ``open_quarantines`` reads
``latest_quarantine``, so a partition two checks withhold prints one line naming one of them.
``dashboard._open_quarantines`` carries all of them, which marketlake #426 built, and marketlake
#456 carries the same for this listing. Signing off is not misled by it, because the report
after the write names what still holds the partition.

``--check`` confirms the check about to be written and cannot select a different one, because
the entry this reads is the deciding entry rather than a chosen one. A selector was tried
against the per-check resolution and refused with "no entry carries check X" while an entry did
carry it, which is a refusal that lies about the ledger. Marketlake #456 carries the selector,
built on ``manifest.latest_quarantine_by_check`` and ``manifest.withholding``, along with the
one case a run-per-check cannot reach: revoking a sign-off while a sibling check still
withholds the partition.

**Both directions exist, because one alone is a one-way door.** Signing off appends a ``clean``
entry under the withholding check. ``--revoke`` appends a ``quarantined`` one. Without the
second, a mistaken sign-off is permanent: the precedence rule stops the battery's own check
re-quarantining what a human cleared, so nothing else could undo it. ``docs/design.md`` already
sanctions the row, saying only a different check's failure "or a new human row" may supersede.
The two directions are one command and one flag rather than two subcommands, because they write
the same entry to the same key and differ only in the verdict.

**The write is confirmed, never assumed.** After appending, this re-reads the ledger and raises
unless its own entry is among the entries a reader sees. A sign-off is a hand operation whose
whole value is that it landed, and one condition makes a successful append invisible: a torn
fragment stops ``manifest.parse_jsonl`` at that line, so every entry after it is dropped by
every reader. Measured on a temp lake, eight lines written and two read, with all six sign-offs
among them invisible while each writer returned success.

Marketlake #469 closed the wider half of that for this ledger. ``manifest.read_quarantine``
refuses when whole lines sit behind the stop, so a fragment already in the body refuses in
:func:`_superseded_entry` before this appends anything. What still reaches this guard is the
one shape that hides nothing: a fragment at the tail, which this tool's own append fuses onto
and loses. That is the shape ``manifest.append_line`` accepts, and
``test_a_torn_line_earlier_in_the_ledger_is_caught_rather_than_reported_as_success`` is that
case. Marketlake #447 carries the manifest and corporate-actions ledgers, which read short
still.

The check is membership rather than currency, on purpose. A writer superseding the sign-off
between the append and the read-back is a different outcome from the sign-off never being
readable, and the report says which happened by printing the partition's current entry beside
it. ``test_the_read_back_accepts_a_sign_off_a_later_entry_superseded`` holds that one.

**This tool's earlier window is open and is not the same defect.**
:func:`_superseded_entry` reads the deciding entry outside the hold :func:`append_verdict`
later takes, and it decides three things from that read: whether to refuse, which direction is
legal, and the check token the entry is written under. What can land in the window is a writer
changing the partition's readability. Executed on a temp lake: a battery ``clean`` under the
same check landing there leaves this tool appending a sign-off that changes nothing and
reporting ``partition was: withheld`` for a partition that already read, where the same ledger
state read without the race refuses with "no entry withholds it". That is a line nobody needed
and one wrong word in a report, against ``battery.judge``'s window, which lost a human decision
outright. Marketlake #470 closed that one and left this one, and the difference in what they
cost is the reason rather than any claim that this read is safe.

Closing the battery's did not mean reaching a wider hold *through* ``append_verdict``.
``lake.lock.lake_lock`` is not re-entrant, so a caller holding it deadlocks against that
function taking it. The battery instead takes the hold itself, reads the ledger inside it, and
appends through ``battery.write_verdict``, which is the same two writes without the lock. The
same two levels are what this tool would need to close its own window.

**Nothing here rolls anything back.** ``append_verdict`` appends the line and then refreshes the
manifest entry, and a failure between them leaves the ledger a line ahead of its manifest entry,
which is the sha mismatch the Sunday scrub exists to catch. An append-only ledger has no
rollback, and writing one would make this a second writer of history.

**Where it refuses, and to whom.** Every refusal is one named line on stderr and exit 2, never a
stack, which is the shape ``lake.onboard`` states and ``retire`` and ``reauth`` both use. A
cleared partition rejoins every read with nothing announcing it, so the write is made
deliberate three ways.

1. ``--reason`` is required and rides into the entry.
2. ``--dry-run`` is the same work with the writer switched off, and it reports the verdict the
   write would land rather than the one already there.
3. The report names the partition's readability before and after, so the operator reads the
   consequence rather than a success line.

There is no confirmation prompt, because nothing in ``src/lake`` calls ``input()``.

A lake-state failure keeps its traceback on purpose. ``manifest.latest_quarantine`` raises on a
body line that parses and names no partition, on a read that stopped with whole lines
behind it, which is ``manifest.TornLedger``, and on bytes that are not valid UTF-8, which is
``manifest.LedgerNotUtf8``. All three are a corrupt ledger rather than an operator
mistake, so the stack is what a reader needs. All three refuse the listing as well as the write,
which is correct rather than an oversight: repairing a ledger is a hand edit under the lock,
never an invocation of this tool. An entry that parses and names a
partition while carrying no ``check`` is different: the reader resolves it without complaint,
since ``manifest.is_quarantined`` reads ``verdict`` alone and never looks at ``check``. It is
this tool that has nothing to write under, so the refusal is this tool's to make and it gets a
named line.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from lake.battery import (
    CLEAN_VERDICT,
    PROVENANCE_HUMAN,
    QUARANTINED_VERDICT,
    SIGNOFF_SOURCE,
    append_verdict,
    build_entry,
)
from lake.clock import Clock, SystemClock
from lake.config import input_errors_exit, load_config
from lake.manifest import (
    VERDICT_FIELD,
    is_quarantined,
    latest_quarantine,
    latest_quarantine_by_check,
    quarantine_path,
    read_quarantine,
    withholding,
)


class SignoffError(Exception):
    """Raised when a sign-off cannot proceed, and printed as one named line by ``main``."""


@dataclass(frozen=True)
class OpenQuarantine:
    """One partition the ledger currently withholds, as the listing prints it."""

    partition: str
    check: str | None
    verdict: str | None


@dataclass(frozen=True)
class SignoffReport:
    """What one run did, for the operator to read the consequence off.

    ``before`` and ``after`` are the partition's deciding entry either side of the write, so a
    racing writer that superseded the sign-off shows up as an ``after`` that is not
    ``appended``. ``still_withheld`` is what ``after`` resolves to, which is the fact that
    matters: a partition is readable or it is not.

    **A dry run reports the consequence, not the state it started from.** ``after`` is the entry
    the write would land, because the report's whole job is to say what the operator is about
    to do. Reporting ``before`` there was tried and inverts the line: a dry-run sign-off of a
    withheld partition printed "partition now: withheld", which reads as a write that would
    change nothing. ``dry_run`` is what says the ledger was not touched, and the report says so
    in words rather than by quietly showing the old state.
    """

    partition: str
    check: str
    verdict: str
    reason: str
    before: dict | None
    after: dict | None
    appended: dict
    dry_run: bool
    ledger_path: Path

    @property
    def still_withheld(self) -> bool:
        return is_quarantined(self.after)

    def render(self) -> str:
        cleared = self.verdict == CLEAN_VERDICT
        if self.dry_run:
            head = "Would sign off" if cleared else "Would revoke"
            was, now = "partition is:   ", "would be:       "
        else:
            head = "Signed off" if cleared else "Revoked"
            was, now = "partition was:  ", "partition now:  "
        lines = [
            f"{head} {self.partition}",
            f"  check:           {self.check}",
            f"  verdict written: {self.verdict} (provenance {PROVENANCE_HUMAN})",
            f"  reason:          {self.reason}",
            f"  {was} {'withheld' if is_quarantined(self.before) else 'readable'}",
            f"  {now} {'withheld' if self.still_withheld else 'readable'}",
        ]
        # Only a verdict this run did not write can be a second holder worth naming. The
        # partition's own new verdict is already on the line above, so printing it here again
        # would read as a warning about something else.
        if self.still_withheld and self.after is not None and self.after != self.appended:
            lines.append(f"  still withheld under: {self.after.get('check')!r}")
        lines.append(f"  ledger:          {self.ledger_path}")
        if self.dry_run:
            lines.append("  nothing was written")
        return "\n".join(lines)


def open_quarantines(lake_root: Path | str) -> list[OpenQuarantine]:
    """Every partition the ledger currently withholds, in partition order.

    This is the tool's own discovery surface. A standing quarantine's partition path reaches an
    operator from one place otherwise, the History panel. ``battery.render`` prints the path
    on a hand run of the battery, once per night the check re-finds the fault, which is a
    console the operator has to go and run rather than a list of what is open.
    ``sweep.count_quarantined`` puts a bare count in the nightly report, and an operator holding
    the count and not the path cannot act on it.

    ``key=str`` sorts rather than the values themselves. ``manifest.latest_quarantine`` raises
    on an entry naming no partition, and since marketlake #514 on one whose partition cannot be
    a dict key. It passes through every other non-string, so a hand-repaired ledger can still
    hold an integer key: ``7``, ``1.5``, ``null`` and ``true`` all reach here. Sorting those
    against strings raised ``TypeError`` out of the listing, which is a bare traceback on the
    path three of this module's own refusals send an operator down when they say repairing a
    ledger is a human's job. The damaged key is printed rather than hidden, because seeing it
    is how the human finds what to repair.
    """
    ledger = latest_quarantine(Path(lake_root))
    return [
        OpenQuarantine(
            partition=partition,
            check=entry.get("check"),
            verdict=entry.get(VERDICT_FIELD),
        )
        for partition, entry in sorted(ledger.items(), key=lambda item: str(item[0]))
        if is_quarantined(entry)
    ]


def render_open(quarantines: Sequence[OpenQuarantine], ledger_path: Path) -> str:
    """The listing, which writes nothing and is what a bare invocation prints."""
    if not quarantines:
        return f"No partition is withheld.\n  ledger: {ledger_path}"
    lines = [f"{len(quarantines)} partition(s) withheld:"]
    for open_one in quarantines:
        check = open_one.check if open_one.check else "(no check named)"
        lines.append(f"  {open_one.partition!s}  {open_one.verdict}  under {check}")
    lines.append(f"  ledger: {ledger_path}")
    return "\n".join(lines)


def _superseded_entry(lake_root: Path, partition: str, check: str | None, *, revoke: bool) -> dict:
    """The entry this run supersedes, or a refusal saying why there is none.

    The two directions want opposite starting states, and getting that backwards is why this
    reads the state rather than assuming it. A sign-off wants the partition withheld, because
    clearing what already reads writes a line that changes nothing. A revoke wants it readable,
    for the same reason in reverse. Both want the ledger to hold an entry for it, because the
    entry is where the check token comes from and nothing here mints one.

    Every refusal here is one an operator reaches by typing, so each names what to do rather
    than what went wrong. A path that finds no entry is the common one, and the line lists what
    is open so a mistyped path corrects itself by being read. Spelling is load-bearing rather
    than cosmetic: ``loader.PartitionAbsent`` records that on macOS ``ticker=spy`` opens the
    ``ticker=SPY`` partition while the quarantine lookup keys on the caller's spelling and finds
    no verdict.
    """
    verb = "revoke" if revoke else "sign off"
    entry = latest_quarantine(lake_root).get(partition)
    if entry is None:
        open_now = open_quarantines(lake_root)
        if not open_now:
            raise SignoffError(
                f"nothing to {verb}: the quarantine ledger holds no entry for {partition!r}, "
                "and it withholds no partition at all."
            )
        listed = ", ".join(str(one.partition) for one in open_now)
        raise SignoffError(
            f"nothing to {verb}: the quarantine ledger holds no entry for {partition!r}. The "
            "partition is the lake-relative path, spelled exactly as the ledger keys it, and "
            f"case matters. Withheld now: {listed}"
        )
    withheld = is_quarantined(entry)
    if not revoke and not withheld:
        raise SignoffError(
            f"nothing to sign off for {partition!r}: no entry withholds it, so it already "
            f"reads. Its current verdict is {entry.get(VERDICT_FIELD)!r} under "
            f"{entry.get('check')!r}."
        )
    if revoke and withheld:
        raise SignoffError(
            f"nothing to revoke for {partition!r}: it is already withheld under "
            f"{entry.get('check')!r}."
        )
    written = entry.get("check")
    if not written:
        raise SignoffError(
            f"the current entry for {partition!r} names no check, so there is no token to "
            f"{verb} under and --check cannot supply one. The entry is damaged, and repairing "
            "a ledger is a human's job under the lock."
        )
    if check is not None and check != written:
        state = "withheld" if withheld else "cleared"
        raise SignoffError(
            f"--check says {check!r}, and the entry deciding {partition!r} carries {written!r}, "
            f"so the partition is {state} under {written!r}. Re-run without --check to act on "
            "that entry. This flag confirms the check about to be written and cannot select a "
            "different one."
        )
    return entry


def _projected(lake_root: Path, partition: str, entry: dict) -> dict:
    """The entry that would decide the partition once ``entry`` is appended.

    A dry run has to answer the question the real run answers, and on a partition several
    checks withhold the answer is not the entry being written. Signing off one check leaves the
    others holding, so ``entry`` alone reports "would be: readable" while the real run leaves
    the partition withheld. That was the first draft's bug in a second form: the first showed
    the state before the write, this one showed a write with nothing else in view.

    The projection goes through ``manifest.withholding`` rather than around it, so the rule
    that decides readability is still the ledger's own. ``latest_quarantine_by_check`` pops a
    check before re-inserting it, so that a check re-stating a verdict moves to the back of the
    order, and a sign-off is exactly such a re-statement. This mirrors that rather than
    assuming the position is kept.
    """
    projected = dict(latest_quarantine_by_check(lake_root).get(partition) or {})
    projected.pop(entry["check"], None)
    projected[entry["check"]] = entry
    held = withholding(projected)
    return held[0] if held else entry


def signoff(
    partition: str,
    *,
    reason: str,
    clock: Clock,
    lake_root: Path | str,
    check: str | None = None,
    revoke: bool = False,
    dry_run: bool = False,
) -> SignoffReport:
    """Clear one partition, or withhold it again, and return the report.

    Every dependency is injected, so this runs offline and touches no config. The steps: read
    the partition's current entry, build the superseding entry under that entry's own check,
    append it through ``battery.append_verdict``, then read the ledger back and confirm the
    entry is there.

    Re-running is refused rather than made idempotent, in both directions. A second sign-off
    finds the partition already reading and stops, and a second revoke finds it already
    withheld and stops, because appending a line that changes nothing is what
    ``battery._transition``'s append-on-transition rule exists to prevent.
    """
    root = Path(lake_root)
    reason = reason.strip()
    if not reason:
        raise SignoffError(
            "a reason is required: it rides into the ledger entry and is what tells the next "
            "reader why a human changed this partition's verdict by hand"
        )

    now = clock.now()
    before = _superseded_entry(root, partition, check, revoke=revoke)
    verdict = QUARANTINED_VERDICT if revoke else CLEAN_VERDICT
    entry = build_entry(
        partition=partition,
        verdict=verdict,
        check=before["check"],
        observed_at=now,
        provenance=PROVENANCE_HUMAN,
        reason=reason,
    )

    if dry_run:
        after = _projected(root, partition, entry)
    else:
        append_verdict(root, entry, observed_at=now, source=SIGNOFF_SOURCE)
        if entry not in read_quarantine(root):
            raise SignoffError(
                f"the entry for {partition!r} was appended and does not read back. A torn line "
                f"earlier in {quarantine_path(root)} hides every entry after it, so this "
                "sign-off is on disk and invisible to every reader. Repairing a ledger is a "
                "human's job under the lock."
            )
        after = latest_quarantine(root).get(partition)

    return SignoffReport(
        partition=partition,
        check=before["check"],
        verdict=verdict,
        reason=reason,
        before=before,
        after=after,
        appended=entry,
        dry_run=dry_run,
        ledger_path=quarantine_path(root),
    )


def signoff_from_config(
    partition: str,
    *,
    reason: str,
    clock: Clock | None = None,
    config_path: str | Path | None = None,
    check: str | None = None,
    revoke: bool = False,
    dry_run: bool = False,
) -> SignoffReport:
    """Sign off wired from the real config. This is the entry ``main`` calls.

    It loads the machine-local config for the lake root and nothing else. No vendor is needed,
    because signing off fetches nothing.
    """
    config = load_config(config_path)
    return signoff(
        partition,
        reason=reason,
        clock=clock if clock is not None else SystemClock(),
        lake_root=config.lake_root,
        check=check,
        revoke=revoke,
        dry_run=dry_run,
    )


def list_from_config(config_path: str | Path | None = None) -> str:
    """The listing, wired from the real config. Reads the ledger and writes nothing."""
    root = Path(load_config(config_path).lake_root)
    return render_open(open_quarantines(root), quarantine_path(root))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lake.signoff",
        description=(
            "Resolve one quarantine verdict by hand. With no partition, list what is withheld."
        ),
    )
    parser.add_argument(
        "partition",
        nargs="?",
        help="The lake-relative partition path, exactly as the ledger keys it. Case matters.",
    )
    parser.add_argument(
        "--reason",
        help="Why a human cleared or withheld this partition. Required with a partition.",
    )
    parser.add_argument(
        "--check",
        help="Confirm the check about to be written. It cannot select a different one.",
    )
    parser.add_argument(
        "--revoke",
        action="store_true",
        help="Withhold the partition again instead of clearing it.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the report and write nothing.",
    )
    parser.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    return parser


def main(argv: Sequence[str] | None = None, *, clock: Clock | None = None) -> int:
    """The ``python -m lake.signoff`` entry. Returns a process exit code.

    A refusal prints one line and exits 2, the code and the shape the sibling commands use for
    an operator mistake. It catches ``SignoffError`` and nothing wider. The manifest's own
    errors keep their tracebacks on purpose, because a ledger that will not read is a corrupt
    lake rather than a mistake somebody typed.
    """
    args = _build_parser().parse_args(argv)
    try:
        with input_errors_exit("signoff"):
            if args.partition is None:
                print(list_from_config(args.config))
                return 0
            if args.reason is None:
                raise SignoffError("--reason is required when a partition is named")
            report = signoff_from_config(
                args.partition,
                reason=args.reason,
                clock=clock,
                config_path=args.config,
                check=args.check,
                revoke=args.revoke,
                dry_run=args.dry_run,
            )
    except SignoffError as exc:
        print(f"signoff: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    print(report.render())
    return 0


__all__ = [
    "OpenQuarantine",
    "SignoffError",
    "SignoffReport",
    "list_from_config",
    "main",
    "open_quarantines",
    "render_open",
    "signoff",
    "signoff_from_config",
]


if __name__ == "__main__":
    raise SystemExit(main())
