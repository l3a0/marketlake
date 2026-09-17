"""The manifest ledger.

The manifest is the integrity root. It records one entry per lake file that carries
data. Each entry names the file, where it came from, its sha256 checksum, its row
count, and when it was fetched. So the lake's true contents are pinned in one place,
and drift or loss becomes detectable rather than silent.

The ledger lives at ``manifest.jsonl`` at the lake root. Its rules are few and exact.

1. *One entry is one line.* An entry is appended with a single ``O_APPEND`` write.
   ``O_APPEND`` is the kernel's atomic append mode. Concurrent writers cannot
   interleave within one write, so a line is never half from one writer and half from
   another. A reader that meets a torn trailing line discards it. A crash tears the
   last line, and it stops being the last one as soon as a later append lands: a torn
   write leaves no terminating newline, so the next entry fuses onto the fragment and
   the entries behind that line are unreachable. Marketlake #447 carries what that
   costs the manifest and the corporate-actions ledgers, which still read short.
2. *Last entry wins*, keyed by the file's path. A re-run legitimately appends a second
   entry for the same path. The current truth is the last entry for that path.
3. *Two-way scrub.* Every entry's file must exist and match its last recorded sha.
   And every data file in the lake must have an entry. The second direction catches a
   crash between writing a file and appending its entry.

The quarantine ledger at ``quarantine.jsonl`` follows rules 1 and 3, and resolves on
``(partition, check)`` rather than on the path alone, because several checks judge one
partition and each keeps its own current verdict. :func:`latest_quarantine_by_check` is that
resolution and marketlake #426 is why it is not the path alone. It records
data-quality verdicts per partition. Un-quarantine is a superseding entry, never a
deletion. This module gives it the same append helper and its own reader.

The read is where it parts from rule 1, and marketlake #469 is why. Its entries are a guard,
so a read that stopped with whole lines behind it would resolve to a ledger missing its own
verdicts and admit the partitions they withhold. :func:`read_quarantine` refuses that with
:class:`TornLedger` instead. The manifest's reader keeps the truncating read **for a torn
tail**, because :func:`scrub` resolves through it and a Sunday scrub that raised would fail on
the very file it exists to report. Bytes that will not decode are not that shape, and both
ledgers refuse them as :class:`LedgerNotUtf8`, because a read that cannot decode the file
answers nothing for the scrub to report either way. Marketlake #499 is that half.

The corporate-actions ledger at ``actions/corporate_actions.jsonl`` follows them too, and
it keys on the action rather than on a path, the way the quarantine ledger keys on the
partition and the check, so ``lake.actions`` resolves its own last entry and reuses
``append_line`` and ``parse_jsonl`` for the line rules alone. Those two are public for that
reason: three ledgers now implement one rule, and a second copy of it would be a second
answer to what a torn tail is.

The same ledger judges the backup copy. ``backup_scrub`` walks the rsync target and
checks it against this manifest rather than against the copy of the manifest riding on
the backup, because the lake is the authority and a copy that rotted alongside its data
would pass a check against itself. The copy is read for one thing: its length says how
far the last sync got, so a partition sealed since then reads as not copied yet rather
than as loss.

The scrub reads. It never writes. Both scrubs do. Repair is a separate, deliberate,
human-invoked step under the lake-root lock. This module supplies the primitives that
step and the daily compaction job call.

Times are injected. ``fetched_at`` is passed in by the caller. Nothing here reads a
wall clock.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from lake.paths import (
    DATE_PREFIX,
    JOURNAL_DIR,
    MANIFEST_FILE,
    QUARANTINE_FILE,
    REPORTS_DIR,
    TICKER_PREFIX,
    parse_segment_rel,
)

# What the reverse scrub excludes, enumerated and not implied. The reverse pass asks
# every file under the lake root for a manifest entry, so the few that never get one
# have to be named here. Three are.
#
# 1. The manifest cannot cover itself.
# 2. Journal segments are manifest-less by rule, so the whole tree is out.
# 3. ``reports/`` holds the nightly report, one dated file per sweep run, and four trees
#    beside it: one file per page that never reached the phone, one file per close+5
#    guard run, one file per ticker-day compaction's merge had something to say about,
#    and one file per finding a vendor-sweep gate refused to land. The daemon writes the
#    first two, compaction writes the third, and the sweep writes the fourth. The design
#    puts the tree inside the backup sync root and outside the manifest, and skips it here
#    by name, so a subdirectory added under it needs nothing added here. None of the five
#    is a measurement.
#
# Neither the quarantine ledger nor the corporate-actions ledger is on this list, and
# both are off it deliberately. Each writer refreshes its own manifest entry in the same
# locked invocation that appends the row, so both are scrubbed like any sealed file. That
# is the check that catches a verdict, or an action, written without its entry.
#
# An entry ending in ``/`` is a directory prefix. Any other entry is an exact filename
# at the lake root. The lock adds no file to skip, because it locks the manifest itself.
SCRUB_EXCLUSIONS: tuple[str, ...] = (MANIFEST_FILE, f"{JOURNAL_DIR}/", f"{REPORTS_DIR}/")


# What a quarantine entry has to say for its partition to read. The ledger records
# data-quality verdicts, and ``read_quarantine`` returns each entry as the opaque mapping
# the writer appended. These two names are the one place that mapping is read for meaning,
# so the battery, the sign-off tool and every reader resolve a verdict the same way.
VERDICT_FIELD = "verdict"
CLEAN_VERDICT = "clean"


class RowCountRegression(Exception):
    """Raised when an append would shrink a manifested partition's row count.

    The standing invariant is that no automatic run ever replaces a manifested
    partition with fewer rows than its recorded count. A late segment beside a sealed
    day could otherwise rebuild a full partition from one segment, and every integrity
    layer would bless the loss.
    """

    def __init__(self, partition: str, recorded: int, proposed: int) -> None:
        super().__init__(f"{partition}: proposed {proposed} rows is fewer than recorded {recorded}")
        self.partition = partition
        self.recorded = recorded
        self.proposed = proposed


# -- paths -------------------------------------------------------------------


class ManifestError(Exception):
    """Raised for a ledger line a reader cannot interpret."""


class TornLedger(ManifestError):
    """Raised for a ledger read that stopped with entries still behind it.

    ``parse_jsonl`` ends the read at the first line it cannot parse. When that line is the
    last one, nothing is hidden and the read is complete, which is the torn tail
    ``append_line`` accepts. When it is anywhere else, every entry after it is invisible to
    every reader while the file still holds them, and a guard resolved from those entries
    answers from a ledger that is missing its own contents.
    """


class LedgerNotUtf8(ManifestError):
    """Raised for a ledger whose bytes this reader cannot decode as UTF-8.

    ``read_text`` decodes strictly, so one byte that is not valid UTF-8 raises
    ``UnicodeDecodeError``. That is a ``ValueError`` rather than a ``ManifestError`` or an
    ``OSError``, so it lands in none of the tuples a damaged ledger is meant to be caught by,
    and the whole 18:30 run ends on it. Executed against `9e767ab`, one flipped byte in
    ``quarantine.jsonl`` killed the sweep inside the bar walk, with no record filed, no report,
    no ping and, on a Friday, no Sunday wake. Marketlake #495 is that defect.

    **Refusing is what keeps the guard from inverting. Decoding with a replacement would
    invert it.** ``bytes.decode("utf-8", "replace")`` was the obvious one-line answer and it is
    wrong here, because a replacement character inside a JSON string leaves the line valid
    JSON with one field silently rewritten. Executed: a byte flipped inside a partition name
    files that entry under a mangled key, so the partition the verdict withholds no longer
    appears in the ledger at all and reads clean, while ``sweep.count_quarantined`` still
    reports one quarantine standing. That is ``is_quarantined``'s stated rule inverted, on
    data already sealed. ``_backup_scrub`` decodes with a replacement for a different
    situation and says so: there the damage is a prefix split mid-character, which sits in the
    torn tail ``parse_jsonl`` discards anyway.

    **It is a ``ManifestError`` so that it needs no new containment anywhere.** Four consumers
    already state in writing what a damaged one raises: ``loader.load_chain`` and
    ``loader.load_bars`` both name ``ManifestError``, ``sweep._LEDGER_REFUSALS`` names it as
    the class, and ``lake.battery``'s command catches it to print a line instead of a stack.
    ``lake.dashboard`` is a fifth reader and states nothing, because it catches bare
    ``Exception`` and reports whatever class it met. Marketlake #469 built that containment for
    ``TornLedger``, and this inherits all of it rather than widening a tuple to reach a
    ``ValueError``.
    """


def manifest_path(lake_root: Path) -> Path:
    """The manifest path for a lake, derived from its root."""
    return Path(lake_root) / MANIFEST_FILE


def quarantine_path(lake_root: Path) -> Path:
    """The quarantine-ledger path for a lake, derived from its root."""
    return Path(lake_root) / QUARANTINE_FILE


# -- checksums ---------------------------------------------------------------


def sha256_bytes(data: bytes) -> str:
    """The sha256 hex digest of bytes already in hand."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """The sha256 hex digest of a file's bytes."""
    return sha256_bytes(Path(path).read_bytes())


# -- reading -----------------------------------------------------------------


# What a ledger loses when its bytes will not decode, one sentence per ledger. The decode is one
# rule and the consequence is not. The quarantine ledger stops being able to say what it
# withholds, and the manifest stops being able to say what the lake holds at all. One sentence
# covering both would name neither, and an operator meeting one of these is reading it to learn
# which file to open and what repairing it is worth.
QUARANTINE_CONSEQUENCE = (
    "Every verdict in this file is unreadable until that byte is repaired, so this ledger "
    "cannot say which partitions it withholds."
)
MANIFEST_CONSEQUENCE = (
    "Every entry in this file is unreadable until that byte is repaired, so this ledger cannot "
    "say which partitions the lake holds, how many rows each one has, or what its checksum was."
)


def decode_utf8(path: Path, raw: bytes, *, consequence: str) -> str:
    """A ledger's bytes as text, or :class:`LedgerNotUtf8` naming the byte that refused.

    ``read_text`` is not used, because its ``UnicodeDecodeError`` reaches none of the tuples
    that name a class, which is where a damaged ledger is meant to land. Two consumers do
    survive it either way, ``sweep._counted`` and ``dashboard._open_quarantines``, because both
    catch bare ``Exception``. What they gain here is a class with a name rather than a
    ``ValueError`` nothing expected. :class:`LedgerNotUtf8`'s own docstring carries why refusing
    beats decoding with a replacement.

    It also pins the encoding. ``read_text`` with no argument decodes in the **locale's**
    encoding rather than UTF-8. Python 3.12 turns UTF-8 mode on by itself under a C locale, so
    a bare ``LC_ALL=C`` is harmless, and it takes UTF-8 mode being off as well before the
    decode narrows to ASCII. Measured: with ``LC_ALL=C`` and ``-X utf8=0``, ``read_text``
    refuses a file that is perfectly good UTF-8. A ledger a writer produced survives that
    anyway, because ``json.dumps`` leaves ``ensure_ascii`` at its default and pure ASCII decodes
    under US-ASCII, and no installed launchd job sets a locale or turns UTF-8 mode off. So what
    pinning removes is a dependence on an interpreter flag nobody tracks rather than a failure
    anything has met.

    **This is the shared rule, and :func:`_decode` is the quarantine ledger's own reader.** Both
    ledgers refuse bytes that will not decode, and the refusal is one implementation, which is
    why it sits apart from the caller. What deliberately does not cross between them is anything
    one ledger decides for itself. Marketlake #506 settled what the quarantine ledger does about
    a byte-order mark and marketlake #519 is that question for the manifest, still open and
    owned by nobody. A manifest read routed through :func:`_decode` would inherit #506's answer
    without anyone deciding it should, so it routes through here instead and #519 keeps its
    question.

    **The consequence sentence belongs to the caller.** The byte, the line and the repair are
    the same for both ledgers. What the damage costs is not, so the caller supplies it rather
    than this function naming one ledger's loss on the other's file.

    **The message sends the person repairing the file to the byte and to the line.** The
    exception carries the byte offset alone, which is the wrong unit for an editor, so the
    line is counted from the newlines in front of it. That is the same care
    :func:`_refuse_hidden_entries` takes over its own line number and for the same reader: no
    writer here emits a byte outside ASCII, so a file that holds one is a file somebody is
    already repairing by hand.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        line = raw.count(b"\n", 0, exc.start) + 1
        raise LedgerNotUtf8(
            f"{path}: byte {exc.start} on line {line} is {raw[exc.start]:#04x}, which is not "
            f"valid UTF-8 ({exc.reason}). Nothing in this lake writes a byte outside ASCII, so "
            f"these bytes were changed by something other than a writer. {consequence} "
            "Repairing a ledger is a human's job under the lock."
        ) from exc


def parse_jsonl(text: str) -> list[dict]:
    """Parse ledger text into entries, discarding a torn trailing line.

    A blank line is skipped. The first line that does not parse ends the read.

    **That line is the last one only until a later append lands behind it.** A torn write
    leaves no terminating newline, so the next append concatenates onto the fragment and the
    fused line sits in the body with every entry after it unread. ``append_line`` states the
    fusing rule and marketlake #447 carries the class. :func:`read_quarantine` refuses that
    case for the quarantine ledger, where the entries behind it are a guard's own verdicts.
    """
    entries: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            # A torn trailing line is discarded, the same rule as the journal tail.
            break
    return entries


def _read_jsonl(path: Path) -> list[dict]:
    """Read a ledger file into entries. A missing file reads as empty.

    Bytes that will not decode raise :class:`LedgerNotUtf8` rather than truncating the read, and
    marketlake #499 is why. The truncating read :func:`parse_jsonl` performs is deliberate for a
    torn tail, where the read still answers and the entries in front of the damage are real. It
    answers nothing here, because a byte that will not decode leaves no text to parse at all.

    **The refusal takes nothing away, because this read already raised.** Executed against
    `6496640`, :func:`read_manifest`, :func:`latest_entries`, :func:`scrub`,
    :func:`would_shrink` and :func:`guard_row_count` every one raised ``UnicodeDecodeError`` on
    such a file. That is a ``ValueError``, so it is neither a ``ManifestError`` nor an
    ``OSError``, and it landed in none of the tuples a damaged ledger is meant to be caught by.
    What changes is where it lands rather than whether it raises.

    **It moves one step from ending the 18:30 run to reporting a refusal, and no more.**
    ``sweep._LEDGER_REFUSALS`` names ``ManifestError``, and the dividend and split walks resolve
    this file through ``actions.surface_ticker_days``, so those two now file a refused piece
    where they used to take the whole run down. ``sweep._BARS_REFUSALS`` names no
    ``ManifestError``, so the bar walk still ends the run, on a named class instead of a bare
    ``ValueError``. Marketlake #517 carries that half and this change does not do it.

    :func:`_backup_scrub` is deliberately not routed through this. It reads the manifest's bytes
    itself and decodes them with a replacement, so the Sunday backup canary keeps answering on a
    file every other reader now refuses. An alarm that raised on the damage it exists to report
    would be the worse trade, which is the argument :func:`read_quarantine` makes for
    :func:`scrub` and the torn tail.
    """
    path = Path(path)
    if not path.exists():
        return []
    return parse_jsonl(decode_utf8(path, path.read_bytes(), consequence=MANIFEST_CONSEQUENCE))


def _latest_by_partition(entries: Sequence[dict], path: Path) -> dict[str, dict]:
    """Resolve last-entry-wins per partition path over entries in file order.

    Two shapes raise ``ManifestError``, and they are deliberately two messages rather than
    one. A line that parses as JSON and names no partition is the first. A line naming a
    partition that cannot be a dict key, which is a JSON list or object, is the second, and
    folding it into the first would tell the person repairing the ledger that an entry
    carrying a partition carries none.

    Skipping either was considered and rejected. The torn-tail precedent does not carry: a
    torn trailing line is a write that did not finish, which ``_read_jsonl`` already
    discards, while a line in the body that parses and names nothing is a record no reader
    can interpret. This file is the lake's integrity root, so a reader that quietly stepped
    over damage in it would make every check downstream weaker than it reads.

    Raising is safe precisely because the two callers that must survive it already catch
    it: the close+5 guard's prologue and the marking pass. Both catch bare ``Exception``, so
    the second shape needs nothing from them that the first did not already have. What it
    gains them is a message naming this ledger and the entry, where a bare ``TypeError``
    named neither. Every other caller is a place where stopping is correct, and the
    compaction child's own silence pages.
    """
    latest: dict[str, dict] = {}
    for position, entry in enumerate(entries, start=1):
        try:
            partition = entry["partition"]
        except (KeyError, TypeError) as exc:
            # TypeError covers a line that parsed to something other than an object, such
            # as a bare list or string, which indexes differently but is damage the same.
            raise ManifestError(f"{path}: entry {position} names no partition") from exc
        try:
            latest[partition] = entry
        except TypeError as exc:
            raise ManifestError(
                f"{path}: entry {position} has a partition that cannot be a key: {partition!r}"
            ) from exc
    return latest


def read_manifest(lake_root: Path) -> list[dict]:
    """Every manifest entry in file order, with the torn trailing line discarded."""
    return _read_jsonl(manifest_path(lake_root))


def latest_entries(lake_root: Path) -> dict[str, dict]:
    """The current authoritative manifest entry per partition path."""
    return _latest_by_partition(read_manifest(lake_root), manifest_path(lake_root))


def _refuse_hidden_entries(path: Path, text: str, entries: Sequence[dict]) -> None:
    """Raise :class:`TornLedger` when whole lines sit after the point the read stopped at.

    ``parse_jsonl`` ends the read at the first line it cannot parse, so the count it returns
    says where it stopped: at line ``len(entries) + 1``. Every non-blank line after that one
    is a complete entry some writer landed and no reader can see. That count is what decides,
    and it is zero in the two cases that are not damage: a file the read consumed whole, and
    a file whose last line is the torn tail ``parse_jsonl`` discards on purpose.

    **The line the read stopped at is not counted, and that is deliberate.** A torn write
    leaves no terminating newline, so the next append concatenates onto the fragment and the
    two become one line. That line costs exactly the one entry appended onto it, which is the
    price ``append_line`` states and accepts and which ``lake.signoff`` guards for a hand
    write. What neither of them accepts, and what this refuses, is every further entry behind
    that line.

    **The fused line is permanent, so the refusal takes hold on the write after it.** Nothing
    repairs an append-only file, so every later append lands behind a line no read gets past.
    Executed on a temp lake, a verdict torn on night one costs night two's verdict to the
    fusion and night three's to the body, and every read from night four on refuses with the
    file frozen at two lines. Two verdicts, then it holds, against a ledger that grows a
    hidden line and fires a page every night without this.

    A fragment whose partial write did end in a newline is its own line rather than a fusion.
    It costs no entry of its own, and the lines after it are counted here like any others.

    **The number in the message is the line as an editor numbers it**, which is why the
    positions are collected rather than the non-blank lines counted. The parsed count alone
    gives the stop's position among non-blank lines, and the two diverge the moment the file
    holds a blank one. That number's whole job is to send the person repairing the file to the
    right line, and no writer here makes a blank line, so a file that has one is already a
    hand-edited file and its reader is exactly the person this addresses.

    What sits behind the stop is counted as lines rather than entries, because damage does not
    have to be well formed. They are whole written lines no reader reaches, and on any ledger a
    writer produced they are verdicts.

    ``hidden`` is the only guard the index needs, so no length check sits above it.
    ``parse_jsonl`` yields at most one entry per non-blank line, so the entries never outnumber
    the positions, and a positive ``hidden`` is exactly the statement that ``len(entries)`` is a
    position this list holds. A length check there was tried and the mutation review found it
    inert: every input it would have returned on, ``hidden <= 0`` returns on first.
    """
    positions = [number for number, line in enumerate(text.splitlines(), start=1) if line.strip()]
    hidden = len(positions) - len(entries) - 1
    if hidden <= 0:
        return
    raise TornLedger(
        f"{path}: the read stopped at line {positions[len(entries)]} and {hidden} "
        f"line{'' if hidden == 1 else 's'} after it {'is' if hidden == 1 else 'are'} written "
        "and unreachable. Every verdict behind that line is invisible, so this ledger cannot "
        "say which partitions it withholds. Repairing a ledger is a human's job under the lock."
    )


def _decode(path: Path, raw: bytes) -> str:
    """The quarantine ledger's bytes as text, or the refusal its damage earns.

    :func:`decode_utf8` is the whole of it today, and the two are kept apart anyway, because
    this is where a rule belonging to this ledger alone goes. Marketlake #506 is adding one, a
    refusal for a byte-order mark, which decodes cleanly and still cannot be read past.
    Marketlake #519 is that same question for the manifest and it is open, so the seam is what
    lets the first land without deciding the second: :func:`_read_jsonl` reads through
    :func:`decode_utf8` and meets only the shared refusal.
    """
    return decode_utf8(path, raw, consequence=QUARANTINE_CONSEQUENCE)


def read_quarantine(lake_root: Path) -> list[dict]:
    """Every quarantine entry in file order, with the torn trailing line discarded.

    Two shapes refuse rather than reading short, and both are a :class:`ManifestError`. A read
    that stops in the body raises :class:`TornLedger` rather than returning the entries in
    front of the damage. Bytes that do not decode raise :class:`LedgerNotUtf8`. This is the one
    reader every quarantine consumer funnels through, so both refusals reach all of them from
    one place.

    **Why the torn-tail refusal is here and not in ``parse_jsonl``.** The rule is the same for
    both ledgers and the consequences are not. :func:`scrub` resolves the manifest through
    :func:`latest_entries`, so a manifest raising on a torn tail would take the Sunday scrub
    down on exactly the file it exists to report, and the entries in front of the tear are real
    and are what the scrub reads. Marketlake #447 carries the manifest ledger and the scrub's
    own reporting of damage. This ledger's readers are a guard, and a guard that cannot read its
    own ledger has to refuse rather than admit, which is :func:`is_quarantined`'s stated rule at
    file scope.

    That argument is about a read that still answers, so it does not reach
    :class:`LedgerNotUtf8`. Both ledgers refuse those bytes, because neither read answers
    anything on a file it cannot decode. Marketlake #499 moved the manifest's half.

    ``_read_jsonl`` is not reused because this needs the text the count is taken from, and
    that function returns entries alone. A missing ledger still reads as no entries, which is
    what keeps every consumer inert on a lake no verdict has been written to yet.
    """
    path = quarantine_path(lake_root)
    if not path.exists():
        return []
    text = _decode(path, path.read_bytes())
    entries = parse_jsonl(text)
    _refuse_hidden_entries(path, text, entries)
    return entries


def latest_quarantine_by_check(lake_root: Path) -> dict[str, dict[str, dict]]:
    """Every partition's current verdict per check: last entry wins within each check.

    The ledger records one check's answer about one partition, and several checks judge the
    same partition. Resolving on the partition alone throws the ``check`` field away and keeps
    whichever line landed last, so one check's ``clean`` buries another check's quarantine and
    the partition reads. Marketlake #426 is that defect.

    The composite key is not a departure from last entry wins. ``actions._latest_by_key``
    already resolves this way on ``(instrument_id, ex_date, type)``, and every quarantine entry
    already carries ``check``, because ``battery.build_entry`` refuses one without it. On a
    ledger written by a single check the two resolutions agree entry for entry, so nothing on
    disk has to change.

    **The key is ``check`` alone, never ``(check, provenance)``.** A human sign-off has to be
    able to clear the battery's quarantine under the same check, which is the whole of
    marketlake #139's purpose. Splitting them by provenance would leave the battery's entry
    standing beside the sign-off and withhold the partition forever.

    Each partition's inner mapping is ordered by where that check's *current* entry sits in the
    file, which is why an existing check is removed before it is re-inserted. A plain
    reassignment keeps the position a key was first seen at, and the two disagree as soon as a
    check has written twice. The order is what :func:`withholding` hands back, and that
    function's own docstring is where what the order does and does not mean is stated: it is
    where each check's current entry sits, which is not the same as longest-standing first.

    An entry whose ``partition`` or whose ``check`` cannot be a dict key raises
    ``ManifestError`` naming this ledger and the entry's position, for the reason
    ``_latest_by_partition`` gives about a missing ``partition``: this file is an integrity
    root, so a reader that stepped over damage in it would make every check downstream weaker
    than it reads.

    The ``partition`` guard is marketlake #514. Until it existed, that shape raised a bare
    ``TypeError``, which is neither a ``ManifestError`` nor an ``OSError``, so it reached
    none of the tuples a damaged ledger is meant to land in and ended the whole 18:30 run.
    The containment held for this reader's other shapes throughout, which is the point: four
    of them already landed as a ``ManifestError`` and the fifth walked past every one of
    their catches. That is the same escape marketlake #495 closed for a ledger whose bytes do
    not decode.
    """
    path = quarantine_path(lake_root)
    latest: dict[str, dict[str, dict]] = {}
    for position, entry in enumerate(read_quarantine(lake_root), start=1):
        try:
            partition = entry["partition"]
        except (KeyError, TypeError) as exc:
            raise ManifestError(f"{path}: entry {position} names no partition") from exc
        try:
            bucket = latest.setdefault(partition, {})
        except TypeError as exc:
            raise ManifestError(
                f"{path}: entry {position} has a partition that cannot be a key: {partition!r}"
            ) from exc
        check = entry.get("check")
        try:
            bucket.pop(check, None)
            bucket[check] = entry
        except TypeError as exc:
            raise ManifestError(
                f"{path}: entry {position} has a check that cannot be a key: {check!r}"
            ) from exc
    return latest


def withholding(by_check: dict[str, dict] | None) -> tuple[dict, ...]:
    """The entries currently withholding one partition, in the ledger's own order.

    The order is where each check's *current* entry sits in the file, so the check that last
    re-stated its verdict comes last. That is deliberately not "longest-standing first": a
    check withholding since line 1 that re-wrote at line 3 sorts after one that first withheld
    at line 2. Readability does not depend on the order, and every consumer that shows it
    shows all of them.

    ``by_check`` is what :func:`latest_quarantine_by_check` returns for one partition, or
    ``None`` when the ledger holds no entry for it. An empty result means the partition reads.

    This is :func:`is_quarantined` folded over every check rather than a second definition of
    what an entry means. One definition is the point: the battery, the sign-off tool and every
    reader resolve a verdict the same way, and a reader that grew its own spelling would
    silently invert the exclusion.
    """
    if not by_check:
        return ()
    return tuple(entry for entry in by_check.values() if is_quarantined(entry))


def latest_quarantine(lake_root: Path) -> dict[str, dict]:
    """The entry that decides each partition's readability.

    That is the first entry :func:`withholding` returns, or the last entry written when none
    withholds. It is deliberately not the ledger's chronologically last line
    for the partition: once several checks judge one partition, the last line can be a ``clean``
    from a check that never saw the fault another check is still holding.

    The shape is unchanged, so ``sweep.count_quarantined`` and ``dashboard._open_quarantines``
    read it exactly as before and both become correct. A caller that wants each check's own
    answer, rather than the one that decides the read, wants
    :func:`latest_quarantine_by_check`. A caller that wants the untouched history wants
    :func:`read_quarantine`.
    """
    decided: dict[str, dict] = {}
    for partition, by_check in latest_quarantine_by_check(lake_root).items():
        held = withholding(by_check)
        decided[partition] = held[0] if held else next(reversed(by_check.values()))
    return decided


def is_quarantined(entry: dict | None) -> bool:
    """Whether a partition's current quarantine entry withholds it from a read.

    ``entry`` is what ``latest_quarantine`` returns for one partition, or ``None`` when
    the ledger holds no entry for it. A partition nothing has judged reads, which is what kept
    the guard inert until marketlake #406's battery wrote the first verdict. ``lake.battery``
    is that writer and it runs nightly inside the 18:30 sweep.

    An entry clears its partition by carrying ``verdict: "clean"``. Every other entry
    withholds it, including one whose shape this does not recognise, because fail closed
    for data already sealed means an unreadable verdict refuses rather than admits.

    This answers about one entry. Several checks judge one partition, so what decides a
    partition is :func:`withholding` folded over every check's current entry, and
    :func:`latest_quarantine` hands back the one that decides.

    The rule sits beside the ledger rather than inside its first reader, because reader and
    writer have to meet at one definition or the exclusion silently inverts. A sign-off tool
    writing its own spelling of "cleared" would leave a partition it just cleared refused
    forever. Marketlake #139 built that tool as ``lake.signoff``, and it writes
    :data:`CLEAN_VERDICT` from ``lake.battery`` rather than a spelling of its own, so the
    hypothesis this paragraph was written against is now settled rather than open.
    """
    return entry is not None and entry.get(VERDICT_FIELD) != CLEAN_VERDICT


# -- appending ---------------------------------------------------------------


def append_line(path: Path, entry: dict) -> None:
    """Append one entry as exactly one line via a single ``O_APPEND`` write.

    ``sort_keys`` keeps the on-disk bytes stable across callers. The line is written
    in one ``os.write`` so it cannot interleave with a concurrent append.

    One write is the whole rule, so nothing here reads the file first. Starting a new line
    when the file does not end in one was tried, to keep a torn fragment from fusing with
    the next entry, and it broke this rule two ways. It takes a second ``os.write``, which
    another writer can interleave with, and its check can observe a concurrent write
    partway and insert a blank line. ``test_concurrent_appends_never_interleave`` caught
    the second within one run. A torn fragment therefore still costs the entry appended
    after it, and repairing one is a human's job under the lock.

    This is the line primitive rather than the way to record a partition. A manifest entry
    goes through ``append_manifest``, which enforces the standing row-count invariant
    first.
    """
    line = (json.dumps(entry, sort_keys=True) + "\n").encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def would_shrink(lake_root: Path, partition: str, rows: int) -> bool:
    """Whether recording ``rows`` for ``partition`` is fewer than its recorded count."""
    current = latest_entries(lake_root).get(partition)
    return current is not None and rows < current["rows"]


def guard_row_count(lake_root: Path, partition: str, rows: int) -> None:
    """Refuse an append that would shrink a manifested partition. Raises on regression."""
    current = latest_entries(lake_root).get(partition)
    if current is not None and rows < current["rows"]:
        raise RowCountRegression(partition, current["rows"], rows)


def append_manifest(
    lake_root: Path,
    *,
    partition: str,
    source: str,
    sha256: str,
    rows: int,
    fetched_at: str | None,
    guard: bool = True,
) -> dict:
    """Append one manifest entry and return it.

    The entry shape matches the fixture-lake builder: ``partition``, ``source``,
    ``sha256``, ``rows``, ``fetched_at``. With ``guard`` on, the standing row-count
    invariant is enforced first. A deliberate human recompaction passes ``guard=False``
    to supersede an entry on its own authority.
    """
    if guard:
        guard_row_count(lake_root, partition, rows)
    entry = {
        "partition": partition,
        "source": source,
        "sha256": sha256,
        "rows": rows,
        "fetched_at": fetched_at,
    }
    append_line(manifest_path(lake_root), entry)
    return entry


def record_partition(
    lake_root: Path,
    partition: str,
    *,
    source: str,
    rows: int,
    fetched_at: str | None,
    guard: bool = True,
) -> dict:
    """Checksum a partition file on disk and append its manifest entry.

    This is the compaction path in one call. ``partition`` is the lake-relative path
    of a file that already exists under ``lake_root``. Its sha256 is read from disk, so
    the entry always matches the bytes on disk at record time.
    """
    sha256 = sha256_file(Path(lake_root) / partition)
    return append_manifest(
        lake_root,
        partition=partition,
        source=source,
        sha256=sha256,
        rows=rows,
        fetched_at=fetched_at,
        guard=guard,
    )


def append_quarantine(lake_root: Path, entry: dict) -> dict:
    """Append one quarantine entry as a single ``O_APPEND`` line and return it.

    The entry is keyed by ``(partition, check)``, so last entry wins within each check and an
    un-quarantine is a superseding row rather than a deletion of history.
    :func:`latest_quarantine_by_check` says why the key carries the check, and marketlake #426
    is the defect that resolving on the partition alone produced.

    **This is the line and nothing else. A writer wants ``battery.append_verdict``.** This takes
    no lock and refreshes no manifest entry, so a verdict written through it alone leaves
    ``quarantine.jsonl`` an orphan to the Sunday scrub, which the comment above
    :data:`SCRUB_EXCLUSIONS` says is exactly the check that catches one. It also accepts any
    mapping, including a verdict spelling ``is_quarantined`` refuses to clear, where
    ``battery.build_entry`` is the one place an entry is assembled and checked.

    It stays public because the two ledgers' line rules live here and a test writing a
    deliberately malformed entry needs a way past the checked builder.
    """
    append_line(quarantine_path(lake_root), entry)
    return entry


# -- the two-way scrub -------------------------------------------------------


@dataclass(frozen=True)
class ScrubResult:
    """The verdict of a two-way scrub.

    Three tuples of partition paths name what is wrong, and in which direction.

    - ``missing``: a manifest entry whose file is gone. A forward-pass failure.
    - ``sha_mismatches``: a file present but not matching its last recorded sha. A
      forward-pass failure.
    - ``orphans``: a data file with no manifest entry. A reverse-pass failure.
    """

    missing: tuple[str, ...]
    sha_mismatches: tuple[str, ...]
    orphans: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """Whether the scrub found nothing wrong in either direction."""
        return not (self.missing or self.sha_mismatches or self.orphans)


def _compacted_partition_for_segment(rel: str) -> str | None:
    """The compacted partition path a journal segment merges into, or ``None``.

    A slice-1 cron run may append a manifest entry keyed by a segment path under
    ``journal/date=D/surface=S/ticker=T/seg-<...>.arrows``. That segment merges into
    the compacted partition ``S/ticker=T/date=D.parquet``. This maps one to the other
    so the scrub can tell when the segment entry has been superseded.

    Deciding whether ``rel`` is a segment at all, and splitting it into its parts, is
    ``parse_segment_rel``'s job. It lives beside the builder that made the path. What is
    left here is the one thing only the scrub needs, the mapping onto the compacted path.
    """
    ref = parse_segment_rel(rel)
    if ref is None:
        return None
    return f"{ref.surface}/{TICKER_PREFIX}{ref.ticker}/{DATE_PREFIX}{ref.day}.parquet"


def _is_excluded(rel: str, exclusions: Sequence[str]) -> bool:
    """Whether a lake-relative path is exempt from the reverse pass."""
    for item in exclusions:
        if item.endswith("/"):
            if rel.startswith(item):
                return True
        elif rel == item:
            return True
    return False


def scrub(lake_root: Path) -> ScrubResult:
    """Run the two-way integrity scrub over a lake. Read-only, never mutating.

    Forward pass: every manifest entry's file must exist and match its last recorded
    sha. A slice-1 segment entry is treated as superseded once an entry exists for its
    matching compacted partition, so compaction's verify-then-delete never strands it.

    Reverse pass: every data file under the lake root must have a manifest entry. The
    enumerated exclusion set is skipped. The lock adds nothing to skip, because it
    locks the manifest, which is already excluded.
    """
    root = Path(lake_root)
    latest = latest_entries(root)

    missing: list[str] = []
    sha_mismatches: list[str] = []
    for partition, entry in latest.items():
        compacted = _compacted_partition_for_segment(partition)
        if compacted is not None and compacted in latest:
            # The segment entry is superseded by its compacted partition. Its file may
            # already be deleted, so it is not a forward-pass failure.
            continue
        path = root / partition
        if not path.exists():
            missing.append(partition)
        elif sha256_file(path) != entry["sha256"]:
            sha_mismatches.append(partition)

    orphans: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if _is_excluded(rel, SCRUB_EXCLUSIONS):
            continue
        if rel not in latest:
            orphans.append(rel)

    return ScrubResult(
        tuple(sorted(missing)),
        tuple(sorted(sha_mismatches)),
        tuple(sorted(orphans)),
    )


# -- the backup-copy scrub ---------------------------------------------------

# How many paths one finding names before it stops and says how many are left. A disk
# going bad names every file it carries, and a line per partition would bury every other
# finding the same run made.
_NAMED_PATHS = 3


def _named(label: str, paths: Sequence[str]) -> list[str]:
    """One line per path, up to the cap, then a line saying how many are left."""
    lines = [f"{label}: {rel}" for rel in paths[:_NAMED_PATHS]]
    if len(paths) > _NAMED_PATHS:
        lines.append(f"{label}: and {len(paths) - _NAMED_PATHS} more")
    return lines


def _first_difference(source: bytes, backup: bytes) -> int:
    """The byte offset where the backup's manifest copy stops matching the lake's.

    When the copy matches for its whole length and is longer, the offset is the end of
    the lake's own ledger, which is where the copy started carrying bytes the lake does
    not have.
    """
    limit = min(len(source), len(backup))
    for index in range(limit):
        if source[index] != backup[index]:
            return index
    return limit


@dataclass(frozen=True)
class BackupScrubResult:
    """The verdict of a scrub over the backup copy.

    ``target`` is the backup root the scrub walked, and every finding repeats it, so a
    line read on its own names the disk it came from.

    Three tuples name what is wrong with the copied files. Each withholds the Sunday
    ping, because each is the copy no longer holding what the lake says it holds.

    - ``missing``: the backup carried this partition at its own watermark and does not
      now.
    - ``sha_mismatches``: the backup carries it and its bytes do not match the sha the
      lake's manifest recorded. This is the silent bit rot the scrub exists to catch.
    - ``unaccounted``: the backup carries a partition whose entry sits past the
      watermark. The sync copies a file and the ledger line that records it from one
      locked snapshot, so a file can never arrive ahead of its line. Seeing one means
      the copy's manifest is shorter than the copy, which makes the watermark a number
      that cannot be trusted.

    Four fields name a scrub that could not run to the end. Each stops the walk where it
    stands, because every answer past it would be derived from a reading already known
    to be wrong, and each withholds the ping for the same reason: a week the scrub could
    not run is a week nothing looked at the copy.

    - ``target_missing``: the backup target is not a mounted directory.
    - ``unreadable``: a read of the target failed partway through.
    - ``manifest_missing``: the target is mounted and carries no usable manifest copy,
      either because the file is absent or because it holds no entries while the lake
      holds some. So no sync has ever landed, or the copy's integrity root is gone.
    - ``manifest_diverged_at``: the byte offset where the copy's manifest stops matching
      the lake's. ``None`` when the copy is a clean prefix.

    Two tuples are reported and never withhold the ping, because neither can be lake
    data going missing.

    - ``orphans``: a file on the backup that the lake's manifest does not record at all.
      A killed ``rsync`` leaves its hidden temp behind, and the design puts that orphan
      here rather than on ``rsync``. So does the debris macOS writes to any mounted
      volume on its own, which is why an orphan rides the report rather than paging.
    - ``pending``: the partitions the lake manifested after the backup's last sync,
      which the backup legitimately does not carry yet.
    """

    target: str
    missing: tuple[str, ...] = ()
    sha_mismatches: tuple[str, ...] = ()
    unaccounted: tuple[str, ...] = ()
    orphans: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    target_missing: bool = False
    manifest_missing: bool = False
    manifest_diverged_at: int | None = None
    unreadable: str | None = None

    @property
    def problem(self) -> str | None:
        """The one finding that withholds the ping, or ``None`` when there is none.

        The four stopping conditions are mutually exclusive by construction, because
        each returns before the next can be reached, so one line always says the whole
        verdict. ``orphans`` and ``pending`` are deliberately absent. An extra file on
        the copy costs space rather than data, and a copy behind its lake is the normal
        state between one sync and the next. Both are named in ``notes`` instead.
        """
        if self.target_missing:
            return f"backup target not mounted: {self.target}"
        if self.unreadable is not None:
            return f"backup could not be read: {self.unreadable}"
        if self.manifest_missing:
            return f"backup carries no usable manifest copy: {self.target}"
        if self.manifest_diverged_at is not None:
            return (
                "backup manifest copy diverged from the lake's at byte "
                f"{self.manifest_diverged_at}: {self.target}"
            )
        if not (self.missing or self.sha_mismatches or self.unaccounted):
            return None
        return (
            f"backup scrub failed: missing={len(self.missing)} "
            f"sha_mismatches={len(self.sha_mismatches)} "
            f"unaccounted={len(self.unaccounted)}: {self.target}"
        )

    @property
    def notes(self) -> tuple[str, ...]:
        """The report-tier lines, which name what a count alone cannot.

        Every wrong file is named here, capped, including the ones ``problem`` already
        counted. A count says whether to ping and a path says where to look, and an
        operator handed "sha_mismatches=1" and nothing else cannot act on it.
        """
        lines = _named("backup file does not match the lake", self.sha_mismatches)
        lines += _named("backup file gone", self.missing)
        lines += _named("backup holds a file its manifest copy does not reach", self.unaccounted)
        lines += _named("backup file the lake never recorded", self.orphans)
        if self.pending:
            lines.append(f"backup behind the lake by {len(self.pending)} partitions")
        return tuple(lines)

    @property
    def ok(self) -> bool:
        """Whether the copy holds what the lake says it should.

        This is ``problem is None`` and nothing more, so the two can never disagree. A
        reported orphan and a backup behind its lake both leave it true, which is the
        point: neither is lake data going missing.
        """
        return self.problem is None


def backup_scrub(lake_root: Path, backup_root: Path) -> BackupScrubResult:
    """Scrub the backup copy against the lake's manifest. Read-only, never mutating.

    The lake's manifest is the authority, not the backup's copy of it. A backup checked
    against its own copy can only say it is self-consistent, and a copy whose manifest
    rotted alongside its data says yes to that question while being wrong. Checking the
    copied files against the lake's own ledger answers both questions that matter at
    once: whether a backup file rotted, and whether the backup still matches the lake.

    The backup's copy of the manifest is still read, for one thing only. It says how far
    the last sync got. That answers the question a backup scrub must answer or else cry
    wolf every week: a partition sealed after the last sync is legitimately absent from
    the backup, and must not read as loss.

    Why the copy can say that exactly. The manifest is append-only, so the copy on the
    backup is a prefix of the lake's. Every lake write appends its manifest entry under
    the lake-root lock, and the compaction job's sync holds that same lock, so at the
    moment the copy was taken every file's bytes matched its newest entry at or before
    the copy's last line. The number of lines in the copy is therefore a watermark.
    Resolving the lake's own entries up to that watermark gives exactly what the backup
    should be carrying, and everything the lake manifested past it is ``pending``.

    The watermark is worth nothing unless the copy really is a prefix, so that is checked
    first, and checked over bytes rather than over parsed entries. The reason is exact.
    ``parse_jsonl`` discards the first line it cannot parse and every line after it, by
    the append rule that says only the last line can be torn. Rot on an SSD obeys no such
    rule. One flipped byte in the middle of the copy would discard the whole tail, the
    watermark would collapse to the rot's position, every partition past it would read as
    not copied yet, and a wholly rotted backup would come back clean. Comparing bytes has
    no such hole: a torn tail is still a prefix, and any rot that changes a byte is not.

    The two passes then mirror ``scrub``.

    Forward: every partition inside the watermark must exist on the backup and match the
    sha the lake recorded for it. A slice-1 segment entry superseded by its compacted
    partition inside the same watermark is skipped, the same rule and for the same
    reason, because compaction unlinked the segment before the sync ran.

    Reverse: every file on the backup must be accounted for. A file the lake never
    recorded is an ``orphan``, and one it recorded past the watermark is ``unaccounted``,
    which is the watermark failing its own cross-check. ``SCRUB_EXCLUSIONS`` is reused
    rather than a second list written, so the two scrubs skip the same files and a
    decision about that list is made once. The price is named rather than hidden: that
    list skips ``journal/`` and ``reports/``, so a killed sync's temp file under either
    is not seen. Scanning the journal instead would name every segment in it, because
    segments carry no manifest entry by rule.

    ``BACKUP_EXCLUSIONS`` is deliberately not consulted. Its two patterns name a temp
    file, which is renamed away before any entry is appended and so can never be
    manifested, and the config directory, which sits outside the lake root. So no
    manifested path can match one, and
    ``test_no_pattern_drops_a_file_a_real_lake_holds`` is what holds that.
    """
    target = Path(backup_root)
    try:
        return _backup_scrub(Path(lake_root), target)
    except OSError as exc:
        # The one disk in this system built to fail, on a cable a person can pull. Every
        # other Sunday check runs after this call, so a raise here would cost the run its
        # canary, its coverage assertion and its re-auth reminder, and report a traceback
        # instead of the disk. A read that fails is a named finding instead.
        return BackupScrubResult(target=str(target), unreadable=f"{type(exc).__name__}: {exc}")


def _backup_scrub(root: Path, target: Path) -> BackupScrubResult:
    """The walk itself. Every read in here is on the removable disk or the lake."""
    if not target.is_dir():
        return BackupScrubResult(target=str(target), target_missing=True)
    copy = manifest_path(target)
    if not copy.exists():
        return BackupScrubResult(target=str(target), manifest_missing=True)

    source_bytes = manifest_path(root).read_bytes() if manifest_path(root).exists() else b""
    backup_bytes = copy.read_bytes()
    if not source_bytes.startswith(backup_bytes):
        return BackupScrubResult(
            target=str(target),
            manifest_diverged_at=_first_difference(source_bytes, backup_bytes),
        )

    # A prefix split mid-character decodes with a replacement, and the line it sits in is
    # the torn tail ``parse_jsonl`` discards anyway.
    source_entries = parse_jsonl(source_bytes.decode("utf-8", "replace"))
    backup_entries = parse_jsonl(backup_bytes.decode("utf-8", "replace"))
    if not backup_entries and source_entries:
        # A watermark of zero over a lake that has entries would make every partition
        # pending and the whole scrub a no-op.
        return BackupScrubResult(target=str(target), manifest_missing=True)

    # The watermark, and the two views of the ledger it splits: what the backup should be
    # carrying, and what the lake holds now.
    copied = _latest_by_partition(source_entries[: len(backup_entries)], manifest_path(root))
    latest = _latest_by_partition(source_entries, manifest_path(root))

    missing: list[str] = []
    sha_mismatches: list[str] = []
    for partition, entry in copied.items():
        compacted = _compacted_partition_for_segment(partition)
        if compacted is not None and compacted in copied:
            continue
        path = target / partition
        if not path.exists():
            missing.append(partition)
        elif sha256_file(path) != entry["sha256"]:
            sha_mismatches.append(partition)

    orphans: list[str] = []
    unaccounted: list[str] = []
    for path in sorted(target.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(target).as_posix()
        if _is_excluded(rel, SCRUB_EXCLUSIONS) or rel in copied:
            continue
        (unaccounted if rel in latest else orphans).append(rel)

    return BackupScrubResult(
        target=str(target),
        missing=tuple(sorted(missing)),
        sha_mismatches=tuple(sorted(sha_mismatches)),
        unaccounted=tuple(sorted(unaccounted)),
        orphans=tuple(sorted(orphans)),
        pending=tuple(sorted(set(latest) - set(copied))),
    )
