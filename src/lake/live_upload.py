"""The live upload: each quotes segment goes to the bucket as soon as its manifest entry lands.

ithildin-runtime decides each session before the close, on a VM of its own that reads only
the bucket (l3a0/ithildin-runtime#16). The nightly upload in ``lake.bucket`` sends a
session's quotes at close+15 at the earliest, so without this module the runtime could
never see the minute it decides on. This module sends each quotes segment a few seconds
after the cycle that wrote it records the segment in ``manifest.jsonl`` (marketlake #943).
The design's Backup section carries the rules and their prices. This docstring says how
the code keeps them.

**Where it runs.** On a thread of its own inside the daemon, started by
``daemon.run_loop_from_config`` through :func:`prepare` and ``LiveUpload.start``, and only
on a host whose ``role`` is primary and whose ``backup_target`` is a bucket. A shadow host
and a path target start nothing. The thread is a daemon thread, so a request that hangs
never delays the process's exit. It runs neither inside a capture cycle nor inside a loop
hook, because the loop hands results on in slot order and a PUT in either place would
delay the watchdog, every later minute and the close+5 guard.

**The client is built once, before the loop starts.** ``lake.aws_session`` hides every
``AWS_*`` variable from the whole process while it builds a client, so a build inside the
running daemon would hide them from a capture cycle and from a child spawned meanwhile.
:func:`prepare` therefore builds the client on the calling thread, which is the daemon's
main thread before ``run_loop`` starts any cycle, through ``bucket.connect``. A build that
fails prints one line and starts no thread, and capture runs on without it. The client
takes the nightly upload's settings, a 10-second connect timeout, a 60-second read timeout
and three attempts, so a request that hangs costs this thread a few minutes and costs
capture nothing.

**It never takes the lake-root lock.** It reads ``manifest.jsonl`` read-only, from a byte
offset, and keeps only whole lines, those ending in a newline. A line still being written
waits for the next read. Two hand-run commands cut the manifest back in place: the bucket
resync and the bar-import undo. So each read first checks that the bytes just before the
offset are still the ones the last read ended on, and starts from the beginning again when
they are not. A file cut shorter than the offset fails that check, since the bytes are no
longer there. So does a file cut back and then grown past the old offset between two reads,
which a comparison of lengths alone would miss.

**What it sends.** Each manifested journal segment on the quotes surface, the
``journal/date=D/surface=quotes/ticker=T/seg-<start_ts>-<pid>.arrows`` entries, whatever
their ``source``. A segment with no manifest entry, such as a gap marker's, is never sent.
Neither is a chains segment, which marketlake #974 defers, nor any other partition,
``manifest.jsonl`` included. The bucket's copy of the manifest stays the nightly upload's
alone, so it still names only files the bucket holds. A segment is dropped and counted as
superseded when the quotes partition it compacts into is already manifested, or when its
file is gone, which is what compaction leaves once it has sealed the day.

**One request per segment.** Each goes up as one ``PutObject`` under the sibling prefix
``<prefix>-live/``, so the VM's segments land at ``lake-live/journal/...``. That keeps them
out of every listing of the lake's own prefix. A target with no prefix refuses with one
line, because its sibling would sit inside the lake's own listing. The request carries four
things.

1. ``IfNoneMatch="*"``, so S3 refuses a second write of the key with a 412.
2. ``ChecksumSHA256``, the manifest entry's digest in base64, so S3 refuses bytes that no
   longer match the entry with ``BadDigest``.
3. ``StorageClass="STANDARD"``. Standard-IA bills each object as at least 128 KB, and a
   quotes segment is about 9 KB.
4. User metadata carrying the rest of the entry: ``sha256``, ``rows``, ``fetched_at`` and
   ``source``, each as an ASCII string and each only when the entry has it. So an entry is
   in the bucket exactly when its segment is.

A 412 is followed by one ``HeadObject`` in checksum mode. A stored SHA-256 that matches the
entry counts as already uploaded, which is what a daemon restart meets for every segment it
sent before. One that differs is a permanent refusal for that key, and so is ``BadDigest``,
because a segment is never rewritten. A refusal is recorded and the alarm pages for it.

**When the bucket does not answer.** Any other failure, a 409 from a concurrent
conditional write included, keeps the segment owed. The pass stops there, since the next
segment would meet the same bucket, and the thread waits longer before each retry, 5
seconds doubling to 60. It holds nothing capture uses while it waits. A whole session
without the bucket loses nothing: the minutes stay on disk, compaction seals them, and the
nightly upload sends the sealed partitions as it always has.

**The alarm runs on the loop thread.** A thread that died could not report its own death,
so ``UploadAlarm.check`` rides the daemon's ``on_tick`` and reads ``LiveUpload.state`` each
minute. It pages once through the daemon's ``Publisher`` when the thread has stopped or the
oldest owed segment is more than ten minutes old, and prints one line when the upload
catches up. It pages once more, under its own event, the first time a key is refused.

**The lag command.** ``python -m lake.live_upload lag --date YYYY-MM-DD`` lists the live
prefix for one day's quotes segments and prints, for each segment and in summary, its
``LastModified`` less the ``start_ts`` its key carries. That is how acceptance item 1 of
marketlake #943, a segment in the bucket within 90 seconds of its cycle's start, is measured
on the VM from the bucket alone. It sends one listing request a page and writes nothing.

Nothing here deletes an object or touches a key under the lake's own prefix.
"""

from __future__ import annotations

import argparse
import sys
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from lake import outbox
from lake.alert import Message, Publisher
from lake.bucket import (
    BAD_DIGEST,
    _entries,
    _error_code,
    _failure,
    _matches,
    _status,
    connect,
    hex_to_b64,
)
from lake.calendar import MARKET_TZ
from lake.clock import Clock
from lake.config import BucketTarget, Config, ConfigError, input_errors_exit, load_config
from lake.gap import SEGMENT_STAMP_FORMAT
from lake.journal import QUOTES_SURFACE
from lake.manifest import _compacted_partition_for_segment, manifest_path
from lake.paths import (
    DATE_PREFIX,
    JOURNAL_DIR,
    QUOTES,
    SEGMENT_PREFIX,
    SEGMENT_SUFFIX,
    SURFACE_PREFIX,
    parse_segment_rel,
)

# How often the thread reads the manifest for new entries. A cycle's entry then waits at
# most this long, which leaves most of the 90 seconds acceptance item 1 allows for the
# cycle itself and the PUT.
POLL_SECONDS = 5.0

# The longest wait between two retries while the bucket keeps failing. The wait starts at
# ``POLL_SECONDS`` and doubles, so a bucket that is down gets one request a minute.
BACKOFF_CAP_SECONDS = 60.0

# How old the oldest owed segment may get before the alarm pages.
BEHIND_AFTER = timedelta(minutes=10)

# The lag acceptance item 1 allows between a cycle's start and its segment in the bucket.
LAG_LIMIT = timedelta(seconds=90)

# What the live prefix adds to the lake's own prefix.
LIVE_SUFFIX = "-live"

# Every live PUT's storage class. Standard-IA's 128 KB minimum would bill a 9 KB segment
# fourteen times over.
STORAGE_CLASS = "STANDARD"

# The two events the alarm pages under, as the design's message table names them.
BEHIND_EVENT = "segment_upload_behind"
REFUSED_EVENT = "segment_upload_refused"
BEHIND_TITLE = "Live upload behind"
REFUSED_TITLE = "Live upload refused a segment"

# S3's answers to a conditional write. A 412 means the key already exists. A 409 means
# another conditional write to the same key was in flight, which is worth a retry.
_EXISTS = "PreconditionFailed"
_CONFLICT = "ConditionalRequestConflict"

# The metadata fields a PUT carries, in the order the manifest entry names them.
_METADATA_FIELDS = ("sha256", "rows", "fetched_at", "source")

# How much of the manifest one read takes into memory at a time. The first read after a
# start takes the whole file, every session the lake ever recorded.
_READ_CHUNK = 1 << 20

# How many bytes before the offset each read compares with the last read's, to tell a
# manifest cut back and grown again from one that only grew. One entry is about 250 bytes.
_ANCHOR_BYTES = 256

# The refused segments a page names before it counts the rest.
_NAMED = 4

# Every manifest line that names a quotes segment or a quotes partition contains this, so a
# line without it is skipped before it is parsed. It only saves parsing. Each line that
# passes is still checked exactly.
_QUOTES_MARK = f"{QUOTES}/".encode()

# The seam ``bucket.connect`` fills: the checked target and a client for it.
Connector = Callable[[Config, BucketTarget], tuple[BucketTarget, Any]]


class LivePrefixRefused(ConfigError):
    """The backup target has no prefix, so its sibling would sit inside the lake's listing."""


def live_target(target: BucketTarget) -> BucketTarget:
    """The sibling target the live upload writes to, ``<prefix>-live`` in the same bucket.

    A target with an empty prefix refuses, because the lake's own keys then sit at the
    bucket's root and every listing of the lake would list the live keys too.
    """
    if not target.prefix:
        raise LivePrefixRefused(
            "backup_target names no key prefix, so the live upload's sibling prefix would "
            "sit inside the lake's own listing. Give backup_target a prefix, such as "
            "s3://<bucket>/lake"
        )
    return BucketTarget(bucket=target.bucket, prefix=f"{target.prefix}{LIVE_SUFFIX}")


def segment_start(rel: str) -> datetime | None:
    """The ``start_ts`` a segment's path carries, in UTC, or ``None`` for any other path.

    The name is ``seg-<start_ts>-<pid>.arrows``, and the stamp is the cycle's own start in
    UTC, written with ``gap.SEGMENT_STAMP_FORMAT``, the shape capture writes too.
    """
    ref = parse_segment_rel(rel)
    if ref is None or not ref.filename.startswith(SEGMENT_PREFIX):
        return None
    stem = ref.filename[len(SEGMENT_PREFIX) : -len(SEGMENT_SUFFIX)]
    stamp, _, pid = stem.rpartition("-")
    if not stamp or not pid.isdigit():
        return None
    try:
        return datetime.strptime(stamp, SEGMENT_STAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def retry_delay(failures: int, *, poll: float, cap: float) -> float:
    """How long the thread waits after ``failures`` passes in a row that a request failed.

    No failure waits ``poll``. The first failure waits ``poll`` too, and each one after it
    doubles the wait, up to ``cap``.
    """
    return min(poll * 2 ** max(failures - 1, 0), cap)


def _metadata(entry: Mapping[str, Any]) -> dict[str, str]:
    """The entry's fields as S3 user metadata: ASCII strings, absent fields left out."""
    found: dict[str, str] = {}
    for name in _METADATA_FIELDS:
        value = entry.get(name)
        if value is None or isinstance(value, bool):
            continue
        text = str(value)
        if text.isascii():
            found[name] = text
    return found


def _usable_digest(value: object) -> bool:
    """Whether a manifest ``sha256`` is the 64 hex characters a ``ChecksumSHA256`` needs."""
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def _describe(exc: BaseException) -> str:
    """A failed request as *refused*, *unreachable* or *failed* and a code, never a message."""
    failure = _failure(exc)
    if failure is None:
        return type(exc).__name__
    kind, code = failure
    return f"{kind} ({code})"


@dataclass(frozen=True)
class UploadState:
    """What the loop thread reads of the upload each minute, copied under the lock.

    ``oldest_owed_since`` is when this process first read the oldest segment still owed.
    It counts from the first read rather than from the ``start_ts`` in the key, because a
    restart re-reads every entry the manifest holds. Counted from the key, every segment
    a restart has not yet re-checked would read as hours old, and each restart would page
    until the backlog of 412s was worked through. Counted from the first read, the alarm
    pages only on a backlog this process has failed to clear for ten minutes.
    """

    alive: bool
    owed: int
    oldest_owed_since: datetime | None
    uploaded: int
    already_there: int
    superseded: int
    refused: tuple[str, ...]


@dataclass
class _Owed:
    """One segment the bucket does not hold yet, and when this process first read it."""

    entry: dict[str, Any]
    since: datetime


class LiveUpload:
    """The thread that tails ``manifest.jsonl`` and PUTs each quotes segment it names.

    ``pass_once`` is one read of the manifest and one attempt at every segment owed. The
    thread repeats it every ``poll_seconds``, waiting longer after a pass that failed. A
    test calls ``pass_once`` directly and never starts the thread.
    """

    def __init__(
        self,
        *,
        lake_root: Path | str,
        target: BucketTarget,
        client: Any,
        clock: Clock,
        poll_seconds: float = POLL_SECONDS,
        backoff_cap_seconds: float = BACKOFF_CAP_SECONDS,
    ) -> None:
        self._root = Path(lake_root)
        self._path = manifest_path(self._root)
        self._target = target
        self._client = client
        self._clock = clock
        self._poll = poll_seconds
        self._cap = backoff_cap_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # How far into the manifest the whole lines read so far reach, and the bytes just
        # before that offset, which the next read checks are still there.
        self._offset = 0
        self._anchor = b""
        # The owed segments in the order they were first read, which is manifest order.
        self._owed: dict[str, _Owed] = {}
        # Each compacted quotes partition, mapped to the owed segments it supersedes once
        # it is manifested, so a segment is dropped the moment its partition is read.
        self._waiting: dict[str, list[str]] = {}
        # Every quotes partition the manifest records. A ticker-day each, so it stays small.
        self._sealed: set[str] = set()
        # The segments the bucket holds, with the digest it holds them under, and the ones
        # refused for good. A re-read after the manifest is cut back sends neither again.
        self._done: dict[str, str] = {}
        self._refused: dict[str, str] = {}
        self._uploaded = 0
        self._already_there = 0
        self._superseded = 0

    # -- the thread ----------------------------------------------------------------

    def start(self) -> None:
        """Start the thread. It is a daemon thread, so it never holds the process open."""
        self._thread = threading.Thread(target=self._run, name="live-upload", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Ask the thread to end after its current pass. It waits for nothing."""
        self._stop.set()

    def _run(self) -> None:
        failures = 0
        try:
            while not self._stop.is_set():
                failed = self.pass_once()
                if failed is not None:
                    failures += 1
                    # One line when a run of failures starts, not one per retry, because a
                    # bucket down for a session would otherwise write a line every minute.
                    if failures == 1:
                        print(f"live_upload: {failed}, retrying", file=sys.stderr)
                elif failures:
                    print(
                        f"live_upload: sending again after {failures} failed passes",
                        file=sys.stderr,
                    )
                    failures = 0
                if self._stop.wait(retry_delay(failures, poll=self._poll, cap=self._cap)):
                    return
        except Exception as exc:  # noqa: BLE001 - the alarm reports a thread that stopped
            # The thread ends here, and the alarm on the loop thread pages for it the next
            # minute. The message is printed whole, because this log is the one place that
            # can say why.
            print(f"live_upload: stopped: {type(exc).__name__}: {exc}", file=sys.stderr)

    # -- what the loop thread reads ------------------------------------------------

    def state(self) -> UploadState:
        """A copy of the upload's state, taken under the lock, cheap enough for each tick."""
        with self._lock:
            oldest = next(iter(self._owed.values()), None)
            return UploadState(
                alive=self._thread is not None and self._thread.is_alive(),
                owed=len(self._owed),
                oldest_owed_since=None if oldest is None else oldest.since,
                uploaded=self._uploaded,
                already_there=self._already_there,
                superseded=self._superseded,
                refused=tuple(self._refused),
            )

    # -- one pass ------------------------------------------------------------------

    def pass_once(self) -> str | None:
        """Read the manifest's new lines, then try every owed segment once, oldest first.

        Returns ``None`` when nothing failed, or the one line naming the failure that ended
        the pass early. That failure leaves its segment, and every one after it, owed.
        """
        try:
            self._read_tail()
        except OSError as exc:
            return f"manifest.jsonl could not be read ({type(exc).__name__})"
        with self._lock:
            owed = list(self._owed)
        for rel in owed:
            failed = self._send(rel)
            if failed is not None:
                return failed
        return None

    def _read_tail(self) -> None:
        """Take every whole line appended since the last read, from the start when cut back."""
        try:
            handle = open(self._path, "rb")
        except FileNotFoundError:
            return
        with handle:
            if not self._anchored(handle):
                self._restart()
            handle.seek(self._offset)
            pending = b""
            while chunk := handle.read(_READ_CHUNK):
                data = pending + chunk
                cut = data.rfind(b"\n") + 1
                whole, pending = data[:cut], data[cut:]
                if whole:
                    self._take(whole)
                    self._offset += len(whole)
                    self._anchor = (self._anchor + whole)[-_ANCHOR_BYTES:]

    def _anchored(self, handle: Any) -> bool:
        """Whether the bytes just before the offset are still the ones the last read ended on.

        A file now shorter than the offset reads short here, so it answers no as well.
        """
        if not self._anchor:
            return True
        handle.seek(self._offset - len(self._anchor))
        return handle.read(len(self._anchor)) == self._anchor

    def _restart(self) -> None:
        """Forget the read position and what it implied, keeping what the bucket holds."""
        self._offset = 0
        self._anchor = b""
        with self._lock:
            self._owed.clear()
            self._waiting.clear()
            self._sealed.clear()

    def _take(self, whole: bytes) -> None:
        """Fold whole manifest lines into the owed segments and the sealed partitions."""
        lines = [line for line in whole.split(b"\n") if _QUOTES_MARK in line]
        if not lines:
            return
        # ``bucket._entries`` parses each line on its own and keeps only an object naming a
        # string ``partition``, so a damaged line costs that line alone.
        for entry in _entries(b"\n".join(lines)):
            self._consider(entry)

    def _consider(self, entry: Mapping[str, Any]) -> None:
        rel = entry["partition"]
        if rel.startswith(f"{QUOTES}/"):
            with self._lock:
                self._sealed.add(rel)
                for segment in self._waiting.pop(rel, ()):
                    if self._owed.pop(segment, None) is not None:
                        self._superseded += 1
            return
        ref = parse_segment_rel(rel)
        if ref is None or ref.surface != QUOTES_SURFACE:
            return
        kept = {name: entry.get(name) for name in _METADATA_FIELDS}
        compacted = _compacted_partition_for_segment(rel)
        with self._lock:
            if rel in self._refused or self._done.get(rel) == kept["sha256"]:
                return
            if compacted in self._sealed:
                self._superseded += 1
                return
            # The latest entry for a path is the one that counts, as everywhere in the lake,
            # and the first read of the path is still when it became owed.
            previous = self._owed.get(rel)
            since = previous.since if previous is not None else self._clock.now()
            self._owed[rel] = _Owed(entry=kept, since=since)
            if previous is None and compacted is not None:
                self._waiting.setdefault(compacted, []).append(rel)

    def _send(self, rel: str) -> str | None:
        """One attempt at one owed segment. ``None`` unless the bucket failed to answer."""
        with self._lock:
            owed = self._owed.get(rel)
        if owed is None:
            return None
        digest = owed.entry["sha256"]
        if not _usable_digest(digest):
            self._refuse(rel, "its manifest entry carries no usable sha256")
            return None
        try:
            data = (self._root / rel).read_bytes()
        except FileNotFoundError:
            # Compaction deletes a day's segments once it has sealed the day, so a file gone
            # is a file the nightly upload carries inside a partition.
            self._settle(rel, digest, superseded=True)
            return None
        except OSError as exc:
            return f"{rel} could not be read ({type(exc).__name__})"
        key = self._target.key(rel)
        try:
            self._client.put_object(
                Bucket=self._target.bucket,
                Key=key,
                Body=data,
                ChecksumSHA256=hex_to_b64(digest),
                StorageClass=STORAGE_CLASS,
                IfNoneMatch="*",
                Metadata=_metadata(owed.entry),
            )
        except Exception as exc:  # noqa: BLE001 - every failure keeps the segment owed
            code = _error_code(exc)
            if code == BAD_DIGEST:
                self._refuse(rel, "its bytes no longer match its manifest entry (BadDigest)")
                return None
            if code == _EXISTS or _status(exc) == 412:
                return self._already_sent(rel, key, digest)
            if code == _CONFLICT or _status(exc) == 409:
                return f"{rel}: another write to the same key was in flight ({_CONFLICT})"
            return f"{rel}: {_describe(exc)}"
        self._settle(rel, digest, uploaded=True)
        return None

    def _already_sent(self, rel: str, key: str, digest: str) -> str | None:
        """Settle a key S3 already holds, by the SHA-256 it stored for it."""
        try:
            head = self._client.head_object(
                Bucket=self._target.bucket, Key=key, ChecksumMode="ENABLED"
            )
        except Exception as exc:  # noqa: BLE001 - the segment stays owed and is retried
            return f"{rel}: already in the bucket, and its HEAD was {_describe(exc)}"
        if _matches(head, digest):
            self._settle(rel, digest)
            return None
        self._refuse(rel, "the bucket already holds other bytes under its key")
        return None

    def _settle(
        self, rel: str, digest: str, *, uploaded: bool = False, superseded: bool = False
    ) -> None:
        with self._lock:
            if self._owed.pop(rel, None) is None:
                return
            if superseded:
                self._superseded += 1
                return
            self._done[rel] = digest
            if uploaded:
                self._uploaded += 1
            else:
                self._already_there += 1

    def _refuse(self, rel: str, why: str) -> None:
        with self._lock:
            self._owed.pop(rel, None)
            self._refused[rel] = why
        print(
            f"live_upload: {rel} refused: {why}. A segment is never rewritten, so it stays "
            "out of the live prefix",
            file=sys.stderr,
        )


def prepare(
    config: Config,
    clock: Clock,
    *,
    connect: Connector,
    poll_seconds: float = POLL_SECONDS,
    backoff_cap_seconds: float = BACKOFF_CAP_SECONDS,
) -> LiveUpload | None:
    """The live upload for this host, with its client built, or ``None`` when it runs none.

    Only a primary with a bucket target runs one. A target with no prefix refuses with one
    line, and so does a client that cannot be built. Either way capture runs on without the
    upload. The client is built here, on the caller's thread, so the daemon builds it
    before ``run_loop`` starts any cycle. ``connect`` is ``bucket.connect`` in production
    and a fake in every test. Nothing here starts the thread.
    """
    role, _ = outbox.role_of(config)
    target = config.backup_target
    if role != outbox.PRIMARY or not isinstance(target, BucketTarget):
        return None
    try:
        live = live_target(target)
    except LivePrefixRefused as exc:
        print(f"live_upload: not started: {exc}", file=sys.stderr)
        return None
    try:
        _, client = connect(config, target)
    except ConfigError as exc:
        # ``bucket.connect`` words every refusal as one operator line naming the setting.
        print(f"live_upload: not started: {exc}", file=sys.stderr)
        return None
    except Exception as exc:  # noqa: BLE001 - capture outranks the live upload
        print(
            f"live_upload: not started: the client build raised {type(exc).__name__}",
            file=sys.stderr,
        )
        return None
    return LiveUpload(
        lake_root=config.lake_root,
        target=live,
        client=client,
        clock=clock,
        poll_seconds=poll_seconds,
        backoff_cap_seconds=backoff_cap_seconds,
    )


# -- the alarm -----------------------------------------------------------------------


def _behind_body(state: UploadState, now: datetime) -> str | None:
    """The page body when the upload is behind, or ``None`` while it keeps up."""
    if not state.alive:
        return (
            "The live upload thread stopped, and the daemon log's live_upload line names "
            "why. Capture is unaffected, and the nightly upload still sends the sealed day. "
            "A daemon restart starts the thread again."
        )
    since = state.oldest_owed_since
    if since is None or now - since <= BEHIND_AFTER:
        return None
    minutes = int((now - since).total_seconds() // 60)
    return (
        f"{state.owed} quotes segments owed, the oldest first read at "
        f"{since.astimezone(MARKET_TZ).strftime('%H:%M')} ET, {minutes} minutes ago. "
        "Capture is unaffected, and the nightly upload still sends the sealed day. The "
        "daemon log's live_upload lines name the failure."
    )


def _refused_body(refused: Sequence[str]) -> str:
    names = ", ".join(refused[:_NAMED])
    rest = len(refused) - _NAMED
    if rest > 0:
        names = f"{names} and {rest} more"
    count = "1 quotes segment" if len(refused) == 1 else f"{len(refused)} quotes segments"
    return (
        f"{count} refused, because the bucket holds other bytes under the key or the bytes "
        f"no longer match the manifest entry: {names}. A segment is never rewritten, so "
        "each stays out of the live prefix. The daemon log names why for each."
    )


class UploadAlarm:
    """Pages for the live upload from the loop thread, once per transition.

    Two transitions page.

    1. The upload falls behind, meaning the thread has stopped or the oldest owed segment
       is more than ten minutes old. It pages once, and prints one line when the upload
       catches up, which re-arms it. A stopped thread never catches up, so it pages once
       for the life of the process.
    2. The first key the upload refuses. A refusal is permanent, so later ones are only
       printed by the thread, and the page counts the refused keys it knew of.

    A recovery is a line rather than a page, because the design cuts a daemon-side
    recovery message from the message table.
    """

    def __init__(self, upload: LiveUpload, publisher: Publisher, clock: Clock) -> None:
        self._upload = upload
        self._publisher = publisher
        self._clock = clock
        self._behind = False
        self._refusal_paged = False

    def check(self, slot: datetime) -> None:
        state = self._upload.state()
        body = _behind_body(state, self._clock.now())
        if body is not None and not self._behind:
            self._behind = True
            self._page(Message(event=BEHIND_EVENT, title=BEHIND_TITLE, body=body), slot)
        elif body is None and self._behind:
            self._behind = False
            print(
                f"live_upload: {slot.isoformat()}: caught up, {state.owed} segments owed",
                file=sys.stderr,
            )
        if state.refused and not self._refusal_paged:
            self._refusal_paged = True
            self._page(
                Message(
                    event=REFUSED_EVENT, title=REFUSED_TITLE, body=_refused_body(state.refused)
                ),
                slot,
            )

    def _page(self, message: Message, slot: datetime) -> None:
        delivery = self._publisher.publish(message, now=slot)
        if not delivery.sent:
            print(f"live_upload: {message.event} page not sent: {delivery.reason}", file=sys.stderr)


# -- the lag command -----------------------------------------------------------------


@dataclass(frozen=True)
class SegmentLag:
    """One live segment: its lake-relative path and its ``LastModified`` less its start."""

    rel: str
    lag: timedelta


def segment_lags(listing: Iterable[Mapping[str, Any]], target: BucketTarget) -> list[SegmentLag]:
    """The lag of every segment key in a listing of ``target``, in key order.

    A key outside the target, or one whose name carries no ``start_ts``, is left out.
    """
    found: list[SegmentLag] = []
    for item in listing:
        rel = target.rel(item["Key"])
        start = None if rel is None else segment_start(rel)
        if rel is None or start is None:
            continue
        found.append(SegmentLag(rel=rel, lag=item["LastModified"] - start))
    return sorted(found, key=lambda one: one.rel)


@dataclass(frozen=True)
class LagSummary:
    """A day's lags folded: how many, the largest, and how many exceed ``LAG_LIMIT``."""

    count: int
    largest: timedelta | None
    over: int

    @classmethod
    def of(cls, lags: Sequence[SegmentLag]) -> LagSummary:
        return cls(
            count=len(lags),
            largest=max((one.lag for one in lags), default=None),
            over=sum(1 for one in lags if one.lag > LAG_LIMIT),
        )


def _seconds(span: timedelta) -> str:
    return f"{span.total_seconds():.1f} s"


def day_prefix(day: date) -> str:
    """The lake-relative prefix of one day's quotes segments."""
    return f"{JOURNAL_DIR}/{DATE_PREFIX}{day.isoformat()}/{SURFACE_PREFIX}{QUOTES_SURFACE}/"


def list_live(client: Any, target: BucketTarget, day: date) -> list[dict[str, Any]]:
    """Every object under one day's quotes segments in the live prefix, every page of it."""
    found: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": target.bucket, "Prefix": target.key(day_prefix(day))}
        if token is not None:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        found.extend(page.get("Contents", ()))
        if not page.get("IsTruncated"):
            return found
        token = page["NextContinuationToken"]


def lag_lines(lags: Sequence[SegmentLag], day: date, target: BucketTarget) -> list[str]:
    """What the lag command prints: one line per segment, then the summary line."""
    summary = LagSummary.of(lags)
    if summary.largest is None:
        return [f"lag: no quotes segments on {day.isoformat()}: {target}"]
    lines = [f"{one.rel} {_seconds(one.lag)}" for one in lags]
    lines.append(
        f"lag: {summary.count} quotes segments on {day.isoformat()}, largest "
        f"{_seconds(summary.largest)}, {summary.over} over {_seconds(LAG_LIMIT)}: {target}"
    )
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lake.live_upload",
        description="Read what the daemon's live upload sent to the bucket. Writes nothing.",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    lag = commands.add_parser(
        "lag",
        help="Each live quotes segment's LastModified less the start_ts its key carries.",
        allow_abbrev=False,
    )
    lag.add_argument("--date", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    lag.add_argument("--config", help="Path to config.yaml (defaults to the standard location).")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """The ``python -m lake.live_upload`` entry. Exit 0 read, 1 bucket failure, 2 bad input."""
    args = build_parser().parse_args(argv)
    with input_errors_exit("live_upload"):
        config = load_config(args.config)
        if not isinstance(config.backup_target, BucketTarget):
            raise ConfigError("backup_target is not an s3:// bucket")
        live = live_target(config.backup_target)
        _, client = connect(config, config.backup_target)
    try:
        listing = list_live(client, live, args.date)
    except Exception as exc:
        failure = _failure(exc)
        if failure is None:
            raise
        kind, code = failure
        print(f"live_upload: lag: the listing was {kind} ({code}): {live}", file=sys.stderr)
        return 1
    for line in lag_lines(segment_lags(listing, live), args.date, live):
        print(line)
    return 0


__all__ = [
    "BACKOFF_CAP_SECONDS",
    "BEHIND_AFTER",
    "BEHIND_EVENT",
    "LAG_LIMIT",
    "LIVE_SUFFIX",
    "POLL_SECONDS",
    "REFUSED_EVENT",
    "STORAGE_CLASS",
    "Connector",
    "LagSummary",
    "LiveUpload",
    "LivePrefixRefused",
    "SegmentLag",
    "UploadAlarm",
    "UploadState",
    "build_parser",
    "day_prefix",
    "lag_lines",
    "list_live",
    "live_target",
    "main",
    "prepare",
    "retry_delay",
    "segment_lags",
    "segment_start",
]


if __name__ == "__main__":
    raise SystemExit(main())
