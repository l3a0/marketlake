"""The bucket backup: the nightly upload, the first upload, and the Sunday bucket scrub.

``backup_target`` takes a bucket URL as well as a path, and this module is what a bucket
target runs. The path form, ``rsync`` to a mounted directory, is unchanged and stays the
default, so switching back is one setting. The design's Backup section carries the
reasoning for a bucket at all, and marketlake #630 carries the provider choice: S3
Standard-IA, versioning on, no Object Lock, and a narrow key.

Three jobs live here.

1. **The nightly upload**, ``BucketBackup``, runs where ``RsyncBackup`` runs, inside the
   close+15 compaction's lake-root lock. It uploads what changed since the last night
   and never deletes.
2. **The first upload**, ``python -m lake.bucket first-upload``, is run by hand. It
   compares every object rather than trusting the bucket's copy of the manifest, so it
   both seeds an empty bucket and re-baselines one whose copy stopped being a prefix.
3. **The bucket scrub**, ``bucket_scrub``, is the Sunday job's check of the bucket. It
   returns the same ``BackupScrubResult`` the path scrub returns, with the same
   findings and the same rule about which of them withhold the ping.

**The manifest's digest travels with every upload.** ``manifest.jsonl`` records each
file's SHA-256 as 64 hex characters. S3 takes a SHA-256 as ``ChecksumSHA256``, the
base64 of the 32 raw digest bytes, so the uploader converts hex to base64 and the scrub
converts back. S3 refuses a PUT whose bytes do not hash to the value sent, with the
error code ``BadDigest``. So a sealed file that changed or rotted after the manifest
recorded it fails its upload rather than landing. Every PUT is one ``PutObject``. A
multipart upload stores a checksum of part checksums, marked ``COMPOSITE``, which cannot
be compared with the manifest, so a file over S3's 5 GiB single-PUT limit fails loudly
rather than falling back to parts.

**The watermark.** The bucket's copy of ``manifest.jsonl`` says how far the last upload
got. A ``HeadObject`` returns its length and its stored SHA-256. Hashing that many
leading bytes of the lake's manifest and comparing proves the copy is a prefix without
downloading it, the same byte-prefix rule ``manifest.backup_scrub`` applies to a path
copy. The number of entries in that prefix is the watermark.

**What stays on the machine.** ``runner.BACKUP_EXCLUSIONS`` decides it, with ``rsync``'s
own matching rules, because the uploader walks the tree itself and ``rsync`` is not
there to apply them. Nothing is ever written under the lake root, which is what keeps
switching back to a path free.

**The client is built from ``config.yaml`` alone.** ``client_from_config`` reads the
access key, secret key and region from the config and clears every ``AWS_*`` variable,
``~/.aws/config``, ``~/.aws/credentials`` and ``~/.aws/models`` out of the client's
reach while it builds. A development run therefore cannot reach a real bucket on
credentials it happened to find on the machine. ``boto3`` is imported there, lazily, so
the offline suite never loads it unless a test builds a client. Every job reaches the
client through ``connect``, which first runs the config's strict bucket checks. Loading
the config runs none of them, because capture loads it every minute and a bad backup
setting must fail the backup and nothing else.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import fnmatch
import hashlib
import os
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock
from lake.config import (
    BUCKET_KEYS,
    BucketTarget,
    Config,
    ConfigError,
    input_errors_exit,
    load_config,
    parse_backup_target,
    require_bucket_settings,
)
from lake.control_plane import (
    COMPACTION_RUN,
    SUNDAY_ASSERTION_END,
    SUNDAY_WAKE,
    VENDOR_SWEEP,
    WallClockTime,
)
from lake.lock import lake_lock
from lake.manifest import (
    SCRUB_EXCLUSIONS,
    BackupScrubResult,
    _compacted_partition_for_segment,
    _first_difference,
    _is_excluded,
    _latest_by_partition,
    manifest_path,
    parse_jsonl,
)
from lake.paths import MANIFEST_FILE
from lake.runner import BACKUP_EXCLUSIONS
from lake.session import SessionClock

# The storage class every PUT sets. Standard-IA bills each version for at least 30 days
# and each object at no less than 128 KB, which marketlake #630 measured and accepted.
STORAGE_CLASS = "STANDARD_IA"

# S3's cap on one ``PutObject``: 5 GiB. Past it the only way in is a multipart upload,
# whose stored SHA-256 is a checksum of part checksums and cannot match the manifest.
MAX_PUT_BYTES = 5 * 1024**3

# The checksum type a single ``PutObject`` stores. A multipart upload stores
# ``COMPOSITE``, whose value carries a ``-N`` part-count suffix.
FULL_OBJECT = "FULL_OBJECT"

# The command that seeds or re-baselines a bucket, named by every refusal that needs it.
FIRST_UPLOAD_COMMAND = "python -m lake.bucket first-upload"

# The error code S3 answers a PUT whose bytes do not match its ``ChecksumSHA256``.
BAD_DIGEST = "BadDigest"

# What a ``HeadObject`` on a missing key answers. The narrow key holds ``s3:ListBucket``,
# so S3 answers 404 rather than 403, and a HEAD error carries no body to name a code in.
_ABSENT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})

# -- the nightly upload's deadline ---------------------------------------------
#
# Capture's per-cycle manifest append takes the same lake-root lock and blocks until it
# gets it, and the 18:30 vendor sweep takes it too. An upload that stalled while holding
# the lock could stop capture in the next session, and a lost minute cannot be
# recovered. So the upload stops at a deadline, raises, and the ``with`` block it runs in
# releases the lock. The design's Backup section states the rule, and marketlake #639's
# item 8 is the plan.
#
# The deadline is a budget counted from the moment the upload starts, derived from the
# schedule the code already pins rather than picked.
#
# 1. Compaction starts at close+15. ``COMPACTION_RUN`` is 16:30, the latest start on a
#    regular day, and an early close starts it earlier, which only widens the room.
# 2. The vendor sweep is ``VENDOR_SWEEP``, 18:30. That is 120 minutes after 16:30.
# 3. The seal runs before the upload, inside the same lock. ``SEAL_ALLOWANCE`` gives it
#    15 of those minutes.
# 4. The deadline is checked between PUTs, so the PUT in flight when it passes still
#    finishes. The largest file on 2026-10-05 was 318 MB, which takes about 14 minutes at
#    the 3.1 Mbit/s the nightly delta needs. ``IN_FLIGHT_ALLOWANCE`` reserves 15.
# 5. ``SWEEP_MARGIN`` leaves the sweep 15 minutes of slack on top.
#
# That leaves 75 minutes. The healthchecks ``compaction`` check is not what sets it. The
# check expects its ping near 17:00, so an upload running past about 25 minutes already
# makes the ping late, which pages and then recovers when the ping lands. Stopping at
# 17:00 would page the same way and leave the night's files off the bucket, so the
# budget runs to the sweep instead.
#
# **The session bounds it too.** A compaction run by hand, the catch-up ``compact.main``
# describes, can start at any hour. One started during a session would hold the lock
# through the seal and then up to 75 minutes of upload while capture waits to append. So
# the deadline is the earlier of two times.
#
# 1. The start plus ``NIGHTLY_UPLOAD_BUDGET``.
# 2. The next session's capture start, which is the session open, less
#    ``IN_FLIGHT_ALLOWANCE`` for the PUT in flight and ``PRE_OPEN_MARGIN`` on top. A
#    session counts as running until its close+5, the last moment an option-close fill
#    may still append, so a run started before then reads that session's open, which has
#    already passed.
#
# An upload started inside a session therefore finds its deadline already passed and
# raises before it sends any request. ``session_bound`` derives the time from the
# calendar through ``SessionClock``, the helper the daemon decides capture with. The
# first upload's locked phase takes the same bound, and its unlocked phase needs none.
SEAL_ALLOWANCE = timedelta(minutes=15)
IN_FLIGHT_ALLOWANCE = timedelta(minutes=15)
SWEEP_MARGIN = timedelta(minutes=15)
PRE_OPEN_MARGIN = timedelta(minutes=15)

# How many days ahead ``session_bound`` looks for the next session. A long weekend with a
# holiday spans four days without one, so eight is ample, and an upload's 75 minutes
# could never reach a session further off.
_SESSION_LOOKAHEAD_DAYS = 8


def _clock_offset(moment: WallClockTime) -> timedelta:
    return timedelta(hours=moment.hour, minutes=moment.minute)


NIGHTLY_UPLOAD_BUDGET = (
    _clock_offset(VENDOR_SWEEP)
    - _clock_offset(COMPACTION_RUN)
    - SEAL_ALLOWANCE
    - IN_FLIGHT_ALLOWANCE
    - SWEEP_MARGIN
)

# Python's ``date.weekday()`` number for Sunday.
_PY_SUNDAY = 6


def session_bound(now: datetime, *, clock: Clock, calendar: Calendar) -> datetime | None:
    """The last moment an upload holding the lock may send a request, from the calendar.

    It is the open of the session that ``now`` falls before or inside, less
    ``IN_FLIGHT_ALLOWANCE`` and ``PRE_OPEN_MARGIN``. A session runs until its close+5
    here, so a ``now`` inside one gives a time already past. ``None`` means the calendar
    names no session in the next ``_SESSION_LOOKAHEAD_DAYS`` days.
    """
    sessions = SessionClock(clock, calendar)
    today = now.astimezone(MARKET_TZ).date()
    for offset in range(_SESSION_LOOKAHEAD_DAYS):
        day = today + timedelta(days=offset)
        if not calendar.is_session(day):
            continue
        bounds = sessions.bounds(day)
        if now < bounds.option_close_deadline:
            return bounds.open - IN_FLIGHT_ALLOWANCE - PRE_OPEN_MARGIN
    return None


# -- refusals -----------------------------------------------------------------


class BucketRefusal(Exception):
    """A bucket job refusing to go on, with a message that is one line for an operator."""


class WatermarkMissing(BucketRefusal):
    """The bucket holds no usable copy of ``manifest.jsonl``, so the nightly run cannot start.

    An empty bucket would otherwise start a whole-lake upload inside compaction's lock,
    and a copy that is not a prefix means a human repaired the manifest or restored the
    lake, since the lake's own code only ever appends to it. The first-upload command is
    the repair for both.
    """


class ManifestedFileMissing(BucketRefusal):
    """A file the manifest records is gone from disk, so the watermark cannot claim it."""


class ObjectTooLarge(BucketRefusal):
    """A file is past S3's single-PUT limit and would need a multipart upload."""


class ChecksumRefused(BucketRefusal):
    """S3 refused a PUT because the bytes did not match the manifest's SHA-256."""


class UploadDeadline(BucketRefusal):
    """The nightly upload reached its deadline and stopped before ``manifest.jsonl``."""


class FirstUploadRefused(BucketRefusal):
    """The first-upload command refused to start."""


class BucketUnreachable(BucketRefusal):
    """The bucket refused a request or could not be reached, as one operator line."""


class BucketSettingsInvalid(BucketRefusal, ConfigError):
    """The config's bucket settings cannot build a client, as one operator line.

    It is a ``ConfigError`` because the repair is an edit to ``config.yaml``, and a
    ``BucketRefusal`` because it is raised when a bucket job runs rather than when the
    config loads, so the job that catches a bucket refusal catches this one too.
    """


# -- checksums ----------------------------------------------------------------


def hex_to_b64(hexdigest: str) -> str:
    """The manifest's hex SHA-256 as S3's ``ChecksumSHA256``: base64 of the raw bytes."""
    return base64.b64encode(bytes.fromhex(hexdigest)).decode("ascii")


def b64_sha256(data: bytes) -> str:
    """The ``ChecksumSHA256`` of bytes in hand."""
    return base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")


def stored_sha256(head: Mapping[str, Any]) -> bytes | None:
    """The 32 raw digest bytes a ``HeadObject`` reports, or ``None`` when it proves nothing.

    Three shapes prove nothing and all read as ``None``, so no caller can mistake them
    for a match.

    1. No ``ChecksumSHA256`` at all. S3 returns one only when a checksum was uploaded
       with the object, so an object put there by a tool that sent none is unverified.
    2. A ``ChecksumType`` other than ``FULL_OBJECT``. A multipart upload stores a
       ``COMPOSITE`` checksum of its parts, which is what ``aws s3 cp`` or
       ``upload_file`` leaves when it rewrites an object.
    3. A value that does not base64-decode to exactly 32 bytes, which a composite's
       ``-N`` suffix also fails.

    An absent ``ChecksumType`` is not refused on its own, because the value's own shape
    already separates a composite from a whole-object digest. The live check prints the
    type S3 returns for a single PUT.
    """
    value = head.get("ChecksumSHA256")
    if not value:
        return None
    kind = head.get("ChecksumType")
    if kind is not None and kind != FULL_OBJECT:
        return None
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None
    return raw if len(raw) == 32 else None


def _matches(head: Mapping[str, Any], hexdigest: str) -> bool:
    """Whether a ``HeadObject`` proves the object holds bytes with this hex SHA-256."""
    stored = stored_sha256(head)
    if stored is None:
        return False
    try:
        return stored == bytes.fromhex(hexdigest)
    except (TypeError, ValueError):
        return False


# -- client errors ------------------------------------------------------------


def _error_code(exc: BaseException) -> str | None:
    """The S3 error code a ``ClientError`` carries, or ``None`` for anything else."""
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return None
    code = response.get("Error", {}).get("Code")
    return None if code is None else str(code)


def _is_client_error(exc: BaseException) -> bool:
    from botocore.exceptions import ClientError  # lazy: only a bucket job needs it

    return isinstance(exc, ClientError)


def _is_absent(exc: BaseException) -> bool:
    """Whether a raised error is S3 saying the key does not exist."""
    return _is_client_error(exc) and _error_code(exc) in _ABSENT_CODES


def _failure(exc: BaseException) -> tuple[str, str] | None:
    """Sort a bucket failure into refused or unreachable, or ``None`` for a bug.

    *Refused* means S3 answered and said no: a ``ClientError``, which is a key that was
    revoked, a policy that lost an action, or a bucket that is gone. That repair is a new
    key or a fixed policy. *Unreachable* means no answer came back at all: a failed
    connection, a timeout, or a socket error. That repair is usually nothing, because the
    network comes back. The two are named apart for that reason.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    if isinstance(exc, ClientError):
        return "refused", _error_code(exc) or type(exc).__name__
    if isinstance(exc, BotoCoreError):
        return "unreachable", type(exc).__name__
    return None


# -- the client ---------------------------------------------------------------

_ENVIRONMENT_LOCK = threading.Lock()


@contextmanager
def _aws_environment_cleared() -> Iterator[None]:
    """Hide every ``AWS_*`` variable and both ``~/.aws`` files while a client is built.

    ``botocore`` reads its settings from the environment and from ``~/.aws/config`` as
    well as from what it is handed. An ``AWS_ENDPOINT_URL`` would send requests signed
    with the bucket's key to another host, an ``AWS_PROFILE`` naming no profile would
    refuse to build the client, and an ``AWS_REGION`` would sign for the wrong region. So
    for the length of the build the variables are removed, the two files are pointed at
    the null device, and the instance-metadata lookup is turned off. Everything is put
    back afterwards, so the process's environment is the same on the way out.
    """
    with _ENVIRONMENT_LOCK:
        saved = {key: value for key, value in os.environ.items() if key.startswith("AWS_")}
        for key in saved:
            del os.environ[key]
        os.environ["AWS_CONFIG_FILE"] = os.devnull
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.devnull
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        try:
            yield
        finally:
            for key in [key for key in os.environ if key.startswith("AWS_")]:
                del os.environ[key]
            os.environ.update(saved)


def client_from_config(config: Config) -> Any:
    """An S3 client built from the config's three bucket values and nothing else.

    The access key and secret key are passed explicitly, the region decides the endpoint,
    and :func:`_aws_environment_cleared` keeps the environment and ``~/.aws`` out of the
    build. The service models load from the installed ``botocore`` only, never from
    ``~/.aws/models``. The timeouts bound a stalled socket. They do not bound a slow PUT,
    which the nightly deadline handles between PUTs.
    """
    absent = [
        key
        for key, value in zip(
            BUCKET_KEYS,
            (config.bucket_access_key_id, config.bucket_secret_access_key, config.bucket_region),
            strict=True,
        )
        if value is None
    ]
    if absent:
        raise ConfigError(f"the bucket needs config key(s): {absent}")
    assert config.bucket_access_key_id is not None
    assert config.bucket_secret_access_key is not None
    from botocore.exceptions import BotoCoreError  # lazy: only a bucket job needs it

    try:
        return _build_client(config)
    except (BotoCoreError, ValueError) as exc:
        # ``botocore`` refuses a malformed region with an error that is both of these.
        # Only the type is named, because a message may quote a value from the config.
        raise ConfigError(
            f"the bucket client could not be built from config.yaml ({type(exc).__name__})"
        ) from None


def _build_client(config: Config) -> Any:
    assert config.bucket_access_key_id is not None
    assert config.bucket_secret_access_key is not None
    with _aws_environment_cleared():
        import boto3  # lazy: only a bucket job builds a client
        import botocore.loaders
        import botocore.session
        from botocore.config import Config as BotoConfig

        core = botocore.session.Session()
        core.register_component(
            "data_loader",
            botocore.loaders.Loader(
                extra_search_paths=[botocore.loaders.Loader.BUILTIN_DATA_PATH],
                include_default_search_paths=False,
            ),
        )
        session = boto3.session.Session(botocore_session=core)
        return session.client(
            "s3",
            region_name=config.bucket_region,
            aws_access_key_id=config.bucket_access_key_id.reveal(),
            aws_secret_access_key=config.bucket_secret_access_key.reveal(),
            config=BotoConfig(
                connect_timeout=10,
                read_timeout=60,
                retries={"mode": "standard", "max_attempts": 3},
                # The SHA-256 is supplied on every PUT, which stops botocore adding a
                # CRC32 of its own and the aws-chunked trailer that carries one.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )


def connect(config: Config, target: BucketTarget | None = None) -> tuple[BucketTarget, Any]:
    """The checked bucket target and a client for it, or ``BucketSettingsInvalid``.

    ``target`` defaults to ``backup_target``. The config's strict bucket checks run
    first, then the client is built. Either failing is one operator line naming what
    to fix in ``config.yaml``.
    """
    try:
        checked = require_bucket_settings(config, target)
        return checked, client_from_config(config)
    except BucketSettingsInvalid:
        raise
    except ConfigError as exc:
        raise BucketSettingsInvalid(str(exc)) from None


class ClientFromConfig:
    """An S3 client that ``connect`` builds from the config on its first use.

    Compaction seals the day before it uploads. Building the client when the job starts
    would let a bad bucket setting stop the seal too. Building it at the first request
    means the setting fails the backup after the seal, the way an unmounted disk fails
    the path form. An upload the session deadline stops before any request never builds
    one at all.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client: Any = None

    def __getattr__(self, name: str) -> Any:
        if self._client is None:
            _, self._client = connect(self._config)
        return getattr(self._client, name)


# -- the exclusion list, with rsync's matching rules ----------------------------


def rsync_excluded(rel: str, *, is_dir: bool, patterns: Sequence[str] = BACKUP_EXCLUSIONS) -> bool:
    """Whether ``rsync --exclude`` would drop the lake-relative path ``rel``.

    This is a second implementation of a rule ``rsync`` already applies to the path
    form, so it implements exactly the shapes ``BACKUP_EXCLUSIONS`` uses and refuses any
    other rather than guessing.

    1. A pattern holding no "/" is matched against the last path component, so it drops
       that name anywhere in the tree. An excluded directory is never walked into, which
       is how a name matched on a directory drops everything under it.
    2. A pattern holding a "/" is matched against the end of the path, one component per
       component, so its "*" never crosses a "/".
    3. A trailing "/" narrows either shape to directories only.

    A leading "/" anchors a pattern to the transfer root and "**" crosses directories.
    Neither is modelled, so both raise ``ValueError`` rather than match nothing.
    """
    components = rel.split("/")
    for pattern in patterns:
        dirs_only = pattern.endswith("/")
        body = pattern.rstrip("/")
        if pattern.startswith("/") or "**" in pattern or not body:
            raise ValueError(f"exclusion shape the uploader does not model: {pattern!r}")
        if dirs_only and not is_dir:
            continue
        wanted = body.split("/")
        if len(wanted) > len(components):
            continue
        tail = components[len(components) - len(wanted) :]
        if all(fnmatch.fnmatchcase(part, want) for part, want in zip(tail, wanted, strict=True)):
            return True
    return False


def walk_lake(
    root: Path, patterns: Sequence[str] = BACKUP_EXCLUSIONS
) -> Iterator[tuple[str, Path]]:
    """Every file under ``root`` the backup would copy, as ``(rel, path)`` in sorted order.

    Excluded directories are pruned before they are walked, the way ``rsync`` never
    descends into one, and excluded files are skipped.
    """
    root = Path(root)
    for current, dirs, files in os.walk(root):
        here = Path(current)
        base = here.relative_to(root).as_posix()
        prefix = "" if base == "." else f"{base}/"
        dirs[:] = sorted(
            name
            for name in dirs
            if not rsync_excluded(f"{prefix}{name}", is_dir=True, patterns=patterns)
        )
        for name in sorted(files):
            rel = f"{prefix}{name}"
            if not rsync_excluded(rel, is_dir=False, patterns=patterns):
                yield rel, here / name


# -- the lake's ledger and the bucket's copy of it -----------------------------


@dataclass(frozen=True)
class Ledger:
    """The lake's ``manifest.jsonl``, read once: its bytes and what they resolve to.

    ``last`` holds the position of each partition's latest entry, which is what decides
    whether that entry sits past the watermark.
    """

    raw: bytes
    entries: tuple[dict, ...]
    latest: Mapping[str, dict]
    last: Mapping[str, int]


def read_ledger(lake_root: Path) -> Ledger:
    """Read the lake's manifest bytes and resolve them.

    The bytes decode with a replacement, the way ``manifest.backup_scrub`` decodes them,
    and for its reason: a damaged byte sits in a line ``parse_jsonl`` stops at, and an
    upload or a scrub that raised on the damage would stop reporting it.
    """
    path = manifest_path(lake_root)
    raw = path.read_bytes() if path.exists() else b""
    entries = tuple(parse_jsonl(raw.decode("utf-8", "replace")))
    latest = _latest_by_partition(entries, path)
    last = {entry["partition"]: index for index, entry in enumerate(entries)}
    return Ledger(raw=raw, entries=entries, latest=latest, last=last)


@dataclass(frozen=True)
class CopyState:
    """What the bucket's ``manifest.jsonl`` says about how far the last upload got.

    ``present`` is false when the bucket holds no copy. ``is_prefix`` is true only when
    S3's stored SHA-256 of the copy equals the SHA-256 of that many leading bytes of the
    lake's manifest. ``length`` is the copy's size in bytes.
    """

    present: bool
    length: int = 0
    is_prefix: bool = False


def read_copy_state(client: Any, target: BucketTarget, lake_bytes: bytes) -> CopyState:
    """Prove whether the bucket's manifest copy is a prefix of the lake's, without a download.

    One ``HeadObject`` with checksum mode returns the copy's length and stored SHA-256.
    Hashing that many leading bytes of the lake's manifest and comparing is the whole
    proof. A copy the lake is shorter than cannot be a prefix, and a stored value that
    proves nothing reads as not a prefix rather than as one.
    """
    try:
        head = client.head_object(
            Bucket=target.bucket, Key=target.key(MANIFEST_FILE), ChecksumMode="ENABLED"
        )
    except Exception as exc:
        if _is_absent(exc):
            return CopyState(present=False)
        raise
    length = int(head.get("ContentLength", 0))
    stored = stored_sha256(head)
    is_prefix = (
        stored is not None
        and length <= len(lake_bytes)
        and hashlib.sha256(lake_bytes[:length]).digest() == stored
    )
    return CopyState(present=True, length=length, is_prefix=is_prefix)


def watermark(lake_bytes: bytes, length: int) -> int:
    """How many manifest entries the bucket's copy of ``length`` bytes carries."""
    return len(parse_jsonl(lake_bytes[:length].decode("utf-8", "replace")))


def list_bucket(client: Any, target: BucketTarget) -> dict[str, int]:
    """Every object under the target, as lake-relative path to size, from one listing.

    A listing returns sizes and no checksum values, so a size is all it can compare.
    """
    found: dict[str, int] = {}
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": target.bucket, "Prefix": target.list_prefix}
        if token is not None:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents", ()):
            rel = target.rel(item["Key"])
            if rel:
                found[rel] = int(item["Size"])
        if not page.get("IsTruncated"):
            return found
        token = page["NextContinuationToken"]


# -- uploading ----------------------------------------------------------------


@dataclass
class UploadSummary:
    """What one upload did, for the line a job prints."""

    target: str
    puts: int = 0
    put_bytes: int = 0
    skipped: int = 0
    seconds: float = 0.0
    rebaselined: bool = False
    uploaded: list[str] = field(default_factory=list)

    def render(self) -> str:
        """One line naming what went up and how fast."""
        megabytes = self.put_bytes / 1_000_000
        rate = (self.put_bytes * 8 / 1_000_000 / self.seconds) if self.seconds > 0 else 0.0
        return (
            f"uploaded {self.puts} file(s), {megabytes:.1f} MB in {self.seconds:.0f} s "
            f"({rate:.1f} Mbit/s), skipped {self.skipped} already in the bucket: {self.target}"
        )


class _Uploader:
    """The PUT and the compare-before-PUT, shared by the nightly and the first upload.

    ``guards`` run before every request, and each raises when the upload must stop. The
    nightly upload's deadline is one, and the first upload's Sunday window is another.
    """

    def __init__(
        self,
        *,
        client: Any,
        target: BucketTarget,
        root: Path,
        clock: Clock,
        summary: UploadSummary,
    ) -> None:
        self.client = client
        self.target = target
        self.root = root
        self.clock = clock
        self.summary = summary
        self.guards: list[Callable[[], None]] = []

    def check(self) -> None:
        """Run every guard. Called before every request the upload sends."""
        for guard in self.guards:
            guard()

    def deadline(self, deadline: datetime, why: str, then: str) -> Callable[[], None]:
        """A guard that raises ``UploadDeadline`` once the clock reaches ``deadline``."""

        def guard() -> None:
            if self.clock.now() >= deadline:
                raise UploadDeadline(
                    f"the bucket upload reached its deadline at {deadline.isoformat()}, "
                    f"{why}, after {self.summary.puts} PUT(s) and before manifest.jsonl, so "
                    f"the lock is released and {then}: {self.target}"
                )

        return guard

    def put(self, rel: str, data: bytes, checksum: str) -> None:
        """One ``PutObject`` carrying ``checksum``, or a named refusal."""
        self.check()
        try:
            self.client.put_object(
                Bucket=self.target.bucket,
                Key=self.target.key(rel),
                Body=data,
                ChecksumSHA256=checksum,
                StorageClass=STORAGE_CLASS,
            )
        except Exception as exc:
            if _error_code(exc) == BAD_DIGEST:
                raise ChecksumRefused(
                    f"S3 refused {rel}: its bytes no longer match the SHA-256 the lake's "
                    f"manifest recorded, so the file changed or rotted after it was sealed"
                ) from exc
            raise
        self.summary.puts += 1
        self.summary.put_bytes += len(data)
        self.summary.uploaded.append(rel)

    def _read(self, rel: str, path: Path) -> bytes:
        size = path.stat().st_size
        if size > MAX_PUT_BYTES:
            raise ObjectTooLarge(
                f"{rel} is {size} bytes, past S3's single-PUT limit of {MAX_PUT_BYTES}. A "
                "multipart upload stores no SHA-256 the manifest can be compared with"
            )
        return path.read_bytes()

    def holds(self, rel: str, hexdigest: str) -> bool:
        """Whether the bucket already holds ``rel`` with exactly this SHA-256."""
        self.check()
        try:
            head = self.client.head_object(
                Bucket=self.target.bucket, Key=self.target.key(rel), ChecksumMode="ENABLED"
            )
        except Exception as exc:
            if _is_absent(exc):
                return False
            raise
        return _matches(head, hexdigest)

    def manifested(self, rel: str, hexdigest: str, *, listed: bool) -> None:
        """Upload a manifested file under the manifest's own digest, unless already there.

        Every PUT on a versioned bucket is a new billed version, so an object whose stored
        checksum already matches is never sent again. The HEAD is skipped when the
        listing did not name the key, since nothing can match then.
        """
        if listed and self.holds(rel, hexdigest):
            self.summary.skipped += 1
            return
        data = self._read(rel, self.root / rel)
        self.put(rel, data, hex_to_b64(hexdigest))

    def unmanifested(self, rel: str, path: Path, listed_size: int | None) -> None:
        """Upload a file the manifest does not record, when its size differs from the bucket's."""
        size = path.stat().st_size
        if listed_size == size:
            self.summary.skipped += 1
            return
        data = self._read(rel, path)
        self.put(rel, data, b64_sha256(data))


def manifested_files(root: Path, ledger: Ledger, rels: Sequence[str]) -> Iterator[tuple[str, str]]:
    """The manifested files to upload among ``rels``, as ``(rel, hex sha256)``.

    A manifested ``journal/`` segment is skipped only when its compacted partition is
    manifested, which is the rule ``_compacted_partition_for_segment`` gives the scrub.
    A day compaction refused keeps its segments, and they are that day's only copy, so
    they upload like any partition. Any other manifested file missing from disk refuses
    the upload rather than letting the watermark claim a file the bucket never got.
    """
    for rel in sorted(rels):
        if rsync_excluded(rel, is_dir=False):
            continue
        compacted = _compacted_partition_for_segment(rel)
        if compacted is not None and compacted in ledger.latest:
            continue
        if not (root / rel).is_file():
            raise ManifestedFileMissing(
                f"{rel} is in the lake's manifest and missing from disk, so the upload "
                "stops rather than let the bucket's watermark claim it"
            )
        yield rel, str(ledger.latest[rel]["sha256"])


def _unmanifested(root: Path, ledger: Ledger) -> Iterator[tuple[str, Path]]:
    """Every file the backup copies that carries no manifest entry, ``manifest.jsonl`` aside.

    The rule is every such file, never a list of kinds. ``rsync`` copies every file it is
    not told to exclude, and a list would silently leave out the next kind of file the
    lake grows.
    """
    for rel, path in walk_lake(root):
        if rel == MANIFEST_FILE or rel in ledger.latest:
            continue
        yield rel, path


def nightly_upload(
    lake_root: Path,
    target: BucketTarget,
    *,
    client: Any,
    clock: Clock,
    calendar: Calendar,
    budget: timedelta = NIGHTLY_UPLOAD_BUDGET,
) -> UploadSummary:
    """Upload what changed since the last night. The caller holds the lake-root lock.

    1. The bucket's manifest copy must be a prefix of the lake's that carries at least
       one whole entry when the lake has any, or this refuses with one line naming the
       first-upload command and uploads nothing.
    2. Every manifested file whose latest entry sits past the watermark is pending and
       uploads under the manifest's digest, segments by the segment rule.
    3. Every file with no manifest entry and no exclusion compares by size against one
       listing and uploads when it differs.
    4. ``manifest.jsonl`` goes last, so the watermark never claims a file the bucket does
       not hold. It goes up only when its length moved.

    Nothing is deleted from the bucket and nothing is written under the lake root. The
    deadline is the earlier of ``budget`` past the start and ``session_bound``. It is
    checked before every request, the first included, so an upload started inside a
    session sends nothing.
    """
    root = Path(lake_root)
    started = clock.monotonic()
    summary = UploadSummary(target=str(target))
    uploader = _Uploader(client=client, target=target, root=root, clock=clock, summary=summary)
    now = clock.now()
    deadline = now + budget
    why = f"{budget.total_seconds() / 60:.0f} minutes after it started"
    bound = session_bound(now, clock=clock, calendar=calendar)
    if bound is not None and bound < deadline:
        deadline, why = bound, "ahead of the next session's capture start"
    uploader.guards.append(uploader.deadline(deadline, why, "the next night carries on"))
    uploader.check()
    ledger = read_ledger(root)
    copy = read_copy_state(client, target, ledger.raw)
    if not copy.present or not copy.is_prefix:
        state = (
            "holds no manifest.jsonl"
            if not copy.present
            else ("holds a manifest.jsonl that is not a prefix of the lake's")
        )
        raise WatermarkMissing(
            f"the bucket {state}, so the nightly upload has no watermark. Run "
            f"{FIRST_UPLOAD_COMMAND} by hand: {target}"
        )
    mark = watermark(ledger.raw, copy.length)
    if mark == 0 and ledger.entries:
        # A copy of zero bytes, or one shorter than a whole line, is a prefix of any
        # manifest, and would start a whole-lake upload inside compaction's lock.
        raise WatermarkMissing(
            f"the bucket holds a manifest.jsonl of {copy.length} byte(s) that carries no "
            f"whole entry, so the nightly upload has no watermark. Run "
            f"{FIRST_UPLOAD_COMMAND} by hand: {target}"
        )

    uploader.check()
    listing = list_bucket(client, target)
    pending = [rel for rel, position in ledger.last.items() if position >= mark]
    for rel, hexdigest in manifested_files(root, ledger, pending):
        uploader.manifested(rel, hexdigest, listed=rel in listing)
    for rel, path in _unmanifested(root, ledger):
        uploader.unmanifested(rel, path, listing.get(rel))
    if copy.length != len(ledger.raw):
        uploader.put(MANIFEST_FILE, ledger.raw, b64_sha256(ledger.raw))
    summary.seconds = clock.monotonic() - started
    return summary


class BucketBackup:
    """The ``BackupRunner`` for a bucket target, run inside compaction's lake-root lock.

    ``client`` is an S3 client. ``compact.main`` passes a ``ClientFromConfig``, and a
    test passes a fake. ``clock`` and ``calendar`` set the deadline. ``last`` is what
    the most recent ``sync`` did.
    """

    def __init__(
        self,
        *,
        client: Any,
        clock: Clock,
        calendar: Calendar,
        budget: timedelta = NIGHTLY_UPLOAD_BUDGET,
    ) -> None:
        self._client = client
        self._clock = clock
        self._calendar = calendar
        self._budget = budget
        self.last: UploadSummary | None = None

    def sync(self, source: Path, target: Path | BucketTarget) -> None:
        if not isinstance(target, BucketTarget):
            raise TypeError(f"BucketBackup uploads to a bucket target, not {target!r}")
        client = self._client
        self.last = nightly_upload(
            source,
            target,
            client=client,
            clock=self._clock,
            calendar=self._calendar,
            budget=self._budget,
        )


# -- the first upload ---------------------------------------------------------


def in_sunday_scrub_window(now: datetime) -> bool:
    """Whether ``now`` falls in Sunday's 19:55 to 23:30, when the Sunday job may be scrubbing.

    The Sunday scrub takes no lock. Mid-upload it would find objects in the bucket past
    the old copy's watermark and name them unaccounted, which withholds the ping and
    pages for a backup that is fine. The window runs from the Sunday wake to the moment
    the scrub's last retry must have pinged.
    """
    local = now.astimezone(MARKET_TZ)
    if local.weekday() != _PY_SUNDAY:
        return False
    return SUNDAY_WAKE.on(local.date()) <= local < SUNDAY_ASSERTION_END.on(local.date())


def first_upload(
    lake_root: Path,
    target: BucketTarget,
    *,
    client: Any,
    clock: Clock,
    calendar: Calendar,
) -> UploadSummary:
    """Upload a whole lake, comparing every object rather than trusting a watermark.

    Sealed files need no lock. Each PUT carries the manifest's digest, so S3 refuses
    bytes that changed underneath, for instance through a ``recompact`` during the run.
    The lock is taken only at the end, for the files the manifest gained meanwhile, the
    files with no manifest entry, and ``manifest.jsonl`` itself. So the command blocks
    nothing for most of its run. A bucket whose copy stopped being a prefix is
    re-baselined by the same pass, because the copy is replaced at the end.

    Three guards stop it with one line and leave no ``manifest.jsonl`` uploaded.

    1. The Sunday scrub window, checked before every request rather than only at the
       start, so a run begun at 19:50 stops when the window opens.
    2. ``session_bound``, checked before every request of the locked phase, so the lock
       is never held into a session.
    3. A lake whose own manifest is empty or absent while the bucket's copy is not. A
       wrong ``lake_root`` reads that way, and the run would replace the bucket's
       manifest with an empty one. This is checked before the lock, because taking the
       lock creates ``manifest.jsonl`` under the root it is handed.
    """
    root = Path(lake_root)
    started = clock.monotonic()
    summary = UploadSummary(target=str(target))
    uploader = _Uploader(client=client, target=target, root=root, clock=clock, summary=summary)

    def sunday_window() -> None:
        if in_sunday_scrub_window(clock.now()):
            raise FirstUploadRefused(
                "the first upload does not run on Sunday from 19:55 to 23:30, while the "
                f"Sunday job may be scrubbing the bucket, and stopped after {summary.puts} "
                "PUT(s) with no manifest.jsonl uploaded. Run it on another evening after "
                "the 18:30 sweep"
            )

    uploader.guards.append(sunday_window)
    uploader.check()
    if not root.is_dir():
        raise FirstUploadRefused(f"lake_root {root} is not a directory: {target}")

    early = read_ledger(root)
    before = read_copy_state(client, target, early.raw)
    if not early.raw and before.present and before.length > 0:
        raise FirstUploadRefused(
            f"the lake's manifest.jsonl under {root} is empty or absent while the bucket's "
            f"copy holds {before.length} byte(s), so lake_root may name the wrong "
            f"directory. Nothing was uploaded: {target}"
        )
    summary.rebaselined = before.present and not before.is_prefix
    uploader.check()
    listing = list_bucket(client, target)
    done: dict[str, str] = {}
    for rel, hexdigest in manifested_files(root, early, list(early.latest)):
        uploader.manifested(rel, hexdigest, listed=rel in listing)
        done[rel] = hexdigest

    with lake_lock(root):
        bound = session_bound(clock.now(), clock=clock, calendar=calendar)
        if bound is not None:
            uploader.guards.append(
                uploader.deadline(
                    bound,
                    "ahead of the next session's capture start",
                    "the first upload can run again after that session's 18:30 sweep",
                )
            )
        uploader.check()
        ledger = read_ledger(root)
        late = [rel for rel, entry in ledger.latest.items() if done.get(rel) != entry["sha256"]]
        for rel, hexdigest in manifested_files(root, ledger, late):
            uploader.manifested(rel, hexdigest, listed=rel in listing)
        for rel, path in _unmanifested(root, ledger):
            uploader.unmanifested(rel, path, listing.get(rel))
        uploader.check()
        copy = read_copy_state(client, target, ledger.raw)
        if not (copy.is_prefix and copy.length == len(ledger.raw)):
            uploader.put(MANIFEST_FILE, ledger.raw, b64_sha256(ledger.raw))
    summary.seconds = clock.monotonic() - started
    return summary


# -- the Sunday scrub ---------------------------------------------------------


def bucket_scrub(lake_root: Path, target: BucketTarget, client: Any) -> BackupScrubResult:
    """Scrub the bucket against the lake's manifest. Read-only on both sides.

    The same findings and the same ping rules as ``manifest.backup_scrub``.

    1. The prefix check is :func:`read_copy_state`. A copy that is not a prefix is
       downloaded once, only then, to name the byte where it diverged.
    2. The forward pass sends one ``HeadObject`` with checksum mode per manifested
       object inside the watermark and compares the stored SHA-256 with the manifest.
    3. The reverse pass lists the bucket for orphans and unaccounted objects, skipping
       ``SCRUB_EXCLUSIONS`` as the path form does.
    4. A bucket that refuses or cannot be reached is a named finding that withholds the
       ping and never raises, so the rest of the Sunday job still runs. The two get
       different names, because one needs a new key and the other needs nothing.
    5. ``GetBucketVersioning`` adds a report line when versioning is anything but
       enabled. It withholds nothing, and no check on objects could see it otherwise.

    **This proves less than the path scrub.** ``HeadObject`` returns the checksum S3
    stored at upload, and does not re-hash the bytes at rest. So the scrub proves each
    object is present, is the current version, and held the manifest's bytes when it
    arrived. An overwrite or a delete marker still shows. Rot at rest is the provider's
    durability guarantee plus the restore test rather than this scrub's job.
    """
    name = str(target)
    root = Path(lake_root)
    try:
        ledger = read_ledger(root)
    except OSError as exc:
        return BackupScrubResult(target=name, unreadable=f"{type(exc).__name__}: {exc}")
    try:
        return _bucket_scrub(root, ledger, target, client)
    except Exception as exc:
        failure = _failure(exc)
        if failure is None:
            raise
        kind, detail = failure
        if kind == "refused":
            return BackupScrubResult(target=name, bucket_refused=detail)
        return BackupScrubResult(target=name, bucket_unreachable=detail)


def _versioning_note(client: Any, target: BucketTarget) -> str | None:
    """A report line when the bucket's versioning is not enabled, or ``None``."""
    try:
        status = client.get_bucket_versioning(Bucket=target.bucket).get("Status")
    except Exception as exc:
        failure = _failure(exc)
        if failure is None:
            raise
        return f"bucket versioning unreadable ({failure[1]}): {target}"
    if status == "Enabled":
        return None
    shown = status or "never enabled"
    return (
        f"bucket versioning is {shown}, so an overwrite or a delete keeps no old version: {target}"
    )


def _bucket_scrub(
    root: Path, ledger: Ledger, target: BucketTarget, client: Any
) -> BackupScrubResult:
    name = str(target)
    copy = read_copy_state(client, target, ledger.raw)
    if not copy.present:
        return BackupScrubResult(target=name, manifest_missing=True)
    length = copy.length
    if not copy.is_prefix:
        body = client.get_object(Bucket=target.bucket, Key=target.key(MANIFEST_FILE))["Body"]
        held = body.read()
        if not ledger.raw.startswith(held):
            return BackupScrubResult(
                target=name, manifest_diverged_at=_first_difference(ledger.raw, held)
            )
        length = len(held)

    mark = watermark(ledger.raw, length)
    if mark == 0 and ledger.entries:
        return BackupScrubResult(target=name, manifest_missing=True)
    path = manifest_path(root)
    copied = _latest_by_partition(ledger.entries[:mark], path)
    latest = ledger.latest

    missing: list[str] = []
    sha_mismatches: list[str] = []
    for partition, entry in copied.items():
        compacted = _compacted_partition_for_segment(partition)
        if compacted is not None and compacted in copied:
            continue
        try:
            head = client.head_object(
                Bucket=target.bucket, Key=target.key(partition), ChecksumMode="ENABLED"
            )
        except Exception as exc:
            if _is_absent(exc):
                missing.append(partition)
                continue
            raise
        if not _matches(head, str(entry.get("sha256"))):
            sha_mismatches.append(partition)

    orphans: list[str] = []
    unaccounted: list[str] = []
    for rel in sorted(list_bucket(client, target)):
        if _is_excluded(rel, SCRUB_EXCLUSIONS) or rel in copied:
            continue
        (unaccounted if rel in latest else orphans).append(rel)

    return BackupScrubResult(
        target=name,
        missing=tuple(sorted(missing)),
        sha_mismatches=tuple(sorted(sha_mismatches)),
        unaccounted=tuple(sorted(unaccounted)),
        orphans=tuple(sorted(orphans)),
        pending=tuple(sorted(set(latest) - set(copied))),
        versioning=_versioning_note(client, target),
    )


# -- the live check -----------------------------------------------------------

# The size of the live check's whole-object probe. Past 8 MiB, the threshold where
# ``upload_file`` and ``aws s3 cp`` switch to a multipart upload, so a single PUT that
# stored a full-object SHA-256 is proved past the size where a tool would split it.
LIVE_PROBE_BYTES = 9 * 1024 * 1024


def live_check(
    client: Any, target: BucketTarget, *, stamp: str, out: Callable[[str], None]
) -> bool:
    """Confirm the four provider behaviors this design rests on, against a real bucket.

    No fake can prove any of the four, so the owner runs this by hand and the suite never
    does. It writes three objects under ``<target>/live-check-<stamp>/`` and deletes
    nothing, because the narrow key cannot delete. Each line it prints is one behavior
    passing or failing.

    1. S3 refuses a PUT whose ``ChecksumSHA256`` does not match the bytes.
    2. ``HeadObject`` with checksum mode returns the stored SHA-256 of a single PUT, as a
       full-object value.
    3. A PUT to an existing key on a versioned bucket creates a new version and keeps
       the old one.
    4. ``put_object`` sends one request and never splits into parts.
    """
    base = f"live-check-{stamp}"
    ok = True

    def report(passed: bool, line: str) -> None:
        nonlocal ok
        ok = ok and passed
        out(f"live-check: {'PASS' if passed else 'FAIL'} {line}")

    sent: list[str] = []

    def count(**kwargs: Any) -> None:
        sent.append("request")

    client.meta.events.register("before-send.s3.PutObject", count)

    # 1. A mismatched checksum is refused.
    wrong = b"marketlake live check: these bytes do not match the checksum sent"
    try:
        client.put_object(
            Bucket=target.bucket,
            Key=target.key(f"{base}/refused"),
            Body=wrong,
            ChecksumSHA256=b64_sha256(b"other bytes"),
            StorageClass=STORAGE_CLASS,
        )
    except Exception as exc:
        code = _error_code(exc)
        report(code == BAD_DIGEST, f"1 mismatched ChecksumSHA256 refused with {code}")
    else:
        report(False, "1 mismatched ChecksumSHA256 was accepted")

    # 2 and 4. One PUT past the multipart threshold, one request, a full-object SHA-256.
    probe = os.urandom(LIVE_PROBE_BYTES)
    key = target.key(f"{base}/probe")
    sent.clear()
    first = client.put_object(
        Bucket=target.bucket,
        Key=key,
        Body=probe,
        ChecksumSHA256=b64_sha256(probe),
        StorageClass=STORAGE_CLASS,
    )
    report(len(sent) == 1, f"4 put_object of {LIVE_PROBE_BYTES} bytes sent {len(sent)} request(s)")
    head = client.head_object(Bucket=target.bucket, Key=key, ChecksumMode="ENABLED")
    stored = stored_sha256(head)
    report(
        stored == hashlib.sha256(probe).digest(),
        f"2 HeadObject returned ChecksumSHA256 {head.get('ChecksumSHA256')} with "
        f"ChecksumType {head.get('ChecksumType')} and StorageClass {head.get('StorageClass')}",
    )

    # 3. A second PUT to the same key makes a new version and keeps the first.
    replacement = os.urandom(1024)
    second = client.put_object(
        Bucket=target.bucket,
        Key=key,
        Body=replacement,
        ChecksumSHA256=b64_sha256(replacement),
        StorageClass=STORAGE_CLASS,
    )
    old_id, new_id = first.get("VersionId"), second.get("VersionId")
    distinct = bool(old_id) and bool(new_id) and old_id != new_id and old_id != "null"
    report(distinct, f"3 two PUTs to one key returned versions {old_id} and {new_id}")
    try:
        kept = client.get_object(Bucket=target.bucket, Key=key, VersionId=old_id)["Body"].read()
    except Exception as exc:
        out(
            f"live-check: the old version could not be read back ({_error_code(exc)}). The "
            "narrow key holds no s3:GetObjectVersion, so confirm by hand in the console that "
            f"{key} shows two versions"
        )
    else:
        report(kept == probe, "3 the first version still holds the first PUT's bytes")

    out(
        f"live-check: delete {target.key(base)}/ and every version under it in the console. "
        "The narrow key cannot delete"
    )
    return ok


# -- the command-line entry ---------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The ``python -m lake.bucket`` argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m lake.bucket",
        description="Seed the backup bucket by hand, or check the provider's behavior live.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    first = sub.add_parser(
        "first-upload",
        help="Upload the whole lake, comparing every object, and re-baseline the bucket.",
    )
    first.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    first.add_argument(
        "--target",
        help="The bucket, as s3://<bucket>[/<prefix>]. Defaults to backup_target when it is one.",
    )
    live = sub.add_parser(
        "live-check", help="Confirm the four provider behaviors against the real bucket."
    )
    live.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    live.add_argument(
        "--target",
        required=True,
        help="Where to write the probe objects, as s3://<bucket>/<prefix>, outside the lake's.",
    )
    return parser


def _target(config: Config, given: str | None) -> BucketTarget:
    """The bucket a command works on: ``--target``, or ``backup_target`` when it is a bucket."""
    if given is not None:
        target = parse_backup_target(given)
        if not isinstance(target, BucketTarget):
            raise ConfigError(f"--target must be an s3:// bucket, got {given!r}")
        return target
    if isinstance(config.backup_target, BucketTarget):
        return config.backup_target
    raise ConfigError(
        "backup_target is still a path, so name the bucket with --target s3://<bucket>[/<prefix>]"
    )


def _one_line(exc: BaseException, target: BucketTarget) -> BucketUnreachable | None:
    """A bucket failure as one operator line, or ``None`` when it is not one."""
    failure = _failure(exc)
    if failure is None:
        return None
    kind, detail = failure
    if kind == "refused":
        return BucketUnreachable(f"the bucket refused the request ({detail}): {target}")
    return BucketUnreachable(f"the bucket could not be reached ({detail}): {target}")


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Clock | None = None,
    calendar: Calendar | None = None,
) -> int:
    """The ``python -m lake.bucket`` entry. Returns a process exit code.

    The client is built here from the config and never accepted, the same rule every
    other ``main`` keeps for a seam that reaches past this process. ``clock`` and
    ``calendar`` stay injectable, since neither reaches past this process.
    """
    args = build_parser().parse_args(argv)
    label = args.command
    with input_errors_exit(label, BucketRefusal):
        config = load_config(args.config)
        target, client = connect(config, _target(config, args.target))
        if clock is None:
            from lake.clock import SystemClock

            clock = SystemClock()
        try:
            if args.command == "first-upload":
                if calendar is None:
                    from lake.calendar import ExchangeCalendar

                    calendar = ExchangeCalendar()
                summary = first_upload(
                    config.lake_root, target, client=client, clock=clock, calendar=calendar
                )
            else:
                stamp = clock.now().strftime("%Y%m%dT%H%M%SZ")
                passed = live_check(client, target, stamp=stamp, out=print)
                return 0 if passed else 1
        except BucketRefusal:
            raise
        except Exception as exc:
            line = _one_line(exc, target)
            if line is None:
                raise
            raise line from exc
    if summary.rebaselined:
        print(
            "first-upload: the bucket's manifest.jsonl was not a prefix of the lake's, and "
            "this upload replaced it"
        )
    print(f"first-upload: {summary.render()}")
    return 0


__all__ = [
    "BAD_DIGEST",
    "FIRST_UPLOAD_COMMAND",
    "FULL_OBJECT",
    "IN_FLIGHT_ALLOWANCE",
    "LIVE_PROBE_BYTES",
    "MAX_PUT_BYTES",
    "NIGHTLY_UPLOAD_BUDGET",
    "PRE_OPEN_MARGIN",
    "SEAL_ALLOWANCE",
    "STORAGE_CLASS",
    "SWEEP_MARGIN",
    "BucketBackup",
    "BucketRefusal",
    "BucketSettingsInvalid",
    "BucketUnreachable",
    "ClientFromConfig",
    "ChecksumRefused",
    "CopyState",
    "FirstUploadRefused",
    "Ledger",
    "ManifestedFileMissing",
    "ObjectTooLarge",
    "UploadDeadline",
    "UploadSummary",
    "WatermarkMissing",
    "b64_sha256",
    "bucket_scrub",
    "build_parser",
    "client_from_config",
    "connect",
    "first_upload",
    "hex_to_b64",
    "in_sunday_scrub_window",
    "list_bucket",
    "live_check",
    "main",
    "manifested_files",
    "nightly_upload",
    "read_copy_state",
    "read_ledger",
    "rsync_excluded",
    "session_bound",
    "stored_sha256",
    "walk_lake",
    "watermark",
]


if __name__ == "__main__":
    raise SystemExit(main())
