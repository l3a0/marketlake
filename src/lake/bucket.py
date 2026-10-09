"""The bucket backup: the nightly upload, the first upload, the Sunday scrub, and the restores.

``backup_target`` takes a bucket URL as well as a path, and this module is what a bucket
target runs. The path form, ``rsync`` to a mounted directory, is unchanged and stays the
default, so switching back is one setting. The design's Backup section carries the
reasoning for a bucket at all, and marketlake #630 carries the provider choice: S3
Standard-IA, versioning on, no Object Lock, and credentials with a narrow policy.

Six jobs live here.

1. **The nightly upload**, ``BucketBackup``, runs where ``RsyncBackup`` runs, inside the
   close+15 compaction's lake-root lock. It uploads what changed since the last night
   and never deletes.
2. **The first upload**, ``python -m lake.bucket first-upload``, is run by hand. It
   compares every object rather than trusting the bucket's copy of the manifest, so it
   both seeds an empty bucket and re-baselines one whose copy a human repaired. It refuses
   a copy where some path's latest entry is one this lake never recorded, since replacing
   that copy would drop another host's sessions from the bucket's record.
3. **The bucket scrub**, ``bucket_scrub``, is the Sunday job's check of the bucket. It
   returns the same ``BackupScrubResult`` the path scrub returns, with the same
   findings and the same rule about which of them withhold the ping. The Sunday restore
   test then downloads the week's share of what it matched through ``bucket_reader``.
4. **The restore**, ``python -m lake.bucket restore <dest>``, is run by hand. It
   downloads the bucket's current versions into an empty directory and verifies every
   file before the directory is filled. ``bucket_reader`` is its download too. On a host
   whose config sets ``lake_window_sessions`` it rebuilds that host's trimmed lake, and
   leaves out each partition the bucket's ``trimmed.jsonl`` says was removed on purpose
   (marketlake #785). Elsewhere it restores the whole lake.
5. **The range restore**, ``python -m lake.bucket restore-range``, is run by hand. It puts
   chosen chains or quotes partitions back into the live lake, verified against the lake's
   own manifest, for a partition lost by accident or a rollback of trimming (marketlake
   #784). While the host's config sets ``lake_window_sessions``, a rollback lasts only until
   the trim runs with a split checkpoint from a session after the restore's day, which drops
   each restored partition outside the window again. A lasting rollback also removes the key.
6. **The resync**, ``python -m lake.bucket resync``, is run by hand on a host about to
   become primary again after the other host was primary (marketlake #832). It reads the
   bucket's ``manifest.jsonl``, tells another host's entries from a hand repair, and plans
   what would bring this lake level with it: what to download, what to delete, and what
   refuses. The plan writes nothing. With ``--apply`` it downloads, deletes, and rewrites the
   lake's ``manifest.jsonl`` in place under the lake-root lock to equal the bucket's.

**The manifest's digest travels with every upload.** ``manifest.jsonl`` records each
file's SHA-256 as 64 hex characters. S3 takes a SHA-256 as ``ChecksumSHA256``, the
base64 of the 32 raw digest bytes, so the uploader converts hex to base64 and the scrub
converts back. S3 refuses a PUT whose bytes do not hash to the value sent, with the
error code ``BadDigest``. So a file whose bytes no longer match its manifest entry fails
its upload rather than landing. Every PUT is one ``PutObject``. A
multipart upload stores a checksum of part checksums, marked ``COMPOSITE``, which cannot
be compared with the manifest, so a file over S3's 5 GiB single-PUT limit fails loudly
rather than falling back to parts.

**The watermark.** The bucket's copy of ``manifest.jsonl`` says how far the last upload
got. A ``HeadObject`` returns its length and its stored SHA-256. Hashing that many
leading bytes of the lake's manifest and comparing proves the copy is a prefix without
downloading it, the same byte-prefix rule ``manifest.backup_scrub`` applies to a path
copy. The number of entries in that prefix is the watermark. The nightly upload downloads
the copy only when it is present and not a prefix, so ``bucket_divergence`` can tell
another host's entries from a hand repair, and the refusal can say which. The first upload
and the bucket scrub download it under the same condition. The restore downloads it every
time, because the copy is what it restores the lake's manifest from.

**What stays on the machine.** ``runner.BACKUP_EXCLUSIONS`` decides it, with ``rsync``'s
own matching rules, because the uploader walks the tree itself and ``rsync`` is not
there to apply them. Of this module's jobs, only the range restore and the resync write
into a live lake. The restore writes only into an empty destination, which on the VM is
``lake_root`` before any lake is there. The range restore writes only partitions the
lake's own manifest already records, a restore line in the trimmed ledger, and that
ledger's manifest entry, which are what any lake writer leaves, so switching back to a path
stays free. The resync leaves the lake's files and its ``manifest.jsonl`` as the bucket's
copy records them. The trim that deletes old chains partitions lives in ``lake.trim`` for
that reason (marketlake #787), and ``current_digest``, the read it checks a partition's
bucket copy with, writes nothing.

**The client is built from ``config.yaml`` alone, with one exception.** On the
instance-profile path its credentials come from the EC2 instance metadata service, and
``config.yaml`` is what decides that. The exception has two other users. The token pull
in ``lake.token_store`` follows ``bucket_credentials`` too, so on the VM it reads the
token parameter with the same instance profile (marketlake #636). ``lake.vm_config``'s
render cannot read ``config.yaml``, because it writes that file, so it takes the
instance profile and the region from the tracked ``config/vm.yaml`` instead (marketlake
#686). ``client_from_config`` reads the region and ``bucket_credentials`` from the
config, and ``lake.aws_session``, the builder all three clients share, clears every
``AWS_*`` variable, ``~/.aws/config``,
``~/.aws/credentials`` and ``~/.aws/models`` out of the client's reach while it builds.
``bucket_credentials`` takes three values.

1. Under ``keys``, the default, the access key and secret key come from the config too.
2. Under ``instance_profile`` they come from the EC2 instance metadata service and from
   nowhere else, so a VM with an instance profile attached carries no long-lived key.
3. Under ``assume_role`` the command key in the config assumes the role
   ``bucket_role_arn`` names, and the client signs with the short-lived credentials STS
   returns (marketlake #737). The first request assumes the role, and a refusal then is
   one line naming the command key and the role, by way of ``_failure``.

A development run therefore cannot reach a real bucket on credentials it happened to find
on the machine, because the metadata service is asked only when ``config.yaml`` says so.
``boto3`` is imported in the builder, lazily, so the offline suite never loads it unless
a test builds a client. Every job reaches the
client through ``connect``, which first runs the config's strict bucket checks. Loading
the config runs none of them, because capture loads it every minute and a bad backup
setting must fail the backup and nothing else.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import fcntl
import fnmatch
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from lake import outbox, runway
from lake.aws_session import (
    _UNAVAILABLE_CODES,
    _AssumeRoleFailed,
    _MetadataLookupFailed,
    build_client,
    source_from_bucket_credentials,
)
from lake.calendar import MARKET_TZ, Calendar
from lake.clock import Clock
from lake.config import (
    BUCKET_CREDENTIALS_KEY,
    BUCKET_ROLE_ARN_KEY,
    COMMAND_KEY_ID_KEY,
    COMMAND_SECRET_KEY,
    CREDENTIALS_FROM_ASSUME_ROLE,
    CREDENTIALS_FROM_INSTANCE_PROFILE,
    BucketTarget,
    Config,
    ConfigError,
    bucket_credential_problems,
    input_errors_exit,
    load_config,
    parse_backup_target,
    require_bucket_settings,
)
from lake.control_plane import (
    COMPACTION_RUN,
    DAEMON_LABEL,
    EOD_SWEEP_LABEL,
    SUNDAY_ASSERTION_END,
    SUNDAY_LABEL,
    SUNDAY_WAKE,
    VENDOR_SWEEP,
    WallClockTime,
)
from lake.journal import F_FULLFSYNC
from lake.lock import lake_lock
from lake.manifest import (
    _NAMED_PATHS,
    SCRUB_EXCLUSIONS,
    BackupScrubResult,
    ManifestError,
    _compacted_partition_for_segment,
    _first_difference,
    _is_excluded,
    _latest_by_partition,
    latest_entries,
    manifest_path,
    parse_jsonl,
    sha256_file,
)
from lake.paths import (
    DATE_PARTITIONED,
    DATE_PREFIX,
    LOST_AND_FOUND,
    MANIFEST_FILE,
    TEMP_MARKER,
    TRIMMED_FILE,
    parse_date_dir,
    parse_partition_rel,
    temp_write_path,
)
from lake.runner import BACKUP_EXCLUSIONS
from lake.session import SessionClock
from lake.trimmed import is_designed_absence, latest_by_partition, latest_trimmed, parse_trimmed
from lake.window import WindowRefused, window_sessions

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

# The command that brings a lake level with a bucket another host uploaded to meanwhile,
# named by the refusals on another host's entries (marketlake #832).
RESYNC_COMMAND = "python -m lake.bucket resync"

# The refusal on a shadow host. Its lake is compared and then discarded, so it uploads
# nothing, and a shadow seeded from the primary would write under the primary's
# credentials.
BUCKET_SHADOW = (
    "this host's role is shadow, so it uploads nothing to a bucket and checks none. "
    "Run this on the primary."
)

# The error code S3 answers a PUT whose bytes do not match its ``ChecksumSHA256``.
BAD_DIGEST = "BadDigest"

# What a ``HeadObject`` on a missing key answers. The bucket's credentials hold
# ``s3:ListBucket``, so S3 answers 404 rather than 403, and a HEAD error carries no body
# to name a code in.
_ABSENT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})

# The error codes that mean the credentials or their policy were turned away. These and
# an HTTP 401 or 403 are the only answers named *refused*, whose repair is new
# credentials or a fixed policy. A bare "403" is what a ``HeadObject`` answers, since it
# has no body.
_CREDENTIAL_CODES = frozenset(
    {
        "401",
        "403",
        "AccessDenied",
        "AccountProblem",
        "AllAccessDisabled",
        "ExpiredToken",
        "InvalidAccessKeyId",
        "InvalidToken",
        "SignatureDoesNotMatch",
        "TokenRefreshRequired",
    }
)

# The codes S3 uses to say it is busy or briefly unable to answer are
# ``lake.aws_session._UNAVAILABLE_CODES``. Those and any 5xx or 429 are named
# *unreachable* with a failed connection, because the repair for all of them is usually
# to wait. They live there because the assume-role refresh sorts STS's answers with them.

# The word every assume-role detail starts with, so a line can tell STS's refusal from
# the bucket's own.
ASSUME_ROLE = "AssumeRole"

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

    An empty bucket would otherwise start a whole-lake upload inside compaction's lock, and
    the first-upload command is its repair. A copy that is not a prefix has four causes,
    the same four ``bucket_divergence`` tells apart.

    1. Another host appended its own sessions to the bucket's copy.
    2. This lake was restored from an older copy, so the bucket holds entries it lost.
    3. A human repaired a line in the lake's manifest or the bucket's copy.
    4. A line that still parses was damaged on either side.

    The first two leave entries this lake never recorded, and the first-upload command
    would drop them from the bucket's record, so the refusal says not to run it and names
    the resync, which brings the lake level instead. The fourth reads the same way,
    because its bytes cannot be told from the first two, and refusing is the safe side. So
    damage to the lake's own manifest refuses and blames another host, while the real
    repair is fixing the lake's line. Only the third keeps the first-upload command as its
    repair.

    A copy stored with no SHA-256 is a separate case. When the lake's manifest starts with
    its bytes, nothing is wrong with it except that it cannot be proved a prefix, and its
    line names the first-upload command too.
    """


class ManifestedFileMissing(BucketRefusal):
    """A file the manifest records is gone from disk, so the watermark cannot claim it."""


class TrimmedNotInBucket(BucketRefusal):
    """A partition trimmed on purpose is absent from the bucket, so no copy of it is left.

    The trimmed ledger says the lake removed this file because the bucket held it. The first
    upload is the one job that re-baselines the bucket, so it checks that claim rather than
    trusting it, and a bucket without the partition means the only copy is gone.
    """


class BucketLedgerRefused(BucketRefusal):
    """The bucket's copy of ``trimmed.jsonl`` is missing or will not parse.

    Without it, no partition the bucket's manifest records can be told apart as removed on
    purpose rather than lost. :func:`read_bucket_trimmed` raises it. It is a plain
    ``BucketRefusal`` rather than a ``RestoreRefused``, because the restore reads the bucket's
    ledger through that helper, and marketlake #832's resync, still open, is to read it the
    same way. Each re-raises this as its own refusal with its own repair.
    """


class BucketLedgerMissing(BucketLedgerRefused):
    """The bucket holds no current ``trimmed.jsonl``, though its manifest records one."""


class BucketLedgerMismatch(BucketLedgerRefused):
    """The bucket's current ``trimmed.jsonl`` does not hash to its bucket manifest entry.

    ``sha256`` is the entry's SHA-256, the hash a copy has to match. A caller that can take a
    matching copy from somewhere else catches this subclass, and a caller that cannot catches
    :class:`BucketLedgerRefused`.
    """

    def __init__(self, message: str, sha256: str) -> None:
        super().__init__(message)
        self.sha256 = sha256


class ObjectTooLarge(BucketRefusal):
    """A file is past S3's single-PUT limit and would need a multipart upload."""


class ChecksumRefused(BucketRefusal):
    """S3 refused a PUT because the bytes did not match the manifest's SHA-256.

    Rot is one cause and not the only one. The bytes also change under a ``recompact``
    that runs while a first upload is going, and a live journal segment grows while a
    compaction run by hand in a session uploads it, since capture writes segment bytes
    outside the lock. The message names all three, so the operator does not read a
    benign race as rot.
    """


class UploadDeadline(BucketRefusal):
    """The nightly upload reached its deadline and stopped before ``manifest.jsonl``."""


class FirstUploadRefused(BucketRefusal):
    """The first-upload command refused to start."""


class BucketUnreachable(BucketRefusal):
    """The bucket refused a request or could not be reached, as one operator line."""


class BucketSettingsInvalid(BucketRefusal, ConfigError):
    """The config's bucket settings cannot build a client, as one operator line.

    It is a ``ConfigError`` because the repair is a setting: an edit to ``config.yaml``,
    or on the instance-profile path the instance profile itself, and the line names
    which. It is a ``BucketRefusal`` because it is raised when a bucket job runs rather
    than when the config loads, so the job that catches a bucket refusal catches this one
    too.
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


def _status(exc: BaseException) -> int | None:
    """The HTTP status a ``ClientError`` carries, or ``None``."""
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return None
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status if isinstance(status, int) else None


def _failure(exc: BaseException) -> tuple[str, str] | None:
    """Sort a bucket failure into refused, unreachable or failed, or ``None`` for a bug.

    The three names send the operator to three repairs.

    1. *Refused* means S3 turned the credentials away: a revoked key, a bad signature, or
       a policy that lost an action. The repair is new credentials or a fixed policy.
    2. *Unreachable* means no usable answer came back: a failed connection, a timeout, a
       5xx, or S3 asking for fewer requests with ``SlowDown`` or a 429. The repair is
       usually nothing, because the network or the service comes back.
    3. *Failed* is any other answer S3 gave, such as ``NoSuchBucket``. It names the code
       and promises no repair, because no single one fits.

    On the assume-role path the first request, and any refresh after it, asks STS for
    the role's credentials first. ``_AssumeRoleFailed`` is STS turning that down, so it
    is *refused* or *unreachable* as ``lake.aws_session`` sorted it where it was caught,
    with the detail ``AssumeRole <code>``. Without this branch every caller would re-raise
    it, with two results on the Sunday job.

    1. The guard on the bucket scrub would name it only as a scrub that raised, rather than
       as the refusal or the outage it is. That guard still withholds the ping.
    2. The same refusal during the restore read through ``bucket_reader`` would raise out
       of the Sunday job before its canary. That read's only catch is ``restore_check``'s
       ``except OSError``, and ``_AssumeRoleFailed`` is not an ``OSError``.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    if isinstance(exc, _AssumeRoleFailed):
        return ("unreachable" if exc.transient else "refused"), f"{ASSUME_ROLE} {exc.code}"
    if isinstance(exc, ClientError):
        code = _error_code(exc) or type(exc).__name__
        status = _status(exc)
        if code in _UNAVAILABLE_CODES or status == 429 or (status or 0) >= 500:
            return "unreachable", code
        if code in _CREDENTIAL_CODES or status in (401, 403):
            return "refused", code
        return "failed", code
    if isinstance(exc, BotoCoreError):
        return "unreachable", type(exc).__name__
    return None


# -- the client ---------------------------------------------------------------

# The S3 client's settings. The timeouts bound a stalled socket. They do not bound a slow
# PUT, which the nightly deadline handles between PUTs.
_S3_CLIENT_CONFIG: Mapping[str, Any] = {
    "connect_timeout": 10,
    "read_timeout": 60,
    "retries": {"mode": "standard", "max_attempts": 3},
    # The SHA-256 is supplied on every PUT, which stops botocore adding a CRC32 of its
    # own and the aws-chunked trailer that carries one.
    "request_checksum_calculation": "when_required",
    "response_checksum_validation": "when_required",
}


def _lookup_failed(detail: str) -> ConfigError:
    """The refusal for an instance-profile build that found no credentials.

    It names both fixes, because the fix depends on the host. On the VM it is the
    instance profile, and on a laptop that carries the setting by mistake it is
    ``config.yaml``, where the laptop's own value is ``assume_role``.
    """
    return ConfigError(
        f"{BUCKET_CREDENTIALS_KEY} is {CREDENTIALS_FROM_INSTANCE_PROFILE} and no credentials "
        f"came from the instance metadata service ({detail}). Attach the instance profile, "
        f"or set {BUCKET_CREDENTIALS_KEY}: {CREDENTIALS_FROM_ASSUME_ROLE} in config.yaml"
    )


def client_from_config(config: Config) -> Any:
    """An S3 client built from the config's bucket settings and nothing else.

    ``bucket_credentials`` picks the credentials. On the key path the access key and
    secret key are passed explicitly. On the instance-profile path they come from the
    instance metadata service, and are fetched before the client is built, so a host
    with no instance profile refuses here as one line rather than at the first request.
    On the assume-role path no credentials are fetched here. The first request assumes
    the role, and a refusal reaches the job that made it, which ``_failure`` sorts. Every
    way, the region decides the endpoint, and ``lake.aws_session.build_client`` keeps the
    environment, ``~/.aws`` and ``~/.aws/models`` out of the build.
    """
    problems = bucket_credential_problems(config)
    if problems:
        raise ConfigError(". ".join(problems))
    from botocore.exceptions import BotoCoreError  # lazy: only a bucket job needs it

    try:
        return _build_client(config)
    except _MetadataLookupFailed as exc:
        raise _lookup_failed(exc.detail) from None
    except (BotoCoreError, ValueError) as exc:
        # ``botocore`` refuses a malformed region with an error that is both of these.
        # Only the type is named, because a message may quote a value from the config.
        raise ConfigError(
            f"the bucket client could not be built from config.yaml ({type(exc).__name__})"
        ) from None


def _build_client(config: Config) -> Any:
    """The S3 client for the config's bucket settings, built by ``lake.aws_session``.

    ``source_from_bucket_credentials`` picks the credential source and refuses any
    ``bucket_credentials`` value but the three it names. The S3 checksum settings are this
    module's own, because only S3 has them.
    """
    return build_client(
        "s3",
        region=config.bucket_region,
        source=source_from_bucket_credentials(config),
        client_config=_S3_CLIENT_CONFIG,
    )


def connect(config: Config, target: BucketTarget | None = None) -> tuple[BucketTarget, Any]:
    """The checked bucket target and a client for it, or ``BucketSettingsInvalid``.

    ``target`` defaults to ``backup_target``. The config's strict bucket checks run
    first, then the client is built. Either failing is one operator line naming what
    to fix: a key in ``config.yaml``, or, when an instance-profile build finds no
    credentials, both the instance profile and the ``config.yaml`` setting, because
    which one is wrong depends on the host.
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
    lake's manifest. ``length`` is the copy's size in bytes. ``stored`` is the 32-byte
    SHA-256 S3 stored for the copy, or ``None`` when the copy carries none that proves
    anything, so a later download of the copy can be checked against the HEAD that sized it.
    """

    present: bool
    length: int = 0
    is_prefix: bool = False
    stored: bytes | None = None


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
    return CopyState(present=True, length=length, is_prefix=is_prefix, stored=stored)


def watermark(lake_bytes: bytes, length: int) -> int:
    """How many manifest entries the bucket's copy of ``length`` bytes carries."""
    return len(parse_jsonl(lake_bytes[:length].decode("utf-8", "replace")))


@dataclass(frozen=True)
class Divergence:
    """How a bucket's ``manifest.jsonl`` and the lake's part, from :func:`bucket_divergence`.

    ``shared_bytes`` is the length of the bytes both share, cut back to a line boundary, and
    ``shared`` is how many entries those bytes hold. ``bucket_tail`` and ``lake_tail`` count
    the entries each side holds past them, and ``bucket_first`` and ``lake_first`` name the
    first partition of each, or ``None`` when that side holds none. ``shared``,
    ``bucket_tail`` and ``lake_tail`` read lines the way :func:`_entries` reads them, so a
    fused line counts the whole entry at its end. ``foreign`` holds the latest entry in the
    bucket's tail for each path where that entry is one this lake never recorded, in the
    bucket's order.
    """

    shared_bytes: int
    shared: int
    bucket_tail: int
    bucket_first: str | None
    lake_tail: int
    lake_first: str | None
    foreign: tuple[dict, ...]


def _is_entry(value: object) -> bool:
    """Whether a parsed line is an object naming a string ``partition``."""
    return isinstance(value, dict) and isinstance(value.get("partition"), str)


def _fused_tail(text: str) -> object:
    """The whole object at the end of a line that does not parse, or ``None``.

    A short write leaves a fragment with no newline, and the next append lands on the same
    line, as ``manifest.append_line`` says. The fragment is lost, but the entry after it is
    whole, and it may be the only record of another host's session. Each ``{`` after the
    line's first character is tried from the left, and the first suffix that parses is
    returned. :func:`_entries` keeps it only when it names a string ``partition``.
    """
    start = text.find("{", 1)
    while start != -1:
        try:
            return json.loads(text[start:])
        except (ValueError, RecursionError):
            start = text.find("{", start + 1)
    return None


def _entries(raw: bytes) -> list[dict]:
    """Every entry naming a string ``partition`` in ``raw``, each line parsed on its own.

    ``parse_jsonl`` stops at the first line that does not parse, which is the right rule
    for a torn tail and the wrong one here: a fused line would hide a foreign entry after
    it. So each line is parsed alone, and a line that does not parse gives up the whole
    entry at its end, by :func:`_fused_tail`. Each line decodes with a replacement, as
    :func:`read_ledger` decodes, because ``json.loads`` on bytes raises
    ``UnicodeDecodeError``, which is not a ``JSONDecodeError``. A line nested deeper than
    the interpreter's recursion limit, such as ``[`` repeated 50,000 times, raises
    ``RecursionError`` rather than ``ValueError``, so both parses catch both. A line that
    parses to something other than an object with a string ``partition`` is skipped.
    """
    entries: list[dict] = []
    for line in raw.split(b"\n"):
        text = line.decode("utf-8", "replace").strip()
        if not text:
            continue
        try:
            value = json.loads(text)
        except (ValueError, RecursionError):
            value = _fused_tail(text)
        if _is_entry(value):
            entries.append(value)
    return entries


def _pair(entry: Mapping[str, Any]) -> tuple[str, str | None]:
    """An entry's partition and sha256, with a sha that is not a string read as ``None``.

    A hand-edited list there would otherwise raise ``TypeError`` on the set lookup.
    """
    sha = entry.get("sha256")
    return entry["partition"], sha if isinstance(sha, str) else None


def bucket_divergence(bucket_raw: bytes, lake_raw: bytes) -> Divergence:
    """Tell where the bucket's ``manifest.jsonl`` leaves the lake's, and what it holds after.

    A copy that is not a prefix of the lake's has four causes, and this tells them apart as
    far as the bytes allow.

    1. Another host appended its own sessions. The bucket then holds entries this lake
       never recorded, which ``foreign`` returns.
    2. The lake was restored from an older copy, so the bucket holds entries the lake lost.
       They read as foreign the same way.
    3. A human repaired a line on either side, such as a fused line, a byte-order mark or a
       line with no partition. Every entry the copy still holds is somewhere in the lake,
       so ``foreign`` is empty.
    4. A line that still parses was damaged on either side. It reads as foreign, which is
       the safe side. So damage to the lake's own manifest, such as a rotted sha or a
       deleted line that was a path's latest entry, refuses and blames another host, while
       the real repair is fixing the lake's line.

    A bucket entry is foreign when its partition and sha256 pair appears in no entry
    anywhere in the lake's manifest. Anywhere rather than in the lake's latest, so the
    18:30 sweep's later entries on the same paths change nothing. Only the latest entry for
    each partition in the bucket's tail counts, so an earlier entry its own host superseded
    loses nothing current. For the same reason, a hand repair that deleted a line from the
    lake's manifest is not refused when a later entry for the same path in the bucket's tail
    replaced that line. Deleting a path's latest or only entry is refused, as cause 4 says.

    The shared bytes are cut where the two first differ, then back to the last newline
    before that, or to 0 when there is none.
    """
    return _divergence(bucket_raw, lake_raw, _entries)


def _divergence(
    bucket_raw: bytes, lake_raw: bytes, entries: Callable[[bytes], list[dict]]
) -> Divergence:
    """:func:`bucket_divergence`, with the lines of each side read by ``entries``.

    The nightly upload and the first upload read with :func:`_entries`, which steps over a
    damaged line. The resync reads with :func:`_whole_entries`, which refuses one, so every
    count and view it prints comes from the one parse it checked.
    """
    cut = _first_difference(lake_raw, bucket_raw)
    shared_bytes = lake_raw.rfind(b"\n", 0, cut) + 1
    bucket_tail = entries(bucket_raw[shared_bytes:])
    lake_tail = entries(lake_raw[shared_bytes:])
    recorded = {_pair(entry) for entry in entries(lake_raw)}
    latest: dict[str, tuple[int, dict]] = {}
    for index, entry in enumerate(bucket_tail):
        latest[entry["partition"]] = (index, entry)
    foreign = tuple(
        entry
        for _, entry in sorted(latest.values(), key=lambda item: item[0])
        if _pair(entry) not in recorded
    )
    return Divergence(
        shared_bytes=shared_bytes,
        shared=len(entries(lake_raw[:shared_bytes])),
        bucket_tail=len(bucket_tail),
        bucket_first=bucket_tail[0]["partition"] if bucket_tail else None,
        lake_tail=len(lake_tail),
        lake_first=lake_tail[0]["partition"] if lake_tail else None,
        foreign=foreign,
    )


def _read_bucket_manifest(client: Any, target: BucketTarget, copy: CopyState) -> bytes | None:
    """Download the bucket's ``manifest.jsonl`` and check it is the copy ``copy`` describes.

    One plain ``GetObject`` through :func:`bucket_reader`, its chunks joined inside the
    reader's own error handling. The body must hash to the SHA-256 the HEAD stored, or, when
    no SHA-256 was stored, have the HEAD's length. A body that fails its check returns
    ``None``: the copy changed between the HEAD and the GET, and another host may be
    uploading now, so each caller words its own refusal. A bucket failure raises
    :class:`BucketReadError`, and anything else raises as itself.

    The GET sends no ``ChecksumMode``. botocore would then check the body itself and raise
    ``FlexibleChecksumError``, which ``_failure`` sorts as unreachable, and the mismatch
    here would never be seen. The client's ``response_checksum_validation`` is
    ``when_required``, so a plain GET's body reaches this check unchecked.
    """
    body = b"".join(bucket_reader(client, target)(MANIFEST_FILE))
    if copy.stored is not None:
        return body if hashlib.sha256(body).digest() == copy.stored else None
    return body if len(body) == copy.length else None


def _quoted(partitions: Sequence[str]) -> str:
    """Up to ``_NAMED_PATHS`` partitions from the bucket, on one line.

    A partition read from the bucket's copy is untrusted and may decode to hold a newline,
    so each renders with ``!r``, as ``_target`` renders its input.
    """
    named = ", ".join(repr(rel) for rel in partitions[:_NAMED_PATHS])
    more = len(partitions) - _NAMED_PATHS
    return f"{named} and {more} more" if more > 0 else named


def _tail(count: int, first: str | None) -> str:
    """A tail's entry count and, when it has one, its first partition."""
    return f"{count} entries" if first is None else f"{count} entries from {first!r}"


# What the refusals on another host's entries say to do instead.
_RESYNC_POINTER = f"{RESYNC_COMMAND}, then the same with --apply, brings this lake level"


def _diverged_copy(
    client: Any,
    target: BucketTarget,
    copy: CopyState,
    lake_raw: bytes,
    uploader: _Uploader,
) -> WatermarkMissing:
    """The nightly upload's refusal for a copy that is present and not a prefix.

    The copy is downloaded only here, so the nightly path that finds a prefix sends no new
    request. Four refusals come out of it, each one line.

    1. On some path the latest entry in the copy's tail is one this lake never recorded.
       Running the first upload would drop it from the bucket's record, so the line says
       not to and does not name the command. The line counts entries for the tails and
       paths for the ones this lake never recorded, and says which is which.
    2. The copy carries no stored SHA-256 and the lake's manifest starts with its bytes. It
       cannot be proved a prefix, and the line names the first upload.
    3. No path's latest entry is one this lake never recorded, which is a hand repair, and
       the line names the first upload as before.
    4. The copy could not be read to tell the first from the third: the deadline passed,
       the GET failed, or the body is not the copy the HEAD described. The line says not to
       run the first upload, since it cannot rule out another host. ``UploadDeadline`` is
       not raised, because its "the next night carries on" would be false here.
    """

    def unread(why: str) -> WatermarkMissing:
        return WatermarkMissing(
            "the bucket holds a manifest.jsonl that is not proved a prefix of the lake's, and "
            f"it could not be read to tell another host's entries from a hand repair ({why}), "
            "so the nightly upload has no watermark. Do not run first-upload, which could "
            f"drop another host's entries from the bucket's record: {target}"
        )

    try:
        uploader.check()
    except UploadDeadline:
        return unread("the upload reached its deadline")
    try:
        body = _read_bucket_manifest(client, target, copy)
    except BucketReadError as exc:
        return unread(str(exc))
    if body is None:
        return unread(
            "the copy changed between its HEAD and its GET, so another host may be uploading now"
        )
    split = bucket_divergence(body, lake_raw)
    if split.foreign:
        return WatermarkMissing(
            f"the bucket's manifest.jsonl shares {split.shared} entries with the lake's, then "
            f"holds {_tail(split.bucket_tail, split.bucket_first)}, while this lake holds "
            f"{_tail(split.lake_tail, split.lake_first)} of its own. On {len(split.foreign)} "
            "path(s) the latest entry in the bucket's tail is one this lake never recorded "
            "(another host's sessions, or a lake restored from an older copy), so do not run "
            "first-upload, which would drop those entries from the bucket's record, and "
            f"{_RESYNC_POINTER}: {target}"
        )
    if copy.stored is None and lake_raw.startswith(body):
        return WatermarkMissing(
            "the bucket's manifest.jsonl carries no stored SHA-256 to prove it a prefix of the "
            "lake's, so the nightly upload has no watermark. Run "
            f"{FIRST_UPLOAD_COMMAND} by hand: {target}"
        )
    return WatermarkMissing(
        "the bucket holds a manifest.jsonl that is not a prefix of the lake's, and no path's "
        "latest entry in its tail is one this lake never recorded, so the nightly upload has "
        f"no watermark. Run {FIRST_UPLOAD_COMMAND} by hand: {target}"
    )


def _refuse_foreign(
    client: Any,
    target: BucketTarget,
    copy: CopyState,
    lake_raw: bytes,
    uploader: _Uploader,
) -> None:
    """The first upload's guard: refuse when the bucket's copy may hold another host's entries.

    It reads only a copy that is present and not a prefix, and runs the upload's guards
    first, so the Sunday window holds before the GET. A failed read raises
    :class:`BucketReadError`, which ``main`` prints as one line.
    """
    if not copy.present or copy.is_prefix:
        return
    uploader.check()
    body = _read_bucket_manifest(client, target, copy)
    puts = uploader.summary.puts
    if body is None:
        raise FirstUploadRefused(
            "the bucket's manifest.jsonl changed between its HEAD and its GET, so another host "
            f"may be uploading now. The first upload stopped after {puts} PUT(s) with no "
            f"manifest.jsonl uploaded: {target}"
        )
    foreign = bucket_divergence(body, lake_raw).foreign
    if foreign:
        raise FirstUploadRefused(
            f"on {len(foreign)} path(s) the latest entry in the tail of the bucket's "
            "manifest.jsonl is one this lake never recorded "
            f"({_quoted([entry['partition'] for entry in foreign])}), from another host's "
            "sessions or a lake restored from an older copy, and replacing the copy would drop "
            f"those entries from the bucket's record. The first upload stopped after {puts} "
            f"PUT(s) with no manifest.jsonl uploaded, and {_RESYNC_POINTER}: {target}"
        )


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
    """What one upload did, for the line a job prints.

    ``deadline`` and ``watermark`` are set by the nightly upload and left ``None`` by the first
    upload, which shares this type. ``deadline`` is the moment the upload had to stop by.
    ``watermark`` is the number of manifest entries the bucket's copy carried when the upload
    started, so an entry at a lower position was already in the bucket before tonight. The
    trim after compaction (marketlake #787) reads both: it stops between partitions at the
    same deadline, and it drops only a partition whose entry sits below that watermark.
    """

    target: str
    puts: int = 0
    put_bytes: int = 0
    skipped: int = 0
    seconds: float = 0.0
    rebaselined: bool = False
    uploaded: list[str] = field(default_factory=list)
    deadline: datetime | None = None
    watermark: int | None = None

    def render(self) -> str:
        """One line naming what went up and how fast."""
        megabytes = self.put_bytes / 1_000_000
        rate = (self.put_bytes * 8 / 1_000_000 / self.seconds) if self.seconds > 0 else 0.0
        return (
            f"uploaded {self.puts} file(s), {megabytes:.1f} MB in {self.seconds:.0f} s "
            f"({rate:.1f} Mbit/s), skipped {self.skipped} already in the bucket: {self.target}"
        )


def _checksum_refusal(rel: str) -> str:
    """The one line ``ChecksumRefused`` carries for ``rel``.

    Every file names rot and the two benign races. The trimmed ledger names a fourth cause,
    because a trim or restore line can land without its manifest entry being refreshed. The
    nightly upload and the first upload share this line, and only the nightly one runs after
    compaction's ledger repair, so the line says the repair runs on a nightly compaction and
    gives the hand repair that holds for both callers.
    """
    message = (
        f"S3 refused {rel}: its bytes no longer match its manifest entry's SHA-256. Rot does "
        "this, and so do two benign causes: a recompact while a first upload runs, and a live "
        "journal segment growing while a compaction runs by hand in a session"
    )
    if rel == TRIMMED_FILE:
        message += (
            ". A fourth cause is a trim or restore line that landed without its manifest "
            "entry. On a nightly compaction, the ledger repair re-records it before the upload "
            "unless it refused, which pages compaction_trimmed_ledger. Either way, "
            "lake.trimmed.repair_trimmed_entry run by hand under the lock re-records it"
        )
    return message + ". Run the job again, and treat a repeat as rot"


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
                raise ChecksumRefused(_checksum_refusal(rel)) from exc
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

    def trimmed(self, rel: str, hexdigest: str, *, listed: bool) -> None:
        """Confirm the bucket holds a partition trimmed on purpose, or refuse.

        The trimmed ledger's claim is that the bucket holds these bytes, so the check is the
        same compare-before-PUT :meth:`manifested` makes. Nothing is read from disk, because
        the file is not there. The HEAD is skipped when the listing did not name the key, since
        nothing can match then.
        """
        if listed and self.holds(rel, hexdigest):
            self.summary.skipped += 1
            return
        raise TrimmedNotInBucket(
            f"{rel} was trimmed from the lake on purpose and the bucket does not hold it with "
            "the manifest's SHA-256, so no copy of it is left. Nothing is uploaded over it and "
            f"no manifest.jsonl goes up: {self.target}"
        )

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


def first_upload_files(
    root: Path, ledger: Ledger, rels: Sequence[str]
) -> Iterator[tuple[str, str, bool]]:
    """:func:`manifested_files` for the first upload, which also meets trimmed partitions.

    It yields ``(rel, hex sha256, trimmed)``. A present file yields ``trimmed`` false and is
    uploaded as before. An absent file that ``lake.trimmed.is_designed_absence`` calls a designed
    absence yields ``trimmed`` true, and the caller checks the bucket holds it rather than
    reading a file that is not there. Any other absent file refuses with
    :class:`ManifestedFileMissing`, exactly as :func:`manifested_files` does.

    The trimmed ledger is read only at the first absent file, so a lake with every file present
    reads exactly what it read before marketlake #782, and a damaged ledger costs nothing until
    an absence needs it. A ledger that cannot be read then refuses as
    :class:`ManifestedFileMissing`, naming why, because an absence nothing explains is loss.

    ``nightly_upload`` keeps :func:`manifested_files`. A trim removes only a partition whose
    entry sits behind the bucket's watermark, and the watermark never moves back, so a designed
    absence among the nightly upload's pending entries cannot happen and the raise guards
    exactly that.
    """
    trimmed: Mapping[str, dict] | None = None
    for rel in sorted(rels):
        if rsync_excluded(rel, is_dir=False):
            continue
        compacted = _compacted_partition_for_segment(rel)
        if compacted is not None and compacted in ledger.latest:
            continue
        # The present file is handled before the absence reads anything, the order
        # :func:`manifested_files` keeps, so an entry with no ``sha256`` whose file is missing
        # refuses as missing here exactly as it does there.
        if (root / rel).is_file():
            yield rel, str(ledger.latest[rel]["sha256"]), False
            continue
        if trimmed is None:
            try:
                trimmed = latest_trimmed(root)
            except (ManifestError, OSError) as exc:
                raise ManifestedFileMissing(
                    f"{rel} is in the lake's manifest and missing from disk, and the trimmed "
                    f"ledger that could say it was removed on purpose cannot be read "
                    f"({type(exc).__name__}), so the upload stops rather than let the bucket's "
                    "watermark claim it"
                ) from exc
        if not is_designed_absence(rel, ledger.latest, trimmed):
            raise ManifestedFileMissing(
                f"{rel} is in the lake's manifest and missing from disk, so the upload "
                "stops rather than let the bucket's watermark claim it"
            )
        # A designed absence has a ``sha256`` on its entry, because the predicate compared it.
        yield rel, str(ledger.latest[rel]["sha256"]), True


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
       one whole entry when the lake has any, or this refuses with one line and uploads
       nothing. An absent copy, one with no whole entry, one stored with no SHA-256 that
       the lake's manifest starts with, and a hand repair name the first-upload command. A
       copy where some path's latest entry is one this lake never recorded, and one that
       could not be read to tell, say not to run it.
    2. Every manifested file whose latest entry sits past the watermark is pending and
       uploads under the manifest's digest, segments by the segment rule.
    3. Every file with no manifest entry and no exclusion compares by size against one
       listing and uploads when it differs.
    4. ``manifest.jsonl`` goes last, so the watermark never claims a file the bucket does
       not hold. It goes up only when its length moved.

    Nothing is deleted from the bucket and nothing is written under the lake root. The
    deadline is the earlier of ``budget`` past the start and ``session_bound``. It is
    checked before every request, the first included, so an upload started inside a
    session sends nothing. The summary returned carries that deadline and the watermark
    the upload started from, for the trim that runs after it.
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
    summary.deadline = deadline
    uploader.guards.append(uploader.deadline(deadline, why, "the next night carries on"))
    uploader.check()
    ledger = read_ledger(root)
    copy = read_copy_state(client, target, ledger.raw)
    if not copy.present:
        raise WatermarkMissing(
            "the bucket holds no manifest.jsonl, so the nightly upload has no watermark. Run "
            f"{FIRST_UPLOAD_COMMAND} by hand: {target}"
        )
    if not copy.is_prefix:
        raise _diverged_copy(client, target, copy, ledger.raw, uploader)
    mark = watermark(ledger.raw, copy.length)
    summary.watermark = mark
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
    the most recent ``sync`` did, and ``compact.main`` prints it as one line. A ``sync``
    that raises leaves ``last`` at ``None``, so nothing reads an earlier night's summary
    as tonight's. The trim reads ``client`` and ``last`` after the upload.
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

    @property
    def client(self) -> Any:
        """The S3 client every request of this backup goes through."""
        return self._client

    def sync(self, source: Path, target: Path | BucketTarget) -> None:
        if not isinstance(target, BucketTarget):
            raise TypeError(f"BucketBackup uploads to a bucket target, not {target!r}")
        self.last = None
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
    nothing for most of its run. A bucket whose copy a human repaired is re-baselined by
    the same pass, because the copy is replaced at the end.

    Four guards stop it with one line and leave no ``manifest.jsonl`` uploaded.

    1. The Sunday scrub window, checked before every request rather than only at the
       start, so a run begun at 19:50 stops when the window opens.
    2. ``session_bound``, checked before every request of the locked phase, so the lock
       is never held into a session.
    3. A lake whose own manifest is empty or absent while the bucket's copy is not. A
       wrong ``lake_root`` reads that way, and the run would replace the bucket's
       manifest with an empty one. This is checked before the lock, because taking the
       lock creates ``manifest.jsonl`` under the root it is handed.
    4. A bucket copy that is present and not a prefix, where ``bucket_divergence`` finds a
       path whose latest entry is one this lake never recorded. Replacing it would drop
       another host's sessions from the bucket's record. It is checked before
       ``list_bucket`` and again under the lock before the manifest PUT. The second check
       limits damage rather than preventing it, since the unlocked pass has already sent
       any shared path whose sha differs. A copy that changed between its HEAD and its GET
       refuses too, and a failed read raises ``BucketReadError``.
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
    _refuse_foreign(client, target, before, early.raw, uploader)
    summary.rebaselined = before.present and not before.is_prefix
    uploader.check()
    listing = list_bucket(client, target)
    done: dict[str, str] = {}
    for rel, hexdigest, trimmed in first_upload_files(root, early, list(early.latest)):
        if trimmed:
            uploader.trimmed(rel, hexdigest, listed=rel in listing)
        else:
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
        for rel, hexdigest, trimmed in first_upload_files(root, ledger, late):
            if trimmed:
                uploader.trimmed(rel, hexdigest, listed=rel in listing)
            else:
                uploader.manifested(rel, hexdigest, listed=rel in listing)
        for rel, path in _unmanifested(root, ledger):
            uploader.unmanifested(rel, path, listing.get(rel))
        uploader.check()
        copy = read_copy_state(client, target, ledger.raw)
        _refuse_foreign(client, target, copy, ledger.raw, uploader)
        if not (copy.is_prefix and copy.length == len(ledger.raw)):
            uploader.put(MANIFEST_FILE, ledger.raw, b64_sha256(ledger.raw))
    summary.seconds = clock.monotonic() - started
    return summary


# -- the Sunday scrub ---------------------------------------------------------


def bucket_scrub(lake_root: Path, target: BucketTarget, client: Any) -> BackupScrubResult:
    """Scrub the bucket against the lake's manifest. Read-only on both sides.

    The same ping rules as ``manifest.backup_scrub``, and its findings with two exceptions.

    1. ``not_regular`` is never set, because an object is never a FIFO or a directory.
    2. ``target_missing`` is never set. A bucket that refuses or cannot be reached plays
       that part, as item 4 below says.

    1. The prefix check is :func:`read_copy_state`. A copy that is not a prefix is
       downloaded once, only then, to name the byte where it diverged.
    2. The forward pass sends one ``HeadObject`` with checksum mode per manifested
       object inside the watermark and compares the stored SHA-256 with the manifest.
    3. The reverse pass lists the bucket for orphans and unaccounted objects, skipping
       ``SCRUB_EXCLUSIONS`` as the path form does.
    4. A bucket that refuses, cannot be reached, or answers with another error is a named
       finding that withholds the ping and never raises, so the rest of the Sunday job
       still runs. The three get different names, because each sends the operator to a
       different repair, as ``_failure`` sets out.
    5. ``GetBucketVersioning`` adds a report line when versioning is anything but
       enabled. It withholds nothing, and no check on objects could see it otherwise. It
       is read first, so the line is reported even when the scrub stops early.

    **This proves less than the path scrub.** ``HeadObject`` returns the checksum S3
    stored at upload, and does not re-hash the bytes at rest. So the scrub proves each
    object is present, is the current version, and held the manifest's bytes when it
    arrived. An overwrite or a delete marker still shows. Rot at rest is the provider's
    durability guarantee plus the restore test rather than this scrub's job. Each object
    whose stored SHA-256 matched is handed to that test in ``matched``.
    """
    name = str(target)
    root = Path(lake_root)
    try:
        ledger = read_ledger(root)
    except OSError as exc:
        return BackupScrubResult(target=name, unreadable=f"{type(exc).__name__}: {exc}")
    versioning = _versioning_note(client, target)
    try:
        return _bucket_scrub(root, ledger, target, client, versioning)
    except Exception as exc:
        failure = _failure(exc)
        if failure is None:
            raise
        kind, detail = failure
        if kind == "refused":
            return BackupScrubResult(target=name, bucket_refused=detail, versioning=versioning)
        if kind == "unreachable":
            return BackupScrubResult(target=name, bucket_unreachable=detail, versioning=versioning)
        return BackupScrubResult(target=name, bucket_failed=detail, versioning=versioning)


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
    root: Path, ledger: Ledger, target: BucketTarget, client: Any, versioning: str | None
) -> BackupScrubResult:
    name = str(target)
    copy = read_copy_state(client, target, ledger.raw)
    if not copy.present:
        return BackupScrubResult(target=name, manifest_missing=True, versioning=versioning)
    length = copy.length
    if not copy.is_prefix:
        body = client.get_object(Bucket=target.bucket, Key=target.key(MANIFEST_FILE))["Body"]
        held = body.read()
        if not ledger.raw.startswith(held):
            return BackupScrubResult(
                target=name,
                manifest_diverged_at=_first_difference(ledger.raw, held),
                versioning=versioning,
            )
        length = len(held)

    mark = watermark(ledger.raw, length)
    if mark == 0 and ledger.entries:
        return BackupScrubResult(target=name, manifest_missing=True, versioning=versioning)
    path = manifest_path(root)
    copied = _latest_by_partition(ledger.entries[:mark], path)
    latest = ledger.latest

    missing: list[str] = []
    sha_mismatches: list[str] = []
    matched: list[tuple[str, str]] = []
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
        sha = str(entry.get("sha256"))
        if _matches(head, sha):
            matched.append((partition, sha))
        else:
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
        versioning=versioning,
        matched=tuple(sorted(matched)),
    )


# -- reading the bucket back --------------------------------------------------

# How much of an object one read takes. A sealed partition runs to a few hundred
# megabytes, so reading one whole would hold all of it in memory at once.
_READ_CHUNK = 1 << 20


class BucketReadError(OSError):
    """A read from the bucket that failed, as the ``OSError`` a ``BackupReader`` raises.

    ``manifest.restore_check`` catches ``OSError`` and nothing else, so the bucket's
    client errors are mapped onto it here. ``code`` keeps the S3 error code, or
    ``AssumeRole <code>`` when STS refused the role, and ``absent`` says S3 answered
    that the key does not exist, which the restore command names as a missing file
    rather than as a bucket it cannot reach.
    """

    def __init__(self, rel: str, kind: str, code: str) -> None:
        verb = {
            "refused": "refused the read",
            "unreachable": "could not be reached or was unavailable",
        }.get(kind, "answered the read with an error")
        super().__init__(f"the bucket {verb} ({code})")
        self.rel = rel
        self.kind = kind
        self.code = code
        self.absent = code in _ABSENT_CODES


def _read_failure(exc: BaseException) -> tuple[str, str] | None:
    """``_failure``, plus a ``urllib3`` error raised from inside a response body.

    ``botocore``'s ``StreamingBody.read`` wraps a read timeout and a dropped connection,
    and lets the rest of ``urllib3``'s errors through, such as an ``SSLError`` partway
    through a body. Those are neither a ``BotoCoreError`` nor an ``OSError``, so without
    this they would escape the Sunday job's ``OSError`` catch as a traceback.
    """
    failure = _failure(exc)
    if failure is not None:
        return failure
    from urllib3.exceptions import HTTPError  # lazy: only a bucket job needs it

    if isinstance(exc, HTTPError):
        return "unreachable", type(exc).__name__
    return None


def bucket_reader(client: Any, target: BucketTarget) -> Callable[[str], Iterator[bytes]]:
    """The ``manifest.BackupReader`` for a bucket: download the current version of ``rel``.

    The body is read in an explicit loop of ``read`` calls, so every byte crosses the
    network. A client error on the request or partway through the body raises
    ``BucketReadError``, which is an ``OSError``. Anything that is not a bucket failure
    is a bug and raises as itself.

    The Sunday restore test and the restore command both read through this, so the
    download the Sunday job runs every week is the one a full restore runs.
    """

    def read(rel: str) -> Iterator[bytes]:
        try:
            body = client.get_object(Bucket=target.bucket, Key=target.key(rel))["Body"]
            try:
                while chunk := body.read(_READ_CHUNK):
                    yield chunk
            finally:
                body.close()
        except Exception as exc:
            failure = _read_failure(exc)
            if failure is None:
                raise
            raise BucketReadError(rel, *failure) from exc

    return read


def usable_version_id(version: object) -> bool:
    """Whether a ``VersionId`` names a version a repair could read back.

    ``None`` is a response that carried no id. ``"null"``, in any case, is what an unversioned
    bucket returns. An empty or whitespace-only id names nothing. The trim (marketlake #787)
    and :func:`live_check` both judge an id here, so the trim never records an id the live
    check would have failed.
    """
    return isinstance(version, str) and bool(version.strip()) and version.lower() != "null"


@dataclass(frozen=True)
class CurrentDigest:
    """What :func:`current_digest` read: the current version's SHA-256 and its id.

    ``sha256`` is 64 hex characters, the form ``manifest.jsonl`` records. ``version_id`` is
    the ``VersionId`` the response carried, or ``None`` when it carried none.
    """

    sha256: str
    version_id: str | None


def current_digest(client: Any, target: BucketTarget, rel: str) -> CurrentDigest:
    """Hash the bucket's current version of ``rel`` and keep the ``VersionId`` it came with.

    The trim (marketlake #787) checks a partition's bucket copy with this before it drops the
    lake's copy. The body streams in ``_READ_CHUNK`` pieces into one SHA-256, so a partition of
    a few hundred megabytes is never held in memory whole, and it is closed when the read ends.
    Nothing is written anywhere, which keeps this module's rule that only the range restore
    writes into a live lake. A bucket failure on the request or partway through the body
    raises ``BucketReadError`` the way :func:`bucket_reader` maps it, so a missing key reads as
    ``absent`` and a lost grant as ``refused``. Anything that is not a bucket failure is a bug
    and raises as itself.

    A ``VersionId`` of ``"null"``, which an unversioned bucket returns, is passed through as
    given. Deciding what it means is the caller's, through :func:`usable_version_id`.
    """
    digest = hashlib.sha256()
    try:
        response = client.get_object(Bucket=target.bucket, Key=target.key(rel))
        body = response["Body"]
        try:
            while chunk := body.read(_READ_CHUNK):
                digest.update(chunk)
        finally:
            body.close()
    except Exception as exc:
        failure = _read_failure(exc)
        if failure is None:
            raise
        raise BucketReadError(rel, *failure) from exc
    version = response.get("VersionId")
    return CurrentDigest(
        sha256=digest.hexdigest(), version_id=None if version is None else str(version)
    )


def read_bucket_trimmed(
    client: Any, target: BucketTarget, bucket_latest: Mapping[str, Mapping]
) -> Mapping[str, dict]:
    """The bucket's ``trimmed.jsonl``, verified and resolved to each partition's latest line.

    ``bucket_latest`` is the latest entry per partition of the bucket's own ``manifest.jsonl``,
    and its entry for ``trimmed.jsonl`` decides everything here. With no entry, the bucket's
    manifest records no trim, so every trimmed partition is still in the bucket, and the answer
    is an empty mapping with no request sent. Otherwise the current version is downloaded, its
    SHA-256 must equal the entry's, and it is parsed by ``trimmed.parse_trimmed`` and resolved by
    ``trimmed.latest_by_partition``, the reader the lake's own copy goes through. A copy whose
    hash matches carries no torn tail, because ``trimmed`` never records an entry over one.

    Three things refuse, each with :class:`BucketLedgerRefused`.

    1. A key S3 answers as absent, as :class:`BucketLedgerMissing`.
    2. Bytes that do not hash to the entry, as :class:`BucketLedgerMismatch` carrying the
       entry's SHA-256.
    3. Bytes that match and do not parse, naming the damage.

    Every other bucket failure, such as a refused role, a 403 or a 5xx, raises as itself, so the
    command names it as it names every transport failure rather than as a damaged ledger. The
    download is joined inside the ``try``, because ``bucket_reader`` sends the request on the
    first iteration rather than when it is called.

    The restore calls this, and marketlake #832's resync, still open, is to call it too, so the
    bucket's ledger is judged one way wherever a designed absence is decided against it.
    """
    entry = bucket_latest.get(TRIMMED_FILE)
    if entry is None:
        return {}
    expected = str(entry.get("sha256"))
    read = bucket_reader(client, target)
    try:
        raw = b"".join(read(TRIMMED_FILE))
    except BucketReadError as exc:
        if not exc.absent:
            raise
        raise BucketLedgerMissing(
            f"the bucket holds no current {TRIMMED_FILE}, which its manifest.jsonl records with "
            f"SHA-256 {expected}, so no partition trimmed on purpose can be told from one lost: "
            f"{target}"
        ) from None
    if hashlib.sha256(raw).hexdigest() != expected:
        raise BucketLedgerMismatch(
            f"the bucket's current {TRIMMED_FILE} does not match the SHA-256 its manifest.jsonl "
            f"records, {expected}, so no partition trimmed on purpose can be told from one lost: "
            f"{target}",
            expected,
        )
    try:
        return latest_by_partition(parse_trimmed(raw, TRIMMED_FILE), TRIMMED_FILE)
    except ManifestError as exc:
        raise BucketLedgerRefused(
            f"the bucket's {TRIMMED_FILE} matches its manifest entry and cannot be read ({exc}), "
            f"so no partition trimmed on purpose can be told from one lost: {target}"
        ) from None


# -- the restore command ------------------------------------------------------
#
# ``python -m lake.bucket restore <dest>`` rebuilds a lake from the bucket into an empty
# directory. A backup that has never been restored from is a hypothesis, and the owner's
# switch of ``backup_target`` to the bucket waits on one full restore passing. marketlake
# #640 is the plan, and the design's Backup section carries the reasoning.
#
# The download lands in a hidden working directory inside the destination,
# ``<dest>/.marketlake-restoring``, so it is always on the destination's own filesystem,
# even when the destination is a volume's mount point. That is the case a new host
# meets: marketlake #686 mounts the VM's lake volume at ``lake_root`` itself. Only after
# every file verifies are the working directory's entries moved up into the destination
# by rename, ``manifest.jsonl`` last, so a lake appears at the destination only once
# everything else is in place. A run that fails partway leaves no ``manifest.jsonl``
# there, and a second run resumes in the working directory.

# The working directory's name inside the destination.
RESTORE_WORK_DIR = ".marketlake-restoring"

# The file that marks a working directory as one a restore made. A run refuses a
# non-empty working directory without it, so a restore never prunes files it did not
# write.
RESTORE_MARKER = ".marketlake-restore"

# The file that says every file in the working directory verified. A run that finds it
# finishes moving the files into the destination and downloads nothing.
VERIFIED_MARKER = ".marketlake-verified"

# The suffix a file carries while its download is in flight. It is renamed onto its own
# name only once its bytes have hashed to what they must.
_PART_SUFFIX = ".part"

# Top-level names a bucket key may not restore to, because the restore itself uses them,
# compared case-folded. macOS's filesystem ignores case, so ``Manifest.jsonl`` there is
# the same file as ``manifest.jsonl``. The exact ``manifest.jsonl`` is the bucket's own
# copy and is handled before this check.
_RESERVED = frozenset(
    name.casefold()
    for name in (RESTORE_MARKER, VERIFIED_MARKER, RESTORE_WORK_DIR, LOST_AND_FOUND, MANIFEST_FILE)
)


class RestoreRefused(BucketRefusal):
    """The restore command refused to start, or stopped, with one line for an operator."""


def _free_bytes(path: Path) -> int:
    """The free space on the filesystem holding ``path``, in bytes."""
    return shutil.disk_usage(path).free


@dataclass
class RestoreSummary:
    """What one restore run did, for the lines the command prints.

    ``failures`` holds ``(rel, why)`` for every file that failed verification, was
    missing, or named a path outside the lake. Any one of them leaves the destination
    without a lake. ``restored`` says whether the files were moved into the destination.
    ``finished_move`` says the run found a download that had already verified and only
    finished moving it. ``unrecorded`` names each restored file the restored manifest
    does not record, outside the scrub's exclusion set. A torn last manifest line
    leaves one behind. Each was verified against the checksum S3 stored at upload, so it
    is named rather than failed.

    Two fields count what a restore that skips designed absences left out, and stay empty in a
    whole-lake restore. ``trimmed_left_out`` counts the listed keys left out because the
    bucket's ``trimmed.jsonl`` says the lake removed them on purpose. ``trimmed_lost`` names each
    designed absence the bucket no longer holds. The rebuilt lake never needed that file, so it
    is named rather than failed, and the Sunday bucket scrub reports the loss every week.
    """

    target: str
    dest: Path
    work: Path
    files: int = 0
    downloaded: int = 0
    downloaded_bytes: int = 0
    resumed: int = 0
    segments_left_out: int = 0
    trimmed_left_out: int = 0
    trimmed_lost: list[str] = field(default_factory=list)
    unrecorded: list[str] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)
    restored: bool = False
    finished_move: bool = False

    def render(self) -> str:
        """One line naming what came down and where it went."""
        if self.finished_move:
            return (
                f"finished moving a restore that had already verified every file into {self.dest}"
            )
        megabytes = self.downloaded_bytes / 1_000_000
        # The counts of designed absences show only when nonzero, so a whole-lake restore's
        # line reads exactly as it did before they existed.
        trimmed = ""
        if self.trimmed_left_out:
            trimmed += f"{self.trimmed_left_out} partition(s) trimmed on purpose left out, "
        if self.trimmed_lost:
            trimmed += (
                f"{len(self.trimmed_lost)} partition(s) trimmed on purpose and missing from "
                "the bucket, "
            )
        return (
            f"restored {self.files} file(s) from {self.target} into {self.dest}: downloaded "
            f"{self.downloaded} ({megabytes:.1f} MB), {self.resumed} already verified in the "
            f"working directory, {trimmed}{self.segments_left_out} compacted journal segment(s) "
            "left out"
        )


def _unsafe(rel: str) -> bool:
    """Whether a bucket key's lake-relative path could write outside the working directory.

    The key comes from the bucket, which a person or another tool can write to. An
    absolute path replaces the base it is joined to, and a ``..`` part climbs out of it,
    so either would land a file anywhere the process can write. An empty or ``.`` part
    names no file of the lake's. A NUL byte names no file at all. A top-level name the
    restore uses for itself is refused too, in any case, so a key cannot overwrite a
    marker or the manifest on a filesystem that ignores case.
    """
    parts = rel.split("/")
    if "\x00" in rel or rel.startswith("/") or any(part in ("", ".", "..") for part in parts):
        return True
    return parts[0].casefold() in _RESERVED


def _inside(work: Path, rel: str) -> bool:
    """Whether ``work / rel`` resolves to a path under ``work``, symlinks followed."""
    return (work / rel).resolve().is_relative_to(work.resolve())


def _local(action: str, exc: OSError) -> str:
    return f"{action} failed ({type(exc).__name__}: {exc.strerror or exc})"


def _destination_state(dest: Path) -> str:
    """Refuse a destination a restore must not write into, or say how this run starts.

    It returns ``"move"`` when the working directory holds a download that already
    verified, so the run only finishes moving it, and ``"download"`` otherwise.

    1. A symbolic link is refused, because the files would land wherever it points.
    2. A destination that exists and is not a directory is refused.
    3. An absent destination needs an existing parent, and is created later.
    4. A directory counts as empty when it holds nothing but ``lost+found`` and the
       working directory. Anything else refuses, which keeps a restore off a live lake.
    5. A working directory with files in it and no marker was not made by a restore,
       and is refused rather than pruned.
    """
    if dest.is_symlink():
        raise RestoreRefused(
            f"{dest} is a symbolic link. Name the directory it points to, so nothing was restored"
        )
    if dest.exists() and not dest.is_dir():
        raise RestoreRefused(f"{dest} is not a directory, so nothing was restored")
    if not dest.exists():
        if not dest.parent.is_dir():
            raise RestoreRefused(f"{dest.parent} does not exist, so nothing was restored")
        return "download"
    work = dest / RESTORE_WORK_DIR
    if work.is_dir() and (work / VERIFIED_MARKER).is_file():
        return "move"
    others = sorted(set(os.listdir(dest)) - {LOST_AND_FOUND, RESTORE_WORK_DIR})
    if (
        others
        and (dest / MANIFEST_FILE).is_file()
        and work.is_dir()
        and set(os.listdir(work)) <= {RESTORE_MARKER}
    ):
        # A run killed after the last file moved in, while it removed its markers.
        raise RestoreRefused(
            f"{dest} already holds a restored lake, and {work} is what that finished "
            "restore left behind. Remove that directory, since nothing is left to restore"
        )
    if others:
        raise RestoreRefused(
            f"{dest} is not empty, it holds {others[0]}. A restore writes only into an "
            "empty directory, which keeps it off a live lake, so nothing was restored"
        )
    if work.exists() and not work.is_dir():
        raise RestoreRefused(f"{work} is not a directory, so nothing was restored")
    if work.is_dir() and any(work.iterdir()) and not (work / RESTORE_MARKER).is_file():
        raise RestoreRefused(
            f"{work} holds files and no restore made it, so nothing was restored. Move it "
            "aside and run the restore again"
        )
    return "download"


def _prepare_work(dest: Path, work: Path) -> None:
    """Create the destination and the working directory, and mark the working directory.

    An empty working directory with no marker is what a run killed between the two
    steps leaves, so it is marked and used rather than refused.
    """
    dest.mkdir(exist_ok=True)
    work.mkdir(exist_ok=True)
    marker = work / RESTORE_MARKER
    if not marker.is_file():
        marker.write_text("a marketlake restore in progress\n")


def _stored_hex(client: Any, target: BucketTarget, rel: str) -> str | None:
    """The SHA-256 S3 stored for ``rel`` at upload, as hex, or ``None`` when it proves nothing.

    A key that does not exist raises ``BucketReadError`` with ``absent`` set. Any other
    bucket failure raises as itself, for the command to name as one line.
    """
    try:
        head = client.head_object(Bucket=target.bucket, Key=target.key(rel), ChecksumMode="ENABLED")
    except Exception as exc:
        if _is_absent(exc):
            raise BucketReadError(rel, "failed", _error_code(exc) or "404") from exc
        raise
    stored = stored_sha256(head)
    return None if stored is None else stored.hex()


def _part(path: Path) -> Path:
    return path.with_name(path.name + _PART_SUFFIX)


def _download_to(read: Callable[[str], Iterator[bytes]], rel: str, part: Path) -> tuple[str, int]:
    """Stream ``rel`` into the in-flight file ``part``, and return its hex SHA-256 and its size.

    The caller names the in-flight file, because its two callers name it differently. The
    ``restore`` command writes ``<name>.part`` inside its own working directory. The range
    restore writes into the live lake, where ``.part`` sits in neither the backup's nor the
    scrub's exclusions, so it writes ``paths.temp_write_path``'s name, which the backup leaves
    out. ``part``'s directory is created when it is missing, which brings back a ticker
    directory an empty-directory pass removed.

    The file is left for the caller to rename or remove. A read failure raises
    ``BucketReadError`` and a write failure raises a plain ``OSError``, and the caller
    tells the two apart by type.
    """
    part.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with open(part, "wb") as handle:
        for chunk in read(rel):
            digest.update(chunk)
            size += len(chunk)
            handle.write(chunk)
    return digest.hexdigest(), size


def _prune(work: Path, keep: set[str]) -> None:
    """Remove every file in the working directory outside ``keep``, then empty directories.

    A run that died leaves a part file behind. A resumed run whose bucket moved on can
    find a file the newer manifest no longer wants, such as a journal segment whose day
    has since been compacted. Neither may reach the destination. The marker stays, so a
    run killed after this still finds a working directory a restore made.
    """
    for current, _dirs, files in os.walk(work, topdown=False):
        here = Path(current)
        for name in files:
            rel = (here / name).relative_to(work).as_posix()
            if rel not in keep and rel != RESTORE_MARKER:
                (here / name).unlink()
        if here != work and not any(here.iterdir()):
            here.rmdir()


def _write_verified(work: Path, plan: Iterable[str]) -> None:
    """Mark the working directory verified, recording each file it must hold and its size.

    The finishing run checks against this list rather than re-hashing, because a year-end
    lake is about 154 GB. Sizes catch a file removed or truncated between the runs. The
    manifest is listed too, so a working directory that lost it never moves in a lake
    with no ledger.
    """
    files = {rel: (work / rel).stat().st_size for rel in [*plan, MANIFEST_FILE]}
    (work / VERIFIED_MARKER).write_text(json.dumps({"files": files}, sort_keys=True) + "\n")


def _check_before_move(work: Path, dest: Path) -> None:
    """Refuse to move anything when the destination or the working directory changed.

    Three changes refuse, each before the first rename.

    1. The destination gained a ``manifest.jsonl`` while the working directory still
       holds its own. ``lake_lock`` creates one when a daemon starts on that root, so
       something is using the destination as a lake, and moving in would replace it.
    2. The destination holds a name the working directory is about to move in, compared
       case-folded for a filesystem that ignores case. A rename would replace an empty
       directory of that name and fail on a full one.
    3. A file the verified marker lists is no longer at its recorded size, either in the
       working directory or already moved in. Moving the rest would land a lake whose
       manifest records a file it does not hold.
    """
    staged = {name.casefold(): name for name in os.listdir(work)}
    for marker in (RESTORE_MARKER, VERIFIED_MARKER):
        staged.pop(marker.casefold(), None)
    present = {
        name.casefold(): name
        for name in os.listdir(dest)
        if name not in (LOST_AND_FOUND, RESTORE_WORK_DIR)
    }
    if MANIFEST_FILE in staged and MANIFEST_FILE in present:
        raise RestoreRefused(
            f"{dest} gained a {present[MANIFEST_FILE]} while the verified restore waited in "
            f"{work}, so something is using it as a lake and nothing was moved. Stop what "
            f"writes there, remove {dest / present[MANIFEST_FILE]} if it holds nothing worth "
            "keeping, and run the restore again"
        )
    clashes = sorted(present[name] for name in staged.keys() & present.keys())
    if clashes:
        raise RestoreRefused(
            f"{dest} holds {clashes[0]}, which the restore is about to move in from {work}, "
            "so nothing was moved. Move it aside and run the restore again"
        )
    try:
        files = json.loads((work / VERIFIED_MARKER).read_text())["files"]
        gaps = [
            rel
            for rel, size in sorted(files.items())
            if not any(
                (root / rel).is_file() and (root / rel).stat().st_size == size
                for root in (work, dest)
            )
        ]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        gaps = [VERIFIED_MARKER]
    if gaps:
        raise RestoreRefused(
            f"{work} no longer holds {gaps[0]} as it verified, so nothing was moved. Delete "
            f"{work} and run the restore again"
        )


def _finish(work: Path, dest: Path) -> None:
    """Move every verified entry up into the destination, ``manifest.jsonl`` last.

    Each top-level entry is one rename on one filesystem, so a run killed partway leaves
    every entry either in the working directory or in the destination, and the next run
    moves what is left. ``manifest.jsonl`` goes last, so the destination holds a lake
    only once everything it records is in place. The markers go after it.
    """
    skip = {name.casefold() for name in (RESTORE_MARKER, VERIFIED_MARKER, MANIFEST_FILE)}
    for name in sorted(os.listdir(work)):
        if name.casefold() not in skip:
            os.rename(work / name, dest / name)
    if (work / MANIFEST_FILE).exists():
        os.rename(work / MANIFEST_FILE, dest / MANIFEST_FILE)
    (work / RESTORE_MARKER).unlink(missing_ok=True)
    (work / VERIFIED_MARKER).unlink(missing_ok=True)
    work.rmdir()


def _finish_or_refuse(work: Path, dest: Path) -> None:
    _check_before_move(work, dest)
    try:
        _finish(work, dest)
    except OSError as exc:
        raise RestoreRefused(
            f"every file verified and {_local(f'moving them from {work} into {dest}', exc)}. "
            "Fix that and run the restore again, which finishes the move"
        ) from None


def _restore_trimmed(
    client: Any,
    target: BucketTarget,
    latest: Mapping[str, Mapping],
    manifest_raw: bytes,
    dest: Path,
    work: Path,
) -> Mapping[str, dict]:
    """The bucket's trimmed ledger for a restore that skips designed absences.

    A ``trimmed.jsonl`` already in the working directory whose hash matches the bucket
    manifest's entry is parsed in place of the bucket's, and no request is sent, as a matching
    working file is resumed for every other download. That is the repair for a bucket whose
    current ledger is newer than its manifest's entry, which a nightly upload stopped between
    the two leaves. A working copy that does not match is ignored.

    Otherwise :func:`read_bucket_trimmed` reads the bucket's, and each of its refusals becomes a
    ``RestoreRefused`` naming the repair. A mismatch first creates the marked working directory
    and writes the bucket's ``manifest.jsonl`` into it, so the operator has the entry to check
    a download against and the next run finds a working directory a restore made. The call
    sits outside the download loop's ``except OSError``, which would read a ``BucketReadError``
    as the local disk's.
    """
    entry = latest.get(TRIMMED_FILE)
    working = work / TRIMMED_FILE
    if entry is not None:
        try:
            held = working.read_bytes() if working.is_file() else None
        except OSError as exc:
            raise RestoreRefused(
                _local(f"reading {TRIMMED_FILE} in {work}", exc) + ", so nothing was restored"
            ) from None
        if held is not None and hashlib.sha256(held).hexdigest() == str(entry.get("sha256")):
            try:
                return latest_by_partition(parse_trimmed(held, working), working)
            except ManifestError as exc:
                raise RestoreRefused(
                    f"{exc} It matches the bucket manifest's entry, so every copy of that "
                    "version is damaged the same way, and nothing was restored"
                ) from None
    try:
        return read_bucket_trimmed(client, target, latest)
    except BucketLedgerMismatch as exc:
        try:
            _prepare_work(dest, work)
        except OSError as failed:
            raise RestoreRefused(
                _local(f"creating {work}", failed) + ", so nothing was restored"
            ) from None
        try:
            (work / MANIFEST_FILE).write_bytes(manifest_raw)
        except OSError as failed:
            raise RestoreRefused(
                _local(f"writing manifest.jsonl into {work}", failed) + ", so nothing was restored"
            ) from None
        raise RestoreRefused(
            f"{exc}. Nothing was restored. Download the version of {TRIMMED_FILE} whose SHA-256 "
            f"is {exc.sha256} from the console, put it at {working}, and run the restore again, "
            'as the README\'s "When the lake is gone" steps describe'
        ) from None
    except BucketLedgerMissing as exc:
        raise RestoreRefused(
            f"{exc}. Nothing was restored. Put back the version whose SHA-256 matches that entry "
            "as the current one, as the README's \"Putting a version back for the range "
            'restore" steps describe, and run the restore again'
        ) from None
    except BucketLedgerRefused as exc:
        raise RestoreRefused(f"{exc}. Nothing was restored") from None


def _reserve_refusal(
    dest: Path, *, needed: int, busiest: int, free: int, short: int, windowed: bool
) -> str:
    """The line a restore refuses with when it would leave less than the journal reserve free.

    It names the plan, the reserve, the free space and the shortfall. A restore that skips
    designed absences runs on a host that keeps a window, which is the hosted VM, so its line
    also names the VM's fix.
    """
    reserve = runway.JOURNAL_RESERVE_SESSIONS * busiest
    line = (
        f"the restore needs {needed / 1_000_000:.1f} MB, and the next session's journal needs "
        f"a reserve of {reserve / 1_000_000:.1f} MB beside it, "
        f"{runway.JOURNAL_RESERVE_SESSIONS} times the busiest sealed day in the bucket "
        f"({busiest / 1_000_000:.1f} MB). The filesystem holding {dest} has "
        f"{free / 1_000_000:.1f} MB free, {short / 1_000_000:.1f} MB short of the reserve, so "
        "nothing was restored. Free that much or use a larger filesystem"
    )
    if windowed:
        line += (
            ". On the hosted VM, raise lake_volume_gib, apply the infrastructure, and rerun the "
            "bootstrap so resize2fs grows the filesystem. A whole-lake restore needs more room, "
            "not less"
        )
    return line


def restore_lake(
    dest: Path | str,
    target: BucketTarget,
    *,
    client: Any,
    free_space: Callable[[Path], int] = _free_bytes,
    skip_designed_absences: bool = False,
) -> RestoreSummary:
    """Restore the bucket's current versions into the empty directory ``dest``.

    ``skip_designed_absences`` chooses what comes back. Left false, the default, the whole
    lake is restored. Set, the restore rebuilds a lake that keeps only a window of sessions,
    leaving out each partition the bucket's ``trimmed.jsonl`` says that lake removed on
    purpose (marketlake #785). ``main`` sets it on a host whose config sets
    ``lake_window_sessions``. Every other caller passes it or takes the whole-lake default,
    and none reads a config.

    The steps, in order.

    1. ``dest`` must be absent, or a directory holding nothing but ``lost+found`` and
       the working directory, and not a symbolic link. Anything else refuses before a
       request is sent. A working directory whose files already verified skips to step 7.
    2. The bucket's ``manifest.jsonl`` is downloaded into memory and checked against the
       SHA-256 S3 stored for it. The latest entry per partition is read from it, with a
       torn last line discarded the way every reader discards one. When skipping designed
       absences, the bucket's ``trimmed.jsonl`` is read too, by :func:`read_bucket_trimmed`,
       unless a copy matching its manifest entry already waits in the working directory.
       A missing, damaged or mismatched ledger refuses before any data file. A mismatch
       first leaves a marked working directory holding the bucket's ``manifest.jsonl``, and
       its refusal names the SHA-256 a copy put there by hand has to match.
    3. The plan is every object under the target, less each journal segment whose
       compacted partition the manifest records, and less S3's zero-byte folder
       markers. The bucket never deletes, so it can hold the segments of a day
       compacted after they uploaded, and the restore leaves them out by the scrub's own
       rule, ``_compacted_partition_for_segment``. When skipping designed absences, each key
       that ``trimmed.is_designed_absence`` calls designed against the bucket's two ledgers
       is left out too. A key whose path would land outside the working directory is never
       written and is named as a failure. A manifested file the bucket does not hold is a
       failure named missing, unless it is a designed absence being skipped. That one is
       named in ``trimmed_lost`` and fails nothing, since the rebuilt lake never needed it.
    4. The free space on the destination's filesystem must cover every planned byte not
       already verified in the working directory, or the command refuses before the
       first data file. What is left after those bytes must then cover the journal
       reserve, by ``runway.reserve_shortfall`` with the busiest sealed day in the bucket's
       listing, or the command refuses the same way. The next session's journal lands on
       this filesystem, and a lake that cannot seal its next close loses captured minutes.
    5. Each planned file streams into ``<dest>/.marketlake-restoring`` and is hashed as
       it arrives. A manifested file must match its latest entry. Any other file must
       match the SHA-256 S3 stored at upload, since the manifest has no entry for it. A
       file already there with the right hash is not downloaded again.
    6. Any failure stops here, names the file, and leaves no ``manifest.jsonl`` at
       ``dest``.
    7. Otherwise the working directory is pruned to the plan, marked verified, and its
       entries are moved up into ``dest``, ``manifest.jsonl`` last.

    Nothing takes the lake-root lock. ``lake_lock`` creates ``manifest.jsonl`` under the
    root it is handed, and a restore writes into a directory nothing else uses. A client
    error other than a missing key stops the run and raises, and a local filesystem
    failure stops it with a ``RestoreRefused``. Either way the working directory keeps
    every file that verified for the next run.
    """
    dest = Path(os.path.abspath(Path(dest).expanduser()))
    work = dest / RESTORE_WORK_DIR
    summary = RestoreSummary(target=str(target), dest=dest, work=work)
    if _destination_state(dest) == "move":
        _finish_or_refuse(work, dest)
        summary.finished_move = True
        summary.restored = True
        return summary
    read = bucket_reader(client, target)

    listing = list_bucket(client, target)
    if MANIFEST_FILE not in listing:
        raise RestoreRefused(
            f"the bucket holds no manifest.jsonl, so there is no lake to restore: {target}"
        )
    manifest_sha = _stored_hex(client, target, MANIFEST_FILE)
    if manifest_sha is None:
        raise RestoreRefused(
            "S3 stores no full-object SHA-256 for the bucket's manifest.jsonl, so nothing it "
            f"records can be verified and nothing was restored: {target}"
        )
    raw = b"".join(read(MANIFEST_FILE))
    if hashlib.sha256(raw).hexdigest() != manifest_sha:
        raise RestoreRefused(
            "the bucket's manifest.jsonl does not match the SHA-256 S3 stored for it, so "
            f"nothing it records can be verified and nothing was restored: {target}"
        )
    try:
        latest = _latest_by_partition(
            parse_jsonl(raw.decode("utf-8", "replace")), Path(MANIFEST_FILE)
        )
    except ManifestError as exc:
        raise RestoreRefused(f"the bucket's {exc}, so nothing was restored") from None
    trimmed_latest: Mapping[str, Mapping] = (
        _restore_trimmed(client, target, latest, raw, dest, work) if skip_designed_absences else {}
    )

    def superseded(rel: str) -> bool:
        compacted = _compacted_partition_for_segment(rel)
        return compacted is not None and compacted in latest

    def designed(rel: str) -> bool:
        return skip_designed_absences and is_designed_absence(rel, latest, trimmed_latest)

    plan: dict[str, str | None] = {}
    for rel in sorted(listing):
        if rel == MANIFEST_FILE or (rel.endswith("/") and listing[rel] == 0):
            # A zero-byte key ending in "/" is the folder marker the S3 console writes.
            # It names no file, and written as one it would block a directory's name.
            continue
        if _unsafe(rel) or not _inside(work, rel):
            summary.failures.append((rel, "names a path outside the lake, so it was not written"))
            continue
        if superseded(rel):
            summary.segments_left_out += 1
            continue
        if designed(rel):
            summary.trimmed_left_out += 1
            continue
        plan[rel] = str(latest[rel]["sha256"]) if rel in latest else None
    by_case: dict[str, list[str]] = {}
    for rel in plan:
        by_case.setdefault(rel.casefold(), []).append(rel)
    for same in by_case.values():
        for rel in same[1:]:
            summary.failures.append(
                (
                    rel,
                    f"differs from {same[0]} only by case, so one would overwrite the other "
                    "on a filesystem that ignores case",
                )
            )
    for rel in sorted(latest):
        if rel not in listing and not superseded(rel):
            if designed(rel):
                summary.trimmed_lost.append(rel)
                continue
            summary.failures.append((rel, "missing from the bucket"))

    needed = len(raw) + sum(
        listing[rel]
        for rel in plan
        if not ((work / rel).is_file() and (work / rel).stat().st_size == listing[rel])
    )
    free = free_space(dest if dest.is_dir() else dest.parent)
    if free < needed:
        raise RestoreRefused(
            f"the restore needs {needed / 1_000_000:.1f} MB and the filesystem holding "
            f"{dest} has {free / 1_000_000:.1f} MB free, so nothing was restored"
        )
    busiest = runway.listing_busiest_sealed_day(listing)
    short = runway.reserve_shortfall(free=free, planned=needed, busiest_sealed_day=busiest)
    if short:
        raise RestoreRefused(
            _reserve_refusal(
                dest,
                needed=needed,
                busiest=busiest,
                free=free,
                short=short,
                windowed=skip_designed_absences,
            )
        )

    try:
        _prepare_work(dest, work)
    except OSError as exc:
        raise RestoreRefused(
            _local(f"creating {work}", exc) + ", so nothing was restored"
        ) from None
    for rel, entry_sha in plan.items():
        path = work / rel
        try:
            expected = entry_sha if entry_sha is not None else _stored_hex(client, target, rel)
        except BucketReadError:
            summary.failures.append((rel, "missing from the bucket"))
            continue
        if expected is None:
            summary.failures.append((rel, "has no SHA-256 stored in the bucket to verify"))
            continue
        try:
            held = sha256_file(path) if path.is_file() else None
        except OSError as exc:
            raise RestoreRefused(
                _local(f"reading {rel} in {work}", exc) + f". Nothing was moved into {dest}"
            ) from None
        if held == expected:
            summary.resumed += 1
            continue
        try:
            actual, size = _download_to(read, rel, _part(path))
        except BucketReadError as exc:
            if not exc.absent:
                raise
            _part(path).unlink(missing_ok=True)
            summary.failures.append((rel, "missing from the bucket"))
            continue
        except OSError as exc:
            # ``bucket_reader`` turns every bucket failure into ``BucketReadError``, a
            # timeout included, so an ``OSError`` that reaches here is the local disk's.
            raise RestoreRefused(
                _local(f"writing {rel} into {work}", exc)
                + f". Nothing was moved into {dest}, and a re-run resumes in {work}"
            ) from None
        if actual != expected:
            _part(path).unlink()
            summary.failures.append((rel, "does not match its SHA-256"))
            continue
        os.replace(_part(path), path)
        summary.downloaded += 1
        summary.downloaded_bytes += size

    summary.files = len(plan) + 1
    summary.unrecorded = [
        rel for rel in plan if rel not in latest and not _is_excluded(rel, SCRUB_EXCLUSIONS)
    ]
    try:
        # The manifest is written even when a file failed, so the operator can read the
        # entry a failing file was checked against. It is still only in the working
        # directory, so the destination holds no lake.
        (work / MANIFEST_FILE).write_bytes(raw)
    except OSError as exc:
        raise RestoreRefused(
            _local(f"writing manifest.jsonl into {work}", exc) + f". Nothing was moved into {dest}"
        ) from None
    if summary.failures:
        return summary
    try:
        _prune(work, {*plan, MANIFEST_FILE})
        _write_verified(work, plan)
    except OSError as exc:
        raise RestoreRefused(
            _local(f"finishing {work}", exc) + f". Nothing was moved into {dest}"
        ) from None
    _finish_or_refuse(work, dest)
    summary.restored = True
    return summary


# -- the range restore --------------------------------------------------------
#
# ``python -m lake.bucket restore-range`` puts chosen chains or quotes partitions back into the
# live lake: a partition lost by accident, or every partition trimmed on purpose when the lake
# rolls back to keeping everything (marketlake #784, #755). The ``restore`` command above stays
# off a live lake on purpose. This one writes into it, so it keeps the rules every lake writer
# keeps, and the design's Backup section carries the reasoning.

# The writer the trimmed ledger's manifest entry names when the range restore records it.
RANGE_RESTORE_SOURCE = "range-restore"

# The range restore's refusal on a shadow host. A shadow's lake is compared with the primary's
# and then discarded, so a partition restored into it would be thrown away with it.
RANGE_RESTORE_SHADOW = (
    "this host's role is shadow, whose lake is compared with the primary's and then "
    "discarded, so the range restore writes nothing into it. Run it on the primary"
)


class RangeRestoreRefused(BucketRefusal):
    """The range restore refused to start, or stopped, with one line for an operator."""


@dataclass
class RangeRestoreSummary:
    """What one range restore did, for the line the command prints.

    ``selected`` counts the manifested partitions the selector matched. Each one ends the run
    either ``restored``, downloaded and moved into the lake, or ``present``, already on disk
    with its manifest sha. ``restore_lines`` counts the restore lines written, both for a
    partition this run restored and for a present one whose latest trimmed line was still a
    trim line. ``ledger_repaired`` says the trimmed ledger's manifest entry was re-recorded at
    the start, and ``temps_removed`` names each leftover temp file removed beside a target.
    """

    target: str
    selected: int = 0
    restored: int = 0
    restored_bytes: int = 0
    present: int = 0
    restore_lines: int = 0
    ledger_repaired: bool = False
    temps_removed: list[str] = field(default_factory=list)

    def render(self) -> str:
        """One line naming what came back and what was already there."""
        megabytes = self.restored_bytes / 1_000_000
        return (
            f"restored {self.restored} of {self.selected} selected partition(s) from "
            f"{self.target} ({megabytes:.1f} MB), {self.present} already present, "
            f"{self.restore_lines} restore line(s) written, "
            f"{len(self.temps_removed)} leftover temp file(s) removed"
            + (
                ", and the trimmed ledger's manifest entry re-recorded"
                if self.ledger_repaired
                else ""
            )
        )


def _fsync_path(path: Path) -> None:
    """Flush a file's or a directory's bytes to the device, so a crash cannot lose them."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _leftover_temps(path: Path) -> list[Path]:
    """Every temp file an atomic write to ``path`` could have left beside it.

    ``paths.temp_write_path`` names it ``<name>.tmp-<pid>``. The directory is listed rather than
    globbed, because a ticker is free text to a glob and ``[`` in one would match other names.
    """
    prefix = f"{path.name}{TEMP_MARKER}"
    try:
        names = os.listdir(path.parent)
    except FileNotFoundError:
        return []
    return [path.parent / name for name in sorted(names) if name.startswith(prefix)]


def restore_range(
    lake_root: Path | str,
    target: BucketTarget,
    *,
    client: Any,
    clock: Clock,
    calendar: Calendar,
    surface: str,
    ticker: str | None,
    first: date,
    last: date,
    free_space: Callable[[Path], int] = _free_bytes,
) -> RangeRestoreSummary:
    """Restore the manifested ``surface`` partitions from ``first`` to ``last`` into the live lake.

    The selector is every latest manifest entry that ``paths.parse_partition_rel`` reads as
    ``surface``, for ``ticker`` or for every ticker when it is ``None``, dated ``first`` to
    ``last`` inclusive. Each selected partition is either present with its manifest sha,
    which is left as it is, or absent, which is downloaded. Both a partition trimmed on purpose
    and one lost by accident are served. A restore line goes to ``trimmed.jsonl`` only when the
    partition's latest trimmed line is a trim line, since a lost partition has nothing there to
    supersede.

    The steps, in order.

    1. Refusals that need no lock: a surface other than ``chains`` or ``quotes``, a range that
       ends before it starts, the Sunday scrub window, a ``now`` inside a session or its
       pre-open margin by ``session_bound``, and a lake root holding no ``manifest.jsonl``,
       since ``lake_lock`` would create one under a wrong ``lake_root``.
    2. The repairs of what a crashed earlier run left. Under ``lake_lock``,
       ``trimmed.repair_trimmed_entry`` re-records a ledger whose entry lags an append, and a
       leftover temp file beside a selected target is removed. Then, unlocked, every selected
       present file is hashed. One whose sha differs from its entry refuses before anything is
       written, and one whose latest trimmed line is still a trim line owes a restore line,
       which a crash after the move and before the line leaves. Those lines are written under
       the lock after a re-check that hashes nothing, because capture's close+5 fill waits on
       the lock with no timeout and a rollback's present files run to gigabytes. This repair
       stays inside the selection, because a trim that crashed between its line and its
       unlink leaves the same state, and the next trim finishes that unlink.
    3. Unlocked, every partition to download must be in the bucket's listing, and the free
       space left after the download must cover the journal reserve, by
       ``runway.reserve_shortfall``. Its basis is ``runway.busiest_sealed_day`` over this
       lake's walk with each planned partition's size added to its day, since a restored day
       inside the growth window can become the busiest one. A lake walk that could not read
       every path refuses, since an unread day could be the busiest.
    4. Each partition is downloaded into ``paths.temp_write_path``'s name beside its target,
       outside the lock, and must hash to its latest manifest sha. The lock is then taken for
       that partition alone: the target must still be absent and its entry unchanged, the file
       is renamed into place and flushed with its directory, and the restore line goes through
       ``trimmed.append_trimmed``, which reads it back and refreshes the ledger's entry. A crash
       between any two of those leaves a state step 2 repairs on the next run.

    The Sunday window and the session bound are checked again after every lock is taken and
    before every download, so a run begun in the evening stops before either. Whatever stops a
    partition, its temp file is removed.

    Every refusal raises ``RangeRestoreRefused`` with one line naming the repair. A refusal
    after some partitions were committed says how many, since each stands on its own and a
    re-run picks up the rest.
    """
    from lake.trimmed import (
        KIND_FIELD,
        TRIM_KIND,
        VERSION_ID_FIELD,
        TrimmedRepairRefused,
        append_trimmed,
        latest_trimmed,
        repair_trimmed_entry,
        restore_line,
    )

    root = Path(lake_root)
    summary = RangeRestoreSummary(target=str(target))
    scope = (
        f"{surface} partitions for {ticker if ticker is not None else 'every ticker'} "
        f"from {first.isoformat()} to {last.isoformat()}"
    )

    def refuse(why: str) -> RangeRestoreRefused:
        done = (
            f" {summary.restored} partition(s) were restored before this stop, and a re-run "
            "picks up the rest."
            if summary.restored
            else ""
        )
        return RangeRestoreRefused(f"{why}.{done}")

    def guard() -> None:
        now = clock.now()
        if in_sunday_scrub_window(now):
            raise refuse(
                "the range restore does not run on Sunday from 19:55 to 23:30, while the Sunday "
                "job may be scrubbing the lake, since a download in flight reads to the scrub as "
                "an orphan. Run it on another evening after the 18:30 sweep"
            )
        bound = session_bound(now, clock=clock, calendar=calendar)
        if bound is not None and now >= bound:
            raise refuse(
                f"the range restore stops at {bound.isoformat()}, ahead of the next session's "
                "capture start, because capture waits on the lake lock with no timeout. Run it "
                "after that session's 18:30 sweep"
            )

    def stamp() -> str:
        return clock.now().astimezone(MARKET_TZ).isoformat()

    def local(action: str, exc: OSError) -> RangeRestoreRefused:
        return refuse(_local(action, exc) + ". Fix that and run the range restore again")

    def is_trimmed(line: Mapping[str, Any] | None) -> bool:
        return line is not None and line.get(KIND_FIELD) == TRIM_KIND

    def unservable(rel: str, why: str, line: Mapping[str, Any] | None) -> RangeRestoreRefused:
        if is_trimmed(line):
            recovery = (
                f"The trim line records version {line.get(VERSION_ID_FIELD)} as verified. Put "
                "that version back as the current one, as the README's \"Putting a version back "
                'for the range restore" steps describe, and run the range restore again'
            )
        else:
            recovery = (
                "Put back an earlier version that matches the manifest as the current one, as "
                'the README\'s "Putting a version back for the range restore" steps describe, '
                "and run the range restore again"
            )
        return refuse(f"{rel}: {why}, so it was not restored. {recovery}")

    if surface not in DATE_PARTITIONED:
        raise refuse(
            f"the range restore takes {' or '.join(sorted(DATE_PARTITIONED))} partitions, not "
            f"{surface!r}, so nothing was restored. Restore any other file by hand from the "
            "bucket"
        )
    if first > last:
        raise refuse(
            f"the range ends on {last.isoformat()}, before it starts on {first.isoformat()}, "
            "so nothing was restored"
        )
    guard()
    if not manifest_path(root).is_file():
        raise refuse(
            f"lake_root {root} holds no manifest.jsonl, so it may name the wrong directory and "
            "nothing was restored"
        )

    # Step 2, the repairs and the plan. The lock is held for reads of the two ledgers, the
    # ledger repair and directory listings, never for hashing a partition, because capture's
    # close+5 fill waits on this lock with no timeout and a rollback's present files run to
    # gigabytes.
    plan: dict[str, str] = {}
    trim_lines: dict[str, Mapping[str, Any] | None] = {}
    present: dict[str, str] = {}
    days: dict[str, date] = {}
    try:
        with lake_lock(root):
            guard()
            try:
                summary.ledger_repaired = repair_trimmed_entry(
                    root, source=RANGE_RESTORE_SOURCE, fetched_at=stamp()
                )
            except TrimmedRepairRefused as exc:
                raise refuse(f"{exc} Nothing was restored") from None
            latest = latest_entries(root)
            trimmed = latest_trimmed(root)
            selected: dict[str, str] = {}
            for rel, entry in sorted(latest.items()):
                ref = parse_partition_rel(rel)
                if (
                    ref is not None
                    and ref.surface == surface
                    and (ticker is None or ref.ticker == ticker)
                    and first <= ref.day <= last
                ):
                    selected[rel] = str(entry["sha256"])
                    days[rel] = ref.day
            if not selected:
                raise refuse(
                    f"the manifest records no {scope}, so nothing was restored. Check the "
                    "surface, the ticker and the dates"
                )
            summary.selected = len(selected)
            for rel, sha in selected.items():
                path = root / rel
                for temp in _leftover_temps(path):
                    temp.unlink()
                    summary.temps_removed.append(temp.relative_to(root).as_posix())
                if path.exists():
                    present[rel] = sha
                else:
                    plan[rel] = sha
                    trim_lines[rel] = trimmed.get(rel)
    except RangeRestoreRefused:
        raise
    except ManifestError as exc:
        raise refuse(f"{exc}, so nothing was restored") from None
    except OSError as exc:
        raise local(f"reading or repairing {scope} under {root}", exc) from None

    # Unlocked, each present file is hashed. One that differs refuses before anything is
    # written, and one under a trim line owes a restore line, which a crash after the move and
    # before the line leaves. The hash runs without the lock, so what it saw is re-checked
    # under the lock below, by reading the ledgers and stat, never by hashing again.
    owed: list[str] = []
    differs: str | None = None
    try:
        for rel, sha in present.items():
            try:
                digest = sha256_file(root / rel)
            except FileNotFoundError:
                # Gone since the plan, which a trim finishing a crashed unlink does. The
                # re-check below puts it into the plan.
                continue
            if digest != sha:
                differs = rel
                break
            if is_trimmed(trimmed.get(rel)):
                owed.append(rel)
    except OSError as exc:
        raise local(f"hashing the {scope} already in the lake", exc) from None
    if present:
        try:
            with lake_lock(root):
                guard()
                latest = latest_entries(root)
                trimmed = latest_trimmed(root)

                def moved(rel: str) -> bool:
                    entry = latest.get(rel)
                    # Compared as text, the way ``present`` holds it, so an entry whose
                    # sha is not a string does not read as moved on every run.
                    return entry is None or str(entry.get("sha256")) != present[rel]

                changed = sorted(rel for rel in present if moved(rel))
                if differs is not None and differs not in changed:
                    raise refuse(
                        f"{differs} is on disk and does not match its manifest entry, which the "
                        "Sunday scrub also reports, so nothing was restored. Move it out of the "
                        "lake and run the range restore again, which brings back the bucket's "
                        "copy if that copy matches the manifest"
                    )
                # A partition whose entry moved since the plan was resealed meanwhile, by a
                # recompaction run by hand. Its bytes are consistent, and the run's view of it
                # is not, so the run stops before it writes a line or plans a download for it.
                stale = [rel for rel in changed if rel == differs or rel in owed]
                stale += [rel for rel in changed if not (root / rel).exists()]
                if stale:
                    raise refuse(
                        f"{stale[0]} changed in the lake while the range restore ran, so nothing "
                        "more was written. Run the range restore again"
                    )
                for rel, sha in present.items():
                    if not (root / rel).exists():
                        plan[rel] = sha
                        trim_lines[rel] = trimmed.get(rel)
                        continue
                    if rel not in owed or not is_trimmed(trimmed.get(rel)):
                        continue
                    at = stamp()
                    append_trimmed(
                        root,
                        restore_line(rel, sha256=sha, restored_at=at),
                        source=RANGE_RESTORE_SOURCE,
                        fetched_at=at,
                    )
                    summary.restore_lines += 1
        except RangeRestoreRefused:
            raise
        except ManifestError as exc:
            raise refuse(f"{exc}, so the range restore stopped") from None
        except OSError as exc:
            raise local(f"writing a restore line under {root}", exc) from None
    summary.present = sum(1 for rel in present if rel not in plan)

    if not plan:
        return summary

    # Step 3, the bucket and the reserve, unlocked.
    listing = list_bucket(client, target)
    for rel in plan:
        if rel not in listing:
            raise unservable(
                rel, f"the bucket holds no current version of it: {target}", trim_lines[rel]
            )
    planned = sum(listing[rel] for rel in plan)
    usage = runway.walk(root)
    if usage.refused:
        raise refuse(
            f"the lake walk could not read {usage.refused} path(s), first "
            f"{usage.refusals[0]}, so the journal reserve cannot be measured and nothing was "
            "restored. Make the lake readable and run the range restore again"
        )
    # The reserve is measured on the lake as it will stand after the restore. A restored day
    # inside the growth window can become the busiest one, and the panel reads it from then on.
    after = dict(usage.day_bytes)
    for rel in plan:
        after[days[rel]] = after.get(days[rel], 0) + listing[rel]
    today = clock.now().astimezone(MARKET_TZ).date()
    busiest = runway.busiest_sealed_day(replace(usage, day_bytes=after), today=today)
    try:
        free = free_space(root)
    except OSError as exc:
        raise local(f"reading free space under {root}", exc) from None
    short = runway.reserve_shortfall(free=free, planned=planned, busiest_sealed_day=busiest)
    if short:
        raise refuse(
            f"the restore needs {planned / 1_000_000:.1f} MB and would leave "
            f"{(free - planned) / 1_000_000:.1f} MB free, {short / 1_000_000:.1f} MB short of "
            f"the journal reserve of {runway.JOURNAL_RESERVE_SESSIONS} times the busiest sealed "
            f"day after the restore ({busiest / 1_000_000:.1f} MB), which the next session's "
            "journal needs. Nothing was restored. Restore a narrower range or grow the volume"
        )

    # Step 4, partition by partition. Whatever stops a partition, a refusal, an error or an
    # interrupt, the ``finally`` removes its temp, which the Sunday scrub would read as an
    # orphan. Once the rename ran there is nothing at that name, and it removes nothing.
    read = bucket_reader(client, target)
    for rel, sha in plan.items():
        path = root / rel
        temp = temp_write_path(path, os.getpid())
        try:
            guard()
            try:
                actual, size = _download_to(read, rel, temp)
            except BucketReadError as exc:
                if exc.absent:
                    raise unservable(
                        rel,
                        f"the bucket holds no current version of it: {target}",
                        trim_lines[rel],
                    ) from None
                raise refuse(f"{_one_line(exc, target)}") from None
            except OSError as exc:
                raise local(f"writing {temp.relative_to(root).as_posix()}", exc) from None
            if actual != sha:
                raise unservable(
                    rel,
                    "the bucket's current version does not match its manifest entry",
                    trim_lines[rel],
                )
            try:
                _fsync_path(temp)
                with lake_lock(root):
                    guard()
                    entry = latest_entries(root).get(rel)
                    if entry is None or str(entry.get("sha256")) != sha or path.exists():
                        raise refuse(
                            f"{rel} changed in the lake while it downloaded, so it was not "
                            "restored. Run the range restore again"
                        )
                    line = latest_trimmed(root).get(rel)
                    os.replace(temp, path)
                    _fsync_path(path.parent)
                    summary.restored += 1
                    summary.restored_bytes += size
                    if is_trimmed(line):
                        at = stamp()
                        append_trimmed(
                            root,
                            restore_line(rel, sha256=sha, restored_at=at),
                            source=RANGE_RESTORE_SOURCE,
                            fetched_at=at,
                        )
                        summary.restore_lines += 1
            except RangeRestoreRefused:
                raise
            except ManifestError as exc:
                raise refuse(f"{exc}, so the range restore stopped at {rel}") from None
            except OSError as exc:
                raise local(f"committing {rel}", exc) from None
        finally:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                # A directory that refuses the unlink refused the write first, and that
                # refusal is the one line the operator needs.
                pass
    return summary


# -- the resync ---------------------------------------------------------------
#
# ``python -m lake.bucket resync`` brings a host that is about to become primary again level
# with the bucket, after the other host was primary meanwhile (marketlake #832). The bucket's
# ``manifest.jsonl`` then holds the other host's sessions past the entries both hosts share,
# and this lake holds a tail of its own from the sweep and the battery a shadow still runs.
# The nightly upload refuses that pair, since the bucket's copy is not a prefix of the lake's.
# The resync drops this lake's own tail, downloads what the bucket's tail names, and leaves
# the lake's ``manifest.jsonl`` equal to the bucket's bytes, so the next nightly upload finds
# a prefix again. The design's Backup section carries the reasoning.
#
# It sends only ``ListObjectsV2``, ``HeadObject`` and ``GetObject``, which both hosts' roles
# grant, so it runs under either role. A dry run is the default, because the command deletes
# and reverts files.


class ResyncRefused(BucketRefusal):
    """The resync refused to start, or stopped, with one line for an operator."""


@dataclass
class ResyncSummary:
    """What one resync found, or did, for the lines the command prints.

    ``level`` says the bucket's ``manifest.jsonl`` was already the lake's or a prefix of it,
    so there was nothing to do. ``shared`` counts the entries both copies share, and the two
    tails count each side's entries past them, with the first partition of each, or ``None``
    when that side holds none. ``downloads`` holds ``(rel, size)`` for each file the bucket's
    tail names that the lake lacks or holds with other bytes, and ``deletions`` holds
    ``(rel, why)`` for each file only this lake's tail names that the resync removes.
    ``unrecorded`` names each file the bucket's manifest does not record whose bucket object
    the next upload replaces, and ``warnings`` holds one line per fact the operator should
    act on that refuses nothing. ``stop_by`` is the moment a run must finish by. With
    ``--apply``, ``applied`` says the commit ran, ``entries`` counts the entries the lake's
    manifest holds after it, and ``temps_removed`` names each leftover temp file removed
    beside a target.
    """

    target: str
    applied: bool = False
    entries: int = 0
    temps_removed: list[str] = field(default_factory=list)
    level: bool = False
    shared: int = 0
    bucket_tail: int = 0
    bucket_first: str | None = None
    lake_tail: int = 0
    lake_first: str | None = None
    downloads: list[tuple[str, int]] = field(default_factory=list)
    deletions: list[tuple[str, str]] = field(default_factory=list)
    unrecorded: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stop_by: datetime | None = None

    @property
    def download_bytes(self) -> int:
        return sum(size for _, size in self.downloads)

    def lines(self) -> list[str]:
        """One line per fact, the plan's facts first and the count last."""
        if self.level:
            return [
                "resync: nothing to do, the bucket's manifest.jsonl is this lake's or a prefix "
                f"of it: {self.target}"
            ]
        mine = (
            f"this host's tail from {self.lake_first!r} ({self.lake_tail} entries)"
            if self.lake_first is not None
            else "this host's tail is empty"
        )
        theirs = (
            f"bucket tail from {self.bucket_first!r} ({self.bucket_tail} entries)"
            if self.bucket_first is not None
            else "the bucket's tail is empty"
        )
        out = [f"resync: shared {self.shared} entries; {theirs}; {mine}"]
        out += [f"resync: removed a leftover temp file: {rel}" for rel in self.temps_removed]
        out += [
            f"resync: download {rel} ({size / 1_000_000:.1f} MB)" for rel, size in self.downloads
        ]
        out += [f"resync: delete {rel} ({why})" for rel, why in self.deletions]
        out += [
            f"resync: unrecorded, the next upload replaces it: {rel}" for rel in self.unrecorded
        ]
        out += [f"resync: warning: {line}" for line in self.warnings]
        return out

    def counts(self) -> str:
        """The download and deletion counts, as the last line words them."""
        return (
            f"{len(self.downloads)} download(s), {self.download_bytes / 1_000_000:.1f} MB, "
            f"{len(self.deletions)} deletion(s)"
        )


def _sha256_stream(path: Path) -> str:
    """A file's hex SHA-256, read in ``_READ_CHUNK`` pieces.

    A sealed partition runs to a few hundred megabytes, and the hosted VM keeps about 1.1 GiB
    for a job, so a file is never read into memory whole here, unlike ``sha256_file``.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


class _DamagedLine(Exception):
    """A manifest line :func:`_whole_entries` cannot read as an entry, by its 1-based number."""

    def __init__(self, number: int) -> None:
        super().__init__(number)
        self.number = number


def _whole_entries(raw: bytes) -> list[dict]:
    """Every entry in ``raw``, raising :class:`_DamagedLine` on a line that is not one.

    This is the resync's one parse of both manifests, so the latest entry for each path, both
    tails and every count come from the same lines. A blank line is skipped. The last line,
    when no newline ends it and it does not parse, is a write that did not finish, and it is
    dropped, the rule ``manifest.parse_jsonl`` applies to a torn tail. Any other line must be
    a JSON object naming a string ``partition``.

    The nightly upload's :func:`_entries` steps over such a line instead, which suits a
    classifier and not a command that rewrites the lake. ``parse_jsonl`` stops at it, and
    either rule on one side of the resync would leave an entry counted in one view and missing
    from another. A fused line would hide the entry torn into it, or the entries after it,
    and a partition that is a number or ``null`` would reach a string method as a traceback.
    So the resync refuses and names the line, and the repair is fixing it by hand.
    """
    entries: list[dict] = []
    lines = raw.split(b"\n")
    for number, line in enumerate(lines, start=1):
        text = line.decode("utf-8", "replace").strip()
        if not text:
            continue
        try:
            value = json.loads(text)
        except (ValueError, RecursionError):
            if number == len(lines):
                continue
            raise _DamagedLine(number) from None
        if not _is_entry(value):
            raise _DamagedLine(number)
        entries.append(value)
    return entries


def _flush_fd(fd: int) -> None:
    """Flush a file's bytes to stable storage, past the drive's own cache where it can.

    On macOS that is ``fcntl(fd, F_FULLFSYNC)``, because plain ``fsync`` stops at the drive
    cache there, as ``lake.journal`` explains beside ``F_FULLFSYNC``. Elsewhere it is
    ``os.fsync``. A directory takes plain ``fsync`` through :func:`_fsync_path`, because
    ``F_FULLFSYNC`` does not apply to a directory.
    """
    if F_FULLFSYNC is not None:
        fcntl.fcntl(fd, F_FULLFSYNC)
    else:  # pragma: no cover - non-macOS path
        os.fsync(fd)


def _flush_file(path: Path) -> None:
    """:func:`_flush_fd` on a file named by its path."""
    fd = os.open(path, os.O_RDONLY)
    try:
        _flush_fd(fd)
    finally:
        os.close(fd)


def _missing_dirs(root: Path, path: Path) -> list[Path]:
    """Each directory between ``root`` and ``path`` that does not exist yet, deepest last."""
    missing = [
        parent
        for parent in path.parents
        if parent != root and parent.is_relative_to(root) and not parent.exists()
    ]
    return missing[::-1]


def _stop_by(now: datetime, *, clock: Clock, calendar: Calendar) -> datetime | None:
    """The first moment a run started at ``now`` must have finished by, or ``None``.

    It is the earlier of ``session_bound`` and, on a Sunday before the Sunday job's wake,
    that wake, since the resync refuses inside either.
    """
    candidates: list[datetime] = []
    bound = session_bound(now, clock=clock, calendar=calendar)
    if bound is not None:
        candidates.append(bound)
    local = now.astimezone(MARKET_TZ)
    if local.weekday() == _PY_SUNDAY and local < SUNDAY_WAKE.on(local.date()):
        candidates.append(SUNDAY_WAKE.on(local.date()))
    return min(candidates, default=None)


@dataclass(frozen=True)
class _ResyncPlan:
    """What the read, the classification and the plan found, for the commit to act on.

    ``lake_raw`` and ``copy`` are what the commit requires unchanged. ``keep`` is the length
    of the shared bytes the manifest is cut back to, and ``bucket_raw`` is what it must equal
    once the bucket's tail is appended. ``downloads`` maps each path to download to the
    SHA-256 its latest entry in the bucket's manifest records.
    """

    lake_raw: bytes
    copy: CopyState
    bucket_raw: bytes
    keep: int
    downloads: Mapping[str, str]
    summary: ResyncSummary


def _plan_resync(
    root: Path,
    target: BucketTarget,
    *,
    client: Any,
    clock: Clock,
    calendar: Calendar,
    free_space: Callable[[Path], int],
    refuse: Callable[[str], ResyncRefused],
) -> _ResyncPlan:
    """Read both manifests, classify them, and plan the resync, refusing on what #832 lists.

    It writes nothing. The steps are :func:`resync`'s 2 to 7, less the downloads.
    """
    from lake.battery import SIGNOFF_SOURCE
    from lake.capture_spans import SPANS_PARTITION
    from lake.paths import BARS, QUARANTINE_FILE, parse_segment_rel
    from lake.schema_versions import LEDGER_PARTITION

    summary = ResyncSummary(target=str(target))
    summary.stop_by = _stop_by(clock.now(), clock=clock, calendar=calendar)

    # Step 2, the read. The bucket's copy is downloaded with the HEAD-then-GET read the
    # nightly upload's refusal uses, so a copy that changed between the two refuses.
    try:
        lake_raw = manifest_path(root).read_bytes()
    except OSError as exc:
        raise refuse(_local(f"reading {manifest_path(root)}", exc)) from None
    copy = read_copy_state(client, target, lake_raw)
    if not copy.present:
        raise refuse(
            "the bucket holds no manifest.jsonl, so there is no lake to resync from and "
            f"--target may name the wrong bucket or prefix: {target}"
        )
    bucket_raw = _read_bucket_manifest(client, target, copy)
    if bucket_raw is None:
        raise refuse(
            "the bucket's manifest.jsonl changed between its HEAD and its GET, so another host "
            f"may be uploading now. Run the resync again once that upload ends: {target}"
        )

    # Both manifests are read once, by one rule, and every view below comes from that read.
    def parsed(raw: bytes, where: str, suffix: str) -> list[dict]:
        try:
            return _whole_entries(raw)
        except _DamagedLine as damaged:
            raise refuse(
                f"line {damaged.number} of {where} is not a whole entry naming a partition, so "
                "the two manifests cannot be compared and the resync changed nothing. Repair "
                f"that line by hand, then run the resync again{suffix}"
            ) from None

    bucket_entries = parsed(bucket_raw, "the bucket's manifest.jsonl", f": {target}")
    parsed(lake_raw, str(manifest_path(root)), "")
    if not bucket_entries:
        raise refuse(
            f"the bucket's manifest.jsonl of {len(bucket_raw)} byte(s) carries no whole entry, "
            f"so --target may name the wrong bucket or prefix: {target}"
        )
    bucket_latest = {entry["partition"]: entry for entry in bucket_entries}

    # Step 3, the classification.
    if lake_raw.startswith(bucket_raw):
        summary.level = True
        return _ResyncPlan(lake_raw, copy, bucket_raw, len(lake_raw), {}, summary)
    if bucket_raw.startswith(lake_raw):
        # The lake is a byte prefix of the bucket's copy, which a crash part-way through a
        # commit also leaves. Nothing of this lake's own is past the shared bytes, so the
        # resync only appends, and a torn last line of the lake's is cut back first.
        keep = lake_raw.rfind(b"\n") + 1
    else:
        split = _divergence(bucket_raw, lake_raw, _whole_entries)
        if not split.foreign:
            raise refuse(
                "the bucket's manifest.jsonl is not a prefix of the lake's and holds no entry "
                "this lake never recorded, which is a hand repair, and a resync would revert "
                f"it, so the resync changed nothing. Run {FIRST_UPLOAD_COMMAND} by hand: {target}"
            )
        keep = split.shared_bytes
    if keep == 0:
        # The commit cuts the manifest back to the shared bytes before it appends the
        # bucket's tail. With none shared, a crash between the two would leave an empty
        # manifest, which the guard on a wrong lake_root then refuses on every run.
        raise refuse(
            "this lake's manifest.jsonl and the bucket's share no whole line, so the resync "
            "would empty the lake's before writing the bucket's, and a crash between the two "
            "would leave a manifest no run accepts. The resync changed nothing. Repair the "
            f"damaged first line by hand, then run the resync again: {target}"
        )
    bucket_tail = _whole_entries(bucket_raw[keep:])
    lake_tail = _whole_entries(lake_raw[keep:])
    summary.shared = len(_whole_entries(lake_raw[:keep]))
    summary.bucket_tail = len(bucket_tail)
    summary.bucket_first = bucket_tail[0]["partition"] if bucket_tail else None
    summary.lake_tail = len(lake_tail)
    summary.lake_first = lake_tail[0]["partition"] if lake_tail else None

    # Step 4, the plan: every path either tail names, aimed at its latest entry in the
    # bucket's copy.
    lake_latest: dict[str, dict] = {}
    for entry in lake_tail:
        lake_latest[entry["partition"]] = entry
    paths = sorted({entry["partition"] for entry in bucket_tail} | set(lake_latest))
    unsafe = [
        rel for rel in paths if rel == MANIFEST_FILE or _unsafe(rel) or not _inside(root, rel)
    ]
    if unsafe:
        raise refuse(
            f"a manifest.jsonl names {_quoted(unsafe)}, which would land outside the lake or "
            f"on a name the resync keeps for itself, so the resync changed nothing: {target}"
        )
    try:
        trimmed_latest = read_bucket_trimmed(client, target, bucket_latest)
    except BucketLedgerRefused as exc:
        raise refuse(f"{exc}. The resync changed nothing") from None

    def held(rel: str) -> str | None:
        path = root / rel
        try:
            return _sha256_stream(path) if path.is_file() else None
        except OSError as exc:
            raise refuse(
                _local(f"hashing {rel}", exc) + ", so the resync changed nothing"
            ) from None

    downloads: dict[str, str] = {}
    lake_only: list[str] = []
    trimmed_here: list[str] = []
    for rel in paths:
        entry = bucket_latest.get(rel)
        if entry is None:
            lake_only.append(rel)
            continue
        compacted = _compacted_partition_for_segment(rel)
        if compacted is not None and compacted in bucket_latest:
            continue
        if is_designed_absence(rel, bucket_latest, trimmed_latest):
            # The bucket's manifest still names the path, and its trimmed ledger says the
            # file is gone on purpose. A file left here would sit under an entry for other
            # bytes, which the scrub reads as a mismatch.
            if (root / rel).exists():
                trimmed_here.append(rel)
            continue
        sha = str(entry.get("sha256"))
        if held(rel) != sha:
            downloads[rel] = sha
    if trimmed_here:
        raise refuse(
            f"the bucket's {TRIMMED_FILE} says {_quoted(trimmed_here)} was removed on purpose, "
            "and this lake still holds a file there, which the bucket's manifest.jsonl would "
            "then describe wrongly, so the resync changed nothing. Move each file out of the "
            "lake by hand, then run the resync again"
        )

    # Step 5, the refusals on this lake's own tail. A file only this lake's tail names is
    # deleted when it is on the delete list, and refuses otherwise, whatever its bytes. Left
    # on disk with no entry, it is an orphan to the scrub, and a segment would be merged
    # unchecked by the next compaction. A human decision refuses only while the file on disk
    # still holds that tail's sha for its path, so moving the file out is the repair.
    def holds_mine(rel: str) -> bool:
        return held(rel) == str(lake_latest[rel].get("sha256"))

    bucket_days = {
        (ref.surface, ref.ticker, ref.day)
        for ref in (parse_segment_rel(rel) for rel in bucket_latest)
        if ref is not None
    }
    only_copy: list[str] = []
    outside: list[str] = []
    for rel in lake_only:
        if not (root / rel).is_file():
            continue
        ref = parse_segment_rel(rel)
        compacted = _compacted_partition_for_segment(rel)
        if compacted is not None and compacted in bucket_latest:
            summary.deletions.append((rel, f"covered by {compacted}"))
        elif rel.split("/", 1)[0] == BARS:
            summary.deletions.append((rel, "the next sweep regenerates it"))
        elif parse_partition_rel(rel) is not None or (
            ref is not None and (ref.surface, ref.ticker, ref.day) not in bucket_days
        ):
            only_copy.append(rel)
        else:
            outside.append(rel)
    if only_copy:
        raise refuse(
            f"this lake's own entries past the shared ones name {len(only_copy)} file(s) the "
            f"bucket's manifest.jsonl does not, which may be the only copy of that capture "
            f"({_quoted(only_copy)}), so the resync changed nothing. Move each file out of "
            "the lake by hand and keep it, then run the resync again"
        )
    decided = sorted(
        {
            entry["partition"]
            for entry in lake_tail
            if entry["partition"] == SPANS_PARTITION
            or (entry["partition"] == QUARANTINE_FILE and entry.get("source") == SIGNOFF_SOURCE)
        }
    )
    decided = [rel for rel in decided if holds_mine(rel)]
    if decided:
        raise refuse(
            f"this lake's own entries past the shared ones record a human decision in "
            f"{_quoted(decided)}, an onboard, retire or seed_spans write to {SPANS_PARTITION} "
            f"or a signoff in {QUARANTINE_FILE}, which the bucket's copy would silently "
            "replace, so the resync changed nothing. Redo that decision on the primary after "
            "the switch, or move the file out of the lake by hand, then run the resync again"
        )
    if outside:
        raise refuse(
            f"this lake's own entries past the shared ones name {len(outside)} file(s) the "
            f"bucket's manifest.jsonl does not record and the resync does not delete "
            f"({_quoted(outside)}), so the resync changed nothing. Move each file out of the "
            "lake by hand, then run the resync again"
        )
    if LEDGER_PARTITION in lake_latest:
        summary.warnings.append(
            f"this lake's own entries past the shared ones rewrote {LEDGER_PARTITION}, and the "
            "bucket's version replaces it. Run python -m lake.schema_versions on the primary "
            "after the switch to record the running version again"
        )

    # Every file the resync writes or deletes, case-folded, against every path the bucket's
    # manifest names and every path this lake's own tail names. On a filesystem that ignores
    # case two of them are one file, so the commit could rename a download in and then
    # delete it, or overwrite one download with another.
    by_case: dict[str, set[str]] = {}
    for rel in set(bucket_latest) | set(lake_latest):
        by_case.setdefault(rel.casefold(), set()).add(rel)
    written = set(downloads) | {rel for rel, _ in summary.deletions}
    clashes = sorted(rel for rel in written if len(by_case[rel.casefold()]) > 1)
    if clashes:
        raise refuse(
            f"the resync would write or delete {_quoted(clashes)} beside a path that differs "
            "only by case, and on a filesystem that ignores case the two are one file, so the "
            f"resync changed nothing: {target}"
        )

    # Step 7's checks, before the first download: every planned file is in the listing, its
    # current version is the one the bucket's manifest names, and the free space left after
    # the downloads covers the journal reserve.
    listing = list_bucket(client, target)
    missing = [rel for rel in downloads if rel not in listing]
    if missing:
        raise refuse(
            f"the bucket's manifest.jsonl names {_quoted(missing)}, and the bucket holds no "
            f"current version of it, so the resync changed nothing: {target}"
        )
    for rel, sha in downloads.items():
        try:
            stored = _stored_hex(client, target, rel)
        except BucketReadError:
            raise refuse(
                f"the bucket's manifest.jsonl names {rel!r}, and the bucket holds no current "
                f"version of it, so the resync changed nothing: {target}"
            ) from None
        if stored != sha:
            raise refuse(
                f"the bucket's current {rel!r} is not the version its manifest.jsonl names. The "
                "other host's last nightly upload stopped before its manifest.jsonl PUT, or is "
                "still running, so the resync changed nothing. Let that upload finish, or run "
                f"it again on the other host, then run the resync again: {target}"
            )
        summary.downloads.append((rel, listing[rel]))
    planned = summary.download_bytes
    busiest = runway.listing_busiest_sealed_day(listing)
    try:
        free = free_space(root)
    except OSError as exc:
        raise refuse(_local(f"reading free space under {root}", exc)) from None
    short = runway.reserve_shortfall(free=free, planned=planned, busiest_sealed_day=busiest)
    if short:
        raise refuse(
            f"the resync needs {planned / 1_000_000:.1f} MB and would leave "
            f"{(free - planned) / 1_000_000:.1f} MB free, {short / 1_000_000:.1f} MB short of "
            f"the journal reserve of {runway.JOURNAL_RESERVE_SESSIONS} times the busiest sealed "
            f"day in the bucket ({busiest / 1_000_000:.1f} MB), which the next session's "
            "journal needs. The resync changed nothing. Free that much or grow the volume"
        )

    # Item 5 of #832: a file the bucket's manifest does not record, whose bucket object at the
    # same key differs in size, is replaced by the next nightly upload.
    deleted = {rel for rel, _ in summary.deletions}
    after = Ledger(raw=bucket_raw, entries=tuple(bucket_entries), latest=bucket_latest, last={})
    for rel, path in _unmanifested(root, after):
        if rel in deleted or rel not in listing:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size != listing[rel]:
            summary.unrecorded.append(rel)
    return _ResyncPlan(lake_raw, copy, bucket_raw, keep, downloads, summary)


# Whether a scheduled job's process is executing, by its label. ``control_plane`` holds the
# two real ones, and a test injects a callable.
JobProbe = Callable[[str], bool]


def default_job_probe() -> JobProbe:
    """The host's job probe, chosen when it is asked for so a test can replace the choice.

    On macOS it is ``launchctl_executing_probe``, and on systemd it is
    ``systemctl_executing_probe``. Both read a call that failed as a job that may be
    executing, so the resync refuses rather than guess. The self-check's ``launchctl_probe``
    reads a failed call as down, which suits an alarm and not a command about to write. The
    systemd one reads ``ActiveState``, because a timer's ``Type=oneshot`` service reads
    ``activating`` while it runs, which ``systemctl_probe``'s ``is-active`` answers as down.
    """
    from lake import control_plane

    if control_plane.is_macos():
        return control_plane.launchctl_executing_probe
    return control_plane.systemctl_executing_probe


def _rewrite_manifest(root: Path, keep: int, tail: bytes, expected: bytes) -> None:
    """Cut ``manifest.jsonl`` back to ``keep`` bytes and append ``tail``, in place.

    The file keeps its inode, because ``lake_lock`` is a ``flock`` on a descriptor of this
    file, and a new file renamed over it would let the next locker lock the new inode while
    the holder still locks the old one. So the file is opened ``O_WRONLY | O_APPEND`` without
    ``O_TRUNC``, truncated to ``keep`` and flushed, then ``tail`` is written and flushed, and
    the whole file must then read back as ``expected``. Each flush is :func:`_flush_fd`'s. A
    crash leaves the file as it was or as a byte prefix of ``expected``, and a re-run finishes
    from either. ``keep`` is never 0, since the plan refuses a rewind that shares no whole
    line, so the file is never empty between the two writes. The lock's own descriptor is
    never touched, so the lock holds throughout.
    """
    path = manifest_path(root)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        os.ftruncate(fd, keep)
        _flush_fd(fd)
        view = memoryview(tail)
        while view:
            # Linux returns a short write at disk full, as ``manifest._append_once`` says, and
            # the next call then raises the error. ``_append_once`` never writes twice, since
            # another writer could land between the calls. This caller holds the lake-root
            # lock, so it finishes the write, and a call that writes nothing raises rather
            # than loop.
            written = os.write(fd, view)
            if written == 0:
                raise OSError(f"{path}: a write took 0 of the {len(view)} bytes left")
            view = view[written:]
        _flush_fd(fd)
    finally:
        os.close(fd)
    if path.read_bytes() != expected:
        raise ResyncRefused(
            f"{path} does not read back as the bucket's manifest.jsonl after the rewrite. Run "
            "the resync again, which finishes from what the file holds now"
        )


def resync(
    lake_root: Path | str,
    target: BucketTarget,
    *,
    client: Any,
    clock: Clock,
    calendar: Calendar,
    apply: bool = False,
    job_probe: JobProbe | None = None,
    geteuid: Callable[[], int] | None = None,
    free_space: Callable[[Path], int] = _free_bytes,
) -> ResyncSummary:
    """Bring this lake level with the bucket's ``manifest.jsonl``, or plan it when not ``apply``.

    The steps are marketlake #832's item 4, in order.

    1. Guards: a lake root holding a non-empty ``manifest.jsonl``, since an empty one would
       read as a prefix of the bucket's and plan a download of the whole bucket into a wrong
       ``lake_root``. A process that is not root, by ``geteuid``, since files a root run
       downloads would block the owner's capture and compaction. With ``apply``, no
       executing daemon, vendor sweep or Sunday job, by ``job_probe``. The dry run skips that
       probe, since it writes nothing and takes no lock, so a classification can be read while
       the daemon is up. The Sunday scrub window, and ``session_bound``.
    2. Read: the bucket's copy B, with the HEAD-then-GET read :func:`_read_bucket_manifest`
       makes, and the lake's L. Both are parsed once by :func:`_whole_entries`, and a line in
       either that is not a whole entry, other than a torn last line, refuses and names its
       line number and its manifest.
    3. Classify: an absent B, or one with no whole entry, refuses and names the target. B
       equal to L, or a prefix of it, is level and has nothing to do. L a byte prefix of B
       only appends. Otherwise the test :func:`bucket_divergence` applies decides: entries
       this lake never recorded make a rewind of this lake's own tail, and none make a hand
       repair, which refuses and names the first upload, since a rewind would revert it. A
       resync whose two manifests share no whole line refuses, since the commit would empty
       the lake's manifest before writing B's.
    4. Plan: every path either tail names, aimed at its latest entry in B. A file on disk
       with that sha is skipped, and one that differs or is absent is downloaded. A segment
       whose compacted partition B records is skipped, and so is a designed absence judged
       against B's own ``trimmed.jsonl``, read by :func:`read_bucket_trimmed`. A designed
       absence this lake holds a file for refuses. A path only this lake's tail names is
       judged in step 5. Every path passes the restore's safety checks first.
    5. Refusals on this lake's tail. A file only this lake's tail names, on disk and outside
       step 6's list, refuses whatever its bytes: a chains or quotes partition B does not
       name, or a journal segment no compacted partition and no segment of B covers, may be
       the only copy, and any other such file is one the resync would leave unrecorded. An
       entry for the capture spans, or a signoff in the quarantine ledger, is a human
       decision the bucket's copy would silently replace, and refuses only while the file on
       disk still holds that tail's sha for its path. An entry for the schema-version ledger
       is a warning instead. Last, a download or a deletion whose path differs only by case
       from another path B names or this lake's tail names refuses.
    6. Deletions: a file only this lake's tail names, on disk, goes when it is a journal
       segment whose compacted partition B records, or a ``bars/`` partition, whatever its
       bytes.
    7. Checks: every download is in the listing, its stored SHA-256 equals B's entry, and the
       free space left covers the journal reserve. A stored SHA-256 that differs means the
       other host's last upload stopped before its ``manifest.jsonl`` or is still running.
       With ``apply``, stale temp files beside each target are removed under the lock, and
       each file then downloads unlocked to ``paths.temp_write_path``'s name, must hash to
       B's entry, and is flushed by :func:`_flush_fd`. The guards run again before every
       download.
    8. Commit, under ``lake_lock``: the guards run again, L must read as it did and B's HEAD
       must answer as it did. Each download is renamed into place and flushed with its
       directory and the parent of each directory a download created, step 6's files are
       deleted, and :func:`_rewrite_manifest` cuts the manifest back to the shared bytes and
       appends B's tail in place.

    A refusal or any other stop removes this run's temp files, so a run that stops at its
    deadline discards its downloads and the next one fetches them again. Every refusal
    raises :class:`ResyncRefused` with one line, and a bucket failure raises for ``main`` to
    print as one line.
    """
    root = Path(lake_root)

    def refuse(why: str) -> ResyncRefused:
        return ResyncRefused(why)

    def guard() -> None:
        if (geteuid or os.geteuid)() == 0:
            raise refuse(
                "the resync does not run as root, because files a root run writes would block "
                "the owner's capture and compaction. Run it as the owner"
            )
        if apply:
            probe = job_probe if job_probe is not None else default_job_probe()
            for label in (DAEMON_LABEL, EOD_SWEEP_LABEL, SUNDAY_LABEL):
                if probe(label):
                    raise refuse(
                        f"{label} is executing, or its state could not be read, and the resync "
                        "with --apply writes under the lake root, so it changed nothing. Stop "
                        "the daemon, or let the job finish, then run the resync again"
                    )
        now = clock.now()
        if in_sunday_scrub_window(now):
            raise refuse(
                "the resync does not run on Sunday from 19:55 to 23:30, while the Sunday job "
                "may be scrubbing the lake, since a download in flight reads to the scrub as an "
                "orphan. Run it on another evening after the 18:30 sweep"
            )
        bound = session_bound(now, clock=clock, calendar=calendar)
        if bound is not None and now >= bound:
            raise refuse(
                f"the resync stops at {bound.isoformat()}, ahead of the next session's capture "
                "start, because capture waits on the lake lock with no timeout. Nothing was "
                "changed. Run it after that session's 18:30 sweep"
            )

    if not manifest_path(root).is_file() or manifest_path(root).stat().st_size == 0:
        raise refuse(
            f"lake_root {root} holds no manifest.jsonl, or an empty one, so it may name the "
            "wrong directory, and the resync would plan to download the whole bucket into it. "
            f"Nothing was changed: {target}"
        )
    guard()
    plan = _plan_resync(
        root,
        target,
        client=client,
        clock=clock,
        calendar=calendar,
        free_space=free_space,
        refuse=refuse,
    )
    summary = plan.summary
    if not apply or summary.level:
        return summary

    temps: list[Path] = []
    try:
        try:
            with lake_lock(root):
                for rel in plan.downloads:
                    for temp in _leftover_temps(root / rel):
                        temp.unlink()
                        summary.temps_removed.append(temp.relative_to(root).as_posix())
        except OSError as exc:
            raise refuse(
                _local(f"removing a leftover temp file under {root}", exc)
                + ", so the resync changed nothing"
            ) from None
        read = bucket_reader(client, target)
        created: set[Path] = set()
        for rel, sha in plan.downloads.items():
            guard()
            temp = temp_write_path(root / rel, os.getpid())
            created.update(_missing_dirs(root, temp))
            temps.append(temp)
            try:
                actual, _size = _download_to(read, rel, temp)
                if actual == sha:
                    _flush_file(temp)
            except BucketReadError as exc:
                if not exc.absent:
                    raise
                raise refuse(
                    f"the bucket holds no current version of {rel!r}, which its manifest.jsonl "
                    f"names, so the resync changed nothing: {target}"
                ) from None
            except OSError as exc:
                raise refuse(
                    _local(f"writing {temp.relative_to(root).as_posix()}", exc)
                    + ", so the resync changed nothing"
                ) from None
            if actual != sha:
                raise refuse(
                    f"the bucket's current {rel!r} does not hash to the SHA-256 its "
                    "manifest.jsonl names, so another host may be uploading now and the resync "
                    f"changed nothing. Run it again once that upload ends: {target}"
                )
        with lake_lock(root):
            guard()
            try:
                current = manifest_path(root).read_bytes()
            except OSError as exc:
                raise refuse(_local(f"reading {manifest_path(root)}", exc)) from None
            if current != plan.lake_raw:
                raise refuse(
                    "the lake's manifest.jsonl changed while the resync downloaded, so a lake "
                    "writer ran meanwhile and the resync changed nothing. Run it again"
                )
            again = read_copy_state(client, target, current)
            if (again.present, again.length, again.stored) != (
                plan.copy.present,
                plan.copy.length,
                plan.copy.stored,
            ):
                raise refuse(
                    "the bucket's manifest.jsonl changed while the resync downloaded, so another "
                    f"host may be uploading now and the resync changed nothing: {target}"
                )
            try:
                for rel, temp in zip(plan.downloads, temps, strict=True):
                    os.replace(temp, root / rel)
                    _fsync_path((root / rel).parent)
                # A directory a download created is an entry in its parent, which a crash
                # could lose with every file under it, so each parent is flushed too.
                for parent in sorted({directory.parent for directory in created}):
                    _fsync_path(parent)
                for rel, _why in summary.deletions:
                    (root / rel).unlink(missing_ok=True)
                    _fsync_path((root / rel).parent)
                _rewrite_manifest(root, plan.keep, plan.bucket_raw[plan.keep :], plan.bucket_raw)
            except OSError as exc:
                raise refuse(
                    _local(f"committing the resync under {root}", exc)
                    + ". Run the resync again, which finishes from what the lake holds now"
                ) from None
    finally:
        for temp in temps:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                # A directory that refuses the unlink refused the write first, and that
                # refusal is the one line the operator needs.
                pass
    summary.applied = True
    summary.entries = len(_whole_entries(plan.bucket_raw))
    return summary


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
    nothing, because the bucket's credentials cannot delete. Each line it prints is one
    behavior passing or failing.

    1. S3 refuses a PUT whose ``ChecksumSHA256`` does not match the bytes.
    2. ``HeadObject`` with checksum mode returns the stored SHA-256 of a single PUT, as a
       full-object value.
    3. A PUT to an existing key on a versioned bucket creates a new version and keeps
       the old one.
    4. ``put_object`` sends one request and never splits into parts.

    Three more lines prove the read grants the scrub and the restore need, which nothing
    else calls while ``backup_target`` is still a path (marketlake #737).

    5. ``ListObjectsV2`` under the check's own prefix lists the probe.
    6. ``GetBucketVersioning`` answers ``Enabled``.
    7. A plain ``GetObject`` of the probe returns the bytes of its current version, and the
       ``VersionId`` of that version. The trim records that id on its trim line
       (marketlake #787), and a response without it would stop every trim.

    On the assume-role path the first request is where the role is assumed. A refusal
    there is STS's and not S3's verdict on behavior 1, so it is raised for ``main`` to
    print as one line.
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
    except _AssumeRoleFailed:
        raise
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
    distinct = usable_version_id(old_id) and usable_version_id(new_id) and old_id != new_id
    report(distinct, f"3 two PUTs to one key returned versions {old_id} and {new_id}")
    try:
        kept = client.get_object(Bucket=target.bucket, Key=key, VersionId=old_id)["Body"].read()
    except Exception as exc:
        out(
            f"live-check: the old version could not be read back ({_error_code(exc)}). The "
            "bucket's credentials hold no s3:GetObjectVersion, so confirm by hand in the "
            f"console that {key} shows two versions"
        )
    else:
        report(kept == probe, "3 the first version still holds the first PUT's bytes")

    def grant(number: int, call: str, read: Callable[[], tuple[bool, str]]) -> None:
        try:
            passed, shown = read()
        except Exception as exc:
            failure = _failure(exc)
            if failure is None:
                raise
            report(False, f"{number} {call} {failure[0]} with {failure[1]}")
        else:
            report(passed, f"{number} {call} {shown}")

    # 5, 6 and 7. The read grants, each by the call the scrub or the restore makes.
    listed_prefix = f"{target.key(base)}/"

    def listing() -> tuple[bool, str]:
        found = client.list_objects_v2(Bucket=target.bucket, Prefix=listed_prefix)
        keys = [entry.get("Key") for entry in found.get("Contents", [])]
        return key in keys, f"under {listed_prefix} listed {len(keys)} object(s)"

    def versioning() -> tuple[bool, str]:
        status = client.get_bucket_versioning(Bucket=target.bucket).get("Status")
        return status == "Enabled", f"returned {status or 'no status'}"

    def current() -> tuple[bool, str]:
        response = client.get_object(Bucket=target.bucket, Key=key)
        body = response["Body"].read()
        version = response.get("VersionId")
        held = "the current version's bytes" if body == replacement else "other bytes"
        named = version == new_id and usable_version_id(version)
        shown = f"VersionId {version}" if named else f"VersionId {version}, not {new_id}"
        return body == replacement and named, f"of the probe returned {held} with {shown}"

    grant(5, "ListObjectsV2", listing)
    grant(6, "GetBucketVersioning", versioning)
    grant(7, "GetObject", current)

    out(
        f"live-check: delete {target.key(base)}/ and every version under it in the console. "
        "The bucket's credentials cannot delete"
    )
    return ok


# -- the command-line entry ---------------------------------------------------


def _restore_command(
    dest: str, target: BucketTarget, client: Any, *, skip_designed_absences: bool
) -> int:
    """Run the restore and print its lines. A refusal raises, for ``main`` to print.

    A designed absence the bucket no longer holds gets its own line on stdout, because the run
    still exits 0 without it.
    """
    summary = restore_lake(
        dest, target, client=client, skip_designed_absences=skip_designed_absences
    )
    for rel in summary.unrecorded:
        print(
            f"restore: restored with no manifest entry, verified against the bucket's "
            f"stored SHA-256: {rel}"
        )
    for rel in summary.trimmed_lost:
        print(
            "restore: trimmed on purpose and missing from the bucket, so it was left out. "
            f"The bucket holds no current version of it: {rel}"
        )
    if not summary.restored:
        for rel, why in summary.failures:
            print(f"restore: {why}: {rel}", file=sys.stderr)
        print(
            f"restore: {len(summary.failures)} file(s) failed, so {summary.dest} holds no "
            f"lake. Every file that verified stays in {summary.work}, and a re-run resumes "
            "there",
            file=sys.stderr,
        )
        return 1
    print(f"restore: {summary.render()}")
    return 0


def _resync_command(
    config: Config,
    target: BucketTarget,
    client: Any,
    *,
    apply: bool,
    clock: Clock,
    calendar: Calendar,
) -> int:
    """Run the resync and print its lines, one fact each, on stdout. A refusal raises.

    After an applied resync it prints the verdicts of ``roster.check_lake`` and
    ``schema_versions.check_running_version``, because the roster file stays this host's
    while the capture spans and the schema-version ledger now come from the bucket. A roster
    refusal prints as a warning and exits 0, since exit 2 means nothing was written. A last
    line names a ``backup_target`` that is not the bucket the resync read from, since this
    host's next close+15 would then not upload there.
    """
    summary = resync(
        config.lake_root, target, client=client, clock=clock, calendar=calendar, apply=apply
    )
    for line in summary.lines():
        print(line)
    if summary.level:
        return 0
    if not summary.applied:
        stop = (
            ""
            if summary.stop_by is None
            else f", must stop by {summary.stop_by.astimezone(MARKET_TZ).isoformat()}"
        )
        print(f"resync: dry run: {summary.counts()}{stop}. Run again with --apply")
        return 0
    print(
        f"resync: applied: {summary.counts()}, and manifest.jsonl now holds the bucket's "
        f"{summary.entries} entries"
    )
    from lake.roster import RosterError, check_lake
    from lake.schema_versions import check_running_version
    from lake.tickers import TickersError, load_tickers

    try:
        check_lake(load_tickers(), config, clock=clock)
    except (RosterError, TickersError) as exc:
        print(f"resync: warning: {' '.join(str(exc).split())}")
    version = check_running_version(config.lake_root)
    if version.ok:
        print(f"resync: schema version {version.version} is recorded in the lake")
    else:
        print(f"resync: warning: schema version: {version.summary}")
    if config.backup_target != target:
        print(
            f"resync: backup_target is {config.backup_target}, not {target}, the bucket this "
            "resync read from, so this host's next close+15 would not upload there. Point "
            "backup_target at it before this host becomes the primary"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    """The ``python -m lake.bucket`` argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m lake.bucket",
        description=(
            "Seed the backup bucket by hand, restore a lake from it, put chosen partitions "
            "back into the live lake from it, bring a lake level with it after the other "
            "host was primary, or check the provider's behavior live."
        ),
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
        "live-check",
        help="Confirm the four provider behaviors and three read grants against the real bucket.",
    )
    live.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    live.add_argument(
        "--target",
        required=True,
        help="Where to write the probe objects, as s3://<bucket>/<prefix>, outside the lake's.",
    )
    restore = sub.add_parser(
        "restore",
        help=(
            "Download the bucket's current versions into an empty directory and verify them. "
            "On a host whose config sets lake_window_sessions, leave out each partition the "
            "bucket's trimmed.jsonl says was removed on purpose."
        ),
    )
    restore.add_argument(
        "dest", help="The empty directory to restore into. It may also not exist yet."
    )
    restore.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    restore.add_argument(
        "--target",
        help="The bucket, as s3://<bucket>[/<prefix>]. Defaults to backup_target when it is one.",
    )
    ranged = sub.add_parser(
        "restore-range",
        help=(
            "Put chosen chains or quotes partitions back into the live lake from the bucket, "
            "for a lost partition or a rollback."
        ),
    )
    ranged.add_argument("--surface", required=True, help="chains or quotes.")
    ranged.add_argument("--ticker", help="One ticker. Every ticker when left out.")
    ranged.add_argument(
        "--from", dest="first", required=True, type=_day_arg, help="The first day, YYYY-MM-DD."
    )
    ranged.add_argument(
        "--to", dest="last", required=True, type=_day_arg, help="The last day, YYYY-MM-DD."
    )
    ranged.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    ranged.add_argument(
        "--target",
        help="The bucket, as s3://<bucket>[/<prefix>]. Defaults to backup_target when it is one.",
    )
    resynced = sub.add_parser(
        "resync",
        help=(
            "Bring this lake level with the bucket after the other host was primary. A dry "
            "run unless --apply is given."
        ),
    )
    resynced.add_argument("--config", help="Path to config.yaml (defaults to the standard place).")
    resynced.add_argument(
        "--target",
        help="The bucket, as s3://<bucket>[/<prefix>]. Defaults to backup_target when it is one.",
    )
    resynced.add_argument(
        "--apply",
        action="store_true",
        help="Download, delete and rewrite manifest.jsonl. Without it the run only plans.",
    )
    return parser


def _day_arg(text: str) -> date:
    """A ``YYYY-MM-DD`` argument, read by the lake's one strict date rule."""
    day = parse_date_dir(f"{DATE_PREFIX}{text}")
    if day is None:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {text!r}")
    return day


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
    failure = (exc.kind, exc.code) if isinstance(exc, BucketReadError) else _failure(exc)
    if failure is None:
        return None
    kind, detail = failure
    if kind == "refused" and detail.startswith(f"{ASSUME_ROLE} "):
        return BucketUnreachable(
            f"the bucket's role could not be assumed ({detail}), so check "
            f"{COMMAND_KEY_ID_KEY}, {COMMAND_SECRET_KEY} and {BUCKET_ROLE_ARN_KEY} in "
            f"config.yaml. A key made minutes ago may not be active yet: {target}"
        )
    if kind == "refused":
        return BucketUnreachable(
            f"the bucket refused the request ({detail}), so the bucket's credentials or "
            f"their policy may need replacing: {target}"
        )
    if kind == "unreachable":
        return BucketUnreachable(
            f"the bucket could not be reached or was unavailable ({detail}): {target}"
        )
    return BucketUnreachable(f"the bucket answered with an error ({detail}): {target}")


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

    Under a ``shadow`` role ``first-upload``, ``live-check`` and ``restore-range`` refuse with
    exit 2 before a client is built. A shadow seeded from the primary would upload under the
    primary's credentials, and ``first-upload`` replaces the bucket's ``manifest.jsonl``
    outright. ``restore-range`` writes into the lake at ``lake_root``, and a shadow's lake is
    compared with the primary's and then discarded, so nothing restored into it would be kept.
    It prints its own line saying to run it on the primary. ``restore`` runs under either
    role. It uploads nothing and writes only into an empty
    directory, which may be a fresh volume's mount point holding only ``lost+found``.
    That is how a new host is seeded. ``resync`` runs under either role too. It sends no
    write to the bucket, and a shadow is the host that runs it, to become level with the
    bucket before it resumes as the primary.

    ``restore`` reads ``lake_window_sessions`` through ``window.window_sessions`` before the
    client is built, so a bad value refuses before any request. A host that sets the key
    keeps a window, and its restore leaves out the designed absences the bucket's
    ``trimmed.jsonl`` records. A host without it restores the whole lake. A malformed value, or
    one under the floor, refuses with one line and exit 2. No other command reads the key, so
    a bad one breaks nothing else.

    ``restore`` exits 0 when the destination was filled, 1 when a file failed
    verification, with one line per failing file, and 2 on a refusal, with one line.
    ``restore-range`` exits 0 when every selected partition is in the lake, and 2 on a
    refusal, with one line. ``resync`` exits 0 when it printed its plan, found nothing to
    do, or applied the plan, and 2 on a refusal, with one line. Once ``--apply`` has
    rewritten the lake, a roster check that refuses prints a warning and still exits 0,
    because exit 2 says nothing was written.
    """
    args = build_parser().parse_args(argv)
    label = args.command
    with input_errors_exit(label, BucketRefusal, WindowRefused):
        config = load_config(args.config)
        role, warning = outbox.role_of(config)
        if warning is not None:
            print(f"{label}: {warning}", file=sys.stderr)
        if role != outbox.PRIMARY and args.command == "restore-range":
            print(f"{label}: {RANGE_RESTORE_SHADOW}", file=sys.stderr)
            return 2
        if role != outbox.PRIMARY and args.command not in ("restore", "resync"):
            print(f"{label}: {BUCKET_SHADOW}", file=sys.stderr)
            return 2
        windowed = False
        if args.command == "restore":
            # Judged before ``connect``, which already fetches credentials on the instance
            # profile path, so a bad key refuses before any request.
            windowed = window_sessions(config.lake_window_sessions, config.guards) is not None
        target, client = connect(config, _target(config, args.target))
        if clock is None:
            from lake.clock import SystemClock

            clock = SystemClock()
        try:
            if args.command == "restore":
                return _restore_command(args.dest, target, client, skip_designed_absences=windowed)
            if args.command == "restore-range":
                if calendar is None:
                    from lake.calendar import ExchangeCalendar

                    calendar = ExchangeCalendar()
                ranged = restore_range(
                    config.lake_root,
                    target,
                    client=client,
                    clock=clock,
                    calendar=calendar,
                    surface=args.surface,
                    ticker=args.ticker,
                    first=args.first,
                    last=args.last,
                )
                for rel in ranged.temps_removed:
                    print(f"restore-range: removed a leftover temp file: {rel}")
                print(f"restore-range: {ranged.render()}")
                return 0
            if args.command == "resync":
                if calendar is None:
                    from lake.calendar import ExchangeCalendar

                    calendar = ExchangeCalendar()
                return _resync_command(
                    config, target, client, apply=args.apply, clock=clock, calendar=calendar
                )
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
            if args.command == "restore":
                line = BucketUnreachable(
                    f"{line}. The destination is untouched, and a re-run resumes in the "
                    "working directory"
                )
            raise line from exc
    if summary.rebaselined:
        print(
            "first-upload: the bucket's manifest.jsonl was not a prefix of the lake's, and "
            "this upload replaced it"
        )
    print(f"first-upload: {summary.render()}")
    return 0


__all__ = [
    "ASSUME_ROLE",
    "BAD_DIGEST",
    "BUCKET_SHADOW",
    "FIRST_UPLOAD_COMMAND",
    "FULL_OBJECT",
    "IN_FLIGHT_ALLOWANCE",
    "JobProbe",
    "LIVE_PROBE_BYTES",
    "MAX_PUT_BYTES",
    "NIGHTLY_UPLOAD_BUDGET",
    "PRE_OPEN_MARGIN",
    "SEAL_ALLOWANCE",
    "STORAGE_CLASS",
    "SWEEP_MARGIN",
    "RESTORE_MARKER",
    "RESTORE_WORK_DIR",
    "RESYNC_COMMAND",
    "VERIFIED_MARKER",
    "BucketBackup",
    "BucketLedgerMismatch",
    "BucketLedgerMissing",
    "BucketLedgerRefused",
    "BucketReadError",
    "BucketRefusal",
    "BucketSettingsInvalid",
    "BucketUnreachable",
    "ClientFromConfig",
    "ChecksumRefused",
    "CopyState",
    "CurrentDigest",
    "Divergence",
    "FirstUploadRefused",
    "Ledger",
    "ManifestedFileMissing",
    "TrimmedNotInBucket",
    "ObjectTooLarge",
    "RANGE_RESTORE_SHADOW",
    "RANGE_RESTORE_SOURCE",
    "RangeRestoreRefused",
    "RangeRestoreSummary",
    "RestoreRefused",
    "RestoreSummary",
    "ResyncRefused",
    "ResyncSummary",
    "UploadDeadline",
    "UploadSummary",
    "WatermarkMissing",
    "b64_sha256",
    "bucket_divergence",
    "bucket_reader",
    "bucket_scrub",
    "build_parser",
    "client_from_config",
    "connect",
    "current_digest",
    "default_job_probe",
    "first_upload",
    "first_upload_files",
    "hex_to_b64",
    "in_sunday_scrub_window",
    "list_bucket",
    "live_check",
    "main",
    "manifested_files",
    "nightly_upload",
    "read_bucket_trimmed",
    "read_copy_state",
    "read_ledger",
    "restore_lake",
    "restore_range",
    "resync",
    "rsync_excluded",
    "session_bound",
    "stored_sha256",
    "usable_version_id",
    "walk_lake",
    "watermark",
]


if __name__ == "__main__":
    raise SystemExit(main())
