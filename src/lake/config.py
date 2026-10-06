"""The configuration module: the ``DATA_DIR`` pattern.

Every machine-specific location and every guard constant resolves through this one
module. Relocating the lake or retargeting the backup is a one-line edit to
``~/.config/marketlake/config.yaml``, never a code change. That single-source rule is
the design's ``DATA_DIR`` pattern.

The file is *machine-local* config: where things live on this machine and how to alert
from it. It is the counterpart to the *portable* ``tickers.yaml`` roster, which says
what to capture and travels on migration. This module loads the machine-local half.
The roster lives in ``lake.tickers``.

Four of the values are always secrets, and two more join them once the bucket's access
key is added. The healthchecks ping key builds the health-ping URLs. The ntfy topic is
an unauthenticated channel that anyone holding the name can read and spoof. The Schwab
API key and app secret are the static app-registration inputs ``schwab-py`` needs to
build the client and refresh the token. The bucket's access key id and secret access
key sign every request to the backup bucket on the key path, and ``lake.bucket`` builds
its client from these two values alone. A host whose ``bucket_credentials`` is
``instance_profile`` carries neither, because its credentials come from the EC2
instance metadata service instead. The rotating token itself is not here. It lives at
``~/.config/marketlake/token.json`` and is handled elsewhere. Every secret is wrapped
in ``Secret``, which redacts itself in every log, repr, and traceback. The one caller
that must use a raw value calls ``reveal``. So a stray ``print(config)`` or a logged
exception never leaks any of them.

``backup_target`` takes two forms. A filesystem path is the default and is copied to
with ``rsync``. A bucket URL, ``s3://<bucket>`` or ``s3://<bucket>/<prefix>``, is
uploaded to by ``lake.bucket``. The scheme is read before any ``Path`` is built,
because ``Path("s3://bucket/x")`` collapses the double slash and would name a local
directory called ``s3:``. The bucket's two key values and its region are needed only
when the target is a bucket. On the key path they may sit in the file beside a path
target, which is how the first upload runs before the target is switched.

``bucket_credentials`` says where the bucket client's credentials come from. ``keys``,
the default when the key is absent, reads the two key values from this file.
``instance_profile`` reads them from the EC2 instance metadata service, for a VM with an
instance profile attached, and then this file must hold neither key value. The value is
stored as read, the way ``role`` is, and only a bucket job checks it.

**Loading never refuses a backup setting.** Capture loads this file every cycle and the
daemon loads it at startup, so a refusal here would stop capture over a setting only
the nightly backup reads, and a lost minute cannot be recovered. A value that does not
start with ``s3://`` loads as a ``Path`` exactly as it always has, whatever it holds. An
``s3://`` value loads as a ``BucketTarget`` with no checks, and a missing bucket key
loads as ``None``, and any ``bucket_credentials`` value loads as read.
``require_bucket_settings`` holds the strict checks, and each bucket job calls it when
it runs, so a bad bucket setting fails only the backup.

One key is optional rather than required: ``schwab_callback_url``, the third static
app-registration input. Only the weekly re-auth in ``lake.reauth`` reads it, and capture
never does, so a config without it loads and the daemon runs. The re-auth is the one
place that refuses without it, naming the key. It is not wrapped in ``Secret``. A
registered callback is a loopback URL rather than a credential, and the re-auth prints
it so the operator can check it against the Schwab app registration, which a redacting
wrapper would make impossible.

A second key is optional too: ``role``, which says whether this host is the ``primary``
or a ``shadow``. The loader refuses nothing. It stores a string as read and any other
value as its ``repr``, and ``lake.outbox`` decides what the value means when a ``main``
asks it for senders. A
refusal here would land inside the capture cycle, which loads this file every minute, so
a typo made mid-session would stop capture until someone fixed the file. The daemon reads
the role only at start, so a check every minute would protect nothing.

A *guard constant* is a tunable threshold the failure machinery reads, like the
watchdog's page-after count or the suspect-snapshot ratio. The defaults here are the
values the design pins. Slice 1 measures the real distributions and recalibrates them.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from pathlib import Path

import yaml

from lake.chain_plan import ChainPlanError
from lake.paths import CONFIG_FILE, LakePaths, config_dir
from lake.tickers import TickersError

# The machine-local config file. Overridable by argument or this environment variable,
# so a test points the loader at a throwaway file.
DEFAULT_CONFIG_PATH = config_dir() / CONFIG_FILE
CONFIG_PATH_ENV = "MARKETLAKE_CONFIG"

# The healthchecks host. Pings go by slug, in the form ``hc-ping.com/<ping-key>/<slug>``.
# The config holds the one rotatable ping key, never six immutable UUID URLs.
HEALTHCHECKS_HOST = "hc-ping.com"

# The Schwab callback key, spelled once. The re-auth refuses without it and names it,
# and the rendered re-auth script names it too, so all three read this rather than
# repeating the string.
CALLBACK_KEY = "schwab_callback_url"

# The bucket form of ``backup_target``. Only S3 is accepted: the design relies on S3
# storing and returning a whole-object SHA-256 for a single PUT, which is what lets the
# manifest's own digest travel with every upload.
BUCKET_SCHEME = "s3://"

# The keys the bucket form needs, spelled once so the refusal names them.
BUCKET_KEY_ID_KEY = "bucket_access_key_id"
BUCKET_SECRET_KEY = "bucket_secret_access_key"
BUCKET_REGION_KEY = "bucket_region"
BUCKET_KEYS = (BUCKET_KEY_ID_KEY, BUCKET_SECRET_KEY, BUCKET_REGION_KEY)

# Where the bucket client's credentials come from, and the two values that key takes.
# ``keys`` reads the two key values from this file and is what an absent key means.
# ``instance_profile`` reads them from the EC2 instance metadata service.
BUCKET_CREDENTIALS_KEY = "bucket_credentials"
CREDENTIALS_FROM_KEYS = "keys"
CREDENTIALS_FROM_INSTANCE_PROFILE = "instance_profile"

# The refusal for any other ``bucket_credentials`` value. It never quotes the value, for
# the reason ``bucket_credential_problems`` gives, and ``lake.bucket`` raises it too.
UNRECOGNISED_CREDENTIALS = (
    f"{BUCKET_CREDENTIALS_KEY} must be {CREDENTIALS_FROM_KEYS} or "
    f"{CREDENTIALS_FROM_INSTANCE_PROFILE}"
)

# What S3 allows in a bucket name: 3 to 63 lowercase letters, digits, dots and hyphens,
# starting and ending with a letter or digit.
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")

# The shape of an AWS region name: a two-to-four letter area, one or more lowercase
# words, and a number, as in ``us-east-2``, ``us-gov-west-1`` or ``eusc-de-east-1``.
# ``botocore`` refuses a malformed name only when the client is built, with an error
# that is not a ``ConfigError``, so the check runs first and names the key instead.
_REGION = re.compile(r"[a-z]{2,4}(-[a-z]+)+-\d+")

# The host-role key, spelled once. ``lake.outbox`` names it in the line it prints for a
# value it does not recognise.
ROLE_KEY = "role"


class _Absent(Enum):
    """The type of ``ROLE_ABSENT``.

    An ``Enum`` member, because ``lake.outbox`` compares it by identity, and a member
    survives ``copy.deepcopy`` and a pickle round trip as the same object. A plain
    ``object()`` comes back as a new one, and a copied config without the key then
    reads as ``shadow``.
    """

    ROLE_ABSENT = "ROLE_ABSENT"

    def __repr__(self) -> str:
        return "ROLE_ABSENT"


# What ``Config.role`` holds when the file has no ``role`` key at all. A key written with
# no value parses to ``None``, and that has to stay distinguishable from no key, because
# an absent key means ``primary`` and an empty one does not.
ROLE_ABSENT = _Absent.ROLE_ABSENT

# The required keys. Guard constants are optional and default to the pinned values, and
# so is ``CALLBACK_KEY``: no capture path reads it, so a config missing it must load
# rather than take the daemon down for a key the daemon has no use for. ``ROLE_KEY`` is
# optional as well, and an absent one means the host is the primary.
_REQUIRED_KEYS = (
    "lake_root",
    "backup_target",
    "healthchecks_ping_key",
    "ntfy_topic",
    "schwab_api_key",
    "schwab_app_secret",
)


class ConfigError(Exception):
    """Raised for a missing config file, a missing required key, or an unknown guard."""


class Secret:
    """A string value that never reveals itself except through ``reveal``.

    Its repr, str, and format all redact. So the ping key and ntfy topic stay out of
    logs, tracebacks, and any accidental string conversion of the config. The one
    caller that must use the raw value, such as building a ping URL or POSTing to
    ntfy, calls ``reveal``.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        """The raw value, for the one caller that must use it."""
        return self._value

    def __repr__(self) -> str:
        return "Secret(***redacted***)"

    __str__ = __repr__

    def __format__(self, spec: str) -> str:
        return self.__repr__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass(frozen=True)
class BucketTarget:
    """The bucket form of ``backup_target``: a bucket name and a key prefix.

    ``prefix`` is empty or a run of path components with no leading or trailing slash.
    A lake file at lake-relative path ``rel`` lives at key ``prefix/rel``, or at ``rel``
    when the prefix is empty. Its ``str`` is the URL, normalised, which is what every
    finding and report line names.
    """

    bucket: str
    prefix: str = ""

    def key(self, rel: str) -> str:
        """The object key for lake-relative path ``rel``."""
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def rel(self, key: str) -> str | None:
        """The lake-relative path for an object key, or ``None`` when it is outside."""
        if not self.prefix:
            return key
        head = f"{self.prefix}/"
        return key[len(head) :] if key.startswith(head) else None

    @property
    def list_prefix(self) -> str:
        """The ``Prefix`` a listing of this target passes."""
        return f"{self.prefix}/" if self.prefix else ""

    def __str__(self) -> str:
        return f"{BUCKET_SCHEME}{self.bucket}/{self.prefix}".rstrip("/")


def parse_backup_target(value: object) -> Path | BucketTarget:
    """Read ``backup_target``: an ``s3://`` URL is a bucket, anything else is a path.

    The scheme is checked on the raw text, before any ``Path`` exists, because a
    ``Path`` collapses ``//`` and would turn the URL into a local directory. Nothing
    here refuses, for the reason the module docstring gives. Any other value becomes a
    ``Path`` exactly as it did before a bucket was possible, and an ``s3://`` value
    becomes a ``BucketTarget`` with empty prefix components dropped. A malformed bucket
    is caught by ``require_bucket_settings`` when a bucket job runs.
    """
    text = str(value)
    if text.startswith(BUCKET_SCHEME):
        bucket, _, prefix = text[len(BUCKET_SCHEME) :].partition("/")
        parts = [part for part in prefix.split("/") if part]
        return BucketTarget(bucket=bucket, prefix="/".join(parts))
    return Path(text).expanduser()


def bucket_target_problems(target: BucketTarget) -> list[str]:
    """What is wrong with a bucket target's name or prefix, as operator phrases."""
    problems = []
    if not _BUCKET_NAME.fullmatch(target.bucket) or ".." in target.bucket:
        problems.append(
            f"{target} names no valid bucket. The form is {BUCKET_SCHEME}<bucket> or "
            f"{BUCKET_SCHEME}<bucket>/<prefix>, with a name of 3 to 63 lowercase letters, "
            "digits, dots and hyphens"
        )
    if any(part in (".", "..") for part in target.prefix.split("/")):
        problems.append(f"{target} has a prefix holding . or ..")
    return problems


def bucket_credential_problems(config: Config) -> list[str]:
    """What is wrong with the config's credential settings, as operator phrases.

    ``bucket_credentials`` decides what must be present.

    1. ``keys`` needs ``bucket_access_key_id``, ``bucket_secret_access_key`` and
       ``bucket_region``.
    2. ``instance_profile`` needs ``bucket_region`` and refuses either key value, so one
       machine cannot hold both by mistake.
    3. Any other value is refused alone, naming the key and the two values it takes.
       The value read is never quoted, because a key about credentials invites a pasted
       secret. It never falls back to ``keys``.

    ``require_bucket_settings`` and ``lake.bucket.client_from_config`` both call this.
    """
    source = config.bucket_credentials
    if source not in (CREDENTIALS_FROM_KEYS, CREDENTIALS_FROM_INSTANCE_PROFILE):
        return [UNRECOGNISED_CREDENTIALS]
    problems = []
    key_id, secret_key = config.bucket_access_key_id, config.bucket_secret_access_key
    if source == CREDENTIALS_FROM_INSTANCE_PROFILE:
        if key_id is not None or secret_key is not None:
            present = [
                key
                for key, value in ((BUCKET_KEY_ID_KEY, key_id), (BUCKET_SECRET_KEY, secret_key))
                if value is not None
            ]
            problems.append(
                f"{BUCKET_CREDENTIALS_KEY} is {CREDENTIALS_FROM_INSTANCE_PROFILE}, so the "
                f"config must not hold {present}"
            )
        needed = ((BUCKET_REGION_KEY, config.bucket_region),)
    else:
        values = (key_id, secret_key, config.bucket_region)
        needed = tuple(zip(BUCKET_KEYS, values, strict=True))
    absent = [key for key, value in needed if value is None]
    if absent:
        problems.append(f"the bucket needs config key(s): {absent}")
    return problems


def require_bucket_settings(config: Config, target: BucketTarget | None = None) -> BucketTarget:
    """The strict checks a bucket job runs before it builds a client, or a ``ConfigError``.

    ``target`` defaults to ``backup_target``, which must then be a bucket. Four things are
    checked, and every failure is named in one line.

    1. The bucket name is one S3 accepts.
    2. The prefix holds no ``.`` or ``..`` component.
    3. The credential settings fit ``bucket_credentials``, as
       :func:`bucket_credential_problems` states: ``bucket_region`` always, the two key
       values on the key path, and neither of them on the instance-profile path.
    4. The region has the shape of an AWS region name.

    Loading the config runs none of these, so a bad bucket setting reaches the job that
    uses it and nothing else. The nightly upload, the Sunday scrub, the first upload and
    the live check each call this when they start.
    """
    if target is None:
        if not isinstance(config.backup_target, BucketTarget):
            raise ConfigError(f"backup_target is not an {BUCKET_SCHEME} bucket")
        target = config.backup_target
    problems = bucket_target_problems(target)
    problems.extend(bucket_credential_problems(config))
    region = config.bucket_region
    if region is not None and not _REGION.fullmatch(region):
        problems.append(f"{BUCKET_REGION_KEY} {region!r} is not an AWS region name like us-east-2")
    if problems:
        raise ConfigError(". ".join(problems))
    return target


# The largest ``capture_stagger_ms`` the config accepts. The field's comment carries why.
_MAX_CAPTURE_STAGGER_MS = 1000

# The range ``capture_request_bound_s`` accepts, in whole seconds. The field's comment
# carries why.
_MIN_CAPTURE_REQUEST_BOUND_S = 1
_MAX_CAPTURE_REQUEST_BOUND_S = 59


@dataclass(frozen=True)
class GuardConstants:
    """The guard constants, with the design's pinned defaults.

    Slice 1 measures the real distributions and recalibrates these. Until then the
    defaults here are what the design pins. Each is glossed at its field.
    """

    # The watchdog pages when a per-ticker, per-surface counter reaches this many
    # consecutive session minutes with no durable data cycle, and when an enabled ticker
    # has spent this many consecutive session minutes outside every capture span.
    watchdog_page_minutes: int = 3
    # A chain snapshot is tagged *suspect* when its contract count falls below this
    # fraction of the trailing-median count.
    suspect_contract_ratio: float = 0.70
    # The trailing window, in sessions, the suspect and battery medians compute over.
    trailing_median_sessions: int = 20
    # The battery's row-count band. A snapshot passes within plus or minus this fraction
    # of the trailing median.
    battery_row_count_band: float = 0.30
    # A feed pages as delayed when session-median staleness exceeds this many seconds.
    staleness_page_seconds: int = 60
    # The dead-man ping's grace, in minutes, before a missed ping pages. It is looser
    # than the watchdog's count because it measures missing network reports, not missing
    # data.
    dead_man_grace_minutes: int = 5
    # Median-relative checks with fewer than this many trailing sessions still run but
    # tag their rows *insufficient_history* instead of clean.
    min_trailing_sessions: int = 5
    # The OI view's freshness test uses the next four. The design names three of them as
    # guard constants and pins no number, and marketlake #137 names the fourth. Slice 1's
    # refresh-moment measurement was meant to calibrate them and cannot: it looks for a
    # later cycle in a session that differs from that session's first, and open interest
    # does not move inside a stored session. So these are provisional placeholders waiting
    # on a calibration that has to come from somewhere else, not design-pinned figures.
    # The minimum comparable-set size below which the OI verdict is *indeterminate*.
    oi_comparable_set_floor: int = 20
    # The fraction of the comparable set that must show changed OI to declare a refresh.
    oi_refresh_quorum: float = 0.50
    # The number of subsequent stored cycles a refreshed OI must hold to be selected.
    oi_plateau_cycles: int = 1
    # How many of session S's top-volume contracts rank into the comparable set. The
    # measurement bounds this from above rather than pinning it. Only 4,443 to 5,287
    # contracts carried non-zero volume in the four sealed close cycles the lake held on
    # 2026-09-16, and a zero-volume contract is exactly what a half-loaded vendor cycle
    # reads as zero, so a set wide enough to admit them is a set the quorum stops
    # protecting. The same cycle changed 45.6 percent of SPY's whole shared set against a
    # 0.50 quorum, a four-point margin, while none of the top 200 by volume changed. 200
    # is that measured margin, not a round number.
    oi_comparable_set_size: int = 200
    # The chain chunker's one constant. A full SPY chain in one request exceeds Schwab's
    # gateway body limit (a 502 with errorcode protocol.http.TooBigBody), so the chain is
    # fetched in date windows and reassembled. The set of windows is not a guard constant.
    # It lives in the machine-owned chain_plan.json, seeded from a measured default and
    # refined by the nightly job, so the plan can drift without a config edit. The one
    # constant here bounds the adaptive fallback: a window that still comes back too big is
    # split at its date midpoint and refetched. This many midpoint splits are tried before
    # the chunker gives up on the offending range. The day-one chain-size measurement, run
    # 2026-09-01, sized both the default plan and this bound: 17 expirations returned at
    # 7.4 MB while the full chain 502'd, so a window near or under ~3 MB has ample margin,
    # and four midpoint splits collapse any oversized window to a single day, which one
    # expiration's ~429 KB always fits.
    chain_chunk_max_split_depth: int = 4
    # The nightly window re-tune's two triggers. The close+15 compaction job groups the
    # day's chains rows by window_start and window_end, takes each plan window's peak
    # per-cycle contract count, and compares it to these two. Both are sized from the
    # day-one measurement, run 2026-09-01: one 7.4 MB response carried 6,278 contracts,
    # about 1.2 KB per contract, and the gateway body limit sits somewhere above 7.4 MB.
    # A window whose peak count is over the max splits at its midpoint offset. 2,500
    # contracts is about 3 MB, well under the limit with room for a dense day. Two
    # adjacent finite windows whose peak counts are both under the min merge into one.
    # 800 contracts is about 1 MB, so a merged pair stays under 2 MB and never nears the
    # split trigger. The open tail is never split and never merged.
    chain_window_max_contracts: int = 2500
    chain_window_min_contracts: int = 800
    # The bar backfill's per-run request budget. `backfill_bars` walks every session the capture
    # spans cover, so a lake whose manifest was rebuilt or restored skips nothing and asks for all
    # of it back to back. Nothing paces that walk, so without a bound one run crosses the vendor
    # ceiling inside its first minute and the ticker-days it loses are never manifested and so are
    # asked for again on the next run. Marketlake #478.
    #
    # The band it sits in is what picks it rather than the digit, and five measurements bound it.
    #
    # 1. The ceiling is 120 a minute per client_id, which the design records as observed and
    #    enforced via 429 rather than contractual. A budget sitting exactly on a non-contractual
    #    ceiling is the wrong place to sit.
    # 2. One run fires at most its budget and the 18:30 job fires once a day, so the budget is also
    #    the most that can reach the vendor in any rolling minute. A budget at or under the ceiling
    #    cannot cross it however fast the run fires, which is why a cap subsumes a pacer here.
    # 3. At 18:30 nothing else draws. CAPTURE_PHASES ends at the 16:15 option close and
    #    `backfill_bars` is the sweep's only vendor caller, so the nightly run has the whole 120.
    # 4. The reservation is owed to the by-hand run instead. `--backfill` takes no date and can be
    #    fired during the session, and the capture loop's draw then is one chain request per ticker
    #    per chain-plan window plus one batched quotes request. At two tickers against the built-in
    #    five-window plan that is 11 a minute, before the midpoint splitter adds any.
    # 5. The steady state is not throttled. Measured read-only against the live lake on an 18:30
    #    clock, a rebuilt manifest today spends 22 requests and an ordinary evening spends 2 to 4.
    #
    # So the band is 22 to 109, and 100 sits inside it leaving 20 for anything else on the same
    # credentials in that minute. The constant is not meaningful past one significant figure,
    # because the ceiling it derives from is itself observed rather than published, so a
    # spuriously precise 109 would claim a precision the input does not have.
    #
    # The guarantee is per run. Two by-hand runs inside one minute put 200 into it, which the
    # design already answers in its own terms: anything else on the same credentials draws from
    # the daemon's 120.
    bars_request_budget: int = 100
    # The capture cycle's two concurrency constants, marketlake #532. A cycle makes one request
    # per chain-plan window per options ticker plus one batched quotes request, 19 at two tickers
    # against the nine-window plan the machine ran on 2026-09-24. Fetched one at a time, a cycle
    # costs the sum of them, and on that afternoon three cycles overran and lost four minutes.
    #
    # The cap is how many requests are in flight at once. It is not the 120-a-minute ceiling,
    # which limits requests per rolling minute and which firing them together does not change.
    # At 20 today's 19 tasks go out in one round, so without a bound a cycle would overrun only
    # once its slowest window passed about 60s, against a mean of about 30s at a cap of 10. The
    # bound below ends the cycle before that, so a slow window now costs its own window rather
    # than the next minute (marketlake #597). What the cap risks is the burst rejection
    # (429-005), whose threshold is unpublished and unmeasured. A cap of 1 fetches exactly as
    # the cycle fetched before #532, with no pool and no stagger, so lowering it here rolls the
    # fetch back on the next cycle. The token-refresh lock and the per-cycle client close from
    # the same change stay in place at a cap of 1.
    capture_max_concurrency: int = 20
    # The pause, in milliseconds, between two submissions to the pool, so a volley leaves over
    # about a second rather than in one instant. The design's figure is "a few tens of
    # milliseconds". It is slept on the injected clock by the thread that submits, and the
    # whole of it comes out of the minute: 19 tasks spend 18 pauses before the last request
    # leaves. So it is bounded at 1000 ms, which already spends 18 seconds of the minute on
    # submission alone. The lever for a burst rejection is the cap above, not this pause. At
    # a stagger of about 3 seconds the submission alone outruns the bound below, so the
    # requests still waiting to leave at the bound are never sent and fail under
    # ``request_abandoned``, every minute.
    capture_stagger_ms: int = 50
    # How long after its minute top a capture cycle waits on its requests, in seconds,
    # marketlake #597. A request not done by the minute's ``snap_ts`` plus this is abandoned:
    # its window or its quote batch is recorded under ``request_abandoned``, the chain lands
    # with whatever its other windows brought, and the cycle ends. Without it one slow
    # request held the whole cycle past its minute, and the loop lost the next minute on every
    # surface. On 2026-09-25 at 16:04 ET, QQQ's chain took 54.2s, and the loop lost 16:05 on all
    # four surfaces.
    #
    # 55 leaves the cycle 5s of its minute to write what landed. A cycle measured at
    # ``4739564`` on a temporary lake, with a synthetic 12,000-contract chain per ticker and a
    # vendor that answered at once, took 1.2s to 1.45s end to end, so the next top is still
    # reached. The option-close cycle's bound sits at the close+5 deadline less the same margin
    # the ordinary bound leaves in its minute, 16:19:55 at the default. So one field moves both,
    # and at every value accepted here no mark taken past close+5 lands tagged ``option_close``.
    #
    # The range is whole seconds from 1 to 59. At 60 or more an ordinary cycle's bound falls
    # in the next minute. The loop no longer waits for a cycle, so that costs the next minute
    # nothing (marketlake #565), but it lets each minute's cycles run into the next, and a
    # vendor that hangs then keeps more requests in flight than one minute makes. At 0 every
    # request is abandoned before it is sent. Monday 2026-09-28's timing file is the first to
    # say how many requests 55 would cut that 59 would keep.
    capture_request_bound_s: int = 55

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object] | None) -> GuardConstants:
        """Merge a config's ``guards`` section over the pinned defaults.

        An unrecognized guard key raises rather than being silently ignored. A typo in
        a recalibration would otherwise revert to the default without a word.
        """
        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            raise ConfigError(f"guards must be a mapping, got {type(mapping).__name__}")
        if not mapping:
            return cls()
        known = {f.name for f in fields(cls)}
        unknown = set(mapping) - known
        if unknown:
            raise ConfigError(f"unknown guard constant(s): {sorted(unknown)}")
        merged = replace(cls(), **dict(mapping))
        # **Four fields are range-checked here, and the rest are not.** This one came first. A
        # zero or negative ``bars_request_budget`` stops the nightly bar fetch for ever, and it
        # does it without being refused anywhere: the run reports success, the ping goes out, and
        # the only thing saying the lake stopped fetching bars is one count on a report line
        # beside two others that are non-zero on a healthy evening. Every command that loads
        # config wraps the load in ``input_errors_exit``, so raising here reaches the operator as
        # one named line and exit 2 from whichever command they ran, at load rather than half way
        # through a walk.
        #
        # This is the instance and not the class. ``from_mapping`` type-checks no value but these
        # four, because ``replace`` does not, and the other fourteen constants carry that gap.
        # Marketlake #487 is the per-field range mechanism for all of them. Reaching for it here
        # would be fixing past the class, so each field a change added is checked at its own
        # site instead.
        #
        # **The type is checked before the range, and that order is the whole point.** A bare
        # ``< 1`` dereferences whatever YAML produced, and ``<`` against an ``int`` raises
        # ``TypeError`` for a string, a list, and a key written with no value. ``input_errors_exit``
        # catches three named classes and not that one, so the operator would meet a traceback and
        # exit 1 from the very check written to hand them one line and exit 2. A key written with
        # no value is not a hypothetical here: ``_optional_text`` below records that exact shape as
        # an operator input this file has to name.
        #
        # ``bool`` is excluded by hand because it is a subclass of ``int``. ``bars_request_budget:
        # yes`` parses to ``True``, which passes a range check against 1 and then bounds the whole
        # nightly walk at a single request. ``float`` is refused for the same reason rather than
        # rounded: a budget is a count of requests, and 1.5 of them is not a quantity the walk can
        # spend.
        budget = merged.bars_request_budget
        if not isinstance(budget, int) or isinstance(budget, bool) or budget < 1:
            raise ConfigError(
                f"bars_request_budget must be a whole number of at least 1, got {budget!r}: "
                "a run that may spend no request never fetches a bar and never says so"
            )
        # The two capture constants from #532 are checked here for the same reason and in the
        # same order, since the cap is advertised as the lever an operator lowers mid-session.
        # Unchecked, a cap of 0 raises ``ValueError`` from ``ThreadPoolExecutor`` inside the
        # cycle, and ``yes`` parses to ``True`` and runs silently as a cap of 1. A negative
        # stagger raises from the clock's sleep. Checked or not, a refused value stops capture,
        # because ``run_cycle_from_config`` loads config every cycle and the daemon exits on the
        # raise. The check makes that one named line rather than a traceback.
        cap = merged.capture_max_concurrency
        if not isinstance(cap, int) or isinstance(cap, bool) or cap < 1:
            raise ConfigError(
                f"capture_max_concurrency must be a whole number of at least 1, got {cap!r}: "
                "it is how many vendor requests a capture cycle has in flight at once"
            )
        stagger = merged.capture_stagger_ms
        if (
            not isinstance(stagger, int)
            or isinstance(stagger, bool)
            or not 0 <= stagger <= _MAX_CAPTURE_STAGGER_MS
        ):
            raise ConfigError(
                f"capture_stagger_ms must be a whole number from 0 to {_MAX_CAPTURE_STAGGER_MS}, "
                f"got {stagger!r}: it is the pause in milliseconds between two capture requests, "
                "and it comes out of the minute once per request; lower "
                "capture_max_concurrency instead to answer a burst rejection"
            )
        bound = merged.capture_request_bound_s
        if (
            not isinstance(bound, int)
            or isinstance(bound, bool)
            or not _MIN_CAPTURE_REQUEST_BOUND_S <= bound <= _MAX_CAPTURE_REQUEST_BOUND_S
        ):
            raise ConfigError(
                "capture_request_bound_s must be a whole number from "
                f"{_MIN_CAPTURE_REQUEST_BOUND_S} to {_MAX_CAPTURE_REQUEST_BOUND_S}, got "
                f"{bound!r}: it is how many seconds after its minute top a capture cycle "
                "waits on its requests, and at 60 or more it runs into the next minute"
            )
        return merged


@dataclass(frozen=True)
class Config:
    """The resolved machine-local configuration."""

    lake_root: Path
    backup_target: Path | BucketTarget
    healthchecks_ping_key: Secret
    ntfy_topic: Secret
    schwab_api_key: Secret
    schwab_app_secret: Secret
    schwab_callback_url: str | None = None
    guards: GuardConstants = field(default_factory=GuardConstants)
    bucket_access_key_id: Secret | None = None
    bucket_secret_access_key: Secret | None = None
    bucket_region: str | None = None
    # The ``role`` string as the file held it, the ``repr`` of any other value, or
    # ``ROLE_ABSENT``. Never the value itself, because a list would make this frozen
    # config unhashable. ``lake.outbox`` names what it read when it is neither role.
    role: str | _Absent = ROLE_ABSENT
    # The ``bucket_credentials`` string as the file held it, or the ``repr`` of any other
    # value, the way ``role`` is stored. An absent key is ``keys``. Only a bucket job
    # checks it, through ``bucket_credential_problems``. It stays out of the repr,
    # because a key named for credentials invites a pasted secret.
    bucket_credentials: str = field(default=CREDENTIALS_FROM_KEYS, repr=False)

    def paths(self) -> LakePaths:
        """The lake path builder rooted at ``lake_root``. The DATA_DIR-to-paths bridge."""
        return LakePaths(self.lake_root)

    def healthchecks_url(self, slug: str) -> str:
        """The health-ping URL for a check ``slug``: ``hc-ping.com/<ping-key>/<slug>``.

        Built here so the ping key stays wrapped in ``Secret`` everywhere else. Log the
        slug, never this URL.
        """
        return f"https://{HEALTHCHECKS_HOST}/{self.healthchecks_ping_key.reveal()}/{slug}"

    def page_secrets(self) -> tuple[str, ...]:
        """The values a page must never carry, for every ``Publisher`` that sends one.

        The healthchecks ping key and the ntfy topic always, and the bucket's two key
        values when the file holds them. One method rather than a tuple at every
        construction site, so a secret added here reaches every publisher at once.
        """
        values = [self.healthchecks_ping_key.reveal(), self.ntfy_topic.reveal()]
        for secret in (self.bucket_access_key_id, self.bucket_secret_access_key):
            if secret is not None:
                values.append(secret.reveal())
        return tuple(values)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> Config:
        """Build a config from an already-parsed mapping.

        This is the value-only core that ``load_config`` calls after reading YAML. A
        missing required key raises ``ConfigError`` naming the key. Paths carrying a
        leading ``~`` are expanded to the home directory. ``schwab_callback_url`` is not
        a required key, so a mapping without it yields ``None`` there and every other
        value as usual. ``role`` is stored as read when it is a string and as its
        ``repr`` otherwise, and a mapping without it yields ``ROLE_ABSENT``.
        ``bucket_credentials`` is stored the same way, and a mapping without it yields
        ``keys``.
        """
        missing = [key for key in _REQUIRED_KEYS if mapping.get(key) is None]
        if missing:
            raise ConfigError(f"config missing required key(s): {missing}")
        backup_target = parse_backup_target(mapping["backup_target"])
        key_id = _optional_text(mapping.get(BUCKET_KEY_ID_KEY))
        secret_key = _optional_text(mapping.get(BUCKET_SECRET_KEY))
        return cls(
            lake_root=Path(str(mapping["lake_root"])).expanduser(),
            backup_target=backup_target,
            healthchecks_ping_key=Secret(str(mapping["healthchecks_ping_key"])),
            ntfy_topic=Secret(str(mapping["ntfy_topic"])),
            schwab_api_key=Secret(str(mapping["schwab_api_key"])),
            schwab_app_secret=Secret(str(mapping["schwab_app_secret"])),
            schwab_callback_url=_optional_text(mapping.get(CALLBACK_KEY)),
            guards=GuardConstants.from_mapping(mapping.get("guards")),
            bucket_access_key_id=None if key_id is None else Secret(key_id),
            bucket_secret_access_key=None if secret_key is None else Secret(secret_key),
            bucket_region=_optional_text(mapping.get(BUCKET_REGION_KEY)),
            role=_role_text(mapping[ROLE_KEY]) if ROLE_KEY in mapping else ROLE_ABSENT,
            bucket_credentials=(
                _role_text(mapping[BUCKET_CREDENTIALS_KEY])
                if BUCKET_CREDENTIALS_KEY in mapping
                else CREDENTIALS_FROM_KEYS
            ),
        )


def _role_text(value: object) -> str:
    """A present ``role`` value as a string: itself when it is one, its ``repr`` if not.

    So an empty ``role:`` stores ``"None"`` and ``role: off`` stores ``"False"``, and
    ``lake.outbox`` still names what the file held. ``bucket_credentials`` is read the
    same way, so a blank one stores ``"None"`` and a bucket job refuses it.
    """
    return value if isinstance(value, str) else repr(value)


def _optional_text(value: object) -> str | None:
    """An optional string value, or ``None`` when the key is absent or left empty.

    A key written with no value parses to ``None``, and one written as blank spaces
    parses to a string that names nothing. Both mean the operator has not set it, so
    both become ``None`` and the one tool that needs the value refuses with the key
    named rather than carrying an empty string into a login flow.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_config(
    path: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Config:
    """Load the machine-local config.

    Path precedence: an explicit ``path`` argument, then the ``MARKETLAKE_CONFIG``
    environment variable, then the default ``~/.config/marketlake/config.yaml``. A test
    passes ``path`` or an ``env`` mapping to point the loader at a throwaway file.

    A parse failure names the file and nothing else. PyYAML quotes the offending line
    back in its message, and four to six of this file's values are secrets, so a stray quote
    on the ping-key line would put that key in the error. A scheduled job's stdout and
    stderr go to the service manager's log, a file under launchd and the journal under
    systemd, so an uncaught traceback leaves the key in that log.
    ``_parse_yaml`` drops the parse error rather than chaining it, and the ``ConfigError``
    is raised outside that handler, so the quoted line is on neither the traceback nor
    the exception's ``__context__``.
    """
    resolved = _resolve_path(path, env, CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH)
    if not resolved.exists():
        raise ConfigError(f"config file not found: {resolved}")
    mapping = _parse_yaml(_read_text(resolved, "config"))
    if mapping is None:
        raise ConfigError(f"config file is not valid YAML: {resolved}")
    if not isinstance(mapping, Mapping):
        raise ConfigError(f"config file is not a mapping: {resolved}")
    return Config.from_mapping(mapping)


def _parse_yaml(text: str) -> object | None:
    """The parsed YAML, or ``None`` when ``text`` is not YAML at all.

    The parse error stays inside this function and is never re-raised. Its message
    quotes the offending source line, and this file holds four to six secrets, so letting it
    out would put one of them wherever the caller's error lands. An empty file and a
    ``null`` document both parse to an empty mapping, so ``None`` means the parse
    failed and nothing else.
    """
    try:
        return yaml.safe_load(text) or {}
    except yaml.YAMLError:
        return None


@contextmanager
def input_errors_exit(command: str, *refusals: type[Exception]) -> Iterator[None]:
    """Turn a bad operator input file into one named line and exit 2.

    Three machine-local files are the operator's to edit, and all three sit in the
    config directory: ``config.yaml``, ``tickers.yaml``, and ``chain_plan.json``. A
    malformed one is an operator mistake, not a bug, so a traceback names the wrong
    thing. The loader is the last frame printed and the line that matters sits under a
    stack to read past. This prints that line and exits 2, the code and the shape
    ``argparse`` already uses for a bad argument in these same entries.

    It wraps the call rather than the load, because two entries load their files inside
    a library helper. Those helpers keep raising, and only a ``main`` turns an
    exception into an exit code.

    ``refusals`` names further classes an entry refuses with in the same shape. The
    bucket's first-upload command passes its own refusal, so a refusal that is not about
    an input file still reaches the operator as one line rather than a traceback.
    """
    try:
        yield
    except (ChainPlanError, ConfigError, TickersError, *refusals) as exc:
        print(f"{command}: {exc}", file=sys.stderr)
        raise SystemExit(2) from None


def _read_text(resolved: Path, kind: str) -> str:
    """The file's text, or a ``ConfigError`` naming what could not be read.

    ``exists()`` passing does not mean the file can be read. A path one character
    short of the file names its directory, a restrictive mode makes it unreadable, and
    a binary file is not text. Each of those raised a bare ``OSError`` before, which is
    the traceback this module exists to avoid. Only the path is named, never the
    exception's own message, so nothing from inside the file can reach the error.
    """
    try:
        return resolved.read_text()
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"{kind} file cannot be read: {resolved}") from None


def _resolve_path(
    path: str | Path | None,
    env: Mapping[str, str] | None,
    env_key: str,
    default: Path,
) -> Path:
    """Resolve a config path: explicit argument, then env var, then the default.

    An argument and an environment override are whatever a person typed, so both may
    carry a ``~`` and both are expanded. ``default`` comes from ``lake.paths`` already
    resolved, so it is returned as it is. A caller passing an unexpanded default would
    get it back unexpanded.
    """
    if path is not None:
        return Path(path).expanduser()
    env = os.environ if env is None else env
    override = env.get(env_key)
    if override:
        return Path(override).expanduser()
    return default
