"""The AWS client builder every client in this package shares.

Three clients reach AWS. ``lake.bucket`` uploads the lake to S3, ``lake.token_store``
puts and gets the Schwab token in SSM Parameter Store, marketlake #636, and
``lake.vm_config`` reads the hosted VM's config parameters to render its ``config.yaml``,
marketlake #686. The first two must be built from ``config.yaml`` alone, which is the
design's isolation rule in its Configuration section: never from an ``AWS_*`` variable,
never from ``~/.aws/config`` or ``~/.aws/credentials``, and never with a service model
from ``~/.aws/models``. A development run then cannot reach a real bucket or a real
parameter on credentials it happened to find on the machine. The render keeps every part
of that rule except the file it reads. It writes ``config.yaml``, so it takes its region
from the tracked ``config/vm.yaml`` instead, and always signs with the instance profile.
An assume-role source, below, gives the bucket or the put an STS client of its own,
built here under the same rule, which signs only ``AssumeRole``.

This module holds that rule once, so the three clients cannot drift apart. It imports
nothing from the package but ``lake.config`` and ``lake.clock``, so the re-auth command,
which puts the token, never loads ``lake.bucket`` or ``lake.manifest``.

The builder takes its credentials as arguments and never a ``Config``. A *source* is one
of three things.

1. A ``KeyPair``, an access key id and a secret access key, each still wrapped in
   ``Secret``. The bucket's ``keys`` path uses one.
2. ``INSTANCE_PROFILE``, which asks the EC2 instance metadata service for short-lived
   credentials and nowhere else. The bucket's ``instance_profile`` path, the token's
   pull on the VM and the config render use it. It is the one exception to building a
   client from ``config.yaml`` alone. ``config.yaml`` is still what decides to take it
   for the bucket and the pull. The render always takes it, and refuses a
   ``config/vm.yaml`` whose ``bucket_credentials`` names any other source.
3. An ``AssumeRole``: a role ARN, the command key as a ``KeyPair``, and a fixed session
   name. The command key signs only STS's ``AssumeRole``, and the client signs with the
   short-lived credentials STS returns, marketlake #737. The bucket's ``assume_role``
   path and the token's put use one, as the sessions ``marketlake-bucket`` and
   ``marketlake-token-put``, so CloudTrail shows each call under
   ``assumed-role/<role>/<session>``.

``source_from_bucket_credentials`` turns the config's ``bucket_credentials`` into a
source. The bucket and the pull share it. The put never calls it, because the put
assumes a role of its own, and neither does the render, which always takes
``INSTANCE_PROFILE``.

``boto3`` and ``botocore`` are imported inside the builder, lazily, so the offline suite
never loads them unless a test builds a client.
"""

from __future__ import annotations

import copy
import os
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from typing import Any, NamedTuple

from lake.clock import Clock, SystemClock
from lake.config import (
    BUCKET_KEY_ID_KEY,
    BUCKET_ROLE_ARN_KEY,
    BUCKET_SECRET_KEY,
    COMMAND_KEY_ID_KEY,
    COMMAND_SECRET_KEY,
    CREDENTIALS_FROM_ASSUME_ROLE,
    CREDENTIALS_FROM_INSTANCE_PROFILE,
    CREDENTIALS_FROM_KEYS,
    UNRECOGNISED_CREDENTIALS,
    Config,
    ConfigError,
    Secret,
)


class KeyPair(NamedTuple):
    """An access key id and its secret access key, both still wrapped in ``Secret``."""

    access_key_id: Secret
    secret_access_key: Secret


class AssumeRole(NamedTuple):
    """The source that assumes ``role_arn`` with ``keys``, as the session ``session_name``.

    ``keys`` is the command key. It signs STS's ``AssumeRole`` and nothing else, and the
    client signs every request with the short-lived credentials STS returns.
    """

    role_arn: str
    keys: KeyPair
    session_name: str


# The session names CloudTrail records each assumed role under, marketlake #737. Fixed, so
# a call reads as ``assumed-role/<role>/marketlake-bucket`` or ``.../marketlake-token-put``.
BUCKET_SESSION_NAME = "marketlake-bucket"
TOKEN_PUT_SESSION_NAME = "marketlake-token-put"


class _InstanceProfile(Enum):
    """The type of ``INSTANCE_PROFILE``. An ``Enum`` member, so it compares by identity."""

    INSTANCE_PROFILE = "INSTANCE_PROFILE"

    def __repr__(self) -> str:
        return "INSTANCE_PROFILE"


# The source that asks the instance metadata service for the credentials.
INSTANCE_PROFILE = _InstanceProfile.INSTANCE_PROFILE

Source = KeyPair | _InstanceProfile | AssumeRole


def source_from_bucket_credentials(config: Config) -> Source:
    """The credential source ``bucket_credentials`` names, or a ``ConfigError``.

    ``keys`` is the two bucket key values, ``instance_profile`` is the metadata service,
    and ``assume_role`` is the command key assuming the role ``bucket_role_arn`` names.
    Each path is taken only on its own value, so any other value refuses as one line and
    never falls back to ``keys``, even when the config holds valid keys. The value read
    is never quoted, for the reason ``bucket_credential_problems`` gives.
    """
    if config.bucket_credentials == CREDENTIALS_FROM_INSTANCE_PROFILE:
        return INSTANCE_PROFILE
    if config.bucket_credentials == CREDENTIALS_FROM_ASSUME_ROLE:
        key_id, secret_key = config.command_access_key_id, config.command_secret_access_key
        role_arn = config.bucket_role_arn
        if key_id is None or secret_key is None or role_arn is None:
            absent = [
                key
                for key, value in (
                    (COMMAND_KEY_ID_KEY, key_id),
                    (COMMAND_SECRET_KEY, secret_key),
                    (BUCKET_ROLE_ARN_KEY, role_arn),
                )
                if value is None
            ]
            raise ConfigError(f"the bucket needs config key(s): {absent}")
        return AssumeRole(role_arn, KeyPair(key_id, secret_key), BUCKET_SESSION_NAME)
    if config.bucket_credentials == CREDENTIALS_FROM_KEYS:
        key_id, secret_key = config.bucket_access_key_id, config.bucket_secret_access_key
        if key_id is None or secret_key is None:
            absent = [
                key
                for key, value in ((BUCKET_KEY_ID_KEY, key_id), (BUCKET_SECRET_KEY, secret_key))
                if value is None
            ]
            raise ConfigError(f"the bucket needs config key(s): {absent}")
        return KeyPair(key_id, secret_key)
    raise ConfigError(UNRECOGNISED_CREDENTIALS)


_ENVIRONMENT_LOCK = threading.Lock()


@contextmanager
def _aws_environment_cleared() -> Iterator[None]:
    """Hide every ``AWS_*`` variable and both ``~/.aws`` files while a client is built.

    ``botocore`` reads its settings from the environment and from ``~/.aws/config`` as
    well as from what it is handed. An ``AWS_ENDPOINT_URL`` would send requests signed
    with the config's credentials to another host, an ``AWS_PROFILE`` naming no profile
    would refuse to build the client, and an ``AWS_REGION`` would sign for the wrong
    region. So for the length of the build the variables are removed, the two files are
    pointed at the null device, and botocore's own instance-metadata lookup is turned
    off. Everything is put back afterwards, so the process's environment is the same on
    the way out.

    This holds on every credential source. The instance-profile source reaches the
    metadata service only through :func:`_metadata_resolver`, whose fetcher ignores the
    environment, so turning the lookup off here does not turn that one off. The
    assume-role source refreshes its credentials long after this has put the
    environment back, and it stays inside the rule all the same: the refresh calls an STS
    client built in here and resolves no setting of its own, so it never reads an
    ``AWS_*`` variable or ``~/.aws``.
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


@contextmanager
def _proxies_cleared() -> Iterator[None]:
    """Hide every variable whose lowercased name ends in ``_proxy``, and put them back.

    The metadata fetcher reads the proxy variables once, when it is built, and keeps
    them, so a proxy set then would carry the metadata lookup and the credentials it
    returns through that proxy. The match is on the lowercased name because urllib
    lowercases names, so ``HTTP_PROXY`` takes effect as surely as ``http_proxy``. The
    ``finally`` matters as much as the removal: a fetcher that fails to build must not
    take the process's proxies with it, Sunday's Schwab canary included.
    """
    saved = {key: value for key, value in os.environ.items() if key.lower().endswith("_proxy")}
    for key in saved:
        del os.environ[key]
    try:
        yield
    finally:
        os.environ.update(saved)


# The instance metadata service's address. Tests point it at a server on loopback, since
# ``_aws_environment_cleared`` removes ``AWS_EC2_METADATA_SERVICE_ENDPOINT`` with every
# other ``AWS_*`` variable. Only this module holds it. A copy elsewhere would be one a
# test could patch while the builder kept reading this one.
METADATA_BASE_URL = "http://169.254.169.254/"

# How long one metadata request may wait, in seconds, and how many tries each gets. A
# hung service then fails the build in about 2 seconds, measured on marketlake #663.
METADATA_TIMEOUT_S = 1
METADATA_ATTEMPTS = 2


def _metadata_resolver() -> Any:
    """A credential resolver that asks the instance metadata service and nothing else.

    botocore's default chain reads ``BOTO_CONFIG``, ``/etc/boto.cfg`` and ``~/.boto``
    ahead of the metadata service, so this resolver holds one provider. The fetcher
    takes ``env={}``, because built inside :func:`_aws_environment_cleared` with its
    default it would read ``AWS_EC2_METADATA_DISABLED`` and return nothing. It uses
    IMDSv2 only, the form that needs a session token. Only its construction runs
    inside :func:`_proxies_cleared`, because the client reads the proxy variables too,
    and those stay as they are on every credential source. The STS client the
    assume-role source builds honours them the way the S3 and SSM clients do.
    """
    from botocore.credentials import CredentialResolver, InstanceMetadataProvider
    from botocore.utils import InstanceMetadataFetcher

    with _proxies_cleared():
        fetcher = InstanceMetadataFetcher(
            timeout=METADATA_TIMEOUT_S,
            num_attempts=METADATA_ATTEMPTS,
            base_url=METADATA_BASE_URL,
            env={},
            config={"ec2_metadata_v1_disabled": True},
        )
    return CredentialResolver(providers=[InstanceMetadataProvider(iam_role_fetcher=fetcher)])


# The STS endpoint the assume-role source's client sends ``AssumeRole`` to. ``None`` in
# production, so botocore resolves the regional endpoint for the service client's region,
# which also decides where CloudTrail logs the call. Tests point it at a server on
# loopback, since ``_aws_environment_cleared`` removes ``AWS_ENDPOINT_URL_STS`` with every
# other ``AWS_*`` variable. Only this module holds it, for the reason
# ``METADATA_BASE_URL`` gives.
STS_ENDPOINT_URL: str | None = None

# The STS client's timeouts in seconds and its retries, botocore's standard ones. Three
# retries are four tries, and a hung STS then fails in about 41 seconds, measured on
# marketlake #737. Reusing the S3 client's settings would stall a refresh for minutes.
STS_CONNECT_TIMEOUT_S = 5
STS_READ_TIMEOUT_S = 10
STS_MAX_ATTEMPTS = 3

# The codes AWS uses to say it is busy or briefly unable to answer. These and any 5xx or
# 429 mean *unreachable* rather than *refused*, because the repair for all of them is
# usually to wait. ``lake.bucket`` reads them for S3's answers too, and they live here
# because the assume-role refresh, which sorts STS's answers, lives here.
_UNAVAILABLE_CODES = frozenset(
    {
        "InternalError",
        "RequestTimeout",
        "RequestLimitExceeded",
        "ServiceUnavailable",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "TooManyRequests",
    }
)


class _AssumeRoleFailed(Exception):
    """STS gave no usable credentials for the role, at the first request or a refresh.

    ``code`` is the AWS error code, the type name of a botocore transport or client-side
    error, or one of this module's names for an answer it refused. It is never STS's
    message, which names the account and the calling principal. ``transient`` says
    whether waiting is likely to fix it: an unavailable code, an HTTP 429 or 5xx, or a
    connection that failed. Anything else, a client-side ``ParamValidationError``
    included, is a refusal, whose repair is the command key, a role ARN, or a policy.

    A plain ``Exception``, because each obvious base sends it down a wrong path.
    ``lake.bucket`` reads a ``BotoCoreError`` as unreachable, the re-auth reads a
    ``ClientError`` as the put's own failure, a ``ValueError`` reads as a client that
    could not be built, and an ``OSError`` is one of the failures a ping swallows.
    """

    def __init__(self, code: str, *, transient: bool) -> None:
        super().__init__(f"AssumeRole {code}")
        self.code = code
        self.transient = transient


class _AssumeRoleRefresh:
    """The refresh an assume-role source's credentials call: one ``AssumeRole`` on ``sts``.

    ``sts`` is built inside :func:`_aws_environment_cleared` and kept, so a refresh, which
    can come an hour into a restore, builds no client and resolves no setting. It returns
    what ``botocore.credentials.RefreshableCredentials`` reads, with the expiry as
    ``isoformat()``. botocore's own refresher formats it with ``%Z``, which drops a
    non-UTC offset and leaves a naive time that fails after this returns.

    Every failure raises ``_AssumeRoleFailed``. The code and ``transient`` are taken
    inside the handler, and the raise comes after the handler closes, as
    ``lake.token_store.push`` and ``lake.config.load_config`` do it. So STS's error, whose
    message names the account and the principal ARN, is on neither ``__cause__`` nor
    ``__context__`` and reaches no traceback. ``from None`` alone would only hide it from
    the printed one.
    """

    def __init__(self, sts: Any, source: AssumeRole, clock: Clock) -> None:
        self.sts = sts
        self.role_arn = source.role_arn
        self.session_name = source.session_name
        self.clock = clock

    def __call__(self) -> dict[str, str]:
        from botocore.exceptions import BotoCoreError, ClientError, HTTPClientError
        from botocore.exceptions import ConnectionError as BotoConnectionError

        failed: _AssumeRoleFailed | None = None
        try:
            response = self.sts.assume_role(
                RoleArn=self.role_arn, RoleSessionName=self.session_name
            )
        except ClientError as exc:
            failed = _sts_refusal(exc)
        except (BotoConnectionError, HTTPClientError) as exc:
            failed = _AssumeRoleFailed(type(exc).__name__, transient=True)
        except BotoCoreError as exc:
            failed = _AssumeRoleFailed(type(exc).__name__, transient=False)
        if failed is not None:
            raise failed
        return _session_credentials(response, self.clock.now())


def _sts_refusal(exc: Any) -> _AssumeRoleFailed:
    """An STS ``ClientError`` as ``_AssumeRoleFailed``, sorted while its status is known."""
    response = getattr(exc, "response", None)
    response = response if isinstance(response, Mapping) else {}
    error = response.get("Error")
    code = error.get("Code") if isinstance(error, Mapping) else None
    code = str(code) if code else type(exc).__name__
    metadata = response.get("ResponseMetadata")
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, Mapping) else None
    status = status if isinstance(status, int) else 0
    transient = code in _UNAVAILABLE_CODES or status == 429 or status >= 500
    return _AssumeRoleFailed(code, transient=transient)


def _session_credentials(response: Any, now: datetime) -> dict[str, str]:
    """The credentials an ``AssumeRole`` answer carries, or ``_AssumeRoleFailed``.

    An answer missing a value, or whose expiry is naive or already past, is refused here.
    botocore would otherwise raise ``TypeError`` or ``RuntimeError`` for the expiry after
    the refresh returned, outside every caller's handling.
    """
    credentials = response.get("Credentials") if isinstance(response, Mapping) else None
    credentials = credentials if isinstance(credentials, Mapping) else {}
    values = [credentials.get(name) for name in ("AccessKeyId", "SecretAccessKey", "SessionToken")]
    if not all(isinstance(value, str) and value for value in values):
        raise _AssumeRoleFailed("IncompleteCredentials", transient=False)
    expiry = credentials.get("Expiration")
    if not isinstance(expiry, datetime) or expiry.utcoffset() is None:
        raise _AssumeRoleFailed("NaiveExpiration", transient=False)
    if expiry <= now:
        raise _AssumeRoleFailed("ExpiredCredentials", transient=False)
    access_key, secret_key, token = values
    return {
        "access_key": access_key,
        "secret_key": secret_key,
        "token": token,
        "expiry_time": expiry.isoformat(),
    }


def _assume_role_resolver(core: Any, region: str | None, source: AssumeRole) -> Any:
    """A credential resolver whose one credential assumes ``source``'s role on first use.

    It builds the STS client here, inside the caller's :func:`_aws_environment_cleared`,
    signed with the command key and holding the STS constants. No STS call is made yet.
    The credentials are botocore's ``DeferredRefreshableCredentials``, so the first
    request assumes the role, and every later request in the last minutes of a session
    assumes it again through the same client.
    """
    from botocore.config import Config as BotoConfig
    from botocore.credentials import (
        CredentialProvider,
        CredentialResolver,
        DeferredRefreshableCredentials,
    )

    sts = core.create_client(
        "sts",
        region_name=region,
        endpoint_url=STS_ENDPOINT_URL,
        aws_access_key_id=source.keys.access_key_id.reveal(),
        aws_secret_access_key=source.keys.secret_access_key.reveal(),
        config=BotoConfig(
            connect_timeout=STS_CONNECT_TIMEOUT_S,
            read_timeout=STS_READ_TIMEOUT_S,
            retries={"mode": "standard", "max_attempts": STS_MAX_ATTEMPTS},
        ),
    )
    # botocore decides when to refresh by the same clock the refresh checks an expiry
    # against, so the two cannot disagree about whether a session has ended.
    clock = SystemClock()
    credentials = DeferredRefreshableCredentials(
        refresh_using=_AssumeRoleRefresh(sts, source, clock),
        method="assume-role",
        time_fetcher=clock.now,
    )

    class _Assumed(CredentialProvider):
        METHOD = "assume-role"
        CANONICAL_NAME = "custom-marketlake-assume-role"

        def load(self) -> Any:
            return credentials

    return CredentialResolver(providers=[_Assumed()])


class _MetadataLookupFailed(Exception):
    """The instance-profile lookup found no usable credentials.

    ``detail`` names why without quoting the answer: the type of what the lookup raised,
    ``none returned`` for a reachable service with no instance profile attached, or
    ``incomplete credentials`` for an answer missing its key. Each caller words its own
    refusal from it. The bucket names both fixes, and the token pull reports it as a
    delay worth retrying.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def build_client(
    service: str,
    *,
    region: str | None,
    source: Source,
    client_config: Mapping[str, Any],
) -> Any:
    """A ``service`` client signed by ``source`` in ``region``, and nothing else.

    :func:`_aws_environment_cleared` keeps the environment and ``~/.aws`` out of the
    build, and the service models load from the installed ``botocore`` only. On a
    ``KeyPair`` the two values are passed explicitly. On ``INSTANCE_PROFILE`` the
    credentials are fetched before the client is built, so a host with no instance
    profile raises ``_MetadataLookupFailed`` here rather than failing at the first
    request.

    On an ``AssumeRole`` the STS client is built here and no STS call is made. The first
    request assumes the role, and a failure then, or at a refresh, raises
    ``_AssumeRoleFailed`` from that request. The two sources differ on purpose. An
    instance profile that serves nothing is a fault of the host's setup, which a refusal
    at build names. An STS that is briefly unavailable is not, and a failure at build
    would read as a config fault for as long as the client lives, which on Sunday is the
    whole job. Assuming at the first request gives both failure times one path.

    ``client_config`` holds the keyword arguments of ``botocore.config.Config``,
    so a caller states its timeouts and checksum settings without importing botocore.
    It is deep-copied first, because botocore rewrites the ``retries`` dict it is handed
    in place, and a caller's module constant would otherwise change on the first build.

    ``botocore`` refuses a malformed region with an error that is both a
    ``BotoCoreError`` and a ``ValueError``, and that reaches the caller unchanged.
    """
    with _aws_environment_cleared():
        import boto3  # lazy: only a job that reaches AWS builds a client
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
        if source is INSTANCE_PROFILE:
            core.register_component("credential_provider", _metadata_resolver())
            # Fetched before the client is built. The session keeps a credential it
            # found, so ``session.client`` signs with it without a second lookup.
            try:
                credentials = core.get_credentials()
            except Exception as exc:
                # An unreachable or token-refusing service raises botocore's own error,
                # and an answer botocore cannot parse raises a plain TypeError or
                # ValueError. Each is a failed lookup, so only the type is named.
                raise _MetadataLookupFailed(type(exc).__name__) from None
            if credentials is None:
                raise _MetadataLookupFailed("none returned")
            frozen = credentials.get_frozen_credentials()
            if not (frozen.access_key and frozen.secret_key):
                raise _MetadataLookupFailed("incomplete credentials")
            keys: dict[str, str] = {}
        elif isinstance(source, AssumeRole):
            core.register_component(
                "credential_provider", _assume_role_resolver(core, region, source)
            )
            keys = {}
        elif isinstance(source, KeyPair):
            keys = {
                "aws_access_key_id": source.access_key_id.reveal(),
                "aws_secret_access_key": source.secret_access_key.reveal(),
            }
        else:
            raise TypeError(
                f"source must be a KeyPair, INSTANCE_PROFILE or AssumeRole, got {type(source)}"
            )
        session = boto3.session.Session(botocore_session=core)
        settings = copy.deepcopy(dict(client_config))
        return session.client(service, region_name=region, **keys, config=BotoConfig(**settings))


__all__ = [
    "BUCKET_SESSION_NAME",
    "INSTANCE_PROFILE",
    "METADATA_ATTEMPTS",
    "METADATA_BASE_URL",
    "METADATA_TIMEOUT_S",
    "STS_CONNECT_TIMEOUT_S",
    "STS_ENDPOINT_URL",
    "STS_MAX_ATTEMPTS",
    "STS_READ_TIMEOUT_S",
    "TOKEN_PUT_SESSION_NAME",
    "AssumeRole",
    "KeyPair",
    "Source",
    "build_client",
    "source_from_bucket_credentials",
]
