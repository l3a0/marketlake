"""The AWS client builder every client in this package shares.

Two clients reach AWS. ``lake.bucket`` uploads the lake to S3, and ``lake.token_store``
puts and gets the Schwab token in SSM Parameter Store, marketlake #636. Both must be
built from ``config.yaml`` alone, which is the design's isolation rule in its
Configuration section: never from an ``AWS_*`` variable, never from ``~/.aws/config`` or
``~/.aws/credentials``, and never with a service model from ``~/.aws/models``. A
development run then cannot reach a real bucket or a real parameter on credentials it
happened to find on the machine.

This module holds that rule once, so the two clients cannot drift apart. It imports
nothing from the package but ``lake.config``, so the re-auth command, which puts the
token, never loads ``lake.bucket`` or ``lake.manifest``.

The builder takes its credentials as arguments and never a ``Config``. A *source* is one
of two things.

1. A ``KeyPair``, an access key id and a secret access key, each still wrapped in
   ``Secret``. The bucket's ``keys`` path and the token's put use one.
2. ``INSTANCE_PROFILE``, which asks the EC2 instance metadata service for short-lived
   credentials and nowhere else. The bucket's ``instance_profile`` path and the token's
   pull on the VM use it. It is the one exception to building a client from
   ``config.yaml`` alone, and ``config.yaml`` is still what decides to take it.

``source_from_bucket_credentials`` turns the config's ``bucket_credentials`` into a
source. The bucket and the pull share it, and the put never calls it, because the put
reads keys of its own.

``boto3`` and ``botocore`` are imported inside the builder, lazily, so the offline suite
never loads them unless a test builds a client.
"""

from __future__ import annotations

import copy
import os
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from enum import Enum
from typing import Any, NamedTuple

from lake.config import (
    BUCKET_KEY_ID_KEY,
    BUCKET_SECRET_KEY,
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


class _InstanceProfile(Enum):
    """The type of ``INSTANCE_PROFILE``. An ``Enum`` member, so it compares by identity."""

    INSTANCE_PROFILE = "INSTANCE_PROFILE"

    def __repr__(self) -> str:
        return "INSTANCE_PROFILE"


# The source that asks the instance metadata service for the credentials.
INSTANCE_PROFILE = _InstanceProfile.INSTANCE_PROFILE

Source = KeyPair | _InstanceProfile


def source_from_bucket_credentials(config: Config) -> Source:
    """The credential source ``bucket_credentials`` names, or a ``ConfigError``.

    ``keys`` is the two bucket key values, and ``instance_profile`` is the metadata
    service. Each path is taken only on its own value, so any other value refuses as one
    line and never falls back to ``keys``, even when the config holds valid keys. The
    value read is never quoted, for the reason ``bucket_credential_problems`` gives.
    """
    if config.bucket_credentials == CREDENTIALS_FROM_INSTANCE_PROFILE:
        return INSTANCE_PROFILE
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

    This holds on both credential sources. The instance-profile source reaches the
    metadata service only through :func:`_metadata_resolver`, whose fetcher ignores the
    environment, so turning the lookup off here does not turn that one off.
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
    and those stay as they are on both credential sources.
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
    request. ``client_config`` holds the keyword arguments of ``botocore.config.Config``,
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
        elif isinstance(source, KeyPair):
            keys = {
                "aws_access_key_id": source.access_key_id.reveal(),
                "aws_secret_access_key": source.secret_access_key.reveal(),
            }
        else:
            raise TypeError(f"source must be a KeyPair or INSTANCE_PROFILE, got {type(source)}")
        session = boto3.session.Session(botocore_session=core)
        settings = copy.deepcopy(dict(client_config))
        return session.client(service, region_name=region, **keys, config=BotoConfig(**settings))


__all__ = [
    "INSTANCE_PROFILE",
    "METADATA_ATTEMPTS",
    "METADATA_BASE_URL",
    "METADATA_TIMEOUT_S",
    "KeyPair",
    "Source",
    "build_client",
    "source_from_bucket_credentials",
]
