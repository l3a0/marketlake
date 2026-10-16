"""The fake S3 client.

``FakeS3`` stands in for the ``boto3`` S3 client the bucket backup uses. It answers the
six calls ``lake.bucket`` makes with the shapes S3 answers them, and raises the errors
S3 raises as real ``botocore`` ``ClientError`` objects, so the code under test sorts them
the way it would sort the real ones. No socket is opened, and the suite's network guard
would fail the test if one were.

Two provider behaviors are modelled, because a fake that skipped either would leave the
code that depends on it untested.

1. **A PUT whose ``ChecksumSHA256`` does not match its bytes is refused.** The value is
   base64-decoded and compared as raw digest bytes, the way S3 compares it. A value that
   does not decode to exactly 32 bytes is refused as ``InvalidRequest``, which is what
   the manifest's 64 hex characters sent unconverted would meet: hex characters are
   valid base64, so the value decodes, to 48 bytes. A value that decodes to the wrong 32
   bytes is refused as ``BadDigest``, HTTP 400.
2. **Every PUT is a new version.** A key keeps a list of versions, newest last, so a test
   can count versions to prove a second run sent nothing. ``GetObject`` and ``HeadObject``
   answer with the ``VersionId`` of the version they served, on a plain request for the
   current version as on a request naming one, which is what S3 does on a versioned
   bucket. ``ListObjectVersions`` lists every version of every key under its ``Prefix``,
   sibling keys such as ``trimmed.jsonl.x`` included, newest first within a key, and pages
   by ``KeyMarker`` and ``VersionIdMarker`` as S3 does (marketlake #838).

``HeadObject`` on a missing key or a missing version answers a bare 404, as S3 does for a
key that holds ``s3:ListBucket``, and its error carries no body. A delete marker, which
``delete_marker`` adds, shows only in the version listing, and a ``HeadObject`` naming it
answers 405 as S3 does. ``fail_with`` makes every call raise one chosen error, which is how
a test models a revoked key or a network that is down. ``deny_versioned_get`` refuses a
``GetObject`` or a ``HeadObject`` that names a version, the two calls ``s3:GetObjectVersion``
covers, and ``deny_list_versions`` refuses ``ListObjectVersions``, so a test can model a role
without those grants. ``deny_versioned_get_object`` refuses only a ``GetObject`` that names a
version and lets a ``HeadObject`` naming one through, so a test can reach a refused read that
follows a lookup whose requests all passed. ``on_put`` and ``on_get`` run before a
``PutObject`` or a ``GetObject`` is answered, so a test can fail or time one kind of call
alone.

Every stored version carries a ``LastModified``, which ``ListObjectsV2``, ``HeadObject`` and
``GetObject`` answer with, as S3 does. ``store`` takes one, and a version stored without one
gets the fake's own clock, which starts at ``EPOCH`` and moves one second per version, so a
later write is always newer. ``deny_prefixes`` refuses a ``ListObjectsV2`` whose ``Prefix``,
or a ``HeadObject`` whose ``Key``, starts with one of them, as S3 refuses a denied request:
AccessDenied for the list, and a bare ``403`` for the head, whose answer carries no body to
name a code. That is how a test models a list grant conditioned on ``s3:prefix`` that no
longer covers a path. The cost read's manifest barrier and its removed grant are tested
through these (marketlake #913).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from botocore.exceptions import ClientError, EndpointConnectionError

FULL_OBJECT = "FULL_OBJECT"

# Where the fake's clock starts. A version stored without a ``last_modified`` gets this plus
# one second per version stored before it.
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


def client_error(code: str, operation: str, status: int = 400) -> ClientError:
    """A ``ClientError`` in the shape ``botocore`` raises for an S3 error answer."""
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


def unreachable() -> EndpointConnectionError:
    """The error ``botocore`` raises when no connection can be made."""
    return EndpointConnectionError(endpoint_url="https://s3.us-east-2.amazonaws.com")


@dataclass
class Version:
    """One stored version of a key."""

    version_id: str
    body: bytes
    checksum: str | None
    checksum_type: str | None = FULL_OBJECT
    storage_class: str | None = None
    last_modified: datetime = EPOCH


class _Events:
    """The ``client.meta.events`` registry, enough for the live check's request count."""

    def __init__(self) -> None:
        self.handlers: dict[str, list] = {}

    def register(self, name: str, handler) -> None:
        self.handlers.setdefault(name, []).append(handler)

    def fire(self, name: str) -> None:
        for handler in self.handlers.get(name, []):
            handler()


@dataclass
class _Meta:
    events: _Events = field(default_factory=_Events)


class FakeS3:
    """An in-memory, versioned S3 bucket behind the client calls ``lake.bucket`` makes."""

    def __init__(
        self,
        bucket: str = "lake-backup",
        *,
        page_size: int = 1000,
        versioning: str | None = "Enabled",
        on_put=None,
        on_get=None,
        deny_versioned_get: bool = False,
        deny_list_versions: bool = False,
        deny_versioned_get_object: bool = False,
        deny_prefixes: tuple[str, ...] = (),
    ) -> None:
        self.bucket = bucket
        self.page_size = page_size
        self.versioning = versioning
        self.on_put = on_put
        self.on_get = on_get
        self.deny_versioned_get = deny_versioned_get
        self.deny_list_versions = deny_list_versions
        self.deny_versioned_get_object = deny_versioned_get_object
        self.deny_prefixes = tuple(deny_prefixes)
        self.objects: dict[str, list[Version]] = {}
        # Each key's delete markers, as ``(sequence, version id)``. The sequence orders them
        # among the key's versions, whose ids carry the same counter.
        self.markers: dict[str, list[tuple[int, str]]] = {}
        self.calls: list[tuple[str, dict]] = []
        self.fail_with: Exception | None = None
        self.meta = _Meta()
        self._next_version = 0

    # -- helpers a test reads --------------------------------------------------

    def puts(self) -> list[dict]:
        """Every ``put_object`` call's arguments, in order, refused ones included."""
        return [kwargs for name, kwargs in self.calls if name == "put_object"]

    def put_keys(self) -> list[str]:
        return [kwargs["Key"] for kwargs in self.puts()]

    def versions(self, key: str) -> list[Version]:
        return self.objects.get(key, [])

    def body(self, key: str) -> bytes:
        return self.objects[key][-1].body

    def keys(self) -> list[str]:
        return sorted(self.objects)

    def store(
        self,
        key: str,
        body: bytes,
        *,
        checksum: str | None = "auto",
        checksum_type: str | None = FULL_OBJECT,
        last_modified: datetime | None = None,
    ) -> None:
        """Put an object straight into the store, the way another tool would have.

        ``checksum="auto"`` stores the true SHA-256, ``None`` stores none, and any other
        value is stored as given, which is how a test models a composite checksum.
        ``last_modified`` defaults to the fake's clock.
        """
        value = (
            base64.b64encode(hashlib.sha256(body).digest()).decode()
            if checksum == "auto"
            else checksum
        )
        self._add(key, body, value, checksum_type, None, last_modified)

    def delete_marker(self, key: str) -> str:
        """Add a delete marker to ``key`` and return its version id.

        Only the version listing shows it. A test that wants the key's current version served
        stores a version after the marker, as S3 too serves the newest version then.
        """
        self._next_version += 1
        version_id = f"v{self._next_version}"
        self.markers.setdefault(key, []).append((self._next_version, version_id))
        return version_id

    def versioned_calls(self) -> list[tuple[str, dict]]:
        """Every call that lists versions or names one, in order."""
        return [
            (name, kwargs)
            for name, kwargs in self.calls
            if name == "list_object_versions" or "VersionId" in kwargs
        ]

    # -- the client calls ------------------------------------------------------

    def _enter(self, name: str, kwargs: dict) -> None:
        self.calls.append((name, dict(kwargs)))
        if self.fail_with is not None:
            raise self.fail_with
        if kwargs.get("Bucket", self.bucket) != self.bucket:
            raise client_error("NoSuchBucket", name, 404)

    def _add(
        self, key, body, checksum, checksum_type, storage_class, last_modified=None
    ) -> Version:
        self._next_version += 1
        if last_modified is None:
            last_modified = EPOCH + timedelta(seconds=self._next_version)
        version = Version(
            f"v{self._next_version}", body, checksum, checksum_type, storage_class, last_modified
        )
        self.objects.setdefault(key, []).append(version)
        return version

    def put_object(self, **kwargs) -> dict:
        self._enter("put_object", kwargs)
        self.meta.events.fire("before-send.s3.PutObject")
        body = kwargs["Body"]
        data = body.read() if hasattr(body, "read") else bytes(body)
        if self.on_put is not None:
            self.on_put(kwargs, data)
        supplied = kwargs.get("ChecksumSHA256")
        if supplied is not None:
            try:
                raw = base64.b64decode(supplied, validate=True)
            except (binascii.Error, ValueError):
                raise client_error("InvalidRequest", "PutObject") from None
            if len(raw) != 32:
                raise client_error("InvalidRequest", "PutObject")
            if raw != hashlib.sha256(data).digest():
                raise client_error("BadDigest", "PutObject")
        version = self._add(
            kwargs["Key"],
            data,
            supplied,
            FULL_OBJECT if supplied else None,
            kwargs.get("StorageClass"),
        )
        response = {"VersionId": version.version_id}
        if supplied is not None:
            response["ChecksumSHA256"] = supplied
        return response

    def _denied(self, path: str) -> bool:
        return any(path.startswith(prefix) for prefix in self.deny_prefixes)

    def head_object(self, **kwargs) -> dict:
        self._enter("head_object", kwargs)
        if self._denied(kwargs["Key"]):
            raise client_error("403", "HeadObject", 403)
        versions = self.objects.get(kwargs["Key"])
        wanted = kwargs.get("VersionId")
        if wanted is not None and self.deny_versioned_get:
            raise client_error("403", "HeadObject", 403)
        if wanted is not None and any(
            marker == wanted for _, marker in self.markers.get(kwargs["Key"], ())
        ):
            raise client_error("405", "HeadObject", 405)
        if not versions:
            raise client_error("404", "HeadObject", 404)
        if wanted is None:
            current = versions[-1]
        else:
            named = [version for version in versions if version.version_id == wanted]
            if not named:
                raise client_error("404", "HeadObject", 404)
            current = named[0]
        response = {
            "ContentLength": len(current.body),
            "VersionId": current.version_id,
            "LastModified": current.last_modified,
        }
        if current.storage_class is not None:
            response["StorageClass"] = current.storage_class
        if kwargs.get("ChecksumMode") == "ENABLED" and current.checksum is not None:
            response["ChecksumSHA256"] = current.checksum
            if current.checksum_type is not None:
                response["ChecksumType"] = current.checksum_type
        return response

    def get_object(self, **kwargs) -> dict:
        self._enter("get_object", kwargs)
        if self.on_get is not None:
            self.on_get(kwargs)
        versions = self.objects.get(kwargs["Key"])
        if not versions:
            raise client_error("NoSuchKey", "GetObject", 404)
        wanted = kwargs.get("VersionId")
        if wanted is None:
            current = versions[-1]
            return {
                "Body": io.BytesIO(current.body),
                "VersionId": current.version_id,
                "LastModified": current.last_modified,
            }
        if self.deny_versioned_get or self.deny_versioned_get_object:
            raise client_error("AccessDenied", "GetObject", 403)
        for version in versions:
            if version.version_id == wanted:
                return {
                    "Body": io.BytesIO(version.body),
                    "VersionId": version.version_id,
                    "LastModified": version.last_modified,
                }
        raise client_error("NoSuchVersion", "GetObject", 404)

    def list_objects_v2(self, **kwargs) -> dict:
        self._enter("list_objects_v2", kwargs)
        prefix = kwargs.get("Prefix", "")
        if self._denied(prefix):
            raise client_error("AccessDenied", "ListObjectsV2", 403)
        keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
        start = int(kwargs.get("ContinuationToken", 0))
        size = min(self.page_size, int(kwargs.get("MaxKeys", self.page_size)))
        page = keys[start : start + size]
        response: dict = {
            "Contents": [
                {
                    "Key": key,
                    "Size": len(self.objects[key][-1].body),
                    "LastModified": self.objects[key][-1].last_modified,
                }
                for key in page
            ],
            "IsTruncated": start + size < len(keys),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + size)
        if not page:
            del response["Contents"]
        return response

    def list_object_versions(self, **kwargs) -> dict:
        """S3's version listing: every version and delete marker under ``Prefix``, in pages.

        Entries run in key order, newest first within a key, and ``IsLatest`` marks each key's
        newest entry. ``KeyMarker`` alone starts after every entry of that key, and with
        ``VersionIdMarker`` it starts after that one entry. A truncated page carries
        ``NextKeyMarker`` and ``NextVersionIdMarker``, its last entry's. ``MaxKeys`` caps a
        page, and otherwise the client's ``page_size`` does.
        """
        self._enter("list_object_versions", kwargs)
        if self.deny_list_versions:
            raise client_error("AccessDenied", "ListObjectVersions", 403)
        prefix = kwargs.get("Prefix", "")
        entries: list[tuple[str, int, str, Version | None]] = []
        for key in sorted(set(self.objects) | set(self.markers)):
            if not key.startswith(prefix):
                continue
            mine: list[tuple[int, str, Version | None]] = [
                (int(version.version_id[1:]), version.version_id, version)
                for version in self.objects.get(key, ())
            ]
            mine += [(seq, marker, None) for seq, marker in self.markers.get(key, ())]
            for seq, version_id, version in sorted(mine, key=lambda item: -item[0]):
                entries.append((key, seq, version_id, version))
        start = 0
        key_marker = kwargs.get("KeyMarker")
        if key_marker is not None:
            id_marker = kwargs.get("VersionIdMarker")
            start = len(entries)
            for index, (key, _seq, version_id, _version) in enumerate(entries):
                if key > key_marker:
                    start = index
                    break
                if key == key_marker and id_marker is not None and version_id == id_marker:
                    start = index + 1
                    break
        size = int(kwargs.get("MaxKeys", self.page_size))
        page = entries[start : start + size]
        newest: dict[str, int] = {}
        for key, seq, _version_id, _version in entries:
            newest[key] = max(newest.get(key, seq), seq)
        response: dict = {"IsTruncated": start + size < len(entries)}
        versions = [
            {
                "Key": key,
                "VersionId": version_id,
                "IsLatest": seq == newest[key],
                "Size": len(version.body),
            }
            for key, seq, version_id, version in page
            if version is not None
        ]
        markers = [
            {"Key": key, "VersionId": version_id, "IsLatest": seq == newest[key]}
            for key, seq, version_id, version in page
            if version is None
        ]
        if versions:
            response["Versions"] = versions
        if markers:
            response["DeleteMarkers"] = markers
        if response["IsTruncated"]:
            last_key, _seq, last_id, _version = page[-1]
            response["NextKeyMarker"] = last_key
            response["NextVersionIdMarker"] = last_id
        return response

    def get_bucket_versioning(self, **kwargs) -> dict:
        self._enter("get_bucket_versioning", kwargs)
        return {} if self.versioning is None else {"Status": self.versioning}
