"""The fake S3 client.

``FakeS3`` stands in for the ``boto3`` S3 client the bucket backup uses. It answers the
five calls ``lake.bucket`` makes with the shapes S3 answers them, and raises the errors
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
   can count versions to prove a second run sent nothing. ``GetObject`` answers with the
   ``VersionId`` of the version it served, on a plain read of the current version as on a
   read of a named one, which is what S3 does on a versioned bucket.

``HeadObject`` on a missing key answers a bare 404, as S3 does for a key that holds
``s3:ListBucket``, and its error carries no body. ``fail_with`` makes every call raise one
chosen error, which is how a test models a revoked key or a network that is down.
``on_put`` and ``on_get`` run before a ``PutObject`` or a ``GetObject`` is answered, so a
test can fail or time one kind of call alone.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
from dataclasses import dataclass, field

from botocore.exceptions import ClientError, EndpointConnectionError

FULL_OBJECT = "FULL_OBJECT"


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
    ) -> None:
        self.bucket = bucket
        self.page_size = page_size
        self.versioning = versioning
        self.on_put = on_put
        self.on_get = on_get
        self.deny_versioned_get = deny_versioned_get
        self.objects: dict[str, list[Version]] = {}
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
    ) -> None:
        """Put an object straight into the store, the way another tool would have.

        ``checksum="auto"`` stores the true SHA-256, ``None`` stores none, and any other
        value is stored as given, which is how a test models a composite checksum.
        """
        value = (
            base64.b64encode(hashlib.sha256(body).digest()).decode()
            if checksum == "auto"
            else checksum
        )
        self._add(key, body, value, checksum_type, None)

    # -- the client calls ------------------------------------------------------

    def _enter(self, name: str, kwargs: dict) -> None:
        self.calls.append((name, dict(kwargs)))
        if self.fail_with is not None:
            raise self.fail_with
        if kwargs.get("Bucket", self.bucket) != self.bucket:
            raise client_error("NoSuchBucket", name, 404)

    def _add(self, key, body, checksum, checksum_type, storage_class) -> Version:
        self._next_version += 1
        version = Version(f"v{self._next_version}", body, checksum, checksum_type, storage_class)
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

    def head_object(self, **kwargs) -> dict:
        self._enter("head_object", kwargs)
        versions = self.objects.get(kwargs["Key"])
        if not versions:
            raise client_error("404", "HeadObject", 404)
        current = versions[-1]
        response = {"ContentLength": len(current.body), "VersionId": current.version_id}
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
            return {"Body": io.BytesIO(current.body), "VersionId": current.version_id}
        if self.deny_versioned_get:
            raise client_error("AccessDenied", "GetObject", 403)
        for version in versions:
            if version.version_id == wanted:
                return {"Body": io.BytesIO(version.body), "VersionId": version.version_id}
        raise client_error("NoSuchVersion", "GetObject", 404)

    def list_objects_v2(self, **kwargs) -> dict:
        self._enter("list_objects_v2", kwargs)
        prefix = kwargs.get("Prefix", "")
        keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
        start = int(kwargs.get("ContinuationToken", 0))
        page = keys[start : start + self.page_size]
        response: dict = {
            "Contents": [{"Key": key, "Size": len(self.objects[key][-1].body)} for key in page],
            "IsTruncated": start + self.page_size < len(keys),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + self.page_size)
        if not page:
            del response["Contents"]
        return response

    def get_bucket_versioning(self, **kwargs) -> dict:
        self._enter("get_bucket_versioning", kwargs)
        return {} if self.versioning is None else {"Status": self.versioning}
