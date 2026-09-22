"""A tiny in-memory stand-in for the S3 client, enough for the ingest paths.

Deliberately NOT a general S3 emulator. It implements the four calls this
platform makes -- head, put, get, and a list_objects_v2 paginator -- so the
normalize stage and `find_pending` can be exercised without MinIO. Anything
else raises, so a test that starts depending on a fifth call fails loudly
rather than silently passing against a mock that agreed with it.

What this CANNOT tell you is whether the real thing behaves the same way, and
the ingest path proper (Spark reading a part, the Nessie branch, the merge) is
not covered here at all. That is verified by running it. See tests/README.md.
"""
from __future__ import annotations

import base64
import hashlib
import io
import itertools
from datetime import datetime, timezone


class NoSuchKey(Exception):
    pass


class NoSuchUpload(Exception):
    pass


class InvalidPart(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class FakeS3:
    """Optionally simulates S3/MinIO SHA-256 checksums (``checksum_capable``)
    and multipart upload, matching the exact behaviour confirmed against real
    MinIO (RELEASE.2024-09-22): a single PUT's ``ChecksumSHA256`` is the
    plain full-object digest; a completed multipart upload's is a COMPOSITE
    digest (``base64(sha256(concat(part digests)))-<part count>``), never
    comparable to a plain full-object hash. ``checksum_capable=False`` (the
    default) omits checksum fields entirely, simulating a backend that does
    not support them -- exercising the full-download fallback, exactly like
    every test written against this fake before checksums existed.
    """

    def __init__(self, checksum_capable: bool = False) -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.checksums: dict[str, str] = {}   # key -> base64 checksum
        self.calls: list[str] = []
        self.checksum_capable = checksum_capable
        self._uploads: dict[str, dict] = {}   # upload_id -> {key, parts: {n: bytes}}
        self._upload_ids = itertools.count(1)

    # ------------------------------------------------------------- helpers
    def put(self, key: str, body: bytes | str,
            when: datetime | None = None) -> None:
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.objects[key] = (body, when or datetime(2026, 8, 1, 6, 0,
                                                    tzinfo=timezone.utc))
        if self.checksum_capable:
            self.checksums[key] = _b64_sha256(body)

    # -------------------------------------------------------- the S3 subset
    def head_object(self, Bucket: str, Key: str,                 # noqa: N803
                    ChecksumMode: str = ""):                     # noqa: N803
        self.calls.append(f"head:{Key}")
        if Key not in self.objects:
            raise NoSuchKey(Key)
        body, when = self.objects[Key]
        result = {"ContentLength": len(body), "LastModified": when}
        if ChecksumMode == "ENABLED" and Key in self.checksums:
            result["ChecksumSHA256"] = self.checksums[Key]
        return result

    def put_object(self, Bucket: str, Key: str, Body: bytes,     # noqa: N803
                   ContentType: str = "",                       # noqa: N803
                   IfNoneMatch: str = "",                        # noqa: N803
                   ChecksumAlgorithm: str = ""):                 # noqa: N803
        self.calls.append(f"put:{Key}")
        if IfNoneMatch == "*" and Key in self.objects:
            raise PreconditionFailed(Key)
        if hasattr(Body, "read"):
            Body = Body.read()
        self.objects[Key] = (Body, datetime.now(timezone.utc))
        result: dict = {}
        if self.checksum_capable and ChecksumAlgorithm == "SHA256":
            checksum = _b64_sha256(Body)
            self.checksums[Key] = checksum
            result["ChecksumSHA256"] = checksum
        else:
            self.checksums.pop(Key, None)
        return result

    def get_object(self, Bucket: str, Key: str):                 # noqa: N803
        self.calls.append(f"get:{Key}")
        if Key not in self.objects:
            raise NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def delete_object(self, Bucket: str, Key: str):              # noqa: N803
        self.calls.append(f"delete:{Key}")
        self.objects.pop(Key, None)
        self.checksums.pop(Key, None)
        return {}

    def get_paginator(self, op: str):
        if op != "list_objects_v2":
            raise NotImplementedError(op)
        return _Paginator(self)

    # ---------------------------------------------------------- multipart
    def create_multipart_upload(self, Bucket: str, Key: str,     # noqa: N803
                                ChecksumAlgorithm: str = ""):    # noqa: N803
        upload_id = f"upload-{next(self._upload_ids)}"
        self.calls.append(f"create_mpu:{Key}:{upload_id}")
        self._uploads[upload_id] = {"key": Key, "parts": {},
                                    "checksum": bool(self.checksum_capable
                                                     and ChecksumAlgorithm)}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket: str, Key: str, PartNumber: int,  # noqa: N803
                    UploadId: str, Body: bytes,                    # noqa: N803
                    ChecksumAlgorithm: str = ""):                  # noqa: N803
        self.calls.append(f"upload_part:{Key}:{UploadId}:{PartNumber}")
        upload = self._require_upload(UploadId)
        if hasattr(Body, "read"):
            Body = Body.read()
        upload["parts"][PartNumber] = Body
        result = {"ETag": f'"{hashlib.md5(Body).hexdigest()}"'}  # noqa: S324
        if upload["checksum"] and ChecksumAlgorithm == "SHA256":
            result["ChecksumSHA256"] = _b64_sha256(Body)
        return result

    def complete_multipart_upload(self, Bucket: str, Key: str,    # noqa: N803
                                  UploadId: str, MultipartUpload: dict,  # noqa: N803
                                  IfNoneMatch: str = ""):          # noqa: N803
        self.calls.append(f"complete_mpu:{Key}:{UploadId}")
        upload = self._require_upload(UploadId)
        if IfNoneMatch == "*" and Key in self.objects:
            raise PreconditionFailed(Key)
        parts = sorted(MultipartUpload["Parts"], key=lambda p: p["PartNumber"])
        # Confirmed against real MinIO: a part uploaded WITH a checksum must
        # be completed with that checksum repeated in its Parts entry, or
        # the whole completion fails with InvalidPart -- ETag alone is not
        # enough once checksums are in play. Enforced here so a caller that
        # forgets this (as this package's first draft did) fails a test
        # rather than only a live MinIO call.
        if upload["checksum"] and any("ChecksumSHA256" not in p for p in parts):
            raise InvalidPart("a checksum-requested multipart upload must "
                              "repeat each part's ChecksumSHA256 in its "
                              "CompleteMultipartUpload entry")
        body = b"".join(upload["parts"][p["PartNumber"]] for p in parts)
        self.objects[Key] = (body, datetime.now(timezone.utc))
        result: dict = {}
        if upload["checksum"]:
            digests = [hashlib.sha256(upload["parts"][p["PartNumber"]]).digest()
                      for p in parts]
            composite = base64.b64encode(
                hashlib.sha256(b"".join(digests)).digest()).decode()
            checksum = f"{composite}-{len(parts)}"
            self.checksums[Key] = checksum
            result["ChecksumSHA256"] = checksum
        else:
            self.checksums.pop(Key, None)
        del self._uploads[UploadId]
        return result

    def abort_multipart_upload(self, Bucket: str, Key: str,      # noqa: N803
                               UploadId: str):                    # noqa: N803
        self.calls.append(f"abort_mpu:{Key}:{UploadId}")
        if UploadId not in self._uploads:
            raise NoSuchUpload(UploadId)
        del self._uploads[UploadId]
        return {}

    def _require_upload(self, upload_id: str) -> dict:
        if upload_id not in self._uploads:
            raise NoSuchUpload(upload_id)
        return self._uploads[upload_id]

    def __getattr__(self, name):
        raise NotImplementedError(
            f"FakeS3 does not implement {name!r} -- add it deliberately")


def _b64_sha256(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode()


class _Paginator:
    def __init__(self, s3: FakeS3) -> None:
        self.s3 = s3

    def paginate(self, Bucket: str, Prefix: str = ""):           # noqa: N803
        contents = [
            {"Key": k, "Size": len(v[0]), "LastModified": v[1]}
            for k, v in sorted(self.s3.objects.items()) if k.startswith(Prefix)
        ]
        yield {"Contents": contents}


def install(monkey: list, s3: FakeS3, bucket: str = "lakehouse") -> None:
    """Point every module that reaches for S3 at this fake.

    `monkey` collects (module, attr, original) so the caller can undo it.
    normalize and retention.ready import `_client`/`_bucket` BY VALUE from
    arrival, so patching arrival alone would miss them -- each importer holds
    its own reference.
    """
    from reporting_platform.ingest import arrival, normalize
    from reporting_platform.retention import ready

    for mod in (arrival, normalize, ready):
        for attr, value in (("_client", lambda: s3), ("_bucket", lambda: bucket)):
            if hasattr(mod, attr):
                monkey.append((mod, attr, getattr(mod, attr)))
                setattr(mod, attr, value)


def uninstall(monkey: list) -> None:
    for mod, attr, original in reversed(monkey):
        setattr(mod, attr, original)
    monkey.clear()
