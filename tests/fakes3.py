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

import io
from datetime import datetime, timezone


class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.calls: list[str] = []

    # ------------------------------------------------------------- helpers
    def put(self, key: str, body: bytes | str,
            when: datetime | None = None) -> None:
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.objects[key] = (body, when or datetime(2026, 8, 1, 6, 0,
                                                    tzinfo=timezone.utc))

    # -------------------------------------------------------- the S3 subset
    def head_object(self, Bucket: str, Key: str):                # noqa: N803
        self.calls.append(f"head:{Key}")
        if Key not in self.objects:
            raise NoSuchKey(Key)
        body, when = self.objects[Key]
        return {"ContentLength": len(body), "LastModified": when}

    def put_object(self, Bucket: str, Key: str, Body: bytes,     # noqa: N803
                   ContentType: str = ""):                       # noqa: N803
        self.calls.append(f"put:{Key}")
        self.objects[Key] = (Body, datetime.now(timezone.utc))
        return {}

    def get_object(self, Bucket: str, Key: str):                 # noqa: N803
        self.calls.append(f"get:{Key}")
        if Key not in self.objects:
            raise NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def delete_object(self, Bucket: str, Key: str):              # noqa: N803
        self.calls.append(f"delete:{Key}")
        self.objects.pop(Key, None)
        return {}

    def get_paginator(self, op: str):
        if op != "list_objects_v2":
            raise NotImplementedError(op)
        return _Paginator(self)

    def __getattr__(self, name):
        raise NotImplementedError(
            f"FakeS3 does not implement {name!r} -- add it deliberately")


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
