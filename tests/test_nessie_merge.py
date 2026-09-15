"""`Nessie.merge` must not send a field that changes conflict behaviour.

Measured live (docs/DECISIONS.md#a-merge-conflict-not-the-pool-keeps-file-
version-unique): two branches cut from the same base both ran
`next_file_version` for one feed/COB date and both computed the same
`_file_version`. Merging the first applied; merging the second then got a
409:

    {"status": 409, "reason": "Conflict",
     "message": "The following keys have been changed in conflict: "
                 "'raw.fo_trade'", "errorCode": "REFERENCE_CONFLICT"}

That is Nessie's v2 merge applying its NORMAL per-content-key merge mode --
the default -- which refuses a merge that would silently overwrite a key
another commit already changed on the target since the branch was cut. It is
what actually keeps two concurrent ingests from both landing under one
`_file_version`, not the `lakehouse_write` pool (`scripts/bulk_ingest.py` and
the CLI both bypass that pool).

Read from the live server's own schema (`curl -s
http://localhost:19120/nessie-openapi/openapi.yaml`, `Merge`/`Merge1`), the
v2 merge request accepts: `fromRefName`, `fromHash`, `keyMergeModes`,
`defaultKeyMergeMode`, `dryRun`, `fetchAdditionalInfo`,
`returnConflictAsResult`, `message`, `commitMeta`. `Nessie.merge` sends only
`fromRefName`, `fromHash` and (when a message or properties are given)
`commitMeta` -- the allowlist below. Everything else on that list changes
what a conflict DOES, not just what is reported, so a caller adding one
without updating this test is exactly the failure this pins against:

  - `defaultKeyMergeMode` / `keyMergeModes` -- FORCE applies over a
    conflicting key instead of refusing; DROP silently skips it. Either
    turns the 409 above into a silent double-write.
  - `returnConflictAsResult` -- `true` returns the conflict as a NORMAL
    response instead of raising an HTTPError, so `ingest()` would treat a
    refused merge as a successful one and delete the branch anyway.
  - `dryRun` / `fetchAdditionalInfo` change what the call reports or
    whether it commits, not what conflicts -- still worth noticing if one
    appears, since neither is a field `Nessie.merge` has ever had reason to
    send.
"""
from __future__ import annotations

# Everything else in the schema list above changes conflict behaviour and is
# deliberately absent.
_ALLOWED_MERGE_BODY_KEYS = {"fromRefName", "fromHash", "commitMeta"}


class _RecordingReq:
    """Stands in for `Nessie._req`, recording every call and returning just
    enough for `merge()` to proceed: a reference lookup for `from_branch` and
    `into`, then a fake merge response."""

    def __init__(self):
        self.calls = []

    def __call__(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if method == "GET" and path.startswith("/trees/") and "/history/merge" not in path:
            name = path.split("/trees/", 1)[1]
            # Unquote just enough: the names this test uses have no special
            # characters, so urllib's %XX escaping never appears in `name`.
            return {"reference": {"type": "BRANCH", "name": name, "hash": "abc123"}}
        if method == "POST" and "/history/merge" in path:
            return {"resultType": "MERGE", "wasApplied": True}
        raise AssertionError(f"unexpected call: {method} {path}")

    def merge_bodies(self):
        return [kwargs["json"] for method, path, kwargs in self.calls
                if method == "POST" and "/history/merge" in path]


def _merging_nessie():
    from reporting_platform.common.nessie import Nessie

    n = Nessie(uri="http://fake-nessie")
    rec = _RecordingReq()
    n._req = rec
    return n, rec


def test_merge_body_has_no_field_outside_the_allowlist():
    n, rec = _merging_nessie()
    n.merge("ingest/fo_trade/20260819/run1", into="main")

    bodies = rec.merge_bodies()
    assert len(bodies) == 1, bodies
    body = bodies[0]
    extra = set(body) - _ALLOWED_MERGE_BODY_KEYS
    assert not extra, (extra, body)
