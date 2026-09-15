"""`Nessie.merge` must not override the server's per-key merge behaviour.

Measured live (docs/DECISIONS.md#a-merge-conflict-not-the-pool-keeps-file-
version-unique): two branches cut from the same base both ran
`next_file_version` for one feed/COB date and both computed the same
`_file_version`. Merging the first into `main` applied; merging the second
then got a 409:

    {"status": 409, "reason": "Conflict",
     "message": "The following keys have been changed in conflict: "
                 "'raw.fo_trade'", "errorCode": "REFERENCE_CONFLICT"}

That is Nessie's v2 merge applying its NORMAL per-content-key merge mode --
the default -- which refuses a merge that would silently overwrite a key
another commit already changed on the target since the branch was cut. It is
what actually keeps two concurrent ingests from both landing under one
`_file_version`, not the `lakehouse_write` pool (`scripts/bulk_ingest.py` and
the CLI both bypass that pool).

That protection is entirely conditional on `Nessie.merge` never asking for a
DIFFERENT per-key mode. Nessie's v2 merge request body accepts
`defaultKeyMergeMode` and `keyMergeModes` for exactly that -- e.g. FORCE
(apply anyway) or DROP (silently skip the conflicting key) -- and either
would turn this 409 back into a silent double-write. This test pins that
`Nessie.merge` sends neither.

It checks only these two field names because they are the ones the live 409
above was diagnosed against; this repo has no vendored copy of Nessie's v2
OpenAPI spec and no other comment naming a field that would change conflict
behaviour, so no other field name is asserted here -- inventing one to check
would be worse than not checking it.
"""
from __future__ import annotations


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


def test_merge_sends_no_key_merge_mode_override():
    n, rec = _merging_nessie()
    n.merge("ingest/fo_trade/20260819/run1", into="main")

    bodies = rec.merge_bodies()
    assert len(bodies) == 1, bodies
    body = bodies[0]
    for forbidden in ("defaultKeyMergeMode", "keyMergeModes"):
        assert forbidden not in body, (forbidden, body)
