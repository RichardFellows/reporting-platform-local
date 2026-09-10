"""The Nessie REST client.

SPLIT OUT OF `context.py` alongside `spark.py`, and for the same reason: this
is a client of a running service, and the config half of `context` had no seam
at which it could be exercised without it. Nothing here reads `feeds.yml`, and
nothing that reads `feeds.yml` needs `requests`.

Deliberately REST rather than the Spark SQL extensions, so branch lifecycle can
be driven from Airflow tasks that need no Spark session at all.
"""
from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote as _urlquote

log = logging.getLogger("nessie")

class Nessie:
    """Thin wrapper over Nessie's REST API."""

    def __init__(self, uri: str | None = None):
        self.uri = (uri or os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")).rstrip("/")

    def _req(self, method: str, path: str, **kwargs):
        import requests

        r = requests.request(method, f"{self.uri}{path}", timeout=30, **kwargs)
        if not r.ok:
            # requests' default raise_for_status() drops the response body,
            # which is where Nessie puts the useful part (status/reason/
            # message/errorCode) -- surface it instead of "404 Client Error".
            raise requests.exceptions.HTTPError(
                f"{r.status_code} {r.reason} for url {r.url}: {r.text}", response=r
            )
        return r.json() if r.content else {}

    def get_reference(self, name: str) -> dict[str, Any]:
        return self._req("GET", f"/trees/{_urlquote(name, safe='')}")

    def create_branch(self, name: str, from_ref: str = "main",
                      exist_ok: bool = False) -> dict[str, Any]:
        """POST /v2/trees?name=<new>&type=BRANCH

        Per Nessie's v2 REST spec (confirmed against the live server's
        /nessie-openapi/openapi.yaml), the new reference's name/type are
        QUERY params, and the JSON body is the SOURCE reference being
        branched from (not the new branch) -- i.e. {type, name, hash} of
        `from_ref`. Getting this backwards produces a 404
        "Named reference '<new-name>' not found", since the server tries to
        resolve the body's name as the existing source ref.
        """
        if exist_ok:
            # Retries must not be poisoned by their own previous attempt. A
            # failed build deliberately leaves its branch behind for inspection
            # (see dbt_builds.keep_failed_branch), so re-running hits 409
            # "already exists" and can NEVER succeed -- which made `retries`
            # actively harmful. Reusing the branch is right: same run_id, so
            # the same logical build.
            try:
                existing = self.get_reference(name)
                log.info("branch %s already exists; reusing it", name)
                return existing
            except Exception:
                pass
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "BRANCH"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def merge(self, from_branch: str, into: str = "main",
              message: str | None = None,
              properties: dict[str, str] | None = None) -> dict[str, Any]:
        """POST /v2/trees/{branch}@{expectedHash}/history/merge

        v2 has no separate `expectedHash` body field -- the target's expected
        HEAD is pinned via `name@hash` in the path, and the server rejects an
        unpinned merge. This only works once `into` has a real commit: pinning
        at Nessie's sentinel "no ancestor" hash fails with "No common ancestor
        in parents of ...". Callers run a one-time bootstrap commit first (see
        `_bootstrap_main_if_empty` in ingest_feed.py).

        `message` and `properties` become the merge commit's CommitMeta, sent
        only when there is something to say.

        READING THEM BACK, the properties are under **`allProperties`**, not
        `properties` -- v2 returns them multi-valued, `{"change_ref":
        ["RPT-1421"]}`. Looking for `properties` finds nothing and reads like
        the server having dropped them.
        """
        src = self.get_reference(from_branch)["reference"]
        tgt = self.get_reference(into)["reference"]
        body: dict[str, Any] = {"fromRefName": from_branch,
                                "fromHash": src["hash"]}
        if message or properties:
            # REQ-405. The merge commit is the only place a publication can
            # say WHY it happened in the catalog itself, and it carried nothing
            # at all: Nessie synthesises "Merge <hash> into main".
            # `properties` is a free-form string map on Nessie's CommitMeta, so
            # the change reference is queryable rather than only greppable.
            meta: dict[str, Any] = {}
            if message:
                meta["message"] = message
            if properties:
                meta["properties"] = {k: str(v) for k, v in properties.items()
                                      if v is not None}
            body["commitMeta"] = meta
        return self._req(
            "POST",
            f"/trees/{_urlquote(into, safe='')}@{tgt['hash']}/history/merge",
            json=body,
        )

    def create_tag(self, name: str, from_ref: str = "main") -> dict[str, Any]:
        src = self.get_reference(from_ref)["reference"]
        return self._req(
            "POST",
            "/trees",
            params={"name": name, "type": "TAG"},
            json={"type": src["type"], "name": src["name"], "hash": src["hash"]},
        )

    def delete_reference(self, name: str) -> None:
        """DELETE /v2/trees/{name}@{hash}?type=...

        Like merge, the expected hash rides in the path (`name@hash`), not
        as a query param -- an `expectedHash` query param is silently
        ignored by the server.
        """
        ref = self.get_reference(name)["reference"]
        self._req("DELETE", f"/trees/{_urlquote(name, safe='')}@{ref['hash']}",
                  params={"type": ref["type"]})

    def list_entries(self, ref: str) -> list[dict[str, Any]]:
        """Every content entry on `ref` -- tables and namespaces -- with the
        content payload inlined.

        `content=true` is what makes `metadataLocation` available, which is the
        only way to learn where a table's files actually live. Without it you
        get names and ids and no way to map a table to object storage.

        This is the cheap way to ask what the catalog holds. The alternative,
        `SHOW TABLES`, costs a SparkSession -- about 22 seconds -- and the
        watchdog runs every five minutes and imports no Spark at all.

        PAGINATED, and the token key is `page-token`. A caller that ignores
        `token` silently sees only the first page; on a warehouse this size
        that is one page, which is exactly why it would go unnoticed until it
        was not.
        """
        out, token = [], None
        enc = _urlquote(ref, safe="")
        while True:
            params: dict[str, Any] = {"content": "true"}
            if token:
                params["page-token"] = token
            page = self._req("GET", f"/trees/{enc}/entries", params=params)
            out.extend(page.get("entries", []))
            token = page.get("token")
            if not token:
                break
        return out

    def list_references(self, prefix: str = "",
                        fetch_all: bool = False) -> list[dict[str, Any]]:
        """List references, optionally with their commit metadata.

        `fetch_all=True` adds `fetch=ALL`, which is what makes each reference
        carry a `metadata.commitMetaOfHEAD.commitTime`. WITHOUT it the server
        returns only type/name/hash -- no metadata key at all. Any caller that
        wants to reason about a branch's age must pass it, or every age check
        silently sees `None` and treats every branch as arbitrarily old.
        Costs an extra lookup per reference server-side, so it is opt-in.
        """
        out, token = [], None
        while True:
            params: dict[str, Any] = {"fetch": "ALL"} if fetch_all else {}
            if token:
                params["page-token"] = token
            page = self._req("GET", "/trees", params=params)
            out.extend(page.get("references", []))
            token = page.get("token")
            if not token:
                break
        return [r for r in out if r["name"].startswith(prefix)]
