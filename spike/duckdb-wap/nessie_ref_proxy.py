"""Make DuckDB able to address a Nessie BRANCH over Iceberg REST.

THE SEAM, measured rather than assumed (see README.md). DuckDB's iceberg
extension does two things that are individually reasonable and together make a
branch unreachable:

  1. it calls `<ENDPOINT>/v1/config?warehouse=<attach name>`, and
  2. it builds every later URL as `<ENDPOINT>/v1/<defaults.prefix>/...`,
     appending to the ENDPOINT IT WAS GIVEN -- not to the `uri` override the
     config response carries.

Nessie's prefix is `{ref}|{warehouse}`, and it will vend a branch-scoped one:
ask `/iceberg/etl_x/v1/config` and it answers `prefix: etl_x%7Cwarehouse`. But
DuckDB then appends that to the same `/iceberg/etl_x` it started from, so the
ref appears TWICE:

    GET /iceberg/etl_x/v1/etl_x%7Cwarehouse/namespaces  ->  404

and DuckDB renders the 404 as an EMPTY CATALOG rather than an error, so
`ATTACH` succeeds and the branch simply looks like it has no tables. That is
the whole blocker, and it is silent.

WHAT THIS DOES. One rewrite: strip the ref segment from every path except
`/v1/config`, which must pass through untouched or Nessie vends `main`'s
prefix and the write lands on the wrong branch.

    /iceberg/etl_x/v1/config          -> /iceberg/v1/config          (as-is)
    /iceberg/etl_x/v1/etl_x%7Cwh/...  -> /iceberg/v1/etl_x%7Cwh/...  (stripped)

Not a component of the platform: a spike artefact that proves the seam is
bridgeable at all, and measures what bridging costs. Anything real would put
the rewrite in the ingress that already fronts Nessie.
"""
from __future__ import annotations

import http.server
import os
import re
import socketserver
import sys
import urllib.error
import urllib.request

UPSTREAM = os.environ.get("NESSIE_UPSTREAM", "http://nessie:19120")
PORT = int(os.environ.get("PROXY_PORT", "18998"))

# /iceberg/<ref>/v1/<anything-but-config>
_REF_PATH = re.compile(r"^/iceberg/(?P<ref>[^/]+)/v1/(?P<rest>(?!config(?:\?|$)).*)$")


def rewrite(path: str) -> str:
    """The one rule. Returns the upstream path for a client path."""
    m = _REF_PATH.match(path)
    return f"/iceberg/v1/{m.group('rest')}" if m else path


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _proxy(self) -> None:
        upstream_path = rewrite(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        req = urllib.request.Request(UPSTREAM + upstream_path, data=body,
                                     method=self.command)
        for key, value in self.headers.items():
            if key.lower() not in ("host", "content-length", "connection"):
                req.add_header(key, value)
        try:
            with urllib.request.urlopen(req) as resp:
                payload, status, headers = resp.read(), resp.status, resp.headers
        except urllib.error.HTTPError as exc:
            payload, status, headers = exc.read(), exc.code, exc.headers
        except urllib.error.URLError as exc:
            payload, status, headers = str(exc).encode(), 502, {}
        if os.environ.get("PROXY_LOG"):
            arrow = "" if upstream_path == self.path else f" => {upstream_path}"
            print(f"{self.command} {self.path}{arrow} -> {status}",
                  file=sys.stderr, flush=True)
        self.send_response(status)
        self.send_header("Content-Type",
                         (headers or {}).get("Content-Type", "application/json"))
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = do_HEAD = do_DELETE = do_PUT = _proxy

    def log_message(self, *args):        # the proxy logs its own, upstream-aware
        pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    print(f"nessie ref proxy on :{PORT} -> {UPSTREAM}", file=sys.stderr, flush=True)
    Server(("0.0.0.0", PORT), Handler).serve_forever()
