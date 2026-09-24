"""/healthz for the two long-running processes the k8s chart probes
(deploy/helm/reporting-platform/{inbox,feed-console}.yaml): the inbox
watcher's `--loop` health server and the feed console's FastAPI route.

The inbox side is a pure function (`health()` in
reporting_platform/ingest/inbox.py) precisely so it is testable with no
server, no thread and no clock -- see its own docstring for why the window is
3x the poll interval. No stack.
"""
from __future__ import annotations

from tests.support import Skipped


def test_inbox_health_fresh():
    from reporting_platform.ingest.inbox import health

    status, body = health(last_poll_at=100.0, interval=10, now=105.0)
    assert status == 200, body


def test_inbox_health_stale():
    from reporting_platform.ingest.inbox import health

    status, body = health(last_poll_at=100.0, interval=10, now=131.0)
    assert status == 503, body
    assert "stale" in body


def test_inbox_health_boundary_is_stale():
    # THE WINDOW IS "<", NOT "<=" -- exactly 3x the interval is already stale,
    # not the last fresh instant, so a probe firing right at the boundary
    # cannot alternate pass/fail depending on floating-point noise in `now`.
    from reporting_platform.ingest.inbox import health

    status, _ = health(last_poll_at=0.0, interval=10, now=30.0)
    assert status == 503


def test_inbox_health_never_polled():
    # The server starts before the first sweep() returns -- `None` must read
    # as unready, not as a pass by omission (CLAUDE.md: "a subject it could
    # not READ is not a subject that is EMPTY").
    from reporting_platform.ingest.inbox import health

    status, body = health(last_poll_at=None, interval=10, now=1000.0)
    assert status == 503
    assert "no poll" in body


def test_ui_healthz_route():
    try:
        from fastapi.testclient import TestClient
    except ImportError as exc:
        raise Skipped(f"fastapi is not installed here: {exc}") from None

    from reporting_platform.ui.app import app

    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True}
