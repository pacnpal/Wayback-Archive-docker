"""Route-level tests for the wayback banner + manual retry endpoint."""
from __future__ import annotations

import pytest
from starlette.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    from webui import app as app_mod
    importlib.reload(app_mod)
    return TestClient(app_mod.app), wp, j


def test_banner_is_empty_when_state_is_up(client):
    c, wp, _ = client
    wp.save_state(wp.ProbeState(state="up"))
    r = c.get("/api/wayback-status")
    assert r.status_code == 200
    assert r.text == ""


def test_banner_renders_when_state_is_down(client):
    c, wp, _ = client
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=5))
    r = c.get("/api/wayback-status")
    assert r.status_code == 200
    assert "Internet Archive unreachable" in r.text
    assert "Try now" in r.text
    assert "banner-retry" in r.text


def test_manual_retry_noop_when_not_down(client, monkeypatch):
    """Guard: if state is 'up' (or 'unknown'), the endpoint must NOT
    actually run probe_once(). Any caller hitting it is operating on
    stale state; return the current (empty) banner so their DOM updates."""
    c, wp, _ = client
    wp.save_state(wp.ProbeState(state="up"))
    called = {"n": 0}

    def boom(*a, **kw):
        called["n"] += 1
        return False  # irrelevant — we expect no call
    monkeypatch.setattr(wp, "probe_once", boom)
    r = c.post("/api/wayback-probe/retry")
    assert r.status_code == 200
    assert r.text == ""
    assert called["n"] == 0


def test_banner_handles_naive_since_timestamp(client):
    """Regression: datetime.fromisoformat returns a naive datetime for
    strings without an offset. Subtracting that from tz-aware `now`
    would raise TypeError and 500 the endpoint. The route must treat a
    bare ISO timestamp as UTC."""
    c, wp, _ = client
    # Store an intentionally naive timestamp (no TZ offset) to simulate
    # older DBs or hand-edited rows.
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=5),
                  since_iso="2026-04-20T12:00:00")
    r = c.get("/api/wayback-status")
    assert r.status_code == 200
    assert "Internet Archive unreachable" in r.text
    assert "down for" in r.text


def test_manual_retry_runs_probe_when_down(client, monkeypatch):
    c, wp, _ = client
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=5))
    called = {"n": 0}

    def fake_probe(*a, **kw):
        called["n"] += 1
        return False  # still down
    monkeypatch.setattr(wp, "probe_once", fake_probe)
    r = c.post("/api/wayback-probe/retry")
    assert r.status_code == 200
    assert called["n"] == 1
    # Still down → banner renders again, button still visible.
    assert "Try now" in r.text
