"""Route-level tests for the wayback banner endpoint."""
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
    import webui.rate_limit as rl
    importlib.reload(rl)
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
    # The "Try now" active-probe button is gone — banner is passive.
    assert "banner-retry" not in r.text


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


def test_banner_shows_rate_limit_clear_eta_when_blocked(client):
    """When webui.rate_limit has installed a hard block, the banner
    must show that cooldown's ETA (the real recovery signal) rather
    than the deferred-job's own retry ETA."""
    c, wp, j = client
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))
    # Install a 1h block directly in the DB.
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(microsecond=0).isoformat()
    with j.connect() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_until", future),
        )
    r = c.get("/api/wayback-status")
    assert r.status_code == 200
    assert "cooldown clears" in r.text.lower()
    # Live-countdown data attribute must carry the ISO timestamp so
    # the client-side widget in base.html can tick it down.
    assert f'data-countdown-iso="{future}"' in r.text


def test_banner_is_stern_on_repeat_offenses(client):
    """tier>=2 must flip the banner into severe mode with the
    blunt 'you are making this worse' copy so a user who keeps
    hammering retry actually sees that they're doubling their own
    cooldown."""
    c, wp, j = client
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=4)).replace(microsecond=0).isoformat()
    with j.connect() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_until", future),
        )
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_tier", "3"),
        )
    r = c.get("/api/wayback-status")
    assert r.status_code == 200
    assert "offense #3" in r.text.lower()
    assert "making this" in r.text.lower()
    assert "wayback-banner--severe" in r.text


def test_probe_retry_endpoint_is_gone(client):
    """The active-probe retry endpoint has been removed. A POST to
    the old path should 404 (not 500/not a no-op) so stale clients
    that cached the button fail visibly instead of silently burning
    CDX budget."""
    c, _, _ = client
    r = c.post("/api/wayback-probe/retry")
    assert r.status_code == 404


def test_probe_timeout_setting_endpoint_is_gone(client):
    """The heartbeat-probe timeout setting has been removed (there is
    no probe). POSTing to the old endpoint 404s."""
    c, _, _ = client
    r = c.post("/settings/wayback-probe-timeout", data={"timeout": "45"})
    assert r.status_code == 404
