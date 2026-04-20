"""Tests for webui.wayback_probe."""
from __future__ import annotations
import json
import urllib.error

import pytest

from webui import wayback_probe


def test_probe_state_default_is_unknown():
    s = wayback_probe.ProbeState()
    assert s.state == "unknown"
    assert s.consecutive_fails == 0
    assert s.consecutive_ok == 0


def test_probe_state_three_fails_flips_to_down():
    s = wayback_probe.ProbeState()
    assert s.observe(False) is None
    assert s.state == "unknown"
    assert s.observe(False) is None
    assert s.state == "unknown"
    assert s.observe(False) == "down"
    assert s.state == "down"


def test_probe_state_two_oks_flip_from_down_to_up():
    s = wayback_probe.ProbeState(state="down", consecutive_fails=3)
    assert s.observe(True) is None
    assert s.state == "down"
    assert s.observe(True) == "up"
    assert s.state == "up"


def test_probe_state_intermittent_fails_do_not_flip():
    s = wayback_probe.ProbeState()
    s.observe(False)
    s.observe(False)
    s.observe(True)       # resets fail counter
    assert s.state == "unknown"
    s.observe(False)
    s.observe(False)
    assert s.state == "unknown"  # only 2 consecutive, need 3


def test_probe_state_single_fail_during_up_does_not_flip_down_immediately():
    s = wayback_probe.ProbeState(state="up")
    s.observe(False)
    assert s.state == "up"
    s.observe(True)
    # The two oks threshold only matters for flipping UP; if already up,
    # staying up is the trivial path.
    assert s.state == "up"


def test_probe_state_recovery_requires_two_consecutive_oks():
    s = wayback_probe.ProbeState(state="down")
    s.observe(True)
    s.observe(False)      # resets ok counter
    s.observe(True)
    assert s.state == "down"
    assert s.observe(True) == "up"


def test_probe_once_returns_true_on_200(monkeypatch):
    class _R:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self, *a): return b"[]"
    monkeypatch.setattr(wayback_probe.urllib.request, "urlopen", lambda *a, **kw: _R())
    assert wayback_probe.probe_once() is True


def test_probe_once_returns_false_on_timeout(monkeypatch):
    def boom(*a, **kw):
        raise urllib.error.URLError("timed out")
    monkeypatch.setattr(wayback_probe.urllib.request, "urlopen", boom)
    assert wayback_probe.probe_once() is False


def test_probe_once_fails_closed_when_status_is_missing(monkeypatch):
    """A health check must never default to 'up' on a malformed response
    object. Regression for the previous `getattr(r, 'status', 200)` that
    would report success when the attribute was absent."""
    class _R:
        # no .status attribute, getcode() raises
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self, *a): return b""
        def getcode(self):
            raise AttributeError("no code here")
    monkeypatch.setattr(wayback_probe.urllib.request, "urlopen", lambda *a, **kw: _R())
    assert wayback_probe.probe_once() is False


def test_probe_once_returns_false_on_non_200(monkeypatch):
    class _R:
        status = 503
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self, *a): return b""
    monkeypatch.setattr(wayback_probe.urllib.request, "urlopen", lambda *a, **kw: _R())
    assert wayback_probe.probe_once() is False


@pytest.fixture
def probe_db(tmp_path, monkeypatch):
    """State-bearing fixture for manual-retry tests: fresh DB + stubbed
    probe_once so we can force ok/fail without touching the network."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    return wp, j


def test_manual_retry_failing_does_not_bump_consecutive_fails(probe_db, monkeypatch):
    wp, _ = probe_db
    wp.save_state(wp.ProbeState(state="up", consecutive_fails=2))
    monkeypatch.setattr(wp, "probe_once", lambda *a, **kw: False)
    snap = wp.run_probe_and_update()
    assert snap["probe_ok"] is False
    # Key guarantee: failing manual click doesn't push us closer to 'down'.
    # (Otherwise a frustrated user mashing the button would flip state at
    # the 3rd click.)
    assert snap["consecutive_fails"] == 2
    assert snap["state"] == "up"
    assert snap.get("flipped_to") is None


def test_probe_loop_reloads_state_each_iteration(probe_db, monkeypatch):
    """Regression: the loop used to cache `state` in a local variable
    for its lifetime, so a manual retry's write could be clobbered by
    the next scheduled save. Verify one iteration reads fresh state."""
    import asyncio
    wp, _ = probe_db
    # Start with state=up. Between iterations, something else writes
    # state=down to the DB — the next loop tick must respect it, not
    # save a stale "up" back on top.
    wp.save_state(wp.ProbeState(state="up"))
    monkeypatch.setattr(wp, "probe_once", lambda *a, **kw: False)

    # Simulate an external mutation (manual retry would do this).
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))

    # Drive exactly one iteration: run probe_loop briefly, then stop.
    async def driver():
        stop = asyncio.Event()
        task = asyncio.create_task(wp.probe_loop(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await task

    asyncio.run(driver())
    # The loop observed False against the RELOADED state (which was
    # already 'down'), so state stays down with fails incremented.
    after = wp.load_state()
    assert after.state == "down"
    assert after.consecutive_fails == 4  # 3 + this probe


def test_manual_retry_success_can_flip_up_and_release(probe_db, monkeypatch):
    wp, jobs_mod = probe_db
    # Seed state=down with one prior ok (so 1 more ok flips us up), plus
    # a deferred job that should get released on the flip.
    wp.save_state(wp.ProbeState(state="down", consecutive_ok=1, consecutive_fails=0))
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).replace(microsecond=0).isoformat()
    with jobs_mod.connect() as c:
        c.execute(
            "INSERT INTO jobs (target_url,timestamp,wayback_url,host,site_dir,"
            "log_path,flags_json,status,created_at,not_before) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("https://x/", "20200101000000",
             "https://web.archive.org/web/20200101000000/https://x/",
             "x", str(jobs_mod.OUTPUT_ROOT / "x" / "20200101000000"),
             str(jobs_mod.OUTPUT_ROOT / "x" / "20200101000000" / ".log"),
             "{}", "pending", jobs_mod.now_iso(), future),
        )
    monkeypatch.setattr(wp, "probe_once", lambda *a, **kw: True)
    snap = wp.run_probe_and_update()
    assert snap["probe_ok"] is True
    assert snap["flipped_to"] == "up"
    assert snap["state"] == "up"
    with jobs_mod.connect() as c:
        row = c.execute("SELECT not_before FROM jobs LIMIT 1").fetchone()
    assert row["not_before"] is None


def test_backoff_schedule():
    # attempts 0→first retry, 1→second, etc. Explicit early schedule,
    # doubling after 120m, capped at 24h.
    assert wayback_probe.backoff_seconds(0) == 5 * 60
    assert wayback_probe.backoff_seconds(1) == 10 * 60
    assert wayback_probe.backoff_seconds(2) == 15 * 60
    assert wayback_probe.backoff_seconds(3) == 20 * 60
    assert wayback_probe.backoff_seconds(4) == 30 * 60
    assert wayback_probe.backoff_seconds(5) == 45 * 60
    assert wayback_probe.backoff_seconds(6) == 60 * 60
    assert wayback_probe.backoff_seconds(7) == 120 * 60
    assert wayback_probe.backoff_seconds(8) == 240 * 60
    assert wayback_probe.backoff_seconds(9) == 480 * 60
    assert wayback_probe.backoff_seconds(10) == 960 * 60
    assert wayback_probe.backoff_seconds(11) == 24 * 3600
    assert wayback_probe.backoff_seconds(99) == 24 * 3600
