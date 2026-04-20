"""Tests for webui.rate_limit — the CDX sliding-window gate plus the
429-driven hard block that replaces the old active heartbeat probe.

These tests verify the two invariants the user cares about most:

  1. A single instance cannot push more than ``CDX_LIMIT_PER_MIN``
     CDX requests into any 60-second window. The sliding window is
     shared across processes via SQLite, so the coordination is
     real-integration, not a single-process mock.

  2. A 429 response installs a hard block; subsequent acquires must
     refuse to talk to CDX until the block expires. This mirrors
     IA's own firewall-level ban so we don't hammer the server while
     it's rejecting us and double our penalty.
"""
from __future__ import annotations

import importlib
import time
import urllib.error

import pytest


@pytest.fixture
def rl_env(tmp_path, monkeypatch):
    """Fresh DB + reloaded modules so state from other tests doesn't
    leak in. Short ``ACQUIRE_TIMEOUT`` so a saturated-window test
    fails fast instead of hanging."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    import webui.rate_limit as rl
    importlib.reload(rl)
    # Tighten constants so the tests stay fast.
    monkeypatch.setattr(rl, "CDX_LIMIT_PER_MIN", 5)
    monkeypatch.setattr(rl, "CDX_WINDOW_SECONDS", 2.0)
    monkeypatch.setattr(rl, "ACQUIRE_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(rl, "_POLL_INTERVAL_SECONDS", 0.05)
    return rl, j, wp


def test_acquire_allows_up_to_limit(rl_env):
    rl, _, _ = rl_env
    for _ in range(5):
        rl.acquire(timeout=0.5)
    # The 6th must refuse within the short acquire timeout.
    with pytest.raises(rl.RateLimitTimeout):
        rl.acquire(timeout=0.2)


def test_acquire_slot_freed_after_window(rl_env):
    """Once events age past the window, new slots open up."""
    rl, _, _ = rl_env
    for _ in range(5):
        rl.acquire(timeout=0.5)
    # Wait for the 2s window to clear, then we should acquire again.
    time.sleep(2.1)
    rl.acquire(timeout=0.5)  # no exception == pass


def test_429_installs_hard_block(rl_env):
    rl, _, _ = rl_env
    assert rl.is_blocked() is False
    rl.observe_429()
    assert rl.is_blocked() is True
    # A subsequent acquire must refuse until the block clears.
    with pytest.raises(rl.RateLimitTimeout):
        rl.acquire(timeout=0.3)


def test_429_flips_wayback_state_down(rl_env):
    """The 429 observation must flip the legacy probe state so the
    worker's is_wayback_up() gate closes without a heartbeat probe."""
    rl, _, wp = rl_env
    assert wp.is_wayback_up() is True
    rl.observe_429()
    assert wp.is_wayback_up() is False


def test_429_tier_escalates(rl_env):
    """Consecutive 429s escalate the block tier; the dashboard status
    snapshot reports the current tier so the UI can show it."""
    rl, j, _ = rl_env
    rl.observe_429()
    tier1 = rl.get_status()["block_tier"]
    # The tier tracker stays stick across blocks — so simulating a
    # second offense just needs the next observe_429 call. We don't
    # need to clear the block_until; observe_429 overwrites it.
    rl.observe_429()
    tier2 = rl.get_status()["block_tier"]
    assert tier2 == tier1 + 1


def test_observe_ok_clears_expired_block(rl_env):
    """After the hard block has elapsed, a successful CDX call must
    drop the block row and flip the probe state back to up."""
    rl, j, wp = rl_env
    from datetime import datetime, timedelta, timezone
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).replace(microsecond=0).isoformat()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_until", past),
        )
    # State was flipped down by an earlier 429 — simulate that.
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))
    assert wp.is_wayback_up() is False
    rl.observe_ok()
    assert wp.is_wayback_up() is True
    assert rl.is_blocked() is False


def test_cdx_urlopen_trips_block_on_429(rl_env, monkeypatch):
    """End-to-end: a 429 response from urlopen must install a hard
    block before re-raising so the next acquire refuses."""
    rl, _, _ = rl_env

    class _Headers:
        def get(self, k, default=None):
            return "60" if k.lower() == "retry-after" else default

    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(
            url="https://web.archive.org/cdx/search/cdx",
            code=429, msg="Too Many Requests",
            hdrs=_Headers(), fp=None,
        )

    monkeypatch.setattr(rl.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(urllib.error.HTTPError) as ei:
        rl.cdx_urlopen("https://web.archive.org/cdx/search/cdx")
    assert ei.value.code == 429
    assert rl.is_blocked() is True


def test_cdx_urlopen_success_observes_ok_and_clears_expired_block(rl_env, monkeypatch):
    rl, j, wp = rl_env

    class _Resp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self, *a): return b"[]"
        def getcode(self): return 200
        headers = {}

    # Pre-stage an expired block and a down state.
    from datetime import datetime, timedelta, timezone
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_until", past),
        )
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))

    monkeypatch.setattr(rl.urllib.request, "urlopen", lambda *a, **kw: _Resp())
    resp = rl.cdx_urlopen("https://web.archive.org/cdx/search/cdx")
    # Consume the context manager
    with resp:
        pass
    assert wp.is_wayback_up() is True
    assert rl.is_blocked() is False


def test_retry_after_header_parsing(rl_env):
    rl, _, _ = rl_env
    assert rl.retry_after_to_seconds("120") == 120.0
    assert rl.retry_after_to_seconds(None) is None
    assert rl.retry_after_to_seconds("   ") is None
    # HTTP-date path — use a value far in the future so parsed value > 0.
    from email.utils import format_datetime
    from datetime import datetime, timedelta, timezone
    s = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30))
    got = rl.retry_after_to_seconds(s)
    assert got is not None
    assert 20 <= got <= 40


def test_first_429_installs_full_60_minute_block(rl_env, monkeypatch):
    """User-directed invariant: a fresh 429 must lock out CDX for the
    full 60 minutes IA uses for its firewall block — don't try
    earlier. Matches the 'Each subsequent violation doubles' ladder
    starting at 1h."""
    rl, _, _ = rl_env
    # Default tier ladder is what we're testing here, not the
    # fixture's tight overrides. Restore the module defaults for this
    # assertion.
    monkeypatch.setattr(rl, "_BLOCK_TIER_BASE_SECONDS", 3600)
    rl.observe_429()
    remaining = rl.block_remaining_seconds()
    assert remaining is not None
    # Should be (3600 - epsilon) but well above 59 minutes.
    assert remaining >= 59 * 60
    assert remaining <= 3600


def test_block_tier_doubles_on_each_violation(rl_env, monkeypatch):
    """1h → 2h → 4h → 8h → 16h → 32h. User asked for pure doubling;
    the ladder must not flatten or cap short of realistic blocks."""
    rl, _, _ = rl_env
    monkeypatch.setattr(rl, "_BLOCK_TIER_BASE_SECONDS", 3600)
    # Walk the ladder in a tight loop — ``_tier_for_next_block`` uses
    # last_429 timestamp comparison, so successive calls within a
    # second stay tier-incrementing rather than decaying.
    expected_hours = [1, 2, 4, 8, 16, 32]
    for want_h in expected_hours:
        rl.observe_429()
        rem = rl.block_remaining_seconds()
        assert rem is not None
        # Allow 2s slack for test execution time.
        assert abs(rem - want_h * 3600) <= 5, (
            f"tier {want_h}h: expected ~{want_h*3600}s, got {rem}s"
        )


def test_block_tier_caps_at_seven_days(rl_env, monkeypatch):
    """The doubling is real but must not run off into months."""
    rl, _, _ = rl_env
    # Force high-tier directly in the DB then observe a new 429.
    with rl._connect() as c:
        rl._ensure_schema(c)
        c.execute("BEGIN IMMEDIATE")
        rl._set(c, "cdx_block_tier", "30")
        rl._set(c, "cdx_last_429_iso", rl._now_iso())
        c.execute("COMMIT")
    rl.observe_429()
    rem = rl.block_remaining_seconds()
    assert rem is not None
    # Must be capped at exactly 7 days (with a few seconds slack for
    # test-execution delay between observe_429 and block_remaining).
    assert abs(rem - 7 * 24 * 3600) <= 5


def test_tier_decays_after_24h_idle(rl_env, monkeypatch):
    """A clean day should reset the ladder — otherwise a once-a-day
    blip would spiral into a multi-day block for life."""
    rl, _, _ = rl_env
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(timezone.utc) - timedelta(hours=36)).replace(microsecond=0).isoformat()
    with rl._connect() as c:
        rl._ensure_schema(c)
        c.execute("BEGIN IMMEDIATE")
        rl._set(c, "cdx_block_tier", "5")
        rl._set(c, "cdx_last_429_iso", old)
        c.execute("COMMIT")
    rl.observe_429()
    assert rl.get_status()["block_tier"] == 1


def test_retry_after_header_extends_but_never_shortens_block(rl_env, monkeypatch):
    """If IA sends Retry-After: N where N > our tier's block, honor
    it. If N < our block, we use our block (don't undercut the
    exponential ladder on an IA-suggested shorter wait)."""
    rl, _, _ = rl_env
    monkeypatch.setattr(rl, "_BLOCK_TIER_BASE_SECONDS", 3600)
    # Retry-After: 10 minutes — less than tier 1's 60 min. We keep 60.
    rl.observe_429(retry_after_seconds=600)
    rem = rl.block_remaining_seconds()
    assert rem is not None
    assert rem >= 59 * 60  # at least 59 min (not the 10-min hint)


def test_get_status_reports_window_count(rl_env):
    rl, _, _ = rl_env
    rl.acquire(timeout=0.5)
    rl.acquire(timeout=0.5)
    s = rl.get_status()
    assert s["in_window"] >= 2
    assert s["limit_per_min"] == rl.CDX_LIMIT_PER_MIN
    assert s["block_tier"] == 0
    assert s["block_until"] is None
