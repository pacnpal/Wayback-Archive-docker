"""Tests for webui.wayback_probe.

The module used to run an active heartbeat probe against CDX; those
paths are gone now (state is driven passively by webui.rate_limit).
The tests here cover the surviving surface: ProbeState semantics,
state persistence round-trips, and the backoff schedule.
"""
from __future__ import annotations

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


def test_get_status_reads_both_settings_in_one_connection(tmp_path, monkeypatch):
    """Regression: get_status() used to load state via one connection
    then read state_since via another, so a concurrent save_state()
    commit between them could produce a mixed snapshot. Now both keys
    come from a single SELECT."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=4, consecutive_ok=0),
                  since_iso="2026-04-20T12:00:00+00:00")
    snap = wp.get_status()
    assert snap["state"] == "down"
    assert snap["since"] == "2026-04-20T12:00:00+00:00"
    assert snap["consecutive_fails"] == 4
    assert snap["consecutive_ok"] == 0


def test_is_wayback_up_fail_open(tmp_path, monkeypatch):
    """fresh DB → no state row → is_wayback_up() returns True so a
    clean install runs jobs immediately."""
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    assert wp.is_wayback_up() is True


def test_is_wayback_up_false_when_state_down(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import importlib
    import webui.jobs as j
    importlib.reload(j)
    j.init_db()
    import webui.wayback_probe as wp
    importlib.reload(wp)
    wp.save_state(wp.ProbeState(state="down", consecutive_fails=3))
    assert wp.is_wayback_up() is False


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
