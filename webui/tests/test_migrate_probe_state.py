"""Tests for the v0.3.x → v0.4.x probe-state migration.

``init_db()`` runs a one-shot cleanup that scrubs legacy
``wayback_probe_state`` rows left behind by the old heartbeat
probe. v0.4.x can't flip those counters back on its own (it only
writes state when it sees a real 429), so without the migration the
dashboard would sit wedged "down" after upgrade.
"""
from __future__ import annotations

import importlib
import json


def _fresh_jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    import webui.jobs as j
    importlib.reload(j)
    return j


def test_migration_clears_legacy_high_fail_counter(tmp_path, monkeypatch):
    """v0.3.x heartbeat probe accumulated consecutive_fails=272-ish
    after multi-hour outages. v0.4.x never writes above threshold=3,
    so anything higher is legacy and must be cleared."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state",
             json.dumps({"state": "down", "consecutive_fails": 272,
                         "consecutive_ok": 0})),
        )
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_state_since", "2026-04-20T17:38:13+00:00"),
        )
    # Re-init: the migration runs at every init_db() call so a fresh
    # container boot after upgrade clears the stale row.
    j.init_db()
    with j.connect() as c:
        rows = c.execute(
            "SELECT key FROM settings "
            "WHERE key IN ('wayback_probe_state','wayback_state_since')"
        ).fetchall()
    assert rows == [], f"legacy rows not cleared: {[r['key'] for r in rows]}"


def test_migration_clears_orphan_down_without_block(tmp_path, monkeypatch):
    """A 'down' state with no active cdx_block_until is also legacy —
    v0.4.x always pairs 'down' with an active block, so a bare 'down'
    row can never heal itself and must be wiped."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state",
             json.dumps({"state": "down", "consecutive_fails": 3,
                         "consecutive_ok": 0})),
        )
    j.init_db()
    with j.connect() as c:
        rows = c.execute(
            "SELECT key FROM settings "
            "WHERE key='wayback_probe_state'"
        ).fetchall()
    assert rows == []


def test_migration_preserves_down_state_with_active_block(tmp_path, monkeypatch):
    """A 'down' paired with an active cdx_block_until is a real
    v0.4.x state — must not be wiped by the migration or the
    worker would pop jobs during an active IA ban."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(microsecond=0).isoformat()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state",
             json.dumps({"state": "down", "consecutive_fails": 3,
                         "consecutive_ok": 0})),
        )
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("cdx_block_until", future),
        )
    j.init_db()
    with j.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_state'"
        ).fetchone()
    assert row is not None
    assert json.loads(row["value"])["state"] == "down"


def test_migration_preserves_fresh_up_state(tmp_path, monkeypatch):
    """A legitimate v0.4.x up-state row (oks=2, fails=0, state=up)
    must survive the migration so the cached state is used instead
    of falling back to 'unknown' on every restart."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state",
             json.dumps({"state": "up", "consecutive_fails": 0,
                         "consecutive_ok": 2})),
        )
    j.init_db()
    with j.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_state'"
        ).fetchone()
    assert row is not None
    assert json.loads(row["value"])["state"] == "up"


def test_migration_drops_orphan_probe_timeout_setting(tmp_path, monkeypatch):
    """v0.4.x removed the probe-timeout UI + endpoint. Any legacy
    setting row should be dropped so the settings table doesn't
    carry dead keys forever."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_timeout", "45"),
        )
    j.init_db()
    with j.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_timeout'"
        ).fetchone()
    assert row is None


def test_migration_tolerates_corrupt_json(tmp_path, monkeypatch):
    """A hand-edited or half-written probe_state row shouldn't crash
    init_db — just drop the bad data."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state", "not json at all {{{"),
        )
    # Must not raise.
    j.init_db()
    with j.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_state'"
        ).fetchone()
    assert row is None


def test_migration_is_idempotent(tmp_path, monkeypatch):
    """Running init_db() repeatedly must not damage healthy state."""
    j = _fresh_jobs(tmp_path, monkeypatch)
    j.init_db()
    with j.connect() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("wayback_probe_state",
             json.dumps({"state": "up", "consecutive_fails": 0,
                         "consecutive_ok": 2})),
        )
    for _ in range(3):
        j.init_db()
    with j.connect() as c:
        row = c.execute(
            "SELECT value FROM settings WHERE key='wayback_probe_state'"
        ).fetchone()
    assert json.loads(row["value"])["state"] == "up"
